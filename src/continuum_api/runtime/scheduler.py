"""Fenced failure policy and restart-safe due-work recovery."""

from __future__ import annotations

import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from uuid import UUID

from sqlalchemy import Engine, text
from sqlalchemy.engine import Connection

from continuum_api.runtime.leases import (
    ExecutionLease,
    LeaseLost,
    _retry_database_only,
    lock_current_execution,
)
from continuum_api.runtime.persistence import (
    AttemptStatus,
    EventActor,
    Principal,
    ReplayPolicy,
    RunStatus,
    RuntimeRepository,
    StepStatus,
)

MAX_FAILURE_ATTEMPTS = 3
MAX_RECOVERY_BATCH = 100
MAX_RETRY_DELAY_SECONDS = 60


class FailureClass(StrEnum):
    INVALID_INPUT = "invalid_input"
    MISSING_CREDENTIAL = "missing_credential"
    TRANSIENT_READ = "transient_read"
    UNSUPPORTED_ENVIRONMENT = "unsupported_environment"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    MODEL_RESPONSE_LOST = "model_response_lost"
    UNKNOWN_EFFECT = "unknown_effect"
    REMOTE_CONFLICT = "remote_conflict"
    CORRUPT_ARTIFACT = "corrupt_artifact"


@dataclass(frozen=True, slots=True)
class FailureDecision:
    attempt_status: AttemptStatus
    job_status: str
    step_status: StepStatus
    run_status: RunStatus | None
    failure_count: int
    available_at: datetime | None
    capacity_resume_at: datetime | None
    retry_scheduled: bool
    event_type: str


@dataclass(frozen=True, slots=True)
class FailureReceipt:
    owner_id: UUID
    run_id: UUID
    job_id: UUID
    attempt_id: UUID
    failure_class: FailureClass
    failure_count: int
    run_status: RunStatus
    available_at: datetime | None
    capacity_resume_at: datetime | None
    retry_scheduled: bool
    event_sequence: int


class RuntimeRecoveryService:
    """Persist activity failures and recover due jobs using PostgreSQL only."""

    def __init__(self, engine: Engine, principal: Principal) -> None:
        self._engine = engine
        self._principal = principal

    def fail_attempt(
        self,
        *,
        lease: ExecutionLease,
        failure_class: FailureClass,
        retry_after: datetime | None = None,
    ) -> FailureReceipt:
        if lease.owner_id != self._principal.account_id:
            raise LeaseLost("runtime_lease_owner_mismatch")
        _validate_retry_after(retry_after)
        return _retry_database_only(
            lambda: self._fail_attempt_once(lease, FailureClass(failure_class), retry_after)
        )

    def _fail_attempt_once(
        self,
        lease: ExecutionLease,
        failure_class: FailureClass,
        retry_after: datetime | None,
    ) -> FailureReceipt:
        with self._engine.begin() as connection:
            repository = RuntimeRepository(connection, self._principal)
            locked = lock_current_execution(connection, self._principal, lease)
            job = connection.execute(
                text(
                    "SELECT failure_count FROM runtime_private.jobs "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id"
                ),
                {"owner_id": lease.owner_id, "run_id": lease.run_id, "job_id": lease.job_id},
            ).one()
            decision = plan_failure(
                failure_class,
                job.failure_count,
                database_now=locked.database_now,
                run_deadline=locked.run_deadline_at,
                retry_after=retry_after,
            )
            attempt_update = connection.execute(
                text(
                    "UPDATE runtime_private.attempts "
                    "SET status=:status, finished_at=:finished_at, failure_class=:failure_class "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id "
                    "AND generation=:generation AND attempt_id=:attempt_id "
                    "AND lease_owner=:lease_owner AND status='running'"
                ),
                {
                    "status": decision.attempt_status.value,
                    "finished_at": locked.database_now,
                    "failure_class": failure_class.value,
                    "owner_id": lease.owner_id,
                    "run_id": lease.run_id,
                    "job_id": lease.job_id,
                    "generation": lease.generation,
                    "attempt_id": lease.attempt_id,
                    "lease_owner": lease.lease_owner,
                },
            )
            if attempt_update.rowcount != 1:
                raise LeaseLost("runtime_attempt_not_current")

            job_update = connection.execute(
                text(
                    "UPDATE runtime_private.jobs SET status=:status, "
                    "failure_count=:failure_count, "
                    "available_at=coalesce(:available_at, available_at), "
                    "capacity_resume_at=:capacity_resume_at, "
                    "lease_owner=NULL, lease_expires_at=NULL "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id "
                    "AND job_id=:job_id AND generation=:generation AND lease_owner=:lease_owner"
                ),
                {
                    "status": decision.job_status,
                    "failure_count": decision.failure_count,
                    "available_at": decision.available_at,
                    "capacity_resume_at": decision.capacity_resume_at,
                    "owner_id": lease.owner_id,
                    "run_id": lease.run_id,
                    "step_id": lease.step_id,
                    "job_id": lease.job_id,
                    "generation": lease.generation,
                    "lease_owner": lease.lease_owner,
                },
            )
            if job_update.rowcount != 1:
                raise LeaseLost("runtime_job_not_current")
            connection.execute(
                text(
                    "UPDATE runtime_private.steps SET status=:status "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
                ),
                {
                    "status": decision.step_status.value,
                    "owner_id": lease.owner_id,
                    "run_id": lease.run_id,
                    "step_id": lease.step_id,
                },
            )
            run_status = decision.run_status or RunStatus(locked.run_status)
            if decision.run_status is not None:
                connection.execute(
                    text(
                        "UPDATE runtime_private.runs SET status=:status "
                        "WHERE owner_id=:owner_id AND run_id=:run_id"
                    ),
                    {
                        "status": run_status.value,
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                    },
                )
            event_sequence = _append_event(
                connection,
                repository,
                owner_id=lease.owner_id,
                run_id=lease.run_id,
                event_type=decision.event_type,
                payload={
                    "attempt_id": str(lease.attempt_id),
                    "failure_class": failure_class.value,
                    "failure_count": decision.failure_count,
                    "job_id": str(lease.job_id),
                    "retry_at": (
                        None if decision.available_at is None else decision.available_at.isoformat()
                    ),
                    "capacity_resume_at": (
                        None
                        if decision.capacity_resume_at is None
                        else decision.capacity_resume_at.isoformat()
                    ),
                    "retry_scheduled": decision.retry_scheduled,
                },
            )
            _assert_execution_live(connection, locked.lease_expires_at, locked.run_deadline_at)
            return FailureReceipt(
                owner_id=lease.owner_id,
                run_id=lease.run_id,
                job_id=lease.job_id,
                attempt_id=lease.attempt_id,
                failure_class=failure_class,
                failure_count=decision.failure_count,
                run_status=run_status,
                available_at=decision.available_at,
                capacity_resume_at=decision.capacity_resume_at,
                retry_scheduled=decision.retry_scheduled,
                event_sequence=event_sequence,
            )

    def recover_due(self, *, limit: int = 25) -> int:
        if not 1 <= limit <= MAX_RECOVERY_BATCH:
            raise ValueError("runtime_recovery_limit_invalid")
        return _retry_database_only(lambda: self._recover_due_once(limit))

    def _recover_due_once(self, limit: int) -> int:
        with self._engine.begin() as connection:
            repository = RuntimeRepository(connection, self._principal)
            runs = (
                connection.execute(
                    text(
                        "SELECT run.run_id, run.status, run.deadline_at "
                        "FROM runtime_private.runs AS run "
                        "WHERE run.owner_id=:owner_id "
                        "AND run.status IN ('running', 'waiting_capacity') "
                        "AND (run.deadline_at IS NULL OR run.deadline_at > clock_timestamp()) "
                        "AND EXISTS (SELECT 1 FROM runtime_private.steps AS step "
                        "JOIN runtime_private.jobs AS job ON job.owner_id=step.owner_id "
                        "AND job.run_id=step.run_id AND job.step_id=step.step_id "
                        "WHERE step.owner_id=run.owner_id AND step.run_id=run.run_id AND "
                        "((run.status='running' AND step.status='running' AND job.status='leased' "
                        "AND job.lease_expires_at <= clock_timestamp()) OR "
                        "(run.status='waiting_capacity' AND job.status='queued' "
                        "AND job.capacity_resume_at IS NOT NULL "
                        "AND job.capacity_resume_at <= clock_timestamp()))) "
                        "ORDER BY run.created_at, run.run_id LIMIT :limit "
                        "FOR UPDATE OF run SKIP LOCKED"
                    ),
                    {"owner_id": self._principal.account_id, "limit": limit},
                )
                .mappings()
                .all()
            )
            recovered = 0
            for run in runs:
                if recovered >= limit:
                    break
                waiting_capacity = run["status"] == RunStatus.WAITING_CAPACITY.value
                steps: Sequence[UUID] = (
                    connection.execute(
                        text(
                            "SELECT step.step_id FROM runtime_private.steps AS step "
                            "WHERE step.owner_id=:owner_id AND step.run_id=:run_id AND "
                            "((:waiting_capacity AND EXISTS "
                            "(SELECT 1 FROM runtime_private.jobs AS job "
                            "WHERE job.owner_id=step.owner_id AND job.run_id=step.run_id "
                            "AND job.step_id=step.step_id AND job.status='queued' "
                            "AND job.capacity_resume_at IS NOT NULL "
                            "AND job.capacity_resume_at <= clock_timestamp())) OR "
                            "(NOT :waiting_capacity AND step.status='running' AND EXISTS "
                            "(SELECT 1 FROM runtime_private.jobs AS job "
                            "WHERE job.owner_id=step.owner_id "
                            "AND job.run_id=step.run_id AND job.step_id=step.step_id "
                            "AND job.status='leased' "
                            "AND job.lease_expires_at <= clock_timestamp()))) "
                            "ORDER BY step.step_id LIMIT :limit FOR UPDATE OF step SKIP LOCKED"
                        ),
                        {
                            "owner_id": self._principal.account_id,
                            "run_id": run["run_id"],
                            "waiting_capacity": waiting_capacity,
                            "limit": limit - recovered,
                        },
                    )
                    .scalars()
                    .all()
                )
                for step_id in steps:
                    if recovered >= limit:
                        break
                    job = connection.execute(
                        text(
                            "SELECT job_id, status, generation, lease_owner, lease_expires_at, "
                            "available_at, capacity_resume_at, replay_policy "
                            "FROM runtime_private.jobs "
                            "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id "
                            "AND ((:waiting_capacity AND status='queued' "
                            "AND capacity_resume_at IS NOT NULL "
                            "AND capacity_resume_at <= clock_timestamp()) OR "
                            "(NOT :waiting_capacity AND status='leased' "
                            "AND lease_expires_at <= clock_timestamp())) "
                            "ORDER BY job_id LIMIT 1 FOR UPDATE SKIP LOCKED"
                        ),
                        {
                            "owner_id": self._principal.account_id,
                            "run_id": run["run_id"],
                            "step_id": step_id,
                            "waiting_capacity": waiting_capacity,
                        },
                    ).one_or_none()
                    if job is None:
                        continue
                    if waiting_capacity:
                        database_now: datetime = connection.execute(
                            text("SELECT clock_timestamp()")
                        ).scalar_one()
                        if run["deadline_at"] is not None and run["deadline_at"] <= database_now:
                            continue
                        if job.capacity_resume_at is None or job.capacity_resume_at > database_now:
                            continue
                        connection.execute(
                            text(
                                "UPDATE runtime_private.runs SET status='running' "
                                "WHERE owner_id=:owner_id AND run_id=:run_id "
                                "AND status='waiting_capacity'"
                            ),
                            {"owner_id": self._principal.account_id, "run_id": run["run_id"]},
                        )
                        connection.execute(
                            text(
                                "UPDATE runtime_private.jobs SET capacity_resume_at=NULL "
                                "WHERE owner_id=:owner_id AND run_id=:run_id"
                            ),
                            {"owner_id": self._principal.account_id, "run_id": run["run_id"]},
                        )
                        _append_event(
                            connection,
                            repository,
                            owner_id=self._principal.account_id,
                            run_id=run["run_id"],
                            event_type="run.capacity_wait_ended",
                            payload={"job_id": str(job.job_id)},
                        )
                    else:
                        attempt = connection.execute(
                            text(
                                "SELECT attempt_id, lease_owner, status "
                                "FROM runtime_private.attempts "
                                "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id "
                                "AND generation=:generation FOR UPDATE"
                            ),
                            {
                                "owner_id": self._principal.account_id,
                                "run_id": run["run_id"],
                                "job_id": job.job_id,
                                "generation": job.generation,
                            },
                        ).one_or_none()
                        if attempt is None or attempt.status != AttemptStatus.RUNNING.value:
                            raise LeaseLost("expired_runtime_attempt_missing")
                        database_now = connection.execute(
                            text("SELECT clock_timestamp()")
                        ).scalar_one()
                        if run["deadline_at"] is not None and run["deadline_at"] <= database_now:
                            continue
                        if job.lease_expires_at is None or job.lease_expires_at > database_now:
                            continue
                        connection.execute(
                            text(
                                "UPDATE runtime_private.attempts SET status='unknown', "
                                "finished_at=:database_now, failure_class=:failure_class "
                                "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id "
                                "AND generation=:generation AND attempt_id=:attempt_id "
                                "AND lease_owner=:lease_owner AND status='running'"
                            ),
                            {
                                "database_now": database_now,
                                "failure_class": (
                                    FailureClass.UNKNOWN_EFFECT.value
                                    if job.replay_policy == ReplayPolicy.ATTENTION_ON_UNKNOWN.value
                                    else "worker_lost"
                                ),
                                "owner_id": self._principal.account_id,
                                "run_id": run["run_id"],
                                "job_id": job.job_id,
                                "generation": job.generation,
                                "attempt_id": attempt.attempt_id,
                                "lease_owner": attempt.lease_owner,
                            },
                        )
                        if job.replay_policy == ReplayPolicy.REPLAY_SAFE.value:
                            connection.execute(
                                text(
                                    "UPDATE runtime_private.jobs SET status='queued', "
                                    "lease_owner=NULL, lease_expires_at=NULL, "
                                    "available_at=:available_at "
                                    "WHERE owner_id=:owner_id AND run_id=:run_id "
                                    "AND job_id=:job_id "
                                    "AND generation=:generation"
                                ),
                                {
                                    "available_at": database_now,
                                    "owner_id": self._principal.account_id,
                                    "run_id": run["run_id"],
                                    "job_id": job.job_id,
                                    "generation": job.generation,
                                },
                            )
                            connection.execute(
                                text(
                                    "UPDATE runtime_private.steps SET status='queued' "
                                    "WHERE owner_id=:owner_id AND run_id=:run_id "
                                    "AND step_id=:step_id"
                                ),
                                {
                                    "owner_id": self._principal.account_id,
                                    "run_id": run["run_id"],
                                    "step_id": step_id,
                                },
                            )
                            event_type = "job.lease_expired_requeued"
                        else:
                            connection.execute(
                                text(
                                    "UPDATE runtime_private.jobs SET status='failed', "
                                    "lease_owner=NULL, lease_expires_at=NULL "
                                    "WHERE owner_id=:owner_id AND run_id=:run_id "
                                    "AND job_id=:job_id "
                                    "AND generation=:generation"
                                ),
                                {
                                    "owner_id": self._principal.account_id,
                                    "run_id": run["run_id"],
                                    "job_id": job.job_id,
                                    "generation": job.generation,
                                },
                            )
                            connection.execute(
                                text(
                                    "UPDATE runtime_private.steps SET status='failed' "
                                    "WHERE owner_id=:owner_id AND run_id=:run_id "
                                    "AND step_id=:step_id"
                                ),
                                {
                                    "owner_id": self._principal.account_id,
                                    "run_id": run["run_id"],
                                    "step_id": step_id,
                                },
                            )
                            connection.execute(
                                text(
                                    "UPDATE runtime_private.runs SET status='needs_attention' "
                                    "WHERE owner_id=:owner_id AND run_id=:run_id "
                                    "AND status='running'"
                                ),
                                {"owner_id": self._principal.account_id, "run_id": run["run_id"]},
                            )
                            event_type = "job.unknown_replay_stopped"
                        _append_event(
                            connection,
                            repository,
                            owner_id=self._principal.account_id,
                            run_id=run["run_id"],
                            event_type=event_type,
                            payload={
                                "attempt_id": str(attempt.attempt_id),
                                "generation": job.generation,
                                "job_id": str(job.job_id),
                                "replay_policy": job.replay_policy,
                            },
                        )
                    _assert_recovery_before_commit(connection, run["deadline_at"])
                    recovered += 1
                    if (
                        waiting_capacity
                        or job.replay_policy == ReplayPolicy.ATTENTION_ON_UNKNOWN.value
                    ):
                        break
            return recovered


def plan_failure(
    failure_class: FailureClass,
    failure_count: int,
    *,
    database_now: datetime,
    run_deadline: datetime | None,
    retry_after: datetime | None,
    jitter: Callable[[float, float], float] = random.uniform,
) -> FailureDecision:
    """Resolve the bounded T10 policy without performing persistence."""

    if failure_count < 0:
        raise ValueError("runtime_failure_count_invalid")
    if failure_class is FailureClass.MISSING_CREDENTIAL:
        return FailureDecision(
            attempt_status=AttemptStatus.PROTOCOL_FAILURE,
            job_status="queued",
            step_status=StepStatus.QUEUED,
            run_status=RunStatus.WAITING_CREDENTIALS,
            failure_count=failure_count,
            available_at=max(database_now, retry_after)
            if retry_after is not None
            else database_now,
            capacity_resume_at=None,
            retry_scheduled=False,
            event_type="run.waiting_credentials",
        )
    if failure_class is FailureClass.RESOURCE_EXHAUSTION:
        capacity_resume_at = (
            retry_after
            if retry_after is not None
            and retry_after > database_now
            and (run_deadline is None or retry_after < run_deadline)
            else None
        )
        return FailureDecision(
            attempt_status=AttemptStatus.PROTOCOL_FAILURE,
            job_status="queued",
            step_status=StepStatus.QUEUED,
            run_status=RunStatus.WAITING_CAPACITY,
            failure_count=failure_count,
            available_at=database_now,
            capacity_resume_at=capacity_resume_at,
            retry_scheduled=False,
            event_type="run.waiting_capacity",
        )
    if failure_class is FailureClass.TRANSIENT_READ:
        next_failure_count = failure_count + 1
        if next_failure_count < MAX_FAILURE_ATTEMPTS:
            maximum_delay = min(MAX_RETRY_DELAY_SECONDS, 2 ** min(failure_count, 6))
            available_at = database_now + timedelta(seconds=jitter(0.0, float(maximum_delay)))
            if retry_after is not None:
                available_at = max(available_at, retry_after)
            if run_deadline is None or available_at < run_deadline:
                return FailureDecision(
                    attempt_status=AttemptStatus.RETRYABLE_FAILURE,
                    job_status="queued",
                    step_status=StepStatus.QUEUED,
                    run_status=None,
                    failure_count=next_failure_count,
                    available_at=available_at,
                    capacity_resume_at=None,
                    retry_scheduled=True,
                    event_type="job.retry_scheduled",
                )
            event_type = "job.retry_rejected_by_deadline"
        else:
            event_type = "job.retry_exhausted"
        return FailureDecision(
            attempt_status=AttemptStatus.RETRYABLE_FAILURE,
            job_status="failed",
            step_status=StepStatus.FAILED,
            run_status=RunStatus.FAILED,
            failure_count=next_failure_count,
            available_at=None,
            capacity_resume_at=None,
            retry_scheduled=False,
            event_type=event_type,
        )

    next_failure_count = failure_count + 1
    if failure_class in {FailureClass.INVALID_INPUT, FailureClass.UNSUPPORTED_ENVIRONMENT}:
        return FailureDecision(
            attempt_status=(
                AttemptStatus.DEFINITIVE_FAILURE
                if failure_class is FailureClass.INVALID_INPUT
                else AttemptStatus.PROTOCOL_FAILURE
            ),
            job_status="failed",
            step_status=StepStatus.FAILED,
            run_status=RunStatus.FAILED,
            failure_count=next_failure_count,
            available_at=None,
            capacity_resume_at=None,
            retry_scheduled=False,
            event_type="job.failed",
        )
    return FailureDecision(
        attempt_status=(
            AttemptStatus.UNKNOWN
            if failure_class in {FailureClass.MODEL_RESPONSE_LOST, FailureClass.UNKNOWN_EFFECT}
            else AttemptStatus.PROTOCOL_FAILURE
        ),
        job_status="failed",
        step_status=StepStatus.FAILED,
        run_status=RunStatus.NEEDS_ATTENTION,
        failure_count=next_failure_count,
        available_at=None,
        capacity_resume_at=None,
        retry_scheduled=False,
        event_type="job.needs_attention",
    )


def _validate_retry_after(retry_after: datetime | None) -> None:
    if retry_after is not None and (retry_after.tzinfo is None or retry_after.utcoffset() is None):
        raise ValueError("runtime_retry_after_must_be_timezone_aware")


def _append_event(
    connection: Connection,
    repository: RuntimeRepository,
    *,
    owner_id: UUID,
    run_id: UUID,
    event_type: str,
    payload: dict[str, object],
) -> int:
    sequence: int = connection.execute(
        text(
            "SELECT coalesce(max(sequence), 0) + 1 FROM runtime_private.events "
            "WHERE owner_id=:owner_id AND run_id=:run_id"
        ),
        {"owner_id": owner_id, "run_id": run_id},
    ).scalar_one()
    repository.append_event(run_id, sequence, event_type, EventActor.SYSTEM, payload=payload)
    return sequence


def _assert_execution_live(
    connection: Connection, lease_expires_at: datetime | None, run_deadline: datetime | None
) -> None:
    now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
    if lease_expires_at is None or lease_expires_at <= now:
        raise LeaseLost("runtime_lease_expired_before_commit")
    if run_deadline is not None and run_deadline <= now:
        raise LeaseLost("runtime_run_expired_before_commit")


def _assert_recovery_before_commit(connection: Connection, run_deadline: datetime | None) -> None:
    now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
    if run_deadline is not None and run_deadline <= now:
        raise LeaseLost("runtime_run_expired_before_commit")
