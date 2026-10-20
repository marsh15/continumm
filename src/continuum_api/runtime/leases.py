"""Principal-scoped queue claims, heartbeat, and fenced intermediate writes."""

from __future__ import annotations

import hashlib
import json
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import DBAPIError

from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository, StepStatus

LEASE_DURATION_SECONDS = 60
HEARTBEAT_INTERVAL_SECONDS = 15
MAX_CHECKPOINT_BYTES = 32_768
MAX_TRANSACTION_ATTEMPTS = 3
RETRYABLE_SQLSTATES = frozenset({"40001", "40P01"})


class LeaseLost(Exception):
    """The worker's generation or lease is no longer current and unexpired."""


class CheckpointIdentityConflict(Exception):
    """A checkpoint key already stores different content in this attempt."""


@dataclass(frozen=True, slots=True)
class ExecutionLease:
    owner_id: UUID
    run_id: UUID
    step_id: UUID
    job_id: UUID
    attempt_id: UUID
    generation: int
    lease_owner: str
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class CheckpointReceipt:
    checkpoint_id: UUID
    checkpoint_key: str
    payload_sha256: bytes
    payload: dict[str, Any]
    adopted: bool


@dataclass(frozen=True, slots=True)
class LockedExecution:
    run_status: str
    run_deadline_at: datetime | None
    step_status: str
    job_status: str
    generation: int
    lease_owner: str | None
    lease_expires_at: datetime | None
    attempt_status: str
    database_now: datetime


class QueueService:
    """Claim one due job at a time and fence every worker-owned write."""

    def __init__(self, engine: Engine, principal: Principal) -> None:
        self._engine = engine
        self._principal = principal

    def claim_one(self, lease_owner: str, *, run_id: UUID | None = None) -> ExecutionLease | None:
        lease_owner = lease_owner.strip()
        if not lease_owner or len(lease_owner) > 128:
            raise ValueError("runtime_lease_owner_invalid")
        if run_id is not None and not isinstance(run_id, UUID):
            raise ValueError("runtime_claim_run_id_invalid")
        return _retry_database_only(lambda: self._claim_once(lease_owner, run_id))

    def heartbeat(self, lease: ExecutionLease) -> ExecutionLease:
        return _retry_database_only(lambda: self._heartbeat_once(lease))

    def persist_checkpoint(
        self,
        lease: ExecutionLease,
        checkpoint_key: str,
        payload: dict[str, Any],
    ) -> CheckpointReceipt:
        checkpoint_key = checkpoint_key.strip()
        if not checkpoint_key or len(checkpoint_key) > 128:
            raise ValueError("runtime_checkpoint_key_invalid")
        payload_json = _canonical_json(payload)
        if len(payload_json.encode("utf-8")) > MAX_CHECKPOINT_BYTES:
            raise ValueError("runtime_checkpoint_too_large")
        payload_digest = hashlib.sha256(payload_json.encode("utf-8")).digest()
        return _retry_database_only(
            lambda: self._persist_checkpoint_once(
                lease, checkpoint_key, payload_json, payload_digest
            )
        )

    def _claim_once(self, lease_owner: str, run_id: UUID | None = None) -> ExecutionLease | None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            owner_id = self._principal.account_id
            candidate = connection.execute(
                text(
                    """
                    SELECT run.run_id, run.status, run.deadline_at
                    FROM runtime_private.runs AS run
                    WHERE run.owner_id=:owner_id
                      AND (CAST(:run_id AS uuid) IS NULL OR run.run_id=CAST(:run_id AS uuid))
                      AND run.status IN ('queued', 'running')
                      AND (run.deadline_at IS NULL OR run.deadline_at > clock_timestamp())
                      AND (
                        EXISTS (
                            SELECT 1
                            FROM runtime_private.steps AS step
                            JOIN runtime_private.jobs AS job
                              ON job.owner_id=step.owner_id AND job.run_id=step.run_id
                             AND job.step_id=step.step_id
                            WHERE step.owner_id=run.owner_id AND step.run_id=run.run_id
                              AND step.status='queued' AND job.status='queued'
                              AND job.available_at <= clock_timestamp()
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM runtime_private.steps AS step
                            JOIN runtime_private.jobs AS job
                              ON job.owner_id=step.owner_id AND job.run_id=step.run_id
                             AND job.step_id=step.step_id
                            WHERE step.owner_id=run.owner_id AND step.run_id=run.run_id
                              AND run.status='running' AND step.status='running'
                              AND job.status='leased' AND job.lease_expires_at <= clock_timestamp()
                              AND job.replay_policy='replay_safe'
                        )
                      )
                    ORDER BY run.created_at, run.run_id
                    LIMIT 1
                    FOR UPDATE OF run SKIP LOCKED
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ).one_or_none()
            if candidate is None:
                return None
            run_id = candidate.run_id

            step = connection.execute(
                text(
                    """
                    SELECT step_id, status
                    FROM runtime_private.steps AS step
                    WHERE owner_id=:owner_id AND run_id=:run_id
                      AND (
                        (step.status='queued' AND EXISTS (
                            SELECT 1 FROM runtime_private.jobs AS job
                            WHERE job.owner_id=step.owner_id AND job.run_id=step.run_id
                              AND job.step_id=step.step_id AND job.status='queued'
                              AND job.available_at <= clock_timestamp()
                        ))
                        OR (step.status='running' AND EXISTS (
                            SELECT 1 FROM runtime_private.jobs AS job
                            WHERE job.owner_id=step.owner_id AND job.run_id=step.run_id
                              AND job.step_id=step.step_id AND job.status='leased'
                              AND job.lease_expires_at <= clock_timestamp()
                              AND job.replay_policy='replay_safe'
                        ))
                      )
                    ORDER BY step_id
                    LIMIT 1
                    FOR UPDATE OF step SKIP LOCKED
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ).one_or_none()
            if step is None:
                return None
            step_id = step.step_id

            job = connection.execute(
                text(
                    """
                    SELECT job_id, status, generation, lease_owner, lease_expires_at,
                           available_at, replay_policy
                    FROM runtime_private.jobs AS job
                    WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id
                      AND (
                        (status='queued' AND available_at <= clock_timestamp())
                        OR (status='leased' AND lease_expires_at <= clock_timestamp()
                            AND replay_policy='replay_safe')
                      )
                    ORDER BY job_id
                    LIMIT 1
                    FOR UPDATE OF job SKIP LOCKED
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id, "step_id": step_id},
            ).one_or_none()
            if job is None:
                return None

            previous_attempt = None
            if job.status == "leased":
                previous_attempt = connection.execute(
                    text(
                        """
                        SELECT attempt_id, status
                        FROM runtime_private.attempts
                        WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id
                          AND generation=:generation
                        FOR UPDATE
                        """
                    ),
                    {
                        "owner_id": owner_id,
                        "run_id": run_id,
                        "job_id": job.job_id,
                        "generation": job.generation,
                    },
                ).one_or_none()
            database_now: datetime = connection.execute(
                text("SELECT clock_timestamp()")
            ).scalar_one()
            run_status = candidate.status
            is_new_job = (
                job.status == "queued"
                and step.status == StepStatus.QUEUED.value
                and run_status in {RunStatus.QUEUED.value, RunStatus.RUNNING.value}
                and (candidate.deadline_at is None or candidate.deadline_at > database_now)
                and job.available_at <= database_now
            )
            is_expired_lease = (
                job.status == "leased"
                and job.replay_policy == "replay_safe"
                and step.status == StepStatus.RUNNING.value
                and run_status == RunStatus.RUNNING.value
                and (candidate.deadline_at is None or candidate.deadline_at > database_now)
                and job.lease_expires_at is not None
                and job.lease_expires_at <= database_now
            )
            if not is_new_job and not is_expired_lease:
                return None

            if is_expired_lease:
                if previous_attempt is None or previous_attempt.status != "running":
                    raise LeaseLost("expired_runtime_attempt_missing")
                connection.execute(
                    text(
                        "UPDATE runtime_private.attempts "
                        "SET status='unknown', finished_at=:database_now, "
                        "failure_class='worker_lost' "
                        "WHERE attempt_id=:attempt_id AND status='running'"
                    ),
                    {"database_now": database_now, "attempt_id": previous_attempt.attempt_id},
                )

            generation = job.generation + 1
            attempt_id = uuid4()
            lease_expires_at = database_now + timedelta(seconds=LEASE_DURATION_SECONDS)
            if run_status == RunStatus.QUEUED.value:
                connection.execute(
                    text(
                        "UPDATE runtime_private.runs SET status='running' "
                        "WHERE owner_id=:owner_id AND run_id=:run_id"
                    ),
                    {"owner_id": owner_id, "run_id": run_id},
                )
            if is_new_job and step.status == StepStatus.QUEUED.value:
                connection.execute(
                    text(
                        "UPDATE runtime_private.steps SET status='running' "
                        "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
                    ),
                    {"owner_id": owner_id, "run_id": run_id, "step_id": step_id},
                )
            connection.execute(
                text(
                    """
                    UPDATE runtime_private.jobs
                    SET status='leased', generation=:generation, lease_owner=:lease_owner,
                        lease_expires_at=:lease_expires_at
                    WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id
                      AND job_id=:job_id AND generation=:previous_generation
                    """
                ),
                {
                    "generation": generation,
                    "lease_owner": lease_owner,
                    "lease_expires_at": lease_expires_at,
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "step_id": step_id,
                    "job_id": job.job_id,
                    "previous_generation": job.generation,
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO runtime_private.attempts (
                        attempt_id, owner_id, run_id, job_id, generation,
                        lease_owner, status, started_at
                    ) VALUES (
                        :attempt_id, :owner_id, :run_id, :job_id, :generation,
                        :lease_owner, 'running', :started_at
                    )
                    """
                ),
                {
                    "attempt_id": attempt_id,
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "job_id": job.job_id,
                    "generation": generation,
                    "lease_owner": lease_owner,
                    "started_at": database_now,
                },
            )
            _append_claim_event(
                connection,
                owner_id=owner_id,
                run_id=run_id,
                event_type="job.lease_reclaimed" if is_expired_lease else "job.claimed",
                worker_id=lease_owner,
                payload={
                    "attempt_id": str(attempt_id),
                    "generation": generation,
                    "job_id": str(job.job_id),
                    "step_id": str(step_id),
                },
            )
            final_database_now: datetime = connection.execute(
                text("SELECT clock_timestamp()")
            ).scalar_one()
            if lease_expires_at <= final_database_now or (
                candidate.deadline_at is not None and candidate.deadline_at <= final_database_now
            ):
                raise LeaseLost("runtime_lease_expired_before_commit")
            return ExecutionLease(
                owner_id=owner_id,
                run_id=run_id,
                step_id=step_id,
                job_id=job.job_id,
                attempt_id=attempt_id,
                generation=generation,
                lease_owner=lease_owner,
                lease_expires_at=lease_expires_at,
            )

    def _heartbeat_once(self, lease: ExecutionLease) -> ExecutionLease:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            locked = lock_current_execution(connection, self._principal, lease)
            new_expiry = locked.database_now + timedelta(seconds=LEASE_DURATION_SECONDS)
            updated = connection.execute(
                text(
                    """
                    UPDATE runtime_private.jobs
                    SET lease_expires_at=:new_expiry
                    WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id
                      AND job_id=:job_id AND status='leased' AND generation=:generation
                      AND lease_owner=:lease_owner AND lease_expires_at > :database_now
                    """
                ),
                {
                    "new_expiry": new_expiry,
                    "owner_id": lease.owner_id,
                    "run_id": lease.run_id,
                    "step_id": lease.step_id,
                    "job_id": lease.job_id,
                    "generation": lease.generation,
                    "lease_owner": lease.lease_owner,
                    "database_now": locked.database_now,
                },
            )
            if updated.rowcount != 1:
                raise LeaseLost("runtime_lease_lost")
            final_database_now: datetime = connection.execute(
                text("SELECT clock_timestamp()")
            ).scalar_one()
            if (
                locked.lease_expires_at is None
                or locked.lease_expires_at <= final_database_now
                or new_expiry <= final_database_now
                or (
                    locked.run_deadline_at is not None
                    and locked.run_deadline_at <= final_database_now
                )
            ):
                raise LeaseLost("runtime_lease_expired_before_commit")
            return replace(lease, lease_expires_at=new_expiry)

    def _persist_checkpoint_once(
        self,
        lease: ExecutionLease,
        checkpoint_key: str,
        payload_json: str,
        payload_digest: bytes,
    ) -> CheckpointReceipt:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            locked = lock_current_execution(connection, self._principal, lease)
            existing = (
                connection.execute(
                    text(
                        """
                    SELECT checkpoint_id, payload_sha256, payload
                    FROM runtime_private.attempt_checkpoints
                    WHERE owner_id=:owner_id AND run_id=:run_id
                      AND attempt_id=:attempt_id AND checkpoint_key=:checkpoint_key
                    """
                    ),
                    {
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                        "attempt_id": lease.attempt_id,
                        "checkpoint_key": checkpoint_key,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if existing["payload_sha256"] != payload_digest:
                    raise CheckpointIdentityConflict("runtime_checkpoint_identity_conflict")
                return CheckpointReceipt(
                    checkpoint_id=existing["checkpoint_id"],
                    checkpoint_key=checkpoint_key,
                    payload_sha256=payload_digest,
                    payload=existing["payload"],
                    adopted=True,
                )

            checkpoint_id = uuid4()
            connection.execute(
                text(
                    """
                    INSERT INTO runtime_private.attempt_checkpoints (
                        checkpoint_id, owner_id, run_id, step_id, job_id, generation,
                        attempt_id, lease_owner, checkpoint_key, payload_sha256, payload
                    ) VALUES (
                        :checkpoint_id, :owner_id, :run_id, :step_id, :job_id, :generation,
                        :attempt_id, :lease_owner, :checkpoint_key, :payload_sha256,
                        CAST(:payload AS jsonb)
                    )
                    """
                ),
                {
                    "checkpoint_id": checkpoint_id,
                    "owner_id": lease.owner_id,
                    "run_id": lease.run_id,
                    "step_id": lease.step_id,
                    "job_id": lease.job_id,
                    "generation": lease.generation,
                    "attempt_id": lease.attempt_id,
                    "lease_owner": lease.lease_owner,
                    "checkpoint_key": checkpoint_key,
                    "payload_sha256": payload_digest,
                    "payload": payload_json,
                },
            )
            final_database_now: datetime = connection.execute(
                text("SELECT clock_timestamp()")
            ).scalar_one()
            if (
                locked.lease_expires_at is None
                or locked.lease_expires_at <= final_database_now
                or (
                    locked.run_deadline_at is not None
                    and locked.run_deadline_at <= final_database_now
                )
            ):
                raise LeaseLost("runtime_lease_expired_before_commit")
            return CheckpointReceipt(
                checkpoint_id=checkpoint_id,
                checkpoint_key=checkpoint_key,
                payload_sha256=payload_digest,
                payload=json.loads(payload_json),
                adopted=False,
            )


def lock_current_execution(
    connection: Connection, principal: Principal, lease: ExecutionLease
) -> LockedExecution:
    """Lock run, step, job, and attempt in that order, then check DB time."""

    if principal.account_id != lease.owner_id:
        raise LeaseLost("runtime_lease_owner_mismatch")
    run = connection.execute(
        text(
            "SELECT run.status, run.deadline_at, "
            "COALESCE((to_jsonb(run)->>'content_expires_at')::timestamptz, "
            "'9999-12-31 23:59:59+00'::timestamptz) > clock_timestamp() "
            "AS content_active, "
            "to_jsonb(run)->>'content_deleted_at' AS content_deleted_at "
            "FROM runtime_private.runs AS run "
            "WHERE owner_id=:owner_id AND run_id=:run_id FOR UPDATE"
        ),
        {"owner_id": lease.owner_id, "run_id": lease.run_id},
    ).one_or_none()
    if run is None:
        raise LeaseLost("runtime_run_not_found")
    step = connection.execute(
        text(
            "SELECT status FROM runtime_private.steps "
            "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id FOR UPDATE"
        ),
        {"owner_id": lease.owner_id, "run_id": lease.run_id, "step_id": lease.step_id},
    ).one_or_none()
    if step is None:
        raise LeaseLost("runtime_step_not_found")
    job = connection.execute(
        text(
            """
            SELECT status, generation, lease_owner, lease_expires_at
            FROM runtime_private.jobs
            WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id AND job_id=:job_id
            FOR UPDATE
            """
        ),
        {
            "owner_id": lease.owner_id,
            "run_id": lease.run_id,
            "step_id": lease.step_id,
            "job_id": lease.job_id,
        },
    ).one_or_none()
    if job is None:
        raise LeaseLost("runtime_job_not_found")
    attempt = connection.execute(
        text(
            """
            SELECT status, lease_owner
            FROM runtime_private.attempts
            WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id
              AND generation=:generation AND attempt_id=:attempt_id
            FOR UPDATE
            """
        ),
        {
            "owner_id": lease.owner_id,
            "run_id": lease.run_id,
            "job_id": lease.job_id,
            "generation": lease.generation,
            "attempt_id": lease.attempt_id,
        },
    ).one_or_none()
    if attempt is None:
        raise LeaseLost("runtime_attempt_not_found")
    database_now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()

    if (
        run.status != RunStatus.RUNNING.value
        or (run.deadline_at is not None and run.deadline_at <= database_now)
        or not run.content_active
        or run.content_deleted_at is not None
        or step.status != StepStatus.RUNNING.value
        or job.status != "leased"
        or job.generation != lease.generation
        or job.lease_owner != lease.lease_owner
        or job.lease_expires_at is None
        or job.lease_expires_at <= database_now
        or attempt.status != "running"
        or attempt.lease_owner != lease.lease_owner
    ):
        raise LeaseLost("runtime_lease_lost")
    return LockedExecution(
        run_status=run.status,
        run_deadline_at=run.deadline_at,
        step_status=step.status,
        job_status=job.status,
        generation=job.generation,
        lease_owner=job.lease_owner,
        lease_expires_at=job.lease_expires_at,
        attempt_status=attempt.status,
        database_now=database_now,
    )


def _append_claim_event(
    connection: Connection,
    *,
    owner_id: UUID,
    run_id: UUID,
    event_type: str,
    worker_id: str,
    payload: dict[str, Any],
) -> None:
    sequence: int = connection.execute(
        text(
            "SELECT coalesce(max(sequence), 0) + 1 FROM runtime_private.events "
            "WHERE owner_id=:owner_id AND run_id=:run_id"
        ),
        {"owner_id": owner_id, "run_id": run_id},
    ).scalar_one()
    connection.execute(
        text(
            """
            INSERT INTO runtime_private.events (
                event_id, owner_id, run_id, sequence, event_type, actor_type, actor_id, payload
            ) VALUES (
                :event_id, :owner_id, :run_id, :sequence, :event_type, 'worker', :worker_id,
                CAST(:payload AS jsonb)
            )
            """
        ),
        {
            "event_id": uuid4(),
            "owner_id": owner_id,
            "run_id": run_id,
            "sequence": sequence,
            "event_type": event_type,
            "worker_id": worker_id,
            "payload": _canonical_json(payload),
        },
    )


def _retry_database_only[T](operation: Callable[[], T]) -> T:
    for attempt in range(MAX_TRANSACTION_ATTEMPTS):
        try:
            return operation()
        except DBAPIError as error:
            if (
                attempt + 1 == MAX_TRANSACTION_ATTEMPTS
                or _sqlstate(error) not in RETRYABLE_SQLSTATES
            ):
                raise
            time.sleep(random.uniform(0, 0.01 * (2**attempt)))
    raise AssertionError("runtime_transaction_retry_loop_exhausted")


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )


def _sqlstate(error: DBAPIError) -> str | None:
    state = getattr(error.orig, "sqlstate", None)
    if state is None:
        state = getattr(error.orig, "pgcode", None)
    return state
