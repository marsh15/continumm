"""Owner-scoped cancellation and dispatch-intent observation boundaries."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, text

from continuum_api.runtime.leases import ExecutionLease, LeaseLost, _retry_database_only
from continuum_api.runtime.persistence import (
    EventActor,
    Principal,
    RunStatus,
    RuntimeRepository,
    StepStatus,
)

MAX_INTENT_BYTES = 4096
_NONTERMINAL_RUN_STATES = frozenset(
    {
        RunStatus.QUEUED.value,
        RunStatus.RUNNING.value,
        RunStatus.WAITING_APPROVAL.value,
        RunStatus.WAITING_CREDENTIALS.value,
        RunStatus.WAITING_CAPACITY.value,
        RunStatus.NEEDS_ATTENTION.value,
    }
)


class CancellationError(Exception):
    """Base class for owner-scoped cancellation operations."""


class RuntimeRunNotFound(CancellationError):
    """The principal cannot access the requested run."""


class DispatchIntentConflict(CancellationError):
    """A stable dispatch key was reused with different intent or observation."""


@dataclass(frozen=True, slots=True)
class CancellationReceipt:
    run_id: UUID
    run_status: RunStatus
    event_sequence: int | None
    adopted: bool
    cancelled_jobs: int = 0
    invalidated_attempts: int = 0


@dataclass(frozen=True, slots=True)
class DispatchIntentReceipt:
    dispatch_id: UUID
    run_id: UUID
    operation_key: str
    generation: int
    request_sha256: bytes
    request_metadata: dict[str, Any]
    observed: bool
    adopted: bool


class CancellationService:
    """Cancel one owned run while fencing current execution in one DB commit."""

    def __init__(self, engine: Engine, principal: Principal) -> None:
        self._engine = engine
        self._principal = principal

    def cancel(self, run_id: UUID) -> CancellationReceipt:
        return _retry_database_only(lambda: self._cancel_once(run_id))

    def _cancel_once(self, run_id: UUID) -> CancellationReceipt:
        with self._engine.begin() as connection:
            repository = RuntimeRepository(connection, self._principal)
            owner_id = self._principal.account_id
            run = connection.execute(
                text(
                    "SELECT status, deadline_at FROM runtime_private.runs "
                    "WHERE owner_id=:owner_id AND run_id=:run_id FOR UPDATE"
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ).one_or_none()
            if run is None:
                raise RuntimeRunNotFound("runtime_run_not_found")

            # A repeated cancel and a cancel against another terminal outcome
            # are read-only. Terminal run state is never rewritten.
            current_status = RunStatus(run.status)
            if current_status not in _NONTERMINAL_RUN_STATES:
                sequence = None
                if current_status is RunStatus.CANCELLED:
                    sequence = connection.execute(
                        text(
                            "SELECT sequence FROM runtime_private.events "
                            "WHERE owner_id=:owner_id AND run_id=:run_id "
                            "AND event_type='run.cancelled' ORDER BY sequence LIMIT 1"
                        ),
                        {"owner_id": owner_id, "run_id": run_id},
                    ).scalar_one_or_none()
                return CancellationReceipt(
                    run_id=run_id,
                    run_status=current_status,
                    event_sequence=sequence,
                    adopted=current_status is RunStatus.CANCELLED,
                )

            receipt = _terminalize_locked_run(
                connection,
                repository,
                owner_id=owner_id,
                run_id=run_id,
                target_status=RunStatus.CANCELLED,
                event_type="run.cancelled",
                actor_type=EventActor.ACCOUNT,
                actor_id=str(owner_id),
                attempt_failure_class="cancelled",
            )
            assert receipt is not None
            return receipt

    def expire_due(self, *, limit: int = 50) -> int:
        """Expire at most ``limit`` owned runs whose database deadline elapsed."""

        if not 1 <= limit <= 100:
            raise ValueError("runtime_expiry_limit_invalid")
        expired = 0
        for _ in range(limit):
            if not _retry_database_only(self._expire_one_once):
                break
            expired += 1
        return expired

    def _expire_one_once(self) -> bool:
        with self._engine.begin() as connection:
            repository = RuntimeRepository(connection, self._principal)
            owner_id = self._principal.account_id
            run = (
                connection.execute(
                    text(
                        "SELECT run_id, status, deadline_at FROM runtime_private.runs "
                        "WHERE owner_id=:owner_id AND status IN "
                        "('queued', 'running', 'waiting_approval', 'waiting_credentials', "
                        "'waiting_capacity', 'needs_attention') "
                        "AND deadline_at <= clock_timestamp() "
                        "ORDER BY deadline_at, run_id LIMIT 1 FOR UPDATE SKIP LOCKED"
                    ),
                    {"owner_id": owner_id},
                )
                .mappings()
                .one_or_none()
            )
            if run is None:
                return False
            receipt = _terminalize_locked_run(
                connection,
                repository,
                owner_id=owner_id,
                run_id=run["run_id"],
                target_status=RunStatus.EXPIRED,
                event_type="run.expired",
                actor_type=EventActor.SYSTEM,
                actor_id=None,
                attempt_failure_class=None,
                deadline_at=run["deadline_at"],
                require_elapsed_deadline=True,
            )
            return receipt is not None


class DispatchIntentService:
    """Commit DB dispatch authorization and accept observations without I/O."""

    def __init__(self, engine: Engine, principal: Principal) -> None:
        self._engine = engine
        self._principal = principal

    def authorize(
        self,
        lease: ExecutionLease,
        operation_key: str,
        request_metadata: dict[str, Any],
    ) -> DispatchIntentReceipt:
        operation_key = operation_key.strip()
        if not operation_key or len(operation_key) > 128:
            raise ValueError("runtime_dispatch_key_invalid")
        request_json = _canonical_json(request_metadata)
        request_digest = hashlib.sha256(request_json.encode("utf-8")).digest()
        if len(request_json.encode("utf-8")) > MAX_INTENT_BYTES:
            raise ValueError("runtime_dispatch_metadata_too_large")
        return _retry_database_only(
            lambda: self._authorize_once(lease, operation_key, request_json, request_digest)
        )

    def observe(
        self, *, run_id: UUID, operation_key: str, observation: dict[str, Any]
    ) -> DispatchIntentReceipt:
        operation_key = operation_key.strip()
        if not operation_key or len(operation_key) > 128:
            raise ValueError("runtime_dispatch_key_invalid")
        observation_json = _canonical_json(observation)
        if len(observation_json.encode("utf-8")) > MAX_INTENT_BYTES:
            raise ValueError("runtime_observation_too_large")
        return _retry_database_only(
            lambda: self._observe_once(run_id, operation_key, observation_json)
        )

    def _authorize_once(
        self,
        lease: ExecutionLease,
        operation_key: str,
        request_json: str,
        request_digest: bytes,
    ) -> DispatchIntentReceipt:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            locked = _lock_dispatch_context(connection, self._principal, lease)
            owner_id = self._principal.account_id
            existing = (
                connection.execute(
                    text(
                        "SELECT dispatch_id, generation, request_sha256, request_metadata, "
                        "observation FROM runtime_private.dispatch_intents "
                        "WHERE owner_id=:owner_id AND run_id=:run_id AND operation_key=:key"
                    ),
                    {"owner_id": owner_id, "run_id": lease.run_id, "key": operation_key},
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if existing["request_sha256"] != request_digest:
                    raise DispatchIntentConflict("runtime_dispatch_intent_conflict")
                return DispatchIntentReceipt(
                    dispatch_id=existing["dispatch_id"],
                    run_id=lease.run_id,
                    operation_key=operation_key,
                    generation=existing["generation"],
                    request_sha256=request_digest,
                    request_metadata=existing["request_metadata"],
                    observed=existing["observation"] is not None,
                    adopted=True,
                )

            _assert_dispatch_lease_is_current(locked, lease)
            dispatch_id = uuid4()
            connection.execute(
                text(
                    "INSERT INTO runtime_private.dispatch_intents "
                    "(dispatch_id, owner_id, run_id, step_id, job_id, generation, "
                    "attempt_id, lease_owner, operation_key, request_sha256, request_metadata) "
                    "VALUES (:dispatch_id, :owner_id, :run_id, :step_id, :job_id, :generation, "
                    ":attempt_id, :lease_owner, :operation_key, :request_sha256, "
                    "CAST(:request_metadata AS jsonb))"
                ),
                {
                    "dispatch_id": dispatch_id,
                    "owner_id": owner_id,
                    "run_id": lease.run_id,
                    "step_id": lease.step_id,
                    "job_id": lease.job_id,
                    "generation": lease.generation,
                    "attempt_id": lease.attempt_id,
                    "lease_owner": lease.lease_owner,
                    "operation_key": operation_key,
                    "request_sha256": request_digest,
                    "request_metadata": request_json,
                },
            )
            final_now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if locked["lease_expires_at"] <= final_now or (
                locked["run_deadline_at"] is not None and locked["run_deadline_at"] <= final_now
            ):
                raise LeaseLost("runtime_lease_expired_before_commit")
            return DispatchIntentReceipt(
                dispatch_id=dispatch_id,
                run_id=lease.run_id,
                operation_key=operation_key,
                generation=lease.generation,
                request_sha256=request_digest,
                request_metadata=json.loads(request_json),
                observed=False,
                adopted=False,
            )

    def _observe_once(
        self, run_id: UUID, operation_key: str, observation_json: str
    ) -> DispatchIntentReceipt:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            owner_id = self._principal.account_id
            identity = (
                connection.execute(
                    text(
                        "SELECT step_id, job_id, generation, attempt_id FROM "
                        "runtime_private.dispatch_intents WHERE owner_id=:owner_id "
                        "AND run_id=:run_id AND operation_key=:key"
                    ),
                    {"owner_id": owner_id, "run_id": run_id, "key": operation_key},
                )
                .mappings()
                .one_or_none()
            )
            if identity is None:
                raise RuntimeRunNotFound("runtime_dispatch_intent_not_found")
            run = connection.execute(
                text(
                    "SELECT status FROM runtime_private.runs "
                    "WHERE owner_id=:owner_id AND run_id=:run_id FOR UPDATE"
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ).one_or_none()
            if run is None:
                raise RuntimeRunNotFound("runtime_run_not_found")
            connection.execute(
                text(
                    "SELECT step_id FROM runtime_private.steps WHERE owner_id=:owner_id "
                    "AND run_id=:run_id AND step_id=:step_id FOR UPDATE"
                ),
                {"owner_id": owner_id, "run_id": run_id, "step_id": identity["step_id"]},
            ).one_or_none()
            connection.execute(
                text(
                    "SELECT job_id FROM runtime_private.jobs WHERE owner_id=:owner_id "
                    "AND run_id=:run_id AND job_id=:job_id FOR UPDATE"
                ),
                {"owner_id": owner_id, "run_id": run_id, "job_id": identity["job_id"]},
            ).one_or_none()
            connection.execute(
                text(
                    "SELECT attempt_id FROM runtime_private.attempts WHERE owner_id=:owner_id "
                    "AND run_id=:run_id AND job_id=:job_id AND generation=:generation "
                    "AND attempt_id=:attempt_id FOR UPDATE"
                ),
                {
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "job_id": identity["job_id"],
                    "generation": identity["generation"],
                    "attempt_id": identity["attempt_id"],
                },
            ).one_or_none()
            intent = (
                connection.execute(
                    text(
                        "SELECT dispatch_id, request_sha256, request_metadata, observation, "
                        "generation FROM runtime_private.dispatch_intents "
                        "WHERE owner_id=:owner_id AND run_id=:run_id AND operation_key=:key "
                        "FOR UPDATE"
                    ),
                    {"owner_id": owner_id, "run_id": run_id, "key": operation_key},
                )
                .mappings()
                .one_or_none()
            )
            if intent is None:
                raise RuntimeRunNotFound("runtime_dispatch_intent_not_found")
            observation = json.loads(observation_json)
            if intent["observation"] is not None:
                if intent["observation"] != observation:
                    raise DispatchIntentConflict("runtime_dispatch_observation_conflict")
                return DispatchIntentReceipt(
                    dispatch_id=intent["dispatch_id"],
                    run_id=run_id,
                    operation_key=operation_key,
                    generation=intent["generation"],
                    request_sha256=intent["request_sha256"],
                    request_metadata=intent["request_metadata"],
                    observed=True,
                    adopted=True,
                )

            observed_at: datetime = connection.execute(
                text("SELECT clock_timestamp()")
            ).scalar_one()
            connection.execute(
                text(
                    "UPDATE runtime_private.dispatch_intents "
                    "SET observation=CAST(:observation AS jsonb), "
                    "observed_at=:observed_at "
                    "WHERE dispatch_id=:dispatch_id AND observation IS NULL"
                ),
                {
                    "observation": observation_json,
                    "observed_at": observed_at,
                    "dispatch_id": intent["dispatch_id"],
                },
            )
            sequence = _next_event_sequence(connection, owner_id, run_id)
            RuntimeRepository(connection, self._principal).append_event(
                run_id,
                sequence,
                "dispatch.observed",
                EventActor.PROVIDER,
                payload={"operation_key": operation_key, "observation": observation},
            )
            return DispatchIntentReceipt(
                dispatch_id=intent["dispatch_id"],
                run_id=run_id,
                operation_key=operation_key,
                generation=intent["generation"],
                request_sha256=intent["request_sha256"],
                request_metadata=intent["request_metadata"],
                observed=True,
                adopted=False,
            )


def _lock_dispatch_context(
    connection: Connection, principal: Principal, lease: ExecutionLease
) -> dict[str, Any]:
    if principal.account_id != lease.owner_id:
        raise LeaseLost("runtime_lease_owner_mismatch")
    run = (
        connection.execute(
            text(
                "SELECT status, deadline_at FROM runtime_private.runs "
                "WHERE owner_id=:owner_id AND run_id=:run_id FOR UPDATE"
            ),
            {"owner_id": lease.owner_id, "run_id": lease.run_id},
        )
        .mappings()
        .one_or_none()
    )
    if run is None:
        raise LeaseLost("runtime_run_not_found")
    step = (
        connection.execute(
            text(
                "SELECT status FROM runtime_private.steps WHERE owner_id=:owner_id "
                "AND run_id=:run_id AND step_id=:step_id FOR UPDATE"
            ),
            {"owner_id": lease.owner_id, "run_id": lease.run_id, "step_id": lease.step_id},
        )
        .mappings()
        .one_or_none()
    )
    if step is None:
        raise LeaseLost("runtime_step_not_found")
    job = (
        connection.execute(
            text(
                "SELECT status, generation, lease_owner, lease_expires_at "
                "FROM runtime_private.jobs "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id "
                "AND job_id=:job_id FOR UPDATE"
            ),
            {
                "owner_id": lease.owner_id,
                "run_id": lease.run_id,
                "step_id": lease.step_id,
                "job_id": lease.job_id,
            },
        )
        .mappings()
        .one_or_none()
    )
    if job is None:
        raise LeaseLost("runtime_job_not_found")
    attempt = (
        connection.execute(
            text(
                "SELECT status, lease_owner FROM runtime_private.attempts WHERE owner_id=:owner_id "
                "AND run_id=:run_id AND job_id=:job_id AND generation=:generation "
                "AND attempt_id=:attempt_id FOR UPDATE"
            ),
            {
                "owner_id": lease.owner_id,
                "run_id": lease.run_id,
                "job_id": lease.job_id,
                "generation": lease.generation,
                "attempt_id": lease.attempt_id,
            },
        )
        .mappings()
        .one_or_none()
    )
    if attempt is None:
        raise LeaseLost("runtime_attempt_not_found")
    locked = {
        "run_status": run["status"],
        "run_deadline_at": run["deadline_at"],
        "step_status": step["status"],
        "job_status": job["status"],
        "generation": job["generation"],
        "lease_owner": job["lease_owner"],
        "lease_expires_at": job["lease_expires_at"],
        "attempt_status": attempt["status"],
        "attempt_lease_owner": attempt["lease_owner"],
    }
    locked["database_now"] = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
    return locked


def _assert_dispatch_lease_is_current(locked: dict[str, Any], lease: ExecutionLease) -> None:
    if (
        locked["run_status"] != RunStatus.RUNNING.value
        or (
            locked["run_deadline_at"] is not None
            and locked["run_deadline_at"] <= locked["database_now"]
        )
        or locked["step_status"] != StepStatus.RUNNING.value
        or locked["job_status"] != "leased"
        or locked["generation"] != lease.generation
        or locked["lease_owner"] != lease.lease_owner
        or locked["lease_expires_at"] is None
        or locked["lease_expires_at"] <= locked["database_now"]
        or locked["attempt_status"] != "running"
        or locked["attempt_lease_owner"] != lease.lease_owner
    ):
        raise LeaseLost("runtime_dispatch_lease_lost")


def _next_event_sequence(connection: Connection, owner_id: UUID, run_id: UUID) -> int:
    return connection.execute(
        text(
            "SELECT coalesce(max(sequence), 0) + 1 FROM runtime_private.events "
            "WHERE owner_id=:owner_id AND run_id=:run_id"
        ),
        {"owner_id": owner_id, "run_id": run_id},
    ).scalar_one()


def _lock_terminalization_rows(connection: Connection, owner_id: UUID, run_id: UUID) -> datetime:
    """Lock run children in the runtime order, then read PostgreSQL time."""

    connection.execute(
        text(
            "SELECT step_id FROM runtime_private.steps "
            "WHERE owner_id=:owner_id AND run_id=:run_id "
            "ORDER BY step_id FOR UPDATE"
        ),
        {"owner_id": owner_id, "run_id": run_id},
    ).all()
    connection.execute(
        text(
            "SELECT job_id FROM runtime_private.jobs "
            "WHERE owner_id=:owner_id AND run_id=:run_id "
            "ORDER BY job_id FOR UPDATE"
        ),
        {"owner_id": owner_id, "run_id": run_id},
    ).all()
    connection.execute(
        text(
            "SELECT attempt_id FROM runtime_private.attempts "
            "WHERE owner_id=:owner_id AND run_id=:run_id "
            "ORDER BY job_id, generation, attempt_id FOR UPDATE"
        ),
        {"owner_id": owner_id, "run_id": run_id},
    ).all()
    # The resource ledger is read-only here; future allocators share the run
    # lock. Dispatch rows can receive observations, so lock them after attempts.
    connection.execute(
        text(
            "SELECT reservation_id FROM runtime_private.resource_reservations "
            "WHERE owner_id=:owner_id AND run_id=:run_id ORDER BY reservation_id"
        ),
        {"owner_id": owner_id, "run_id": run_id},
    ).all()
    connection.execute(
        text(
            "SELECT dispatch_id FROM runtime_private.dispatch_intents "
            "WHERE owner_id=:owner_id AND run_id=:run_id "
            "ORDER BY dispatch_id FOR UPDATE"
        ),
        {"owner_id": owner_id, "run_id": run_id},
    ).all()
    return connection.execute(text("SELECT clock_timestamp()")).scalar_one()


def _terminalize_locked_run(
    connection: Connection,
    repository: RuntimeRepository,
    *,
    owner_id: UUID,
    run_id: UUID,
    target_status: RunStatus,
    event_type: str,
    actor_type: EventActor,
    actor_id: str | None,
    attempt_failure_class: str | None,
    deadline_at: datetime | None = None,
    require_elapsed_deadline: bool = False,
) -> CancellationReceipt | None:
    """Commit the shared cancellation/expiry terminal state under a run lock."""

    database_now = _lock_terminalization_rows(connection, owner_id, run_id)
    if require_elapsed_deadline and (deadline_at is None or deadline_at > database_now):
        return None

    jobs = connection.execute(
        text(
            "UPDATE runtime_private.jobs SET status='cancelled', "
            "generation=generation + 1, lease_owner=NULL, lease_expires_at=NULL "
            "WHERE owner_id=:owner_id AND run_id=:run_id "
            "AND status IN ('queued', 'leased')"
        ),
        {"owner_id": owner_id, "run_id": run_id},
    )
    connection.execute(
        text(
            "UPDATE runtime_private.steps SET status='cancelled' "
            "WHERE owner_id=:owner_id AND run_id=:run_id "
            "AND status IN ('queued', 'running')"
        ),
        {"owner_id": owner_id, "run_id": run_id},
    )
    attempts = connection.execute(
        text(
            "UPDATE runtime_private.attempts SET status='unknown', "
            "finished_at=:database_now, failure_class=:failure_class "
            "WHERE owner_id=:owner_id AND run_id=:run_id AND status='running'"
        ),
        {
            "database_now": database_now,
            "failure_class": attempt_failure_class,
            "owner_id": owner_id,
            "run_id": run_id,
        },
    )
    connection.execute(
        text(
            "UPDATE runtime_private.runs SET status=:status "
            "WHERE owner_id=:owner_id AND run_id=:run_id"
        ),
        {"status": target_status.value, "owner_id": owner_id, "run_id": run_id},
    )

    sequence = _next_event_sequence(connection, owner_id, run_id)
    repository.append_event(
        run_id,
        sequence,
        event_type,
        actor_type,
        actor_id=actor_id,
        payload={
            "cancelled_jobs": jobs.rowcount,
            "invalidated_attempts": attempts.rowcount,
        },
    )
    for cleanup_kind in ("stop_execution", "observe_effects"):
        connection.execute(
            text(
                "INSERT INTO runtime_private.cleanup_intents "
                "(cleanup_id, owner_id, run_id, cleanup_kind) "
                "VALUES (:cleanup_id, :owner_id, :run_id, :cleanup_kind)"
            ),
            {
                "cleanup_id": uuid4(),
                "owner_id": owner_id,
                "run_id": run_id,
                "cleanup_kind": cleanup_kind,
            },
        )
    return CancellationReceipt(
        run_id=run_id,
        run_status=target_status,
        event_sequence=sequence,
        adopted=False,
        cancelled_jobs=jobs.rowcount,
        invalidated_attempts=attempts.rowcount,
    )


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )
