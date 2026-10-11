"""Atomic, replayable runtime activity transitions."""

from __future__ import annotations

import hashlib
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError

from continuum_api.runtime.leases import ExecutionLease, LeaseLost, lock_current_execution
from continuum_api.runtime.persistence import (
    EventActor,
    Principal,
    RunStatus,
    RuntimeRepository,
)

MAX_RESULT_BYTES = 65_536
MAX_TRANSACTION_ATTEMPTS = 3
RETRYABLE_SQLSTATES = frozenset({"40001", "40P01"})
_WAIT_OR_TERMINAL_RUN_STATES = frozenset(
    {
        RunStatus.WAITING_APPROVAL,
        RunStatus.WAITING_CREDENTIALS,
        RunStatus.WAITING_CAPACITY,
        RunStatus.NEEDS_ATTENTION,
        RunStatus.FAILED,
    }
)


class TransitionError(Exception):
    """Base class for rejected runtime transitions."""


class RuntimeRecordNotFound(TransitionError):
    """The principal cannot access one of the transition records."""


class InvalidTransition(TransitionError):
    """The locked runtime state cannot accept the requested transition."""


class ResultIdentityConflict(TransitionError):
    """A logical step already accepted a different transition intent."""


@dataclass(frozen=True, slots=True)
class NextWork:
    logical_key: str
    activity: str
    revision: int = 0


@dataclass(frozen=True, slots=True)
class TransitionReceipt:
    result_id: UUID
    run_id: UUID
    step_id: UUID
    job_id: UUID
    result_sha256: bytes
    result: dict[str, Any]
    run_status: RunStatus
    event_sequence: int
    next_step_id: UUID | None
    next_job_id: UUID | None
    adopted: bool


class TransitionService:
    """Commit one activity result and its state advancement as one transaction.

    The lease identity is mandatory. Exact committed receipt adoption is
    read-only and can succeed after lease expiry; every new durable write
    validates the current generation and database-time lease after row locks.
    No remote operation runs inside this database retry loop.
    """

    def __init__(self, engine: Engine, principal: Principal) -> None:
        self._engine = engine
        self._principal = principal

    def commit_result(
        self,
        *,
        lease: ExecutionLease,
        result: dict[str, Any],
        target_run_status: RunStatus,
        next_work: NextWork | None = None,
    ) -> TransitionReceipt:
        self._validate_plan(target_run_status, next_work)
        result_json = _canonical_json(result)
        if len(result_json.encode("utf-8")) > MAX_RESULT_BYTES:
            raise InvalidTransition("accepted_result_too_large")
        result_digest = hashlib.sha256(result_json.encode("utf-8")).digest()
        intent_json = _canonical_json(
            {
                "next_work": None
                if next_work is None
                else {
                    "activity": next_work.activity,
                    "logical_key": next_work.logical_key,
                    "revision": next_work.revision,
                },
                "result": result,
                "target_run_status": target_run_status.value,
            }
        )
        intent_digest = hashlib.sha256(intent_json.encode("utf-8")).digest()

        for attempt in range(MAX_TRANSACTION_ATTEMPTS):
            try:
                return self._commit_once(
                    lease=lease,
                    result_json=result_json,
                    result_digest=result_digest,
                    intent_digest=intent_digest,
                    target_run_status=target_run_status,
                    next_work=next_work,
                )
            except DBAPIError as error:
                if (
                    attempt + 1 == MAX_TRANSACTION_ATTEMPTS
                    or _sqlstate(error) not in RETRYABLE_SQLSTATES
                ):
                    raise
                time.sleep(random.uniform(0, 0.01 * (2**attempt)))
        raise AssertionError("transition_retry_loop_exhausted")

    def _commit_once(
        self,
        *,
        lease: ExecutionLease,
        result_json: str,
        result_digest: bytes,
        intent_digest: bytes,
        target_run_status: RunStatus,
        next_work: NextWork | None,
    ) -> TransitionReceipt:
        with self._engine.begin() as connection:
            repository = RuntimeRepository(connection, self._principal)
            owner_id = self._principal.account_id
            run_id = lease.run_id
            step_id = lease.step_id
            job_id = lease.job_id
            run = connection.execute(
                text(
                    """
                    SELECT status
                    FROM runtime_private.runs
                    WHERE owner_id=:owner_id AND run_id=:run_id
                    FOR UPDATE
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ).one_or_none()
            if run is None:
                raise RuntimeRecordNotFound("runtime_run_not_found")

            step = connection.execute(
                text(
                    """
                    SELECT status
                    FROM runtime_private.steps
                    WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id
                    FOR UPDATE
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id, "step_id": step_id},
            ).one_or_none()
            if step is None:
                raise RuntimeRecordNotFound("runtime_step_not_found")

            job = connection.execute(
                text(
                    """
                    SELECT status, lease_owner
                    FROM runtime_private.jobs
                    WHERE owner_id=:owner_id AND run_id=:run_id
                      AND step_id=:step_id AND job_id=:job_id
                    FOR UPDATE
                    """
                ),
                {
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "step_id": step_id,
                    "job_id": job_id,
                },
            ).one_or_none()
            if job is None:
                raise RuntimeRecordNotFound("runtime_job_not_found")

            existing = (
                connection.execute(
                    text(
                        """
                    SELECT result_id, result_sha256, intent_sha256, payload,
                           run_status, event_sequence, next_job_id,
                           next_step.step_id AS next_step_id
                    FROM runtime_private.accepted_results AS accepted
                    LEFT JOIN runtime_private.jobs AS next_step
                      ON next_step.owner_id=accepted.owner_id
                     AND next_step.run_id=accepted.run_id
                     AND next_step.job_id=accepted.next_job_id
                    WHERE accepted.owner_id=:owner_id AND accepted.run_id=:run_id
                      AND accepted.step_id=:step_id AND accepted.job_id=:job_id
                    """
                    ),
                    {"owner_id": owner_id, "run_id": run_id, "step_id": step_id, "job_id": job_id},
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if (
                    existing["result_sha256"] != result_digest
                    or existing["intent_sha256"] != intent_digest
                ):
                    raise ResultIdentityConflict("accepted_result_intent_conflict")
                return _receipt_from_row(
                    existing,
                    run_id=run_id,
                    step_id=step_id,
                    job_id=job_id,
                    adopted=True,
                )

            # Re-locking held rows is safe; this also locks the exact attempt
            # and obtains database time only after run, step, job, and attempt.
            locked = lock_current_execution(connection, self._principal, lease)
            if locked.run_status != RunStatus.RUNNING.value:
                raise InvalidTransition("runtime_run_not_running")

            connection.execute(
                text(
                    """
                    UPDATE runtime_private.steps SET status='succeeded'
                    WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id, "step_id": step_id},
            )
            connection.execute(
                text(
                    """
                    UPDATE runtime_private.jobs
                    SET status='succeeded', lease_owner=NULL, lease_expires_at=NULL
                    WHERE owner_id=:owner_id AND run_id=:run_id
                      AND step_id=:step_id AND job_id=:job_id
                    """
                ),
                {
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "step_id": step_id,
                    "job_id": job_id,
                },
            )
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET status=:status "
                    "WHERE owner_id=:owner_id AND run_id=:run_id"
                ),
                {
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "status": target_run_status.value,
                },
            )
            attempt_update = connection.execute(
                text(
                    """
                    UPDATE runtime_private.attempts
                    SET status='succeeded', finished_at=:finished_at
                    WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id
                      AND generation=:generation AND attempt_id=:attempt_id
                      AND lease_owner=:lease_owner AND status='running'
                    """
                ),
                {
                    "finished_at": locked.database_now,
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "job_id": job_id,
                    "generation": lease.generation,
                    "attempt_id": lease.attempt_id,
                    "lease_owner": lease.lease_owner,
                },
            )
            if attempt_update.rowcount != 1:
                raise LeaseLost("runtime_attempt_not_current")

            next_step_id: UUID | None = None
            next_job_id: UUID | None = None
            if next_work is not None:
                next_step_id = repository.create_step(
                    run_id,
                    next_work.logical_key,
                    next_work.activity,
                    revision=next_work.revision,
                )
                next_job_id = repository.create_job(run_id, next_step_id)

            event_sequence: int = connection.execute(
                text(
                    """
                    SELECT coalesce(max(sequence), 0) + 1
                    FROM runtime_private.events
                    WHERE owner_id=:owner_id AND run_id=:run_id
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ).scalar_one()
            result_id = uuid4()
            repository.append_event(
                run_id,
                event_sequence,
                "activity.result_accepted",
                EventActor.WORKER,
                actor_id=lease.lease_owner,
                payload={
                    "next_job_id": None if next_job_id is None else str(next_job_id),
                    "result_id": str(result_id),
                    "result_sha256": result_digest.hex(),
                    "step_id": str(step_id),
                    "target_run_status": target_run_status.value,
                },
            )
            connection.execute(
                text(
                    """
                    INSERT INTO runtime_private.accepted_results (
                        result_id, owner_id, run_id, step_id, job_id,
                        result_sha256, intent_sha256, run_status, event_sequence,
                        next_job_id, payload, generation, attempt_id, lease_owner
                    ) VALUES (
                        :result_id, :owner_id, :run_id, :step_id, :job_id,
                        :result_sha256, :intent_sha256, :run_status, :event_sequence,
                        :next_job_id, CAST(:payload AS jsonb), :generation, :attempt_id,
                        :lease_owner
                    )
                    """
                ),
                {
                    "result_id": result_id,
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "step_id": step_id,
                    "job_id": job_id,
                    "result_sha256": result_digest,
                    "intent_sha256": intent_digest,
                    "run_status": target_run_status.value,
                    "event_sequence": event_sequence,
                    "next_job_id": next_job_id,
                    "payload": result_json,
                    "generation": lease.generation,
                    "attempt_id": lease.attempt_id,
                    "lease_owner": lease.lease_owner,
                },
            )
            final_database_now: datetime = connection.execute(
                text("SELECT clock_timestamp()")
            ).scalar_one()
            if locked.lease_expires_at is None or locked.lease_expires_at <= final_database_now:
                raise LeaseLost("runtime_lease_expired_before_commit")
            if locked.run_deadline_at is not None and locked.run_deadline_at <= final_database_now:
                raise LeaseLost("runtime_run_expired_before_commit")
            return TransitionReceipt(
                result_id=result_id,
                run_id=run_id,
                step_id=step_id,
                job_id=job_id,
                result_sha256=result_digest,
                result=json.loads(result_json),
                run_status=target_run_status,
                event_sequence=event_sequence,
                next_step_id=next_step_id,
                next_job_id=next_job_id,
                adopted=False,
            )

    @staticmethod
    def _validate_plan(target_run_status: RunStatus, next_work: NextWork | None) -> None:
        if target_run_status in {RunStatus.SUCCEEDED, RunStatus.CANCELLED, RunStatus.EXPIRED}:
            raise InvalidTransition("activity_result_cannot_set_terminal_run_status")
        if next_work is not None:
            if target_run_status is not RunStatus.RUNNING:
                raise InvalidTransition("next_work_requires_running_status")
            return
        if target_run_status not in _WAIT_OR_TERMINAL_RUN_STATES:
            raise InvalidTransition("transition_requires_next_work_or_wait_or_terminal_status")


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )


def _receipt_from_row(
    row: Any, *, run_id: UUID, step_id: UUID, job_id: UUID, adopted: bool
) -> TransitionReceipt:
    next_job_id = row["next_job_id"]
    return TransitionReceipt(
        result_id=row["result_id"],
        run_id=run_id,
        step_id=step_id,
        job_id=job_id,
        result_sha256=row["result_sha256"],
        result=row["payload"],
        run_status=RunStatus(row["run_status"]),
        event_sequence=row["event_sequence"],
        next_step_id=row["next_step_id"],
        next_job_id=next_job_id,
        adopted=adopted,
    )


def _sqlstate(error: DBAPIError) -> str | None:
    original = error.orig
    state = getattr(original, "sqlstate", None)
    if state is None:
        state = getattr(original, "pgcode", None)
    return state
