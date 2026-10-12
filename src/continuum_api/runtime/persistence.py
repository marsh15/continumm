"""Principal-scoped PostgreSQL repositories for the durable runtime slice."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Connection, text


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    WAITING_CREDENTIALS = "waiting_credentials"
    WAITING_CAPACITY = "waiting_capacity"
    NEEDS_ATTENTION = "needs_attention"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class StepStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class JobStatus(StrEnum):
    QUEUED = "queued"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AttemptStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    RETRYABLE_FAILURE = "retryable_failure"
    DEFINITIVE_FAILURE = "definitive_failure"
    UNKNOWN = "unknown"
    PROTOCOL_FAILURE = "protocol_failure"


class ReplayPolicy(StrEnum):
    REPLAY_SAFE = "replay_safe"
    ATTENTION_ON_UNKNOWN = "attention_on_unknown"


class EventActor(StrEnum):
    ACCOUNT = "account"
    SYSTEM = "system"
    WORKER = "worker"
    PROVIDER = "provider"


@dataclass(frozen=True, slots=True)
class Principal:
    """Authenticated account identity supplied by the application boundary."""

    account_id: UUID


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: UUID
    owner_id: UUID
    github_repository_id: int | None
    issue_number: int | None
    status: RunStatus
    revision: int
    created_at: datetime
    deadline_at: datetime | None
    event_watermark: int


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    event_id: UUID
    run_id: UUID
    sequence: int
    event_type: str
    actor_type: EventActor
    actor_id: str | None
    payload_version: int
    payload: dict[str, Any]
    created_at: datetime


class RuntimeRepository:
    """SQL repository bound to one account for one caller-owned transaction.

    Construct repositories only after opening a transaction. The principal is
    fixed for that transaction, reflected in every owner predicate, and also
    installed in a transaction-local PostgreSQL setting used by forced RLS.
    """

    def __init__(self, connection: Connection, principal: Principal) -> None:
        if not connection.in_transaction():
            raise ValueError("runtime_repository_requires_transaction")
        self._connection = connection
        self._principal = principal

        configured_principal = connection.execute(
            text("SELECT NULLIF(current_setting('continuum.principal_id', true), '')")
        ).scalar_one_or_none()
        principal_id = str(principal.account_id)
        if configured_principal is not None and configured_principal != principal_id:
            raise ValueError("runtime_transaction_principal_mismatch")
        connection.execute(
            text("SELECT set_config('continuum.principal_id', :principal_id, true)"),
            {"principal_id": principal_id},
        )

    def create_run(
        self,
        *,
        run_id: UUID | None = None,
        github_repository_id: int | None = None,
        issue_number: int | None = None,
        deadline_at: datetime | None = None,
    ) -> UUID:
        selected_run_id = run_id or uuid4()
        self._connection.execute(
            text(
                """
                INSERT INTO runtime_private.runs (
                    run_id, owner_id, github_repository_id, issue_number, deadline_at
                ) VALUES (
                    :run_id, :owner_id, :github_repository_id, :issue_number, :deadline_at
                )
                """
            ),
            {
                "run_id": selected_run_id,
                "owner_id": self._principal.account_id,
                "github_repository_id": github_repository_id,
                "issue_number": issue_number,
                "deadline_at": deadline_at,
            },
        )
        return selected_run_id

    def get_run(self, run_id: UUID) -> RunRecord | None:
        row = (
            self._connection.execute(
                text(
                    """
                SELECT run_id, owner_id, github_repository_id, issue_number,
                       status, revision, created_at, deadline_at,
                       coalesce((
                           SELECT max(event.sequence)
                           FROM runtime_private.events AS event
                           WHERE event.owner_id = run.owner_id
                             AND event.run_id = run.run_id
                       ), 0) AS event_watermark
                FROM runtime_private.runs AS run
                WHERE run.owner_id = :owner_id AND run.run_id = :run_id
                """
                ),
                {"owner_id": self._principal.account_id, "run_id": run_id},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        return RunRecord(
            run_id=row["run_id"],
            owner_id=row["owner_id"],
            github_repository_id=row["github_repository_id"],
            issue_number=row["issue_number"],
            status=RunStatus(row["status"]),
            revision=row["revision"],
            created_at=row["created_at"],
            deadline_at=row["deadline_at"],
            event_watermark=row["event_watermark"],
        )

    def create_step(
        self,
        run_id: UUID,
        logical_key: str,
        activity: str,
        *,
        step_id: UUID | None = None,
        revision: int = 0,
    ) -> UUID:
        selected_step_id = step_id or uuid4()
        self._connection.execute(
            text(
                """
                INSERT INTO runtime_private.steps (
                    step_id, owner_id, run_id, logical_key, activity, revision
                ) VALUES (
                    :step_id, :owner_id, :run_id, :logical_key, :activity, :revision
                )
                """
            ),
            {
                "step_id": selected_step_id,
                "owner_id": self._principal.account_id,
                "run_id": run_id,
                "logical_key": logical_key,
                "activity": activity,
                "revision": revision,
            },
        )
        return selected_step_id

    def create_job(
        self,
        run_id: UUID,
        step_id: UUID,
        *,
        job_id: UUID | None = None,
        available_at: datetime | None = None,
        replay_policy: ReplayPolicy = ReplayPolicy.ATTENTION_ON_UNKNOWN,
    ) -> UUID:
        selected_job_id = job_id or uuid4()
        if replay_policy is ReplayPolicy.REPLAY_SAFE:
            self._connection.execute(
                text(
                    "INSERT INTO runtime_private.jobs "
                    "(job_id, owner_id, run_id, step_id, available_at, replay_policy) "
                    "VALUES (:job_id, :owner_id, :run_id, :step_id, "
                    "coalesce(:available_at, clock_timestamp()), :replay_policy)"
                ),
                {
                    "job_id": selected_job_id,
                    "owner_id": self._principal.account_id,
                    "run_id": run_id,
                    "step_id": step_id,
                    "available_at": available_at,
                    "replay_policy": replay_policy.value,
                },
            )
            return selected_job_id
        self._connection.execute(
            text(
                """
                INSERT INTO runtime_private.jobs (
                    job_id, owner_id, run_id, step_id, available_at
                ) VALUES (
                    :job_id, :owner_id, :run_id, :step_id,
                    coalesce(:available_at, clock_timestamp())
                )
                """
            ),
            {
                "job_id": selected_job_id,
                "owner_id": self._principal.account_id,
                "run_id": run_id,
                "step_id": step_id,
                "available_at": available_at,
            },
        )
        return selected_job_id

    def create_attempt(
        self,
        run_id: UUID,
        job_id: UUID,
        generation: int,
        *,
        attempt_id: UUID | None = None,
    ) -> UUID:
        selected_attempt_id = attempt_id or uuid4()
        self._connection.execute(
            text(
                """
                INSERT INTO runtime_private.attempts (
                    attempt_id, owner_id, run_id, job_id, generation
                ) VALUES (
                    :attempt_id, :owner_id, :run_id, :job_id, :generation
                )
                """
            ),
            {
                "attempt_id": selected_attempt_id,
                "owner_id": self._principal.account_id,
                "run_id": run_id,
                "job_id": job_id,
                "generation": generation,
            },
        )
        return selected_attempt_id

    def append_event(
        self,
        run_id: UUID,
        sequence: int,
        event_type: str,
        actor_type: EventActor,
        *,
        actor_id: str | None = None,
        payload_version: int = 1,
        payload: dict[str, Any] | None = None,
        event_id: UUID | None = None,
    ) -> UUID:
        run_row = self._connection.execute(
            text(
                """
                SELECT run_id
                FROM runtime_private.runs
                WHERE owner_id = :owner_id AND run_id = :run_id
                FOR UPDATE
                """
            ),
            {"owner_id": self._principal.account_id, "run_id": run_id},
        ).one_or_none()
        if run_row is None:
            raise ValueError("runtime_run_not_found")
        selected_event_id = event_id or uuid4()
        serialized_payload = json.dumps(
            payload or {}, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        )
        self._connection.execute(
            text(
                """
                INSERT INTO runtime_private.events (
                    event_id, owner_id, run_id, sequence, event_type, actor_type,
                    actor_id, payload_version, payload
                ) VALUES (
                    :event_id, :owner_id, :run_id, :sequence, :event_type, :actor_type,
                    :actor_id, :payload_version, CAST(:payload AS jsonb)
                )
                """
            ),
            {
                "event_id": selected_event_id,
                "owner_id": self._principal.account_id,
                "run_id": run_id,
                "sequence": sequence,
                "event_type": event_type,
                "actor_type": actor_type.value,
                "actor_id": actor_id,
                "payload_version": payload_version,
                "payload": serialized_payload,
            },
        )
        return selected_event_id

    def list_events(
        self,
        run_id: UUID,
        *,
        after_sequence: int = 0,
        limit: int = 100,
    ) -> tuple[RuntimeEvent, ...]:
        if after_sequence < 0 or not 1 <= limit <= 101:
            raise ValueError("runtime_event_cursor_invalid")
        rows = self._connection.execute(
            text(
                """
                SELECT event_id, run_id, sequence, event_type, actor_type, actor_id,
                       payload_version, payload, created_at
                FROM runtime_private.events
                WHERE owner_id = :owner_id AND run_id = :run_id
                  AND sequence > :after_sequence
                ORDER BY sequence
                LIMIT :limit
                """
            ),
            {
                "owner_id": self._principal.account_id,
                "run_id": run_id,
                "after_sequence": after_sequence,
                "limit": limit,
            },
        ).mappings()
        return tuple(
            RuntimeEvent(
                event_id=row["event_id"],
                run_id=row["run_id"],
                sequence=row["sequence"],
                event_type=row["event_type"],
                actor_type=EventActor(row["actor_type"]),
                actor_id=row["actor_id"],
                payload_version=row["payload_version"],
                payload=row["payload"],
                created_at=row["created_at"],
            )
            for row in rows
        )
