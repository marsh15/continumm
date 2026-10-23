"""Narrow synchronous adapter for the bounded, database-fenced wake worker."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from sqlalchemy import Engine, text


@dataclass(frozen=True, slots=True)
class DrainLease:
    wake_id: UUID
    token: UUID
    generation: int


@dataclass(frozen=True, slots=True)
class InboxWork:
    delivery_id: UUID
    event_name: str
    payload: dict[str, Any]
    owner_id: UUID | None
    github_app_id: int | None = None
    command_state: Literal["none", "expired", "accepted_unresolved", "completed"] = "none"


@dataclass(frozen=True, slots=True)
class RunWork:
    owner_id: UUID
    run_id: UUID


@dataclass(frozen=True, slots=True)
class StatusWork:
    owner_id: UUID
    run_id: UUID
    event_sequence: int
    snapshot_key: bytes
    reconcile_only: bool


class PostgresWakeWorkerStore:
    """Calls SECURITY DEFINER functions using the dedicated wake-worker login."""

    def __init__(self, engine: Engine, *, github_app_id: int) -> None:
        if github_app_id <= 0:
            raise ValueError("wake_worker_github_app_id_invalid")
        self._engine = engine
        self._github_app_id = github_app_id

    def close(self) -> None:
        self._engine.dispose()

    def claim(self, wake_id: UUID, attempt_id: UUID, generation: int) -> DrainLease | None:
        with self._engine.begin() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT * FROM wake_private.worker_claim("
                        "CAST(:wake_id AS uuid), CAST(:attempt_id AS uuid), "
                        "CAST(:dispatch_generation AS bigint), CAST(:github_app_id AS bigint))"
                    ),
                    {
                        "wake_id": wake_id,
                        "attempt_id": attempt_id,
                        "dispatch_generation": generation,
                        "github_app_id": self._github_app_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
        if row is None or row["token"] is None:
            return None
        return DrainLease(row["wake_id"], row["token"], row["generation"])

    def heartbeat(self, lease: DrainLease) -> bool:
        with self._engine.begin() as connection:
            return bool(
                connection.execute(
                    text(
                        "SELECT wake_private.worker_heartbeat("
                        "CAST(:wake_id AS uuid), CAST(:token AS uuid), "
                        "CAST(:generation AS bigint))"
                    ),
                    _lease_values(lease),
                ).scalar_one()
            )

    def next_inbox(self, lease: DrainLease) -> InboxWork | None:
        with self._engine.begin() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT * FROM wake_private.worker_next_inbox("
                        "CAST(:wake_id AS uuid), CAST(:token AS uuid), "
                        "CAST(:generation AS bigint))"
                    ),
                    _lease_values(lease),
                )
                .mappings()
                .one_or_none()
            )
        if row is None or row["delivery_id"] is None:
            return None
        return InboxWork(
            delivery_id=row["delivery_id"],
            event_name=row["event_name"],
            payload=row["payload"],
            owner_id=row["owner_id"],
            github_app_id=row["github_app_id"],
            command_state=row["command_state"],
        )

    def complete_inbox(self, lease: DrainLease, delivery_id: UUID, outcome: str) -> bool:
        with self._engine.begin() as connection:
            return bool(
                connection.execute(
                    text(
                        "SELECT wake_private.worker_complete_inbox("
                        "CAST(:wake_id AS uuid), CAST(:token AS uuid), "
                        "CAST(:generation AS bigint), CAST(:delivery_id AS uuid), "
                        "CAST(:outcome AS text))"
                    ),
                    {**_lease_values(lease), "delivery_id": delivery_id, "outcome": outcome},
                ).scalar_one()
            )

    def next_run(self, lease: DrainLease) -> RunWork | None:
        with self._engine.begin() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT * FROM wake_private.worker_next_run("
                        "CAST(:wake_id AS uuid), CAST(:token AS uuid), "
                        "CAST(:generation AS bigint))"
                    ),
                    _lease_values(lease),
                )
                .mappings()
                .one_or_none()
            )
        if row is None or row["run_id"] is None:
            return None
        return RunWork(owner_id=row["owner_id"], run_id=row["run_id"])

    def next_status(self, lease: DrainLease) -> StatusWork | None:
        with self._engine.begin() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT * FROM wake_private.worker_next_status("
                        "CAST(:wake_id AS uuid), CAST(:token AS uuid), "
                        "CAST(:generation AS bigint))"
                    ),
                    _lease_values(lease),
                )
                .mappings()
                .one_or_none()
            )
        if row is None or row["run_id"] is None:
            return None
        return StatusWork(
            owner_id=row["owner_id"],
            run_id=row["run_id"],
            event_sequence=row["event_sequence"],
            snapshot_key=bytes(row["snapshot_key"]),
            reconcile_only=row["reconcile_only"],
        )

    def complete_status(self, lease: DrainLease, work: StatusWork) -> bool:
        with self._engine.begin() as connection:
            return bool(
                connection.execute(
                    text(
                        "SELECT wake_private.worker_complete_status("
                        "CAST(:wake_id AS uuid), CAST(:token AS uuid), "
                        "CAST(:generation AS bigint), CAST(:owner_id AS uuid), "
                        "CAST(:run_id AS uuid), CAST(:event_sequence AS bigint), "
                        "CAST(:snapshot_key AS bytea))"
                    ),
                    {
                        **_lease_values(lease),
                        "owner_id": work.owner_id,
                        "run_id": work.run_id,
                        "event_sequence": work.event_sequence,
                        "snapshot_key": work.snapshot_key,
                    },
                ).scalar_one()
            )

    def finish(self, lease: DrainLease, complete: bool) -> bool:
        with self._engine.begin() as connection:
            return bool(
                connection.execute(
                    text(
                        "SELECT wake_private.worker_finish("
                        "CAST(:wake_id AS uuid), CAST(:token AS uuid), "
                        "CAST(:generation AS bigint), CAST(:complete AS boolean))"
                    ),
                    {**_lease_values(lease), "complete": complete},
                ).scalar_one()
            )


def _lease_values(lease: DrainLease) -> dict[str, object]:
    return {"wake_id": lease.wake_id, "token": lease.token, "generation": lease.generation}
