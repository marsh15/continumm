"""Immediate content access revocation and bounded, idempotent cleanup."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.runtime.cancellation import _terminalize_locked_run
from continuum_api.runtime.persistence import EventActor, Principal, RunStatus, RuntimeRepository

MAX_CLEANUP_BATCH = 100
_ACTIVE = (
    "queued",
    "running",
    "waiting_approval",
    "waiting_credentials",
    "waiting_capacity",
    "needs_attention",
)


class RetentionError(RuntimeError):
    """A sanitized retention failure."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class DeleteOutcome(StrEnum):
    DELETED = "deleted"
    UNSUPPORTED = "unsupported"
    UNKNOWN = "unknown"


class DeletionAdapter(Protocol):
    """Idempotently erase one opaque remote object reference."""

    def delete(self, sink: str, reference_id: str, provider: str | None) -> DeleteOutcome: ...


@dataclass(frozen=True, slots=True)
class DeletionReceipt:
    scope: str
    subject_id: UUID
    accepted: bool
    state: str
    cleanup_pending: int


@dataclass(frozen=True, slots=True)
class CleanupSummary:
    attempted: int
    deleted: int
    unsupported: int
    unknown: int
    pending: int


@dataclass(frozen=True, slots=True)
class RetentionDrainSummary:
    owners: int
    expired_runs: int
    scrubbed_runs: int
    webhook_bodies_scrubbed: int
    cleanup: CleanupSummary


@dataclass(frozen=True, slots=True)
class _CleanupItem:
    cleanup_id: UUID
    sink: str
    reference_id: str
    provider: str | None


class RetentionService:
    """Owner-scoped retention controls; remote deletion is always explicit."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        *,
        deletion_adapter: DeletionAdapter | None = None,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._deletion_adapter = deletion_adapter

    @classmethod
    def drain_due(
        cls,
        engine: Engine,
        *,
        deletion_adapter: DeletionAdapter | None = None,
        limit: int = MAX_CLEANUP_BATCH,
    ) -> RetentionDrainSummary:
        """Drain at most one run and one remote reference per due owner."""
        if not 1 <= limit <= MAX_CLEANUP_BATCH:
            raise ValueError("retention_batch_limit_invalid")
        try:
            with engine.begin() as connection:
                owners = cast(
                    list[UUID],
                    connection.execute(
                        text("SELECT owner_id FROM runtime_private.retention_due_owners(:limit)"),
                        {"limit": limit},
                    )
                    .scalars()
                    .all(),
                )
                webhook_bodies_scrubbed: int = connection.execute(
                    text("SELECT runtime_private.retention_scrub_webhook_bodies(:limit)"),
                    {"limit": limit},
                ).scalar_one()
            expired_runs = 0
            scrubbed_runs = 0
            deleted = unsupported = unknown = pending = 0
            for owner in owners:
                service = cls(engine, Principal(owner), deletion_adapter=deletion_adapter)
                expired_runs += service.expire_due(limit=1)
                scrubbed_runs += service.sweep_account_deletion(limit=1)
                cleanup = service.cleanup_due(limit=1)
                deleted += cleanup.deleted
                unsupported += cleanup.unsupported
                unknown += cleanup.unknown
                pending = max(pending, cleanup.pending)
                with engine.begin() as connection:
                    RuntimeRepository(connection, Principal(owner))
                    connection.execute(
                        text("SELECT runtime_private.retention_finalize_account_deletion(:owner)"),
                        {"owner": owner},
                    )
            return RetentionDrainSummary(
                len(owners),
                expired_runs,
                scrubbed_runs,
                webhook_bodies_scrubbed,
                CleanupSummary(
                    deleted + unsupported + unknown, deleted, unsupported, unknown, pending
                ),
            )
        except SQLAlchemyError:
            raise RetentionError("retention_storage_unavailable") from None

    def delete_run(self, run_id: UUID) -> DeletionReceipt:
        try:
            with self._engine.begin() as connection:
                repository = RuntimeRepository(connection, self._principal)
                owner = self._principal.account_id
                run = (
                    connection.execute(
                        text(
                            "SELECT status, content_deleted_at, content_expires_at "
                            "FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run "
                            "FOR UPDATE"
                        ),
                        {"owner": owner, "run": run_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if run is None:
                    raise RetentionError("run_not_found")
                self._record_receipt(connection, owner, "run", run_id, "delete")
                connection.execute(
                    text(
                        "UPDATE runtime_private.runs SET content_deleted_at="
                        "COALESCE(content_deleted_at, clock_timestamp()) "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": owner, "run": run_id},
                )
                if run["status"] in _ACTIVE:
                    _terminalize_locked_run(
                        connection,
                        repository,
                        owner_id=owner,
                        run_id=run_id,
                        target_status=RunStatus.CANCELLED,
                        event_type="run.cancelled",
                        actor_type=EventActor.ACCOUNT,
                        actor_id=str(owner),
                        attempt_failure_class="cancelled",
                    )
                self._queue_external_cleanup(connection, owner, "run", run_id)
                self._scrub_run_content(connection, owner, run_id)
                counts = connection.execute(
                    text(
                        "SELECT count(*) FILTER "
                        "(WHERE status IN ('pending','unknown')) AS pending, "
                        "count(*) FILTER (WHERE status='unsupported') AS unsupported "
                        "FROM runtime_private.retention_cleanup_items "
                        "WHERE owner_id=:owner AND scope='run' AND subject_id=:run"
                    ),
                    {"owner": owner, "run": run_id},
                ).one()
                pending = counts.pending + counts.unsupported
                return DeletionReceipt(
                    "run",
                    run_id,
                    True,
                    "needs_provider_action"
                    if counts.unsupported
                    else "pending"
                    if pending
                    else "complete",
                    pending,
                )
        except RetentionError:
            raise
        except SQLAlchemyError:
            raise RetentionError("retention_storage_unavailable") from None

    def delete_account(self) -> DeletionReceipt:
        """Revoke sessions and keys before scheduling bounded content erasure."""
        owner = self._principal.account_id
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                connection.execute(
                    text("SELECT runtime_private.retention_begin_account_deletion(:owner)"),
                    {"owner": owner},
                )
                self._record_receipt(connection, owner, "account", owner, "delete")
                connection.execute(
                    text(
                        "UPDATE runtime_private.provider_credentials SET status='deleted', "
                        "nonce=NULL, ciphertext=NULL, "
                        "ended_at=COALESCE(ended_at, clock_timestamp()) "
                        "WHERE owner_id=:owner AND status='active'"
                    ),
                    {"owner": owner},
                )
                connection.execute(
                    text(
                        "UPDATE runtime_private.runs SET content_deleted_at="
                        "COALESCE(content_deleted_at, clock_timestamp()) "
                        "WHERE owner_id=:owner"
                    ),
                    {"owner": owner},
                )
                runs_pending: int = connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.runs WHERE owner_id=:owner "
                        "AND content_scrubbed_at IS NULL"
                    ),
                    {"owner": owner},
                ).scalar_one()
                cleanup_pending: int = connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.retention_cleanup_items "
                        "WHERE owner_id=:owner AND status IN "
                        "('pending','unknown','unsupported')"
                    ),
                    {"owner": owner},
                ).scalar_one()
                if not runs_pending and not cleanup_pending:
                    connection.execute(
                        text("SELECT runtime_private.retention_finalize_account_deletion(:owner)"),
                        {"owner": owner},
                    )
                return DeletionReceipt(
                    "account",
                    owner,
                    True,
                    "pending" if runs_pending or cleanup_pending else "complete",
                    runs_pending + cleanup_pending,
                )
        except RetentionError:
            raise
        except SQLAlchemyError:
            raise RetentionError("retention_storage_unavailable") from None

    def expire_due(self, *, limit: int = MAX_CLEANUP_BATCH) -> int:
        """Revoke access and scrub at most ``limit`` expired runs."""
        if not 1 <= limit <= MAX_CLEANUP_BATCH:
            raise ValueError("retention_batch_limit_invalid")
        expired = 0
        try:
            for _ in range(limit):
                with self._engine.begin() as connection:
                    RuntimeRepository(connection, self._principal)
                    row = (
                        connection.execute(
                            text(
                                "SELECT run_id, status FROM runtime_private.runs "
                                "WHERE owner_id=:owner AND (content_expires_at<=clock_timestamp() "
                                "OR (status IN ('queued','running','waiting_approval',"
                                "'waiting_credentials','waiting_capacity','needs_attention') "
                                "AND deadline_at<=clock_timestamp())) "
                                "AND content_deleted_at IS NULL "
                                "ORDER BY content_expires_at, run_id "
                                "FOR UPDATE SKIP LOCKED LIMIT 1"
                            ),
                            {"owner": self._principal.account_id},
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if row is None:
                        break
                    run_id = row["run_id"]
                    self._record_receipt(
                        connection, self._principal.account_id, "run", run_id, "expire"
                    )
                    connection.execute(
                        text(
                            "UPDATE runtime_private.runs SET content_deleted_at=clock_timestamp() "
                            "WHERE owner_id=:owner AND run_id=:run"
                        ),
                        {"owner": self._principal.account_id, "run": run_id},
                    )
                    if row["status"] in _ACTIVE:
                        _terminalize_locked_run(
                            connection,
                            RuntimeRepository(connection, self._principal),
                            owner_id=self._principal.account_id,
                            run_id=run_id,
                            target_status=RunStatus.EXPIRED,
                            event_type="run.expired",
                            actor_type=EventActor.SYSTEM,
                            actor_id=None,
                            attempt_failure_class=None,
                        )
                    self._queue_external_cleanup(
                        connection, self._principal.account_id, "run", run_id
                    )
                    self._scrub_run_content(connection, self._principal.account_id, run_id)
                    expired += 1
            return expired
        except SQLAlchemyError:
            raise RetentionError("retention_storage_unavailable") from None

    def sweep_account_deletion(self, *, limit: int = MAX_CLEANUP_BATCH) -> int:
        """Scrub the next bounded page for an account already access-revoked."""
        if not 1 <= limit <= MAX_CLEANUP_BATCH:
            raise ValueError("retention_batch_limit_invalid")
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                runs = cast(
                    list[Mapping[str, Any]],
                    connection.execute(
                        text(
                            "SELECT run_id, status FROM runtime_private.runs WHERE owner_id=:owner "
                            "AND content_scrubbed_at IS NULL ORDER BY created_at, run_id "
                            "FOR UPDATE SKIP LOCKED LIMIT :limit"
                        ),
                        {"owner": self._principal.account_id, "limit": limit},
                    )
                    .mappings()
                    .all(),
                )
                for run in runs:
                    run_id = run["run_id"]
                    if run["status"] in _ACTIVE:
                        _terminalize_locked_run(
                            connection,
                            RuntimeRepository(connection, self._principal),
                            owner_id=self._principal.account_id,
                            run_id=run_id,
                            target_status=RunStatus.CANCELLED,
                            event_type="run.cancelled",
                            actor_type=EventActor.SYSTEM,
                            actor_id=None,
                            attempt_failure_class=None,
                        )
                    self._queue_external_cleanup(
                        connection,
                        self._principal.account_id,
                        "account",
                        self._principal.account_id,
                        run_id=run_id,
                    )
                    self._scrub_run_content(connection, self._principal.account_id, run_id)
                return len(runs)
        except SQLAlchemyError:
            raise RetentionError("retention_storage_unavailable") from None

    def cleanup_due(self, *, limit: int = MAX_CLEANUP_BATCH) -> CleanupSummary:
        """Attempt a bounded batch through an injected idempotent delete adapter."""
        if not 1 <= limit <= MAX_CLEANUP_BATCH:
            raise ValueError("retention_batch_limit_invalid")
        if self._deletion_adapter is None:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                pending: int = connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.retention_cleanup_items "
                        "WHERE owner_id=:owner AND status IN ('pending','unknown')"
                    ),
                    {"owner": self._principal.account_id},
                ).scalar_one()
            return CleanupSummary(0, 0, 0, 0, pending)

        rows = self._claim_cleanup(limit)
        counts = {state.value: 0 for state in DeleteOutcome}
        for row in rows:
            try:
                outcome = self._deletion_adapter.delete(row.sink, row.reference_id, row.provider)
                if not isinstance(outcome, DeleteOutcome):
                    outcome = DeleteOutcome.UNKNOWN
            except Exception:
                outcome = DeleteOutcome.UNKNOWN
            self._finish_cleanup(row.cleanup_id, outcome)
            counts[outcome.value] += 1
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            remaining_pending: int = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.retention_cleanup_items "
                    "WHERE owner_id=:owner AND status IN ('pending','unknown')"
                ),
                {"owner": self._principal.account_id},
            ).scalar_one()
        return CleanupSummary(
            len(rows),
            counts["deleted"],
            counts["unsupported"],
            counts["unknown"],
            remaining_pending,
        )

    def _claim_cleanup(self, limit: int) -> list[_CleanupItem]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            rows = (
                connection.execute(
                    text(
                        "SELECT cleanup_id, sink, reference_id, provider "
                        "FROM runtime_private.retention_cleanup_items "
                        "WHERE owner_id=:owner AND status IN ('pending','unknown') "
                        "AND next_attempt_at<=clock_timestamp() "
                        "ORDER BY next_attempt_at, cleanup_id "
                        "FOR UPDATE SKIP LOCKED LIMIT :limit"
                    ),
                    {"owner": self._principal.account_id, "limit": limit},
                )
                .mappings()
                .all()
            )
            # Claiming is a short DB-only lease. A process death makes the item
            # eligible again; every adapter call must therefore be idempotent.
            if rows:
                ids = [row["cleanup_id"] for row in rows]
                connection.execute(
                    text(
                        "UPDATE runtime_private.retention_cleanup_items SET "
                        "next_attempt_at=clock_timestamp()+interval '2 minutes', "
                        "attempts=attempts+1 WHERE owner_id=:owner AND cleanup_id=ANY(:ids)"
                    ),
                    {"owner": self._principal.account_id, "ids": ids},
                )
            return [
                _CleanupItem(
                    cleanup_id=row["cleanup_id"],
                    sink=row["sink"],
                    reference_id=row["reference_id"],
                    provider=row["provider"],
                )
                for row in rows
            ]

    def _finish_cleanup(self, cleanup_id: UUID, outcome: DeleteOutcome) -> None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            item = (
                connection.execute(
                    text(
                        "SELECT sink, reference_id FROM runtime_private.retention_cleanup_items "
                        "WHERE owner_id=:owner AND cleanup_id=:id FOR UPDATE"
                    ),
                    {"owner": self._principal.account_id, "id": cleanup_id},
                )
                .mappings()
                .one_or_none()
            )
            if item is None:
                return
            if item["sink"] == "object" and outcome is DeleteOutcome.DELETED:
                self._release_deleted_object(connection, item["reference_id"])
            connection.execute(
                text(
                    "UPDATE runtime_private.retention_cleanup_items SET status=:status, "
                    "last_error_code=:error, finished_at=CASE WHEN :is_finished "
                    "THEN clock_timestamp() ELSE NULL END, "
                    "next_attempt_at=CASE WHEN :is_unknown THEN "
                    "clock_timestamp()+interval '5 minutes' ELSE next_attempt_at END "
                    "WHERE owner_id=:owner AND cleanup_id=:id"
                ),
                {
                    "status": outcome.value,
                    "is_finished": outcome in {DeleteOutcome.DELETED, DeleteOutcome.UNSUPPORTED},
                    "is_unknown": outcome is DeleteOutcome.UNKNOWN,
                    "error": "remote_copy_unsupported"
                    if outcome is DeleteOutcome.UNSUPPORTED
                    else None,
                    "owner": self._principal.account_id,
                    "id": cleanup_id,
                },
            )

    def _release_deleted_object(self, connection: Connection, object_key: str) -> None:
        upload = (
            connection.execute(
                text(
                    "SELECT upload_id, compressed_bytes, status "
                    "FROM runtime_private.artifact_uploads WHERE owner_id=:owner "
                    "AND object_key=:key FOR UPDATE"
                ),
                {"owner": self._principal.account_id, "key": object_key},
            )
            .mappings()
            .one_or_none()
        )
        if upload is None or upload["status"] == "released":
            return
        capacity = connection.execute(
            text(
                "SELECT charged_bytes FROM runtime_private.artifact_capacity "
                "WHERE capacity_id=1 FOR UPDATE"
            )
        ).scalar_one_or_none()
        if capacity is None or capacity < upload["compressed_bytes"]:
            raise RetentionError("artifact_capacity_ledger_invalid")
        connection.execute(
            text(
                "UPDATE runtime_private.artifact_uploads SET status='released', "
                "released_at=clock_timestamp(), updated_at=clock_timestamp() "
                "WHERE owner_id=:owner AND upload_id=:upload AND status<>'released'"
            ),
            {"owner": self._principal.account_id, "upload": upload["upload_id"]},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.artifact_capacity "
                "SET charged_bytes=charged_bytes-:bytes WHERE capacity_id=1"
            ),
            {"bytes": upload["compressed_bytes"]},
        )

    @staticmethod
    def _record_receipt(
        connection: Connection, owner: UUID, scope: str, subject: UUID, action: str
    ) -> None:
        connection.execute(
            text(
                "INSERT INTO runtime_private.retention_receipts "
                "(receipt_id, owner_id, scope, subject_id, action) "
                "VALUES (:id,:owner,:scope,:subject,:action) "
                "ON CONFLICT (scope,owner_id,subject_id,action) DO NOTHING"
            ),
            {"id": uuid4(), "owner": owner, "scope": scope, "subject": subject, "action": action},
        )

    @staticmethod
    def _queue_external_cleanup(
        connection: Connection,
        owner: UUID,
        scope: str,
        subject: UUID,
        *,
        run_id: UUID | None = None,
    ) -> None:
        run_id = subject if run_id is None else run_id
        uploads = cast(
            list[str],
            connection.execute(
                text(
                    "SELECT object_key FROM runtime_private.artifact_uploads "
                    "WHERE owner_id=:owner AND run_id=:run AND status<>'released'"
                ),
                {"owner": owner, "run": run_id},
            )
            .scalars()
            .all(),
        )
        for object_key in uploads:
            RetentionService._enqueue(connection, owner, scope, subject, "object", object_key)
        allocations = connection.execute(
            text(
                "SELECT provider_name, provider_session_id "
                "FROM runtime_private.sandbox_allocations "
                "WHERE owner_id=:owner AND run_id=:run AND provider_session_id IS NOT NULL"
            ),
            {"owner": owner, "run": run_id},
        ).all()
        for provider, session_id in allocations:
            RetentionService._enqueue(
                connection, owner, scope, subject, "sandbox", f"{provider}:{session_id}"
            )
        provider = connection.execute(
            text(
                "SELECT provider FROM runtime_private.run_admission_pins "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        ).scalar_one_or_none()
        if provider in {"openai", "anthropic"}:
            response_ids = cast(
                list[str],
                connection.execute(
                    text(
                        "SELECT response->>'id' FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner AND run_id=:run "
                        "AND dispatch_authorized_at IS NOT NULL "
                        "AND response->>'id' IS NOT NULL"
                    ),
                    {"owner": owner, "run": run_id},
                )
                .scalars()
                .all(),
            )
            for remote_id in response_ids:
                if isinstance(remote_id, str) and 1 <= len(remote_id) <= 256:
                    RetentionService._enqueue(
                        connection, owner, scope, subject, "provider", remote_id, provider
                    )

    @staticmethod
    def _enqueue(
        connection: Connection,
        owner: UUID,
        scope: str,
        subject: UUID,
        sink: str,
        reference_id: str,
        provider: str | None = None,
    ) -> None:
        connection.execute(
            text(
                "INSERT INTO runtime_private.retention_cleanup_items "
                "(cleanup_id,owner_id,subject_id,scope,sink,provider,reference_id) "
                "VALUES (:id,:owner,:subject,:scope,:sink,:provider,:reference) "
                "ON CONFLICT (owner_id,scope,subject_id,sink,reference_id) DO NOTHING"
            ),
            {
                "id": uuid4(),
                "owner": owner,
                "subject": subject,
                "scope": scope,
                "sink": sink,
                "provider": provider,
                "reference": reference_id,
            },
        )

    @staticmethod
    def _scrub_run_content(connection: Connection, owner: UUID, run_id: UUID) -> None:
        connection.execute(
            text(
                "UPDATE runtime_private.run_admission_pins SET "
                "task_context=jsonb_build_object('task','[deleted]'), issue_snapshot=NULL, "
                "source_receipt='{}'::jsonb WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET model_task_context='{}'::jsonb "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.agent_turns SET context='{}'::jsonb, "
                "tool_schemas='[]'::jsonb, "
                "response=CASE WHEN status IN ('response_accepted','invalid_response') "
                "THEN '{}'::jsonb ELSE NULL END "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.agent_invocations SET arguments='{}'::jsonb "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.accepted_results SET payload='{}'::jsonb "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.approvals SET manifest='{}'::jsonb "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.events SET payload='{}'::jsonb "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.revision_feedback SET feedback='[deleted]' "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.github_command_payloads AS payload "
                "SET payload=jsonb_build_object('feedback','') "
                "FROM runtime_private.github_command_receipts AS receipt, "
                "runtime_private.runs AS run WHERE receipt.owner_id=:owner "
                "AND run.owner_id=:owner AND run.run_id=:run "
                "AND receipt.repository_id=run.github_repository_id "
                "AND receipt.issue_number=run.issue_number "
                "AND payload.owner_id=receipt.owner_id "
                "AND payload.repository_id=receipt.repository_id "
                "AND payload.comment_id=receipt.comment_id"
            ),
            {"owner": owner, "run": run_id},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET content_scrubbed_at="
                "COALESCE(content_scrubbed_at,clock_timestamp()) "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        )
