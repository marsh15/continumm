"""Fenced upload reservations and immutable source-artifact references."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.artifacts.archives import MAX_COMPRESSED_BYTES, MAX_FILES, MAX_SOURCE_BYTES
from continuum_api.runtime.leases import (
    ExecutionLease,
    LeaseLost,
    LockedExecution,
    lock_current_execution,
)
from continuum_api.runtime.persistence import Principal, RuntimeRepository

ARTIFACT_CAPACITY_BYTES = 600 * 1024 * 1024


class ArtifactPersistenceError(RuntimeError):
    """A redacted artifact persistence failure with a stable code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ArtifactCapacityExceeded(ArtifactPersistenceError):
    def __init__(self) -> None:
        super().__init__("artifact_capacity_exceeded")


class ArtifactIdentityConflict(ArtifactPersistenceError):
    def __init__(self) -> None:
        super().__init__("artifact_checkpoint_identity_conflict")


class ArtifactReservationLost(ArtifactPersistenceError):
    def __init__(self) -> None:
        super().__init__("artifact_upload_reservation_lost")


@dataclass(frozen=True, slots=True)
class ArtifactUploadReservation:
    upload_id: UUID
    object_key: str
    is_new: bool
    status: str


@dataclass(frozen=True, slots=True)
class ArtifactReference:
    artifact_id: UUID
    repo_id: int
    object_key: str
    archive_sha256: str
    tree_sha256: str
    format_version: int
    compressed_bytes: int
    source_bytes: int
    file_count: int
    github_user_id: int
    content_expires_at: datetime
    parent_artifact_id: UUID | None = None


class ArtifactPersistence:
    """Persist upload lifecycle and account-scoped checkpoint references.

    The singleton capacity row serializes new byte reservations across API
    processes. Every byte remains charged until a known-absence proof is
    committed or future garbage collection confirms deletion. Storage calls
    must happen outside every transaction opened here.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def reserve_upload(
        self,
        principal: Principal,
        lease: ExecutionLease,
        checkpoint_key: str,
        archive_sha256: str,
        tree_sha256: str,
        format_version: int,
        compressed_bytes: int,
        source_bytes: int,
        file_count: int,
        intent_sha256: bytes,
    ) -> ArtifactUploadReservation:
        self._validate_request(
            principal,
            lease,
            checkpoint_key,
            archive_sha256,
            tree_sha256,
            format_version,
            compressed_bytes,
            source_bytes,
            file_count,
            intent_sha256,
        )
        try:
            # A prior commit may have succeeded even when its acknowledgment
            # was lost. Exact committed identity is safe to adopt read-only
            # after the original lease expires, without re-uploading bytes.
            with self._connection(principal) as connection:
                prior_reservation = self._find_reservation(
                    connection, principal, lease, checkpoint_key, for_update=False
                )
                if prior_reservation is not None:
                    self._assert_creator_identity(prior_reservation, principal, lease)
                    self._assert_same_intent(
                        prior_reservation,
                        archive_sha256,
                        tree_sha256,
                        format_version,
                        compressed_bytes,
                        source_bytes,
                        file_count,
                        intent_sha256,
                    )
                    if prior_reservation["status"] == "committed":
                        expires_at: datetime = connection.execute(
                            text("SELECT clock_timestamp()")
                        ).scalar_one()
                        if prior_reservation["content_expires_at"] <= expires_at:
                            raise ArtifactReservationLost()
                    # Existing state is observation-only. In particular an
                    # expired lease can inspect its immutable upload record,
                    # but only a fresh row grants the one POST authorization.
                    return self._reservation_from_row(prior_reservation, is_new=False)

            with self._connection(principal) as connection:
                capacity = (
                    connection.execute(
                        text(
                            "SELECT capacity_limit_bytes, charged_bytes "
                            "FROM runtime_private.artifact_capacity "
                            "WHERE capacity_id = 1 FOR UPDATE"
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if capacity is None:
                    raise ArtifactPersistenceError("artifact_capacity_unavailable")

                locked = lock_current_execution(connection, principal, lease)
                run = (
                    connection.execute(
                        text(
                            "SELECT github_repository_id, created_at, "
                            "created_at + interval '14 days' > clock_timestamp() "
                            "AS content_active "
                            "FROM runtime_private.runs "
                            "WHERE owner_id = :owner_id AND run_id = :run_id"
                        ),
                        {"owner_id": lease.owner_id, "run_id": lease.run_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if run is None or run["github_repository_id"] is None:
                    raise ArtifactPersistenceError("artifact_repository_unavailable")
                if not run["content_active"]:
                    raise ArtifactPersistenceError("artifact_content_expired")

                existing = self._find_reservation(
                    connection, principal, lease, checkpoint_key, for_update=True
                )
                if existing is not None:
                    self._assert_same_intent(
                        existing,
                        archive_sha256,
                        tree_sha256,
                        format_version,
                        compressed_bytes,
                        source_bytes,
                        file_count,
                        intent_sha256,
                    )
                    self._assert_creator_identity(existing, principal, lease)
                    self._assert_execution_live(connection, locked)
                    return self._reservation_from_row(existing, is_new=False)

                parent_upload_id = connection.execute(
                    text(
                        "SELECT artifact.upload_id "
                        "FROM runtime_private.accepted_results AS accepted "
                        "JOIN runtime_private.artifact_uploads AS artifact "
                        "ON artifact.owner_id = accepted.owner_id "
                        "AND artifact.run_id = accepted.run_id "
                        "AND artifact.upload_id = accepted.artifact_id "
                        "WHERE accepted.owner_id = :owner_id AND accepted.run_id = :run_id "
                        "AND artifact.status = 'committed' "
                        "AND artifact.content_expires_at > clock_timestamp() "
                        "ORDER BY accepted.event_sequence DESC LIMIT 1"
                    ),
                    {"owner_id": lease.owner_id, "run_id": lease.run_id},
                ).scalar_one_or_none()

                charged_bytes: int = capacity["charged_bytes"]
                capacity_limit_bytes: int = capacity["capacity_limit_bytes"]
                if capacity_limit_bytes != ARTIFACT_CAPACITY_BYTES:
                    raise ArtifactPersistenceError("artifact_capacity_ledger_invalid")
                if charged_bytes + compressed_bytes > capacity_limit_bytes:
                    raise ArtifactCapacityExceeded()

                upload_id = uuid4()
                object_key = f"checkpoints/{upload_id}.tar.gz"
                connection.execute(
                    text(
                        "UPDATE runtime_private.artifact_capacity "
                        "SET charged_bytes = charged_bytes + :compressed_bytes "
                        "WHERE capacity_id = 1"
                    ),
                    {"compressed_bytes": compressed_bytes},
                )
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.artifact_uploads ("
                        "upload_id, owner_id, run_id, parent_upload_id, step_id, job_id, "
                        "generation, "
                        "attempt_id, lease_owner, github_repository_id, checkpoint_key, "
                        "intent_sha256, object_key, archive_sha256, tree_sha256, "
                        "format_version, compressed_bytes, source_bytes, file_count, "
                        "content_expires_at) VALUES ("
                        ":upload_id, :owner_id, :run_id, :parent_upload_id, :step_id, :job_id, "
                        ":generation, "
                        ":attempt_id, :lease_owner, :repo_id, :checkpoint_key, :intent_sha256, "
                        ":object_key, :archive_sha256, :tree_sha256, :format_version, "
                        ":compressed_bytes, :source_bytes, :file_count, "
                        "CAST(:run_created_at AS timestamptz) + interval '14 days')"
                    ),
                    {
                        "upload_id": upload_id,
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                        "parent_upload_id": parent_upload_id,
                        "step_id": lease.step_id,
                        "job_id": lease.job_id,
                        "generation": lease.generation,
                        "attempt_id": lease.attempt_id,
                        "lease_owner": lease.lease_owner,
                        "repo_id": run["github_repository_id"],
                        "checkpoint_key": checkpoint_key,
                        "intent_sha256": intent_sha256,
                        "object_key": object_key,
                        "archive_sha256": bytes.fromhex(archive_sha256),
                        "tree_sha256": bytes.fromhex(tree_sha256),
                        "format_version": format_version,
                        "compressed_bytes": compressed_bytes,
                        "source_bytes": source_bytes,
                        "file_count": file_count,
                        "run_created_at": run["created_at"],
                    },
                )
                self._assert_execution_live(connection, locked)
                return ArtifactUploadReservation(
                    upload_id=upload_id,
                    object_key=object_key,
                    is_new=True,
                    status="reserved",
                )
        except (ArtifactPersistenceError, LeaseLost):
            raise
        except SQLAlchemyError:
            raise ArtifactPersistenceError("artifact_storage_unavailable") from None

    def mark_uploading(self, principal: Principal, lease: ExecutionLease, upload_id: UUID) -> bool:
        """Authorize a single first POST while the full execution lease is live."""
        try:
            with self._connection(principal) as connection:
                locked = lock_current_execution(connection, principal, lease)
                row = self._lock_upload(connection, principal, lease, upload_id)
                if row["status"] != "reserved":
                    return False
                connection.execute(
                    text(
                        "UPDATE runtime_private.artifact_uploads "
                        "SET status = 'uploading', updated_at = clock_timestamp() "
                        "WHERE owner_id = :owner_id AND upload_id = :upload_id "
                        "AND status = 'reserved'"
                    ),
                    {"owner_id": lease.owner_id, "upload_id": upload_id},
                )
                self._assert_execution_live(connection, locked)
                return True
        except (ArtifactPersistenceError, LeaseLost):
            raise
        except SQLAlchemyError:
            raise ArtifactPersistenceError("artifact_storage_unavailable") from None

    def mark_unknown(self, principal: Principal, lease: ExecutionLease, upload_id: UUID) -> None:
        self._mark_observation(principal, lease, upload_id, "unknown")

    def mark_orphan(self, principal: Principal, lease: ExecutionLease, upload_id: UUID) -> None:
        self._mark_observation(principal, lease, upload_id, "orphan")

    def mark_verified(
        self, principal: Principal, lease: ExecutionLease, upload_id: UUID, object_bytes: int
    ) -> None:
        if object_bytes < 0:
            raise ValueError("artifact_object_size_invalid")
        try:
            with self._connection(principal) as connection:
                row = self._lock_upload(connection, principal, lease, upload_id)
                if row["status"] in {"verified", "committed"}:
                    if row["object_bytes"] != object_bytes:
                        raise ArtifactIdentityConflict()
                    return
                if row["status"] not in {"uploading", "unknown"}:
                    raise ArtifactReservationLost()
                if object_bytes != row["compressed_bytes"]:
                    raise ArtifactPersistenceError("artifact_object_size_mismatch")
                connection.execute(
                    text(
                        "UPDATE runtime_private.artifact_uploads SET status = 'verified', "
                        "object_bytes = :object_bytes, verified_at = clock_timestamp(), "
                        "updated_at = clock_timestamp() "
                        "WHERE owner_id = :owner_id AND upload_id = :upload_id "
                        "AND status IN ('uploading', 'unknown')"
                    ),
                    {
                        "object_bytes": object_bytes,
                        "owner_id": lease.owner_id,
                        "upload_id": upload_id,
                    },
                )
        except (ArtifactPersistenceError, LeaseLost):
            raise
        except SQLAlchemyError:
            raise ArtifactPersistenceError("artifact_storage_unavailable") from None

    def release_absent(
        self,
        principal: Principal,
        lease: ExecutionLease,
        upload_id: UUID,
        proof: Literal["not_dispatched", "known_failure_and_absence"],
    ) -> None:
        """Release bytes only for a never-dispatched or known-absent upload."""
        if proof not in {"not_dispatched", "known_failure_and_absence"}:
            raise ValueError("artifact_absence_proof_invalid")
        try:
            with self._connection(principal) as connection:
                capacity = (
                    connection.execute(
                        text(
                            "SELECT charged_bytes FROM runtime_private.artifact_capacity "
                            "WHERE capacity_id = 1 FOR UPDATE"
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if capacity is None:
                    raise ArtifactPersistenceError("artifact_capacity_unavailable")
                locked = lock_current_execution(connection, principal, lease)
                row = self._lock_upload(connection, principal, lease, upload_id)
                if row["status"] == "released":
                    return
                allowed_status = "reserved" if proof == "not_dispatched" else "uploading"
                if row["status"] != allowed_status:
                    raise ArtifactReservationLost()
                if capacity["charged_bytes"] < row["compressed_bytes"]:
                    raise ArtifactPersistenceError("artifact_capacity_ledger_invalid")
                self._assert_execution_live(connection, locked)
                connection.execute(
                    text(
                        "UPDATE runtime_private.artifact_capacity "
                        "SET charged_bytes = charged_bytes - :compressed_bytes "
                        "WHERE capacity_id = 1"
                    ),
                    {"compressed_bytes": row["compressed_bytes"]},
                )
                connection.execute(
                    text(
                        "UPDATE runtime_private.artifact_uploads SET status = 'released', "
                        "released_at = clock_timestamp(), updated_at = clock_timestamp() "
                        "WHERE owner_id = :owner_id AND upload_id = :upload_id "
                        "AND status = :status"
                    ),
                    {
                        "owner_id": lease.owner_id,
                        "upload_id": upload_id,
                        "status": allowed_status,
                    },
                )
                self._assert_execution_live(connection, locked)
        except (ArtifactPersistenceError, LeaseLost):
            raise
        except SQLAlchemyError:
            raise ArtifactPersistenceError("artifact_storage_unavailable") from None

    def attach_verified(
        self,
        connection: Connection,
        principal: Principal,
        lease: ExecutionLease,
        upload_id: UUID,
    ) -> ArtifactReference:
        """Fence and commit a verified upload in the caller's T08 transaction."""
        if not connection.in_transaction():
            raise ValueError("artifact_attachment_requires_transaction")
        RuntimeRepository(connection, principal)
        locked = lock_current_execution(connection, principal, lease)
        row = self._lock_upload(connection, principal, lease, upload_id)
        if row["status"] != "verified":
            raise ArtifactReservationLost()
        reference = self._reference_from_row(connection, principal, row)
        connection.execute(
            text(
                "UPDATE runtime_private.artifact_uploads SET status = 'committed', "
                "committed_at = clock_timestamp(), updated_at = clock_timestamp() "
                "WHERE owner_id = :owner_id AND upload_id = :upload_id AND status = 'verified'"
            ),
            {"owner_id": lease.owner_id, "upload_id": upload_id},
        )
        self._assert_execution_live(connection, locked)
        return reference

    def get_committed(
        self, principal: Principal, lease: ExecutionLease, artifact_id: UUID
    ) -> ArtifactReference:
        """Read a committed reference for later access revalidation and restore."""
        try:
            with self._connection(principal) as connection:
                lock_current_execution(connection, principal, lease)
                row = (
                    connection.execute(
                        text(
                            "SELECT artifact.*, app_user.github_user_id "
                            "FROM runtime_private.artifact_uploads AS artifact "
                            "JOIN runtime_private.users AS app_user "
                            "ON app_user.user_id = artifact.owner_id "
                            "WHERE artifact.owner_id = :owner_id "
                            "AND artifact.run_id = :run_id AND artifact.upload_id = :upload_id "
                            "AND artifact.status = 'committed' "
                            "AND artifact.content_expires_at > clock_timestamp() "
                            "AND app_user.status = 'active'"
                        ),
                        {
                            "owner_id": principal.account_id,
                            "run_id": lease.run_id,
                            "upload_id": artifact_id,
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise ArtifactReservationLost()
                return self._reference_from_row(connection, principal, dict(row))
        except (ArtifactPersistenceError, LeaseLost):
            raise
        except SQLAlchemyError:
            raise ArtifactPersistenceError("artifact_storage_unavailable") from None

    def get_upload_access_context(
        self, principal: Principal, lease: ExecutionLease, upload_id: UUID
    ) -> tuple[int, int]:
        """Return DB-derived repository and actor IDs for a live upload readback."""
        try:
            with self._connection(principal) as connection:
                locked = lock_current_execution(connection, principal, lease)
                row = self._lock_upload(connection, principal, lease, upload_id)
                if (
                    row["status"] not in {"reserved", "uploading", "unknown", "verified"}
                    or row["content_expires_at"] <= locked.database_now
                ):
                    raise ArtifactReservationLost()
                github_user_id = connection.execute(
                    text(
                        "SELECT github_user_id FROM runtime_private.users "
                        "WHERE user_id = :owner_id AND status = 'active'"
                    ),
                    {"owner_id": principal.account_id},
                ).scalar_one_or_none()
                if github_user_id is None:
                    raise ArtifactReservationLost()
                self._assert_execution_live(connection, locked)
                return row["github_repository_id"], github_user_id
        except (ArtifactPersistenceError, LeaseLost):
            raise
        except SQLAlchemyError:
            raise ArtifactPersistenceError("artifact_storage_unavailable") from None

    def _mark_observation(
        self,
        principal: Principal,
        lease: ExecutionLease,
        upload_id: UUID,
        observed_status: Literal["unknown", "orphan"],
    ) -> None:
        """Record storage observations even if the creating lease has expired."""
        try:
            with self._connection(principal) as connection:
                row = self._lock_upload(connection, principal, lease, upload_id)
                if row["status"] in {"committed", "released", "deleting"}:
                    return
                if row["status"] == "orphan":
                    return
                if row["status"] == observed_status:
                    return
                connection.execute(
                    text(
                        "UPDATE runtime_private.artifact_uploads SET status = :status, "
                        "orphaned_at = CASE WHEN :is_orphan "
                        "THEN clock_timestamp() ELSE orphaned_at END, "
                        "orphan_grace_until = CASE WHEN :is_orphan "
                        "THEN LEAST(content_expires_at, clock_timestamp() + interval '1 hour') "
                        "ELSE orphan_grace_until END, "
                        "updated_at = clock_timestamp() "
                        "WHERE owner_id = :owner_id AND upload_id = :upload_id "
                        "AND status <> 'committed' AND status <> 'released'"
                    ),
                    {
                        "status": observed_status,
                        "is_orphan": observed_status == "orphan",
                        "owner_id": lease.owner_id,
                        "upload_id": upload_id,
                    },
                )
        except (ArtifactPersistenceError, LeaseLost):
            raise
        except SQLAlchemyError:
            raise ArtifactPersistenceError("artifact_storage_unavailable") from None

    @contextmanager
    def _connection(self, principal: Principal) -> Iterator[Connection]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, principal)
            yield connection

    @staticmethod
    def _find_reservation(
        connection: Connection,
        principal: Principal,
        lease: ExecutionLease,
        checkpoint_key: str,
        *,
        for_update: bool,
    ) -> Mapping[str, Any] | None:
        lock_clause = " FOR UPDATE" if for_update else ""
        row = (
            connection.execute(
                text(
                    "SELECT * FROM runtime_private.artifact_uploads "
                    "WHERE owner_id = :owner_id AND run_id = :run_id "
                    "AND attempt_id = :attempt_id AND checkpoint_key = :checkpoint_key"
                    + lock_clause
                ),
                {
                    "owner_id": principal.account_id,
                    "run_id": lease.run_id,
                    "attempt_id": lease.attempt_id,
                    "checkpoint_key": checkpoint_key,
                },
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else dict(row)

    @staticmethod
    def _lock_upload(
        connection: Connection, principal: Principal, lease: ExecutionLease, upload_id: UUID
    ) -> Mapping[str, Any]:
        row = (
            connection.execute(
                text(
                    "SELECT * FROM runtime_private.artifact_uploads "
                    "WHERE owner_id = :owner_id AND run_id = :run_id AND upload_id = :upload_id "
                    "FOR UPDATE"
                ),
                {
                    "owner_id": principal.account_id,
                    "run_id": lease.run_id,
                    "upload_id": upload_id,
                },
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ArtifactReservationLost()
        upload = dict(row)
        ArtifactPersistence._assert_creator_identity(upload, principal, lease)
        return upload

    @staticmethod
    def _assert_creator_identity(
        row: Mapping[str, Any], principal: Principal, lease: ExecutionLease
    ) -> None:
        if (
            principal.account_id != lease.owner_id
            or row["owner_id"] != lease.owner_id
            or row["run_id"] != lease.run_id
            or row["step_id"] != lease.step_id
            or row["job_id"] != lease.job_id
            or row["generation"] != lease.generation
            or row["attempt_id"] != lease.attempt_id
            or row["lease_owner"] != lease.lease_owner
        ):
            raise ArtifactReservationLost()

    @staticmethod
    def _assert_same_intent(
        row: Mapping[str, Any],
        archive_sha256: str,
        tree_sha256: str,
        format_version: int,
        compressed_bytes: int,
        source_bytes: int,
        file_count: int,
        intent_sha256: bytes,
    ) -> None:
        if (
            row["archive_sha256"].hex() != archive_sha256
            or row["tree_sha256"].hex() != tree_sha256
            or row["format_version"] != format_version
            or row["compressed_bytes"] != compressed_bytes
            or row["source_bytes"] != source_bytes
            or row["file_count"] != file_count
            or row["intent_sha256"] != intent_sha256
        ):
            raise ArtifactIdentityConflict()

    @staticmethod
    def _reservation_from_row(row: Mapping[str, Any], *, is_new: bool) -> ArtifactUploadReservation:
        return ArtifactUploadReservation(
            upload_id=row["upload_id"],
            object_key=row["object_key"],
            is_new=is_new,
            status=row["status"],
        )

    @staticmethod
    def _reference_from_row(
        connection: Connection, principal: Principal, row: Mapping[str, Any]
    ) -> ArtifactReference:
        github_user_id = row.get("github_user_id")
        if github_user_id is None:
            github_user_id = connection.execute(
                text(
                    "SELECT github_user_id FROM runtime_private.users "
                    "WHERE user_id = :owner_id AND status = 'active'"
                ),
                {"owner_id": principal.account_id},
            ).scalar_one_or_none()
        if github_user_id is None:
            raise ArtifactReservationLost()
        return ArtifactReference(
            artifact_id=row["upload_id"],
            repo_id=row["github_repository_id"],
            object_key=row["object_key"],
            archive_sha256=row["archive_sha256"].hex(),
            tree_sha256=row["tree_sha256"].hex(),
            format_version=row["format_version"],
            compressed_bytes=row["compressed_bytes"],
            source_bytes=row["source_bytes"],
            file_count=row["file_count"],
            github_user_id=github_user_id,
            content_expires_at=row["content_expires_at"],
            parent_artifact_id=row["parent_upload_id"],
        )

    @staticmethod
    def _assert_execution_live(connection: Connection, locked: LockedExecution) -> None:
        now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
        if locked.lease_expires_at is None or locked.lease_expires_at <= now:
            raise LeaseLost("runtime_lease_expired_before_commit")
        if locked.run_deadline_at is not None and locked.run_deadline_at <= now:
            raise LeaseLost("runtime_run_expired_before_commit")

    @staticmethod
    def _validate_request(
        principal: Principal,
        lease: ExecutionLease,
        checkpoint_key: str,
        archive_sha256: str,
        tree_sha256: str,
        format_version: int,
        compressed_bytes: int,
        source_bytes: int,
        file_count: int,
        intent_sha256: bytes,
    ) -> None:
        if principal.account_id != lease.owner_id:
            raise LeaseLost("runtime_lease_owner_mismatch")
        if (
            not checkpoint_key
            or len(checkpoint_key) > 128
            or checkpoint_key.strip() != checkpoint_key
        ):
            raise ValueError("artifact_checkpoint_key_invalid")
        for digest in (archive_sha256, tree_sha256):
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise ValueError("artifact_digest_invalid")
        if len(intent_sha256) != 32:
            raise ValueError("artifact_intent_digest_invalid")
        if format_version != 1:
            raise ValueError("artifact_format_version_invalid")
        if not 1 <= compressed_bytes <= MAX_COMPRESSED_BYTES:
            raise ValueError("artifact_compressed_size_invalid")
        if not 0 <= source_bytes <= MAX_SOURCE_BYTES:
            raise ValueError("artifact_source_size_invalid")
        if not 0 <= file_count <= MAX_FILES:
            raise ValueError("artifact_file_count_invalid")
