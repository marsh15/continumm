"""Fenced upload and restore orchestration for source-only checkpoints."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from continuum_api.artifacts.archives import (
    MAX_COMPRESSED_BYTES,
    ArchiveError,
    ArchiveFile,
    ArchiveManifest,
    VerifiedArchive,
    create_archive,
    verify_archive,
)
from continuum_api.connectivity.database import EffectState
from continuum_api.connectivity.storage import StorageFailure
from continuum_api.repositories.preflight import (
    GitHubRepositoryAdapter,
    PendingRepositoryAccessVerifier,
    RepositoryAccessProof,
    RepositoryAccessVerifier,
    RepositoryPreflightError,
)
from continuum_api.runtime.artifact_persistence import (
    ArtifactPersistence,
    ArtifactPersistenceError,
    ArtifactUploadReservation,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost
from continuum_api.runtime.persistence import Principal, RunStatus
from continuum_api.runtime.scheduler import FailureClass, RuntimeRecoveryService
from continuum_api.runtime.transitions import (
    InvalidTransition,
    NextWork,
    ResultIdentityConflict,
    TransitionReceipt,
    TransitionService,
    transition_intent_sha256,
)

CHECKPOINT_DEADLINE_SECONDS = 60.0
STATE_FINALIZE_DEADLINE_SECONDS = 5.0
_LOGGER = logging.getLogger(__name__)


class CheckpointStorageError(RuntimeError):
    """A sanitized checkpoint lifecycle error with a stable code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class CheckpointObjectStorage(Protocol):
    """Private object storage operations required by checkpoint orchestration."""

    @property
    def byte_limit(self) -> int: ...

    async def check_private_bucket(self) -> None: ...

    async def upload(self, object_key: str, content: bytes) -> None: ...

    async def download(self, object_key: str) -> bytes: ...


@dataclass(frozen=True, slots=True)
class CheckpointReceipt:
    artifact_id: UUID
    object_key: str
    archive_sha256: str
    tree_sha256: str
    compressed_bytes: int
    source_bytes: int
    file_count: int
    transition: TransitionReceipt
    uploaded: bool
    adopted: bool


class CheckpointStorageService:
    """Commit one verified private archive with no blind upload retry."""

    def __init__(
        self,
        persistence: ArtifactPersistence,
        storage: CheckpointObjectStorage,
        transitions: TransitionService,
        recovery: RuntimeRecoveryService,
        *,
        access_verifier: RepositoryAccessVerifier | None = None,
        github_adapter_factory: Callable[[], GitHubRepositoryAdapter] = GitHubRepositoryAdapter,
        deadline_seconds: float = CHECKPOINT_DEADLINE_SECONDS,
    ) -> None:
        if deadline_seconds <= 0:
            raise ValueError("checkpoint deadline must be positive")
        self._persistence = persistence
        self._storage = storage
        self._transitions = transitions
        self._recovery = recovery
        self._access_verifier = access_verifier or PendingRepositoryAccessVerifier()
        self._github_adapter_factory = github_adapter_factory
        self._deadline_seconds = deadline_seconds

    async def commit_checkpoint(
        self,
        principal: Principal,
        lease: ExecutionLease,
        checkpoint_key: str,
        files: Iterable[ArchiveFile],
        *,
        result: dict[str, Any],
        target_run_status: RunStatus,
        next_work: NextWork | None = None,
    ) -> CheckpointReceipt:
        """Create, reserve, upload once, read back, then atomically accept."""
        try:
            async with asyncio.timeout(self._deadline_seconds):
                base_transition_intent = transition_intent_sha256(
                    result, target_run_status, next_work
                )
                bundle = await asyncio.to_thread(create_archive, files)
                verified_local = await asyncio.to_thread(
                    verify_archive,
                    bundle.archive,
                    expected_archive_sha256=bundle.archive_sha256,
                    expected_tree_sha256=bundle.manifest.tree_sha256,
                    expected_manifest=bundle.manifest,
                )
                del verified_local
                compressed_bytes = len(bundle.archive)
                if (
                    compressed_bytes > MAX_COMPRESSED_BYTES
                    or compressed_bytes > self._storage.byte_limit
                ):
                    raise CheckpointStorageError("artifact_compressed_size_limit")
                source_bytes = sum(entry.size for entry in bundle.manifest.entries)
                file_count = len(bundle.manifest.entries)
                intent_sha256 = _upload_intent_sha256(
                    principal,
                    lease,
                    checkpoint_key,
                    bundle.manifest,
                    compressed_bytes,
                    base_transition_intent,
                )
                reservation = await asyncio.to_thread(
                    self._persistence.reserve_upload,
                    principal,
                    lease,
                    checkpoint_key,
                    bundle.archive_sha256,
                    bundle.manifest.tree_sha256,
                    bundle.manifest.format_version,
                    compressed_bytes,
                    source_bytes,
                    file_count,
                    intent_sha256,
                )

                if reservation.status == "committed":
                    transition = await self._commit_transition(
                        principal,
                        lease,
                        result,
                        target_run_status,
                        next_work,
                        reservation.upload_id,
                    )
                    return _receipt(
                        reservation,
                        bundle.archive_sha256,
                        bundle.manifest,
                        compressed_bytes,
                        source_bytes,
                        transition,
                        uploaded=False,
                    )

                if reservation.status in {"orphan", "released", "deleting"}:
                    raise CheckpointStorageError("artifact_reservation_not_reusable")

                if reservation.is_new:
                    try:
                        await self._verify_upload_repository_access(principal, lease, reservation)
                    except CheckpointStorageError:
                        await self._release_not_dispatched(principal, lease, reservation)
                        raise
                    await self._check_private_bucket_before_dispatch(principal, lease, reservation)
                    may_dispatch = await asyncio.to_thread(
                        self._persistence.mark_uploading,
                        principal,
                        lease,
                        reservation.upload_id,
                    )
                    if may_dispatch:
                        try:
                            await self._storage.upload(reservation.object_key, bundle.archive)
                        except StorageFailure as error:
                            return await self._reconcile_failed_upload(
                                principal,
                                lease,
                                reservation,
                                bundle.archive,
                                bundle.archive_sha256,
                                bundle.manifest,
                                error,
                                result,
                                target_run_status,
                                next_work,
                            )
                        uploaded = True
                    else:
                        uploaded = False
                else:
                    uploaded = False
                    if reservation.status == "reserved":
                        await self._release_not_dispatched(principal, lease, reservation)
                        raise CheckpointStorageError("artifact_upload_not_dispatched")
                    await self._storage.check_private_bucket()
                stored_bytes = await self._download_for_reconciliation(
                    principal, lease, reservation
                )
                if stored_bytes is None:
                    await self._observe_unknown(principal, lease, reservation)
                    raise CheckpointStorageError("artifact_upload_outcome_unknown")
                await self._verify_stored_upload(
                    principal,
                    lease,
                    reservation,
                    stored_bytes,
                    bundle.archive_sha256,
                    bundle.manifest,
                    compressed_bytes,
                )
                try:
                    transition = await self._commit_transition(
                        principal,
                        lease,
                        result,
                        target_run_status,
                        next_work,
                        reservation.upload_id,
                    )
                except LeaseLost:
                    await self._observe_orphan(principal, lease, reservation)
                    raise CheckpointStorageError("artifact_lease_lost") from None
                except ResultIdentityConflict:
                    await self._observe_orphan(principal, lease, reservation)
                    raise CheckpointStorageError("artifact_result_identity_conflict") from None
                except InvalidTransition:
                    await self._observe_orphan(principal, lease, reservation)
                    raise CheckpointStorageError("artifact_transition_rejected") from None
                except Exception:
                    # If commit acknowledgment is lost, the persistence layer
                    # protects an already committed row from orphaning. If it
                    # did not commit, keep the verified bytes charged as an
                    # orphan until reference-aware cleanup is added.
                    await self._observe_orphan(principal, lease, reservation)
                    raise
                return _receipt(
                    reservation,
                    bundle.archive_sha256,
                    bundle.manifest,
                    compressed_bytes,
                    source_bytes,
                    transition,
                    uploaded=uploaded,
                )
        except TimeoutError:
            raise CheckpointStorageError("artifact_checkpoint_timeout") from None
        except CheckpointStorageError:
            raise
        except ArchiveError as error:
            raise CheckpointStorageError(error.code) from None
        except ArtifactPersistenceError as error:
            raise CheckpointStorageError(error.code) from None
        except StorageFailure as error:
            raise CheckpointStorageError(error.code) from None
        except LeaseLost:
            raise CheckpointStorageError("artifact_lease_lost") from None
        except InvalidTransition as error:
            known_codes = {
                "accepted_result_too_large",
                "activity_result_cannot_set_terminal_run_status",
                "next_work_requires_running_status",
                "runtime_artifact_identity_mismatch",
                "runtime_artifact_persistence_unavailable",
                "runtime_run_not_running",
                "transition_requires_next_work_or_wait_or_terminal_status",
            }
            code = str(error)
            raise CheckpointStorageError(
                code if code in known_codes else "artifact_transition_rejected"
            ) from None
        except ResultIdentityConflict:
            raise CheckpointStorageError("artifact_transition_rejected") from None

    async def restore_archive(
        self,
        principal: Principal,
        lease: ExecutionLease,
        artifact_id: UUID,
    ) -> VerifiedArchive:
        """Authorize and verify an owned committed archive for current work."""
        try:
            async with asyncio.timeout(self._deadline_seconds):
                reference = await asyncio.to_thread(
                    self._persistence.get_committed,
                    principal,
                    lease,
                    artifact_id,
                )
                await self._verify_current_repository_access(
                    reference.repo_id, reference.github_user_id
                )
                try:
                    await self._storage.check_private_bucket()
                except StorageFailure as error:
                    raise CheckpointStorageError(error.code) from None
                try:
                    archive = await self._storage.download(reference.object_key)
                except StorageFailure as error:
                    if error.status_code == 404:
                        await self._fail_corrupt_artifact(lease)
                        raise CheckpointStorageError("corrupt_artifact") from None
                    raise CheckpointStorageError("artifact_download_unavailable") from None

                try:
                    verified = await asyncio.to_thread(
                        verify_archive,
                        archive,
                        expected_archive_sha256=reference.archive_sha256,
                        expected_tree_sha256=reference.tree_sha256,
                    )
                    if (
                        len(archive) != reference.compressed_bytes
                        or verified.manifest.format_version != reference.format_version
                        or len(verified.manifest.entries) != reference.file_count
                        or sum(entry.size for entry in verified.manifest.entries)
                        != reference.source_bytes
                    ):
                        raise ArchiveError("archive_metadata_mismatch")
                    return verified
                except ArchiveError:
                    await self._fail_corrupt_artifact(lease)
                    raise CheckpointStorageError("corrupt_artifact") from None
        except TimeoutError:
            raise CheckpointStorageError("artifact_restore_timeout") from None
        except ArtifactPersistenceError as error:
            if error.code == "artifact_upload_reservation_lost":
                await self._fail_corrupt_artifact(lease)
            raise CheckpointStorageError(error.code) from None
        except LeaseLost:
            raise CheckpointStorageError("artifact_lease_lost") from None

    async def fail_unrecoverable_restore(self, lease: ExecutionLease) -> None:
        """Move the current attempt to attention when no authoritative object exists."""
        await self._fail_corrupt_artifact(lease)

    async def _check_private_bucket_before_dispatch(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: ArtifactUploadReservation,
    ) -> None:
        try:
            await self._storage.check_private_bucket()
        except StorageFailure as error:
            await self._release_not_dispatched(principal, lease, reservation)
            raise CheckpointStorageError(error.code) from None

    async def _reconcile_failed_upload(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: ArtifactUploadReservation,
        archive: bytes,
        archive_sha256: str,
        manifest: ArchiveManifest,
        upload_error: StorageFailure,
        result: dict[str, Any],
        target_run_status: RunStatus,
        next_work: NextWork | None,
    ) -> CheckpointReceipt:
        known_failure_can_prove_absence = (
            upload_error.effect is EffectState.KNOWN
            and upload_error.status_code is not None
            and upload_error.status_code != 409
        )
        if not known_failure_can_prove_absence:
            await self._observe_unknown(principal, lease, reservation)

        try:
            await self._storage.check_private_bucket()
            stored_bytes = await self._download_for_reconciliation(principal, lease, reservation)
        except CheckpointStorageError:
            await self._observe_unknown(principal, lease, reservation)
            raise
        except StorageFailure:
            await self._observe_unknown(principal, lease, reservation)
            raise CheckpointStorageError("artifact_storage_unavailable") from None
        if stored_bytes is None:
            if known_failure_can_prove_absence:
                await self._release_known_failure_and_absence(principal, lease, reservation)
                raise CheckpointStorageError("artifact_upload_rejected") from None
            await self._observe_unknown(principal, lease, reservation)
            raise CheckpointStorageError("artifact_upload_outcome_unknown") from None

        compressed_bytes = len(archive)
        source_bytes = sum(entry.size for entry in manifest.entries)
        await self._verify_stored_upload(
            principal,
            lease,
            reservation,
            stored_bytes,
            archive_sha256,
            manifest,
            compressed_bytes,
        )
        try:
            transition = await self._commit_transition(
                principal,
                lease,
                result,
                target_run_status,
                next_work,
                reservation.upload_id,
            )
        except LeaseLost:
            await self._observe_orphan(principal, lease, reservation)
            raise CheckpointStorageError("artifact_lease_lost") from None
        except ResultIdentityConflict:
            await self._observe_orphan(principal, lease, reservation)
            raise CheckpointStorageError("artifact_result_identity_conflict") from None
        except InvalidTransition:
            await self._observe_orphan(principal, lease, reservation)
            raise CheckpointStorageError("artifact_transition_rejected") from None
        except Exception:
            await self._observe_orphan(principal, lease, reservation)
            raise
        return _receipt(
            reservation,
            archive_sha256,
            manifest,
            compressed_bytes,
            source_bytes,
            transition,
            uploaded=True,
        )

    async def _verify_stored_upload(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: ArtifactUploadReservation,
        stored_bytes: bytes,
        archive_sha256: str,
        manifest: ArchiveManifest,
        compressed_bytes: int,
    ) -> None:
        try:
            await asyncio.to_thread(
                verify_archive,
                stored_bytes,
                expected_archive_sha256=archive_sha256,
                expected_tree_sha256=manifest.tree_sha256,
                expected_manifest=manifest,
            )
            if len(stored_bytes) != compressed_bytes:
                raise ArchiveError("archive_size_mismatch")
        except ArchiveError:
            await self._observe_orphan(principal, lease, reservation)
            await self._fail_corrupt_artifact(lease)
            raise CheckpointStorageError("artifact_object_verification_failed") from None
        try:
            await asyncio.to_thread(
                self._persistence.mark_verified,
                principal,
                lease,
                reservation.upload_id,
                len(stored_bytes),
            )
        except ArtifactPersistenceError:
            raise

    async def _download_for_reconciliation(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: ArtifactUploadReservation,
    ) -> bytes | None:
        await self._verify_upload_repository_access(principal, lease, reservation)
        try:
            return await self._storage.download(reservation.object_key)
        except StorageFailure as error:
            if error.status_code == 404:
                return None
            raise CheckpointStorageError("artifact_reconciliation_unavailable") from None

    async def _verify_upload_repository_access(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: ArtifactUploadReservation,
    ) -> None:
        try:
            repo_id, github_user_id = await asyncio.to_thread(
                self._persistence.get_upload_access_context,
                principal,
                lease,
                reservation.upload_id,
            )
        except ArtifactPersistenceError as error:
            raise CheckpointStorageError(error.code) from None
        await self._verify_current_repository_access(repo_id, github_user_id)

    async def _verify_current_repository_access(self, repo_id: int, github_user_id: int) -> None:
        try:
            proof = await self._access_verifier.verify(
                repository_id=repo_id,
                github_user_id=github_user_id,
            )
        except RepositoryPreflightError as error:
            raise CheckpointStorageError(error.code) from None
        except Exception:
            raise CheckpointStorageError("repository_authorization_unavailable") from None
        if (
            not isinstance(proof, RepositoryAccessProof)
            or proof.repository_id != repo_id
            or proof.github_user_id != github_user_id
            or proof.actor_can_write is not True
            or proof.app_can_access is not True
        ):
            raise CheckpointStorageError("repository_access_denied")

        adapter = self._github_adapter_factory()
        try:
            await adapter.current_public_repository(repo_id)
        except RepositoryPreflightError as error:
            raise CheckpointStorageError(error.code) from None
        except Exception:
            raise CheckpointStorageError("repository_metadata_unavailable") from None
        finally:
            await adapter.aclose()

    async def _fail_corrupt_artifact(self, lease: ExecutionLease) -> None:
        try:
            await asyncio.to_thread(
                self._recovery.fail_attempt,
                lease=lease,
                failure_class=FailureClass.CORRUPT_ARTIFACT,
            )
        except LeaseLost:
            _LOGGER.debug("checkpoint_corrupt_recovery_stale_lease")
        except Exception as error:
            # Keep the verified-content failure primary and emit no error body.
            _LOGGER.warning(
                "checkpoint_corrupt_recovery_failed error_type=%s",
                type(error).__name__,
            )
            return

    async def _commit_transition(
        self,
        principal: Principal,
        lease: ExecutionLease,
        result: dict[str, Any],
        target_run_status: RunStatus,
        next_work: NextWork | None,
        artifact_id: UUID,
    ) -> TransitionReceipt:
        del principal  # TransitionService is already bound to this principal.
        return await asyncio.to_thread(
            self._transitions.commit_result,
            lease=lease,
            result=result,
            target_run_status=target_run_status,
            next_work=next_work,
            artifact_id=artifact_id,
        )

    async def _release_not_dispatched(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: ArtifactUploadReservation,
    ) -> None:
        await self._bounded_state_call(
            self._persistence.release_absent,
            principal,
            lease,
            reservation.upload_id,
            "not_dispatched",
        )

    async def _release_known_failure_and_absence(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: ArtifactUploadReservation,
    ) -> None:
        await self._bounded_state_call(
            self._persistence.release_absent,
            principal,
            lease,
            reservation.upload_id,
            "known_failure_and_absence",
        )

    async def _observe_unknown(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: ArtifactUploadReservation,
    ) -> None:
        await self._bounded_state_call(
            self._persistence.mark_unknown,
            principal,
            lease,
            reservation.upload_id,
        )

    async def _observe_orphan(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: ArtifactUploadReservation,
    ) -> None:
        await self._bounded_state_call(
            self._persistence.mark_orphan,
            principal,
            lease,
            reservation.upload_id,
        )

    async def _bounded_state_call(self, function: Any, *args: Any) -> None:
        try:
            async with asyncio.timeout(STATE_FINALIZE_DEADLINE_SECONDS):
                await asyncio.to_thread(function, *args)
        except Exception as error:
            # The reservation remains charged if durable reconciliation cannot
            # be recorded; never replace the primary storage/lease failure.
            _LOGGER.warning(
                "checkpoint_observation_write_failed operation=%s error_type=%s",
                getattr(function, "__name__", "state_call"),
                type(error).__name__,
            )
            return


def _upload_intent_sha256(
    principal: Principal,
    lease: ExecutionLease,
    checkpoint_key: str,
    manifest: ArchiveManifest,
    compressed_bytes: int,
    base_transition_intent: bytes,
) -> bytes:
    payload = {
        "archive_format_version": manifest.format_version,
        "checkpoint_key": checkpoint_key,
        "compressed_bytes": compressed_bytes,
        "owner_id": str(principal.account_id),
        "run_id": str(lease.run_id),
        "step_id": str(lease.step_id),
        "job_id": str(lease.job_id),
        "generation": lease.generation,
        "attempt_id": str(lease.attempt_id),
        "lease_owner": lease.lease_owner,
        "source_tree_sha256": manifest.tree_sha256,
        "transition_base_intent_sha256": base_transition_intent.hex(),
    }
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).digest()


def _receipt(
    reservation: ArtifactUploadReservation,
    archive_sha256: str,
    manifest: ArchiveManifest,
    compressed_bytes: int,
    source_bytes: int,
    transition: TransitionReceipt,
    *,
    uploaded: bool,
) -> CheckpointReceipt:
    return CheckpointReceipt(
        artifact_id=reservation.upload_id,
        object_key=reservation.object_key,
        archive_sha256=archive_sha256,
        tree_sha256=manifest.tree_sha256,
        compressed_bytes=compressed_bytes,
        source_bytes=source_bytes,
        file_count=len(manifest.entries),
        transition=transition,
        uploaded=uploaded,
        adopted=transition.adopted,
    )
