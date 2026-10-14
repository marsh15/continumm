"""Lease-fenced recovery of source work from committed checkpoint artifacts."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.artifacts.archives import VerifiedArchive
from continuum_api.artifacts.checkpoints import (
    CheckpointReceipt,
    CheckpointStorageError,
    CheckpointStorageService,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, lock_current_execution
from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository
from continuum_api.runtime.sandbox_allocations import (
    AllocationReservation,
    SandboxAllocationError,
    SandboxAllocationStore,
)
from continuum_api.runtime.transitions import (
    NextWork,
    TransitionReceipt,
    TransitionService,
)
from continuum_api.tools.source_tools import (
    PatchCandidate,
    SourceSnapshot,
    SourceToolError,
    snapshot_from_verified_archive,
    verify_patch_candidate,
    verify_source_archive,
)

_INACTIVE_ALLOCATION_STATES = frozenset({"stopped", "destroyed", "not_applied", "released"})


class WorkspaceRecoveryError(RuntimeError):
    """Sanitized worker recovery failure with a stable code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class AllocationReconciler(Protocol):
    """Existing T16 owner-scoped observation and stop operations."""

    async def observe(self, principal: Principal, allocation_id: UUID) -> AllocationReservation: ...

    async def stop(self, principal: Principal, allocation_id: UUID) -> AllocationReservation: ...


@dataclass(frozen=True, slots=True)
class WorkspaceSnapshot:
    """A verified source tree restored from the exact accepted artifact."""

    owner_id: UUID
    run_id: UUID
    artifact_id: UUID
    generation: int
    attempt_id: UUID
    verified: VerifiedArchive
    source: SourceSnapshot


@dataclass(frozen=True, slots=True)
class WorkspaceRecovery:
    """Either an already accepted receipt or the source for a resumed activity."""

    accepted_result: TransitionReceipt | None
    workspace: WorkspaceSnapshot | None


class WorkspaceRecoveryService:
    """Restore committed source and persist prepared candidates through T08/T15."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        checkpoints: CheckpointStorageService,
        transitions: TransitionService,
        *,
        allocation_store: SandboxAllocationStore | None = None,
        allocation_reconciler: AllocationReconciler | None = None,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._checkpoints = checkpoints
        self._transitions = transitions
        self._allocation_store = allocation_store or SandboxAllocationStore(engine)
        self._allocation_reconciler = allocation_reconciler

    async def recover(self, principal: Principal, lease: ExecutionLease) -> WorkspaceRecovery:
        """Adopt an accepted logical result before attempting any artifact restore."""
        self._check_principal(principal, lease)
        accepted = await self._accepted_result(lease)
        if accepted is not None:
            return WorkspaceRecovery(accepted_result=accepted, workspace=None)
        workspace = await self._restore_latest(principal, lease)
        return WorkspaceRecovery(accepted_result=None, workspace=workspace)

    async def restore_latest(
        self, principal: Principal, lease: ExecutionLease
    ) -> WorkspaceSnapshot:
        """Restore only the latest accepted artifact under this current lease."""
        self._check_principal(principal, lease)
        if await self._accepted_result(lease) is not None:
            raise WorkspaceRecoveryError("workspace_activity_already_accepted")
        return await self._restore_latest(principal, lease)

    async def _accepted_result(self, lease: ExecutionLease) -> TransitionReceipt | None:
        try:
            return await asyncio.to_thread(self._transitions.get_accepted_result, lease)
        except LeaseLost:
            raise WorkspaceRecoveryError("workspace_owner_mismatch") from None
        except SQLAlchemyError:
            raise WorkspaceRecoveryError("workspace_recovery_unavailable") from None

    async def accept_candidate(
        self,
        principal: Principal,
        lease: ExecutionLease,
        checkpoint_key: str,
        base: WorkspaceSnapshot,
        candidate: PatchCandidate,
        *,
        result: dict[str, Any],
        target_run_status: RunStatus,
        next_work: NextWork | None = None,
    ) -> CheckpointReceipt:
        """Validate a prepared patch and atomically accept its uploaded archive."""
        self._check_principal(principal, lease)
        if await self._accepted_result(lease) is not None:
            raise WorkspaceRecoveryError("workspace_activity_already_accepted")
        if (
            base.owner_id != principal.account_id
            or base.run_id != lease.run_id
            or base.generation != lease.generation
            or base.attempt_id != lease.attempt_id
        ):
            raise WorkspaceRecoveryError("workspace_checkpoint_lease_mismatch")

        await self._reconcile_prior_allocations(principal, lease)
        latest_artifact_id = await self._latest_authoritative_artifact_id(principal, lease)
        if latest_artifact_id != base.artifact_id:
            raise WorkspaceRecoveryError("workspace_checkpoint_stale")
        authoritative_base = await self._restore_artifact(principal, lease, base.artifact_id)
        await self._validate_workspace_snapshot_identity(base, authoritative_base)

        try:
            verified_candidate = await asyncio.to_thread(
                verify_patch_candidate, authoritative_base.verified, candidate
            )
        except SourceToolError as error:
            raise WorkspaceRecoveryError(error.code) from None

        accepted_result = self._candidate_result(authoritative_base, candidate, result)
        try:
            return await self._checkpoints.commit_checkpoint(
                principal,
                lease,
                checkpoint_key,
                verified_candidate.files,
                result=accepted_result,
                target_run_status=target_run_status,
                next_work=next_work,
            )
        except CheckpointStorageError:
            raise

    async def _restore_latest(
        self, principal: Principal, lease: ExecutionLease
    ) -> WorkspaceSnapshot:
        await self._reconcile_prior_allocations(principal, lease)
        artifact_id = await self._latest_authoritative_artifact_id(principal, lease)
        if artifact_id is None:
            await self._checkpoints.fail_unrecoverable_restore(lease)
            raise WorkspaceRecoveryError("workspace_checkpoint_missing")
        return await self._restore_artifact(principal, lease, artifact_id)

    async def _restore_artifact(
        self, principal: Principal, lease: ExecutionLease, artifact_id: UUID
    ) -> WorkspaceSnapshot:
        try:
            verified = await self._checkpoints.restore_archive(principal, lease, artifact_id)
            source = await asyncio.to_thread(
                snapshot_from_verified_archive,
                verified,
                expected_archive_sha256=verified.archive_sha256,
                expected_tree_sha256=verified.manifest.tree_sha256,
            )
        except CheckpointStorageError as error:
            raise WorkspaceRecoveryError(error.code) from None
        except SourceToolError as error:
            raise WorkspaceRecoveryError(error.code) from None
        return WorkspaceSnapshot(
            owner_id=principal.account_id,
            run_id=lease.run_id,
            artifact_id=artifact_id,
            generation=lease.generation,
            attempt_id=lease.attempt_id,
            verified=verified,
            source=source,
        )

    async def _latest_authoritative_artifact_id(
        self, principal: Principal, lease: ExecutionLease
    ) -> UUID | None:
        try:
            return await asyncio.to_thread(
                self._latest_authoritative_artifact_id_sync, principal, lease
            )
        except LeaseLost:
            raise WorkspaceRecoveryError("workspace_lease_lost") from None
        except SQLAlchemyError:
            raise WorkspaceRecoveryError("workspace_recovery_unavailable") from None

    def _latest_authoritative_artifact_id_sync(
        self, principal: Principal, lease: ExecutionLease
    ) -> UUID | None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, principal)
            lock_current_execution(connection, principal, lease)
            return connection.execute(
                text(
                    "SELECT artifact_id FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND artifact_id IS NOT NULL "
                    "ORDER BY event_sequence DESC LIMIT 1"
                ),
                {"owner_id": lease.owner_id, "run_id": lease.run_id},
            ).scalar_one_or_none()

    async def _reconcile_prior_allocations(
        self, principal: Principal, lease: ExecutionLease
    ) -> None:
        try:
            allocations = await asyncio.to_thread(
                self._allocation_store.list_recovery_allocations, principal, lease
            )
        except LeaseLost:
            raise WorkspaceRecoveryError("workspace_lease_lost") from None
        except SandboxAllocationError:
            raise WorkspaceRecoveryError("workspace_allocation_state_unavailable") from None
        except SQLAlchemyError:
            raise WorkspaceRecoveryError("workspace_allocation_state_unavailable") from None
        if allocations and self._allocation_reconciler is None:
            raise WorkspaceRecoveryError("workspace_prior_allocation_unresolved")

        for allocation in allocations:
            assert self._allocation_reconciler is not None
            try:
                current = await self._allocation_reconciler.observe(
                    principal, allocation.allocation_id
                )
                self._assert_allocation_identity(principal, lease, allocation, current)
                if current.status not in _INACTIVE_ALLOCATION_STATES:
                    current = await self._allocation_reconciler.stop(
                        principal, allocation.allocation_id
                    )
                    self._assert_allocation_identity(principal, lease, allocation, current)
            except WorkspaceRecoveryError:
                raise
            except Exception:
                raise WorkspaceRecoveryError("workspace_prior_allocation_unresolved") from None
            if current.status not in _INACTIVE_ALLOCATION_STATES:
                raise WorkspaceRecoveryError("workspace_prior_allocation_unresolved")

    async def _validate_workspace_snapshot_identity(
        self, supplied: WorkspaceSnapshot, authoritative: WorkspaceSnapshot
    ) -> None:
        try:
            await asyncio.to_thread(
                snapshot_from_verified_archive,
                supplied.verified,
                expected_archive_sha256=authoritative.source.archive_sha256,
                expected_tree_sha256=authoritative.source.tree_sha256,
                expected_manifest=authoritative.source.manifest,
            )
            await asyncio.to_thread(
                verify_source_archive,
                supplied.source.archive,
                expected_archive_sha256=authoritative.source.archive_sha256,
                expected_tree_sha256=authoritative.source.tree_sha256,
                expected_manifest=authoritative.source.manifest,
            )
        except SourceToolError:
            raise WorkspaceRecoveryError("workspace_checkpoint_identity_mismatch") from None

    def _candidate_result(
        self,
        base: WorkspaceSnapshot,
        candidate: PatchCandidate,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(result, dict) or "workspace_candidate" in result:
            raise WorkspaceRecoveryError("workspace_candidate_result_invalid")
        return {
            **result,
            "workspace_candidate": {
                "base_artifact_id": str(base.artifact_id),
                "base_tree_sha256": base.source.tree_sha256,
                "archive_sha256": candidate.source.archive_sha256,
                "tree_sha256": candidate.source.tree_sha256,
                "changed_paths": list(candidate.changed_paths),
                "preimages": [
                    {"path": preimage.path, "sha256": preimage.sha256}
                    for preimage in candidate.preimages
                ],
            },
        }

    @staticmethod
    def _assert_allocation_identity(
        principal: Principal,
        lease: ExecutionLease,
        expected: AllocationReservation,
        observed: AllocationReservation,
    ) -> None:
        if (
            observed.allocation_id != expected.allocation_id
            or observed.owner_id != principal.account_id
            or observed.owner_id != expected.owner_id
            or observed.run_id != lease.run_id
            or observed.run_id != expected.run_id
            or observed.step_id != expected.step_id
            or observed.job_id != expected.job_id
            or observed.generation != expected.generation
            or observed.attempt_id != expected.attempt_id
            or observed.lease_owner != expected.lease_owner
            or observed.provider_name != expected.provider_name
            or observed.max_requested_seconds != expected.max_requested_seconds
            or observed.vcpus != expected.vcpus
            or observed.memory_mb != expected.memory_mb
            or observed.allocation_class != expected.allocation_class
            or expected.provider_session_id is not None
            and observed.provider_session_id != expected.provider_session_id
        ):
            raise WorkspaceRecoveryError("workspace_allocation_identity_mismatch")

    def _check_principal(self, principal: Principal, lease: ExecutionLease) -> None:
        if (
            self._principal.account_id != principal.account_id
            or principal.account_id != lease.owner_id
        ):
            raise WorkspaceRecoveryError("workspace_owner_mismatch")
