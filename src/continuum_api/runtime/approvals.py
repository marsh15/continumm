"""Durable exact-publication manifests and deadline-bound owner decisions."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4

from sqlalchemy import Engine, text

from continuum_api.approvals.manifest import (
    ApprovalManifestError,
    deterministic_commit,
    digest_manifest,
)
from continuum_api.artifacts.archives import (
    ArchiveFile,
    VerifiedArchive,
    verify_archive,
)
from continuum_api.repositories.preflight import (
    RepositoryAccessProof,
)
from continuum_api.runtime.cancellation import _terminalize_locked_run
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, lock_current_execution
from continuum_api.runtime.persistence import (
    EventActor,
    Principal,
    ReplayPolicy,
    RunStatus,
    RuntimeRepository,
)
from continuum_api.runtime.sandbox_allocations import AllocationClass
from continuum_api.runtime.transitions import (
    NextWork,
    TransitionReceipt,
    TransitionService,
)
from continuum_api.validation.contracts import (
    PhaseOutcome,
    ValidationClassification,
    ValidationContractError,
    ValidationEvidence,
)
from continuum_api.validation.managed_hidden_checks import (
    ManagedHiddenCheckError,
    ManagedHiddenCheckEvidence,
)

_MAX_APPROVAL_LIFETIME = timedelta(hours=48)
_POLICY_VERSION = "t27-approval-v1"
_BRANCH_PATTERN = re.compile(r"^continumm/run/[0-9a-f-]{36}$")
_PASSING_CLASSIFICATIONS = {
    ValidationClassification.PASSED,
    ValidationClassification.PASSED_WITH_BASELINE_FAILURE,
}
_PROFILE_COMMANDS = {
    "python-pytest": ("python-pytest-v1", "pytest"),
    "node-npm": ("node-npm-test-v1", "npm_test"),
}


class ApprovalError(RuntimeError):
    """Sanitized approval lifecycle rejection."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class ApprovalNotFound(ApprovalError):
    def __init__(self) -> None:
        super().__init__("approval_not_found")


class ApprovalConflict(ApprovalError):
    def __init__(self, code: str = "approval_conflict") -> None:
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class RepositoryPublicationFacts:
    repository_id: int
    github_user_id: int
    owner: str
    name: str
    default_branch: str
    base_commit_sha: str
    access_proof: RepositoryAccessProof


class PublicationFactsProvider(Protocol):
    async def current(
        self, repository_id: int, github_user_id: int
    ) -> RepositoryPublicationFacts: ...


class PendingPublicationFactsProvider:
    async def current(self, repository_id: int, github_user_id: int) -> RepositoryPublicationFacts:
        del repository_id, github_user_id
        raise ApprovalError("repository_authorization_pending")


class ApprovalObjectStorage(Protocol):
    async def check_private_bucket(self) -> None: ...

    async def download(self, object_key: str) -> bytes: ...


@dataclass(frozen=True, slots=True)
class ApprovalReceipt:
    approval_id: UUID
    run_id: UUID
    revision: int
    manifest_sha256: bytes
    expires_at: datetime
    status: str
    adopted: bool


@dataclass(frozen=True, slots=True)
class ApprovalDispatchGrant:
    approval_id: UUID
    run_id: UUID
    manifest_sha256: bytes
    revision: int
    authorized_at: datetime
    expires_at: datetime
    grant_id: UUID
    operation: str
    generation: int
    attempt_id: UUID


@dataclass(frozen=True, slots=True)
class ApprovalPublicationSnapshot:
    """Owner-scoped frozen publication inputs, readable for reconciliation."""

    approval_id: UUID
    owner_id: UUID
    run_id: UUID
    repository_id: int
    github_user_id: int
    revision: int
    manifest_sha256: bytes
    manifest: dict[str, Any]
    status: str
    expires_at: datetime
    candidate_object_key: str | None


@dataclass(frozen=True, slots=True)
class _AcceptedExecutionIdentity:
    run_id: UUID
    step_id: UUID
    job_id: UUID
    attempt_id: UUID
    generation: int
    lease_owner: str


class ApprovalService:
    """Persist T20 evidence and freeze/decide exact owner-scoped approvals."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        transitions: TransitionService,
        storage: ApprovalObjectStorage | None,
        *,
        publication_facts: PublicationFactsProvider | None = None,
        validation_allocation_class: AllocationClass = "public",
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._transitions = transitions
        self._storage = storage
        self._publication_facts = publication_facts or PendingPublicationFactsProvider()
        if validation_allocation_class not in {"public", "operator"}:
            raise ValueError("validation_allocation_class_invalid")
        self._validation_allocation_class = validation_allocation_class

    def publication_snapshot(
        self, approval_id: UUID, manifest_sha256: str
    ) -> ApprovalPublicationSnapshot:
        """Read exact frozen publication inputs without authorizing a mutation.

        This accessor intentionally remains available after expiry or cancellation so
        callers can reconcile an already-dispatched remote effect. Every mutation
        still needs a fresh ``authorize_dispatch`` grant.
        """
        if not re.fullmatch(r"[0-9a-f]{64}", manifest_sha256):
            raise ApprovalConflict("approval_digest_invalid")
        expected_digest = bytes.fromhex(manifest_sha256)
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = (
                connection.execute(
                    text(
                        "SELECT approval.owner_id, approval.run_id, approval.manifest_sha256, "
                        "approval.manifest, "
                        "approval.status, approval.revision, approval.expires_at, "
                        "run.github_repository_id, users.github_user_id, upload.object_key "
                        "FROM runtime_private.approvals AS approval "
                        "JOIN runtime_private.runs AS run USING (owner_id, run_id) "
                        "JOIN runtime_private.users AS users ON users.user_id=run.owner_id "
                        "LEFT JOIN runtime_private.artifact_uploads AS upload "
                        "ON upload.owner_id=approval.owner_id AND upload.run_id=approval.run_id "
                        "AND upload.upload_id="
                        "(approval.manifest->'candidate'->>'checkpoint_id')::uuid "
                        "WHERE approval.owner_id=:owner AND approval.approval_id=:approval"
                    ),
                    {"owner": self._principal.account_id, "approval": approval_id},
                )
                .mappings()
                .one_or_none()
            )
        if row is None or bytes(row["manifest_sha256"]) != expected_digest:
            raise ApprovalNotFound()
        manifest = row["manifest"]
        try:
            _encoded, actual_digest = digest_manifest(manifest)
        except ApprovalManifestError:
            raise ApprovalConflict("approval_manifest_corrupt") from None
        if actual_digest != expected_digest:
            raise ApprovalConflict("approval_manifest_corrupt")
        if (
            manifest.get("run_id") != str(row["run_id"])
            or manifest.get("revision") != row["revision"]
            or not isinstance(manifest.get("repository"), dict)
            or manifest.get("repository", {}).get("id") != row["github_repository_id"]
        ):
            raise ApprovalConflict("approval_manifest_corrupt")
        return ApprovalPublicationSnapshot(
            approval_id=approval_id,
            owner_id=row["owner_id"],
            run_id=row["run_id"],
            repository_id=row["github_repository_id"],
            github_user_id=row["github_user_id"],
            revision=row["revision"],
            manifest_sha256=expected_digest,
            manifest=manifest,
            status=row["status"],
            expires_at=row["expires_at"],
            candidate_object_key=row["object_key"],
        )

    async def assert_current_repository_write_access(
        self, repository_id: int, github_user_id: int
    ) -> RepositoryPublicationFacts:
        """Freshly prove owner write rights for trusted cross-channel commands."""
        try:
            facts = await self._publication_facts.current(repository_id, github_user_id)
        except ApprovalError:
            raise
        except Exception:
            raise ApprovalError("repository_authorization_pending") from None
        _validate_publication_facts(facts, repository_id, github_user_id)
        return facts

    async def download_publication_candidate(
        self, approval_id: UUID, manifest_sha256: str
    ) -> bytes:
        """Load the archive referenced by an owner-scoped publication snapshot."""
        snapshot = self.publication_snapshot(approval_id, manifest_sha256)
        if self._storage is None or snapshot.candidate_object_key is None:
            raise ApprovalError("approval_candidate_unavailable")
        try:
            return await self._storage.download(snapshot.candidate_object_key)
        except Exception:
            raise ApprovalError("approval_candidate_unavailable") from None

    def record_validation(
        self,
        lease: ExecutionLease,
        candidate_artifact_id: UUID,
        evidence: ValidationEvidence,
        *,
        hidden_check_evidence: ManagedHiddenCheckEvidence | None = None,
    ) -> TransitionReceipt:
        """Commit typed T20 evidence through T08 while pinning the candidate."""
        if lease.owner_id != self._principal.account_id:
            raise ApprovalError("approval_owner_mismatch")
        try:
            evidence = ValidationEvidence.model_validate(evidence, strict=True)
        except (ValidationContractError, TypeError, ValueError):
            raise ApprovalError("approval_validation_evidence_invalid") from None
        _validate_pass_evidence(evidence)
        hidden_evidence: ManagedHiddenCheckEvidence | None = None
        if hidden_check_evidence is not None:
            if self._validation_allocation_class != "operator":
                raise ApprovalConflict("approval_hidden_evidence_not_allowed")
            try:
                hidden_evidence = ManagedHiddenCheckEvidence.model_validate_json(
                    _canonical_json(hidden_check_evidence.model_dump(mode="json")), strict=True
                )
            except (ManagedHiddenCheckError, TypeError, ValueError):
                raise ApprovalConflict("approval_hidden_evidence_invalid") from None
            _validate_hidden_binding(hidden_evidence, evidence, lease)
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            try:
                lock_current_execution(connection, self._principal, lease)
            except LeaseLost:
                raise ApprovalConflict("approval_execution_stale") from None
            run = (
                connection.execute(
                    text(
                        "SELECT status, revision, github_repository_id, deadline_at "
                        "FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
                    ),
                    {"owner": self._principal.account_id, "run": lease.run_id},
                )
                .mappings()
                .one_or_none()
            )
            if run is None:
                raise ApprovalNotFound()
            if run["status"] != RunStatus.RUNNING.value:
                raise ApprovalConflict("approval_run_not_running")
            step = (
                connection.execute(
                    text(
                        "SELECT activity, revision FROM runtime_private.steps "
                        "WHERE owner_id=:owner "
                        "AND run_id=:run AND step_id=:step"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "run": lease.run_id,
                        "step": lease.step_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if step is None or step["activity"] != "candidate_validation":
                raise ApprovalConflict("approval_validation_activity_required")
            if step["revision"] != run["revision"]:
                raise ApprovalConflict("approval_execution_stale")
            artifact = (
                connection.execute(
                    text(
                        "SELECT github_repository_id, archive_sha256, tree_sha256, status, "
                        "content_expires_at FROM runtime_private.artifact_uploads "
                        "WHERE owner_id=:owner AND run_id=:run AND upload_id=:artifact"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "run": lease.run_id,
                        "artifact": candidate_artifact_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            latest_artifact = connection.execute(
                text(
                    "SELECT accepted.artifact_id FROM runtime_private.accepted_results AS accepted "
                    "JOIN runtime_private.steps AS step ON step.owner_id=accepted.owner_id "
                    "AND step.run_id=accepted.run_id AND step.step_id=accepted.step_id "
                    "WHERE accepted.owner_id=:owner AND accepted.run_id=:run "
                    "AND accepted.artifact_id IS NOT NULL AND step.revision=:revision "
                    "AND accepted.event_sequence=(SELECT MAX(event_sequence) "
                    "FROM runtime_private.accepted_results WHERE owner_id=:owner AND run_id=:run "
                    "AND artifact_id IS NOT NULL) "
                    "ORDER BY accepted.event_sequence DESC LIMIT 1"
                ),
                {
                    "owner": self._principal.account_id,
                    "run": lease.run_id,
                    "revision": run["revision"],
                },
            ).scalar_one_or_none()
            baseline = (
                connection.execute(
                    text(
                        "SELECT archive_sha256, tree_sha256 FROM runtime_private.artifact_uploads "
                        "WHERE owner_id=:owner AND run_id=:run AND upload_id=:artifact "
                        "AND status='committed'"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "run": lease.run_id,
                        "artifact": evidence.baseline_artifact_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if (
                artifact is None
                or artifact["status"] != "committed"
                or latest_artifact != candidate_artifact_id
                or evidence.candidate_archive_sha256 != bytes(artifact["archive_sha256"]).hex()
                or evidence.candidate_source_tree_sha256 != bytes(artifact["tree_sha256"]).hex()
                or evidence.candidate.repository_id != run["github_repository_id"]
                or evidence.baseline.repository_id != run["github_repository_id"]
                or evidence.candidate.base_commit_sha != evidence.baseline.base_commit_sha
                or evidence.candidate.profile_id != evidence.baseline.profile_id
                or evidence.candidate.profile_version != evidence.baseline.profile_version
                or baseline is None
                or evidence.baseline.baseline_archive_sha256
                != bytes(baseline["archive_sha256"]).hex()
                or evidence.baseline.baseline_archive_tree_sha256
                != bytes(baseline["tree_sha256"]).hex()
            ):
                raise ApprovalConflict("approval_candidate_evidence_mismatch")
            for phase in (evidence.baseline, evidence.candidate):
                assert phase.allocation_id is not None
                allocation = (
                    connection.execute(
                        text(
                            "SELECT status, allocation_class, logical_key FROM "
                            "runtime_private.sandbox_allocations WHERE owner_id=:owner "
                            "AND run_id=:run AND allocation_id=:allocation"
                        ),
                        {
                            "owner": self._principal.account_id,
                            "run": lease.run_id,
                            "allocation": phase.allocation_id,
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
                expected_key = f"t20:v1:{evidence.candidate_lineage_sha256}:{phase.phase.value}"
                if (
                    allocation is None
                    or allocation["status"] not in {"stopped", "destroyed"}
                    or allocation["allocation_class"] != self._validation_allocation_class
                    or allocation["logical_key"] != expected_key
                    or phase.allocation_status != allocation["status"]
                ):
                    raise ApprovalConflict("approval_validation_allocation_mismatch")
            if hidden_evidence is not None:
                _validate_hidden_allocation(
                    connection,
                    owner_id=self._principal.account_id,
                    lease=lease,
                    evidence=hidden_evidence,
                )
            database_now: datetime = connection.execute(
                text("SELECT clock_timestamp()")
            ).scalar_one()
            if run["deadline_at"] is None or run["deadline_at"] <= database_now:
                raise ApprovalConflict("approval_run_expired")
            revision = run["revision"]

        payload = {
            "approval_validation": evidence.model_dump(mode="json"),
            "candidate_artifact_id": str(candidate_artifact_id),
            "run_revision": revision,
        }
        if hidden_evidence is not None:
            payload["hidden_check_evidence"] = hidden_evidence.model_dump(mode="json")
        return self._transitions.commit_result(
            lease=lease,
            result=payload,
            target_run_status=RunStatus.RUNNING,
            next_work=NextWork(
                logical_key=f"t27:freeze:{candidate_artifact_id.hex}:r{revision}",
                activity="approval_freeze",
                revision=revision,
                replay_policy=ReplayPolicy.REPLAY_SAFE,
            ),
        )

    async def freeze(self, lease: ExecutionLease) -> ApprovalReceipt:
        """Freeze a manifest from the latest accepted evidence and candidate bytes."""
        if lease.owner_id != self._principal.account_id:
            raise ApprovalError("approval_owner_mismatch")
        adopted = self._adopt_prepared_manifest(lease)
        if adopted is not None:
            return adopted
        context = self._freeze_context(lease.run_id)
        if self._storage is None:
            raise ApprovalError("approval_candidate_unavailable")
        read_facts = await self._publication_facts.current(
            context["repository_id"], context["github_user_id"]
        )
        _validate_publication_facts(read_facts, context["repository_id"], context["github_user_id"])
        _validate_candidate_base(context, read_facts)
        try:
            await self._storage.check_private_bucket()
            candidate_bytes = await self._storage.download(context["candidate_object_key"])
            baseline_bytes = await self._storage.download(context["baseline_object_key"])
            candidate = await asyncio.to_thread(
                verify_archive,
                candidate_bytes,
                expected_archive_sha256=context["candidate_archive_sha256"],
                expected_tree_sha256=context["candidate_source_tree_sha256"],
            )
            baseline = await asyncio.to_thread(
                verify_archive,
                baseline_bytes,
                expected_archive_sha256=context["baseline_archive_sha256"],
                expected_tree_sha256=context["baseline_source_tree_sha256"],
            )
        except Exception as error:
            if isinstance(error, ApprovalError):
                raise
            raise ApprovalError("approval_candidate_restore_failed") from None
        facts = await self._publication_facts.current(
            context["repository_id"], context["github_user_id"]
        )
        _validate_publication_facts(facts, context["repository_id"], context["github_user_id"])
        _validate_candidate_base(context, facts)
        if (
            facts.default_branch != read_facts.default_branch
            or facts.base_commit_sha != read_facts.base_commit_sha
        ):
            raise ApprovalConflict("approval_base_branch_moved")
        manifest, encoded, digest = _build_manifest(context, facts, candidate, baseline)
        receipt = self._persist_manifest(
            lease=lease,
            run_id=lease.run_id,
            revision=context["revision"],
            candidate_artifact_id=context["candidate_artifact_id"],
            evidence_result_id=context["evidence_result_id"],
            manifest=manifest,
            encoded=encoded,
            digest=digest,
        )
        try:
            self._transitions.commit_result(
                lease=lease,
                result={"approval_manifest": manifest, "manifest_sha256": digest.hex()},
                target_run_status=RunStatus.WAITING_APPROVAL,
            )
        except Exception:
            raise ApprovalError("approval_manifest_transition_failed") from None
        return receipt

    def _adopt_prepared_manifest(self, lease: ExecutionLease) -> ApprovalReceipt | None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = (
                connection.execute(
                    text(
                        "SELECT approval.approval_id, approval.revision, approval.manifest, "
                        "approval.manifest_sha256, approval.expires_at, approval.status, "
                        "run.status AS run_status, run.revision AS run_revision, "
                        "clock_timestamp() AS database_now "
                        "FROM runtime_private.approvals AS approval "
                        "JOIN runtime_private.runs AS run "
                        "USING (owner_id,run_id) WHERE approval.owner_id=:owner "
                        "AND approval.run_id=:run ORDER BY approval.revision DESC LIMIT 1"
                    ),
                    {"owner": self._principal.account_id, "run": lease.run_id},
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        if row["status"] != "pending" or row["revision"] != row["run_revision"]:
            return None
        if row["expires_at"] <= row["database_now"]:
            self._expire_one(row["approval_id"])
            raise ApprovalConflict("approval_expired")
        manifest = row["manifest"]
        _encoded, digest = digest_manifest(manifest)
        if digest != row["manifest_sha256"]:
            raise ApprovalConflict("approval_manifest_corrupt")
        if row["run_status"] == RunStatus.WAITING_APPROVAL.value:
            return ApprovalReceipt(
                row["approval_id"],
                lease.run_id,
                row["revision"],
                digest,
                row["expires_at"],
                row["status"],
                True,
            )
        if row["run_status"] != RunStatus.RUNNING.value:
            raise ApprovalConflict("approval_stale")
        try:
            self._transitions.commit_result(
                lease=lease,
                result={"approval_manifest": manifest, "manifest_sha256": digest.hex()},
                target_run_status=RunStatus.WAITING_APPROVAL,
            )
        except Exception:
            raise ApprovalError("approval_manifest_transition_failed") from None
        return ApprovalReceipt(
            row["approval_id"],
            lease.run_id,
            row["revision"],
            digest,
            row["expires_at"],
            row["status"],
            True,
        )

    def _freeze_context(self, run_id: UUID) -> dict[str, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            run = (
                connection.execute(
                    text(
                        "SELECT run_id, owner_id, status, revision, created_at, deadline_at, "
                        "github_repository_id, model_task_context FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
                    ),
                    {"owner": self._principal.account_id, "run": run_id},
                )
                .mappings()
                .one_or_none()
            )
            if run is None:
                raise ApprovalNotFound()
            if run["status"] != RunStatus.RUNNING.value:
                raise ApprovalConflict("approval_run_not_running")
            if run["github_repository_id"] is None or run["deadline_at"] is None:
                raise ApprovalConflict("approval_run_incomplete")
            evidence = (
                connection.execute(
                    text(
                        "SELECT accepted.result_id, accepted.result_sha256, accepted.payload, "
                        "accepted.step_id, accepted.job_id, accepted.generation, "
                        "accepted.attempt_id, accepted.lease_owner "
                        "FROM runtime_private.accepted_results AS accepted "
                        "JOIN runtime_private.steps AS step ON step.owner_id=accepted.owner_id "
                        "AND step.run_id=accepted.run_id AND step.step_id=accepted.step_id "
                        "WHERE accepted.owner_id=:owner AND accepted.run_id=:run "
                        "AND step.activity='candidate_validation' "
                        "AND accepted.payload ? 'approval_validation' "
                        "ORDER BY accepted.event_sequence DESC LIMIT 1"
                    ),
                    {"owner": self._principal.account_id, "run": run_id},
                )
                .mappings()
                .one_or_none()
            )
            if evidence is None:
                raise ApprovalConflict("approval_validation_not_persisted")
            if hashlib.sha256(_canonical_json(evidence["payload"])).digest() != bytes(
                evidence["result_sha256"]
            ):
                raise ApprovalConflict("approval_validation_result_hash_mismatch")
            try:
                validated = ValidationEvidence.model_validate_json(
                    _canonical_json(evidence["payload"]["approval_validation"]),
                    strict=True,
                )
                candidate_artifact_id = UUID(evidence["payload"]["candidate_artifact_id"])
            except (ValidationContractError, TypeError, ValueError, KeyError):
                raise ApprovalConflict("approval_validation_not_persisted") from None
            if (
                evidence["payload"].get("run_revision") != run["revision"]
                or validated.classification not in _PASSING_CLASSIFICATIONS
                or validated.candidate.outcome is not PhaseOutcome.PASSED
            ):
                raise ApprovalConflict("approval_validation_stale")
            _validate_pass_evidence(validated)
            hidden_evidence: ManagedHiddenCheckEvidence | None = None
            if "hidden_check_evidence" in evidence["payload"]:
                if self._validation_allocation_class != "operator":
                    raise ApprovalConflict("approval_hidden_evidence_not_allowed")
                try:
                    hidden_evidence = ManagedHiddenCheckEvidence.model_validate_json(
                        _canonical_json(evidence["payload"]["hidden_check_evidence"]), strict=True
                    )
                except (ManagedHiddenCheckError, TypeError, ValueError):
                    raise ApprovalConflict("approval_hidden_evidence_invalid") from None
                _validate_hidden_binding(
                    hidden_evidence,
                    validated,
                    _AcceptedExecutionIdentity(
                        run_id=run_id,
                        step_id=evidence["step_id"],
                        job_id=evidence["job_id"],
                        attempt_id=evidence["attempt_id"],
                        generation=evidence["generation"],
                        lease_owner=evidence["lease_owner"],
                    ),
                )
            candidate_row = (
                connection.execute(
                    text(
                        "SELECT upload.object_key, upload.archive_sha256, upload.tree_sha256, "
                        "upload.content_expires_at, upload.status, upload.github_repository_id "
                        "FROM runtime_private.artifact_uploads AS upload "
                        "WHERE upload.owner_id=:owner AND upload.run_id=:run "
                        "AND upload.upload_id=:artifact"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "run": run_id,
                        "artifact": candidate_artifact_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            baseline_row = (
                connection.execute(
                    text(
                        "SELECT object_key, archive_sha256, tree_sha256, "
                        "content_expires_at, status "
                        "FROM runtime_private.artifact_uploads "
                        "WHERE owner_id=:owner AND run_id=:run "
                        "AND upload_id=:artifact"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "run": run_id,
                        "artifact": validated.baseline_artifact_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            latest_artifact_id = connection.execute(
                text(
                    "SELECT artifact_id FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL "
                    "ORDER BY event_sequence DESC LIMIT 1"
                ),
                {"owner": self._principal.account_id, "run": run_id},
            ).scalar_one_or_none()
            github_user_id: int = connection.execute(
                text("SELECT github_user_id FROM runtime_private.users WHERE user_id=:owner"),
                {"owner": self._principal.account_id},
            ).scalar_one()
            now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if (
                candidate_row is None
                or baseline_row is None
                or candidate_row["status"] != "committed"
                or baseline_row["status"] != "committed"
                or latest_artifact_id != candidate_artifact_id
                or candidate_row["github_repository_id"] != run["github_repository_id"]
                or bytes(candidate_row["archive_sha256"]).hex()
                != validated.candidate_archive_sha256
                or bytes(candidate_row["tree_sha256"]).hex()
                != validated.candidate_source_tree_sha256
                or bytes(baseline_row["archive_sha256"]).hex()
                != validated.baseline.baseline_archive_sha256
                or bytes(baseline_row["tree_sha256"]).hex()
                != validated.baseline.baseline_archive_tree_sha256
                or min(candidate_row["content_expires_at"], baseline_row["content_expires_at"])
                <= now
                or run["deadline_at"] <= now
            ):
                raise ApprovalConflict("approval_candidate_stale_or_expired")
            _verify_candidate_checkpoint_chain(
                connection,
                owner_id=self._principal.account_id,
                run_id=run_id,
                repository_id=run["github_repository_id"],
                candidate_artifact_id=candidate_artifact_id,
                baseline_artifact_id=validated.baseline_artifact_id,
            )
            for phase in (validated.baseline, validated.candidate):
                assert phase.allocation_id is not None
                allocation = (
                    connection.execute(
                        text(
                            "SELECT status, allocation_class, logical_key "
                            "FROM runtime_private.sandbox_allocations WHERE owner_id=:owner "
                            "AND run_id=:run AND allocation_id=:allocation"
                        ),
                        {
                            "owner": self._principal.account_id,
                            "run": run_id,
                            "allocation": phase.allocation_id,
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    allocation is None
                    or allocation["status"] not in {"stopped", "destroyed"}
                    or allocation["allocation_class"] != self._validation_allocation_class
                    or allocation["logical_key"]
                    != f"t20:v1:{validated.candidate_lineage_sha256}:{phase.phase.value}"
                    or allocation["status"] != phase.allocation_status
                ):
                    raise ApprovalConflict("approval_validation_allocation_mismatch")
            if hidden_evidence is not None:
                _validate_hidden_allocation(
                    connection,
                    owner_id=self._principal.account_id,
                    lease=_AcceptedExecutionIdentity(
                        run_id=run_id,
                        step_id=evidence["step_id"],
                        job_id=evidence["job_id"],
                        attempt_id=evidence["attempt_id"],
                        generation=evidence["generation"],
                        lease_owner=evidence["lease_owner"],
                    ),
                    evidence=hidden_evidence,
                )
            return {
                "run_id": run_id,
                "owner_id": self._principal.account_id,
                "github_user_id": github_user_id,
                "revision": run["revision"],
                "created_at": run["created_at"],
                "deadline_at": run["deadline_at"],
                "task_context": run["model_task_context"] or {},
                "repository_id": run["github_repository_id"],
                "candidate_artifact_id": candidate_artifact_id,
                "candidate_object_key": candidate_row["object_key"],
                "candidate_archive_sha256": bytes(candidate_row["archive_sha256"]).hex(),
                "candidate_source_tree_sha256": bytes(candidate_row["tree_sha256"]).hex(),
                "candidate_content_expires_at": candidate_row["content_expires_at"],
                "baseline_object_key": baseline_row["object_key"],
                "baseline_archive_sha256": bytes(baseline_row["archive_sha256"]).hex(),
                "baseline_source_tree_sha256": bytes(baseline_row["tree_sha256"]).hex(),
                "baseline_content_expires_at": baseline_row["content_expires_at"],
                "evidence_result_id": evidence["result_id"],
                "evidence_result_sha256": bytes(evidence["result_sha256"]).hex(),
                "evidence": validated,
                "database_now": now,
            }

    def _persist_manifest(
        self,
        *,
        lease: ExecutionLease,
        run_id: UUID,
        revision: int,
        candidate_artifact_id: UUID,
        evidence_result_id: UUID,
        manifest: dict[str, Any],
        encoded: bytes,
        digest: bytes,
    ) -> ApprovalReceipt:
        approval_id = uuid4()
        expires_at = datetime.fromisoformat(manifest["approval_expires_at"])
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            try:
                locked = lock_current_execution(connection, self._principal, lease)
            except LeaseLost:
                raise ApprovalConflict("approval_execution_stale") from None
            run = (
                connection.execute(
                    text(
                        "SELECT status, revision, deadline_at FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
                    ),
                    {"owner": self._principal.account_id, "run": run_id},
                )
                .mappings()
                .one_or_none()
            )
            if run is None:
                raise ApprovalNotFound()
            existing = (
                connection.execute(
                    text(
                        "SELECT approval_id, revision, manifest, manifest_sha256, "
                        "expires_at, status "
                        "FROM runtime_private.approvals WHERE owner_id=:owner AND run_id=:run "
                        "AND revision=:revision FOR UPDATE"
                    ),
                    {"owner": self._principal.account_id, "run": run_id, "revision": revision},
                )
                .mappings()
                .one_or_none()
            )
            now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if existing is not None:
                if existing["manifest_sha256"] != digest or existing["manifest"] != manifest:
                    raise ApprovalConflict("approval_manifest_conflict")
                return ApprovalReceipt(
                    existing["approval_id"],
                    run_id,
                    revision,
                    digest,
                    existing["expires_at"],
                    existing["status"],
                    True,
                )
            if (
                run["status"] != RunStatus.RUNNING.value
                or run["revision"] != revision
                or run["deadline_at"] is None
                or expires_at <= now
                or expires_at > run["deadline_at"]
                or locked.run_deadline_at is None
                or locked.run_deadline_at <= now
            ):
                raise ApprovalConflict("approval_manifest_stale_or_expired")
            if hashlib.sha256(encoded).digest() != digest:
                raise ApprovalConflict("approval_manifest_digest_invalid")
            latest_candidate = connection.execute(
                text(
                    "SELECT artifact_id FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL "
                    "ORDER BY event_sequence DESC LIMIT 1"
                ),
                {"owner": self._principal.account_id, "run": run_id},
            ).scalar_one_or_none()
            evidence_digest = connection.execute(
                text(
                    "SELECT accepted.result_sha256 "
                    "FROM runtime_private.accepted_results AS accepted "
                    "JOIN runtime_private.steps AS step ON step.owner_id=accepted.owner_id "
                    "AND step.run_id=accepted.run_id AND step.step_id=accepted.step_id "
                    "WHERE accepted.owner_id=:owner AND accepted.run_id=:run "
                    "AND accepted.result_id=:evidence AND step.activity='candidate_validation' "
                    "AND accepted.payload ? 'approval_validation'"
                ),
                {
                    "owner": self._principal.account_id,
                    "run": run_id,
                    "evidence": evidence_result_id,
                },
            ).scalar_one_or_none()
            artifact = (
                connection.execute(
                    text(
                        "SELECT status, archive_sha256, content_expires_at "
                        "FROM runtime_private.artifact_uploads WHERE owner_id=:owner "
                        "AND run_id=:run AND upload_id=:artifact FOR UPDATE"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "run": run_id,
                        "artifact": candidate_artifact_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if (
                latest_candidate != candidate_artifact_id
                or evidence_digest is None
                or bytes(evidence_digest).hex() != manifest["validation"]["evidence_result_sha256"]
                or artifact is None
                or artifact["status"] != "committed"
                or bytes(artifact["archive_sha256"]).hex()
                != manifest["candidate"]["archive_sha256"]
                or artifact["content_expires_at"] <= now
            ):
                raise ApprovalConflict("approval_candidate_stale")
            connection.execute(
                text(
                    "INSERT INTO runtime_private.approvals "
                    "(approval_id, owner_id, run_id, revision, candidate_artifact_id, "
                    "evidence_result_id, manifest, manifest_sha256, created_at, expires_at) "
                    "VALUES (:id, :owner, :run, :revision, :artifact, :evidence, "
                    "CAST(:manifest AS jsonb), :digest, :created, :expires)"
                ),
                {
                    "id": approval_id,
                    "owner": self._principal.account_id,
                    "run": run_id,
                    "revision": revision,
                    "artifact": candidate_artifact_id,
                    "evidence": evidence_result_id,
                    "manifest": encoded.decode("utf-8"),
                    "digest": digest,
                    "created": datetime.fromisoformat(manifest["approval_created_at"]),
                    "expires": expires_at,
                },
            )
            return ApprovalReceipt(
                approval_id, run_id, revision, digest, expires_at, "pending", False
            )

    async def decide(
        self,
        *,
        approval_id: UUID,
        manifest_sha256: str,
        decision: Literal["approve", "reject", "changes"],
        feedback: str | None = None,
        idempotency_key: str | None = None,
    ) -> ApprovalReceipt:
        """Accept one owner decision after a fresh current-repository rights proof."""
        if not re.fullmatch(r"[0-9a-f]{64}", manifest_sha256):
            raise ApprovalConflict("approval_digest_invalid")
        if decision == "changes":
            feedback = _validate_revision_feedback_text(feedback)
        elif feedback is not None:
            raise ApprovalConflict("approval_feedback_unexpected")
        if (
            idempotency_key is not None
            and re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", idempotency_key) is None
        ):
            raise ApprovalConflict("approval_idempotency_key_invalid")
        expected_digest = bytes.fromhex(manifest_sha256)
        request_digest = hashlib.sha256(
            _canonical_json(
                {
                    "approval_id": str(approval_id),
                    "decision": decision,
                    "feedback": feedback,
                    "manifest_sha256": manifest_sha256,
                }
            )
        ).digest()
        if idempotency_key is not None:
            replay = self._read_decision_command(idempotency_key, request_digest)
            if replay is not None:
                return replay
        context = self._approval_context(approval_id, expected_digest)
        status = {"approve": "approved", "reject": "rejected", "changes": "changes"}[decision]
        if context["status"] != "pending":
            if context["status"] == status:
                if status == "changes" and context.get("decided_feedback") != feedback:
                    raise ApprovalConflict("approval_decision_conflict")
                if idempotency_key is not None:
                    return self._decide_locked(
                        approval_id,
                        expected_digest,
                        status,
                        feedback=feedback,
                        idempotency_key=idempotency_key,
                        request_digest=request_digest,
                    )
                return ApprovalReceipt(
                    approval_id,
                    context["run_id"],
                    context["revision"],
                    expected_digest,
                    context["expires_at"],
                    status,
                    True,
                )
            if context["status"] == "expired":
                raise ApprovalConflict("approval_expired")
            raise ApprovalConflict("approval_decision_conflict")
        facts = await self._publication_facts.current(
            context["repository_id"], context["github_user_id"]
        )
        _validate_publication_facts(facts, context["repository_id"], context["github_user_id"])
        _validate_approved_base(context, facts)
        receipt = self._decide_locked(
            approval_id,
            expected_digest,
            status,
            feedback=feedback,
            idempotency_key=idempotency_key,
            request_digest=request_digest,
        )
        if receipt.status == "expired":
            raise ApprovalConflict("approval_expired")
        return receipt

    def expire_due(self, *, limit: int = 50) -> int:
        """Expire due owner approvals, terminalizing waiting runs atomically."""
        if not 1 <= limit <= 100:
            raise ValueError("approval_expiry_limit_invalid")
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            approval_ids: tuple[UUID, ...] = tuple(
                connection.execute(
                    text(
                        "SELECT approval_id FROM runtime_private.approvals "
                        "WHERE owner_id=:owner AND status IN ('pending','approved') "
                        "AND expires_at<=clock_timestamp() "
                        "ORDER BY expires_at, approval_id LIMIT :limit"
                    ),
                    {"owner": self._principal.account_id, "limit": limit},
                ).scalars()
            )
        expired = 0
        for approval_id in approval_ids:
            if self._expire_one(approval_id):
                expired += 1
        return expired

    def _expire_one(self, approval_id: UUID) -> bool:
        with self._engine.begin() as connection:
            repository = RuntimeRepository(connection, self._principal)
            run_id = connection.execute(
                text(
                    "SELECT run_id FROM runtime_private.approvals WHERE owner_id=:owner "
                    "AND approval_id=:approval"
                ),
                {"owner": self._principal.account_id, "approval": approval_id},
            ).scalar_one_or_none()
            if run_id is None:
                return False
            run = (
                connection.execute(
                    text(
                        "SELECT status, revision FROM runtime_private.runs WHERE owner_id=:owner "
                        "AND run_id=:run FOR UPDATE"
                    ),
                    {"owner": self._principal.account_id, "run": run_id},
                )
                .mappings()
                .one_or_none()
            )
            approval = (
                connection.execute(
                    text(
                        "SELECT status, manifest_sha256, revision, expires_at "
                        "FROM runtime_private.approvals WHERE owner_id=:owner "
                        "AND approval_id=:approval FOR UPDATE"
                    ),
                    {"owner": self._principal.account_id, "approval": approval_id},
                )
                .mappings()
                .one_or_none()
            )
            now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if (
                approval is None
                or approval["status"] not in {"pending", "approved"}
                or approval["expires_at"] > now
            ):
                return False
            connection.execute(
                text(
                    "UPDATE runtime_private.approvals SET status='expired', "
                    "decision_actor_id=NULL, decided_at=:now "
                    "WHERE owner_id=:owner AND approval_id=:approval"
                ),
                {"now": now, "owner": self._principal.account_id, "approval": approval_id},
            )
            if (
                run is not None
                and run["revision"] == approval["revision"]
                and run["status"]
                in {
                    RunStatus.WAITING_APPROVAL.value,
                    RunStatus.RUNNING.value,
                }
            ):
                _terminalize_locked_run(
                    connection,
                    repository,
                    owner_id=self._principal.account_id,
                    run_id=run_id,
                    target_status=RunStatus.EXPIRED,
                    event_type="run.expired",
                    actor_type=EventActor.SYSTEM,
                    actor_id=None,
                    attempt_failure_class=None,
                )
                _erase_run_credentials(connection, self._principal.account_id, run_id)
            sequence = connection.execute(
                text(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_private.events "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": self._principal.account_id, "run": run_id},
            ).scalar_one()
            repository.append_event(
                run_id,
                sequence,
                "approval.expired",
                EventActor.SYSTEM,
                payload={
                    "approval_id": str(approval_id),
                    "manifest_sha256": bytes(approval["manifest_sha256"]).hex(),
                    "revision": approval["revision"],
                },
            )
            return True

    def _approval_context(self, approval_id: UUID, expected_digest: bytes) -> dict[str, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = (
                connection.execute(
                    text(
                        "SELECT approval.run_id, run.github_repository_id, users.github_user_id, "
                        "approval.manifest_sha256, approval.manifest, approval.status, "
                        "approval.revision, approval.expires_at, "
                        "revision_feedback.feedback AS decided_feedback "
                        "FROM runtime_private.approvals AS approval "
                        "JOIN runtime_private.runs AS run USING (owner_id,run_id) "
                        "JOIN runtime_private.users AS users ON users.user_id=run.owner_id "
                        "LEFT JOIN runtime_private.revision_feedback AS revision_feedback "
                        "ON revision_feedback.owner_id=approval.owner_id "
                        "AND revision_feedback.run_id=approval.run_id "
                        "AND revision_feedback.approval_id=approval.approval_id "
                        "WHERE approval.owner_id=:owner AND approval.approval_id=:approval"
                    ),
                    {"owner": self._principal.account_id, "approval": approval_id},
                )
                .mappings()
                .one_or_none()
            )
            if row is None or row["manifest_sha256"] != expected_digest:
                raise ApprovalNotFound()
            if row["status"] != "pending":
                return {
                    "run_id": row["run_id"],
                    "repository_id": row["github_repository_id"],
                    "github_user_id": row["github_user_id"],
                    "manifest": row["manifest"],
                    "status": row["status"],
                    "revision": row["revision"],
                    "expires_at": row["expires_at"],
                    "decided_feedback": row["decided_feedback"],
                }
            return {
                "run_id": row["run_id"],
                "repository_id": row["github_repository_id"],
                "github_user_id": row["github_user_id"],
                "manifest": row["manifest"],
                "status": row["status"],
                "revision": row["revision"],
                "expires_at": row["expires_at"],
                "decided_feedback": row["decided_feedback"],
            }

    def _read_decision_command(
        self, idempotency_key: str, request_digest: bytes
    ) -> ApprovalReceipt | None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = (
                connection.execute(
                    text(
                        "SELECT request_sha256, receipt FROM runtime_private.approval_commands "
                        "WHERE owner_id=:owner AND idempotency_key=:key "
                        "AND created_at>clock_timestamp()-interval '24 hours'"
                    ),
                    {"owner": self._principal.account_id, "key": idempotency_key},
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        if bytes(row["request_sha256"]) != request_digest:
            raise ApprovalConflict("approval_idempotency_conflict")
        try:
            payload = row["receipt"]
            return ApprovalReceipt(
                approval_id=UUID(payload["approval_id"]),
                run_id=UUID(payload["run_id"]),
                revision=int(payload["revision"]),
                manifest_sha256=bytes.fromhex(payload["manifest_sha256"]),
                expires_at=datetime.fromisoformat(payload["expires_at"]),
                status=payload["status"],
                adopted=payload["adopted"],
            )
        except (KeyError, TypeError, ValueError):
            raise ApprovalConflict("approval_idempotency_record_corrupt") from None

    def _decide_locked(
        self,
        approval_id: UUID,
        digest: bytes,
        status: str,
        *,
        feedback: str | None,
        idempotency_key: str | None,
        request_digest: bytes,
    ) -> ApprovalReceipt:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            if idempotency_key is not None:
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key,0))"),
                    {"lock_key": f"{self._principal.account_id}:{idempotency_key}"},
                )
                connection.execute(
                    text(
                        "DELETE FROM runtime_private.approval_commands WHERE owner_id=:owner "
                        "AND created_at<=clock_timestamp()-interval '24 hours'"
                    ),
                    {"owner": self._principal.account_id},
                )
                prior_command = (
                    connection.execute(
                        text(
                            "SELECT request_sha256, receipt FROM runtime_private.approval_commands "
                            "WHERE owner_id=:owner AND idempotency_key=:key"
                        ),
                        {"owner": self._principal.account_id, "key": idempotency_key},
                    )
                    .mappings()
                    .one_or_none()
                )
                if prior_command is not None:
                    if bytes(prior_command["request_sha256"]) != request_digest:
                        raise ApprovalConflict("approval_idempotency_conflict")
                    payload = prior_command["receipt"]
                    return ApprovalReceipt(
                        approval_id=UUID(payload["approval_id"]),
                        run_id=UUID(payload["run_id"]),
                        revision=int(payload["revision"]),
                        manifest_sha256=bytes.fromhex(payload["manifest_sha256"]),
                        expires_at=datetime.fromisoformat(payload["expires_at"]),
                        status=payload["status"],
                        adopted=payload["adopted"],
                    )
            run_id = connection.execute(
                text(
                    "SELECT run_id FROM runtime_private.approvals WHERE owner_id=:owner "
                    "AND approval_id=:approval"
                ),
                {"owner": self._principal.account_id, "approval": approval_id},
            ).scalar_one_or_none()
            if run_id is None:
                raise ApprovalNotFound()
            run = (
                connection.execute(
                    text(
                        "SELECT status, revision, deadline_at FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
                    ),
                    {"owner": self._principal.account_id, "run": run_id},
                )
                .mappings()
                .one_or_none()
            )
            approval = (
                connection.execute(
                    text(
                        "SELECT revision, manifest_sha256, expires_at, status "
                        "FROM runtime_private.approvals WHERE owner_id=:owner "
                        "AND approval_id=:approval FOR UPDATE"
                    ),
                    {"owner": self._principal.account_id, "approval": approval_id},
                )
                .mappings()
                .one_or_none()
            )
            now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if approval is None or approval["manifest_sha256"] != digest:
                raise ApprovalNotFound()
            if approval["status"] != "pending":
                if approval["status"] == status:
                    if status == "changes":
                        decided_feedback = connection.execute(
                            text(
                                "SELECT feedback FROM runtime_private.revision_feedback "
                                "WHERE owner_id=:owner AND run_id=:run AND approval_id=:approval"
                            ),
                            {
                                "owner": self._principal.account_id,
                                "run": run_id,
                                "approval": approval_id,
                            },
                        ).scalar_one_or_none()
                        if decided_feedback != feedback:
                            raise ApprovalConflict("approval_decision_conflict")
                    receipt = ApprovalReceipt(
                        approval_id,
                        run_id,
                        approval["revision"],
                        digest,
                        approval["expires_at"],
                        status,
                        True,
                    )
                    if idempotency_key is not None:
                        _store_approval_command(
                            connection,
                            owner_id=self._principal.account_id,
                            idempotency_key=idempotency_key,
                            request_digest=request_digest,
                            receipt=receipt,
                        )
                    return receipt
                raise ApprovalConflict("approval_decision_conflict")
            if (
                run is None
                or run["status"] != RunStatus.WAITING_APPROVAL.value
                or run["revision"] != approval["revision"]
                or run["deadline_at"] is None
            ):
                raise ApprovalConflict("approval_stale")
            if approval["expires_at"] <= now or run["deadline_at"] <= now:
                connection.execute(
                    text(
                        "UPDATE runtime_private.approvals SET status='expired', "
                        "decision_actor_id=NULL, decided_at=:now "
                        "WHERE owner_id=:owner AND approval_id=:approval"
                    ),
                    {"now": now, "owner": self._principal.account_id, "approval": approval_id},
                )
                if run["status"] in {
                    RunStatus.WAITING_APPROVAL.value,
                    RunStatus.RUNNING.value,
                }:
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
                    _erase_run_credentials(connection, self._principal.account_id, run_id)
                sequence = connection.execute(
                    text(
                        "SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_private.events "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": self._principal.account_id, "run": run_id},
                ).scalar_one()
                RuntimeRepository(connection, self._principal).append_event(
                    run_id,
                    sequence,
                    "approval.expired",
                    EventActor.SYSTEM,
                    payload={
                        "approval_id": str(approval_id),
                        "manifest_sha256": digest.hex(),
                        "revision": approval["revision"],
                    },
                )
                return ApprovalReceipt(
                    approval_id,
                    run_id,
                    approval["revision"],
                    digest,
                    approval["expires_at"],
                    "expired",
                    False,
                )
            if status == "changes" and (run["revision"] >= 2 or feedback is None):
                raise ApprovalConflict(
                    "approval_revisions_exhausted"
                    if run["revision"] >= 2
                    else "approval_feedback_required"
                )
            connection.execute(
                text(
                    "UPDATE runtime_private.approvals SET status=:status, "
                    "decision_actor_id=:actor, decided_at=:now "
                    "WHERE owner_id=:owner AND approval_id=:approval"
                ),
                {
                    "status": status,
                    "actor": self._principal.account_id,
                    "now": now,
                    "owner": self._principal.account_id,
                    "approval": approval_id,
                },
            )
            next_job_id: UUID | None = None
            next_revision: int | None = None
            if status == "approved":
                repository = RuntimeRepository(connection, self._principal)
                next_step_id = repository.create_step(
                    run_id,
                    f"publish-approved:{approval_id.hex}",
                    "publish_approved",
                    revision=approval["revision"],
                )
                next_job_id = repository.create_job(
                    run_id,
                    next_step_id,
                    replay_policy=ReplayPolicy.ATTENTION_ON_UNKNOWN,
                )
                connection.execute(
                    text(
                        "UPDATE runtime_private.runs SET status='running' "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": self._principal.account_id, "run": run_id},
                )
            elif status == "rejected":
                repository = RuntimeRepository(connection, self._principal)
                _terminalize_locked_run(
                    connection,
                    repository,
                    owner_id=self._principal.account_id,
                    run_id=run_id,
                    target_status=RunStatus.FAILED,
                    event_type="run.failed",
                    actor_type=EventActor.ACCOUNT,
                    actor_id=str(self._principal.account_id),
                    attempt_failure_class=None,
                )
                _erase_run_credentials(connection, self._principal.account_id, run_id)
            elif status == "changes":
                next_revision = approval["revision"] + 1
                feedback_id = uuid4()
                feedback_digest = hashlib.sha256(feedback.encode("utf-8")).digest()
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.revision_feedback "
                        "(feedback_id,owner_id,run_id,revision,approval_id,feedback,"
                        "feedback_sha256,actor_id,created_at) VALUES "
                        "(:id,:owner,:run,:revision,:approval,:feedback,:digest,:actor,:created)"
                    ),
                    {
                        "id": feedback_id,
                        "owner": self._principal.account_id,
                        "run": run_id,
                        "revision": next_revision,
                        "approval": approval_id,
                        "feedback": feedback,
                        "digest": feedback_digest,
                        "actor": self._principal.account_id,
                        "created": now,
                    },
                )
                repository = RuntimeRepository(connection, self._principal)
                next_step_id = repository.create_step(
                    run_id,
                    f"t28:rework:{approval_id.hex}:r{next_revision}",
                    "agent_turn",
                    revision=next_revision,
                )
                next_job_id = repository.create_job(
                    run_id, next_step_id, replay_policy=ReplayPolicy.REPLAY_SAFE
                )
                connection.execute(
                    text(
                        "UPDATE runtime_private.runs SET revision=:revision,status='running' "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {
                        "revision": next_revision,
                        "owner": self._principal.account_id,
                        "run": run_id,
                    },
                )
            sequence = connection.execute(
                text(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_private.events "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": self._principal.account_id, "run": run_id},
            ).scalar_one()
            RuntimeRepository(connection, self._principal).append_event(
                run_id,
                sequence,
                f"approval.{status}",
                EventActor.ACCOUNT,
                actor_id=str(self._principal.account_id),
                payload={
                    "approval_id": str(approval_id),
                    "manifest_sha256": digest.hex(),
                    "revision": approval["revision"],
                    "next_revision": next_revision,
                    "feedback_sha256": None if feedback is None else feedback_digest.hex(),
                    "next_job_id": None if next_job_id is None else str(next_job_id),
                },
            )
            receipt = ApprovalReceipt(
                approval_id,
                run_id,
                approval["revision"],
                digest,
                approval["expires_at"],
                status,
                False,
            )
            if idempotency_key is not None:
                _store_approval_command(
                    connection,
                    owner_id=self._principal.account_id,
                    idempotency_key=idempotency_key,
                    request_digest=request_digest,
                    receipt=receipt,
                )
            return receipt

    async def authorize_dispatch(
        self,
        approval_id: UUID,
        manifest_sha256: str,
        *,
        operation: Literal["write_git_objects", "update_managed_branch", "create_pull_request"],
        lease: ExecutionLease,
    ) -> ApprovalDispatchGrant:
        """Issue a fresh deadline-bound grant for one later internal mutation."""
        if not re.fullmatch(r"[0-9a-f]{64}", manifest_sha256):
            raise ApprovalConflict("approval_digest_invalid")
        if lease.owner_id != self._principal.account_id:
            raise ApprovalConflict("approval_owner_mismatch")
        digest = bytes.fromhex(manifest_sha256)
        context = self._approval_context(approval_id, digest)
        if context["run_id"] != lease.run_id:
            raise ApprovalConflict("approval_execution_mismatch")
        if operation not in context["manifest"]["requested_remote_operations"]:
            raise ApprovalConflict("approval_operation_not_in_manifest")
        facts = await self._publication_facts.current(
            context["repository_id"], context["github_user_id"]
        )
        _validate_publication_facts(facts, context["repository_id"], context["github_user_id"])
        _validate_approved_base(context, facts)
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            run = (
                connection.execute(
                    text(
                        "SELECT status, revision FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
                    ),
                    {"owner": self._principal.account_id, "run": context["run_id"]},
                )
                .mappings()
                .one_or_none()
            )
            approval = (
                connection.execute(
                    text(
                        "SELECT revision, manifest_sha256, expires_at, status "
                        "FROM runtime_private.approvals WHERE owner_id=:owner "
                        "AND approval_id=:approval FOR UPDATE"
                    ),
                    {"owner": self._principal.account_id, "approval": approval_id},
                )
                .mappings()
                .one_or_none()
            )
            now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if (
                run is None
                or approval is None
                or run["status"] != RunStatus.RUNNING.value
                or run["revision"] != approval["revision"]
                or approval["status"] != "approved"
                or approval["manifest_sha256"] != digest
                or approval["expires_at"] <= now
            ):
                raise ApprovalConflict("approval_dispatch_not_authorized")
            try:
                locked = lock_current_execution(connection, self._principal, lease)
            except LeaseLost:
                raise ApprovalConflict("approval_execution_stale") from None
            now = locked.database_now
            if approval["expires_at"] <= now:
                raise ApprovalConflict("approval_dispatch_not_authorized")
            grant_id = uuid4()
            sequence: int = connection.execute(
                text(
                    "SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_private.events "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": self._principal.account_id, "run": context["run_id"]},
            ).scalar_one()
            RuntimeRepository(connection, self._principal).append_event(
                context["run_id"],
                sequence,
                "approval.dispatch_authorized",
                EventActor.WORKER,
                actor_id=lease.lease_owner,
                payload={
                    "approval_id": str(approval_id),
                    "grant_id": str(grant_id),
                    "manifest_sha256": digest.hex(),
                    "operation": operation,
                    "generation": lease.generation,
                    "attempt_id": str(lease.attempt_id),
                },
            )
            final_now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if (
                approval["expires_at"] <= final_now
                or locked.lease_expires_at is None
                or locked.lease_expires_at <= final_now
                or locked.run_deadline_at is None
                or locked.run_deadline_at <= final_now
            ):
                raise ApprovalConflict("approval_dispatch_deadline_elapsed")
            return ApprovalDispatchGrant(
                approval_id=approval_id,
                run_id=context["run_id"],
                manifest_sha256=digest,
                revision=approval["revision"],
                authorized_at=final_now,
                expires_at=approval["expires_at"],
                grant_id=grant_id,
                operation=operation,
                generation=lease.generation,
                attempt_id=lease.attempt_id,
            )


_MAX_CANDIDATE_CHECKPOINT_CHAIN = 300


def _verify_candidate_checkpoint_chain(
    connection: Any,
    *,
    owner_id: UUID,
    run_id: UUID,
    repository_id: int,
    candidate_artifact_id: UUID,
    baseline_artifact_id: UUID,
) -> None:
    """Prove accepted apply-patch checkpoints form a bounded chain to T20's base."""
    current_id = candidate_artifact_id
    visited: set[UUID] = set()
    links = 0
    while current_id != baseline_artifact_id:
        if current_id in visited or links >= _MAX_CANDIDATE_CHECKPOINT_CHAIN:
            raise ApprovalConflict("approval_candidate_lineage_mismatch")
        visited.add(current_id)
        row = (
            connection.execute(
                text(
                    "SELECT upload.parent_upload_id, upload.archive_sha256, upload.tree_sha256, "
                    "upload.status, upload.github_repository_id, accepted.payload, "
                    "accepted.result_sha256, parent.tree_sha256 AS parent_tree_sha256, "
                    "parent.status AS parent_status, "
                    "parent.github_repository_id AS parent_repository_id "
                    "FROM runtime_private.artifact_uploads AS upload "
                    "JOIN runtime_private.accepted_results AS accepted "
                    "ON accepted.owner_id=upload.owner_id AND accepted.run_id=upload.run_id "
                    "AND accepted.artifact_id=upload.upload_id "
                    "JOIN runtime_private.steps AS step "
                    "ON step.owner_id=accepted.owner_id AND step.run_id=accepted.run_id "
                    "AND step.step_id=accepted.step_id "
                    "LEFT JOIN runtime_private.artifact_uploads AS parent "
                    "ON parent.owner_id=upload.owner_id AND parent.run_id=upload.run_id "
                    "AND parent.upload_id=upload.parent_upload_id "
                    "WHERE upload.owner_id=:owner AND upload.run_id=:run "
                    "AND upload.upload_id=:artifact AND upload.status='committed' "
                    "AND upload.github_repository_id=:repository "
                    "AND step.activity='agent_tool' "
                    "AND accepted.payload->>'tool_name'='apply_patch' "
                    "AND accepted.payload ? 'workspace_candidate' "
                    "ORDER BY accepted.event_sequence DESC LIMIT 1"
                ),
                {
                    "owner": owner_id,
                    "run": run_id,
                    "artifact": current_id,
                    "repository": repository_id,
                },
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ApprovalConflict("approval_candidate_lineage_mismatch")
        payload = row["payload"]
        if hashlib.sha256(_canonical_json(payload)).digest() != bytes(row["result_sha256"]):
            raise ApprovalConflict("approval_candidate_result_hash_mismatch")
        workspace_candidate = payload.get("workspace_candidate")
        tool_result = payload.get("tool_result")
        parent_id = row["parent_upload_id"]
        if (
            not isinstance(workspace_candidate, dict)
            or not isinstance(tool_result, dict)
            or parent_id is None
            or row["parent_status"] != "committed"
            or row["parent_repository_id"] != repository_id
            or workspace_candidate.get("base_artifact_id") != str(parent_id)
            or workspace_candidate.get("base_tree_sha256") != bytes(row["parent_tree_sha256"]).hex()
            or workspace_candidate.get("archive_sha256") != bytes(row["archive_sha256"]).hex()
            or workspace_candidate.get("tree_sha256") != bytes(row["tree_sha256"]).hex()
            or tool_result.get("name") != "apply_patch"
            or tool_result.get("candidate_archive_sha256") != bytes(row["archive_sha256"]).hex()
            or tool_result.get("candidate_tree_sha256") != bytes(row["tree_sha256"]).hex()
            or not isinstance(tool_result.get("changed_paths"), list)
            or any(not isinstance(path, str) for path in tool_result["changed_paths"])
            or not isinstance(workspace_candidate.get("changed_paths"), list)
            or any(not isinstance(path, str) for path in workspace_candidate["changed_paths"])
        ):
            raise ApprovalConflict("approval_candidate_lineage_mismatch")
        current_id = parent_id
        links += 1


def _validate_publication_facts(
    facts: RepositoryPublicationFacts, repository_id: int, github_user_id: int
) -> None:
    if not isinstance(facts, RepositoryPublicationFacts):
        raise ApprovalError("repository_access_denied")
    proof = facts.access_proof
    if (
        facts.repository_id != repository_id
        or facts.github_user_id != github_user_id
        or not isinstance(facts.owner, str)
        or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", facts.owner)
        or not isinstance(facts.name, str)
        or not re.fullmatch(r"[A-Za-z0-9_.-]{1,100}", facts.name)
        or not isinstance(facts.default_branch, str)
        or not 1 <= len(facts.default_branch) <= 255
        or facts.default_branch.startswith("/")
        or facts.default_branch.endswith("/")
        or "//" in facts.default_branch
        or ".." in facts.default_branch
        or facts.default_branch.startswith(".")
        or facts.default_branch.endswith(".")
        or facts.default_branch.endswith(".lock")
        or "@{" in facts.default_branch
        or any(char in facts.default_branch for char in " ~^:?*[\\")
        or any(ord(char) < 32 or char == " " for char in facts.default_branch)
        or not isinstance(facts.base_commit_sha, str)
        or not re.fullmatch(r"[0-9a-f]{40}", facts.base_commit_sha)
        or not isinstance(proof, RepositoryAccessProof)
        or proof.repository_id != repository_id
        or proof.github_user_id != github_user_id
        or proof.actor_can_write is not True
        or proof.app_can_access is not True
    ):
        raise ApprovalError("repository_access_denied")


def _validate_pass_evidence(evidence: ValidationEvidence) -> None:
    expected = _PROFILE_COMMANDS.get(evidence.candidate.profile_id)
    baseline_passed = evidence.baseline.outcome is PhaseOutcome.PASSED
    baseline_failed = evidence.baseline.outcome is PhaseOutcome.TEST_FAILED
    if (
        evidence.classification not in _PASSING_CLASSIFICATIONS
        or evidence.candidate.outcome is not PhaseOutcome.PASSED
        or evidence.candidate.test_exit_code != 0
        or evidence.candidate.error_code is not None
        or evidence.candidate.allocation_id is None
        or evidence.baseline.allocation_id is None
        or evidence.candidate.allocation_id == evidence.baseline.allocation_id
        or evidence.candidate.allocation_status not in {"stopped", "destroyed"}
        or expected is None
        or evidence.candidate.command_id != expected[0]
        or evidence.candidate.image is None
        or not evidence.candidate.runtime_version
        or evidence.baseline.profile_id != evidence.candidate.profile_id
        or evidence.baseline.profile_version != evidence.candidate.profile_version
        or evidence.baseline.base_commit_sha != evidence.candidate.base_commit_sha
        or evidence.baseline.error_code is not None
        or evidence.baseline.image != evidence.candidate.image
        or not re.fullmatch(r".+@sha256:[0-9a-f]{64}", evidence.candidate.image or "")
        or evidence.baseline.package_count != evidence.candidate.package_count
        or evidence.baseline.command_id != (expected[0] if expected else None)
        or evidence.baseline.allocation_status not in {"stopped", "destroyed"}
        or not evidence.baseline.runtime_version
        or evidence.baseline.runtime_version != evidence.candidate.runtime_version
        or evidence.baseline.npm_version != evidence.candidate.npm_version
        or evidence.candidate.profile_version != "1"
        or not evidence.candidate.commands
        or any(command.exit_code != 0 for command in evidence.candidate.commands)
        or not any(
            command.phase == expected[1] and command.exit_code == 0
            for command in evidence.candidate.commands
        )
    ):
        raise ApprovalError("approval_candidate_validation_failed")
    if evidence.candidate.profile_id == "python-pytest":
        from continuum_api.repositories.preflight import PYTHON_TARGET_VERSION

        version_valid = evidence.candidate.runtime_version == PYTHON_TARGET_VERSION
    else:
        from continuum_api.sandbox.node_bootstrap_recipe import NODE_VERSION, NPM_VERSION

        version_valid = (
            evidence.candidate.runtime_version == NODE_VERSION
            and evidence.candidate.npm_version == NPM_VERSION
        )
    if not version_valid:
        raise ApprovalError("approval_candidate_validation_failed")
    if evidence.classification is ValidationClassification.PASSED:
        valid_baseline = baseline_passed and evidence.baseline.test_exit_code == 0
    else:
        valid_baseline = baseline_failed and evidence.baseline.test_exit_code not in {None, 0}
    baseline_commands = evidence.baseline.commands
    valid_baseline = (
        valid_baseline
        and all(
            command.exit_code == evidence.baseline.test_exit_code
            if command.phase == expected[1]
            else command.exit_code == 0
            for command in baseline_commands
        )
        and any(
            command.phase == expected[1] and command.exit_code == evidence.baseline.test_exit_code
            for command in baseline_commands
        )
    )
    if not valid_baseline:
        raise ApprovalError("approval_baseline_classification_mismatch")


def _validate_hidden_binding(
    hidden: ManagedHiddenCheckEvidence,
    normal: ValidationEvidence,
    lease: ExecutionLease | _AcceptedExecutionIdentity,
) -> None:
    execution = hidden.execution
    candidate = normal.candidate
    if (
        hidden.comparison.outcome != "passed"
        or hidden.run_id != lease.run_id
        or hidden.step_id != lease.step_id
        or hidden.attempt_id != lease.attempt_id
        or hidden.generation != lease.generation
        or execution.allocation_id in {normal.baseline.allocation_id, candidate.allocation_id}
        or execution.artifact_id != candidate.artifact_id
        or execution.repository_id != candidate.repository_id
        or execution.base_commit_sha != candidate.base_commit_sha
        or execution.baseline_git_tree_sha256 != candidate.baseline_git_tree_sha256
        or execution.baseline_archive_sha256 != candidate.baseline_archive_sha256
        or execution.baseline_archive_tree_sha256 != candidate.baseline_archive_tree_sha256
        or execution.source_archive_sha256 != normal.candidate_archive_sha256
        or execution.source_tree_sha256 != normal.candidate_source_tree_sha256
        or execution.profile_id != candidate.profile_id
        or execution.profile_version != candidate.profile_version
        or execution.image != candidate.image
        or execution.runtime_version != candidate.runtime_version
        or execution.npm_version != candidate.npm_version
        or execution.package_count != candidate.package_count
    ):
        raise ApprovalConflict("approval_hidden_evidence_mismatch")


def _validate_hidden_allocation(
    connection: Any,
    *,
    owner_id: UUID,
    lease: ExecutionLease | _AcceptedExecutionIdentity,
    evidence: ManagedHiddenCheckEvidence,
) -> None:
    allocation_id = evidence.execution.allocation_id
    assert allocation_id is not None
    allocation = (
        connection.execute(
            text(
                "SELECT owner_id, run_id, step_id, job_id, generation, attempt_id, "
                "lease_owner, status, allocation_class, logical_key "
                "FROM runtime_private.sandbox_allocations WHERE allocation_id=:allocation "
                "FOR SHARE"
            ),
            {"allocation": allocation_id},
        )
        .mappings()
        .one_or_none()
    )
    if (
        allocation is None
        or allocation["owner_id"] != owner_id
        or allocation["run_id"] != lease.run_id
        or allocation["step_id"] != lease.step_id
        or allocation["job_id"] != lease.job_id
        or allocation["generation"] != lease.generation
        or allocation["attempt_id"] != lease.attempt_id
        or allocation["lease_owner"] != lease.lease_owner
        or allocation["allocation_class"] != "operator"
        or allocation["status"] not in {"stopped", "destroyed"}
        or allocation["status"] != evidence.execution.allocation_status
        or allocation["logical_key"] != f"t48:hidden:v1:{evidence.invocation_sha256}"
    ):
        raise ApprovalConflict("approval_hidden_allocation_mismatch")


def _validate_approved_base(context: dict[str, Any], facts: RepositoryPublicationFacts) -> None:
    manifest = context.get("manifest")
    if manifest is not None and (
        manifest.get("base", {}).get("branch") != facts.default_branch
        or manifest.get("base", {}).get("commit_sha") != facts.base_commit_sha
    ):
        raise ApprovalConflict("approval_base_branch_moved")


def _validate_candidate_base(context: dict[str, Any], facts: RepositoryPublicationFacts) -> None:
    if facts.base_commit_sha != context["evidence"].candidate.base_commit_sha:
        raise ApprovalConflict("approval_base_branch_moved")


def _build_manifest(
    context: dict[str, Any],
    facts: RepositoryPublicationFacts,
    candidate: VerifiedArchive,
    baseline: VerifiedArchive,
) -> tuple[dict[str, Any], bytes, bytes]:
    evidence: ValidationEvidence = context["evidence"]
    if evidence.candidate.base_commit_sha != evidence.baseline.base_commit_sha:
        raise ApprovalConflict("approval_base_commit_mismatch")
    parent = evidence.candidate.base_commit_sha
    if len(parent) != 40:
        raise ApprovalConflict("approval_git_object_format_unsupported")
    patch_rows = _patch_rows(baseline.files, candidate.files)
    if len(patch_rows) != evidence.changed_path_count:
        raise ApprovalConflict("approval_candidate_patch_mismatch")
    patch_sha256 = hashlib.sha256(_canonical_json(patch_rows)).hexdigest()
    timestamp = int(context["created_at"].astimezone(UTC).timestamp())
    commit = deterministic_commit(
        candidate,
        parent_sha=parent,
        author_name="Continumm",
        author_email="noreply@continumm.invalid",
        timestamp=timestamp,
        message=f"Apply reviewed change for run {context['run_id']}\n",
    )
    branch = f"continumm/run/{context['run_id']}"
    if not _BRANCH_PATTERN.fullmatch(branch):
        raise ApprovalConflict("approval_managed_branch_invalid")
    task = context["task_context"]
    title_seed = task.get("objective", task.get("task", "Reviewed code change"))
    if not isinstance(title_seed, str):
        title_seed = "Reviewed code change"
    first_line = title_seed.splitlines()[0].strip()[:140] or "Reviewed code change"
    pr_title = f"Continumm: {first_line}"[:256]
    marker = f"<!-- continumm-run:{context['run_id']} -->"
    pr_body = (
        f"Automated change for run {context['run_id']}.\n\n"
        f"Validated profile: {evidence.candidate.profile_id}"
        f"@{evidence.candidate.profile_version}.\n"
        f"Evidence SHA-256: {context['evidence_result_sha256']}\n\n{marker}"
    )
    now = context["database_now"]
    expires_at = min(
        now + _MAX_APPROVAL_LIFETIME,
        context["deadline_at"],
        context["candidate_content_expires_at"],
        context["baseline_content_expires_at"],
    )
    if expires_at <= now:
        raise ApprovalConflict("approval_expired")
    manifest = {
        "manifest_version": 1,
        "repository": {"id": context["repository_id"], "owner": facts.owner, "name": facts.name},
        "base": {"branch": facts.default_branch, "commit_sha": parent},
        "candidate": {
            "checkpoint_id": str(context["candidate_artifact_id"]),
            "archive_sha256": context["candidate_archive_sha256"],
            "source_tree_sha256": context["candidate_source_tree_sha256"],
            "git_tree_sha": commit.tree_sha,
            "patch_sha256": patch_sha256,
            "changed_paths": patch_rows,
        },
        "commit": {
            "sha": commit.commit_sha,
            "bytes_base64": base64.b64encode(commit.commit_bytes).decode("ascii"),
            "author": {
                "name": "Continumm",
                "email": "noreply@continumm.invalid",
                "timestamp": timestamp,
                "timezone": "+0000",
            },
            "committer": {
                "name": "Continumm",
                "email": "noreply@continumm.invalid",
                "timestamp": timestamp,
                "timezone": "+0000",
            },
            "message": f"Apply reviewed change for run {context['run_id']}\n",
        },
        "branch": {"name": branch},
        "pull_request": {
            "title": pr_title,
            "body": pr_body,
            "base": facts.default_branch,
            "head": branch,
            "marker": marker,
        },
        "validation": {
            "evidence_result_id": str(context["evidence_result_id"]),
            "evidence_result_sha256": context["evidence_result_sha256"],
            "classification": evidence.classification.value,
            "profile_id": evidence.candidate.profile_id,
            "profile_version": evidence.candidate.profile_version,
            "candidate_outcome": evidence.candidate.outcome.value,
        },
        "requested_remote_operations": [
            "write_git_objects",
            "update_managed_branch",
            "create_pull_request",
        ],
        "approval_created_at": now.isoformat(),
        "approval_expires_at": expires_at.isoformat(),
        "content_expires_at": min(
            context["candidate_content_expires_at"], context["baseline_content_expires_at"]
        ).isoformat(),
        "policy_version": _POLICY_VERSION,
        "run_id": str(context["run_id"]),
        "revision": context["revision"],
    }
    encoded, digest = digest_manifest(manifest)
    if len(encoded) > 65_000:
        raise ApprovalConflict("approval_manifest_too_large")
    return manifest, encoded, digest


def _patch_rows(
    baseline: tuple[ArchiveFile, ...], candidate: tuple[ArchiveFile, ...]
) -> list[dict[str, Any]]:
    old = {item.path: item for item in baseline}
    new = {item.path: item for item in candidate}
    rows: list[dict[str, Any]] = []
    for path in sorted(old.keys() | new.keys(), key=lambda item: item.encode("utf-8")):
        before, after = old.get(path), new.get(path)
        if (
            before is not None
            and after is not None
            and before.content == after.content
            and before.mode == after.mode
        ):
            continue
        rows.append(
            {
                "path": path,
                "before_sha256": None
                if before is None
                else hashlib.sha256(before.content).hexdigest(),
                "after_sha256": None
                if after is None
                else hashlib.sha256(after.content).hexdigest(),
                "before_mode": None if before is None else f"{before.mode:04o}",
                "after_mode": None if after is None else f"{after.mode:04o}",
            }
        )
    return rows


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise ApprovalManifestError("approval_manifest_not_canonical") from None


def _validate_revision_feedback_text(value: str | None) -> str:
    if not isinstance(value, str):
        raise ApprovalConflict("approval_feedback_required")
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        raise ApprovalConflict("approval_feedback_invalid") from None
    if not value.strip() or len(encoded) > 16_384 or "\x00" in value:
        raise ApprovalConflict("approval_feedback_invalid")
    return value


def _store_approval_command(
    connection: Any,
    *,
    owner_id: UUID,
    idempotency_key: str,
    request_digest: bytes,
    receipt: ApprovalReceipt,
) -> None:
    connection.execute(
        text(
            "INSERT INTO runtime_private.approval_commands "
            "(owner_id,idempotency_key,request_sha256,receipt) VALUES "
            "(:owner,:key,:digest,CAST(:receipt AS jsonb))"
        ),
        {
            "owner": owner_id,
            "key": idempotency_key,
            "digest": request_digest,
            "receipt": json.dumps(
                {
                    "approval_id": str(receipt.approval_id),
                    "run_id": str(receipt.run_id),
                    "revision": receipt.revision,
                    "manifest_sha256": receipt.manifest_sha256.hex(),
                    "expires_at": receipt.expires_at.isoformat(),
                    "status": receipt.status,
                    "adopted": receipt.adopted,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
    )


def _erase_run_credentials(connection: Any, owner_id: UUID, run_id: UUID) -> None:
    connection.execute(
        text(
            "UPDATE runtime_private.provider_credentials SET status='revoked', nonce=NULL, "
            "ciphertext=NULL, ended_at=clock_timestamp() WHERE owner_id=:owner "
            "AND scope='run' AND scope_run_id=:run AND status='active'"
        ),
        {"owner": owner_id, "run": run_id},
    )
