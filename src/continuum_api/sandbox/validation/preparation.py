"""Prepare T20 candidate inputs without impersonating a T13 eligibility receipt."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from uuid import UUID

from continuum_api.artifacts.archives import (
    ArchiveError,
    ArchiveManifest,
    VerifiedArchive,
    verify_archive,
)
from continuum_api.repositories.preflight import EligibilityReceipt, RepositoryPreflightError
from continuum_api.sandbox.node_bootstrap_recipe import NODE_PROFILE_ID, NODE_PROFILE_VERSION
from continuum_api.sandbox.python_bootstrap_recipe import PYTHON_PROFILE_ID, PYTHON_PROFILE_VERSION
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import (
    ToolPolicyContext,
    is_protected_source_path,
    is_sensitive_source_path,
)
from continuum_api.tools.source_tools import (
    MAX_PATCH_FILES,
    PatchCandidate,
    PatchPreimage,
    SourceSnapshot,
    SourceToolError,
    discover_baseline_test_paths,
    snapshot_from_verified_archive,
    source_paths,
    verify_patch_candidate,
)

MAX_CANDIDATE_CHANGED_PATHS = MAX_PATCH_FILES
_SUPPORTED_PROFILES = {
    (PYTHON_PROFILE_ID, PYTHON_PROFILE_VERSION),
    (NODE_PROFILE_ID, NODE_PROFILE_VERSION),
}


class CandidateValidationError(RuntimeError):
    """Stable source-preparation failure without repository content."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class ValidationSourceAuthorization:
    """In-memory authorization for testing one candidate against a T13 baseline.

    The embedded eligibility receipt remains the original T13 receipt. Candidate
    identity is represented by separate T14 archive/tree digests and never by a
    fabricated Git commit or tree object ID.
    """

    baseline_artifact_id: UUID
    eligibility: EligibilityReceipt
    profile_id: str
    profile_version: str
    baseline_archive_sha256: str
    baseline_source_tree_sha256: str
    protected_manifest_sha256: str
    lineage_sha256: str
    candidate_archive: bytes
    candidate_manifest: ArchiveManifest
    candidate_archive_sha256: str
    candidate_source_tree_sha256: str
    candidate_file_count: int
    candidate_source_bytes: int
    changed_paths: tuple[str, ...]
    preimages: tuple[PatchPreimage, ...]
    tool_policy_context: ToolPolicyContext


def prepare_validation_source(
    eligibility: EligibilityReceipt,
    baseline_artifact_id: UUID,
    baseline_verified: VerifiedArchive,
    candidate: PatchCandidate,
    tool_context: ToolPolicyContext,
) -> ValidationSourceAuthorization:
    """Validate a source-tools candidate and bind it to an unchanged T13 receipt."""
    baseline = _snapshot_from_verified(baseline_verified)
    _validate_context(eligibility, baseline, tool_context)
    _validate_profile_baseline(eligibility, baseline_verified)
    candidate_verified = _revalidate_candidate(baseline_verified, candidate)
    _validate_candidate_policy(
        eligibility,
        baseline_verified,
        candidate_verified,
        candidate,
        tool_context,
    )
    protected_manifest_sha256 = _protected_manifest_digest(eligibility)
    file_count = len(candidate_verified.manifest.entries)
    source_bytes = sum(entry.size for entry in candidate_verified.manifest.entries)
    lineage_sha256 = _lineage_digest(
        baseline_artifact_id=baseline_artifact_id,
        eligibility=eligibility,
        profile_id=eligibility.profile_id,
        profile_version=eligibility.profile_version,
        baseline_archive_sha256=baseline.archive_sha256,
        baseline_source_tree_sha256=baseline.tree_sha256,
        protected_manifest_sha256=protected_manifest_sha256,
        candidate_archive_sha256=candidate_verified.archive_sha256,
        candidate_source_tree_sha256=candidate_verified.manifest.tree_sha256,
        candidate_file_count=file_count,
        candidate_source_bytes=source_bytes,
        changed_paths=candidate.changed_paths,
        preimages=candidate.preimages,
        tool_context=tool_context,
    )
    return ValidationSourceAuthorization(
        baseline_artifact_id=baseline_artifact_id,
        eligibility=eligibility,
        profile_id=eligibility.profile_id,
        profile_version=eligibility.profile_version,
        baseline_archive_sha256=baseline.archive_sha256,
        baseline_source_tree_sha256=baseline.tree_sha256,
        protected_manifest_sha256=protected_manifest_sha256,
        lineage_sha256=lineage_sha256,
        candidate_archive=candidate.source.archive,
        candidate_manifest=candidate_verified.manifest,
        candidate_archive_sha256=candidate_verified.archive_sha256,
        candidate_source_tree_sha256=candidate_verified.manifest.tree_sha256,
        candidate_file_count=file_count,
        candidate_source_bytes=source_bytes,
        changed_paths=candidate.changed_paths,
        preimages=candidate.preimages,
        tool_policy_context=tool_context,
    )


def verify_validation_source_authorization(
    source: ValidationSourceAuthorization,
    *,
    eligibility: EligibilityReceipt,
    baseline_artifact_id: UUID,
    restored_baseline: VerifiedArchive,
) -> VerifiedArchive:
    """Revalidate T15's restored baseline and return only its authorized candidate."""
    if not isinstance(source, ValidationSourceAuthorization):
        raise CandidateValidationError("validation_source_authorization_invalid")
    if (
        source.baseline_artifact_id != baseline_artifact_id
        or source.eligibility.to_cache_payload() != eligibility.to_cache_payload()
        or source.profile_id != eligibility.profile_id
        or source.profile_version != eligibility.profile_version
    ):
        raise CandidateValidationError("validation_source_baseline_identity_mismatch")

    try:
        baseline = _snapshot_from_verified(restored_baseline)
        if (
            baseline.archive_sha256 != source.baseline_archive_sha256
            or baseline.tree_sha256 != source.baseline_source_tree_sha256
        ):
            raise CandidateValidationError("validation_source_baseline_pin_mismatch")
        _validate_context(eligibility, baseline, source.tool_policy_context)
        _validate_profile_baseline(eligibility, restored_baseline)
        candidate_verified = verify_archive(
            source.candidate_archive,
            expected_archive_sha256=source.candidate_archive_sha256,
            expected_tree_sha256=source.candidate_source_tree_sha256,
            expected_manifest=source.candidate_manifest,
        )
        candidate_source = _candidate_snapshot(source, candidate_verified)
        actual_candidate = _candidate_delta(
            restored_baseline,
            candidate_source,
        )
        if (
            actual_candidate.changed_paths != source.changed_paths
            or actual_candidate.preimages != source.preimages
        ):
            raise CandidateValidationError("validation_source_candidate_lineage_mismatch")
        reverified = _revalidate_candidate(restored_baseline, actual_candidate)
        _validate_candidate_policy(
            eligibility,
            restored_baseline,
            reverified,
            actual_candidate,
            source.tool_policy_context,
        )
        protected_manifest_sha256 = _protected_manifest_digest(eligibility)
        if protected_manifest_sha256 != source.protected_manifest_sha256:
            raise CandidateValidationError("validation_source_manifest_pin_mismatch")
        file_count = len(reverified.manifest.entries)
        source_bytes = sum(entry.size for entry in reverified.manifest.entries)
        if (
            file_count != source.candidate_file_count
            or source_bytes != source.candidate_source_bytes
            or source.lineage_sha256
            != _lineage_digest(
                baseline_artifact_id=baseline_artifact_id,
                eligibility=eligibility,
                profile_id=eligibility.profile_id,
                profile_version=eligibility.profile_version,
                baseline_archive_sha256=baseline.archive_sha256,
                baseline_source_tree_sha256=baseline.tree_sha256,
                protected_manifest_sha256=protected_manifest_sha256,
                candidate_archive_sha256=reverified.archive_sha256,
                candidate_source_tree_sha256=reverified.manifest.tree_sha256,
                candidate_file_count=file_count,
                candidate_source_bytes=source_bytes,
                changed_paths=actual_candidate.changed_paths,
                preimages=actual_candidate.preimages,
                tool_context=source.tool_policy_context,
            )
        ):
            raise CandidateValidationError("validation_source_lineage_pin_mismatch")
        return reverified
    except CandidateValidationError:
        raise
    except (ArchiveError, SourceToolError, RepositoryPreflightError) as error:
        raise CandidateValidationError(
            f"validation_source_invalid_{getattr(error, 'code', 'archive')}"
        ) from None


def _snapshot_from_verified(verified: VerifiedArchive) -> SourceSnapshot:
    if not isinstance(verified, VerifiedArchive):
        raise CandidateValidationError("validation_baseline_archive_invalid")
    try:
        return snapshot_from_verified_archive(
            verified,
            expected_archive_sha256=verified.archive_sha256,
            expected_tree_sha256=verified.manifest.tree_sha256,
            expected_manifest=verified.manifest,
        )
    except (ArchiveError, SourceToolError) as error:
        raise CandidateValidationError(
            f"validation_baseline_archive_invalid_{getattr(error, 'code', 'archive')}"
        ) from None


def _validate_profile_baseline(
    eligibility: EligibilityReceipt,
    baseline: VerifiedArchive,
) -> None:
    try:
        if (eligibility.profile_id, eligibility.profile_version) == (
            PYTHON_PROFILE_ID,
            PYTHON_PROFILE_VERSION,
        ):
            from continuum_api.sandbox.python_profile import build_python_profile_plan

            build_python_profile_plan(eligibility, baseline)
        elif (eligibility.profile_id, eligibility.profile_version) == (
            NODE_PROFILE_ID,
            NODE_PROFILE_VERSION,
        ):
            from continuum_api.sandbox.node_profile import build_node_profile_plan

            build_node_profile_plan(eligibility, baseline)
        else:
            raise CandidateValidationError("validation_profile_unsupported")
    except CandidateValidationError:
        raise
    except Exception:
        raise CandidateValidationError("validation_baseline_receipt_mismatch") from None


def _validate_context(
    eligibility: EligibilityReceipt,
    baseline: SourceSnapshot,
    context: ToolPolicyContext,
) -> None:
    if not isinstance(context, ToolPolicyContext):
        raise CandidateValidationError("validation_tool_context_invalid")
    if (eligibility.profile_id, eligibility.profile_version) not in _SUPPORTED_PROFILES:
        raise CandidateValidationError("validation_profile_unsupported")
    baseline_paths = frozenset(source_paths(baseline))
    if (
        context.profile_id != eligibility.profile_id
        or context.profile_version != eligibility.profile_version
        or context.baseline_tree_sha256 != baseline.tree_sha256
        or context.baseline_source_paths != baseline_paths
        or context.stage is not ToolStage.AGENT_WORK
        or ToolCapability.APPLY_PATCH not in context.capabilities
        or not context.actor_can_write_repository
        or not context.app_can_access_repository
        or not context.run_budget_available
        or not context.baseline_paths_verified
    ):
        raise CandidateValidationError("validation_tool_context_mismatch")
    try:
        discovered = discover_baseline_test_paths(
            baseline,
            eligibility.profile_id,
            expected_tree_sha256=baseline.tree_sha256,
        )
    except SourceToolError as error:
        raise CandidateValidationError(f"validation_baseline_discovery_{error.code}") from None
    if not discovered or not discovered <= context.baseline_test_paths:
        raise CandidateValidationError("validation_baseline_tests_unverified")


def _revalidate_candidate(
    baseline: VerifiedArchive,
    candidate: PatchCandidate,
) -> VerifiedArchive:
    if len(candidate.changed_paths) > MAX_CANDIDATE_CHANGED_PATHS:
        raise CandidateValidationError("validation_candidate_change_limit")
    try:
        return verify_patch_candidate(baseline, candidate)
    except SourceToolError as error:
        raise CandidateValidationError(f"validation_candidate_lineage_{error.code}") from None


def _candidate_snapshot(
    source: ValidationSourceAuthorization,
    candidate: VerifiedArchive,
) -> SourceSnapshot:
    try:
        return snapshot_from_verified_archive(
            candidate,
            expected_archive_sha256=source.candidate_archive_sha256,
            expected_tree_sha256=source.candidate_source_tree_sha256,
            expected_manifest=source.candidate_manifest,
        )
    except (ArchiveError, SourceToolError) as error:
        raise CandidateValidationError(
            f"validation_candidate_archive_invalid_{getattr(error, 'code', 'archive')}"
        ) from None


def _candidate_delta(
    baseline: VerifiedArchive,
    candidate: SourceSnapshot,
) -> PatchCandidate:
    try:
        candidate_verified = verify_archive(
            candidate.archive,
            expected_archive_sha256=candidate.archive_sha256,
            expected_tree_sha256=candidate.tree_sha256,
            expected_manifest=candidate.manifest,
        )
    except ArchiveError as error:
        raise CandidateValidationError(f"validation_candidate_archive_{error.code}") from None
    before = {file.path: file for file in baseline.files}
    after = {file.path: file for file in candidate_verified.files}
    if set(before) - set(after):
        raise CandidateValidationError("validation_candidate_file_removed")
    changed_paths = tuple(
        sorted(
            (
                path
                for path in after
                if path not in before
                or after[path].content != before[path].content
                or after[path].mode != before[path].mode
            ),
            key=lambda item: item.encode("utf-8"),
        )
    )
    if len(changed_paths) > MAX_CANDIDATE_CHANGED_PATHS:
        raise CandidateValidationError("validation_candidate_change_limit")
    preimages = tuple(
        PatchPreimage(
            path,
            None if path not in before else hashlib.sha256(before[path].content).hexdigest(),
        )
        for path in changed_paths
    )
    return PatchCandidate(
        source=candidate,
        base_tree_sha256=baseline.manifest.tree_sha256,
        changed_paths=changed_paths,
        preimages=preimages,
    )


def _validate_candidate_policy(
    eligibility: EligibilityReceipt,
    baseline: VerifiedArchive,
    candidate: VerifiedArchive,
    patch: PatchCandidate,
    context: ToolPolicyContext,
) -> None:
    before = {file.path: file for file in baseline.files}
    after = {file.path: file for file in candidate.files}
    if set(before) - set(after):
        raise CandidateValidationError("validation_candidate_file_removed")
    for path in patch.changed_paths:
        if is_sensitive_source_path(path) or is_protected_source_path(path, context):
            raise CandidateValidationError("validation_candidate_protected_path_changed")
    for path, original in before.items():
        if is_protected_source_path(path, context) and after.get(path) != original:
            raise CandidateValidationError("validation_candidate_baseline_protection_changed")
    for path in set(after) - set(before):
        if is_sensitive_source_path(path) or is_protected_source_path(path, context):
            raise CandidateValidationError("validation_candidate_protected_path_added")

    manifest_paths = {path for path, _ in eligibility.manifest_blobs}
    for path in manifest_paths:
        if before.get(path) != after.get(path):
            raise CandidateValidationError("validation_candidate_manifest_changed")


def _protected_manifest_digest(eligibility: EligibilityReceipt) -> str:
    try:
        entries = sorted(eligibility.manifest_blobs, key=lambda item: item[0].encode("utf-8"))
        if len({path for path, _ in entries}) != len(entries):
            raise ValueError
        payload = json.dumps(entries, ensure_ascii=True, separators=(",", ":"))
    except (AttributeError, TypeError, ValueError, UnicodeEncodeError):
        raise CandidateValidationError("validation_manifest_receipt_invalid") from None
    return hashlib.sha256(payload.encode("ascii")).hexdigest()


def _lineage_digest(
    *,
    baseline_artifact_id: UUID,
    eligibility: EligibilityReceipt,
    profile_id: str,
    profile_version: str,
    baseline_archive_sha256: str,
    baseline_source_tree_sha256: str,
    protected_manifest_sha256: str,
    candidate_archive_sha256: str,
    candidate_source_tree_sha256: str,
    candidate_file_count: int,
    candidate_source_bytes: int,
    changed_paths: tuple[str, ...],
    preimages: tuple[PatchPreimage, ...],
    tool_context: ToolPolicyContext,
) -> str:
    payload = {
        "format": "continuum-validation-source-v1",
        "baseline_artifact_id": str(baseline_artifact_id),
        "eligibility": eligibility.to_cache_payload(),
        "profile_id": profile_id,
        "profile_version": profile_version,
        "baseline_archive_sha256": baseline_archive_sha256,
        "baseline_source_tree_sha256": baseline_source_tree_sha256,
        "protected_manifest_sha256": protected_manifest_sha256,
        "candidate_archive_sha256": candidate_archive_sha256,
        "candidate_source_tree_sha256": candidate_source_tree_sha256,
        "candidate_file_count": candidate_file_count,
        "candidate_source_bytes": candidate_source_bytes,
        "changed_paths": list(changed_paths),
        "preimages": [[item.path, item.sha256] for item in preimages],
        "tool_context": {
            "capabilities": sorted(item.value for item in tool_context.capabilities),
            "profile_id": tool_context.profile_id,
            "profile_version": tool_context.profile_version,
            "actor_can_write_repository": tool_context.actor_can_write_repository,
            "app_can_access_repository": tool_context.app_can_access_repository,
            "stage": tool_context.stage.value,
            "run_budget_available": tool_context.run_budget_available,
            "baseline_tree_sha256": tool_context.baseline_tree_sha256,
            "baseline_source_paths": sorted(tool_context.baseline_source_paths),
            "baseline_test_paths": sorted(tool_context.baseline_test_paths),
            "baseline_paths_verified": tool_context.baseline_paths_verified,
            "protected_paths": sorted(tool_context.protected_paths),
        },
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()
