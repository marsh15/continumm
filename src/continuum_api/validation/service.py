"""Run fixed baseline and immutable-candidate validation phases."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from typing import Protocol
from uuid import UUID

from continuum_api.artifacts.archives import ArchiveError, ArchiveManifest, verify_archive
from continuum_api.repositories.preflight import PYTHON_TARGET_VERSION, EligibilityReceipt
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.sandbox_allocations import AllocationClass, AllocationReservation
from continuum_api.sandbox.node_bootstrap_recipe import NODE_VERSION, NPM_VERSION
from continuum_api.sandbox.validation.preparation import ValidationSourceAuthorization
from continuum_api.validation.contracts import (
    CommandEvidence,
    PhaseEvidence,
    PhaseOutcome,
    ValidationClassification,
    ValidationContractError,
    ValidationEvidence,
    ValidationPhase,
)

_PYTHON_PROFILE = ("python-pytest", "1", "python-pytest-v1")
_NODE_PROFILE = ("node-npm", "1", "node-npm-test-v1")
_COMPLETED_ALLOCATION_STATES = frozenset({"stopped", "destroyed"})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_CODE = re.compile(r"^[a-z0-9_]{1,96}$")
DEFAULT_VALIDATION_ALLOCATION_SECONDS = 300
MAX_VALIDATION_TOTAL_ALLOCATION_SECONDS = 900
_VALIDATION_ALLOCATION_KEY_PREFIX = "t20:v1"


class ValidationServiceError(RuntimeError):
    """A sanitized request or source-identity rejection."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class ProfileResult(Protocol):
    @property
    def profile_id(self) -> str: ...

    @property
    def profile_version(self) -> str: ...

    @property
    def repository_id(self) -> int: ...

    @property
    def commit_sha(self) -> str: ...

    @property
    def tree_sha256(self) -> str: ...

    @property
    def artifact_id(self) -> UUID: ...

    @property
    def allocation_id(self) -> UUID: ...

    @property
    def package_count(self) -> int: ...

    @property
    def test_exit_code(self) -> int | None: ...

    @property
    def test_passed(self) -> bool: ...

    @property
    def outputs(self) -> Sequence[object]: ...

    @property
    def allocation_status(self) -> str: ...

    @property
    def validation_phase(self) -> str: ...

    @property
    def baseline_git_tree_sha256(self) -> str: ...

    @property
    def baseline_archive_tree_sha256(self) -> str: ...

    @property
    def candidate_archive_sha256(self) -> str | None: ...

    @property
    def candidate_source_tree_sha256(self) -> str | None: ...

    @property
    def image(self) -> str: ...

    @property
    def command_id(self) -> str: ...


class ProfileRunner(Protocol):
    @property
    def configured_image(self) -> str | None: ...

    async def run(
        self,
        principal: Principal,
        lease: ExecutionLease,
        artifact_id: UUID,
        eligibility: EligibilityReceipt,
        allocation_id: UUID,
        *,
        validation_source: ValidationSourceAuthorization | None = None,
    ) -> ProfileResult: ...


class AllocationCreator(Protocol):
    async def create(
        self,
        principal: Principal,
        lease: ExecutionLease,
        logical_key: str,
        requested_seconds: int,
        *,
        allocation_class: AllocationClass = "public",
    ) -> AllocationReservation: ...


class ValidationService:
    """Collect immutable baseline and candidate results, without executing on-host.

    This service returns a prepared evidence value. The caller must commit that
    JSON through the fenced runtime transition before describing it as durable.
    Each phase receives a distinct T16 allocation. A second allocation is
    created only after the baseline profile returns with verified cleanup.
    """

    def __init__(
        self,
        python_profile: ProfileRunner,
        node_profile: ProfileRunner,
        allocation_creator: AllocationCreator,
        *,
        allocation_seconds_per_phase: int = DEFAULT_VALIDATION_ALLOCATION_SECONDS,
    ) -> None:
        if (
            type(allocation_seconds_per_phase) is not int
            or allocation_seconds_per_phase < 1
            or allocation_seconds_per_phase * 2 > MAX_VALIDATION_TOTAL_ALLOCATION_SECONDS
        ):
            raise ValueError("validation allocation duration is invalid")
        self._profiles = {
            _PYTHON_PROFILE[0]: python_profile,
            _NODE_PROFILE[0]: node_profile,
        }
        self._allocation_creator = allocation_creator
        self._allocation_seconds_per_phase = allocation_seconds_per_phase

    async def run(
        self,
        principal: Principal,
        lease: ExecutionLease,
        baseline_artifact_id: UUID,
        eligibility: EligibilityReceipt,
        validation_source: ValidationSourceAuthorization,
    ) -> ValidationEvidence:
        if principal.account_id != lease.owner_id:
            raise ValidationServiceError("validation_owner_mismatch")
        profile = self._profiles.get(eligibility.profile_id)
        if profile is None:
            raise ValidationServiceError("validation_profile_unsupported")
        expected_profile = (
            _PYTHON_PROFILE if eligibility.profile_id == _PYTHON_PROFILE[0] else _NODE_PROFILE
        )
        if eligibility.profile_version != expected_profile[1]:
            raise ValidationServiceError("validation_profile_version_unsupported")
        self._validate_source(
            validation_source,
            baseline_artifact_id=baseline_artifact_id,
            eligibility=eligibility,
        )

        baseline = await self._run_phase(
            profile,
            principal,
            lease,
            phase=ValidationPhase.BASELINE,
            logical_key=_allocation_key(validation_source, ValidationPhase.BASELINE),
            baseline_artifact_id=baseline_artifact_id,
            eligibility=eligibility,
            source_archive_sha256=validation_source.baseline_archive_sha256,
            source_tree_sha256=validation_source.baseline_source_tree_sha256,
            baseline_source_tree_sha256=validation_source.baseline_source_tree_sha256,
            baseline_archive_sha256=validation_source.baseline_archive_sha256,
            candidate_archive_sha256=None,
            candidate_source_tree_sha256=None,
            expected_profile=expected_profile,
            validation_source=None,
        )

        if baseline.outcome not in {PhaseOutcome.PASSED, PhaseOutcome.TEST_FAILED}:
            candidate = self._not_run_phase(
                phase=ValidationPhase.CANDIDATE,
                allocation_id=None,
                baseline_artifact_id=baseline_artifact_id,
                eligibility=eligibility,
                source_archive_sha256=validation_source.candidate_archive_sha256,
                source_tree_sha256=validation_source.candidate_source_tree_sha256,
                baseline_source_tree_sha256=validation_source.baseline_source_tree_sha256,
                baseline_archive_sha256=validation_source.baseline_archive_sha256,
                command_id=expected_profile[2],
                image=getattr(profile, "configured_image", None),
            )
            classification = self._baseline_classification(baseline.outcome)
        else:
            candidate = await self._run_phase(
                profile,
                principal,
                lease,
                phase=ValidationPhase.CANDIDATE,
                logical_key=_allocation_key(validation_source, ValidationPhase.CANDIDATE),
                baseline_artifact_id=baseline_artifact_id,
                eligibility=eligibility,
                source_archive_sha256=validation_source.candidate_archive_sha256,
                source_tree_sha256=validation_source.candidate_source_tree_sha256,
                baseline_source_tree_sha256=validation_source.baseline_source_tree_sha256,
                baseline_archive_sha256=validation_source.baseline_archive_sha256,
                candidate_archive_sha256=validation_source.candidate_archive_sha256,
                candidate_source_tree_sha256=validation_source.candidate_source_tree_sha256,
                expected_profile=expected_profile,
                validation_source=validation_source,
            )
            classification = self._candidate_classification(baseline.outcome, candidate.outcome)

        try:
            return ValidationEvidence(
                classification=classification,
                baseline_artifact_id=baseline_artifact_id,
                candidate_archive_sha256=validation_source.candidate_archive_sha256,
                candidate_source_tree_sha256=validation_source.candidate_source_tree_sha256,
                candidate_lineage_sha256=validation_source.lineage_sha256,
                changed_path_count=len(validation_source.changed_paths),
                baseline=baseline,
                candidate=candidate,
            )
        except ValidationContractError as error:
            raise ValidationServiceError(error.code) from None

    async def _run_phase(
        self,
        profile: ProfileRunner,
        principal: Principal,
        lease: ExecutionLease,
        *,
        phase: ValidationPhase,
        logical_key: str,
        baseline_artifact_id: UUID,
        eligibility: EligibilityReceipt,
        source_archive_sha256: str,
        source_tree_sha256: str,
        baseline_source_tree_sha256: str,
        baseline_archive_sha256: str,
        candidate_archive_sha256: str | None,
        candidate_source_tree_sha256: str | None,
        expected_profile: tuple[str, str, str],
        validation_source: ValidationSourceAuthorization | None,
    ) -> PhaseEvidence:
        allocation_id: UUID | None = None
        expected_image = getattr(profile, "configured_image", None)
        try:
            reservation = await self._allocation_creator.create(
                principal,
                lease,
                logical_key,
                self._allocation_seconds_per_phase,
                allocation_class="public",
            )
            created_id = getattr(reservation, "allocation_id", None)
            if not isinstance(created_id, UUID):
                raise ValidationServiceError("validation_allocation_identity_missing")
            allocation_id = created_id
            if validation_source is None:
                result = await profile.run(
                    principal,
                    lease,
                    baseline_artifact_id,
                    eligibility,
                    allocation_id,
                )
            else:
                result = await profile.run(
                    principal,
                    lease,
                    baseline_artifact_id,
                    eligibility,
                    allocation_id,
                    validation_source=validation_source,
                )
            return self._phase_from_result(
                result,
                phase=phase,
                expected_allocation_id=allocation_id,
                baseline_artifact_id=baseline_artifact_id,
                eligibility=eligibility,
                source_archive_sha256=source_archive_sha256,
                source_tree_sha256=source_tree_sha256,
                baseline_source_tree_sha256=baseline_source_tree_sha256,
                baseline_archive_sha256=baseline_archive_sha256,
                candidate_archive_sha256=candidate_archive_sha256,
                candidate_source_tree_sha256=candidate_source_tree_sha256,
                expected_command_id=expected_profile[2],
                expected_image=expected_image,
            )
        except Exception as error:
            return self._phase_from_error(
                error,
                phase=phase,
                allocation_id=allocation_id,
                baseline_artifact_id=baseline_artifact_id,
                eligibility=eligibility,
                source_archive_sha256=source_archive_sha256,
                source_tree_sha256=source_tree_sha256,
                baseline_source_tree_sha256=baseline_source_tree_sha256,
                baseline_archive_sha256=baseline_archive_sha256,
                command_id=expected_profile[2],
                image=expected_image,
            )

    @staticmethod
    def _validate_source(
        source: ValidationSourceAuthorization,
        *,
        baseline_artifact_id: UUID,
        eligibility: EligibilityReceipt,
    ) -> None:
        if (
            not isinstance(source, ValidationSourceAuthorization)
            or not isinstance(source.baseline_artifact_id, UUID)
            or source.baseline_artifact_id != baseline_artifact_id
            or not isinstance(source.eligibility, EligibilityReceipt)
            or source.eligibility != eligibility
            or source.profile_id != eligibility.profile_id
            or source.profile_version != eligibility.profile_version
            or not isinstance(source.candidate_archive, bytes)
            or not isinstance(source.candidate_manifest, ArchiveManifest)
            or not isinstance(source.changed_paths, tuple)
            or not 0 <= len(source.changed_paths) <= 10
            or any(not isinstance(path, str) for path in source.changed_paths)
            or len(set(source.changed_paths)) != len(source.changed_paths)
            or type(source.candidate_file_count) is not int
            or not 0 <= source.candidate_file_count <= 5_000
            or type(source.candidate_source_bytes) is not int
            or not 0 <= source.candidate_source_bytes <= 50 * 1024 * 1024
            or not _is_sha256(source.baseline_archive_sha256)
            or not _is_sha256(source.baseline_source_tree_sha256)
            or not _is_sha256(source.candidate_archive_sha256)
            or not _is_sha256(source.candidate_source_tree_sha256)
            or not _is_sha256(source.lineage_sha256)
            or hashlib.sha256(source.candidate_archive).hexdigest()
            != source.candidate_archive_sha256
        ):
            raise ValidationServiceError("validation_source_identity_mismatch")
        manifest = source.candidate_manifest
        if (
            manifest.format_version != 1
            or manifest.tree_sha256 != source.candidate_source_tree_sha256
            or len(manifest.entries) != source.candidate_file_count
            or sum(entry.size for entry in manifest.entries) != source.candidate_source_bytes
        ):
            raise ValidationServiceError("validation_candidate_manifest_mismatch")
        try:
            verify_archive(
                source.candidate_archive,
                expected_archive_sha256=source.candidate_archive_sha256,
                expected_tree_sha256=source.candidate_source_tree_sha256,
                expected_manifest=manifest,
            )
        except ArchiveError as error:
            raise ValidationServiceError(error.code) from None

    @staticmethod
    def _phase_from_result(
        result: ProfileResult,
        *,
        phase: ValidationPhase,
        expected_allocation_id: UUID,
        baseline_artifact_id: UUID,
        eligibility: EligibilityReceipt,
        source_archive_sha256: str,
        source_tree_sha256: str,
        baseline_source_tree_sha256: str,
        baseline_archive_sha256: str,
        candidate_archive_sha256: str | None,
        candidate_source_tree_sha256: str | None,
        expected_command_id: str,
        expected_image: str | None,
    ) -> PhaseEvidence:
        runtime_version = getattr(result, "runtime_version", None)
        npm_version: str | None = None
        if eligibility.profile_id == _PYTHON_PROFILE[0]:
            version_valid = runtime_version == PYTHON_TARGET_VERSION
        else:
            runtime_version = getattr(result, "node_version", None)
            npm_version = getattr(result, "npm_version", None)
            version_valid = runtime_version == NODE_VERSION and npm_version == NPM_VERSION
        if (
            result.allocation_id != expected_allocation_id
            or result.artifact_id != baseline_artifact_id
            or result.repository_id != eligibility.repository_id
            or result.commit_sha != eligibility.commit_sha
            or result.profile_id != eligibility.profile_id
            or result.profile_version != eligibility.profile_version
            or type(result.package_count) is not int
            or result.package_count != eligibility.package_count
            or result.validation_phase != phase.value
            or result.command_id != expected_command_id
            or not isinstance(expected_image, str)
            or not expected_image.strip()
            or result.image != expected_image
            or result.tree_sha256 != source_tree_sha256
            or result.baseline_git_tree_sha256 != eligibility.tree_sha
            or result.baseline_archive_tree_sha256 != baseline_source_tree_sha256
            or result.candidate_archive_sha256 != candidate_archive_sha256
            or result.candidate_source_tree_sha256 != candidate_source_tree_sha256
            or type(result.test_exit_code) is not int
            or type(result.test_passed) is not bool
            or result.test_passed != (result.test_exit_code == 0)
            or result.allocation_status not in _COMPLETED_ALLOCATION_STATES
            or not version_valid
        ):
            raise ValidationServiceError("validation_profile_result_mismatch")
        outcome = _test_outcome(eligibility.profile_id, result.test_exit_code)
        command_summaries = _summarize_commands(result.outputs)
        return PhaseEvidence(
            phase=phase,
            outcome=outcome,
            allocation_id=expected_allocation_id,
            artifact_id=baseline_artifact_id,
            repository_id=eligibility.repository_id,
            base_commit_sha=eligibility.commit_sha,
            baseline_git_tree_sha256=eligibility.tree_sha,
            profile_id=eligibility.profile_id,
            profile_version=eligibility.profile_version,
            image=result.image,
            runtime_version=runtime_version,
            npm_version=npm_version,
            command_id=expected_command_id,
            baseline_archive_sha256=baseline_archive_sha256,
            baseline_archive_tree_sha256=baseline_source_tree_sha256,
            source_archive_sha256=source_archive_sha256,
            source_tree_sha256=source_tree_sha256,
            test_exit_code=result.test_exit_code,
            package_count=eligibility.package_count,
            allocation_status=result.allocation_status,
            commands=command_summaries,
        )

    @staticmethod
    def _phase_from_error(
        error: Exception,
        *,
        phase: ValidationPhase,
        allocation_id: UUID | None,
        baseline_artifact_id: UUID,
        eligibility: EligibilityReceipt,
        source_archive_sha256: str,
        source_tree_sha256: str,
        baseline_source_tree_sha256: str,
        baseline_archive_sha256: str,
        command_id: str,
        image: str | None,
    ) -> PhaseEvidence:
        code = getattr(error, "code", None)
        if not isinstance(code, str) or not _SAFE_CODE.fullmatch(code):
            code = "validation_profile_error"
        outcome = _classify_error(code)
        return PhaseEvidence(
            phase=phase,
            outcome=outcome,
            allocation_id=allocation_id,
            artifact_id=baseline_artifact_id,
            repository_id=eligibility.repository_id,
            base_commit_sha=eligibility.commit_sha,
            baseline_git_tree_sha256=eligibility.tree_sha,
            profile_id=eligibility.profile_id,
            profile_version=eligibility.profile_version,
            image=image.strip() if isinstance(image, str) and image.strip() else None,
            runtime_version=None,
            npm_version=None,
            command_id=command_id,
            baseline_archive_sha256=baseline_archive_sha256,
            baseline_archive_tree_sha256=baseline_source_tree_sha256,
            source_archive_sha256=source_archive_sha256,
            source_tree_sha256=source_tree_sha256,
            package_count=eligibility.package_count,
            error_code=code,
            commands=(),
        )

    @staticmethod
    def _not_run_phase(
        *,
        phase: ValidationPhase,
        allocation_id: UUID | None,
        baseline_artifact_id: UUID,
        eligibility: EligibilityReceipt,
        source_archive_sha256: str,
        source_tree_sha256: str,
        baseline_source_tree_sha256: str,
        baseline_archive_sha256: str,
        command_id: str,
        image: str | None,
    ) -> PhaseEvidence:
        return PhaseEvidence(
            phase=phase,
            outcome=PhaseOutcome.NOT_RUN,
            allocation_id=allocation_id,
            artifact_id=baseline_artifact_id,
            repository_id=eligibility.repository_id,
            base_commit_sha=eligibility.commit_sha,
            baseline_git_tree_sha256=eligibility.tree_sha,
            profile_id=eligibility.profile_id,
            profile_version=eligibility.profile_version,
            image=image.strip() if isinstance(image, str) and image.strip() else None,
            runtime_version=None,
            npm_version=None,
            command_id=command_id,
            baseline_archive_sha256=baseline_archive_sha256,
            baseline_archive_tree_sha256=baseline_source_tree_sha256,
            source_archive_sha256=source_archive_sha256,
            source_tree_sha256=source_tree_sha256,
            package_count=eligibility.package_count,
            commands=(),
        )

    @staticmethod
    def _baseline_classification(outcome: PhaseOutcome) -> ValidationClassification:
        if outcome is PhaseOutcome.UNSUPPORTED:
            return ValidationClassification.BASELINE_UNSUPPORTED
        if outcome is PhaseOutcome.SETUP_FAILED:
            return ValidationClassification.BASELINE_SETUP_FAILED
        return ValidationClassification.BASELINE_UNKNOWN

    @staticmethod
    def _candidate_classification(
        baseline: PhaseOutcome, candidate: PhaseOutcome
    ) -> ValidationClassification:
        if candidate is PhaseOutcome.PASSED:
            if baseline is PhaseOutcome.TEST_FAILED:
                return ValidationClassification.PASSED_WITH_BASELINE_FAILURE
            return ValidationClassification.PASSED
        if candidate is PhaseOutcome.TEST_FAILED:
            return ValidationClassification.CANDIDATE_TEST_FAILED
        if candidate is PhaseOutcome.UNSUPPORTED:
            return ValidationClassification.CANDIDATE_UNSUPPORTED
        if candidate is PhaseOutcome.SETUP_FAILED:
            return ValidationClassification.CANDIDATE_SETUP_FAILED
        return ValidationClassification.CANDIDATE_UNKNOWN


def _classify_error(code: str) -> PhaseOutcome:
    lowered = code.casefold()
    if "unsupported" in lowered:
        return PhaseOutcome.UNSUPPORTED
    if "exit" in lowered and any(marker in lowered for marker in ("missing", "unknown")):
        return PhaseOutcome.UNKNOWN
    if "cleanup" in lowered and any(
        marker in lowered for marker in ("missing", "unknown", "unresolved", "failed")
    ):
        return PhaseOutcome.UNKNOWN
    if "authorization" in lowered and any(
        marker in lowered for marker in ("invalid", "mismatch", "unresolved")
    ):
        return PhaseOutcome.UNKNOWN
    if any(
        marker in lowered
        for marker in (
            "unknown",
            "unavailable",
            "lease_lost",
            "deadline",
            "timeout",
            "unresolved",
            "not_observed",
            "identity_mismatch",
            "mismatch",
            "not_current",
            "effect",
        )
    ):
        return PhaseOutcome.UNKNOWN
    return PhaseOutcome.SETUP_FAILED


def _summarize_commands(outputs: Sequence[object]) -> tuple[CommandEvidence, ...]:
    if len(outputs) > 24:
        raise ValidationServiceError("validation_command_count_limit")
    summaries: list[CommandEvidence] = []
    for item in outputs:
        phase = getattr(item, "phase", None)
        exit_code = getattr(item, "exit_code", None)
        stdout = getattr(item, "stdout", None)
        stderr = getattr(item, "stderr", None)
        if (
            not isinstance(phase, str)
            or not isinstance(stdout, str)
            or not isinstance(stderr, str)
            or (exit_code is not None and type(exit_code) is not int)
        ):
            raise ValidationServiceError("validation_command_summary_invalid")
        stdout_bytes = stdout.encode("utf-8", errors="replace")
        stderr_bytes = stderr.encode("utf-8", errors="replace")
        summaries.append(
            CommandEvidence(
                phase=phase,
                exit_code=exit_code,
                stdout_bytes=len(stdout_bytes),
                stderr_bytes=len(stderr_bytes),
                stdout_sha256=hashlib.sha256(stdout_bytes).hexdigest(),
                stderr_sha256=hashlib.sha256(stderr_bytes).hexdigest(),
            )
        )
    return tuple(summaries)


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def _allocation_key(source: ValidationSourceAuthorization, phase: ValidationPhase) -> str:
    return f"{_VALIDATION_ALLOCATION_KEY_PREFIX}:{source.lineage_sha256}:{phase.value}"


def _test_outcome(profile_id: str, exit_code: int) -> PhaseOutcome:
    if exit_code == 0:
        return PhaseOutcome.PASSED
    if profile_id == _PYTHON_PROFILE[0]:
        if exit_code in {2, 3}:
            return PhaseOutcome.UNKNOWN
        if exit_code in {4, 5}:
            return PhaseOutcome.UNSUPPORTED
    return PhaseOutcome.TEST_FAILED
