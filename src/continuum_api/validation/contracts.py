"""Bounded, versioned evidence for baseline and candidate validation."""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_VALIDATION_EVIDENCE_BYTES = 65_536
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_CODE = re.compile(r"^[a-z0-9_]{1,96}$")
_SAFE_COMMAND = re.compile(r"^[a-z0-9_-]{1,64}$")


class ValidationContractError(ValueError):
    """Stable contract error that never includes repository content."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class ValidationPhase(StrEnum):
    BASELINE = "baseline"
    CANDIDATE = "candidate"


class PhaseOutcome(StrEnum):
    PASSED = "passed"
    TEST_FAILED = "test_failed"
    UNSUPPORTED = "unsupported"
    SETUP_FAILED = "setup_failed"
    UNKNOWN = "unknown"
    NOT_RUN = "not_run"


class ValidationClassification(StrEnum):
    PASSED = "passed"
    PASSED_WITH_BASELINE_FAILURE = "passed_with_baseline_failure"
    CANDIDATE_TEST_FAILED = "candidate_test_failed"
    BASELINE_UNSUPPORTED = "baseline_unsupported"
    BASELINE_SETUP_FAILED = "baseline_setup_failed"
    BASELINE_UNKNOWN = "baseline_unknown"
    CANDIDATE_UNSUPPORTED = "candidate_unsupported"
    CANDIDATE_SETUP_FAILED = "candidate_setup_failed"
    CANDIDATE_UNKNOWN = "candidate_unknown"


class _StrictEvidence(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, revalidate_instances="always"
    )


class CommandEvidence(_StrictEvidence):
    """Digest-only summary of one bounded fixed profile command."""

    phase: str = Field(min_length=1, max_length=64)
    exit_code: int | None = Field(default=None, ge=-255, le=255)
    stdout_bytes: int = Field(ge=0, le=2 * 1024 * 1024)
    stderr_bytes: int = Field(ge=0, le=2 * 1024 * 1024)
    stdout_sha256: str
    stderr_sha256: str

    @model_validator(mode="after")
    def validate_values(self) -> Self:
        if not self.phase.isascii() or not re.fullmatch(r"[a-z0-9_-]{1,64}", self.phase):
            raise ValidationContractError("validation_command_phase_invalid")
        if (
            _SHA256.fullmatch(self.stdout_sha256) is None
            or _SHA256.fullmatch(self.stderr_sha256) is None
        ):
            raise ValidationContractError("validation_command_digest_invalid")
        return self


class PhaseEvidence(_StrictEvidence):
    phase: ValidationPhase
    outcome: PhaseOutcome
    allocation_id: UUID | None = None
    artifact_id: UUID
    repository_id: int = Field(gt=0)
    base_commit_sha: str = Field(min_length=40, max_length=64)
    baseline_git_tree_sha256: str = Field(min_length=40, max_length=64)
    profile_id: str = Field(min_length=1, max_length=64)
    profile_version: str = Field(min_length=1, max_length=32)
    image: str | None = Field(default=None, max_length=256)
    runtime_version: str | None = Field(default=None, max_length=32)
    npm_version: str | None = Field(default=None, max_length=32)
    command_id: str = Field(min_length=1, max_length=64)
    baseline_archive_sha256: str = Field(min_length=64, max_length=64)
    baseline_archive_tree_sha256: str = Field(min_length=64, max_length=64)
    source_archive_sha256: str = Field(min_length=64, max_length=64)
    source_tree_sha256: str = Field(min_length=64, max_length=64)
    test_exit_code: int | None = Field(default=None, ge=-255, le=255)
    package_count: int = Field(ge=0, le=24)
    allocation_status: str | None = Field(default=None, max_length=32)
    error_code: str | None = Field(default=None, max_length=96)
    commands: tuple[CommandEvidence, ...] = Field(max_length=24)

    @model_validator(mode="after")
    def validate_values(self) -> Self:
        for value in (self.base_commit_sha, self.baseline_git_tree_sha256):
            if len(value) not in {40, 64} or re.fullmatch(r"[0-9a-f]+", value) is None:
                raise ValidationContractError("validation_git_identity_invalid")
        if any(
            _SHA256.fullmatch(value) is None
            for value in (
                self.baseline_archive_sha256,
                self.baseline_archive_tree_sha256,
                self.source_archive_sha256,
                self.source_tree_sha256,
            )
        ):
            raise ValidationContractError("validation_source_digest_invalid")
        if _SAFE_COMMAND.fullmatch(self.command_id) is None:
            raise ValidationContractError("validation_command_id_invalid")
        if self.error_code is not None and _SAFE_CODE.fullmatch(self.error_code) is None:
            raise ValidationContractError("validation_error_code_invalid")
        return self


class ValidationEvidence(_StrictEvidence):
    """Prepared evidence. It becomes durable only with a fenced T08 commit."""

    evidence_version: Literal[1] = 1
    classification: ValidationClassification
    baseline_artifact_id: UUID
    candidate_archive_sha256: str = Field(min_length=64, max_length=64)
    candidate_source_tree_sha256: str = Field(min_length=64, max_length=64)
    candidate_lineage_sha256: str = Field(min_length=64, max_length=64)
    changed_path_count: int = Field(ge=0, le=10)
    regression_on_original: Literal["absent"] = "absent"
    baseline: PhaseEvidence
    candidate: PhaseEvidence

    @model_validator(mode="after")
    def validate_values(self) -> Self:
        for value in (
            self.candidate_archive_sha256,
            self.candidate_source_tree_sha256,
            self.candidate_lineage_sha256,
        ):
            if _SHA256.fullmatch(value) is None:
                raise ValidationContractError("validation_candidate_digest_invalid")
        if self.baseline.phase is not ValidationPhase.BASELINE:
            raise ValidationContractError("validation_baseline_phase_invalid")
        if self.candidate.phase is not ValidationPhase.CANDIDATE:
            raise ValidationContractError("validation_candidate_phase_invalid")
        if self.baseline.outcome is PhaseOutcome.NOT_RUN:
            raise ValidationContractError("validation_baseline_not_run_invalid")
        if (
            self.candidate.outcome is PhaseOutcome.NOT_RUN
            and self.candidate.allocation_id is not None
        ):
            raise ValidationContractError("validation_not_run_allocation_invalid")
        if self.baseline.artifact_id != self.baseline_artifact_id:
            raise ValidationContractError("validation_baseline_artifact_mismatch")
        if self.candidate.artifact_id != self.baseline_artifact_id:
            raise ValidationContractError("validation_candidate_baseline_mismatch")
        if (
            self.baseline.allocation_id is not None
            and self.candidate.allocation_id is not None
            and self.baseline.allocation_id == self.candidate.allocation_id
        ):
            raise ValidationContractError("validation_allocations_must_be_distinct")
        if (
            self.baseline.source_archive_sha256 != self.baseline.baseline_archive_sha256
            or self.baseline.source_tree_sha256 != self.baseline.baseline_archive_tree_sha256
            or self.candidate.baseline_archive_sha256 != self.baseline.baseline_archive_sha256
            or self.candidate.baseline_archive_tree_sha256
            != self.baseline.baseline_archive_tree_sha256
            or self.candidate.source_archive_sha256 != self.candidate_archive_sha256
            or self.candidate.source_tree_sha256 != self.candidate_source_tree_sha256
        ):
            raise ValidationContractError("validation_source_phase_identity_mismatch")
        if self.candidate.outcome is PhaseOutcome.NOT_RUN and self.classification not in {
            ValidationClassification.BASELINE_UNSUPPORTED,
            ValidationClassification.BASELINE_SETUP_FAILED,
            ValidationClassification.BASELINE_UNKNOWN,
        }:
            raise ValidationContractError("validation_not_run_classification_mismatch")
        if (
            self.classification
            in {
                ValidationClassification.PASSED,
                ValidationClassification.PASSED_WITH_BASELINE_FAILURE,
            }
            and self.candidate.outcome is not PhaseOutcome.PASSED
        ):
            raise ValidationContractError("validation_pass_without_candidate_pass")
        if self.classification is ValidationClassification.PASSED and (
            self.baseline.outcome is not PhaseOutcome.PASSED
        ):
            raise ValidationContractError("validation_baseline_classification_mismatch")
        if (
            self.classification is ValidationClassification.PASSED_WITH_BASELINE_FAILURE
            and self.baseline.outcome is not PhaseOutcome.TEST_FAILED
        ):
            raise ValidationContractError("validation_baseline_failure_classification_mismatch")
        serialized_size = len(self.model_dump_json(warnings=False).encode("utf-8"))
        if serialized_size > MAX_VALIDATION_EVIDENCE_BYTES:
            raise ValidationContractError("validation_evidence_size_limit")
        return self
