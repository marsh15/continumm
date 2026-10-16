from __future__ import annotations

from uuid import uuid4

import pytest

from continuum_api.runtime.approvals import ApprovalError, _validate_pass_evidence
from continuum_api.validation.contracts import (
    CommandEvidence,
    PhaseEvidence,
    PhaseOutcome,
    ValidationClassification,
    ValidationEvidence,
    ValidationPhase,
)


def _evidence() -> ValidationEvidence:
    baseline_id, candidate_id = uuid4(), uuid4()
    command = CommandEvidence(
        phase="pytest",
        exit_code=0,
        stdout_bytes=0,
        stderr_bytes=0,
        stdout_sha256="0" * 64,
        stderr_sha256="0" * 64,
    )
    common = {
        "allocation_id": baseline_id,
        "artifact_id": uuid4(),
        "repository_id": 101,
        "base_commit_sha": "a" * 40,
        "baseline_git_tree_sha256": "b" * 40,
        "profile_id": "python-pytest",
        "profile_version": "1",
        "image": f"python:3.13@sha256:{'f' * 64}",
        "runtime_version": "3.13.12",
        "command_id": "python-pytest-v1",
        "baseline_archive_sha256": "1" * 64,
        "baseline_archive_tree_sha256": "2" * 64,
        "source_archive_sha256": "1" * 64,
        "source_tree_sha256": "2" * 64,
        "test_exit_code": 0,
        "package_count": 0,
        "allocation_status": "stopped",
        "commands": (command,),
    }
    baseline = PhaseEvidence(phase=ValidationPhase.BASELINE, outcome=PhaseOutcome.PASSED, **common)
    candidate = PhaseEvidence(
        phase=ValidationPhase.CANDIDATE,
        outcome=PhaseOutcome.PASSED,
        **{
            **common,
            "allocation_id": candidate_id,
            "source_archive_sha256": "3" * 64,
            "source_tree_sha256": "4" * 64,
        },
    )
    return ValidationEvidence(
        classification=ValidationClassification.PASSED,
        baseline_artifact_id=baseline.artifact_id,
        candidate_archive_sha256="3" * 64,
        candidate_source_tree_sha256="4" * 64,
        candidate_lineage_sha256="5" * 64,
        changed_path_count=0,
        baseline=baseline,
        candidate=candidate,
    )


def test_approval_requires_consistent_passing_validation() -> None:
    _validate_pass_evidence(_evidence())


def test_candidate_pass_label_cannot_hide_a_failed_exit() -> None:
    evidence = _evidence()
    with pytest.raises(ApprovalError, match="approval_candidate_validation_failed"):
        _validate_pass_evidence(
            evidence.model_copy(
                update={"candidate": evidence.candidate.model_copy(update={"test_exit_code": 1})}
            )
        )


def test_baseline_failure_cannot_be_labeled_as_a_pass() -> None:
    evidence = _evidence()
    corrupted_baseline = evidence.baseline.model_copy(
        update={"test_exit_code": 1, "error_code": "profile_error"}
    )
    with pytest.raises(ApprovalError):
        _validate_pass_evidence(evidence.model_copy(update={"baseline": corrupted_baseline}))


@pytest.mark.parametrize(
    "field,value",
    [
        ("image", "python:latest"),
        ("image", "python@sha256:" + "a" * 64),
        ("package_count", 1),
    ],
)
def test_baseline_and_candidate_environment_must_match(field: str, value: object) -> None:
    evidence = _evidence()
    with pytest.raises(ApprovalError):
        _validate_pass_evidence(
            evidence.model_copy(
                update={
                    "candidate": evidence.candidate.model_copy(update={field: value}),
                }
            )
        )
