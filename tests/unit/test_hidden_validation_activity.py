from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from continuum_api.runtime.leases import ExecutionLease, LeaseLost
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.validation_activity import (
    CandidateValidationActivityHandler,
    CandidateValidationInput,
    HiddenCheckSelection,
)
from continuum_api.validation.contracts import (
    CommandEvidence,
    PhaseEvidence,
    PhaseOutcome,
    ValidationClassification,
    ValidationEvidence,
    ValidationPhase,
)
from continuum_api.validation.hidden_checks import (
    HiddenCheckCase,
    HiddenCheckResult,
    HiddenCheckSuite,
)
from continuum_api.validation.managed_hidden_checks import (
    ManagedHiddenCheckEvidence,
    _invocation_digest,
)


def _fixture(outcome: str = "passed"):
    owner, run_id, step_id, job_id, attempt_id = (uuid4() for _ in range(5))
    lease = ExecutionLease(
        owner, run_id, step_id, job_id, attempt_id, 2, "worker", datetime.now(UTC)
    )
    source = SimpleNamespace(
        profile_id="python-pytest",
        profile_version="1",
        candidate_archive_sha256="3" * 64,
        candidate_source_tree_sha256="4" * 64,
        candidate_manifest=SimpleNamespace(entries=(SimpleNamespace(path="solution.py"),)),
    )
    suite = HiddenCheckSuite(
        "suite-1",
        source.profile_id,
        source.profile_version,
        source.candidate_archive_sha256,
        source.candidate_source_tree_sha256,
        (HiddenCheckCase("case-1", "[1]", "[null]"),),
    )
    inputs = CandidateValidationInput(uuid4(), uuid4(), object(), source)
    phase_outcome = PhaseOutcome.PASSED if outcome == "passed" else PhaseOutcome.TEST_FAILED
    exit_code = 0 if outcome == "passed" else 1
    allocation_id = uuid4()
    command = CommandEvidence(
        phase="hidden_probe",
        exit_code=exit_code,
        stdout_bytes=0,
        stderr_bytes=0,
        stdout_sha256="5" * 64,
        stderr_sha256="6" * 64,
    )
    phase = PhaseEvidence(
        phase=ValidationPhase.CANDIDATE,
        outcome=phase_outcome,
        allocation_id=allocation_id,
        artifact_id=inputs.baseline_artifact_id,
        repository_id=101,
        base_commit_sha="a" * 40,
        baseline_git_tree_sha256="b" * 40,
        profile_id=source.profile_id,
        profile_version=source.profile_version,
        image="python:3.13@sha256:" + "f" * 64,
        runtime_version="3.13.12",
        command_id="python-hidden-probe-v1",
        baseline_archive_sha256="1" * 64,
        baseline_archive_tree_sha256="2" * 64,
        source_archive_sha256=source.candidate_archive_sha256,
        source_tree_sha256=source.candidate_source_tree_sha256,
        test_exit_code=exit_code,
        package_count=0,
        allocation_status="stopped",
        commands=(command,),
    )
    comparison = HiddenCheckResult(
        outcome=outcome,
        passed_count=1 if outcome == "passed" else 0,
        total_count=1,
        suite_sha256=suite.digest,
        candidate_archive_sha256=source.candidate_archive_sha256,
        candidate_tree_sha256=source.candidate_source_tree_sha256,
        stdout_sha256=command.stdout_sha256,
        stderr_sha256=command.stderr_sha256,
        code="hidden_checks_passed" if outcome == "passed" else "hidden_check_mismatch",
    )
    hidden_evidence = ManagedHiddenCheckEvidence(
        run_id=run_id,
        step_id=step_id,
        attempt_id=attempt_id,
        generation=2,
        suite_id=suite.suite_id,
        suite_sha256=suite.digest,
        entry_path="solution.py",
        invocation_sha256=_invocation_digest(suite.digest, "solution.py", "python-hidden-probe-v1"),
        execution=phase,
        comparison=comparison,
    )
    baseline_command = CommandEvidence(
        phase="pytest",
        exit_code=0,
        stdout_bytes=0,
        stderr_bytes=0,
        stdout_sha256="0" * 64,
        stderr_sha256="0" * 64,
    )
    baseline = PhaseEvidence(
        phase=ValidationPhase.BASELINE,
        outcome=PhaseOutcome.PASSED,
        allocation_id=uuid4(),
        artifact_id=inputs.baseline_artifact_id,
        repository_id=101,
        base_commit_sha="a" * 40,
        baseline_git_tree_sha256="b" * 40,
        profile_id=source.profile_id,
        profile_version=source.profile_version,
        image="python:3.13@sha256:" + "f" * 64,
        runtime_version="3.13.12",
        command_id="python-pytest-v1",
        baseline_archive_sha256="1" * 64,
        baseline_archive_tree_sha256="2" * 64,
        source_archive_sha256="1" * 64,
        source_tree_sha256="2" * 64,
        test_exit_code=0,
        package_count=0,
        allocation_status="stopped",
        commands=(baseline_command,),
    )
    candidate = PhaseEvidence(
        phase=ValidationPhase.CANDIDATE,
        outcome=PhaseOutcome.PASSED,
        allocation_id=uuid4(),
        artifact_id=inputs.baseline_artifact_id,
        repository_id=101,
        base_commit_sha="a" * 40,
        baseline_git_tree_sha256="b" * 40,
        profile_id=source.profile_id,
        profile_version=source.profile_version,
        image="python:3.13@sha256:" + "f" * 64,
        runtime_version="3.13.12",
        command_id="python-pytest-v1",
        baseline_archive_sha256="1" * 64,
        baseline_archive_tree_sha256="2" * 64,
        source_archive_sha256=source.candidate_archive_sha256,
        source_tree_sha256=source.candidate_source_tree_sha256,
        test_exit_code=0,
        package_count=0,
        allocation_status="stopped",
        commands=(baseline_command,),
    )
    standard = ValidationEvidence(
        classification=ValidationClassification.PASSED,
        baseline_artifact_id=inputs.baseline_artifact_id,
        candidate_archive_sha256=source.candidate_archive_sha256,
        candidate_source_tree_sha256=source.candidate_source_tree_sha256,
        candidate_lineage_sha256="8" * 64,
        changed_path_count=1,
        baseline=baseline,
        candidate=candidate,
    )
    return lease, inputs, suite, hidden_evidence, standard


def _handler(lease, inputs, suite, hidden_service, validation, approvals, transitions):
    handler = CandidateValidationActivityHandler(
        object(),
        Principal(lease.owner_id),
        lambda _lease: inputs,
        validation,
        approvals,
        transitions,
        hidden_checks=hidden_service,
        hidden_check_selector=lambda _lease, _inputs: HiddenCheckSelection(suite, "solution.py"),
    )
    handler._assert_current_candidate_step = lambda _lease, expected_revision=None: 4
    handler._assert_persisted_artifact_binding = lambda *_args: None
    return handler


def test_standard_validation_failure_skips_hidden_allocation() -> None:
    lease, inputs, suite, _hidden, evidence = _fixture()
    evidence = evidence.model_copy(
        update={"classification": ValidationClassification.CANDIDATE_TEST_FAILED}
    )

    class Validation:
        async def run(self, *_args):
            return evidence

    class Hidden:
        async def run(self, *_args, **_kwargs):
            pytest.fail("hidden checks must wait for ordinary validation to pass")

    transitions = SimpleNamespace(commit_result=lambda **kwargs: kwargs)
    handler = _handler(lease, inputs, suite, Hidden(), Validation(), object(), transitions)
    result = asyncio.run(handler(lease))

    assert result["target_run_status"].value == "needs_attention"


@pytest.mark.parametrize("hidden_outcome", ["passed", "failed"])
def test_hidden_result_is_committed_only_after_standard_validation(hidden_outcome: str) -> None:
    lease, inputs, suite, hidden_evidence, standard = _fixture(hidden_outcome)
    order: list[str] = []

    class Validation:
        async def run(self, *_args):
            order.append("standard")
            return standard

    class Hidden:
        async def run(self, *_args, **kwargs):
            order.append("hidden")
            assert kwargs["allow_operator_sandbox"] is True
            assert kwargs["entry_path"] == "solution.py"
            return hidden_evidence

    class Approvals:
        def record_validation(self, *_args, **kwargs):
            order.append("approval")
            return kwargs

    def commit_result(**kwargs):
        order.append("attention")
        return kwargs

    transitions = SimpleNamespace(commit_result=commit_result)
    handler = _handler(lease, inputs, suite, Hidden(), Validation(), Approvals(), transitions)
    result = asyncio.run(handler(lease))

    assert order[:2] == ["standard", "hidden"]
    if hidden_outcome == "passed":
        assert order[-1] == "approval"
        assert result["hidden_check_evidence"].model_dump(
            mode="json"
        ) == hidden_evidence.model_dump(mode="json")
    else:
        assert order[-1] == "attention"
        assert result["result"]["classification"] == "hidden_check_failed"
        assert result["result"]["validation_evidence"] == standard.model_dump(mode="json")
        assert result["result"]["hidden_check_evidence"] == hidden_evidence.model_dump(mode="json")


def test_lease_loss_after_hidden_execution_never_commits_acceptance() -> None:
    lease, inputs, suite, hidden_evidence, standard = _fixture()

    class Validation:
        async def run(self, *_args):
            return standard

    class Hidden:
        async def run(self, *_args, **_kwargs):
            return hidden_evidence

    approvals = SimpleNamespace(
        record_validation=lambda *_args, **_kwargs: pytest.fail("stale accept")
    )
    handler = _handler(lease, inputs, suite, Hidden(), Validation(), approvals, object())
    checks = 0

    def fence(_lease, expected_revision=None):
        nonlocal checks
        checks += 1
        if checks >= 5:
            raise LeaseLost
        return 4

    handler._assert_current_candidate_step = fence
    with pytest.raises(LeaseLost):
        asyncio.run(handler(lease))


def test_selection_boundary_preserves_lease_loss_before_validation() -> None:
    lease, inputs, suite, _hidden_evidence, _standard = _fixture()

    class Validation:
        async def run(self, *_args):
            pytest.fail("stale selection must not start standard validation")

    class Hidden:
        async def run(self, *_args, **_kwargs):
            pytest.fail("stale selection must not start hidden checks")

    handler = _handler(lease, inputs, suite, Hidden(), Validation(), object(), object())

    def selector(_lease, _inputs):
        raise LeaseLost

    handler._hidden_check_selector = selector
    with pytest.raises(LeaseLost):
        asyncio.run(handler(lease))


def test_selection_is_refenced_before_standard_validation_cost() -> None:
    lease, inputs, suite, _hidden_evidence, _standard = _fixture()

    class Validation:
        async def run(self, *_args):
            pytest.fail("selection that crossed a stale fence must not validate")

    class Hidden:
        async def run(self, *_args, **_kwargs):
            pytest.fail("selection that crossed a stale fence must not allocate")

    handler = _handler(lease, inputs, suite, Hidden(), Validation(), object(), object())
    checks = 0

    def fence(_lease, expected_revision=None):
        nonlocal checks
        checks += 1
        if checks == 2:
            raise LeaseLost
        return 4

    handler._assert_current_candidate_step = fence
    with pytest.raises(LeaseLost):
        asyncio.run(handler(lease))


def test_hidden_service_error_becomes_unknown_attention_with_sanitized_code() -> None:
    lease, inputs, suite, _hidden_evidence, standard = _fixture()

    class Validation:
        async def run(self, *_args):
            return standard

    class Hidden:
        async def run(self, *_args, **_kwargs):
            raise RuntimeError("candidate output and secret material")

    transitions = SimpleNamespace(commit_result=lambda **kwargs: kwargs)
    handler = _handler(lease, inputs, suite, Hidden(), Validation(), object(), transitions)
    committed = asyncio.run(handler(lease))
    result = committed["result"]

    assert result["classification"] == "hidden_check_unknown"
    assert result["hidden_check_status"] == "unknown"
    assert result["hidden_check_error_code"] == "hidden_check_service_failed"
    assert result["run_revision"] == 4
    assert "candidate output" not in str(result)
