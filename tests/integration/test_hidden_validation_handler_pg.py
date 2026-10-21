"""PostgreSQL proof for the lease-fenced hidden validation activity boundary."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_agent_loop import T22Database
from test_agent_loop import t22_database as t22_database
from test_approvals import (
    _close_scenario,
    _seed_allocations,
    create_approval_scenario,
)
from test_candidate_validation import _LOCK, _Profile
from test_live_evaluation_pg import _seed_admission_pins

from continuum_api.artifacts.archives import ArchiveFile
from continuum_api.repositories.preflight import EligibilityReceipt
from continuum_api.runtime.approvals import ApprovalService
from continuum_api.runtime.leases import LeaseLost
from continuum_api.runtime.live_evaluation import read_live_evaluation_snapshot
from continuum_api.runtime.persistence import RunStatus, RuntimeRepository
from continuum_api.runtime.validation_activity import (
    ApprovalFreezeActivityHandler,
    CandidateValidationActivityHandler,
    CandidateValidationInput,
    HiddenCheckSelection,
)
from continuum_api.sandbox.python_profile import _git_blob_sha, _git_tree_sha
from continuum_api.sandbox.validation.preparation import prepare_validation_source
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import ToolPolicyContext
from continuum_api.validation.contracts import CommandEvidence, PhaseOutcome
from continuum_api.validation.hidden_checks import (
    HiddenCheckCase,
    HiddenCheckResult,
    HiddenCheckSuite,
)
from continuum_api.validation.managed_hidden_checks import (
    ManagedHiddenCheckEvidence,
    _invocation_digest,
)
from continuum_api.validation.service import ValidationService

_PERIOD = "t27-approval-fixture"


def _baseline_files() -> tuple[ArchiveFile, ...]:
    return (
        ArchiveFile("pyproject.toml", b"[tool.pytest.ini_options]\ntestpaths = ['tests']\n"),
        ArchiveFile("requirements.lock", _LOCK),
        ArchiveFile("src/app.py", b"def answer():\n    return 'base'\n"),
        ArchiveFile(
            "tests/test_app.py",
            b"from src.app import answer\n\ndef test_answer():\n    assert answer() == 'base'\n",
        ),
    )


def _prepare_inputs(store: T22Database, tmp_path: Path):
    from test_approvals import _BASE_SHA, _REPOSITORY_ID

    baseline_files = _baseline_files()
    scenario = create_approval_scenario(
        store, tmp_path, prepare_only=True, baseline_files=baseline_files
    )
    baseline = scenario.baseline_verified
    eligibility = EligibilityReceipt(
        repository_id=_REPOSITORY_ID,
        commit_sha=_BASE_SHA,
        tree_sha=_git_tree_sha(baseline_files, "sha1"),
        profile_id="python-pytest",
        profile_version="1",
        file_count=len(baseline.manifest.entries),
        source_bytes=sum(item.size for item in baseline.manifest.entries),
        manifest_blobs=(
            (
                "pyproject.toml",
                _git_blob_sha(b"[tool.pytest.ini_options]\ntestpaths = ['tests']\n", "sha1"),
            ),
            ("requirements.lock", _git_blob_sha(_LOCK, "sha1")),
        ),
        package_count=1,
    )
    tool_context = ToolPolicyContext(
        capabilities=frozenset(ToolCapability),
        profile_id=eligibility.profile_id,
        profile_version=eligibility.profile_version,
        actor_can_write_repository=True,
        app_can_access_repository=True,
        stage=ToolStage.AGENT_WORK,
        run_budget_available=True,
        baseline_tree_sha256=scenario.run.baseline_tree_sha256,
        baseline_source_paths=frozenset(scenario.run.baseline_source_paths),
        baseline_test_paths=frozenset(scenario.run.baseline_test_paths),
        baseline_paths_verified=True,
        protected_paths=frozenset(),
    )
    source = prepare_validation_source(
        eligibility,
        scenario.baseline_artifact_id,
        baseline,
        scenario.candidate_patch,
        tool_context,
    )
    entry_path = "src/app.py"
    suite = HiddenCheckSuite(
        "handler-suite",
        source.profile_id,
        source.profile_version,
        source.candidate_archive_sha256,
        source.candidate_source_tree_sha256,
        (HiddenCheckCase("answer-case", "[]", "null"),),
    )
    assert entry_path in {item.path for item in source.candidate_manifest.entries}
    return scenario, eligibility, source, suite, entry_path


def _make_handler_context(store: T22Database, tmp_path: Path, *, hidden_outcome: str = "passed"):
    scenario, eligibility, source, suite, entry_path = _prepare_inputs(store, tmp_path)
    normal_allocation_ids = (uuid4(), uuid4())
    hidden_allocation_id = uuid4()

    class FixedAllocator:
        def __init__(self) -> None:
            self.index = 0

        async def create(self, *_args: Any, **_kwargs: Any) -> Any:
            allocation_id = normal_allocation_ids[self.index]
            self.index += 1
            return SimpleNamespace(allocation_id=allocation_id)

    profile = _Profile(source, [])
    allocator = FixedAllocator()
    validation_service = ValidationService(profile, profile, allocator, allocation_class="operator")

    class ValidationCapture:
        evidence = None

        async def run(self, *args: Any, **kwargs: Any):
            self.evidence = await validation_service.run(*args, **kwargs)
            return self.evidence

    validation = ValidationCapture()
    _seed_allocations(
        store,
        scenario.run,
        scenario.validation_lease,
        normal_allocation_ids,
        scenario.evidence,
        lineage_sha256=source.lineage_sha256,
    )
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_allocations "
                "SET allocation_class='operator' WHERE owner_id=:owner AND run_id=:run "
                "AND allocation_id = ANY(:ids)"
            ),
            {
                "owner": scenario.run.owner_id,
                "run": scenario.run.run_id,
                "ids": list(normal_allocation_ids),
            },
        )

    suite_digest = suite.digest
    invocation = _invocation_digest(suite_digest, entry_path, "python-hidden-probe-v1")
    hidden_sha = hashlib.sha256(b"hidden probe output").hexdigest()
    empty_sha = hashlib.sha256(b"").hexdigest()
    now = scenario.validation_lease.lease_expires_at
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.sandbox_period_usage "
                "(provider_period_id, allocation_class, charged_cpu_seconds) "
                "VALUES (:period, 'operator', 0) ON CONFLICT DO NOTHING"
            ),
            {"period": _PERIOD},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.sandbox_allocations "
                "(allocation_id, owner_id, run_id, step_id, job_id, generation, attempt_id, "
                "lease_owner, logical_key, request_sha256, provider_name, provider_period_id, "
                "allocation_class, requested_seconds, vcpus, memory_mb, reserved_cpu_seconds, "
                "status, create_authorized_at, last_observed_at, last_sandbox_status) VALUES "
                "(:id, :owner, :run, :step, :job, :generation, :attempt, :lease_owner, "
                ":logical_key, :request_sha, :provider, :period, 'operator', 120, 2, 4096, 240, "
                "'stopped', :now, :now, 'stopped')"
            ),
            {
                "id": hidden_allocation_id,
                "owner": scenario.run.owner_id,
                "run": scenario.run.run_id,
                "step": scenario.validation_lease.step_id,
                "job": scenario.validation_lease.job_id,
                "generation": scenario.validation_lease.generation,
                "attempt": scenario.validation_lease.attempt_id,
                "lease_owner": scenario.validation_lease.lease_owner,
                "logical_key": f"t48:hidden:v1:{invocation}",
                "request_sha": bytes.fromhex(hashlib.sha256(b"hidden-handler").hexdigest()),
                "provider": f"continuum-{hidden_allocation_id.hex}",
                "period": _PERIOD,
                "now": now,
            },
        )

    class FakeHiddenCheckService:
        expire_lease = False

        async def run(
            self,
            _principal,
            lease,
            selected_source,
            selected_suite,
            *,
            entry_path: str,
            allow_operator_sandbox: bool,
        ) -> ManagedHiddenCheckEvidence:
            assert selected_source is source
            assert selected_suite is suite
            assert entry_path == "src/app.py"
            assert allow_operator_sandbox is True
            assert validation.evidence is not None
            if self.expire_lease:
                with store.admin_engine.begin() as connection:
                    connection.execute(
                        text(
                            "UPDATE runtime_private.jobs SET lease_expires_at="
                            "clock_timestamp()-interval '1 second' "
                            "WHERE owner_id=:owner AND run_id=:run AND job_id=:job"
                        ),
                        {
                            "owner": lease.owner_id,
                            "run": lease.run_id,
                            "job": lease.job_id,
                        },
                    )
            passed = hidden_outcome == "passed"
            exit_code = 0 if passed else 1
            stdout = hidden_sha if passed else hashlib.sha256(b"mismatch").hexdigest()
            phase = validation.evidence.candidate.model_copy(
                update={
                    "allocation_id": hidden_allocation_id,
                    "allocation_status": "stopped",
                    "outcome": PhaseOutcome.PASSED if passed else PhaseOutcome.TEST_FAILED,
                    "test_exit_code": exit_code,
                    "command_id": "python-hidden-probe-v1",
                    "commands": (
                        CommandEvidence(
                            phase="hidden_probe",
                            exit_code=exit_code,
                            stdout_bytes=1,
                            stderr_bytes=0,
                            stdout_sha256=stdout,
                            stderr_sha256=empty_sha,
                        ),
                    ),
                }
            )
            return ManagedHiddenCheckEvidence(
                run_id=lease.run_id,
                step_id=lease.step_id,
                attempt_id=lease.attempt_id,
                generation=lease.generation,
                suite_id=suite.suite_id,
                suite_sha256=suite_digest,
                entry_path=entry_path,
                invocation_sha256=invocation,
                execution=phase,
                comparison=HiddenCheckResult(
                    outcome="passed" if passed else "failed",
                    passed_count=1 if passed else 0,
                    total_count=1,
                    suite_sha256=suite_digest,
                    candidate_archive_sha256=source.candidate_archive_sha256,
                    candidate_tree_sha256=source.candidate_source_tree_sha256,
                    stdout_sha256=stdout,
                    stderr_sha256=empty_sha,
                    code="hidden_checks_passed" if passed else "hidden_check_mismatch",
                ),
            )

    hidden_service = FakeHiddenCheckService()
    approval_service = ApprovalService(
        store.runtime_engine,
        scenario.principal,
        scenario.transitions,
        scenario.storage,
        publication_facts=scenario.facts_provider,
        validation_allocation_class="operator",
    )
    handler = CandidateValidationActivityHandler(
        store.runtime_engine,
        scenario.principal,
        lambda _lease: CandidateValidationInput(
            scenario.baseline_artifact_id,
            scenario.candidate_artifact_id,
            eligibility,
            source,
        ),
        validation,
        approval_service,
        scenario.transitions,
        hidden_checks=hidden_service,
        hidden_check_selector=lambda _lease, _inputs: HiddenCheckSelection(suite, entry_path),
    )
    return scenario, handler, hidden_service, validation, approval_service


def _accepted_result(store: T22Database, scenario, result_id):
    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, scenario.principal)
        return connection.execute(
            text(
                "SELECT payload FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner AND run_id=:run AND result_id=:result"
            ),
            {"owner": scenario.run.owner_id, "run": scenario.run.run_id, "result": result_id},
        ).scalar_one()


def test_real_candidate_validation_handler_commits_hidden_evidence_and_freezes_hash(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario, handler, hidden, validation, approvals = _make_handler_context(t22_database, tmp_path)
    try:
        receipt = asyncio.run(handler(scenario.validation_lease))
        assert receipt.result_id is not None and receipt.next_job_id is not None
        assert validation.evidence is not None and hidden is not None
        payload = _accepted_result(t22_database, scenario, receipt.result_id)
        hidden_result = payload["hidden_check_evidence"]
        assert hidden_result["suite_sha256"]
        assert hidden_result["comparison"]["outcome"] == "passed"
        _seed_admission_pins(t22_database, scenario)
        snapshot = read_live_evaluation_snapshot(
            t22_database.runtime_engine, scenario.run.owner_id, scenario.run.run_id
        )
        assert snapshot["independent_hidden_checks"] == "passed"
        assert snapshot["task_correctness"] == "not_assessed"

        freeze_lease = scenario.queue.claim_one("hidden-handler-freeze")
        assert freeze_lease is not None and freeze_lease.job_id == receipt.next_job_id
        frozen = asyncio.run(
            ApprovalFreezeActivityHandler(
                t22_database.runtime_engine, scenario.principal, approvals
            )(freeze_lease)
        )
        assert frozen.status == "pending"
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, scenario.principal)
            anchored_digest = connection.execute(
                text(
                    "SELECT manifest->'validation'->>'evidence_result_sha256' "
                    "FROM runtime_private.approvals WHERE owner_id=:owner AND approval_id=:approval"
                ),
                {"owner": scenario.run.owner_id, "approval": frozen.approval_id},
            ).scalar_one()
        assert anchored_digest == receipt.result_sha256.hex()
    finally:
        _close_scenario(scenario)


def test_real_candidate_validation_handler_routes_hidden_failure_to_attention(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario, handler, _hidden, _validation, _approvals = _make_handler_context(
        t22_database, tmp_path, hidden_outcome="failed"
    )
    try:
        receipt = asyncio.run(handler(scenario.validation_lease))
        assert receipt.run_status is RunStatus.NEEDS_ATTENTION
        assert receipt.next_job_id is None
        payload = _accepted_result(t22_database, scenario, receipt.result_id)
        assert payload["classification"] == "hidden_check_failed"
        assert payload["hidden_check_status"] == "failed"
        assert payload["hidden_check_evidence"]["comparison"]["outcome"] == "failed"
        _seed_admission_pins(t22_database, scenario)
        snapshot = read_live_evaluation_snapshot(
            t22_database.runtime_engine, scenario.run.owner_id, scenario.run.run_id
        )
        assert snapshot["independent_hidden_checks"] == "failed"
        assert snapshot["task_correctness"] == "not_assessed"
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, scenario.principal)
            approval_count = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.approvals "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
            ).scalar_one()
        assert approval_count == 0
    finally:
        _close_scenario(scenario)


def test_real_candidate_validation_handler_does_not_commit_after_hidden_lease_loss(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario, handler, hidden, _validation, _approvals = _make_handler_context(
        t22_database, tmp_path
    )
    hidden.expire_lease = True
    try:
        with pytest.raises(LeaseLost):
            asyncio.run(handler(scenario.validation_lease))
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, scenario.principal)
            accepted_validation_count = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner AND run_id=:run "
                    "AND (payload ? 'approval_validation' OR payload ? 'validation_evidence')"
                ),
                {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
            ).scalar_one()
        assert accepted_validation_count == 0
    finally:
        _close_scenario(scenario)
