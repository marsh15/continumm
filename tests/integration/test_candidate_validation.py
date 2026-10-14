"""Local, fake-runner proof for T20 candidate validation orchestration."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from continuum_api.artifacts.archives import ArchiveFile, create_archive, verify_archive
from continuum_api.repositories.preflight import PYTHON_TARGET_VERSION, EligibilityReceipt
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.persistence import Principal
from continuum_api.sandbox.allocation_service import SandboxAllocationServiceError
from continuum_api.sandbox.node_bootstrap_recipe import NODE_VERSION, NPM_VERSION
from continuum_api.sandbox.python_profile import _git_blob_sha, _git_tree_sha
from continuum_api.sandbox.validation.preparation import (
    ValidationSourceAuthorization,
    prepare_validation_source,
)
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import ToolPolicyContext
from continuum_api.tools.source_tools import (
    TextEdit,
    apply_text_edits,
    discover_baseline_test_paths,
    snapshot_from_verified_archive,
    source_paths,
)
from continuum_api.validation.contracts import (
    MAX_VALIDATION_EVIDENCE_BYTES,
    PhaseOutcome,
    ValidationClassification,
    ValidationPhase,
)
from continuum_api.validation.service import (
    DEFAULT_VALIDATION_ALLOCATION_SECONDS,
    ValidationService,
    ValidationServiceError,
)

_LOCK = b"pytest==8.3.5 --hash=sha256:" + b"a" * 64 + b"\n"
_IMAGE = f"vercel/sandbox/python:3.13.12@sha256:{'f' * 64}"
_BASELINE_FILES = (
    ArchiveFile("module.py", b"value = 1\n"),
    ArchiveFile("requirements.lock", _LOCK),
    ArchiveFile("tests/test_original.py", b"assert True\n"),
)


class _AllocationCreator:
    def __init__(self, events: list[tuple[str, object]] | None = None) -> None:
        self.events = events if events is not None else []
        self.calls: list[tuple[str, int, str]] = []
        self.fail_code: str | None = None

    async def create(
        self,
        _principal: Principal,
        _lease: ExecutionLease,
        logical_key: str,
        requested_seconds: int,
        *,
        allocation_class: str = "public",
    ) -> SimpleNamespace:
        self.events.append(("allocation", logical_key.rsplit(":", 1)[-1]))
        self.calls.append((logical_key, requested_seconds, allocation_class))
        if self.fail_code is not None:
            raise SandboxAllocationServiceError(self.fail_code)
        return SimpleNamespace(allocation_id=uuid4())


class _Profile:
    configured_image = _IMAGE

    def __init__(
        self,
        source: ValidationSourceAuthorization,
        events: list[tuple[str, object]],
        *,
        baseline_exit: int | None = 0,
        candidate_exit: int | None = 0,
        result_overrides: dict[str, dict[str, object]] | None = None,
        failures: dict[str, str] | None = None,
        mutate_candidate_workspace: bool = False,
    ) -> None:
        self.source = source
        self.events = events
        self.baseline_exit = baseline_exit
        self.candidate_exit = candidate_exit
        self.result_overrides = result_overrides or {}
        self.failures = failures or {}
        self.mutate_candidate_workspace = mutate_candidate_workspace
        self.workspace_before_mutation: dict[str, bytes] = {}
        self.workspace_after_mutation: dict[str, bytes] = {}
        self.candidate_posttest_archive: bytes | None = None
        self.candidate_posttest_archive_sha256: str | None = None
        self.workspace_read_count = 0
        self.calls: list[tuple[str, UUID, object | None]] = []

    def read_workspace(self, _allocation_id: UUID) -> object:
        self.workspace_read_count += 1
        raise AssertionError("T20 must not read a post-test workspace")

    async def run(
        self,
        _principal: Principal,
        _lease: ExecutionLease,
        artifact_id: UUID,
        eligibility: EligibilityReceipt,
        allocation_id: UUID,
        *,
        validation_source: ValidationSourceAuthorization | None = None,
    ) -> SimpleNamespace:
        phase = "candidate" if validation_source is not None else "baseline"
        self.events.append(("profile", phase))
        self.calls.append((phase, allocation_id, validation_source))
        if phase in self.failures:
            raise SandboxAllocationServiceError(self.failures[phase])
        exit_code = self.baseline_exit if phase == "baseline" else self.candidate_exit
        source = self.source
        tree_sha256 = (
            source.baseline_source_tree_sha256
            if phase == "baseline"
            else source.candidate_source_tree_sha256
        )
        posttest_archive: bytes | None = None
        if phase == "candidate" and self.mutate_candidate_workspace:
            verified_candidate = verify_archive(
                validation_source.candidate_archive,
                expected_archive_sha256=validation_source.candidate_archive_sha256,
                expected_tree_sha256=validation_source.candidate_source_tree_sha256,
                expected_manifest=validation_source.candidate_manifest,
            )
            workspace = {item.path: item for item in verified_candidate.files}
            self.workspace_before_mutation = {
                path: item.content for path, item in workspace.items()
            }
            workspace["module.py"] = ArchiveFile("module.py", b"value = 999\n")
            workspace["tests/test_original.py"] = ArchiveFile(
                "tests/test_original.py", b"assert False  # changed after invocation\n"
            )
            workspace["tests/test_added.py"] = ArchiveFile(
                "tests/test_added.py", b"assert True  # added after invocation\n"
            )
            self.workspace_after_mutation = {path: item.content for path, item in workspace.items()}
            posttest_bundle = create_archive(tuple(workspace.values()))
            posttest_archive = posttest_bundle.archive
            self.candidate_posttest_archive = posttest_bundle.archive
            self.candidate_posttest_archive_sha256 = posttest_bundle.archive_sha256
        result = {
            "profile_id": eligibility.profile_id,
            "profile_version": eligibility.profile_version,
            "repository_id": eligibility.repository_id,
            "commit_sha": eligibility.commit_sha,
            "tree_sha256": tree_sha256,
            "artifact_id": artifact_id,
            "allocation_id": allocation_id,
            "package_count": eligibility.package_count,
            "test_exit_code": exit_code,
            "test_passed": exit_code == 0,
            "outputs": (
                SimpleNamespace(
                    phase="pytest",
                    exit_code=exit_code,
                    stdout="private test output",
                    stderr="",
                ),
            ),
            "allocation_status": "stopped",
            "validation_phase": phase,
            "baseline_git_tree_sha256": eligibility.tree_sha,
            "baseline_archive_tree_sha256": source.baseline_source_tree_sha256,
            "candidate_archive_sha256": (
                source.candidate_archive_sha256 if phase == "candidate" else None
            ),
            "candidate_source_tree_sha256": (
                source.candidate_source_tree_sha256 if phase == "candidate" else None
            ),
            "image": _IMAGE,
            "command_id": "python-pytest-v1",
            "runtime_version": PYTHON_TARGET_VERSION,
            "posttest_archive": posttest_archive,
            "read_workspace": self.read_workspace,
        }
        result.update(self.result_overrides.get(phase, {}))
        return SimpleNamespace(**result)


def _prepared_source():
    bundle = create_archive(_BASELINE_FILES)
    verified = verify_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    snapshot = snapshot_from_verified_archive(
        verified,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    eligibility = EligibilityReceipt(
        repository_id=123,
        commit_sha="c" * 40,
        tree_sha=_git_tree_sha(_BASELINE_FILES, "sha1"),
        profile_id="python-pytest",
        profile_version="1",
        file_count=len(_BASELINE_FILES),
        source_bytes=sum(len(item.content) for item in _BASELINE_FILES),
        manifest_blobs=(("requirements.lock", _git_blob_sha(_LOCK, "sha1")),),
        package_count=1,
    )
    baseline_tests = discover_baseline_test_paths(
        snapshot,
        eligibility.profile_id,
        expected_tree_sha256=snapshot.tree_sha256,
    )
    context = ToolPolicyContext(
        capabilities=frozenset(ToolCapability),
        profile_id=eligibility.profile_id,
        profile_version=eligibility.profile_version,
        actor_can_write_repository=True,
        app_can_access_repository=True,
        stage=ToolStage.AGENT_WORK,
        run_budget_available=True,
        baseline_tree_sha256=snapshot.tree_sha256,
        baseline_source_paths=frozenset(source_paths(snapshot)),
        baseline_test_paths=baseline_tests,
        baseline_paths_verified=True,
        protected_paths=frozenset(),
    )
    candidate = apply_text_edits(
        snapshot,
        [TextEdit("module.py", "value = 1", "value = 2")],
        expected_tree_sha256=snapshot.tree_sha256,
        authorized_write_paths={"module.py"},
    )
    source = prepare_validation_source(
        eligibility,
        uuid4(),
        verified,
        candidate,
        context,
    )
    return source, eligibility


def _caller() -> tuple[Principal, ExecutionLease]:
    owner_id = uuid4()
    principal = Principal(owner_id)
    lease = ExecutionLease(
        owner_id=owner_id,
        run_id=uuid4(),
        step_id=uuid4(),
        job_id=uuid4(),
        attempt_id=uuid4(),
        generation=1,
        lease_owner="validation-test",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    return principal, lease


def _service(*, profile: _Profile, allocator: _AllocationCreator) -> ValidationService:
    return ValidationService(profile, profile, allocator)


def test_baseline_failure_candidate_pass_uses_sequential_digest_bound_reservations() -> None:
    source, eligibility = _prepared_source()
    events: list[tuple[str, object]] = []
    allocator = _AllocationCreator(events)
    profile = _Profile(source, events, baseline_exit=1, candidate_exit=0)
    principal, lease = _caller()

    evidence = asyncio.run(
        _service(profile=profile, allocator=allocator).run(
            principal, lease, source.baseline_artifact_id, eligibility, source
        )
    )

    assert evidence.classification is ValidationClassification.PASSED_WITH_BASELINE_FAILURE
    assert evidence.baseline.outcome is PhaseOutcome.TEST_FAILED
    assert evidence.candidate.outcome is PhaseOutcome.PASSED
    assert evidence.baseline.runtime_version == PYTHON_TARGET_VERSION
    assert evidence.candidate.runtime_version == PYTHON_TARGET_VERSION
    assert evidence.baseline.npm_version is None
    assert evidence.baseline.allocation_id != evidence.candidate.allocation_id
    assert events == [
        ("allocation", "baseline"),
        ("profile", "baseline"),
        ("allocation", "candidate"),
        ("profile", "candidate"),
    ]
    assert [seconds for _, seconds, _ in allocator.calls] == [300, 300]
    assert all(allocation_class == "public" for _, _, allocation_class in allocator.calls)
    assert all(len(key) <= 128 and key.startswith("t20:v1:") for key, _, _ in allocator.calls)
    assert [key.rsplit(":", 1)[-1] for key, _, _ in allocator.calls] == [
        "baseline",
        "candidate",
    ]
    assert all(source.lineage_sha256 in key for key, _, _ in allocator.calls)
    serialized = evidence.model_dump_json(warnings=False).encode("utf-8")
    assert len(serialized) <= MAX_VALIDATION_EVIDENCE_BYTES
    assert b"private test output" not in serialized
    assert (
        evidence.baseline.commands[0].stdout_sha256
        == hashlib.sha256(b"private test output").hexdigest()
    )
    assert evidence.regression_on_original == "absent"


def test_candidate_posttest_workspace_changes_are_discarded_from_prepared_evidence() -> None:
    source, eligibility = _prepared_source()
    authorized_archive = source.candidate_archive
    authorized_archive_digest = source.candidate_archive_sha256
    authorized_tree_digest = source.candidate_source_tree_sha256
    events: list[tuple[str, object]] = []
    allocator = _AllocationCreator(events)
    profile = _Profile(source, events, mutate_candidate_workspace=True)
    principal, lease = _caller()

    evidence = asyncio.run(
        _service(profile=profile, allocator=allocator).run(
            principal, lease, source.baseline_artifact_id, eligibility, source
        )
    )

    assert "value = 2" in profile.workspace_before_mutation["module.py"].decode()
    assert profile.workspace_before_mutation["tests/test_original.py"] == b"assert True\n"
    assert profile.workspace_after_mutation["module.py"] == b"value = 999\n"
    assert b"changed after invocation" in profile.workspace_after_mutation["tests/test_original.py"]
    assert profile.workspace_after_mutation["tests/test_added.py"].startswith(b"assert True")
    assert profile.candidate_posttest_archive is not None
    assert profile.candidate_posttest_archive_sha256 is not None
    assert profile.candidate_posttest_archive_sha256 != authorized_archive_digest
    assert profile.workspace_read_count == 0

    assert source.candidate_archive is authorized_archive
    assert hashlib.sha256(source.candidate_archive).hexdigest() == authorized_archive_digest
    assert evidence.candidate_archive_sha256 == authorized_archive_digest
    assert evidence.candidate.source_archive_sha256 == authorized_archive_digest
    assert evidence.candidate.source_tree_sha256 == authorized_tree_digest
    assert evidence.baseline.source_archive_sha256 == source.baseline_archive_sha256
    assert evidence.baseline.source_tree_sha256 == source.baseline_source_tree_sha256
    assert evidence.baseline.outcome is PhaseOutcome.PASSED
    assert evidence.baseline.test_exit_code == 0
    assert evidence.baseline.commands[0].phase == "pytest"
    assert evidence.baseline.commands[0].exit_code == 0
    assert (
        evidence.baseline.commands[0].stdout_sha256
        == hashlib.sha256(b"private test output").hexdigest()
    )
    assert evidence.changed_path_count == len(source.changed_paths)
    serialized = evidence.model_dump_json(warnings=False)
    assert "posttest_archive" not in serialized
    assert profile.candidate_posttest_archive_sha256 not in serialized


def test_candidate_exit_one_is_reported_separately_from_passing_baseline() -> None:
    source, eligibility = _prepared_source()
    events: list[tuple[str, object]] = []
    allocator = _AllocationCreator(events)
    profile = _Profile(source, events, baseline_exit=0, candidate_exit=1)
    principal, lease = _caller()

    evidence = asyncio.run(
        _service(profile=profile, allocator=allocator).run(
            principal, lease, source.baseline_artifact_id, eligibility, source
        )
    )

    assert evidence.classification is ValidationClassification.CANDIDATE_TEST_FAILED
    assert evidence.baseline.outcome is PhaseOutcome.PASSED
    assert evidence.baseline.test_exit_code == 0
    assert evidence.baseline.source_archive_sha256 == source.baseline_archive_sha256
    assert evidence.candidate.outcome is PhaseOutcome.TEST_FAILED
    assert evidence.candidate.test_exit_code == 1
    assert evidence.candidate.source_archive_sha256 == source.candidate_archive_sha256
    assert evidence.regression_on_original == "absent"


@pytest.mark.parametrize(
    ("overrides", "exit_code", "expected_outcome", "expected_classification"),
    [
        (
            {"test_exit_code": None, "test_passed": False},
            0,
            PhaseOutcome.UNKNOWN,
            ValidationClassification.BASELINE_UNKNOWN,
        ),
        (
            {"test_exit_code": False, "test_passed": True},
            0,
            PhaseOutcome.UNKNOWN,
            ValidationClassification.BASELINE_UNKNOWN,
        ),
        (
            {"allocation_status": "unknown"},
            0,
            PhaseOutcome.UNKNOWN,
            ValidationClassification.BASELINE_UNKNOWN,
        ),
        ({}, 2, PhaseOutcome.UNKNOWN, ValidationClassification.BASELINE_UNKNOWN),
        ({}, 3, PhaseOutcome.UNKNOWN, ValidationClassification.BASELINE_UNKNOWN),
        ({}, 4, PhaseOutcome.UNSUPPORTED, ValidationClassification.BASELINE_UNSUPPORTED),
        ({}, 5, PhaseOutcome.UNSUPPORTED, ValidationClassification.BASELINE_UNSUPPORTED),
    ],
)
def test_unverified_or_unsupported_baseline_skips_candidate_without_allocation(
    overrides: dict[str, object],
    exit_code: int,
    expected_outcome: PhaseOutcome,
    expected_classification: ValidationClassification,
) -> None:
    source, eligibility = _prepared_source()
    events: list[tuple[str, object]] = []
    allocator = _AllocationCreator(events)
    profile = _Profile(
        source,
        events,
        baseline_exit=exit_code,
        result_overrides={"baseline": overrides},
    )
    principal, lease = _caller()

    evidence = asyncio.run(
        _service(profile=profile, allocator=allocator).run(
            principal, lease, source.baseline_artifact_id, eligibility, source
        )
    )

    assert evidence.classification is expected_classification
    assert evidence.baseline.outcome is expected_outcome
    assert evidence.candidate.outcome is PhaseOutcome.NOT_RUN
    assert evidence.candidate.allocation_id is None
    assert len(allocator.calls) == 1
    assert [phase for phase, _, _ in profile.calls] == ["baseline"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("baseline_git_tree_sha256", "0" * 40),
        ("baseline_archive_tree_sha256", "0" * 64),
        ("image", "different-image"),
        ("runtime_version", "3.13.11"),
        ("package_count", 2),
    ],
)
def test_candidate_must_echo_baseline_identity_and_fixed_profile_values(
    field: str, value: object
) -> None:
    source, eligibility = _prepared_source()
    events: list[tuple[str, object]] = []
    allocator = _AllocationCreator(events)
    profile = _Profile(
        source,
        events,
        result_overrides={"candidate": {field: value}},
    )
    principal, lease = _caller()

    evidence = asyncio.run(
        _service(profile=profile, allocator=allocator).run(
            principal, lease, source.baseline_artifact_id, eligibility, source
        )
    )

    assert evidence.classification is ValidationClassification.CANDIDATE_UNKNOWN
    assert evidence.baseline.baseline_git_tree_sha256 == eligibility.tree_sha
    assert evidence.baseline.baseline_archive_tree_sha256 == source.baseline_source_tree_sha256
    assert evidence.candidate.outcome is PhaseOutcome.UNKNOWN
    assert evidence.candidate.runtime_version is None
    assert evidence.candidate.npm_version is None
    assert len(allocator.calls) == 2


@pytest.mark.parametrize(
    "error_code",
    ["python_profile_test_exit_missing", "python_profile_cleanup_unresolved"],
)
def test_missing_exit_or_unresolved_cleanup_error_is_unknown_and_skips_candidate(
    error_code: str,
) -> None:
    source, eligibility = _prepared_source()
    events: list[tuple[str, object]] = []
    allocator = _AllocationCreator(events)
    profile = _Profile(source, events, failures={"baseline": error_code})
    principal, lease = _caller()

    evidence = asyncio.run(
        _service(profile=profile, allocator=allocator).run(
            principal, lease, source.baseline_artifact_id, eligibility, source
        )
    )

    assert evidence.classification is ValidationClassification.BASELINE_UNKNOWN
    assert evidence.baseline.outcome is PhaseOutcome.UNKNOWN
    assert evidence.candidate.outcome is PhaseOutcome.NOT_RUN
    assert evidence.candidate.allocation_id is None
    assert evidence.baseline.allocation_id is not None
    assert len(allocator.calls) == 1


@pytest.mark.parametrize(
    "error_code",
    [
        "python_profile_candidate_authorization_invalid",
        "node_profile_candidate_authorization_invalid",
    ],
)
def test_candidate_authorization_errors_are_unknown(error_code: str) -> None:
    source, eligibility = _prepared_source()
    events: list[tuple[str, object]] = []
    allocator = _AllocationCreator(events)
    profile = _Profile(source, events, failures={"candidate": error_code})
    principal, lease = _caller()

    evidence = asyncio.run(
        _service(profile=profile, allocator=allocator).run(
            principal, lease, source.baseline_artifact_id, eligibility, source
        )
    )

    assert evidence.classification is ValidationClassification.CANDIDATE_UNKNOWN
    assert evidence.baseline.outcome is PhaseOutcome.PASSED
    assert evidence.candidate.outcome is PhaseOutcome.UNKNOWN
    assert evidence.candidate.allocation_id is not None
    assert len(allocator.calls) == 2


def test_baseline_identity_mismatch_is_unknown_and_candidate_is_not_allocated() -> None:
    source, eligibility = _prepared_source()
    events: list[tuple[str, object]] = []
    allocator = _AllocationCreator(events)
    profile = _Profile(
        source,
        events,
        result_overrides={"baseline": {"baseline_git_tree_sha256": "0" * 40}},
    )
    principal, lease = _caller()

    evidence = asyncio.run(
        _service(profile=profile, allocator=allocator).run(
            principal, lease, source.baseline_artifact_id, eligibility, source
        )
    )

    assert evidence.classification is ValidationClassification.BASELINE_UNKNOWN
    assert evidence.baseline.outcome is PhaseOutcome.UNKNOWN
    assert evidence.candidate.outcome is PhaseOutcome.NOT_RUN
    assert evidence.candidate.allocation_id is None
    assert len(allocator.calls) == 1


def test_node_runtime_versions_are_recorded_only_at_exact_fixed_targets() -> None:
    eligibility = EligibilityReceipt(
        repository_id=456,
        commit_sha="d" * 40,
        tree_sha="e" * 40,
        profile_id="node-npm",
        profile_version="1",
        file_count=1,
        source_bytes=1,
        manifest_blobs=(),
        package_count=1,
    )
    artifact_id = uuid4()
    allocation_id = uuid4()
    baseline_tree = "1" * 64
    source_archive = "2" * 64
    result = SimpleNamespace(
        allocation_id=allocation_id,
        artifact_id=artifact_id,
        repository_id=eligibility.repository_id,
        commit_sha=eligibility.commit_sha,
        profile_id=eligibility.profile_id,
        profile_version=eligibility.profile_version,
        validation_phase="baseline",
        command_id="node-npm-test-v1",
        image="node-image",
        tree_sha256=baseline_tree,
        baseline_git_tree_sha256=eligibility.tree_sha,
        baseline_archive_tree_sha256=baseline_tree,
        candidate_archive_sha256=None,
        candidate_source_tree_sha256=None,
        test_exit_code=0,
        test_passed=True,
        allocation_status="stopped",
        package_count=eligibility.package_count,
        outputs=(),
        node_version=NODE_VERSION,
        npm_version=NPM_VERSION,
    )

    evidence = ValidationService._phase_from_result(
        result,
        phase=ValidationPhase.BASELINE,
        expected_allocation_id=allocation_id,
        baseline_artifact_id=artifact_id,
        eligibility=eligibility,
        source_archive_sha256=source_archive,
        source_tree_sha256=baseline_tree,
        baseline_source_tree_sha256=baseline_tree,
        baseline_archive_sha256="3" * 64,
        candidate_archive_sha256=None,
        candidate_source_tree_sha256=None,
        expected_command_id="node-npm-test-v1",
        expected_image="node-image",
    )

    assert evidence.runtime_version == NODE_VERSION
    assert evidence.npm_version == NPM_VERSION
    result.node_version = "24.20.0"
    with pytest.raises(ValidationServiceError, match="validation_profile_result_mismatch"):
        ValidationService._phase_from_result(
            result,
            phase=evidence.phase,
            expected_allocation_id=allocation_id,
            baseline_artifact_id=artifact_id,
            eligibility=eligibility,
            source_archive_sha256=source_archive,
            source_tree_sha256=baseline_tree,
            baseline_source_tree_sha256=baseline_tree,
            baseline_archive_sha256="3" * 64,
            candidate_archive_sha256=None,
            candidate_source_tree_sha256=None,
            expected_command_id="node-npm-test-v1",
            expected_image="node-image",
        )


def test_source_mutation_is_rejected_before_any_reservation_or_profile_call() -> None:
    source, eligibility = _prepared_source()
    events: list[tuple[str, object]] = []
    allocator = _AllocationCreator(events)
    profile = _Profile(source, events)
    principal, lease = _caller()
    mutated = replace(source, candidate_archive=b"mutated")

    with pytest.raises(ValidationServiceError, match="validation_source_identity_mismatch"):
        asyncio.run(
            _service(profile=profile, allocator=allocator).run(
                principal, lease, source.baseline_artifact_id, eligibility, mutated
            )
        )

    assert allocator.calls == []
    assert profile.calls == []


def test_allocation_failure_does_not_invent_an_allocation_id_for_not_run_candidate() -> None:
    source, eligibility = _prepared_source()
    events: list[tuple[str, object]] = []
    allocator = _AllocationCreator(events)
    allocator.fail_code = "sandbox_allocation_capacity_busy"
    profile = _Profile(source, events)
    principal, lease = _caller()

    evidence = asyncio.run(
        _service(profile=profile, allocator=allocator).run(
            principal, lease, source.baseline_artifact_id, eligibility, source
        )
    )

    assert evidence.classification is ValidationClassification.BASELINE_SETUP_FAILED
    assert evidence.baseline.allocation_id is None
    assert evidence.candidate.outcome is PhaseOutcome.NOT_RUN
    assert evidence.candidate.allocation_id is None
    assert profile.calls == []
    assert len(allocator.calls) == 1


def test_phase_budget_is_bounded_to_nine_hundred_total_seconds() -> None:
    source, _ = _prepared_source()
    events: list[tuple[str, object]] = []
    allocator = _AllocationCreator(events)
    profile = _Profile(source, events)

    assert DEFAULT_VALIDATION_ALLOCATION_SECONDS == 300
    with pytest.raises(ValueError, match="validation allocation duration is invalid"):
        ValidationService(profile, profile, allocator, allocation_seconds_per_phase=451)


def test_runtime_targets_are_fixed_profile_values() -> None:
    assert PYTHON_TARGET_VERSION == "3.13.12"
    assert (NODE_VERSION, NPM_VERSION) == ("24.21.0", "11.19.0")
