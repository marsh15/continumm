from __future__ import annotations

import asyncio
import importlib.util
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from continuum_api.artifacts.archives import ArchiveFile
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.sandbox_allocations import AllocationReservation
from continuum_api.sandbox.node_bootstrap_recipe import NODE_VERSION, NPM_VERSION
from continuum_api.sandbox.python_profile import ProfileCommandOutput
from continuum_api.validation.hidden_checks import HiddenCheckCase, HiddenCheckSuite
from continuum_api.validation.managed_hidden_checks import (
    ManagedHiddenCheckError,
    ManagedHiddenCheckService,
)
from continuum_api.validation.service import ValidationServiceError

preparation_module_spec = importlib.util.spec_from_file_location(
    "validation_preparation_test_helpers",
    Path(__file__).with_name("test_validation_preparation.py"),
)
assert preparation_module_spec is not None and preparation_module_spec.loader is not None
preparation_tests = importlib.util.module_from_spec(preparation_module_spec)
preparation_module_spec.loader.exec_module(preparation_tests)

_IMAGE = "registry.example/continuum/profile@sha256:" + "d" * 64
_PRIVATE_STDOUT = '{"protocol_version":1,"values":[{"ok":true}]}'
_PRIVATE_STDERR = "private diagnostic that must not be returned"


def _identities() -> tuple[Principal, ExecutionLease]:
    owner = uuid4()
    principal = Principal(owner)
    lease = ExecutionLease(
        owner_id=owner,
        run_id=uuid4(),
        step_id=uuid4(),
        job_id=uuid4(),
        attempt_id=uuid4(),
        generation=2,
        lease_owner="worker-test",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    return principal, lease


def _reservation(lease: ExecutionLease, **changes: Any) -> AllocationReservation:
    reservation = AllocationReservation(
        allocation_id=uuid4(),
        provider_name="continuum-test",
        status="running",
        is_new=True,
        max_requested_seconds=300,
        vcpus=2,
        memory_mb=4096,
        provider_period_id="period-test",
        allocation_class="operator",
        provider_session_id="session-test",
        owner_id=lease.owner_id,
        run_id=lease.run_id,
        step_id=lease.step_id,
        job_id=lease.job_id,
        generation=lease.generation,
        attempt_id=lease.attempt_id,
        lease_owner=lease.lease_owner,
    )
    return replace(reservation, **changes)


def _suite(
    source: Any, *, profile_id: str | None = None, archive_sha: str | None = None
) -> HiddenCheckSuite:
    return HiddenCheckSuite(
        suite_id="operator-suite-v1",
        profile_id=profile_id or source.profile_id,
        profile_version="1",
        candidate_archive_sha256=archive_sha or source.candidate_archive_sha256,
        candidate_tree_sha256=source.candidate_source_tree_sha256,
        cases=(HiddenCheckCase("known", '[{"x":1}]', '{"ok":true}'),),
    )


def _result(source: Any, lease: ExecutionLease, allocation_id: Any, stdout: str) -> SimpleNamespace:
    profile_id = source.profile_id
    node = profile_id == "node-npm"
    return SimpleNamespace(
        profile_id=profile_id,
        profile_version="1",
        runtime_version="3.13.12",
        node_version=NODE_VERSION,
        npm_version=NPM_VERSION,
        repository_id=source.eligibility.repository_id,
        commit_sha=source.eligibility.commit_sha,
        tree_sha256=source.candidate_source_tree_sha256,
        artifact_id=source.baseline_artifact_id,
        allocation_id=allocation_id,
        package_count=source.eligibility.package_count,
        test_exit_code=0,
        test_passed=True,
        outputs=(ProfileCommandOutput("hidden_probe", 0, stdout, _PRIVATE_STDERR),),
        allocation_status="stopped",
        validation_phase="candidate",
        baseline_git_tree_sha256=source.eligibility.tree_sha,
        baseline_archive_tree_sha256=source.baseline_source_tree_sha256,
        candidate_archive_sha256=source.candidate_archive_sha256,
        candidate_source_tree_sha256=source.candidate_source_tree_sha256,
        image=_IMAGE,
        command_id=("node-hidden-probe-v1" if node else "python-hidden-probe-v1"),
    )


class _Creator:
    def __init__(
        self, lease: ExecutionLease, reservation: AllocationReservation | None = None
    ) -> None:
        self.lease = lease
        self.reservation = reservation or _reservation(lease)
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def create(self, *args: Any, **kwargs: Any) -> AllocationReservation:
        self.calls.append((args, kwargs))
        return self.reservation


class _Cleanup:
    def __init__(self) -> None:
        self.calls: list[Any] = []
        self.event = asyncio.Event()

    async def stop(self, principal: Principal, allocation_id: Any) -> AllocationReservation:
        self.calls.append((principal, allocation_id))
        self.event.set()
        return SimpleNamespace(allocation_id=allocation_id, status="stopped")  # type: ignore[return-value]


class _Profile:
    def __init__(self, stdout: str = _PRIVATE_STDOUT, *, image: str | None = _IMAGE) -> None:
        self.configured_image = image
        self.stdout = stdout
        self.result_change: dict[str, Any] = {}
        self.raise_error: BaseException | None = None
        self.calls: list[tuple[Any, Any, Any, Any, dict[str, Any]]] = []

    async def run_hidden_probe(
        self,
        principal: Principal,
        lease: ExecutionLease,
        artifact_id: Any,
        eligibility: Any,
        allocation_id: Any,
        **kwargs: Any,
    ) -> Any:
        self.calls.append((principal, lease, artifact_id, eligibility, kwargs))
        if self.raise_error is not None:
            raise self.raise_error
        result = _result(kwargs["validation_source"], lease, allocation_id, self.stdout)
        for name, value in self.result_change.items():
            setattr(result, name, value)
        return result


def _harness(
    source: Any,
    lease: ExecutionLease,
    *,
    profile: _Profile | None = None,
    reservation: AllocationReservation | None = None,
) -> tuple[ManagedHiddenCheckService, _Creator, _Cleanup, _Profile]:
    selected = profile or _Profile()
    creator = _Creator(lease, reservation)
    cleanup = _Cleanup()
    unused_profile = _Profile(image=_IMAGE)
    service = ManagedHiddenCheckService(
        selected if source.profile_id == "python-pytest" else unused_profile,
        selected if source.profile_id == "node-npm" else unused_profile,
        creator,
        cleanup,
    )
    return service, creator, cleanup, selected


def _invoke(
    service: ManagedHiddenCheckService,
    principal: Principal,
    lease: ExecutionLease,
    source: Any,
    suite: HiddenCheckSuite,
    *,
    entry_path: str = "module.py",
    allow_operator_sandbox: bool = True,
) -> Any:
    return asyncio.run(
        service.run(
            principal,
            lease,
            source,
            suite,
            entry_path=entry_path,
            allow_operator_sandbox=allow_operator_sandbox,
        )
    )


def test_python_hidden_probe_returns_digest_only_comparison_evidence(tmp_path: Any) -> None:
    _, _, _, source, _ = preparation_tests._prepared(tmp_path, "python-pytest")
    principal, lease = _identities()
    service, creator, cleanup, profile = _harness(source, lease)

    evidence = _invoke(service, principal, lease, source, _suite(source))
    round_trip = json.loads(evidence.model_dump_json())
    assert round_trip["comparison"]["outcome"] == "passed"
    assert round_trip["comparison"]["passed_count"] == 1
    assert round_trip["execution"]["phase"] == "candidate"
    assert profile.calls[0][4]["probe"].entry_path == "module.py"
    assert creator.calls[0][1] == {"allocation_class": "operator", "require_new": True}
    assert creator.calls[0][0][2].startswith("t48:hidden:v1:")
    assert cleanup.calls == []
    serialized = evidence.model_dump_json()
    assert _PRIVATE_STDOUT not in serialized
    assert _PRIVATE_STDERR not in serialized
    assert "expected" not in serialized


def test_node_profile_adapter_receives_fixed_probe(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = preparation_tests._source_files
    monkeypatch.setattr(
        preparation_tests,
        "_source_files",
        lambda profile: (
            original(profile)
            + (ArchiveFile("probe.cjs", b"module.exports.solve = () => ({ ok: true });\n"),)
        ),
    )
    _, _, _, source, _ = preparation_tests._prepared(tmp_path, "node-npm")
    principal, lease = _identities()
    service, _, _, profile = _harness(source, lease)
    evidence = _invoke(service, principal, lease, source, _suite(source), entry_path="probe.cjs")
    assert evidence.comparison.outcome == "passed"
    assert profile.calls[0][4]["probe"].command_id == "node-hidden-probe-v1"
    assert profile.calls[0][4]["probe"].entry_path == "probe.cjs"


def test_wrong_answer_and_forged_pass_field_never_create_a_pass(tmp_path: Any) -> None:
    _, _, _, source, _ = preparation_tests._prepared(tmp_path, "python-pytest")
    principal, lease = _identities()
    wrong_profile = _Profile('{"protocol_version":1,"values":[{"ok":false}]}')
    service, _, _, _ = _harness(source, lease, profile=wrong_profile)
    assert _invoke(service, principal, lease, source, _suite(source)).comparison.outcome == "failed"

    forged = _Profile('{"protocol_version":1,"values":[{"ok":true}],"passed":true}')
    service, _, _, _ = _harness(source, lease, profile=forged)
    evidence = _invoke(service, principal, lease, source, _suite(source))
    assert evidence.comparison.outcome == "invalid"
    assert evidence.comparison.code == "hidden_output_protocol_invalid"


@pytest.mark.parametrize("case", ["opt_in", "owner", "suite_profile", "suite_pin", "entry"])
def test_authorization_and_source_mismatches_fail_before_allocation(
    tmp_path: Any, case: str
) -> None:
    _, _, _, source, _ = preparation_tests._prepared(tmp_path, "python-pytest")
    principal, lease = _identities()
    service, creator, cleanup, _ = _harness(source, lease)
    suite = _suite(source)
    options: dict[str, Any] = {}
    entry_path = "module.py"
    if case == "opt_in":
        options["allow_operator_sandbox"] = False
    elif case == "owner":
        principal = Principal(uuid4())
    elif case == "suite_profile":
        suite = _suite(source, profile_id="node-npm")
    elif case == "suite_pin":
        suite = _suite(source, archive_sha="a" * 64)
    elif case == "entry":
        entry_path = "missing.py"
    with pytest.raises(ManagedHiddenCheckError):
        _invoke(service, principal, lease, source, suite, entry_path=entry_path, **options)
    assert creator.calls == []
    assert cleanup.calls == []


def test_unconfigured_image_fails_before_allocation(tmp_path: Any) -> None:
    _, _, _, source, _ = preparation_tests._prepared(tmp_path, "python-pytest")
    principal, lease = _identities()
    service, creator, cleanup, _ = _harness(source, lease, profile=_Profile(image=None))
    with pytest.raises(ManagedHiddenCheckError, match="hidden_image_unconfigured"):
        _invoke(service, principal, lease, source, _suite(source))
    assert creator.calls == []
    assert cleanup.calls == []


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ("image", "validation_profile_result_mismatch"),
        ("source", "validation_profile_result_mismatch"),
        ("cleanup", "validation_profile_result_mismatch"),
    ],
)
def test_profile_result_identity_or_cleanup_mismatch_is_rejected_and_cleaned(
    tmp_path: Any, change: str, code: str
) -> None:
    _, _, _, source, _ = preparation_tests._prepared(tmp_path, "python-pytest")
    principal, lease = _identities()
    profile = _Profile()
    profile.result_change = {
        "image": {"image": "registry.example/other@sha256:" + "e" * 64},
        "source": {"candidate_archive_sha256": "f" * 64},
        "cleanup": {"allocation_status": "running"},
    }[change]
    service, _, cleanup, _ = _harness(source, lease, profile=profile)
    with pytest.raises(ValidationServiceError, match=code):
        _invoke(service, principal, lease, source, _suite(source))
    assert len(cleanup.calls) == 1


def test_wrong_allocation_identity_is_never_executed_or_stopped(tmp_path: Any) -> None:
    _, _, _, source, _ = preparation_tests._prepared(tmp_path, "python-pytest")
    principal, lease = _identities()
    foreign = _reservation(lease, owner_id=uuid4())
    service, creator, cleanup, profile = _harness(source, lease, reservation=foreign)
    with pytest.raises(ManagedHiddenCheckError, match="hidden_allocation_identity_mismatch"):
        _invoke(service, principal, lease, source, _suite(source))
    assert creator.calls
    assert profile.calls == []
    assert cleanup.calls == []


@pytest.mark.parametrize("cancel", [False, True])
def test_profile_exception_or_cancellation_attempts_cleanup(tmp_path: Any, cancel: bool) -> None:
    _, _, _, source, _ = preparation_tests._prepared(tmp_path, "python-pytest")
    principal, lease = _identities()
    profile = _Profile()
    profile.raise_error = asyncio.CancelledError() if cancel else RuntimeError("private detail")
    service, _, cleanup, _ = _harness(source, lease, profile=profile)

    def run() -> None:
        with pytest.raises(asyncio.CancelledError if cancel else RuntimeError):
            _invoke(service, principal, lease, source, _suite(source))

    run()

    async def wait_for_cleanup() -> None:
        await asyncio.wait_for(cleanup.event.wait(), timeout=1)

    asyncio.run(wait_for_cleanup())
    assert len(cleanup.calls) == 1


def test_prepared_evidence_round_trip_and_identity_tampering(tmp_path: Any) -> None:
    _, _, _, source, _ = preparation_tests._prepared(tmp_path, "python-pytest")
    principal, lease = _identities()
    service, _, _, _ = _harness(source, lease)
    evidence = _invoke(service, principal, lease, source, _suite(source))
    assert type(evidence).model_validate_json(evidence.model_dump_json()) == evidence
    for field, value in (
        ("entry_path", "other.py"),
        ("invocation_sha256", "a" * 64),
    ):
        changed = json.loads(evidence.model_dump_json())
        changed[field] = value
        with pytest.raises(ManagedHiddenCheckError):
            type(evidence).model_validate_json(json.dumps(changed))
    changed = json.loads(evidence.model_dump_json())
    changed["comparison"]["stdout_sha256"] = "a" * 64
    with pytest.raises(ManagedHiddenCheckError, match="hidden_evidence_observation_mismatch"):
        type(evidence).model_validate_json(json.dumps(changed))
