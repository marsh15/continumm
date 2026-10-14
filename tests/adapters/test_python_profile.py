"""Local proof for the fixed Python profile and its deny-egress boundary."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import uuid4

import pytest
from vercel.sandbox import NetworkPolicyReadback

from continuum_api.artifacts.archives import (
    ArchiveFile,
    VerifiedArchive,
    create_archive,
    verify_archive,
)
from continuum_api.repositories.preflight import EligibilityReceipt
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.sandbox_allocations import AllocationReservation
from continuum_api.sandbox.config import PythonProfileSettings, SandboxAllocationLimits
from continuum_api.sandbox.contracts import CommandResult, EffectState, SandboxObservation
from continuum_api.sandbox.python_bootstrap_recipe import (
    CONTROL_DIRECTORY,
    PYTHON_EXECUTABLE,
    PYTHON_PROFILE_ID,
    PYTHON_PROFILE_VERSION,
    WORKSPACE_PATH,
)
from continuum_api.sandbox.python_profile import (
    PythonProfileError,
    PythonProfileService,
    _build_python_candidate_profile_plan,
    build_python_profile_plan,
)
from continuum_api.sandbox.validation.preparation import prepare_validation_source
from continuum_api.sandbox.vercel import VercelSandboxProvider
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import ToolPolicyContext
from continuum_api.tools.source_tools import (
    TextEdit as SourceTextEdit,
)
from continuum_api.tools.source_tools import (
    apply_text_edits,
    discover_baseline_test_paths,
    snapshot_from_verified_archive,
    source_paths,
)

_IMAGE = f"vercel/sandbox/python:3.13.12@sha256:{'f' * 64}"
_COMMIT_SHA = "c" * 40
_GIT_TREE_SHA = "5b4f81f2cd419d52af1bda795f8dd5a98014656f"
_LOCK = b"pytest==8.3.5 --hash=sha256:" + b"a" * 64 + b"\n"
_SOURCE_FILES = (
    ArchiveFile("module.py", b"x = 1\n"),
    ArchiveFile("requirements.lock", _LOCK),
)


class _Store:
    def __init__(self, events: list[tuple[str, Any]], reservation: AllocationReservation) -> None:
        self.events = events
        self.reservation = reservation
        self.remaining_seconds = 300
        self.authorization_count = 0
        self.lose_lease_at: int | None = None

    def get_for_observation(
        self, _principal: Principal, _allocation_id: Any
    ) -> AllocationReservation:
        self.events.append(("get_reservation", None))
        return self.reservation

    def authorize_profile_action(
        self,
        _principal: Principal,
        _lease: ExecutionLease,
        _allocation_id: Any,
        expected_session_id: str,
        _provider_period_id: str | None,
    ) -> int:
        self.events.append(("authorize", expected_session_id))
        self.authorization_count += 1
        if self.authorization_count == self.lose_lease_at:
            return 0
        return self.remaining_seconds


class _CheckpointRestorer:
    def __init__(self, events: list[tuple[str, Any]], verified: VerifiedArchive) -> None:
        self.events = events
        self.verified = verified

    async def restore_archive(
        self, _principal: Principal, _lease: ExecutionLease, _artifact_id: Any
    ) -> VerifiedArchive:
        self.events.append(("restore_committed_archive", None))
        return self.verified


class _Cleanup:
    def __init__(self, events: list[tuple[str, Any]], reservation: AllocationReservation) -> None:
        self.events = events
        self.reservation = reservation
        self.calls = 0
        self.return_status = "stopped"

    async def stop(self, _principal: Principal, _allocation_id: Any) -> AllocationReservation:
        self.calls += 1
        self.events.append(("stop", None))
        self.reservation = replace(self.reservation, status=self.return_status)
        return self.reservation


class _Allocation:
    def __init__(
        self,
        events: list[tuple[str, Any]],
        reservation: AllocationReservation,
        plan: Any,
    ) -> None:
        self.events = events
        self.reservation = reservation
        self.plan = plan
        self.name = reservation.provider_name
        self.session_id = reservation.provider_session_id
        self.current_session_id = reservation.provider_session_id
        self.image = _IMAGE
        self.network_policy = NetworkPolicyReadback(mode="deny-all")
        self.observation_data = SandboxObservation(
            name=self.name,
            sandbox_status="RUNNING",
            session_id=self.session_id,
            session_status="RUNNING",
        )
        self.policy_override: NetworkPolicyReadback | None = None
        self.profile_archive: bytes | None = None
        self.probe_forced_allowed: bool | None = None
        self.block_phase: str | None = None
        self.blocked_command_started = asyncio.Event()
        self.blocked_command_continue = asyncio.Event()
        self.lock_package_count_override: int | None = None

    def observation(self) -> SandboxObservation:
        return self.observation_data

    async def write_profile_archive(self, content: bytes, *, timeout_seconds: int) -> None:
        self.events.append(("archive_write", timeout_seconds))
        self.profile_archive = content

    async def set_python_profile_network_policy(
        self, policy: str, *, timeout_seconds: int
    ) -> NetworkPolicyReadback:
        self.events.append(("policy", (policy, timeout_seconds)))
        if self.policy_override is not None:
            self.network_policy = self.policy_override
        elif policy == "package-download":
            self.network_policy = NetworkPolicyReadback(
                mode="custom",
                allowed_domains=("files.pythonhosted.org", "pypi.org"),
            )
        else:
            self.network_policy = NetworkPolicyReadback(mode="deny-all")
        return self.network_policy

    async def run_profile_command(
        self,
        command: str,
        args: Sequence[str],
        *,
        cwd: str,
        env: Mapping[str, str] | None,
        timeout_seconds: int,
        output_limit_bytes: int,
    ) -> CommandResult:
        assert command == PYTHON_EXECUTABLE
        self.events.append(("command_timeout", timeout_seconds))
        assert output_limit_bytes == 256 * 1024
        if cwd == WORKSPACE_PATH:
            phase = "pytest"
            assert args == ("-P", "-S", "-m", "pytest", "-q")
            assert env is not None
            assert (
                env["PYTHONPATH"]
                == "/opt/continuum/deps:/vercel/sandbox/workspace/src:/vercel/sandbox/workspace"
            )
            stdout = "1 passed\n"
        else:
            assert cwd == CONTROL_DIRECTORY
            phase = args[3]
            if phase == self.block_phase:
                self.blocked_command_started.set()
                await self.blocked_command_continue.wait()
            if phase == "verify-runtime":
                receipt = {
                    "implementation": "CPython",
                    "machine": "x86_64",
                    "profile_version": "1",
                    "python_version": "3.13.12",
                }
            elif phase == "extract":
                receipt = {
                    "archive_sha256": self.plan.archive_sha256,
                    "file_count": self.plan.file_count,
                    "lockfile_sha256": self.plan.lockfile_sha256,
                    "source_bytes": self.plan.source_bytes,
                    "tree_sha256": self.plan.tree_sha256,
                }
            elif phase == "validate-lock":
                receipt = {
                    "lockfile_sha256": self.plan.lockfile_sha256,
                    "package_count": self.lock_package_count_override or 1,
                }
            elif phase == "probe":
                expected = args[args.index("--expected") + 1]
                observed_allowed = expected == "allowed"
                if self.probe_forced_allowed is not None:
                    observed_allowed = self.probe_forced_allowed
                receipt = {"expected": expected, "observed_allowed": observed_allowed}
            elif phase == "download-verify":
                receipt = {"package_count": 1, "total_bytes": 2_048}
            elif phase == "install":
                receipt = {"installed_wheel_count": 1}
            else:
                raise AssertionError(f"unexpected trusted phase {phase}")
            stdout = json.dumps(receipt, sort_keys=True) + "\n"
        self.events.append(("command", phase))
        return CommandResult(exit_code=0, stdout=stdout, stderr="", effect=EffectState.APPLIED)


class _Provider:
    def __init__(self, events: list[tuple[str, Any]], allocation: _Allocation) -> None:
        self.events = events
        self.allocation = allocation

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[_Provider]:
        self.events.append(("provider_connection", None))
        yield self

    async def observe_exact_session(self, name: str, session_id: str) -> _Allocation:
        self.events.append(("observe_exact", (name, session_id)))
        assert name == self.allocation.name
        assert session_id == self.allocation.session_id
        return self.allocation


def _reservation(*, status: str = "running") -> AllocationReservation:
    return AllocationReservation(
        allocation_id=uuid4(),
        provider_name="continuum-t17-profilefixture001",
        status=status,
        is_new=False,
        max_requested_seconds=300,
        vcpus=2,
        memory_mb=4096,
        provider_period_id="period-p0",
        allocation_class="public",
        provider_session_id="session-t17-fixture",
        owner_id=uuid4(),
        run_id=uuid4(),
        step_id=uuid4(),
        job_id=uuid4(),
        generation=1,
        attempt_id=uuid4(),
        lease_owner="python-profile-worker",
    )


def _principal_lease(reservation: AllocationReservation) -> tuple[Principal, ExecutionLease]:
    principal = Principal(reservation.owner_id)
    lease = ExecutionLease(
        owner_id=reservation.owner_id,
        run_id=reservation.run_id,
        step_id=reservation.step_id,
        job_id=reservation.job_id,
        attempt_id=reservation.attempt_id,
        generation=reservation.generation,
        lease_owner=reservation.lease_owner,
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    return principal, lease


def _eligibility(files: Sequence[ArchiveFile] = _SOURCE_FILES) -> EligibilityReceipt:
    from continuum_api.sandbox.python_profile import _git_blob_sha, _git_tree_sha

    return EligibilityReceipt(
        repository_id=123,
        commit_sha=_COMMIT_SHA,
        tree_sha=_git_tree_sha(files, "sha1"),
        profile_id=PYTHON_PROFILE_ID,
        profile_version=PYTHON_PROFILE_VERSION,
        file_count=len(files),
        source_bytes=sum(len(file.content) for file in files),
        manifest_blobs=(
            (
                "requirements.lock",
                _git_blob_sha(_LOCK, "sha1"),
            ),
        ),
        package_count=1,
    )


def _plan():
    bundle = create_archive(_SOURCE_FILES)
    verified = verify_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    return build_python_profile_plan(_eligibility(), verified)


def _validation_source():
    files = (
        *_SOURCE_FILES,
        ArchiveFile("tests/test_original.py", b"def test_original_behavior():\n    assert True\n"),
    )
    eligibility = _eligibility(files)
    bundle = create_archive(files)
    baseline_verified = verify_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    baseline = snapshot_from_verified_archive(
        baseline_verified,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    discovered = discover_baseline_test_paths(
        baseline,
        PYTHON_PROFILE_ID,
        expected_tree_sha256=baseline.tree_sha256,
    )
    context = ToolPolicyContext(
        capabilities=frozenset(ToolCapability),
        profile_id=PYTHON_PROFILE_ID,
        profile_version=PYTHON_PROFILE_VERSION,
        actor_can_write_repository=True,
        app_can_access_repository=True,
        stage=ToolStage.AGENT_WORK,
        run_budget_available=True,
        baseline_tree_sha256=baseline.tree_sha256,
        baseline_source_paths=frozenset(source_paths(baseline)),
        baseline_test_paths=discovered,
        baseline_paths_verified=True,
        protected_paths=frozenset(),
    )
    patch = apply_text_edits(
        baseline,
        [SourceTextEdit("module.py", "x = 1", "x = 2")],
        expected_tree_sha256=baseline.tree_sha256,
        authorized_write_paths={"module.py"},
    )
    baseline_artifact_id = uuid4()
    authorization = prepare_validation_source(
        eligibility,
        baseline_artifact_id,
        baseline_verified,
        patch,
        context,
    )
    return eligibility, baseline_verified, baseline_artifact_id, authorization


def _service(
    *,
    reservation: AllocationReservation | None = None,
    image: str | None = _IMAGE,
    policy_override: NetworkPolicyReadback | None = None,
    remaining_seconds: int = 300,
    lose_lease_at: int | None = None,
    cleanup_status: str = "stopped",
    forced_probe_allowed: bool | None = None,
    block_phase: str | None = None,
    eligibility: EligibilityReceipt | None = None,
    verified: VerifiedArchive | None = None,
    expected_plan: Any | None = None,
) -> tuple[PythonProfileService, list[tuple[str, Any]], _Cleanup, _Provider]:
    selected_reservation = reservation or _reservation()
    principal, lease = _principal_lease(selected_reservation)
    events: list[tuple[str, Any]] = []
    if verified is None:
        files_bundle = create_archive(_SOURCE_FILES)
        verified = verify_archive(
            files_bundle.archive,
            expected_archive_sha256=files_bundle.archive_sha256,
            expected_tree_sha256=files_bundle.manifest.tree_sha256,
            expected_manifest=files_bundle.manifest,
        )
    plan = expected_plan or build_python_profile_plan(eligibility or _eligibility(), verified)
    store = _Store(events, selected_reservation)
    store.remaining_seconds = remaining_seconds
    store.lose_lease_at = lose_lease_at
    cleanup = _Cleanup(events, selected_reservation)
    cleanup.return_status = cleanup_status
    allocation = _Allocation(events, selected_reservation, plan)
    allocation.policy_override = policy_override
    allocation.probe_forced_allowed = forced_probe_allowed
    allocation.block_phase = block_phase
    provider = _Provider(events, allocation)
    service = PythonProfileService(
        cast(Any, store),
        cast(Any, _CheckpointRestorer(events, verified)),
        cast(Any, cleanup),
        image=image,
        provider_period_id="period-p0",
        provider_factory=cast(
            Callable[[SandboxAllocationLimits], VercelSandboxProvider], lambda _limits: provider
        ),
    )
    return service, events, cleanup, provider


def test_profile_validates_commit_tree_archive_and_manifest_pins() -> None:
    bundle = create_archive(_SOURCE_FILES)
    verified = verify_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )

    plan = build_python_profile_plan(_eligibility(), verified)

    assert plan.package_count == 1
    assert plan.file_count == 2
    assert plan.lockfile_sha256
    assert verify_archive(plan.archive).manifest == verified.manifest


def test_candidate_profile_sends_only_immutable_candidate_and_preserves_original_tests() -> None:
    async def scenario() -> None:
        eligibility, baseline, artifact_id, authorization = _validation_source()
        candidate_plan = _build_python_candidate_profile_plan(
            eligibility,
            baseline,
            authorization,
            baseline_artifact_id=artifact_id,
        )
        service, events, cleanup, provider = _service(
            verified=baseline,
            expected_plan=candidate_plan,
        )
        principal, lease = _principal_lease(provider.allocation.reservation)

        result = await service.run(
            principal,
            lease,
            artifact_id,
            eligibility,
            provider.allocation.reservation.allocation_id,
            validation_source=authorization,
        )

        sent = provider.allocation.profile_archive
        assert sent == authorization.candidate_archive
        candidate = verify_archive(
            sent,
            expected_archive_sha256=authorization.candidate_archive_sha256,
            expected_tree_sha256=authorization.candidate_source_tree_sha256,
            expected_manifest=authorization.candidate_manifest,
        )
        before_tests = next(
            file.content for file in baseline.files if file.path == "tests/test_original.py"
        )
        after_tests = next(
            file.content for file in candidate.files if file.path == "tests/test_original.py"
        )
        assert before_tests == after_tests
        assert authorization.changed_paths == ("module.py",)
        assert result.validation_phase == "candidate"
        assert result.artifact_id == artifact_id
        assert result.tree_sha256 == authorization.candidate_source_tree_sha256
        assert result.candidate_archive_sha256 == authorization.candidate_archive_sha256
        assert [value for kind, value in events if kind == "command"].count("pytest") == 1
        assert cleanup.calls == 1

    asyncio.run(scenario())


def test_candidate_profile_rejects_tampered_authorization_and_cleans_up() -> None:
    async def scenario() -> None:
        eligibility, baseline, artifact_id, authorization = _validation_source()
        service, events, cleanup, provider = _service(
            verified=baseline,
            expected_plan=build_python_profile_plan(eligibility, baseline),
        )
        principal, lease = _principal_lease(provider.allocation.reservation)
        tampered = replace(authorization, candidate_archive=b"tampered archive")

        with pytest.raises(
            PythonProfileError,
            match="python_profile_candidate_authorization_invalid",
        ):
            await service.run(
                principal,
                lease,
                artifact_id,
                eligibility,
                provider.allocation.reservation.allocation_id,
                validation_source=tampered,
            )

        assert not any(kind == "archive_write" for kind, _value in events)
        assert cleanup.calls == 1

    asyncio.run(scenario())


def test_candidate_profile_requires_authorization_baseline_artifact_id() -> None:
    async def scenario() -> None:
        eligibility, baseline, artifact_id, authorization = _validation_source()
        service, events, cleanup, provider = _service(eligibility=eligibility, verified=baseline)
        principal, lease = _principal_lease(provider.allocation.reservation)

        with pytest.raises(
            PythonProfileError,
            match="python_profile_candidate_baseline_identity_mismatch",
        ):
            await service.run(
                principal,
                lease,
                uuid4(),
                eligibility,
                provider.allocation.reservation.allocation_id,
                validation_source=authorization,
            )

        assert not any(kind == "get_reservation" for kind, _value in events)
        assert cleanup.calls == 0
        assert artifact_id == authorization.baseline_artifact_id

    asyncio.run(scenario())


def test_python_profile_image_setting_is_operator_supplied_and_fail_closed() -> None:
    with pytest.raises(ValueError, match="python_profile_image_unconfigured"):
        PythonProfileSettings(python_profile_image=None).require_image()
    assert PythonProfileSettings(python_profile_image=_IMAGE).require_image() == _IMAGE


def test_git_tree_hash_matches_independent_git_nested_executable_vector() -> None:
    from continuum_api.sandbox.python_profile import _git_tree_sha

    git_files = (
        ArchiveFile("requirements.lock", _LOCK),
        ArchiveFile("src/__init__.py", b""),
        ArchiveFile("src/tools.py", b"def run():\n    return 1\n", mode=0o755),
    )

    # Precomputed with `git write-tree` from an isolated synthetic repository.
    assert _git_tree_sha(git_files, "sha1") == "2cbc9bdf97c25b59c819f194304bf60a8d2a59af"


def test_git_tree_binding_matches_independent_nested_executable_object_vector() -> None:
    from continuum_api.sandbox.python_profile import _git_tree_sha

    files = (
        ArchiveFile("requirements.lock", _LOCK),
        ArchiveFile("src/__init__.py", b""),
        ArchiveFile("src/tools.py", b"def run():\n    return 1\n", mode=0o755),
    )

    assert _git_tree_sha(files, "sha1") == "2cbc9bdf97c25b59c819f194304bf60a8d2a59af"


def test_profile_rejects_different_git_tree_even_when_manifest_lock_matches() -> None:
    changed = (ArchiveFile("module.py", b"x = 2\n"), _SOURCE_FILES[1])
    bundle = create_archive(changed)
    verified = verify_archive(bundle.archive)

    with pytest.raises(PythonProfileError, match="python_profile_source_tree_mismatch"):
        build_python_profile_plan(_eligibility(), verified)


def test_profile_rejects_mismatched_python_version_file_and_build_hooks() -> None:
    for extra, expected in (
        (ArchiveFile(".python-version", b"3.12.1\n"), "version_unsupported"),
        (ArchiveFile("setup.py", b"raise SystemExit(0)\n"), "build_hook_unsupported"),
    ):
        files = (*_SOURCE_FILES, extra)
        receipt = _eligibility(files)
        bundle = create_archive(files)
        with pytest.raises(PythonProfileError, match=expected):
            build_python_profile_plan(receipt, verify_archive(bundle.archive))


def test_profile_runs_fixed_phases_with_fence_before_each_mutation_and_stops() -> None:
    async def scenario() -> None:
        reservation = _reservation()
        service, events, cleanup, _provider = _service(reservation=reservation)
        principal, lease = _principal_lease(reservation)

        result = await service.run(
            principal,
            lease,
            uuid4(),
            _eligibility(),
            reservation.allocation_id,
        )

        assert result.test_passed is True
        assert result.allocation_status == "stopped"
        assert result.log_budget_scope == "profile_invocation_pending_run_ledger"
        commands = [value for kind, value in events if kind == "command"]
        assert commands == [
            "verify-runtime",
            "extract",
            "validate-lock",
            "probe",
            "download-verify",
            "probe",
            "install",
            "pytest",
        ]
        mutations = [
            kind for kind, _value in events if kind in {"archive_write", "policy", "command"}
        ]
        assert [kind for kind, _value in events].count("authorize") == len(mutations)
        deny_policy_index = next(
            index
            for index, (kind, value) in enumerate(events)
            if kind == "policy" and value[0] == "deny-all"
        )
        deny_probe_index = events.index(("command", "probe"), deny_policy_index)
        assert deny_policy_index < deny_probe_index
        assert (
            deny_probe_index
            < events.index(("command", "install"))
            < events.index(("command", "pytest"))
        )
        assert cleanup.calls == 1

    asyncio.run(scenario())


def test_policy_readback_mismatch_cleans_up_before_probe_install_or_tests() -> None:
    async def scenario() -> None:
        reservation = _reservation()
        service, events, cleanup, _provider = _service(
            reservation=reservation, policy_override=NetworkPolicyReadback(mode="allow-all")
        )
        principal, lease = _principal_lease(reservation)

        with pytest.raises(
            PythonProfileError, match="python_profile_package_egress_policy_unverified"
        ):
            await service.run(
                principal,
                lease,
                uuid4(),
                _eligibility(),
                reservation.allocation_id,
            )

        commands = [value for kind, value in events if kind == "command"]
        assert (
            "probe" not in commands
            and "download-verify" not in commands
            and "pytest" not in commands
        )
        assert cleanup.calls == 1

    asyncio.run(scenario())


def test_failed_deny_probe_blocks_install_and_pytest_then_stops_once() -> None:
    async def scenario() -> None:
        reservation = _reservation()
        service, events, cleanup, _provider = _service(
            reservation=reservation,
            forced_probe_allowed=True,
        )
        principal, lease = _principal_lease(reservation)

        with pytest.raises(PythonProfileError, match="python_profile_egress_denial_unverified"):
            await service.run(
                principal,
                lease,
                uuid4(),
                _eligibility(),
                reservation.allocation_id,
            )

        commands = [value for kind, value in events if kind == "command"]
        assert "download-verify" in commands
        assert "install" not in commands and "pytest" not in commands
        assert cleanup.calls == 1

    asyncio.run(scenario())


def test_lock_package_count_is_checked_before_any_pytest_command() -> None:
    async def scenario() -> None:
        reservation = _reservation()
        service, events, cleanup, provider = _service(reservation=reservation)
        provider.allocation.lock_package_count_override = 2
        principal, lease = _principal_lease(reservation)

        with pytest.raises(PythonProfileError, match="python_profile_lock_receipt_mismatch"):
            await service.run(
                principal,
                lease,
                uuid4(),
                _eligibility(),
                reservation.allocation_id,
            )

        commands = [value for kind, value in events if kind == "command"]
        assert "validate-lock" in commands
        assert "pytest" not in commands
        assert cleanup.calls == 1

    asyncio.run(scenario())


def test_lease_fence_loss_between_phases_stops_without_following_mutation() -> None:
    async def scenario() -> None:
        reservation = _reservation()
        service, events, cleanup, _provider = _service(
            reservation=reservation,
            lose_lease_at=10,
        )
        principal, lease = _principal_lease(reservation)

        with pytest.raises(PythonProfileError, match="python_profile_allocation_time_exhausted"):
            await service.run(
                principal,
                lease,
                uuid4(),
                _eligibility(),
                reservation.allocation_id,
            )

        commands = [value for kind, value in events if kind == "command"]
        assert "download-verify" in commands
        assert "install" not in commands and "pytest" not in commands
        assert cleanup.calls == 1

    asyncio.run(scenario())


def test_cancelled_profile_command_uses_one_protected_stop() -> None:
    async def scenario() -> None:
        reservation = _reservation()
        service, events, cleanup, provider = _service(
            reservation=reservation,
            block_phase="download-verify",
        )
        principal, lease = _principal_lease(reservation)
        allocation = provider.allocation

        task = asyncio.create_task(
            service.run(
                principal,
                lease,
                uuid4(),
                _eligibility(),
                reservation.allocation_id,
            )
        )
        await asyncio.wait_for(allocation.blocked_command_started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert cleanup.calls == 1
        commands = [value for kind, value in events if kind == "command"]
        assert "pytest" not in commands
        assert not any(kind == "policy" and value[0] == "deny-all" for kind, value in events)

    asyncio.run(scenario())


def test_profile_uses_db_remaining_seconds_for_each_remote_action() -> None:
    async def scenario() -> None:
        reservation = _reservation()
        service, events, cleanup, _provider = _service(
            reservation=reservation,
            remaining_seconds=17,
        )
        principal, lease = _principal_lease(reservation)

        await service.run(
            principal,
            lease,
            uuid4(),
            _eligibility(),
            reservation.allocation_id,
        )

        limits = [
            value for kind, value in events if kind == "archive_write" or kind == "command_timeout"
        ] + [timeout for kind, value in events if kind == "policy" for _policy, timeout in [value]]
        assert limits and max(limits) == 17
        assert cleanup.calls == 1

    asyncio.run(scenario())


def test_unresolved_success_cleanup_does_not_return_a_successful_result() -> None:
    async def scenario() -> None:
        reservation = _reservation()
        service, _events, cleanup, _provider = _service(
            reservation=reservation,
            cleanup_status="stop_unknown",
        )
        principal, lease = _principal_lease(reservation)

        with pytest.raises(PythonProfileError, match="python_profile_cleanup_unresolved"):
            await service.run(
                principal,
                lease,
                uuid4(),
                _eligibility(),
                reservation.allocation_id,
            )

        assert cleanup.calls == 1

    asyncio.run(scenario())


def test_wrong_attempt_does_not_stop_someone_elses_allocation() -> None:
    async def scenario() -> None:
        reservation = _reservation()
        service, events, cleanup, _provider = _service(reservation=reservation)
        principal, lease = _principal_lease(reservation)
        wrong_lease = replace(lease, generation=lease.generation + 1)

        with pytest.raises(PythonProfileError, match="allocation_not_current"):
            await service.run(
                principal, wrong_lease, uuid4(), _eligibility(), reservation.allocation_id
            )

        assert cleanup.calls == 0
        assert not any(kind == "provider_connection" for kind, _value in events)

    asyncio.run(scenario())


def test_unconfigured_profile_image_fails_closed_and_stops_owned_allocation() -> None:
    async def scenario() -> None:
        reservation = _reservation()
        service, events, cleanup, _provider = _service(reservation=reservation, image=None)
        principal, lease = _principal_lease(reservation)

        with pytest.raises(PythonProfileError, match="python_profile_image_unconfigured"):
            await service.run(principal, lease, uuid4(), _eligibility(), reservation.allocation_id)

        assert cleanup.calls == 1
        assert not any(kind == "provider_connection" for kind, _value in events)

    asyncio.run(scenario())
