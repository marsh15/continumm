"""Local plan and orchestration proofs for the fixed Node/npm profile."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import subprocess
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
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
from continuum_api.sandbox.config import NodeProfileSettings
from continuum_api.sandbox.contracts import CommandResult, EffectState, SandboxObservation
from continuum_api.sandbox.node_bootstrap_recipe import (
    CONTROL_DIRECTORY,
    NODE_EXECUTABLE,
    NODE_PROFILE_ID,
    NODE_PROFILE_VERSION,
    NODE_VERSION,
    NPM_VERSION,
    WORKSPACE_PATH,
    npm_test_argv,
    npm_test_environment,
)
from continuum_api.sandbox.node_profile import (
    NodeProfileError,
    NodeProfileService,
    _build_node_candidate_profile_plan,
    build_node_profile_plan,
)
from continuum_api.sandbox.validation.preparation import prepare_validation_source
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

_COMMIT_SHA = "c" * 40
_INTEGRITY = "sha512-" + base64.b64encode(bytes(64)).decode("ascii")
_IMAGE = f"registry.example/node:24.21.0@sha256:{'f' * 64}"


def _source_files(*extra: ArchiveFile) -> tuple[ArchiveFile, ...]:
    package = {
        "name": "node-profile-fixture",
        "version": "1.0.0",
        "packageManager": "npm@11.19.0",
        "scripts": {"test": "node --test"},
        "dependencies": {"is-number": "7.0.0"},
    }
    lock = {
        "name": package["name"],
        "version": package["version"],
        "lockfileVersion": 3,
        "requires": True,
        "packages": {
            "": {
                "name": package["name"],
                "version": package["version"],
                "dependencies": package["dependencies"],
            },
            "node_modules/is-number": {
                "version": "7.0.0",
                "resolved": "https://registry.npmjs.org/is-number/-/is-number-7.0.0.tgz",
                "integrity": _INTEGRITY,
            },
        },
    }
    return (
        ArchiveFile("package.json", json.dumps(package, sort_keys=True).encode()),
        ArchiveFile("package-lock.json", json.dumps(lock, sort_keys=True).encode()),
        ArchiveFile("src/index.js", b"export const value = 1;\n"),
        *extra,
    )


def _eligibility_and_archive(
    tmp_path: Path, files: tuple[ArchiveFile, ...] | None = None
) -> tuple[EligibilityReceipt, VerifiedArchive]:
    source_files = _source_files() if files is None else files
    repository = tmp_path / str(uuid4())
    repository.mkdir()
    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    for source_file in source_files:
        path = repository / source_file.path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(source_file.content)
        path.chmod(source_file.mode)
    subprocess.run(["git", "-C", str(repository), "add", "--all"], check=True)
    git_tree_sha = subprocess.run(
        ["git", "-C", str(repository), "write-tree"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    listed_blobs = subprocess.run(
        ["git", "-C", str(repository), "ls-files", "--stage"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    git_blobs = {fields[3]: fields[1] for line in listed_blobs if len(fields := line.split()) == 4}
    manifest_paths = {"package.json", "package-lock.json"}
    manifest_paths.update(
        source_file.path
        for source_file in source_files
        if source_file.path.rsplit("/", 1)[-1] == ".gitattributes"
    )
    bundle = create_archive(source_files)
    verified = verify_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
    )
    receipt = EligibilityReceipt(
        repository_id=1234,
        commit_sha=_COMMIT_SHA,
        tree_sha=git_tree_sha,
        profile_id="node-npm",
        profile_version="1",
        file_count=len(source_files),
        source_bytes=sum(len(source_file.content) for source_file in source_files),
        manifest_blobs=tuple((path, git_blobs[path]) for path in sorted(manifest_paths)),
        package_count=1,
    )
    return receipt, verified


def test_node_profile_plan_binds_archive_to_independent_git_objects(tmp_path: Path) -> None:
    receipt, verified = _eligibility_and_archive(tmp_path)

    plan = build_node_profile_plan(receipt, verified)

    assert plan.archive_sha256 == verified.archive_sha256
    assert plan.tree_sha256 == verified.manifest.tree_sha256
    assert (
        plan.lockfile_sha256
        == hashlib.sha256(
            next(file.content for file in verified.files if file.path == "package-lock.json")
        ).hexdigest()
    )
    assert plan.file_count == receipt.file_count
    assert plan.source_bytes == receipt.source_bytes
    assert plan.package_count == receipt.package_count


def test_node_profile_rejects_receipt_for_a_different_git_tree(tmp_path: Path) -> None:
    receipt, verified = _eligibility_and_archive(tmp_path)
    bad_receipt = EligibilityReceipt(**{**receipt.to_cache_payload(), "tree_sha": "d" * 40})

    with pytest.raises(NodeProfileError, match="node_profile_source_tree_mismatch"):
        build_node_profile_plan(bad_receipt, verified)


@pytest.mark.parametrize(
    ("extra", "package_edit", "lock_version", "expected_code"),
    [
        (
            ArchiveFile(".npmrc", b"registry=https://attacker.invalid\n"),
            None,
            3,
            "node_profile_repository_config_unsupported",
        ),
        (
            ArchiveFile(".nvmrc", b"26\n"),
            None,
            3,
            "node_profile_version_file_unsupported",
        ),
        (
            ArchiveFile("src/addon.node", b"native"),
            None,
            3,
            "node_profile_native_source_unsupported",
        ),
        (
            ArchiveFile("src/extensionless-binary", b"\x7fELF\x02\x01\x01"),
            None,
            3,
            "node_profile_native_source_unsupported",
        ),
        (ArchiveFile("binding.gyp", b"{}"), None, 3, "node_profile_native_source_unsupported"),
        (
            ArchiveFile(".gitattributes", b"*.tgz filter=lfs\n"),
            None,
            3,
            "node_profile_lfs_unsupported",
        ),
        (None, {"packageManager": "npm@10.0.0"}, 3, "node_profile_package_manager_unsupported"),
        (
            None,
            {"scripts": {"test": "node --test", "pretest": "echo not-allowed"}},
            3,
            "node_profile_lifecycle_hook_unsupported",
        ),
        (None, None, 1, "repository_node_lock_unsupported"),
    ],
)
def test_node_profile_plan_rejects_unsupported_source_inputs(
    tmp_path: Path,
    extra: ArchiveFile | None,
    package_edit: dict[str, str] | None,
    lock_version: int,
    expected_code: str,
) -> None:
    files = list(_source_files(*(() if extra is None else (extra,))))
    if package_edit is not None:
        package = json.loads(files[0].content)
        package.update(package_edit)
        files[0] = ArchiveFile("package.json", json.dumps(package, sort_keys=True).encode())
    if lock_version != 3:
        lock = json.loads(files[1].content)
        lock["lockfileVersion"] = lock_version
        files[1] = ArchiveFile("package-lock.json", json.dumps(lock, sort_keys=True).encode())
    receipt, verified = _eligibility_and_archive(tmp_path, tuple(files))

    with pytest.raises(NodeProfileError, match=expected_code):
        build_node_profile_plan(receipt, verified)


def test_node_profile_plan_accepts_the_existing_supported_npm_lock_versions(tmp_path: Path) -> None:
    files = list(_source_files())
    lock = json.loads(files[1].content)
    lock["lockfileVersion"] = 2
    files[1] = ArchiveFile("package-lock.json", json.dumps(lock, sort_keys=True).encode())
    receipt, verified = _eligibility_and_archive(tmp_path, tuple(files))

    plan = build_node_profile_plan(receipt, verified)

    assert plan.package_count == 1


@pytest.mark.parametrize(
    "package_name",
    [
        "chrome-aws-lambda",
        "@sparticuz/chromium",
        "@sparticuz/chromium-min",
        "@puppeteer/browsers",
    ],
)
def test_node_profile_rejects_known_browser_download_packages(
    tmp_path: Path, package_name: str
) -> None:
    files = list(_source_files())
    package = json.loads(files[0].content)
    package["dependencies"] = {package_name: "1.0.0"}
    lock = json.loads(files[1].content)
    lock["packages"][""]["dependencies"] = {package_name: "1.0.0"}
    lock["packages"].pop("node_modules/is-number")
    lock["packages"][f"node_modules/{package_name}"] = {
        "version": "1.0.0",
        "resolved": "https://registry.npmjs.org/browser/-/browser-1.0.0.tgz",
        "integrity": _INTEGRITY,
    }
    files[0] = ArchiveFile("package.json", json.dumps(package, sort_keys=True).encode())
    files[1] = ArchiveFile("package-lock.json", json.dumps(lock, sort_keys=True).encode())
    receipt, verified = _eligibility_and_archive(tmp_path, tuple(files))

    with pytest.raises(NodeProfileError, match="node_profile_browser_dependency_unsupported"):
        build_node_profile_plan(receipt, verified)


@pytest.mark.parametrize(
    "asset_name",
    [
        "chromium.br",
        "swiftshader.tar.br",
        "chrome-headless-shell-linux64.zip",
        "chrome-headless-shell-mac-x64.zip",
        "chrome-headless-shell-win64.zip",
        "chrome-linux.zip",
        "chrome-mac.zip",
        "chrome-win.zip",
    ],
)
def test_node_profile_rejects_known_browser_assets_in_source_archive(
    tmp_path: Path, asset_name: str
) -> None:
    receipt, verified = _eligibility_and_archive(
        tmp_path,
        (*_source_files(), ArchiveFile(f"assets/{asset_name}", b"synthetic browser payload")),
    )

    with pytest.raises(NodeProfileError, match="node_profile_browser_dependency_unsupported"):
        build_node_profile_plan(receipt, verified)


def test_node_profile_image_setting_is_operator_supplied_and_fail_closed() -> None:
    with pytest.raises(ValueError, match="node_profile_image_unconfigured"):
        NodeProfileSettings(node_profile_image=None).require_image()

    settings = NodeProfileSettings(node_profile_image=_IMAGE)
    assert settings.require_image() == _IMAGE
    with pytest.raises(ValueError):
        NodeProfileSettings(node_profile_image="registry.example/node:latest").require_image()


def _reservation(*, status: str = "running") -> AllocationReservation:
    return AllocationReservation(
        allocation_id=uuid4(),
        provider_name="continuum-t18-profilefixture001",
        status=status,
        is_new=False,
        max_requested_seconds=300,
        vcpus=2,
        memory_mb=4096,
        provider_period_id="period-p0",
        allocation_class="public",
        provider_session_id="session-t18-fixture",
        owner_id=uuid4(),
        run_id=uuid4(),
        step_id=uuid4(),
        job_id=uuid4(),
        generation=1,
        attempt_id=uuid4(),
        lease_owner="node-profile-worker",
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


class _Restorer:
    def __init__(self, events: list[tuple[str, Any]], verified: VerifiedArchive) -> None:
        self.events = events
        self.verified = verified

    async def restore_archive(
        self, _principal: Principal, _lease: ExecutionLease, _artifact_id: Any
    ) -> VerifiedArchive:
        self.events.append(("restore_archive", None))
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
        plan,
    ) -> None:
        self.events = events
        self.reservation = reservation
        self.plan = plan
        self.name = reservation.provider_name
        self.session_id = reservation.provider_session_id
        self.current_session_id = reservation.provider_session_id
        self.image = _IMAGE
        self.network_policy = NetworkPolicyReadback(mode="deny-all")
        self.policy_override: NetworkPolicyReadback | None = None
        self.probe_overrides: dict[str, bool] = {}
        self.npm_ci_package_count = 1
        self.test_exit_code = 0
        self.block_phase: str | None = None
        self.blocked_command_started = asyncio.Event()
        self.blocked_command_continue = asyncio.Event()
        self.profile_archive: bytes | None = None

    def observation(self) -> SandboxObservation:
        return SandboxObservation(
            name=self.name,
            sandbox_status="RUNNING",
            session_id=self.session_id,
            session_status="RUNNING",
        )

    async def write_profile_archive(self, content: bytes, *, timeout_seconds: int) -> None:
        self.events.append(("archive_write", timeout_seconds))
        self.profile_archive = content

    async def set_node_profile_network_policy(
        self, policy: str, *, timeout_seconds: int
    ) -> NetworkPolicyReadback:
        self.events.append(("policy", (policy, timeout_seconds)))
        if self.policy_override is not None:
            self.network_policy = self.policy_override
        elif policy == "package-download":
            self.network_policy = NetworkPolicyReadback(
                mode="custom", allowed_domains=("registry.npmjs.org",)
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
        self.events.append(("command_timeout", timeout_seconds))
        assert output_limit_bytes == 256 * 1024
        if cwd == WORKSPACE_PATH:
            assert self.network_policy.mode == "deny-all"
            assert command == npm_test_argv()[0] == NODE_EXECUTABLE
            assert tuple(args) == npm_test_argv()[1:]
            assert env == npm_test_environment()
            phase = "npm_test"
            stdout = "test output\n"
            exit_code = self.test_exit_code
        else:
            assert cwd == CONTROL_DIRECTORY
            assert command == "/usr/local/bin/python3.13"
            assert env is None
            phase = args[3]
            if phase == self.block_phase:
                self.blocked_command_started.set()
                await self.blocked_command_continue.wait()
            if phase == "verify-runtime":
                receipt = {
                    "machine": "x86_64",
                    "node_version": NODE_VERSION,
                    "npm_version": NPM_VERSION,
                    "profile_version": NODE_PROFILE_VERSION,
                }
            elif phase == "extract":
                receipt = {
                    "archive_sha256": self.plan.archive_sha256,
                    "tree_sha256": self.plan.tree_sha256,
                    "lockfile_sha256": self.plan.lockfile_sha256,
                    "file_count": self.plan.file_count,
                    "source_bytes": self.plan.source_bytes,
                }
            elif phase == "validate-lock":
                receipt = {
                    "lockfile_sha256": self.plan.lockfile_sha256,
                    "package_count": 1,
                }
            elif phase == "probe":
                expected = args[args.index("--expected") + 1]
                receipt = {
                    "expected": expected,
                    "observed_allowed": self.probe_overrides.get(expected, expected == "allowed"),
                }
            elif phase == "npm-ci":
                assert self.network_policy.mode == "custom"
                assert self.network_policy.allowed_domains == ("registry.npmjs.org",)
                receipt = {
                    "lockfile_sha256": self.plan.lockfile_sha256,
                    "package_count": self.npm_ci_package_count,
                }
            elif phase == "validate-install":
                assert self.network_policy.mode == "deny-all"
                receipt = {
                    "lockfile_sha256": self.plan.lockfile_sha256,
                    "package_count": 1,
                    "file_count": 10,
                    "total_bytes": 4_096,
                }
            else:
                raise AssertionError(f"unexpected trusted phase {phase}")
            stdout = json.dumps(receipt, sort_keys=True) + "\n"
            exit_code = 0
        self.events.append(("command", phase))
        return CommandResult(
            exit_code=exit_code, stdout=stdout, stderr="", effect=EffectState.APPLIED
        )


class _Provider:
    def __init__(self, events: list[tuple[str, Any]], allocation: _Allocation) -> None:
        self.events = events
        self.allocation = allocation

    @asynccontextmanager
    async def connection(self):
        self.events.append(("provider_connection", None))
        yield self

    async def observe_exact_session(self, name: str, session_id: str) -> _Allocation:
        self.events.append(("observe_exact", (name, session_id)))
        assert name == self.allocation.name
        assert session_id == self.allocation.session_id
        return self.allocation


def _service(
    tmp_path: Path,
    *,
    image: str | None = _IMAGE,
    cleanup_status: str = "stopped",
    policy_override: NetworkPolicyReadback | None = None,
    probe_overrides: dict[str, bool] | None = None,
    lose_lease_at: int | None = None,
    block_phase: str | None = None,
    eligibility: EligibilityReceipt | None = None,
    verified: VerifiedArchive | None = None,
    expected_plan: Any | None = None,
) -> tuple[Any, Principal, ExecutionLease, list[tuple[str, Any]], _Cleanup, _Provider]:
    if eligibility is None or verified is None:
        receipt, baseline = _eligibility_and_archive(tmp_path)
        eligibility = receipt if eligibility is None else eligibility
        verified = baseline if verified is None else verified
    assert eligibility is not None and verified is not None
    plan = expected_plan or build_node_profile_plan(eligibility, verified)
    reservation = _reservation()
    principal, lease = _principal_lease(reservation)
    events: list[tuple[str, Any]] = []
    store = _Store(events, reservation)
    store.lose_lease_at = lose_lease_at
    cleanup = _Cleanup(events, reservation)
    cleanup.return_status = cleanup_status
    allocation = _Allocation(events, reservation, plan)
    allocation.policy_override = policy_override
    allocation.probe_overrides = probe_overrides or {}
    allocation.block_phase = block_phase
    provider = _Provider(events, allocation)
    service = NodeProfileService(
        store,
        _Restorer(events, verified),
        cleanup,
        image=image,
        provider_period_id="period-p0",
        provider_factory=lambda _limits: provider,
    )
    return service, principal, lease, events, cleanup, provider


def _validation_source(
    tmp_path: Path,
) -> tuple[EligibilityReceipt, VerifiedArchive, Any, Any]:
    files = _source_files(
        ArchiveFile(
            "tests/original.test.js",
            b"import test from 'node:test';\ntest('original behavior', () => {});\n",
        )
    )
    eligibility, baseline_verified = _eligibility_and_archive(tmp_path, files)
    baseline = snapshot_from_verified_archive(
        baseline_verified,
        expected_archive_sha256=baseline_verified.archive_sha256,
        expected_tree_sha256=baseline_verified.manifest.tree_sha256,
        expected_manifest=baseline_verified.manifest,
    )
    discovered = discover_baseline_test_paths(
        baseline,
        NODE_PROFILE_ID,
        expected_tree_sha256=baseline.tree_sha256,
    )
    context = ToolPolicyContext(
        capabilities=frozenset(ToolCapability),
        profile_id=NODE_PROFILE_ID,
        profile_version=NODE_PROFILE_VERSION,
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
        [SourceTextEdit("src/index.js", "value = 1", "value = 2")],
        expected_tree_sha256=baseline.tree_sha256,
        authorized_write_paths={"src/index.js"},
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


def test_node_profile_runs_fixed_phases_with_registry_allowlist_then_deny_all(
    tmp_path: Path,
) -> None:
    service, principal, lease, events, cleanup, provider = _service(tmp_path)

    result = asyncio.run(
        service.run(
            principal,
            lease,
            uuid4(),
            _eligibility_and_archive(tmp_path)[0],
            provider.allocation.reservation.allocation_id,
        )
    )

    phases = [value for kind, value in events if kind == "command"]
    assert phases == [
        "verify-runtime",
        "extract",
        "validate-lock",
        "probe",
        "npm-ci",
        "probe",
        "validate-install",
        "npm_test",
    ]
    policies = [value[0] for kind, value in events if kind == "policy"]
    assert policies == ["package-download", "deny-all"]
    assert phases.index("probe") < phases.index("npm-ci")
    assert phases.index("npm-ci") < phases.index("validate-install") < phases.index("npm_test")
    assert result.profile_id == NODE_PROFILE_ID
    assert result.profile_version == NODE_PROFILE_VERSION
    assert result.node_version == NODE_VERSION
    assert result.npm_version == NPM_VERSION
    assert result.test_passed is True
    assert result.allocation_status == "stopped"
    assert cleanup.calls == 1


def test_node_candidate_profile_sends_immutable_candidate_and_preserves_original_tests(
    tmp_path: Path,
) -> None:
    eligibility, baseline, artifact_id, authorization = _validation_source(tmp_path)
    candidate_plan = _build_node_candidate_profile_plan(
        eligibility,
        baseline,
        authorization,
        baseline_artifact_id=artifact_id,
    )
    service, principal, lease, events, cleanup, provider = _service(
        tmp_path,
        eligibility=eligibility,
        verified=baseline,
        expected_plan=candidate_plan,
    )

    result = asyncio.run(
        service.run(
            principal,
            lease,
            artifact_id,
            eligibility,
            provider.allocation.reservation.allocation_id,
            validation_source=authorization,
        )
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
        file.content for file in baseline.files if file.path == "tests/original.test.js"
    )
    after_tests = next(
        file.content for file in candidate.files if file.path == "tests/original.test.js"
    )
    assert before_tests == after_tests
    assert authorization.changed_paths == ("src/index.js",)
    assert result.validation_phase == "candidate"
    assert result.artifact_id == artifact_id
    assert result.tree_sha256 == authorization.candidate_source_tree_sha256
    assert result.candidate_archive_sha256 == authorization.candidate_archive_sha256
    assert [value for kind, value in events if kind == "command"].count("npm_test") == 1
    assert cleanup.calls == 1


def test_node_candidate_profile_rejects_tampered_authorization_and_cleans_up(
    tmp_path: Path,
) -> None:
    eligibility, baseline, artifact_id, authorization = _validation_source(tmp_path)
    service, principal, lease, events, cleanup, provider = _service(
        tmp_path,
        eligibility=eligibility,
        verified=baseline,
        expected_plan=build_node_profile_plan(eligibility, baseline),
    )
    tampered = replace(authorization, candidate_archive=b"tampered archive")

    with pytest.raises(
        NodeProfileError,
        match="node_profile_candidate_authorization_invalid",
    ):
        asyncio.run(
            service.run(
                principal,
                lease,
                artifact_id,
                eligibility,
                provider.allocation.reservation.allocation_id,
                validation_source=tampered,
            )
        )

    assert not any(kind == "archive_write" for kind, _value in events)
    assert cleanup.calls == 1


def test_node_candidate_profile_requires_authorization_baseline_artifact_id(
    tmp_path: Path,
) -> None:
    eligibility, baseline, artifact_id, authorization = _validation_source(tmp_path)
    service, principal, lease, events, cleanup, provider = _service(
        tmp_path,
        eligibility=eligibility,
        verified=baseline,
    )

    with pytest.raises(
        NodeProfileError,
        match="node_profile_candidate_baseline_identity_mismatch",
    ):
        asyncio.run(
            service.run(
                principal,
                lease,
                uuid4(),
                eligibility,
                provider.allocation.reservation.allocation_id,
                validation_source=authorization,
            )
        )

    assert not any(kind == "get_reservation" for kind, _value in events)
    assert cleanup.calls == 0
    assert artifact_id == authorization.baseline_artifact_id


def test_node_profile_rejects_failed_registry_probe_before_npm_ci_or_test(tmp_path: Path) -> None:
    service, principal, lease, events, cleanup, provider = _service(
        tmp_path, probe_overrides={"allowed": False}
    )

    with pytest.raises(NodeProfileError, match="node_profile_registry_probe_unverified"):
        asyncio.run(
            service.run(
                principal,
                lease,
                uuid4(),
                _eligibility_and_archive(tmp_path)[0],
                provider.allocation.reservation.allocation_id,
            )
        )

    assert not any(
        value == "npm-ci" or value == "npm_test" for kind, value in events if kind == "command"
    )
    assert cleanup.calls == 1


def test_node_profile_requires_deny_all_proof_before_install_validation_and_tests(
    tmp_path: Path,
) -> None:
    service, principal, lease, events, cleanup, provider = _service(
        tmp_path, probe_overrides={"denied": True}
    )

    with pytest.raises(NodeProfileError, match="node_profile_egress_denial_unverified"):
        asyncio.run(
            service.run(
                principal,
                lease,
                uuid4(),
                _eligibility_and_archive(tmp_path)[0],
                provider.allocation.reservation.allocation_id,
            )
        )

    phases = [value for kind, value in events if kind == "command"]
    assert "npm-ci" in phases
    assert "validate-install" not in phases
    assert "npm_test" not in phases
    assert cleanup.calls == 1


def test_node_profile_stops_after_action_fence_expires_before_policy_change(
    tmp_path: Path,
) -> None:
    service, principal, lease, events, cleanup, provider = _service(tmp_path, lose_lease_at=5)

    with pytest.raises(NodeProfileError, match="node_profile_allocation_time_exhausted"):
        asyncio.run(
            service.run(
                principal,
                lease,
                uuid4(),
                _eligibility_and_archive(tmp_path)[0],
                provider.allocation.reservation.allocation_id,
            )
        )

    phases = [value for kind, value in events if kind == "command"]
    assert phases == ["verify-runtime", "extract", "validate-lock"]
    assert not any(kind == "policy" for kind, _ in events)
    assert cleanup.calls == 1


def test_node_profile_cancellation_stops_the_owned_allocation_once(tmp_path: Path) -> None:
    service, principal, lease, _events, cleanup, provider = _service(tmp_path, block_phase="npm-ci")
    allocation = provider.allocation

    async def scenario() -> None:
        task = asyncio.create_task(
            service.run(
                principal,
                lease,
                uuid4(),
                _eligibility_and_archive(tmp_path)[0],
                allocation.reservation.allocation_id,
            )
        )
        await asyncio.wait_for(allocation.blocked_command_started.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert cleanup.calls == 1


def test_node_profile_does_not_return_success_when_cleanup_is_unresolved(tmp_path: Path) -> None:
    service, principal, lease, _events, cleanup, provider = _service(
        tmp_path, cleanup_status="unknown"
    )

    with pytest.raises(NodeProfileError, match="node_profile_cleanup_unresolved"):
        asyncio.run(
            service.run(
                principal,
                lease,
                uuid4(),
                _eligibility_and_archive(tmp_path)[0],
                provider.allocation.reservation.allocation_id,
            )
        )

    assert cleanup.calls == 1


def test_node_profile_does_not_stop_another_attempts_allocation(tmp_path: Path) -> None:
    service, principal, lease, _events, cleanup, provider = _service(tmp_path)
    other_reservation = replace(provider.allocation.reservation, attempt_id=uuid4())
    service._store.reservation = other_reservation

    with pytest.raises(NodeProfileError, match="node_profile_allocation_not_current"):
        asyncio.run(
            service.run(
                principal,
                lease,
                uuid4(),
                _eligibility_and_archive(tmp_path)[0],
                other_reservation.allocation_id,
            )
        )

    assert cleanup.calls == 0
