"""Fixed, fail-closed Python/pytest profile inside a managed sandbox."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol
from uuid import UUID

from continuum_api.artifacts.archives import (
    MAX_COMPRESSED_BYTES,
    ArchiveError,
    ArchiveFile,
    VerifiedArchive,
    create_archive,
    verify_archive,
)
from continuum_api.repositories.preflight import (
    PYTHON_TARGET_VERSION,
    EligibilityReceipt,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.sandbox_allocations import (
    AllocationReservation,
    SandboxAllocationError,
)
from continuum_api.sandbox.config import (
    ALLOCATION_MEMORY_MB,
    ALLOCATION_VCPUS,
    MAX_PROFILE_ARCHIVE_BYTES,
    MAX_PROFILE_COMMAND_OUTPUT_BYTES,
    MAX_PROFILE_COMMAND_SECONDS,
    MAX_PROFILE_INVOCATION_OUTPUT_BYTES,
    SandboxAllocationLimits,
)
from continuum_api.sandbox.contracts import CommandResult, EffectState, SandboxEffectError
from continuum_api.sandbox.python_bootstrap_recipe import (
    CONTROL_DIRECTORY,
    PYTHON_EXECUTABLE,
    PYTHON_PROFILE_ID,
    PYTHON_PROFILE_VERSION,
    WORKSPACE_PATH,
    download_verify_argv,
    extract_argv,
    install_argv,
    network_probe_argv,
    pytest_argv,
    pytest_environment,
    validate_lock_argv,
    verify_runtime_argv,
)
from continuum_api.sandbox.vercel import VercelSandboxAllocation, VercelSandboxProvider

if TYPE_CHECKING:
    from continuum_api.sandbox.validation.preparation import ValidationSourceAuthorization

PYTHON_PROFILE_EGRESS_DOMAINS = ("pypi.org", "files.pythonhosted.org")
PYTHON_PROFILE_ACTION_DEADLINE_SECONDS = 900.0
PYTHON_PROFILE_COMMAND_ID = "python-pytest-v1"
_NATIVE_SUFFIXES = (
    ".c",
    ".h",
    ".cpp",
    ".cc",
    ".cxx",
    ".hpp",
    ".hh",
    ".pyx",
    ".so",
    ".pyd",
    ".dll",
    ".dylib",
    ".a",
    ".o",
    ".obj",
    ".node",
)
_NATIVE_BUILD_FILES = frozenset({"binding.gyp", "cmakelists.txt"})
_LFS_ATTRIBUTE_PATTERN = re.compile(
    r"(?:^|\s)(?:filter|diff|merge)\s*=\s*lfs(?:\s|$)",
    re.IGNORECASE | re.MULTILINE,
)
_GIT_OID_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_LOGGER = logging.getLogger(__name__)


class PythonProfileError(RuntimeError):
    """Stable profile error without repository or provider text."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class PythonProfilePlan:
    """Verified source and lock identities passed only to trusted bootstrap."""

    archive: bytes
    archive_sha256: str
    tree_sha256: str
    lockfile_sha256: str
    package_count: int
    file_count: int
    source_bytes: int


@dataclass(frozen=True, slots=True)
class ProfileCommandOutput:
    phase: str
    exit_code: int | None
    stdout: str
    stderr: str


@dataclass(frozen=True, slots=True)
class PythonProfileResult:
    profile_id: str
    profile_version: str
    runtime_version: str
    repository_id: int
    commit_sha: str
    tree_sha256: str
    artifact_id: UUID
    allocation_id: UUID
    validation_phase: Literal["baseline", "candidate"]
    image: str
    command_id: str
    baseline_git_tree_sha256: str
    baseline_archive_tree_sha256: str
    candidate_archive_sha256: str | None
    candidate_source_tree_sha256: str | None
    package_count: int
    test_exit_code: int
    test_passed: bool
    outputs: tuple[ProfileCommandOutput, ...]
    allocation_status: str
    log_budget_scope: Literal["profile_invocation_pending_run_ledger"] = (
        "profile_invocation_pending_run_ledger"
    )

    @property
    def python_version(self) -> str:
        """Explicit runtime field for validation evidence; keep the existing alias."""
        return self.runtime_version


class PythonProfileStore(Protocol):
    def get_for_observation(
        self, principal: Principal, allocation_id: UUID
    ) -> AllocationReservation: ...

    def authorize_profile_action(
        self,
        principal: Principal,
        lease: ExecutionLease,
        allocation_id: UUID,
        expected_session_id: str,
        provider_period_id: str | None,
    ) -> int: ...


class CheckpointRestorer(Protocol):
    async def restore_archive(
        self, principal: Principal, lease: ExecutionLease, artifact_id: UUID
    ) -> VerifiedArchive: ...


class AllocationCleanup(Protocol):
    async def stop(self, principal: Principal, allocation_id: UUID) -> AllocationReservation: ...


class PythonProfileService:
    """Restore a committed source, install approved wheels, then run pytest."""

    def __init__(
        self,
        store: PythonProfileStore,
        checkpoints: CheckpointRestorer,
        cleanup_service: AllocationCleanup,
        *,
        image: str | None,
        provider_period_id: str | None,
        provider_factory: Callable[[SandboxAllocationLimits], VercelSandboxProvider],
        deadline_seconds: float = PYTHON_PROFILE_ACTION_DEADLINE_SECONDS,
    ) -> None:
        if deadline_seconds <= 0:
            raise ValueError("python profile deadline must be positive")
        self._store = store
        self._checkpoints = checkpoints
        self._cleanup_service = cleanup_service
        self._image = image
        self._provider_period_id = provider_period_id
        self._provider_factory = provider_factory
        self._deadline_seconds = deadline_seconds

    @property
    def configured_image(self) -> str | None:
        """Configured image reference to bind to both success and failure evidence."""
        return self._image

    @property
    def command_id(self) -> str:
        return PYTHON_PROFILE_COMMAND_ID

    async def run(
        self,
        principal: Principal,
        lease: ExecutionLease,
        artifact_id: UUID,
        eligibility: EligibilityReceipt,
        allocation_id: UUID,
        *,
        validation_source: ValidationSourceAuthorization | None = None,
    ) -> PythonProfileResult:
        """Validate pins, restore via T15 authorization, and execute fixed phases."""
        if validation_source is not None:
            from continuum_api.sandbox.validation.preparation import ValidationSourceAuthorization

            if not isinstance(validation_source, ValidationSourceAuthorization):
                raise PythonProfileError("python_profile_candidate_authorization_invalid")
            if artifact_id != validation_source.baseline_artifact_id:
                raise PythonProfileError("python_profile_candidate_baseline_identity_mismatch")
        if principal.account_id != lease.owner_id:
            raise PythonProfileError("python_profile_owner_mismatch")

        reservation: AllocationReservation | None = None
        cleanup_attempted = False
        try:
            async with asyncio.timeout(self._deadline_seconds):
                candidate = await asyncio.to_thread(
                    self._store.get_for_observation,
                    principal,
                    allocation_id,
                )
                self._validate_reservation(candidate, lease)
                reservation = candidate
                image = self._require_image()
                if self._provider_period_id is None:
                    raise PythonProfileError("python_profile_period_unconfigured")
                verified = await self._checkpoints.restore_archive(principal, lease, artifact_id)
                if validation_source is None:
                    plan = build_python_profile_plan(eligibility, verified)
                else:
                    plan = _build_python_candidate_profile_plan(
                        eligibility,
                        verified,
                        validation_source,
                        baseline_artifact_id=artifact_id,
                    )
                assert reservation.provider_session_id is not None

                limits = SandboxAllocationLimits(
                    image=image,
                    session_seconds=reservation.max_requested_seconds,
                    vcpus=ALLOCATION_VCPUS,
                    memory_mb=ALLOCATION_MEMORY_MB,
                )
                provider = self._provider_factory(limits)
                outputs: list[ProfileCommandOutput] = []
                total_output_bytes = 0
                async with provider.connection() as connected_provider:
                    allocation = await connected_provider.observe_exact_session(
                        reservation.provider_name,
                        reservation.provider_session_id,
                    )
                    self._validate_active_session(reservation, allocation, image)
                    assert allocation is not None
                    self._require_deny_all(allocation.network_policy)

                    runtime, total_output_bytes = await self._run_trusted(
                        principal,
                        lease,
                        reservation,
                        allocation,
                        verify_runtime_argv(),
                        "verify_runtime",
                        outputs,
                        total_output_bytes,
                    )
                    if (
                        runtime.get("implementation") != "CPython"
                        or runtime.get("python_version") != PYTHON_TARGET_VERSION
                        or runtime.get("machine") != "x86_64"
                        or runtime.get("profile_version") != PYTHON_PROFILE_VERSION
                    ):
                        raise PythonProfileError("python_profile_runtime_mismatch")

                    timeout = await self._authorize(principal, lease, reservation, allocation)
                    await allocation.write_profile_archive(
                        plan.archive,
                        timeout_seconds=min(MAX_PROFILE_COMMAND_SECONDS, timeout),
                    )

                    extracted, total_output_bytes = await self._run_trusted(
                        principal,
                        lease,
                        reservation,
                        allocation,
                        extract_argv(
                            archive_sha256=plan.archive_sha256,
                            tree_sha256=plan.tree_sha256,
                            lockfile_sha256=plan.lockfile_sha256,
                        ),
                        "extract_source",
                        outputs,
                        total_output_bytes,
                    )
                    if (
                        extracted.get("archive_sha256") != plan.archive_sha256
                        or extracted.get("tree_sha256") != plan.tree_sha256
                        or extracted.get("lockfile_sha256") != plan.lockfile_sha256
                        or extracted.get("file_count") != plan.file_count
                        or extracted.get("source_bytes") != plan.source_bytes
                    ):
                        raise PythonProfileError("python_profile_extraction_receipt_mismatch")
                    lock_receipt, total_output_bytes = await self._run_trusted(
                        principal,
                        lease,
                        reservation,
                        allocation,
                        validate_lock_argv(lockfile_sha256=plan.lockfile_sha256),
                        "validate_lock",
                        outputs,
                        total_output_bytes,
                    )
                    if (
                        lock_receipt.get("lockfile_sha256") != plan.lockfile_sha256
                        or lock_receipt.get("package_count") != plan.package_count
                    ):
                        raise PythonProfileError("python_profile_lock_receipt_mismatch")
                    await self._set_and_verify_policy(
                        principal,
                        lease,
                        reservation,
                        allocation,
                        "package-download",
                    )
                    allowed_probe, total_output_bytes = await self._run_trusted(
                        principal,
                        lease,
                        reservation,
                        allocation,
                        network_probe_argv(expected="allowed"),
                        "pypi_egress_probe",
                        outputs,
                        total_output_bytes,
                    )
                    if (
                        allowed_probe.get("expected") != "allowed"
                        or allowed_probe.get("observed_allowed") is not True
                    ):
                        raise PythonProfileError("python_profile_pypi_probe_unverified")
                    downloaded_wheels, total_output_bytes = await self._run_trusted(
                        principal,
                        lease,
                        reservation,
                        allocation,
                        download_verify_argv(lockfile_sha256=plan.lockfile_sha256),
                        "download_verify_wheels",
                        outputs,
                        total_output_bytes,
                    )
                    total_wheel_bytes = downloaded_wheels.get("total_bytes")
                    if (
                        downloaded_wheels.get("package_count") != plan.package_count
                        or type(total_wheel_bytes) is not int
                    ):
                        raise PythonProfileError("python_profile_wheel_receipt_invalid")
                    if not 0 < total_wheel_bytes <= 100 * 1024 * 1024:
                        raise PythonProfileError("python_profile_wheel_receipt_invalid")

                    await self._set_and_verify_policy(
                        principal,
                        lease,
                        reservation,
                        allocation,
                        "deny-all",
                    )
                    denied_probe, total_output_bytes = await self._run_trusted(
                        principal,
                        lease,
                        reservation,
                        allocation,
                        network_probe_argv(expected="denied"),
                        "deny_all_egress_probe",
                        outputs,
                        total_output_bytes,
                    )
                    if (
                        denied_probe.get("expected") != "denied"
                        or denied_probe.get("observed_allowed") is not False
                    ):
                        raise PythonProfileError("python_profile_egress_denial_unverified")
                    install_receipt, total_output_bytes = await self._run_trusted(
                        principal,
                        lease,
                        reservation,
                        allocation,
                        install_argv(lockfile_sha256=plan.lockfile_sha256),
                        "install_verified_wheels",
                        outputs,
                        total_output_bytes,
                    )
                    if install_receipt.get("installed_wheel_count") != plan.package_count:
                        raise PythonProfileError("python_profile_install_receipt_mismatch")

                    pytest_output, total_output_bytes = await self._run_command(
                        principal,
                        lease,
                        reservation,
                        allocation,
                        pytest_argv()[0],
                        pytest_argv()[1:],
                        phase="pytest",
                        timeout_seconds=MAX_PROFILE_COMMAND_SECONDS,
                        cwd=WORKSPACE_PATH,
                        env=pytest_environment(),
                        outputs=outputs,
                        total_output_bytes=total_output_bytes,
                    )
                assert pytest_output.exit_code is not None
                cleanup_attempted = True
                cleanup = await self._stop_allocation_once(principal, allocation_id)
                if cleanup is None or cleanup.status not in {
                    "stopped",
                    "destroyed",
                    "not_applied",
                    "released",
                }:
                    raise PythonProfileError("python_profile_cleanup_unresolved")
                return PythonProfileResult(
                    profile_id=PYTHON_PROFILE_ID,
                    profile_version=PYTHON_PROFILE_VERSION,
                    runtime_version=PYTHON_TARGET_VERSION,
                    repository_id=eligibility.repository_id,
                    commit_sha=eligibility.commit_sha,
                    tree_sha256=plan.tree_sha256,
                    artifact_id=artifact_id,
                    allocation_id=allocation_id,
                    validation_phase="candidate" if validation_source is not None else "baseline",
                    image=image,
                    command_id=PYTHON_PROFILE_COMMAND_ID,
                    baseline_git_tree_sha256=eligibility.tree_sha,
                    baseline_archive_tree_sha256=verified.manifest.tree_sha256,
                    candidate_archive_sha256=plan.archive_sha256
                    if validation_source is not None
                    else None,
                    candidate_source_tree_sha256=plan.tree_sha256
                    if validation_source is not None
                    else None,
                    package_count=plan.package_count,
                    test_exit_code=pytest_output.exit_code,
                    test_passed=pytest_output.exit_code == 0,
                    outputs=tuple(outputs),
                    allocation_status=cleanup.status,
                )
        except asyncio.CancelledError:
            if reservation is not None and not cleanup_attempted:
                await self._stop_allocation_once(principal, allocation_id)
            raise
        except TimeoutError:
            if reservation is not None and not cleanup_attempted:
                await self._stop_allocation_once(principal, allocation_id)
            raise PythonProfileError("python_profile_deadline_elapsed") from None
        except PythonProfileError:
            if reservation is not None and not cleanup_attempted:
                await self._stop_allocation_once(principal, allocation_id)
            raise
        except SandboxEffectError as error:
            if reservation is not None and not cleanup_attempted:
                await self._stop_allocation_once(principal, allocation_id)
            raise PythonProfileError(f"python_profile_{error.reason}") from None
        except SandboxAllocationError as error:
            if reservation is not None and not cleanup_attempted:
                await self._stop_allocation_once(principal, allocation_id)
            raise PythonProfileError(error.code) from None
        except LeaseLost:
            if reservation is not None and not cleanup_attempted:
                await self._stop_allocation_once(principal, allocation_id)
            raise PythonProfileError("python_profile_lease_lost") from None
        except ArchiveError as error:
            if reservation is not None and not cleanup_attempted:
                await self._stop_allocation_once(principal, allocation_id)
            raise PythonProfileError(error.code) from None
        except Exception as error:
            if reservation is not None and not cleanup_attempted:
                await self._stop_allocation_once(principal, allocation_id)
            _LOGGER.warning(
                "python_profile_failed error_type=%s",
                type(error).__name__,
            )
            raise PythonProfileError("python_profile_unavailable") from None

    async def _stop_allocation_once(
        self, principal: Principal, allocation_id: UUID
    ) -> AllocationReservation | None:
        task = asyncio.create_task(self._cleanup_service.stop(principal, allocation_id))
        try:
            async with asyncio.timeout(95):
                reservation = await asyncio.shield(task)
                if reservation.status not in {
                    "stopped",
                    "destroyed",
                    "not_applied",
                    "released",
                }:
                    _LOGGER.warning(
                        "python_profile_cleanup_unresolved status=%s",
                        reservation.status,
                    )
                return reservation
        except asyncio.CancelledError:
            try:
                async with asyncio.timeout(95):
                    return await asyncio.shield(task)
            except Exception as error:
                _LOGGER.warning(
                    "python_profile_cleanup_failed error_type=%s",
                    type(error).__name__,
                )
        except Exception as error:
            _LOGGER.warning(
                "python_profile_cleanup_failed error_type=%s",
                type(error).__name__,
            )
        return None

    def _require_image(self) -> str:
        if self._image is None or not self._image.strip():
            raise PythonProfileError("python_profile_image_unconfigured")
        image = self._image.strip()
        try:
            SandboxAllocationLimits(
                image=image,
                session_seconds=1,
                vcpus=ALLOCATION_VCPUS,
                memory_mb=ALLOCATION_MEMORY_MB,
            )
        except ValueError:
            raise PythonProfileError("python_profile_image_invalid") from None
        return image

    @staticmethod
    def _validate_reservation(reservation: AllocationReservation, lease: ExecutionLease) -> None:
        if (
            reservation.status != "running"
            or reservation.provider_session_id is None
            or reservation.owner_id != lease.owner_id
            or reservation.run_id != lease.run_id
            or reservation.step_id != lease.step_id
            or reservation.job_id != lease.job_id
            or reservation.generation != lease.generation
            or reservation.attempt_id != lease.attempt_id
            or reservation.lease_owner != lease.lease_owner
        ):
            raise PythonProfileError("python_profile_allocation_not_current")

    @staticmethod
    def _validate_active_session(
        reservation: AllocationReservation,
        allocation: VercelSandboxAllocation | None,
        image: str,
    ) -> None:
        if allocation is None:
            raise PythonProfileError("python_profile_session_not_observed")
        observation = allocation.observation()
        if (
            allocation.name != reservation.provider_name
            or allocation.session_id != reservation.provider_session_id
            or allocation.current_session_id != reservation.provider_session_id
            or allocation.image != image
            or (observation.sandbox_status or "").casefold() != "running"
            or (observation.session_status or "").casefold() != "running"
        ):
            raise PythonProfileError("python_profile_session_identity_mismatch")

    async def _authorize(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: AllocationReservation,
        allocation: VercelSandboxAllocation,
    ) -> int:
        session_id = reservation.provider_session_id
        if session_id is None:
            raise PythonProfileError("python_profile_session_identity_missing")
        try:
            remaining = await asyncio.to_thread(
                self._store.authorize_profile_action,
                principal,
                lease,
                reservation.allocation_id,
                session_id,
                self._provider_period_id,
            )
        except (SandboxAllocationError, LeaseLost) as error:
            code = error.code if isinstance(error, SandboxAllocationError) else "lease_lost"
            raise PythonProfileError(code) from None
        if remaining <= 0:
            raise PythonProfileError("python_profile_allocation_time_exhausted")
        if (
            allocation.name != reservation.provider_name
            or allocation.session_id != session_id
            or allocation.current_session_id != session_id
        ):
            raise PythonProfileError("python_profile_session_identity_mismatch")
        return remaining

    async def _run_trusted(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: AllocationReservation,
        allocation: VercelSandboxAllocation,
        argv: Sequence[str],
        phase: str,
        outputs: list[ProfileCommandOutput],
        total_output_bytes: int,
    ) -> tuple[dict[str, object], int]:
        if not argv or argv[0] != PYTHON_EXECUTABLE:
            raise PythonProfileError("python_profile_bootstrap_command_invalid")
        result, total = await self._run_command(
            principal,
            lease,
            reservation,
            allocation,
            argv[0],
            argv[1:],
            phase=phase,
            timeout_seconds=MAX_PROFILE_COMMAND_SECONDS,
            cwd=CONTROL_DIRECTORY,
            env=None,
            outputs=outputs,
            total_output_bytes=total_output_bytes,
        )
        if result.exit_code != 0:
            raise PythonProfileError(f"python_profile_{phase}_failed")
        try:
            payload = json.loads(result.stdout)
        except (json.JSONDecodeError, RecursionError):
            raise PythonProfileError(f"python_profile_{phase}_receipt_invalid") from None
        if not isinstance(payload, dict):
            raise PythonProfileError(f"python_profile_{phase}_receipt_invalid")
        return payload, total

    async def _run_command(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: AllocationReservation,
        allocation: VercelSandboxAllocation,
        command: str,
        args: Sequence[str],
        *,
        phase: str,
        timeout_seconds: int,
        cwd: str,
        env: Mapping[str, str] | None,
        outputs: list[ProfileCommandOutput],
        total_output_bytes: int,
    ) -> tuple[CommandResult, int]:
        remaining = await self._authorize(principal, lease, reservation, allocation)
        action_timeout = min(timeout_seconds, MAX_PROFILE_COMMAND_SECONDS, remaining)
        if action_timeout <= 0:
            raise PythonProfileError("python_profile_allocation_time_exhausted")
        result = await allocation.run_profile_command(
            command,
            args,
            cwd=cwd,
            env=env,
            timeout_seconds=action_timeout,
            output_limit_bytes=MAX_PROFILE_COMMAND_OUTPUT_BYTES,
        )
        output_bytes = len(result.stdout.encode("utf-8")) + len(result.stderr.encode("utf-8"))
        if (
            result.effect is not EffectState.APPLIED
            or output_bytes > MAX_PROFILE_COMMAND_OUTPUT_BYTES
            or total_output_bytes + output_bytes > MAX_PROFILE_INVOCATION_OUTPUT_BYTES
        ):
            raise PythonProfileError("python_profile_command_outcome_unknown")
        outputs.append(
            ProfileCommandOutput(
                phase=phase,
                exit_code=result.exit_code,
                stdout=result.stdout,
                stderr=result.stderr,
            )
        )
        return result, total_output_bytes + output_bytes

    async def _set_and_verify_policy(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: AllocationReservation,
        allocation: VercelSandboxAllocation,
        policy: Literal["package-download", "deny-all"],
    ) -> None:
        remaining = await self._authorize(principal, lease, reservation, allocation)
        readback = await allocation.set_python_profile_network_policy(
            policy,
            timeout_seconds=min(MAX_PROFILE_COMMAND_SECONDS, remaining),
        )
        if policy == "deny-all":
            self._require_deny_all(readback)
        elif not (
            readback.mode == "custom"
            and set(readback.allowed_domains) == set(PYTHON_PROFILE_EGRESS_DOMAINS)
            and len(readback.allowed_domains) == len(PYTHON_PROFILE_EGRESS_DOMAINS)
            and readback.subnets is None
            and not readback.injection_rules
            and not readback.forward_rules
            and not readback.response_rules
        ):
            raise PythonProfileError("python_profile_package_egress_policy_unverified")

    @staticmethod
    def _require_deny_all(readback: object) -> None:
        if readback is None or not (
            getattr(readback, "mode", None) == "deny-all"
            and not getattr(readback, "allowed_domains", ())
            and getattr(readback, "subnets", None) is None
            and not getattr(readback, "injection_rules", ())
            and not getattr(readback, "forward_rules", ())
            and not getattr(readback, "response_rules", ())
        ):
            raise PythonProfileError("python_profile_deny_all_policy_unverified")


def build_python_profile_plan(
    eligibility: EligibilityReceipt, verified: VerifiedArchive
) -> PythonProfilePlan:
    """Bind a committed T14 archive to T13's full Git tree and manifest pins."""
    return _build_python_profile_plan(eligibility, verified)


def _build_python_profile_plan(
    eligibility: EligibilityReceipt,
    verified: VerifiedArchive,
    *,
    candidate_authorization: ValidationSourceAuthorization | None = None,
) -> PythonProfilePlan:
    """Build baseline or already-authorized candidate bytes for the profile runner."""
    expected_file_count = (
        eligibility.file_count
        if candidate_authorization is None
        else candidate_authorization.candidate_file_count
    )
    expected_source_bytes = (
        eligibility.source_bytes
        if candidate_authorization is None
        else candidate_authorization.candidate_source_bytes
    )
    if (
        eligibility.profile_id != PYTHON_PROFILE_ID
        or eligibility.profile_version != PYTHON_PROFILE_VERSION
        or type(eligibility.repository_id) is not int
        or eligibility.repository_id <= 0
        or type(eligibility.package_count) is not int
        or not 0 < eligibility.package_count <= 24
        or not isinstance(eligibility.commit_sha, str)
        or _GIT_OID_PATTERN.fullmatch(eligibility.commit_sha) is None
        or not isinstance(eligibility.tree_sha, str)
        or _GIT_OID_PATTERN.fullmatch(eligibility.tree_sha) is None
        or len(eligibility.tree_sha) != len(eligibility.commit_sha)
        or verified.manifest.format_version != 1
        or re.fullmatch(r"[0-9a-f]{64}", verified.manifest.tree_sha256) is None
        or re.fullmatch(r"[0-9a-f]{64}", verified.archive_sha256) is None
        or type(eligibility.file_count) is not int
        or type(eligibility.source_bytes) is not int
        or len(verified.manifest.entries) != expected_file_count
        or sum(entry.size for entry in verified.manifest.entries) != expected_source_bytes
    ):
        raise PythonProfileError("python_profile_eligibility_archive_mismatch")

    hash_name = "sha1" if len(eligibility.commit_sha) == 40 else "sha256"
    git_files: dict[str, ArchiveFile] = {}
    attributes_files: dict[str, bytes] = {}
    for source_file in verified.files:
        if source_file.path in git_files:
            raise PythonProfileError("python_profile_archive_duplicate_path")
        git_files[source_file.path] = source_file
        basename = source_file.path.rsplit("/", 1)[-1].lower()
        if basename in _NATIVE_BUILD_FILES or source_file.path.lower().endswith(_NATIVE_SUFFIXES):
            raise PythonProfileError("python_profile_native_source_unsupported")
        if basename == ".lfsconfig":
            raise PythonProfileError("python_profile_lfs_unsupported")
        if basename == ".gitattributes":
            attributes_files[source_file.path] = source_file.content
    lock_file = git_files.get("requirements.lock")
    if lock_file is None or len(lock_file.content) > 256 * 1024:
        raise PythonProfileError("python_profile_lock_missing_or_oversized")
    if "setup.py" in git_files or "setup.cfg" in git_files:
        raise PythonProfileError("python_profile_build_hook_unsupported")
    pyproject = git_files.get("pyproject.toml")
    if pyproject is not None:
        try:
            project_metadata = tomllib.loads(pyproject.content.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError, RecursionError):
            raise PythonProfileError("python_profile_pyproject_invalid") from None
        if "build-system" in project_metadata:
            raise PythonProfileError("python_profile_build_hook_unsupported")
    expected_manifest_paths = {"requirements.lock", *attributes_files}
    if pyproject is not None:
        expected_manifest_paths.add("pyproject.toml")
    for content in attributes_files.values():
        try:
            attributes = content.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            raise PythonProfileError("python_profile_attributes_invalid") from None
        if _LFS_ATTRIBUTE_PATTERN.search(attributes):
            raise PythonProfileError("python_profile_lfs_unsupported")
    python_version_file = git_files.get(".python-version")
    if python_version_file is not None and python_version_file.content.strip() != (
        PYTHON_TARGET_VERSION.encode("ascii")
    ):
        raise PythonProfileError("python_profile_version_unsupported")
    if not any(
        path.endswith(".py") and ("/" not in path or path.startswith("src/")) for path in git_files
    ):
        raise PythonProfileError("python_profile_layout_unsupported")

    manifest_blobs: dict[str, str] = {}
    for path, git_sha in eligibility.manifest_blobs:
        if (
            path in manifest_blobs
            or not isinstance(git_sha, str)
            or len(git_sha) != len(eligibility.commit_sha)
            or _GIT_OID_PATTERN.fullmatch(git_sha) is None
        ):
            raise PythonProfileError("python_profile_manifest_pin_invalid")
        manifest_blobs[path] = git_sha
    if "requirements.lock" not in manifest_blobs:
        raise PythonProfileError("python_profile_lock_pin_missing")
    if set(manifest_blobs) != expected_manifest_paths:
        raise PythonProfileError("python_profile_manifest_pin_invalid")
    for path, expected_sha in manifest_blobs.items():
        manifest_file = git_files.get(path)
        if manifest_file is None or _git_blob_sha(manifest_file.content, hash_name) != expected_sha:
            raise PythonProfileError("python_profile_manifest_pin_mismatch")

    actual_tree_sha = _git_tree_sha(verified.files, hash_name)
    if candidate_authorization is None and actual_tree_sha != eligibility.tree_sha:
        raise PythonProfileError("python_profile_source_tree_mismatch")

    try:
        bundle = create_archive(verified.files)
    except ArchiveError as error:
        raise PythonProfileError(error.code) from None
    if (
        bundle.manifest != verified.manifest
        or len(bundle.archive) > MAX_COMPRESSED_BYTES
        or len(bundle.archive) > MAX_PROFILE_ARCHIVE_BYTES
        or (
            candidate_authorization is not None
            and (
                bundle.archive != candidate_authorization.candidate_archive
                or bundle.archive_sha256 != candidate_authorization.candidate_archive_sha256
            )
        )
    ):
        raise PythonProfileError("python_profile_archive_rebuild_mismatch")
    try:
        rechecked = verify_archive(
            bundle.archive,
            expected_archive_sha256=bundle.archive_sha256,
            expected_tree_sha256=bundle.manifest.tree_sha256,
            expected_manifest=verified.manifest,
        )
    except ArchiveError as error:
        raise PythonProfileError(error.code) from None
    if rechecked.manifest != verified.manifest:
        raise PythonProfileError("python_profile_archive_rebuild_mismatch")
    return PythonProfilePlan(
        archive=bundle.archive,
        archive_sha256=bundle.archive_sha256,
        tree_sha256=bundle.manifest.tree_sha256,
        lockfile_sha256=hashlib.sha256(lock_file.content).hexdigest(),
        package_count=eligibility.package_count,
        file_count=expected_file_count,
        source_bytes=expected_source_bytes,
    )


def _build_python_candidate_profile_plan(
    eligibility: EligibilityReceipt,
    restored_baseline: VerifiedArchive,
    validation_source: ValidationSourceAuthorization,
    *,
    baseline_artifact_id: UUID,
) -> PythonProfilePlan:
    """Validate the committed baseline, then plan only the reauthorized candidate."""
    from continuum_api.sandbox.validation.preparation import (
        verify_validation_source_authorization,
    )

    try:
        candidate = verify_validation_source_authorization(
            validation_source,
            eligibility=eligibility,
            baseline_artifact_id=baseline_artifact_id,
            restored_baseline=restored_baseline,
        )
        authorization_matches = (
            validation_source.profile_id == PYTHON_PROFILE_ID
            and validation_source.profile_version == PYTHON_PROFILE_VERSION
            and validation_source.eligibility.to_cache_payload() == eligibility.to_cache_payload()
            and validation_source.candidate_archive_sha256 == candidate.archive_sha256
            and validation_source.candidate_source_tree_sha256 == candidate.manifest.tree_sha256
            and validation_source.candidate_manifest == candidate.manifest
            and validation_source.candidate_file_count == len(candidate.manifest.entries)
            and validation_source.candidate_source_bytes
            == sum(entry.size for entry in candidate.manifest.entries)
        )
    except Exception:
        raise PythonProfileError("python_profile_candidate_authorization_invalid") from None
    if not authorization_matches:
        raise PythonProfileError("python_profile_candidate_authorization_invalid")
    return _build_python_profile_plan(
        eligibility,
        candidate,
        candidate_authorization=validation_source,
    )


def _git_blob_sha(content: bytes, hash_name: str) -> str:
    digest = hashlib.new(hash_name)
    digest.update(f"blob {len(content)}\0".encode("ascii"))
    digest.update(content)
    return digest.hexdigest()


def _git_tree_sha(files: Sequence[ArchiveFile], hash_name: str) -> str:
    """Recreate Git's tree object IDs from T14's validated regular files."""
    root: dict[str, object] = {}
    for source_file in files:
        components = source_file.path.split("/")
        directory = root
        for component in components[:-1]:
            current = directory.get(component)
            if current is None:
                current = {}
                directory[component] = current
            if not isinstance(current, dict):
                raise PythonProfileError("python_profile_archive_path_conflict")
            directory = current
        filename = components[-1]
        if filename in directory:
            raise PythonProfileError("python_profile_archive_duplicate_path")
        directory[filename] = source_file

    def hash_directory(directory: dict[str, object]) -> str:
        entries: list[tuple[bytes, bytes]] = []
        for name, value in directory.items():
            try:
                name_bytes = name.encode("utf-8")
            except UnicodeEncodeError:
                raise PythonProfileError("python_profile_archive_path_invalid") from None
            if isinstance(value, dict):
                mode = b"40000"
                object_sha = hash_directory(value)
                order_key = name_bytes + b"/"
            elif isinstance(value, ArchiveFile):
                mode = b"100755" if value.mode == 0o755 else b"100644"
                object_sha = _git_blob_sha(value.content, hash_name)
                order_key = name_bytes + b"\0"
            else:
                raise PythonProfileError("python_profile_archive_path_invalid")
            entry = mode + b" " + name_bytes + b"\0" + bytes.fromhex(object_sha)
            entries.append((order_key, entry))
        entries.sort(key=lambda item: item[0])
        tree_content = b"".join(entry for _, entry in entries)
        digest = hashlib.new(hash_name)
        digest.update(f"tree {len(tree_content)}\0".encode("ascii"))
        digest.update(tree_content)
        return digest.hexdigest()

    return hash_directory(root)
