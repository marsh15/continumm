"""Narrow, exact-session adapter over the official asynchronous Vercel SDK."""

from __future__ import annotations

import asyncio
import re
import subprocess
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import timedelta
from typing import Any, Protocol, TypeVar

from vercel import sandbox as vercel_sandbox
from vercel.api import session as sdk_session
from vercel.sandbox import (
    NetworkPolicy,
    NetworkPolicyReadback,
    Sandbox,
    SandboxApiError,
    SandboxCredentials,
    SandboxCredentialsError,
    SandboxResources,
    SandboxRuntimeSession,
    SandboxServiceOptions,
    SandboxTerminalStateError,
)

from continuum_api.sandbox.config import (
    MAX_COMMAND_SECONDS,
    MAX_OUTPUT_BYTES,
    MAX_PROFILE_ARCHIVE_BYTES,
    MAX_PROFILE_COMMAND_OUTPUT_BYTES,
    MAX_PROFILE_COMMAND_SECONDS,
    MAX_TRANSFER_BYTES,
    PROFILE_ARCHIVE_CHUNK_BYTES,
    SANDBOX_WORKING_DIRECTORY,
    TRANSFER_CHUNK_BYTES,
    SandboxAllocationLimits,
    SandboxLimits,
    VercelCredentials,
    validate_fixture_filename,
    validate_sandbox_name,
)
from continuum_api.sandbox.contracts import (
    CommandResult,
    EffectState,
    ProcessObservation,
    SandboxAction,
    SandboxEffectError,
    SandboxObservation,
)
from continuum_api.sandbox.node_bootstrap_recipe import (
    npm_ci_environment,
    npm_test_environment,
)
from continuum_api.sandbox.python_bootstrap_recipe import (
    ARCHIVE_INPUT_PATH,
    CONTROL_DIRECTORY,
    WORKSPACE_PATH,
)

_T = TypeVar("_T")
_ACTION_TIMEOUT_SECONDS = 30
_CREATE_TIMEOUT_SECONDS = 60
_OBSERVE_TIMEOUT_SECONDS = 10
_SESSION_STOP_TIMEOUT_SECONDS = 20
_DESTROY_TIMEOUT_SECONDS = 20
_FILE_TRANSFER_TIMEOUT_SECONDS = 20
_MAX_EXACT_SESSION_LOOKUPS = 20
_PROCESS_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class _SandboxAPI(Protocol):
    def create_sandbox(self, **kwargs: Any) -> Awaitable[Sandbox]: ...

    async def get_sandbox(self, *, name: str) -> Sandbox: ...

    def query_sessions(
        self, *, name: str, page_size: int, sort_order: str
    ) -> AsyncIterator[SandboxRuntimeSession]: ...


def _error_effect(action: SandboxAction, error: Exception) -> SandboxEffectError:
    """Map SDK exceptions to a safe category without exposing response text."""

    if isinstance(error, SandboxTerminalStateError):
        effect = EffectState.APPLIED if action is SandboxAction.CREATE else EffectState.UNKNOWN
        return SandboxEffectError(action, effect, "provider_terminal_state_resource_may_exist")
    if isinstance(error, SandboxCredentialsError):
        return SandboxEffectError(action, EffectState.NOT_APPLIED, "credentials_unavailable")
    if isinstance(error, SandboxApiError):
        status_code = error.status_code
        if action is SandboxAction.OBSERVE:
            reason = "provider_not_found" if status_code == 404 else "observation_unavailable"
            return SandboxEffectError(action, EffectState.UNKNOWN, reason)
        if 400 <= status_code < 500 and status_code not in {408, 409, 425, 429}:
            reason = "provider_not_found" if status_code == 404 else "provider_rejected"
            return SandboxEffectError(action, EffectState.NOT_APPLIED, reason)
        return SandboxEffectError(action, EffectState.UNKNOWN, "provider_response_uncertain")
    if isinstance(error, TimeoutError):
        return SandboxEffectError(action, EffectState.UNKNOWN, "client_deadline_elapsed")
    return SandboxEffectError(action, EffectState.UNKNOWN, "provider_call_interrupted")


def _status_text(status: object | None) -> str | None:
    if status is None:
        return None
    value = getattr(status, "value", status)
    return str(value)


def _is_stopped(status: object | None) -> bool:
    value = _status_text(status)
    return value is not None and value.casefold() == "stopped"


def _utf8_prefix(value: str, byte_limit: int) -> tuple[str, int]:
    """Return the longest whole-code-point prefix within a byte budget."""

    parts: list[str] = []
    used = 0
    for character in value:
        size = len(character.encode("utf-8"))
        if used + size > byte_limit:
            break
        parts.append(character)
        used += size
    return "".join(parts), used


class VercelSandboxProvider:
    """One project-scoped SDK session; deliberately has no allocation queue."""

    def __init__(
        self,
        *,
        limits: SandboxLimits | SandboxAllocationLimits,
        credentials: VercelCredentials,
        sdk: _SandboxAPI = vercel_sandbox,
        sdk_session_factory: Callable[..., AbstractAsyncContextManager[Any]] = sdk_session,
    ) -> None:
        self._limits = limits
        self._credentials = credentials
        self._sdk = sdk
        self._sdk_session_factory = sdk_session_factory
        self._create_attempted: set[str] = set()
        self._destroy_attempted: set[str] = set()

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[VercelSandboxProvider]:
        """Bind explicit control-plane credentials to the SDK context only."""

        async def credentials_factory() -> SandboxCredentials:
            return SandboxCredentials(
                token=self._credentials.token,
                team_id=self._credentials.team_id,
                project_id=self._credentials.project_id,
            )

        options = SandboxServiceOptions(
            credentials_factory=credentials_factory,
            file_transfer_timeout=timedelta(seconds=_FILE_TRANSFER_TIMEOUT_SECONDS),
        )
        async with self._sdk_session_factory(service_options=[options]):
            yield self

    async def create(
        self,
        name: str,
        *,
        tags: Mapping[str, str] | None = None,
    ) -> VercelSandboxAllocation:
        """Create one bounded, deny-all, non-persistent named sandbox."""

        validate_sandbox_name(name)
        if name in self._create_attempted:
            raise SandboxEffectError(
                SandboxAction.CREATE,
                EffectState.NOT_APPLIED,
                "create_already_attempted_in_this_process",
            )
        self._create_attempted.add(name)

        try:
            box = await self._call(
                SandboxAction.CREATE,
                _CREATE_TIMEOUT_SECONDS,
                self._sdk.create_sandbox(
                    name=name,
                    image=self._limits.image,
                    ports=[],
                    execution_time_limit=timedelta(seconds=self._limits.session_seconds),
                    resources=SandboxResources(
                        vcpus=self._limits.vcpus,
                        memory=self._limits.memory_mb,
                    ),
                    persistent=False,
                    network_policy=NetworkPolicy.deny_all(),
                    env=None,
                    tags=None if tags is None else dict(tags),
                    mounts=None,
                    source=None,
                ),
            )
        except SandboxEffectError:
            raise
        except Exception as error:
            raise _error_effect(SandboxAction.CREATE, error) from None

        runtime = box.current_session
        if runtime is None:
            raise SandboxEffectError(
                SandboxAction.CREATE,
                EffectState.APPLIED,
                "create_acknowledged_without_session_handle",
            )
        return VercelSandboxAllocation(self, box, runtime)

    async def observe(self, name: str) -> VercelSandboxAllocation | None:
        """Read a stable name without resuming it or repeating creation."""

        validate_sandbox_name(name)
        box = await self._observe_box(name)
        return None if box is None else VercelSandboxAllocation(self, box, box.current_session)

    async def observe_exact_session(
        self, name: str, expected_session_id: str
    ) -> VercelSandboxAllocation | None:
        """Passively locate one previously bound session without resuming it."""

        validate_sandbox_name(name)
        if not expected_session_id or len(expected_session_id) > 128:
            raise SandboxEffectError(
                SandboxAction.OBSERVE,
                EffectState.NOT_APPLIED,
                "session_identity_invalid",
            )
        box = await self._observe_box(name)
        if box is None:
            return None
        current = box.current_session
        if current is not None and current.id == expected_session_id:
            return VercelSandboxAllocation(self, box, current)
        query_sessions = getattr(self._sdk, "query_sessions", None)
        if not callable(query_sessions):
            raise SandboxEffectError(
                SandboxAction.OBSERVE,
                EffectState.UNKNOWN,
                "exact_session_lookup_unavailable",
            )
        try:
            async with asyncio.timeout(_OBSERVE_TIMEOUT_SECONDS):
                count = 0
                async for runtime in query_sessions(
                    name=name,
                    page_size=_MAX_EXACT_SESSION_LOOKUPS,
                    sort_order="desc",
                ):
                    count += 1
                    if runtime.id == expected_session_id and runtime.sandbox_name == name:
                        return VercelSandboxAllocation(self, box, runtime)
                    if count >= _MAX_EXACT_SESSION_LOOKUPS:
                        break
        except SandboxEffectError as error:
            raise error
        except Exception as error:
            raise _error_effect(SandboxAction.OBSERVE, error) from None
        raise SandboxEffectError(
            SandboxAction.OBSERVE,
            EffectState.UNKNOWN,
            "exact_session_not_observed_within_bound",
        )

    async def _observe_box(self, name: str) -> Sandbox | None:
        try:
            return await self._call(
                SandboxAction.OBSERVE,
                _OBSERVE_TIMEOUT_SECONDS,
                self._sdk.get_sandbox(name=name),
            )
        except SandboxEffectError as error:
            if error.reason == "provider_not_found":
                return None
            raise

    async def _call(self, action: SandboxAction, timeout: int, operation: Awaitable[_T]) -> _T:
        try:
            async with asyncio.timeout(timeout):
                return await operation
        except SandboxEffectError:
            raise
        except Exception as error:
            raise _error_effect(action, error) from None

    async def _destroy_once(self, box: Sandbox) -> None:
        if box.name in self._destroy_attempted:
            raise SandboxEffectError(
                SandboxAction.DESTROY,
                EffectState.NOT_APPLIED,
                "destroy_already_attempted_in_this_process",
            )
        self._destroy_attempted.add(box.name)

        await self._call(
            SandboxAction.DESTROY,
            _DESTROY_TIMEOUT_SECONDS,
            box.destroy(delete_orphan_snapshots=True),
        )
        observed = await self.observe(box.name)
        if observed is not None:
            raise SandboxEffectError(
                SandboxAction.DESTROY,
                EffectState.UNKNOWN,
                "sandbox_still_observed_after_destroy_acknowledgment",
            )


class VercelSandboxAllocation:
    """A named sandbox plus its exact current runtime-session handle."""

    def __init__(
        self,
        provider: VercelSandboxProvider,
        box: Sandbox,
        runtime: SandboxRuntimeSession | None,
    ) -> None:
        self._provider = provider
        self._box = box
        self._runtime = runtime
        self._stop_attempted = False

    @property
    def name(self) -> str:
        return self._box.name

    @property
    def image(self) -> str | None:
        image = getattr(self._box, "image", None)
        return None if image is None else str(image)

    @property
    def network_policy(self) -> NetworkPolicyReadback | None:
        return None if self._runtime is None else self._runtime.network_policy

    @property
    def session_id(self) -> str | None:
        return None if self._runtime is None else self._runtime.id

    @property
    def current_session_id(self) -> str | None:
        current_id = getattr(self._box, "current_session_id", None)
        if current_id is not None:
            return str(current_id)
        current = getattr(self._box, "current_session", None)
        return None if current is None else str(current.id)

    @property
    def current_session_status(self) -> str | None:
        current = getattr(self._box, "current_session", None)
        return None if current is None else _status_text(current.status)

    def observation(self) -> SandboxObservation:
        return SandboxObservation(
            name=self._box.name,
            sandbox_status=_status_text(self._box.status),
            session_id=self.session_id,
            session_status=None if self._runtime is None else _status_text(self._runtime.status),
        )

    async def run(
        self,
        command: str,
        args: Sequence[str],
        *,
        timeout_seconds: int = 10,
        output_limit_bytes: int = MAX_OUTPUT_BYTES,
    ) -> CommandResult:
        """Run one binary directly, with a server kill deadline and streamed byte cap."""

        return await self._run_bounded(
            command,
            args,
            timeout_seconds=timeout_seconds,
            output_limit_bytes=output_limit_bytes,
            max_timeout_seconds=MAX_COMMAND_SECONDS,
            max_output_bytes=MAX_OUTPUT_BYTES,
            cwd=SANDBOX_WORKING_DIRECTORY,
            env=None,
        )

    async def run_profile_command(
        self,
        command: str,
        args: Sequence[str],
        *,
        cwd: str,
        env: Mapping[str, str] | None,
        timeout_seconds: int,
        output_limit_bytes: int = MAX_PROFILE_COMMAND_OUTPUT_BYTES,
    ) -> CommandResult:
        """Run one profile phase with its separate 120-second and log caps."""
        if cwd not in {CONTROL_DIRECTORY, WORKSPACE_PATH}:
            raise SandboxEffectError(
                SandboxAction.COMMAND,
                EffectState.NOT_APPLIED,
                "profile_working_directory_unsupported",
            )
        valid_python_environment = set(env or {}) == {"PYTHONPATH"} and (
            len(env["PYTHONPATH"]) <= 256 if env is not None else True
        )
        node_environment = tuple(sorted((env or {}).items()))
        valid_node_environment = node_environment in {
            tuple(sorted(npm_ci_environment().items())),
            tuple(sorted(npm_test_environment().items())),
        }
        if env is not None and not (valid_python_environment or valid_node_environment):
            raise SandboxEffectError(
                SandboxAction.COMMAND,
                EffectState.NOT_APPLIED,
                "profile_environment_unsupported",
            )
        return await self._run_bounded(
            command,
            args,
            timeout_seconds=timeout_seconds,
            output_limit_bytes=output_limit_bytes,
            max_timeout_seconds=MAX_PROFILE_COMMAND_SECONDS,
            max_output_bytes=MAX_PROFILE_COMMAND_OUTPUT_BYTES,
            cwd=cwd,
            env=env,
        )

    async def _run_bounded(
        self,
        command: str,
        args: Sequence[str],
        *,
        timeout_seconds: int,
        output_limit_bytes: int,
        max_timeout_seconds: int,
        max_output_bytes: int,
        cwd: str,
        env: Mapping[str, str] | None,
    ) -> CommandResult:
        if self._runtime is None:
            raise SandboxEffectError(
                SandboxAction.COMMAND, EffectState.NOT_APPLIED, "no_active_session_handle"
            )
        if (
            self.current_session_id != self.session_id
            or (_status_text(self._runtime.status) or "").casefold() != "running"
        ):
            raise SandboxEffectError(
                SandboxAction.COMMAND, EffectState.UNKNOWN, "profile_session_not_current"
            )
        if not command or len(command) > 128 or any("\x00" in arg for arg in args):
            raise SandboxEffectError(
                SandboxAction.COMMAND, EffectState.NOT_APPLIED, "invalid_command_arguments"
            )
        if len(args) > 32 or any(len(arg) > 4_096 for arg in args):
            raise SandboxEffectError(
                SandboxAction.COMMAND, EffectState.NOT_APPLIED, "invalid_command_arguments"
            )
        if not 1 <= timeout_seconds <= max_timeout_seconds:
            raise SandboxEffectError(
                SandboxAction.COMMAND, EffectState.NOT_APPLIED, "command_timeout_out_of_range"
            )
        if not 1 <= output_limit_bytes <= max_output_bytes:
            raise SandboxEffectError(
                SandboxAction.COMMAND, EffectState.NOT_APPLIED, "output_limit_out_of_range"
            )

        process_options: dict[str, Any] = {
            "cwd": cwd,
            "sudo": False,
            "kill_after": timeout_seconds,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
        }
        if env is not None:
            process_options["env"] = dict(env)
        try:
            async with asyncio.timeout(timeout_seconds + 5):
                process = await self._provider._call(
                    SandboxAction.COMMAND,
                    timeout_seconds + 2,
                    self._runtime.create_process(
                        command,
                        list(args),
                        **process_options,
                    ),
                )
                stdout, stderr, return_code, truncated = await self._collect_process(
                    process, output_limit_bytes, timeout_seconds=timeout_seconds
                )
        except SandboxEffectError:
            raise
        except TimeoutError:
            raise SandboxEffectError(
                SandboxAction.COMMAND,
                EffectState.UNKNOWN,
                "local_command_deadline_elapsed_server_kill_is_bounded",
            ) from None
        except Exception as error:
            raise _error_effect(SandboxAction.COMMAND, error) from None

        return CommandResult(
            exit_code=return_code,
            stdout=stdout,
            stderr=stderr,
            effect=EffectState.UNKNOWN if truncated else EffectState.APPLIED,
        )

    async def _collect_process(
        self,
        process: Any,
        maximum_bytes: int,
        *,
        timeout_seconds: int = MAX_COMMAND_SECONDS,
    ) -> tuple[str, str, int | None, bool]:
        output_lock = asyncio.Lock()
        kill_lock = asyncio.Lock()
        captured_bytes = 0
        kill_attempted = False
        truncated = False
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []

        async def kill_once() -> None:
            nonlocal kill_attempted
            async with kill_lock:
                if kill_attempted:
                    return
                kill_attempted = True
                try:
                    async with asyncio.timeout(_ACTION_TIMEOUT_SECONDS):
                        await process.kill()
                except Exception:
                    # The command's own server kill_after remains the final bound.
                    pass

        async def drain(reader: Any, target: list[str]) -> None:
            nonlocal captured_bytes, truncated
            if reader is None:
                return
            while chunk := await reader.read(256):
                async with output_lock:
                    remaining = maximum_bytes - captured_bytes
                    prefix, byte_count = _utf8_prefix(chunk, remaining)
                    target.append(prefix)
                    captured_bytes += byte_count
                    exceeded = byte_count < len(chunk.encode("utf-8"))
                    if exceeded:
                        truncated = True
                if exceeded:
                    await kill_once()
                    return

        stdout_task = asyncio.create_task(drain(process.stdout, stdout_parts))
        stderr_task = asyncio.create_task(drain(process.stderr, stderr_parts))
        wait_task = asyncio.create_task(process.wait())
        try:
            async with asyncio.timeout(timeout_seconds + 5):
                await asyncio.gather(stdout_task, stderr_task, wait_task)
        except asyncio.CancelledError:
            kill_task = asyncio.create_task(kill_once())
            try:
                async with asyncio.timeout(_ACTION_TIMEOUT_SECONDS):
                    await asyncio.shield(kill_task)
            except (TimeoutError, asyncio.CancelledError):
                # The server kill_after remains the final bound if local cleanup
                # itself is cancelled or its control request stalls.
                pass
            raise
        except TimeoutError:
            await kill_once()
            raise
        except Exception:
            await kill_once()
            raise
        finally:
            for task in (stdout_task, stderr_task, wait_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(stdout_task, stderr_task, wait_task, return_exceptions=True)
        return (
            "".join(stdout_parts),
            "".join(stderr_parts),
            wait_task.result(),
            truncated,
        )

    async def write_file(self, filename: str, content: bytes) -> None:
        """Stream one capped binary fixture through the exact session handle."""

        validate_fixture_filename(filename)
        if len(content) > MAX_TRANSFER_BYTES:
            raise SandboxEffectError(
                SandboxAction.FILE_WRITE, EffectState.NOT_APPLIED, "file_exceeds_transfer_limit"
            )
        if self._runtime is None:
            raise SandboxEffectError(
                SandboxAction.FILE_WRITE, EffectState.NOT_APPLIED, "no_active_session_handle"
            )
        try:
            async with asyncio.timeout(_FILE_TRANSFER_TIMEOUT_SECONDS):
                async with self._runtime.fs.open(
                    filename,
                    "wb",
                    cwd=SANDBOX_WORKING_DIRECTORY,
                    size=len(content),
                    permissions=0o600,
                ) as writer:
                    for offset in range(0, len(content), TRANSFER_CHUNK_BYTES):
                        await writer.write(content[offset : offset + TRANSFER_CHUNK_BYTES])
        except Exception as error:
            raise _error_effect(SandboxAction.FILE_WRITE, error) from None

    async def write_profile_archive(self, content: bytes, *, timeout_seconds: int) -> None:
        """Write a verified source archive into the image's fixed control inbox."""
        if not 1 <= len(content) <= MAX_PROFILE_ARCHIVE_BYTES:
            raise SandboxEffectError(
                SandboxAction.FILE_WRITE,
                EffectState.NOT_APPLIED,
                "profile_archive_size_invalid",
            )
        if not 1 <= timeout_seconds <= MAX_PROFILE_COMMAND_SECONDS:
            raise SandboxEffectError(
                SandboxAction.FILE_WRITE,
                EffectState.NOT_APPLIED,
                "profile_archive_timeout_invalid",
            )
        if self._runtime is None:
            raise SandboxEffectError(
                SandboxAction.FILE_WRITE, EffectState.NOT_APPLIED, "no_active_session_handle"
            )
        if (
            self.current_session_id != self.session_id
            or (_status_text(self._runtime.status) or "").casefold() != "running"
        ):
            raise SandboxEffectError(
                SandboxAction.FILE_WRITE, EffectState.UNKNOWN, "profile_session_not_current"
            )
        try:
            async with asyncio.timeout(timeout_seconds):
                async with self._runtime.fs.open(
                    "source.tar.gz",
                    "wb",
                    cwd=ARCHIVE_INPUT_PATH.rsplit("/", 1)[0],
                    size=len(content),
                    permissions=0o600,
                ) as writer:
                    for offset in range(0, len(content), PROFILE_ARCHIVE_CHUNK_BYTES):
                        await writer.write(content[offset : offset + PROFILE_ARCHIVE_CHUNK_BYTES])
        except SandboxEffectError:
            raise
        except TimeoutError:
            raise SandboxEffectError(
                SandboxAction.FILE_WRITE, EffectState.UNKNOWN, "profile_archive_write_timeout"
            ) from None
        except Exception as error:
            raise _error_effect(SandboxAction.FILE_WRITE, error) from None

    async def set_python_profile_network_policy(
        self, policy: str, *, timeout_seconds: int
    ) -> NetworkPolicyReadback:
        """Change only between the fixed PyPI allowlist and deny-all."""
        return await self._set_profile_network_policy(
            policy,
            timeout_seconds=timeout_seconds,
            allowed_domains={"pypi.org": (), "files.pythonhosted.org": ()},
        )

    async def set_node_profile_network_policy(
        self, policy: str, *, timeout_seconds: int
    ) -> NetworkPolicyReadback:
        """Change only between the fixed npm registry allowlist and deny-all."""
        return await self._set_profile_network_policy(
            policy,
            timeout_seconds=timeout_seconds,
            allowed_domains={"registry.npmjs.org": ()},
        )

    async def _set_profile_network_policy(
        self,
        policy: str,
        *,
        timeout_seconds: int,
        allowed_domains: Mapping[str, Sequence[Any]],
    ) -> NetworkPolicyReadback:
        if policy not in {"package-download", "deny-all"}:
            raise SandboxEffectError(
                SandboxAction.NETWORK_POLICY,
                EffectState.NOT_APPLIED,
                "profile_network_policy_unsupported",
            )
        if not 1 <= timeout_seconds <= MAX_PROFILE_COMMAND_SECONDS:
            raise SandboxEffectError(
                SandboxAction.NETWORK_POLICY,
                EffectState.NOT_APPLIED,
                "profile_network_policy_timeout_invalid",
            )
        if self._runtime is None:
            raise SandboxEffectError(
                SandboxAction.NETWORK_POLICY,
                EffectState.NOT_APPLIED,
                "no_active_session_handle",
            )
        if (
            self.current_session_id != self.session_id
            or (_status_text(self._runtime.status) or "").casefold() != "running"
        ):
            raise SandboxEffectError(
                SandboxAction.NETWORK_POLICY,
                EffectState.UNKNOWN,
                "profile_session_not_current",
            )
        network_policy = NetworkPolicy.custom(allow=allowed_domains)
        if policy == "deny-all":
            network_policy = NetworkPolicy.deny_all()
        try:
            async with asyncio.timeout(timeout_seconds):
                await self._provider._call(
                    SandboxAction.NETWORK_POLICY,
                    timeout_seconds,
                    self._runtime.update_network_policy(network_policy),
                )
                await self._provider._call(
                    SandboxAction.OBSERVE,
                    timeout_seconds,
                    self._runtime.refresh(),
                )
        except TimeoutError:
            raise SandboxEffectError(
                SandboxAction.NETWORK_POLICY,
                EffectState.UNKNOWN,
                "profile_network_policy_transition_unknown",
            ) from None
        except SandboxEffectError:
            raise
        except Exception as error:
            raise _error_effect(SandboxAction.NETWORK_POLICY, error) from None
        readback = self._runtime.network_policy
        if readback is None:
            raise SandboxEffectError(
                SandboxAction.NETWORK_POLICY,
                EffectState.UNKNOWN,
                "profile_network_policy_readback_missing",
            )
        return readback

    async def start_timeout_fixture(self) -> str:
        """Start the harmless server-timed sleep used for the crash proof."""

        if self._runtime is None:
            raise SandboxEffectError(
                SandboxAction.COMMAND, EffectState.NOT_APPLIED, "no_active_session_handle"
            )
        process = await self._provider._call(
            SandboxAction.COMMAND,
            MAX_COMMAND_SECONDS + 2,
            self._runtime.create_process(
                "python",
                ["-c", "import time; time.sleep(3600)"],
                cwd=SANDBOX_WORKING_DIRECTORY,
                sudo=False,
                kill_after=MAX_COMMAND_SECONDS,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ),
        )
        return process.id

    async def observe_process(self, process_id: str) -> ProcessObservation:
        """Inspect a process on the exact current session without resuming it."""

        if self._runtime is None:
            raise SandboxEffectError(
                SandboxAction.OBSERVE, EffectState.UNKNOWN, "no_active_session_handle"
            )
        if _PROCESS_ID.fullmatch(process_id) is None:
            raise ValueError("process id must be a provider-safe identifier")
        process = await self._provider._call(
            SandboxAction.OBSERVE,
            _OBSERVE_TIMEOUT_SECONDS,
            self._runtime.get_process(process_id),
        )
        return ProcessObservation(
            process_id=process.id,
            status=_status_text(process.status),
            exit_code=process.returncode,
        )

    async def read_file(self, filename: str, *, limit: int = MAX_TRANSFER_BYTES) -> bytes:
        """Stream a file into bounded memory, failing before exceeding its cap."""

        validate_fixture_filename(filename)
        if not 1 <= limit <= MAX_TRANSFER_BYTES:
            raise ValueError("file read limit is outside the sandbox transfer cap")
        if self._runtime is None:
            raise SandboxEffectError(
                SandboxAction.OBSERVE, EffectState.UNKNOWN, "no_active_session_handle"
            )
        data = bytearray()
        try:
            async with asyncio.timeout(_FILE_TRANSFER_TIMEOUT_SECONDS):
                async with self._runtime.fs.open(
                    filename, "rb", cwd=SANDBOX_WORKING_DIRECTORY
                ) as reader:
                    while chunk := await reader.read(TRANSFER_CHUNK_BYTES):
                        if len(data) + len(chunk) > limit:
                            raise SandboxEffectError(
                                SandboxAction.OBSERVE,
                                EffectState.UNKNOWN,
                                "file_exceeds_read_limit",
                            )
                        data.extend(chunk)
        except SandboxEffectError:
            raise
        except Exception as error:
            raise _error_effect(SandboxAction.OBSERVE, error) from None
        return bytes(data)

    async def stop(self) -> None:
        """Stop the exact runtime session once and verify it through a read."""

        if self._runtime is None:
            if _is_stopped(self._box.status):
                return
            raise SandboxEffectError(
                SandboxAction.STOP, EffectState.UNKNOWN, "no_session_handle_to_stop"
            )
        if _is_stopped(self._runtime.status):
            return
        if self._stop_attempted:
            raise SandboxEffectError(
                SandboxAction.STOP,
                EffectState.NOT_APPLIED,
                "stop_already_attempted_in_this_process",
            )
        self._stop_attempted = True
        await self._provider._call(
            SandboxAction.STOP,
            _SESSION_STOP_TIMEOUT_SECONDS,
            self._runtime.stop(),
        )
        await self._provider._call(
            SandboxAction.OBSERVE,
            _OBSERVE_TIMEOUT_SECONDS,
            self._runtime.refresh(),
        )
        if not _is_stopped(self._runtime.status):
            raise SandboxEffectError(
                SandboxAction.STOP, EffectState.UNKNOWN, "stopped_state_not_observed"
            )

    async def destroy(self) -> None:
        """Destroy the named sandbox once and verify absence without retrying."""

        await self._provider._destroy_once(self._box)
