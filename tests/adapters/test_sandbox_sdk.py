"""Contract tests for the Vercel adapter using a local SDK-shaped fake."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import httpx2
import pytest
from vercel.sandbox import Sandbox, SandboxApiError, SandboxCredentials, SandboxStatus
from vercel.sandbox._internal.state import (
    RuntimeSessionStopState,
    SandboxRuntimeSessionState,
    SandboxState,
)

from continuum_api.sandbox.config import (
    SandboxAllocationLimits,
    SandboxLimits,
    VercelCredentials,
)
from continuum_api.sandbox.contracts import EffectState, SandboxEffectError
from continuum_api.sandbox.vercel import VercelSandboxProvider, _is_stopped


class FakeReader:
    def __init__(self, content: str) -> None:
        self._content = content

    async def read(self, size: int = -1) -> str:
        if not self._content:
            return ""
        if size < 0:
            result, self._content = self._content, ""
            return result
        result, self._content = self._content[:size], self._content[size:]
        return result


class FakeWriter:
    def __init__(self, filesystem: FakeFilesystem, filename: str) -> None:
        self._filesystem = filesystem
        self._filename = filename
        self._content = bytearray()

    async def write(self, content: bytes) -> None:
        self._filesystem.write_chunk_sizes.append(len(content))
        self._content.extend(content)

    async def __aenter__(self) -> FakeWriter:
        return self

    async def __aexit__(self, *_: object) -> None:
        self._filesystem.files[self._filename] = bytes(self._content)


class FakeBinaryReader:
    def __init__(self, content: bytes) -> None:
        self._content = content

    async def read(self, size: int = -1) -> bytes:
        if not self._content:
            return b""
        if size < 0:
            result, self._content = self._content, b""
            return result
        result, self._content = self._content[:size], self._content[size:]
        return result

    async def __aenter__(self) -> FakeBinaryReader:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None


class FakeFilesystem:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.write_chunk_sizes: list[int] = []

    def open(self, filename: str, mode: str, **_: object) -> FakeWriter | FakeBinaryReader:
        if mode == "wb":
            return FakeWriter(self, filename)
        return FakeBinaryReader(self.files[filename])


class FakeProcess:
    def __init__(self, stdout: str = "hello\n", stderr: str = "") -> None:
        self.stdout = FakeReader(stdout)
        self.stderr = FakeReader(stderr)
        self.returncode = 0
        self.kill_calls = 0

    async def wait(self) -> int:
        return self.returncode

    async def kill(self) -> None:
        self.kill_calls += 1


class FakeRuntimeSession:
    def __init__(self, *, stdout: str = "hello\n", stderr: str = "") -> None:
        self.id = "session-fixture"
        self.sandbox_name = ""
        self.status = SandboxStatus.RUNNING
        self.fs = FakeFilesystem()
        self.process = FakeProcess(stdout, stderr)
        self.create_process_calls: list[tuple[str, Sequence[str], dict[str, Any]]] = []
        self.stop_calls = 0
        self.stop_error: Exception | None = None

    async def create_process(self, command: str, args: Sequence[str], **kwargs: Any) -> FakeProcess:
        self.create_process_calls.append((command, args, kwargs))
        return self.process

    async def stop(self) -> None:
        self.stop_calls += 1
        if self.stop_error is not None:
            raise self.stop_error
        self.status = SandboxStatus.STOPPED

    async def refresh(self) -> None:
        return None


class FakeSandbox:
    def __init__(self, name: str, runtime: FakeRuntimeSession) -> None:
        self.name = name
        self.current_session = runtime
        runtime.sandbox_name = name
        self.status = SandboxStatus.RUNNING
        self.destroy_calls = 0
        self.destroy_error: Exception | None = None

    @property
    def current_session_id(self) -> str | None:
        return None if self.current_session is None else self.current_session.id

    async def destroy(self, *, delete_orphan_snapshots: bool = False) -> None:
        assert delete_orphan_snapshots is True
        self.destroy_calls += 1
        if self.destroy_error is not None:
            raise self.destroy_error


class FakeSDK:
    def __init__(self, runtime: FakeRuntimeSession) -> None:
        self.runtime = runtime
        self.sessions = [runtime]
        self.create_calls: list[dict[str, Any]] = []
        self.box: FakeSandbox | None = None

    async def create_sandbox(self, **kwargs: Any) -> FakeSandbox:
        self.create_calls.append(kwargs)
        self.box = FakeSandbox(kwargs["name"], self.runtime)
        return self.box

    async def query_sessions(
        self, *, name: str, page_size: int, sort_order: str
    ) -> AsyncIterator[FakeRuntimeSession]:
        assert page_size == 20
        assert sort_order == "desc"
        if self.box is not None and self.box.name == name:
            for runtime in self.sessions:
                if runtime.sandbox_name == name:
                    yield runtime

    async def get_sandbox(self, *, name: str) -> FakeSandbox:
        if self.box is None or self.box.name != name or self.box.destroy_calls:
            request = httpx2.Request("GET", "https://vercel.example/sandbox")
            response = httpx2.Response(404, request=request)
            raise SandboxApiError(response, "hidden test fixture response")
        return self.box


def make_provider(
    sdk: FakeSDK,
    seen_options: list[Sequence[object]],
    limits: SandboxLimits | SandboxAllocationLimits | None = None,
) -> VercelSandboxProvider:
    @asynccontextmanager
    async def fake_session(*, service_options: Sequence[object]):
        seen_options.append(service_options)
        option = service_options[0]
        credentials = await option.credentials_factory()
        assert isinstance(credentials, SandboxCredentials)
        assert credentials.token == "control-token"
        yield object()

    return VercelSandboxProvider(
        limits=limits or SandboxLimits(image=f"vercel/sandbox/python:3.14@sha256:{'c' * 64}"),
        credentials=VercelCredentials(
            token="control-token", team_id="team-test", project_id="project-test"
        ),
        sdk=sdk,
        sdk_session_factory=fake_session,
    )


def test_adapter_lifecycle_uses_pinned_bounded_session_without_injecting_credentials() -> None:
    async def scenario() -> None:
        runtime = FakeRuntimeSession()
        sdk = FakeSDK(runtime)
        seen_options: list[Sequence[object]] = []
        provider = make_provider(sdk, seen_options)

        async with provider.connection():
            allocation = await provider.create("continuum-t04-fixture001")
            request = sdk.create_calls[0]
            assert request["image"].endswith(f"@sha256:{'c' * 64}")
            assert request["execution_time_limit"] == timedelta(seconds=120)
            assert request["resources"].vcpus == 1
            assert request["resources"].memory == 2_048
            assert request["persistent"] is False
            assert request["network_policy"].mode == "deny-all"
            assert request["ports"] == []
            assert request["mounts"] is None
            assert request["env"] is None
            assert request["source"] is None
            assert "control-token" not in repr(request)

            result = await allocation.run("python3", ["-c", "print('hello')"])
            assert result.exit_code == 0
            assert result.stdout == "hello\n"
            assert result.effect is EffectState.APPLIED
            command, args, command_options = runtime.create_process_calls[0]
            assert (command, list(args)) == ("python3", ["-c", "print('hello')"])
            assert command_options["kill_after"] == 10
            assert command_options["cwd"] == "/vercel/sandbox"
            assert "env" not in command_options

            fixture = bytes(range(200))
            await allocation.write_file("fixture.bin", fixture)
            assert await allocation.read_file("fixture.bin") == fixture
            assert len(runtime.fs.write_chunk_sizes) > 1
            assert max(runtime.fs.write_chunk_sizes) <= 64

            await allocation.stop()
            await allocation.destroy()

        assert len(seen_options) == 1
        assert runtime.stop_calls == 1
        assert sdk.box is not None and sdk.box.destroy_calls == 1
        assert runtime.process.kill_calls == 0

    asyncio.run(scenario())


def test_t16_provider_uses_reserved_runtime_envelope_and_correlation_tag() -> None:
    async def scenario() -> None:
        runtime = FakeRuntimeSession()
        sdk = FakeSDK(runtime)
        provider = make_provider(
            sdk,
            [],
            SandboxAllocationLimits(
                image=f"vercel/sandbox/python:3.14@sha256:{'e' * 64}",
                session_seconds=900,
            ),
        )

        async with provider.connection():
            await provider.create(
                "continuum-t16-allocationfixture001",
                tags={"continuum-allocation-id": "allocation-fixture"},
            )

        request = sdk.create_calls[0]
        assert request["execution_time_limit"] == timedelta(seconds=900)
        assert request["resources"].vcpus == 2
        assert request["resources"].memory == 4_096
        assert request["persistent"] is False
        assert request["ports"] == []
        assert request["env"] is None
        assert request["tags"] == {"continuum-allocation-id": "allocation-fixture"}

    asyncio.run(scenario())


def test_exact_session_lookup_is_passive_and_does_not_adopt_current_session() -> None:
    async def scenario() -> None:
        current = FakeRuntimeSession()
        historical = FakeRuntimeSession()
        historical.id = "persisted-session"
        historical.status = SandboxStatus.STOPPED
        sdk = FakeSDK(current)
        box = FakeSandbox("continuum-t16-exactsession0001", current)
        historical.sandbox_name = box.name
        sdk.box = box
        sdk.sessions.append(historical)
        provider = make_provider(sdk, [])

        async with provider.connection():
            observed = await provider.observe_exact_session(box.name, historical.id)

        assert observed is not None
        assert observed.session_id == "persisted-session"
        assert observed.current_session_id == current.id
        assert observed.observation().session_status == "stopped"
        assert current.create_process_calls == []
        assert historical.create_process_calls == []

    asyncio.run(scenario())


def test_output_overflow_kills_once_and_marks_command_effect_unknown() -> None:
    async def scenario() -> None:
        runtime = FakeRuntimeSession(stdout="abc🙂def", stderr="")
        sdk = FakeSDK(runtime)
        provider = make_provider(sdk, [])

        async with provider.connection():
            allocation = await provider.create("continuum-t04-overflow001")
            result = await allocation.run("python3", ["-c", "print('large')"], output_limit_bytes=7)

        assert result.stdout == "abc🙂"
        assert len(result.stdout.encode("utf-8")) == 7
        assert result.effect is EffectState.UNKNOWN
        assert runtime.process.kill_calls == 1

    asyncio.run(scenario())


def test_unknown_create_is_not_repeated_in_the_same_provider() -> None:
    class InterruptedSDK(FakeSDK):
        async def create_sandbox(self, **kwargs: Any) -> FakeSandbox:
            self.create_calls.append(kwargs)
            raise TimeoutError("simulated lost create response")

    async def scenario() -> None:
        sdk = InterruptedSDK(FakeRuntimeSession())
        provider = make_provider(sdk, [])
        async with provider.connection():
            with pytest.raises(SandboxEffectError) as first:
                await provider.create("continuum-t04-unknown001")
            with pytest.raises(SandboxEffectError) as second:
                await provider.create("continuum-t04-unknown001")

        assert first.value.effect is EffectState.UNKNOWN
        assert second.value.effect is EffectState.NOT_APPLIED
        assert len(sdk.create_calls) == 1

    asyncio.run(scenario())


def test_observe_after_destroy_is_read_only_not_found() -> None:
    async def scenario() -> None:
        sdk = FakeSDK(FakeRuntimeSession())
        provider = make_provider(sdk, [])
        async with provider.connection():
            allocation = await provider.create("continuum-t04-observe001")
            await allocation.stop()
            await allocation.destroy()
            assert await provider.observe(allocation.name) is None

    asyncio.run(scenario())


def test_unknown_stop_and_destroy_effects_do_not_repeat_mutations() -> None:
    async def scenario() -> None:
        runtime = FakeRuntimeSession()
        sdk = FakeSDK(runtime)
        provider = make_provider(sdk, [])
        async with provider.connection():
            allocation = await provider.create("continuum-t04-no-retry0001")
            runtime.stop_error = TimeoutError("lost stop response")
            with pytest.raises(SandboxEffectError) as first_stop:
                await allocation.stop()
            with pytest.raises(SandboxEffectError) as second_stop:
                await allocation.stop()
            assert first_stop.value.effect is EffectState.UNKNOWN
            assert second_stop.value.effect is EffectState.NOT_APPLIED
            assert runtime.stop_calls == 1

            assert sdk.box is not None
            sdk.box.destroy_error = TimeoutError("lost destroy response")
            with pytest.raises(SandboxEffectError) as first_destroy:
                await allocation.destroy()
            with pytest.raises(SandboxEffectError) as second_destroy:
                await allocation.destroy()
            assert first_destroy.value.effect is EffectState.UNKNOWN
            assert second_destroy.value.effect is EffectState.NOT_APPLIED
            assert sdk.box.destroy_calls == 1

    asyncio.run(scenario())


def test_pinned_sdk_exact_session_handle_does_not_resume_or_replay() -> None:
    class ExactSessionService:
        def __init__(self) -> None:
            self.process_calls = 0
            self.stop_calls = 0
            self.resume_calls = 0

        async def run_process(self, **_: object) -> object:
            self.process_calls += 1
            request = httpx2.Request("POST", "https://vercel.example/process")
            raise SandboxApiError(
                httpx2.Response(410, request=request),
                "stopped session fixture",
                data={"error": {"code": "sandbox_stopped"}},
            )

        async def stop_runtime_session(self, *, session_id: str) -> RuntimeSessionStopState:
            self.stop_calls += 1
            return RuntimeSessionStopState(
                session=SandboxRuntimeSessionState(
                    id=session_id,
                    sandbox_name="continuum-t04-real-sdk0001",
                    status=SandboxStatus.STOPPED,
                )
            )

        async def resume_sandbox(self, **_: object) -> SandboxState:
            self.resume_calls += 1
            raise AssertionError("exact-session operations must not resume")

    async def scenario() -> None:
        service = ExactSessionService()
        runtime_state = SandboxRuntimeSessionState(
            id="session-exact",
            sandbox_name="continuum-t04-real-sdk0001",
            status=SandboxStatus.RUNNING,
        )
        box = Sandbox(
            payload=SandboxState(
                name="continuum-t04-real-sdk0001",
                current_session_id=runtime_state.id,
                current_session=runtime_state,
                status=SandboxStatus.RUNNING,
            ),
            service=service,
        )

        exact_session = box.current_session
        assert exact_session is not None
        assert exact_session.status is SandboxStatus.RUNNING
        with pytest.raises(SandboxApiError):
            await exact_session.run_process("python", ["-c", "pass"])
        assert service.process_calls == 1
        assert service.resume_calls == 0

        await exact_session.stop()
        assert service.stop_calls == 1
        assert exact_session.status is SandboxStatus.STOPPED
        assert _is_stopped(SandboxStatus.STOPPED)
        assert not _is_stopped(SandboxStatus.RUNNING)

    asyncio.run(scenario())
