"""Sandbox contract and limit checks without provider access."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

from continuum_api.sandbox.config import (
    ALLOCATION_MEMORY_MB,
    ALLOCATION_VCPUS,
    MAX_ALLOCATION_SECONDS,
    MAX_SESSION_SECONDS,
    SandboxAllocationLimits,
    SandboxLimits,
    SandboxSmokeSettings,
    VercelCredentials,
    validate_fixture_filename,
    validate_sandbox_name,
)
from continuum_api.sandbox.contracts import EffectState, SandboxAction, SandboxEffectError
from continuum_api.sandbox.smoke import _crash_fixture_start
from continuum_api.sandbox.vercel import _utf8_prefix


def test_sandbox_limits_require_digest_image_and_fixed_resources() -> None:
    digest = "a" * 64
    assert SandboxLimits(image=f"vercel/sandbox/python:3.14@sha256:{digest}").vcpus == 1

    with pytest.raises(ValueError, match="immutable sha256 digest"):
        SandboxLimits(image="vercel/sandbox/python:3.14")
    with pytest.raises(ValueError, match="fixed at 1 vCPU"):
        SandboxLimits(image=f"vercel/sandbox/python:3.14@sha256:{digest}", vcpus=2)
    with pytest.raises(ValueError, match="session_seconds"):
        SandboxLimits(
            image=f"vercel/sandbox/python:3.14@sha256:{digest}",
            session_seconds=MAX_SESSION_SECONDS + 1,
        )


def test_t16_runtime_envelope_is_separate_from_fixed_t04_limits() -> None:
    image = f"vercel/sandbox/python:3.14@sha256:{'e' * 64}"
    limits = SandboxAllocationLimits(image=image, session_seconds=MAX_ALLOCATION_SECONDS)

    assert (limits.vcpus, limits.memory_mb, limits.session_seconds) == (
        ALLOCATION_VCPUS,
        ALLOCATION_MEMORY_MB,
        900,
    )
    assert SandboxLimits(image=image).session_seconds == 120
    with pytest.raises(ValueError, match="session_seconds"):
        SandboxAllocationLimits(image=image, session_seconds=MAX_ALLOCATION_SECONDS + 1)
    with pytest.raises(ValueError, match="2 vCPUs and 4096 MiB"):
        SandboxAllocationLimits(image=image, session_seconds=120, vcpus=1)


def test_sandbox_names_and_fixture_paths_cannot_escape_scope() -> None:
    assert validate_sandbox_name("continuum-t04-abc123") == "continuum-t04-abc123"
    assert validate_fixture_filename("fixture.bin") == "fixture.bin"

    for value in ("../secret", "/absolute", "nested/file", "..", ""):
        with pytest.raises(ValueError):
            validate_fixture_filename(value)
    with pytest.raises(ValueError):
        validate_sandbox_name("User supplied name")


def test_hosted_smoke_requires_explicit_opt_in_complete_credentials_and_digest() -> None:
    with pytest.raises(ValueError, match="ENABLE_SANDBOX_SMOKE=1"):
        SandboxSmokeSettings().require_hosted_smoke()

    configured = SandboxSmokeSettings(
        enable_sandbox_smoke=True,
        sandbox_image=f"vercel/sandbox/python:3.14@sha256:{'b' * 64}",
        t04_smoke_id="123e4567-e89b-12d3-a456-426614174000",
        vercel_token="control-plane-secret",
        vercel_team_id="team-fixture",
        vercel_project_id="project-fixture",
    )
    _, credentials, name = configured.require_hosted_smoke()
    assert credentials.token == "control-plane-secret"
    assert "control-plane-secret" not in repr(credentials)
    assert name == "continuum-t04-123e4567e89b12d3a456426614174000"


def test_create_smoke_requires_a_stable_canonical_run_id() -> None:
    config = {
        "enable_sandbox_smoke": True,
        "sandbox_image": f"vercel/sandbox/python:3.14@sha256:{'d' * 64}",
        "vercel_token": "control-plane-secret",
        "vercel_team_id": "team-fixture",
        "vercel_project_id": "project-fixture",
    }
    with pytest.raises(ValueError, match="stable UUID"):
        SandboxSmokeSettings(**config).require_hosted_smoke()
    with pytest.raises(ValueError, match="canonical UUID"):
        SandboxSmokeSettings(**config, t04_smoke_id="task-17").require_hosted_smoke()
    _, _, no_name = SandboxSmokeSettings(**config).require_hosted_smoke(require_smoke_id=False)
    assert no_name is None


def test_hosted_smoke_does_not_invent_an_image_digest() -> None:
    configured = SandboxSmokeSettings(
        enable_sandbox_smoke=True,
        sandbox_image="vercel/sandbox/python:3.14",
        vercel_token="control-plane-secret",
        vercel_team_id="team-fixture",
        vercel_project_id="project-fixture",
    )
    with pytest.raises(ValueError, match="immutable sha256 digest"):
        configured.require_hosted_smoke()


def test_credentials_repr_never_contains_control_plane_token() -> None:
    assert "private-token" not in repr(
        VercelCredentials(token="private-token", team_id="team", project_id="project")
    )


def test_sandbox_effect_error_survives_context_manager_propagation() -> None:
    @contextmanager
    def scope():
        yield

    error = SandboxEffectError(
        SandboxAction.CREATE,
        EffectState.UNKNOWN,
        "provider_call_interrupted",
    )
    with pytest.raises(SandboxEffectError) as captured:
        with scope():
            raise error
    assert captured.value is error
    assert captured.value.effect is EffectState.UNKNOWN


def test_command_output_cap_counts_utf8_bytes_without_splitting_characters() -> None:
    prefix, size = _utf8_prefix("a🙂b", 5)
    assert prefix == "a🙂"
    assert size == 5


def test_hosted_smoke_cli_refuses_by_default_without_provider_calls() -> None:
    env = os.environ.copy()
    env["CONTINUUM_ENABLE_SANDBOX_SMOKE"] = "0"
    env["CONTINUUM_VERCEL_TOKEN"] = "secret-sentinel"
    completed = subprocess.run(
        [sys.executable, "-m", "continuum_api.sandbox.smoke", "lifecycle"],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 2
    assert "set CONTINUUM_ENABLE_SANDBOX_SMOKE=1" in completed.stdout
    assert "secret-sentinel" not in completed.stdout + completed.stderr
    assert "Traceback" not in completed.stderr


def test_hosted_smoke_cli_sanitizes_malformed_secret_bearing_environment() -> None:
    env = os.environ.copy()
    env["CONTINUUM_ENABLE_SANDBOX_SMOKE"] = "invalid-boolean-secret-sentinel"
    env["CONTINUUM_VERCEL_TOKEN"] = "control-token-sentinel"
    completed = subprocess.run(
        [sys.executable, "-m", "continuum_api.sandbox.smoke", "lifecycle"],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 2
    assert "blocked" in completed.stdout
    assert "secret-sentinel" not in completed.stdout + completed.stderr
    assert "control-token-sentinel" not in completed.stdout + completed.stderr
    assert "invalid-boolean-secret-sentinel" not in completed.stdout + completed.stderr
    assert "Traceback" not in completed.stderr


@pytest.mark.parametrize(
    "arguments",
    [
        ["lifecycle"],
        ["crash-fixture"],
        ["cleanup", "--name", "continuum-t04-123e4567e89b12d3a456426614174000"],
    ],
)
def test_t04_hosted_mutations_are_blocked_until_allocation_accounting(
    arguments: list[str],
) -> None:
    env = os.environ.copy()
    env.update(
        {
            "CONTINUUM_ENABLE_SANDBOX_SMOKE": "1",
            "CONTINUUM_SANDBOX_IMAGE": f"vercel/sandbox/python:3.14@sha256:{'f' * 64}",
            "CONTINUUM_T04_SMOKE_ID": "123e4567-e89b-12d3-a456-426614174000",
            "CONTINUUM_VERCEL_TOKEN": "non-secret-test-token",
            "CONTINUUM_VERCEL_TEAM_ID": "team-fixture",
            "CONTINUUM_VERCEL_PROJECT_ID": "project-fixture",
        }
    )
    completed = subprocess.run(
        [sys.executable, "-m", "continuum_api.sandbox.smoke", *arguments],
        cwd=Path(__file__).resolve().parents[2],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2
    assert "sandbox_allocation_accounting_required" in completed.stdout
    assert "non-secret-test-token" not in completed.stdout + completed.stderr
    assert "Traceback" not in completed.stderr


def test_crash_fixture_report_includes_exact_local_pid_and_provider_identifiers() -> None:
    class FakeAllocation:
        session_id = "sandbox-session-fixture-12345678"

        async def start_timeout_fixture(self) -> str:
            return "process-fixture-123"

    class FakeProvider:
        async def create(self, name: str) -> FakeAllocation:
            assert name == "continuum-t04-123e4567e89b12d3a456426614174000"
            return FakeAllocation()

    code, payload = asyncio.run(
        _crash_fixture_start(FakeProvider(), name="continuum-t04-123e4567e89b12d3a456426614174000")
    )
    assert code == 0
    assert payload["local_pid"] == os.getpid()
    assert payload["process_id"] == "process-fixture-123"
