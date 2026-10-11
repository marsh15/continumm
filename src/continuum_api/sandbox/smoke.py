"""Explicitly gated T04 hosted lifecycle and server-timeout fixtures."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
from typing import Any

from pydantic import ValidationError

from continuum_api.sandbox.config import SandboxSmokeSettings, validate_sandbox_name
from continuum_api.sandbox.contracts import EffectState, SandboxAction, SandboxEffectError
from continuum_api.sandbox.vercel import VercelSandboxAllocation, VercelSandboxProvider
from continuum_api.settings import safe_settings_error

_FIXTURE = bytes(range(256)) * 4
_CRASH_NAME_PREFIX = "continuum-t04-"
_PROCESS_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


def _emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True))


def _failure(error: SandboxEffectError) -> dict[str, str]:
    return {
        "action": error.action.value,
        "effect": error.effect.value,
        "reason": error.reason,
    }


async def _cleanup(allocation: VercelSandboxAllocation) -> dict[str, str]:
    try:
        await allocation.stop()
    except SandboxEffectError as error:
        return {
            "stop": f"{error.effect.value}:{error.reason}",
            "destroy": "not_attempted",
        }
    try:
        await allocation.destroy()
    except SandboxEffectError as error:
        return {
            "stop": "confirmed",
            "destroy": f"{error.effect.value}:{error.reason}",
        }
    return {"stop": "confirmed", "destroy": "confirmed"}


async def _lifecycle_smoke(
    provider: VercelSandboxProvider, *, name: str
) -> tuple[int, dict[str, Any]]:
    allocation: VercelSandboxAllocation | None = None
    failure: dict[str, str] | None = None
    command_exit_code: int | None = None
    credential_environment_present: bool | None = None
    file_matches: bool | None = None
    try:
        allocation = await provider.create(name)
        credential_probe = (
            "import os; names=('VERCEL_TOKEN','VERCEL_OIDC_TOKEN',"
            "'CONTINUUM_VERCEL_TOKEN','CONTINUUM_DATABASE_URL','DATABASE_URL',"
            "'CONTINUUM_SUPABASE_SECRET_KEY','SUPABASE_SECRET_KEY',"
            "'SUPABASE_SERVICE_ROLE_KEY','CONTINUUM_GITHUB_TOKEN','GITHUB_TOKEN',"
            "'CONTINUUM_OPENAI_API_KEY','OPENAI_API_KEY',"
            "'CONTINUUM_ANTHROPIC_API_KEY','ANTHROPIC_API_KEY',"
            "'CONTINUUM_APP_SIGNING_KEY','APP_SIGNING_KEY',"
            "'CONTINUUM_ENCRYPTION_KEY'); "
            "print('credentials_present=' + str(bool(set(names) & os.environ.keys())))"
        )
        result = await allocation.run("python", ["-c", credential_probe])
        command_exit_code = result.exit_code
        credential_environment_present = "credentials_present=True" in result.stdout
        if result.effect is EffectState.UNKNOWN:
            raise SandboxEffectError(
                SandboxAction.COMMAND, result.effect, "command_output_limit_exceeded"
            )
        if result.exit_code != 0:
            raise SandboxEffectError(
                SandboxAction.COMMAND, result.effect, "harmless_command_failed"
            )
        if credential_environment_present or "credentials_present=False" not in result.stdout:
            raise SandboxEffectError(
                SandboxAction.COMMAND, result.effect, "credential_environment_was_not_empty"
            )

        await allocation.write_file("t04-fixture.bin", _FIXTURE)
        downloaded = await allocation.read_file("t04-fixture.bin")
        file_matches = downloaded == _FIXTURE
        if not file_matches:
            raise SandboxEffectError(
                SandboxAction.OBSERVE, EffectState.UNKNOWN, "file_round_trip_mismatch"
            )
    except SandboxEffectError as error:
        failure = _failure(error)
        if allocation is None and error.action is SandboxAction.CREATE:
            if error.effect is not EffectState.NOT_APPLIED:
                try:
                    allocation = await provider.observe(name)
                except SandboxEffectError:
                    allocation = None
            observed = None if allocation is None else allocation.observation()
            return 2, {
                "result": "create_unresolved",
                "sandbox_name": name,
                "observed_status": None if observed is None else observed.sandbox_status,
                "session_id_suffix": (
                    None
                    if observed is None or observed.session_id is None
                    else observed.session_id[-8:]
                ),
                "session_status": None if observed is None else observed.session_status,
                "cleanup": {"stop": "not_attempted", "destroy": "not_attempted"},
                "failure": failure,
                "next_step": "inspect this exact name; do not reuse the smoke id",
            }
    except Exception:
        failure = {
            "action": "local_harness",
            "effect": "unknown",
            "reason": "local_harness_error",
        }

    cleanup = {"stop": "not_attempted", "destroy": "not_attempted"}
    if allocation is not None:
        cleanup = await _cleanup(allocation)
    passed = failure is None and cleanup == {"stop": "confirmed", "destroy": "confirmed"}
    return (0 if passed else 2), {
        "result": "passed" if passed else "failed_or_unresolved",
        "sandbox_name": name,
        "session_id_suffix": (
            allocation.session_id[-8:]
            if allocation is not None and allocation.session_id is not None
            else None
        ),
        "command_exit_code": command_exit_code,
        "credential_environment_present": credential_environment_present,
        "fixture_byte_count": len(_FIXTURE),
        "fixture_sha256": hashlib.sha256(_FIXTURE).hexdigest(),
        "file_round_trip_matches": file_matches,
        "cleanup": cleanup,
        "failure": failure,
    }


async def _crash_fixture_start(
    provider: VercelSandboxProvider, *, name: str
) -> tuple[int, dict[str, Any]]:
    try:
        allocation = await provider.create(name)
        process_id = await allocation.start_timeout_fixture()
    except SandboxEffectError as error:
        return 2, {
            "result": "create_or_process_unresolved",
            "sandbox_name": name,
            "failure": _failure(error),
            "next_step": "inspect this exact name; do not reuse the smoke id",
        }
    return 0, {
        "result": "fixture_started",
        "sandbox_name": name,
        "session_id_suffix": allocation.session_id[-8:] if allocation.session_id else None,
        "process_id": process_id,
        "local_pid": os.getpid(),
        "server_command_timeout_seconds": 20,
    }


def _validate_fixture_name(name: str) -> str:
    validate_sandbox_name(name)
    if not name.startswith(_CRASH_NAME_PREFIX):
        raise ValueError("operator fixture name must begin with continuum-t04-")
    return name


async def _observe(
    provider: VercelSandboxProvider,
    *,
    name: str,
    process_id: str | None,
) -> tuple[int, dict[str, Any]]:
    try:
        allocation = await provider.observe(_validate_fixture_name(name))
        if allocation is None:
            return 0, {"result": "absent", "sandbox_name": name}
        observation = allocation.observation()
        payload: dict[str, Any] = {
            "result": "observed",
            "sandbox_name": observation.name,
            "sandbox_status": observation.sandbox_status,
            "session_id_suffix": (
                observation.session_id[-8:] if observation.session_id is not None else None
            ),
            "session_status": observation.session_status,
        }
        if process_id is not None:
            if _PROCESS_ID.fullmatch(process_id) is None:
                raise ValueError("process id must be a provider-safe identifier")
            process = await allocation.observe_process(process_id)
            payload["process"] = {
                "status": process.status,
                "exit_code": process.exit_code,
            }
        return 0, payload
    except SandboxEffectError as error:
        return 2, {"result": "unknown", "sandbox_name": name, "failure": _failure(error)}


async def _cleanup_named(
    provider: VercelSandboxProvider, *, name: str
) -> tuple[int, dict[str, Any]]:
    try:
        allocation = await provider.observe(_validate_fixture_name(name))
        if allocation is None:
            return 0, {"result": "already_absent", "sandbox_name": name}
        cleanup = await _cleanup(allocation)
        passed = cleanup == {"stop": "confirmed", "destroy": "confirmed"}
        return (0 if passed else 2), {
            "result": "cleaned" if passed else "unresolved",
            "sandbox_name": name,
            "cleanup": cleanup,
        }
    except SandboxEffectError as error:
        return 2, {"result": "unknown", "sandbox_name": name, "failure": _failure(error)}


async def _run(args: argparse.Namespace) -> int:
    try:
        requires_smoke_id = args.command in {"lifecycle", "crash-fixture"}
        limits, credentials, smoke_name = SandboxSmokeSettings().require_hosted_smoke(
            require_smoke_id=requires_smoke_id
        )
    except ValidationError as error:
        _emit({"result": "blocked", "reason": safe_settings_error(error)})
        return 2
    except ValueError as error:
        _emit({"result": "blocked", "reason": str(error)})
        return 2

    provider = VercelSandboxProvider(limits=limits, credentials=credentials)
    outcome: dict[str, Any] | None = None
    exit_code = 2
    try:
        async with provider.connection():
            if args.command == "lifecycle":
                assert smoke_name is not None
                exit_code, outcome = await _lifecycle_smoke(provider, name=smoke_name)
            elif args.command == "crash-fixture":
                assert smoke_name is not None
                exit_code, outcome = await _crash_fixture_start(provider, name=smoke_name)
                _emit(outcome)
                if exit_code == 0:
                    await asyncio.Event().wait()
                return exit_code
            elif args.command == "observe":
                exit_code, outcome = await _observe(
                    provider, name=args.name, process_id=args.process_id
                )
            elif args.command == "cleanup":
                exit_code, outcome = await _cleanup_named(provider, name=args.name)
    except Exception:
        if outcome is None:
            outcome = {"result": "failed_or_unresolved", "reason": "sdk_context_error"}
        else:
            outcome["sdk_context_teardown"] = "unknown"
            if outcome.get("result") == "passed":
                outcome["result"] = "failed_or_unresolved"
            exit_code = 2
    if outcome is not None:
        _emit(outcome)
    return exit_code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("lifecycle", help="run one bounded create/command/file/stop/destroy smoke")
    commands.add_parser(
        "crash-fixture", help="start a harmless server-timed process for manual crash proof"
    )
    observe = commands.add_parser("observe", help="passively inspect one named T04 fixture")
    observe.add_argument("--name", required=True)
    observe.add_argument("--process-id")
    cleanup = commands.add_parser("cleanup", help="stop and destroy one named T04 fixture")
    cleanup.add_argument("--name", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        _emit({"result": "interrupted", "effect": "unknown"})
        return 2
    except Exception:
        _emit({"result": "failed_or_unresolved", "reason": "local_harness_error"})
        return 2


if __name__ == "__main__":
    sys.exit(main())
