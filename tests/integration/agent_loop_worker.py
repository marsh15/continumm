"""Subprocess worker used by the T22 PostgreSQL recovery tests."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import create_engine, text

from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.connectivity.storage import SupabaseStorage
from continuum_api.repositories.preflight import (
    GitHubRepositoryAdapter,
    RepositoryAccessProof,
    RepositoryMetadata,
)
from continuum_api.runtime import agent_loop as agent_loop_module
from continuum_api.runtime.agent_loop import (
    AgentLoopError,
    DurableAgentLoopService,
    ScriptedModelResponse,
    ScriptedResponseQueue,
)
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.runtime.scheduler import RuntimeRecoveryService
from continuum_api.runtime.transitions import TransitionService
from continuum_api.runtime.workspace_recovery import WorkspaceRecoveryService
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import ToolPolicyContext

STORAGE_URL = "https://fixture-project.supabase.co"
STORAGE_SECRET = "sb_secret_t22_scripted_fixture_only"
STORAGE_BUCKET = "continuum-checkpoints"


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True), flush=True)


def _barrier(point: str) -> None:
    _emit({"kind": "barrier", "point": point})
    sys.stdin.readline()


def _append_log(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    with os.fdopen(descriptor, "ab") as output:
        output.write(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        output.flush()
        os.fsync(output.fileno())


def _private_object_path(root: Path, key: str) -> Path:
    target = (root / key).resolve()
    if not target.is_relative_to(root.resolve()):
        raise RuntimeError("storage_fixture_path_invalid")
    return target


def _storage(root: Path) -> SupabaseStorage:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith(f"/bucket/{STORAGE_BUCKET}"):
            return httpx.Response(200, json={"id": STORAGE_BUCKET, "public": False})
        upload_prefix = f"/storage/v1/object/{STORAGE_BUCKET}/"
        if request.method == "POST" and request.url.path.startswith(upload_prefix):
            key = request.url.path.removeprefix(upload_prefix)
            target = _private_object_path(root, key)
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            descriptor = os.open(
                target,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            with os.fdopen(descriptor, "wb") as stored:
                stored.write(request.read())
                stored.flush()
                os.fsync(stored.fileno())
            return httpx.Response(200, json={"Key": key})
        download_prefix = f"/storage/v1/object/authenticated/{STORAGE_BUCKET}/"
        if request.method == "GET" and request.url.path.startswith(download_prefix):
            key = request.url.path.removeprefix(download_prefix)
            try:
                content = _private_object_path(root, key).read_bytes()
            except FileNotFoundError:
                return httpx.Response(404)
            return httpx.Response(200, content=content)
        raise RuntimeError("storage_fixture_request_invalid")

    return SupabaseStorage(
        STORAGE_URL,
        STORAGE_SECRET,
        STORAGE_BUCKET,
        transport=httpx.MockTransport(handler),
        fixture_byte_limit=10 * 1024 * 1024,
        request_deadline_seconds=5.0,
    )


class _AccessVerifier:
    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
        return RepositoryAccessProof(repository_id, github_user_id, True, True)


class _PublicRepositoryAdapter(GitHubRepositoryAdapter):
    async def current_public_repository(self, repository_id: int) -> RepositoryMetadata:
        return RepositoryMetadata(repository_id, "fixture-owner", "fixture-project", 1)

    async def aclose(self) -> None:
        return


class _RecordingAdapter:
    def __init__(
        self,
        queue: ScriptedResponseQueue,
        call_log: Path,
        *,
        pause_before_return: bool,
    ) -> None:
        self._queue = queue
        self._call_log = call_log
        self._pause_before_return = pause_before_return

    async def complete(self, request: object) -> ScriptedModelResponse:
        _append_log(
            self._call_log,
            {"request_sha256": request.request_sha256, "turn_id": str(request.turn_id)},
        )
        if self._pause_before_return:
            _barrier("provider_response_ready")
        return await self._queue.complete(request)


def _policy_context(payload: dict[str, object]) -> ToolPolicyContext:
    return ToolPolicyContext(
        capabilities=frozenset(ToolCapability(item) for item in payload["capabilities"]),
        profile_id=payload["profile_id"],
        profile_version=payload["profile_version"],
        actor_can_write_repository=payload["actor_can_write_repository"],
        app_can_access_repository=payload["app_can_access_repository"],
        stage=ToolStage(payload["stage"]),
        run_budget_available=payload["run_budget_available"],
        baseline_tree_sha256=payload["baseline_tree_sha256"],
        baseline_source_paths=frozenset(payload["baseline_source_paths"]),
        baseline_test_paths=frozenset(payload["baseline_test_paths"]),
        baseline_paths_verified=payload["baseline_paths_verified"],
        protected_paths=frozenset(payload["protected_paths"]),
    )


async def _run_turn(
    *,
    service: DurableAgentLoopService,
    queue: QueueService,
    response: ScriptedModelResponse,
    logical_key: str,
    baseline_artifact_id: UUID,
    model_log: Path,
    fault: str | None,
) -> None:
    lease = queue.claim_one("t22-agent-turn-worker")
    if lease is None:
        raise AgentLoopError("agent_turn_lease_missing")
    _emit(_lease_payload(lease))
    intent = await service.prepare_turn(
        lease,
        logical_key,
        context={"task": "read the answer, patch it, then read it again"},
        tool_schemas=_tool_schemas(),
        baseline_artifact_id=baseline_artifact_id,
    )
    adapter = _RecordingAdapter(
        ScriptedResponseQueue((response,)),
        model_log,
        pause_before_return=fault == "provider_response_ready",
    )
    receipt = await service.infer_or_reuse(lease, intent.turn_id, adapter)
    _emit(
        {
            "kind": "response_saved",
            "turn_id": str(receipt.turn_id),
            "adopted": receipt.adopted,
            "request_sha256": receipt.request_sha256,
            "response_sha256": receipt.response_sha256,
            "calls": [
                {
                    "invocation_id": str(call.invocation_id),
                    "ordinal": call.ordinal,
                    "provider_call_id": call.provider_call_id,
                    "logical_key": call.logical_key,
                    "accepted": call.accepted,
                }
                for call in receipt.invocations
            ],
        }
    )
    if fault == "after_response_saved":
        _barrier("after_response_saved")
    transition = await service.complete_turn(lease, receipt.turn_id)
    _emit(
        {
            "kind": "turn_completed",
            "turn_id": str(receipt.turn_id),
            "result_id": str(transition.result_id),
            "next_job_id": None if transition.next_job_id is None else str(transition.next_job_id),
        }
    )


async def _run_tools(
    *,
    engine: object,
    principal: Principal,
    service: DurableAgentLoopService,
    queue: QueueService,
    turn_id: UUID,
    policy_context: ToolPolicyContext,
    tool_log: Path,
    limit: int,
    fault: str | None,
) -> None:
    for _ in range(limit):
        lease = queue.claim_one("t22-agent-tool-worker")
        if lease is None:
            raise AgentLoopError("agent_tool_lease_missing")
        _emit(_lease_payload(lease))
        invocations = await service.pending_invocations(lease, turn_id)
        with engine.begin() as connection:
            RuntimeRepository(connection, principal)
            current_key = connection.execute(
                text(
                    "SELECT logical_key FROM runtime_private.steps "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
                ),
                {
                    "owner_id": lease.owner_id,
                    "run_id": lease.run_id,
                    "step_id": lease.step_id,
                },
            ).scalar_one()
        selected = next((item for item in invocations if item.logical_key == current_key), None)
        if selected is None:
            raise AgentLoopError("agent_invocation_not_current")
        original_execute = agent_loop_module.execute_tool_call
        trace_record = {
            "ordinal": selected.ordinal,
            "provider_call_id": selected.provider_call_id,
            "generation": lease.generation,
            "invocation_id": str(selected.invocation_id),
            "tool_name": selected.name,
        }
        pause_before_accept = fault == "before_tool_result_accept"

        def traced_execute(
            *args: Any,
            _trace_record: dict[str, object] = trace_record,
            _original_execute: Any = original_execute,
            _pause_before_accept: bool = pause_before_accept,
            **kwargs: Any,
        ) -> Any:
            _append_log(tool_log, _trace_record)
            execution = _original_execute(*args, **kwargs)
            if _pause_before_accept:
                _barrier("before_tool_result_accept")
            return execution

        agent_loop_module.execute_tool_call = traced_execute
        try:
            receipt = await service.execute_tool_invocation(
                lease,
                turn_id,
                selected.invocation_id,
                policy_context=policy_context,
            )
        finally:
            agent_loop_module.execute_tool_call = original_execute
        _emit(
            {
                "kind": "tool_accepted",
                "turn_id": str(receipt.turn_id),
                "invocation_id": str(receipt.invocation_id),
                "ordinal": receipt.ordinal,
                "artifact_id": None if receipt.artifact_id is None else str(receipt.artifact_id),
                "result_id": str(receipt.transition.result_id),
                "adopted": receipt.adopted,
                "result": receipt.transition.result,
            }
        )
        if fault == "after_tool_accepted" and selected.ordinal == 0:
            _barrier("after_tool_accepted")


def _lease_payload(lease: object) -> dict[str, object]:
    return {
        "kind": "lease_claimed",
        "owner_id": str(lease.owner_id),
        "run_id": str(lease.run_id),
        "step_id": str(lease.step_id),
        "job_id": str(lease.job_id),
        "attempt_id": str(lease.attempt_id),
        "generation": lease.generation,
        "lease_owner": lease.lease_owner,
        "lease_expires_at": lease.lease_expires_at.isoformat(),
    }


def _tool_schemas() -> tuple[dict[str, object], ...]:
    return (
        {
            "name": "read_file",
            "parameters": {"type": "object", "properties": {"path": {"type": "string"}}},
        },
        {
            "name": "apply_patch",
            "parameters": {"type": "object", "properties": {"edits": {"type": "array"}}},
        },
    )


def main() -> int:
    (
        owner_text,
        object_root_text,
        mode,
        turn_key,
        turn_id_text,
        baseline_artifact_text,
        response_text,
        policy_context_text,
        model_log_text,
        tool_log_text,
        tool_limit_text,
        fault_text,
    ) = sys.argv[1:]
    principal = Principal(UUID(owner_text))
    object_root = Path(object_root_text)
    turn_id = None if turn_id_text == "-" else UUID(turn_id_text)
    baseline_artifact_id = UUID(baseline_artifact_text)
    response = ScriptedModelResponse.model_validate_json(response_text, strict=True)
    policy_context = _policy_context(json.loads(policy_context_text))
    model_log = Path(model_log_text)
    tool_log = Path(tool_log_text)
    tool_limit = int(tool_limit_text)
    fault = None if fault_text == "-" else fault_text
    engine = create_engine(
        os.environ["CONTINUUM_T22_RUNTIME_DATABASE_URL"],
        hide_parameters=True,
        pool_size=1,
        max_overflow=0,
        connect_args={
            "application_name": "t22-agent-loop-worker",
            "connect_timeout": 5,
            "options": "-c statement_timeout=5000 -c lock_timeout=2000",
        },
    )
    storage = _storage(object_root)
    try:
        persistence = ArtifactPersistence(engine)
        transitions = TransitionService(engine, principal, artifact_persistence=persistence)
        checkpoints = CheckpointStorageService(
            persistence,
            storage,
            transitions,
            RuntimeRecoveryService(engine, principal),
            access_verifier=_AccessVerifier(),
            github_adapter_factory=_PublicRepositoryAdapter,
        )
        workspace_recovery = WorkspaceRecoveryService(engine, principal, checkpoints, transitions)
        service = DurableAgentLoopService(
            engine,
            principal,
            transitions,
            workspace_recovery=workspace_recovery,
            checkpoints=checkpoints,
        )
        queue = QueueService(engine, principal)
        if mode == "turn":
            asyncio.run(
                _run_turn(
                    service=service,
                    queue=queue,
                    response=response,
                    logical_key=turn_key,
                    baseline_artifact_id=baseline_artifact_id,
                    model_log=model_log,
                    fault=fault,
                )
            )
        elif mode == "tools":
            if turn_id is None:
                raise AgentLoopError("agent_turn_id_required")
            asyncio.run(
                _run_tools(
                    engine=engine,
                    principal=principal,
                    service=service,
                    queue=queue,
                    turn_id=turn_id,
                    policy_context=policy_context,
                    tool_log=tool_log,
                    limit=tool_limit,
                    fault=fault,
                )
            )
        else:
            raise AgentLoopError("agent_worker_mode_invalid")
        _emit({"kind": "worker_done"})
        return 0
    except AgentLoopError as error:
        _emit({"kind": "worker_error", "code": error.code})
        return 2
    except BaseException as error:
        _emit({"kind": "worker_error", "code": type(error).__name__})
        return 2
    finally:
        asyncio.run(storage.aclose())
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
