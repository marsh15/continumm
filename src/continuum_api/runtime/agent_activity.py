"""Lease-only dispatcher for persisted agent turn and tool activities."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import inspect
import json
import re
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.adapters.openai import OpenAIModelAdapter
from continuum_api.runtime.agent_context import AgentContextError, canonicalize_initial_context
from continuum_api.runtime.agent_loop import (
    ANTHROPIC_MODEL_ID,
    MAX_AGENT_TOOL_SCHEMAS,
    OPENAI_MODEL_ID,
    AgentLoopError,
    DurableAgentLoopService,
)
from continuum_api.runtime.leases import (
    ExecutionLease,
    LeaseLost,
    lock_current_execution,
)
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus, RuntimeRepository
from continuum_api.runtime.transitions import NextWork
from continuum_api.tools.policy import ToolPolicyContext

_TOOL_KEY = re.compile(r"^agent-tool:([0-9a-f]{32}):([0-9]{1,2})$")
_MAX_TOOL_SCHEMA_BYTES = 98_304


@dataclass(frozen=True, slots=True)
class AgentActivityStep:
    activity: str
    logical_key: str
    revision: int


@dataclass(frozen=True, slots=True)
class AgentTurnInputs:
    """Trusted, owner-scoped values resolved from persisted exact input refs."""

    initial_context: dict[str, Any]
    tool_schemas: tuple[dict[str, Any], ...]
    baseline_artifact_id: UUID
    model_id: str = OPENAI_MODEL_ID


class _AgentService(Protocol):
    async def prepare_turn(
        self,
        lease: ExecutionLease,
        logical_key: str,
        *,
        context: dict[str, Any],
        tool_schemas: Iterable[dict[str, Any]],
        baseline_artifact_id: UUID,
        model_id: str,
    ) -> Any: ...

    async def infer_or_reuse(self, lease: ExecutionLease, turn_id: UUID, adapter: Any) -> Any: ...

    async def complete_turn(
        self,
        lease: ExecutionLease,
        turn_id: UUID,
        *,
        next_work: NextWork | None = None,
        target_run_status: RunStatus = RunStatus.RUNNING,
    ) -> Any: ...

    async def pending_invocations(
        self, lease: ExecutionLease, turn_id: UUID
    ) -> tuple[Any, ...]: ...

    async def execute_tool_invocation(
        self,
        lease: ExecutionLease,
        turn_id: UUID,
        invocation_id: UUID,
        *,
        policy_context: ToolPolicyContext,
    ) -> Any: ...


InputResolver = Callable[
    [ExecutionLease, AgentActivityStep], AgentTurnInputs | Awaitable[AgentTurnInputs]
]
PolicyResolver = Callable[
    [ExecutionLease, AgentActivityStep], ToolPolicyContext | Awaitable[ToolPolicyContext]
]
NextWorkResolver = Callable[[ExecutionLease, AgentActivityStep], NextWork]


def _canonical_json(value: Any, code: str) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        encoded.encode("utf-8", errors="strict")
        return encoded
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError):
        raise ValueError(code) from None


class PersistedAgentTurnInputResolver:
    """Resolve a turn only from owner-scoped, already pinned run inputs.

    A prior trusted `prepare_turn` call must have pinned the model, task context,
    and schema digest before this resolver can dispatch a resumed turn. It has
    no defaults for those persisted values.
    """

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        *,
        tool_schemas: Iterable[dict[str, Any]],
    ) -> None:
        schemas = tuple(tool_schemas)
        if len(schemas) > MAX_AGENT_TOOL_SCHEMAS or any(
            not isinstance(schema, dict) for schema in schemas
        ):
            raise ValueError("agent_tool_schema_invalid")
        encoded = _canonical_json(schemas, "agent_tool_schema_invalid")
        if len(encoded.encode("utf-8")) > _MAX_TOOL_SCHEMA_BYTES:
            raise ValueError("agent_tool_schema_size_limit")
        self._schemas = tuple(json.loads(encoded))
        self._schema_digest = hashlib.sha256(encoded.encode("utf-8")).digest()
        self._engine = engine
        self._principal = principal

    async def __call__(self, lease: ExecutionLease, step: AgentActivityStep) -> AgentTurnInputs:
        try:
            return await asyncio.to_thread(self._resolve_once, lease, step)
        except LeaseLost:
            raise AgentLoopError("agent_lease_lost") from None
        except AgentContextError as error:
            raise AgentLoopError(error.code) from None
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    def _resolve_once(self, lease: ExecutionLease, step: AgentActivityStep) -> AgentTurnInputs:
        if step.activity != "agent_turn":
            raise AgentLoopError("agent_turn_activity_required")
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                lock_current_execution(connection, self._principal, lease)
                stored_step = (
                    connection.execute(
                        text(
                            "SELECT activity, logical_key, revision "
                            "FROM runtime_private.steps "
                            "WHERE owner_id=:owner AND run_id=:run AND step_id=:step"
                        ),
                        {"owner": lease.owner_id, "run": lease.run_id, "step": lease.step_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    stored_step is None
                    or stored_step["activity"] != step.activity
                    or stored_step["logical_key"] != step.logical_key
                    or stored_step["revision"] != step.revision
                ):
                    raise AgentLoopError("agent_step_identity_conflict")

                run = (
                    connection.execute(
                        text(
                            "SELECT model_id, model_task_context, model_tool_schema_sha256 "
                            "FROM runtime_private.runs "
                            "WHERE owner_id=:owner AND run_id=:run"
                        ),
                        {"owner": lease.owner_id, "run": lease.run_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if run is None:
                    raise AgentLoopError("agent_turn_inputs_missing")
                context = run["model_task_context"]
                model_id = run["model_id"]
                schema_digest = run["model_tool_schema_sha256"]
                if context is None or model_id is None or schema_digest is None:
                    raise AgentLoopError("agent_turn_inputs_unpinned")
                if model_id not in {OPENAI_MODEL_ID, ANTHROPIC_MODEL_ID, "scripted-v1"}:
                    raise AgentLoopError("agent_context_model_invalid")
                initial_context = canonicalize_initial_context(context)
                if bytes(schema_digest) != self._schema_digest:
                    raise AgentLoopError("agent_tool_schema_pinned")

                baseline_artifact_id = connection.execute(
                    text(
                        "SELECT artifact_id FROM runtime_private.accepted_results "
                        "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL "
                        "ORDER BY event_sequence ASC LIMIT 1"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                ).scalar_one_or_none()
                if baseline_artifact_id is None:
                    raise AgentLoopError("agent_baseline_artifact_missing")
                return AgentTurnInputs(
                    initial_context=initial_context,
                    tool_schemas=copy.deepcopy(self._schemas),
                    baseline_artifact_id=baseline_artifact_id,
                    model_id=model_id,
                )
        except AgentLoopError:
            raise
        except LeaseLost:
            raise
        except (TypeError, ValueError, OverflowError):
            raise AgentLoopError("agent_turn_inputs_invalid") from None


class AgentActivityDispatcher:
    """Dispatch a claimed agent activity using only its fenced lease."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        service: DurableAgentLoopService,
        *,
        resolve_turn_inputs: InputResolver,
        resolve_policy_context: PolicyResolver,
        candidate_validation_work: NextWorkResolver,
        adapter: Any | None = None,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._service = service
        self._resolve_turn_inputs = resolve_turn_inputs
        self._resolve_policy_context = resolve_policy_context
        self._candidate_validation_work = candidate_validation_work
        self._adapter = adapter if adapter is not None else OpenAIModelAdapter()

    async def __call__(self, lease: ExecutionLease) -> Any:
        step = await asyncio.to_thread(_read_claimed_step, self._engine, self._principal, lease)
        if step.activity == "agent_turn":
            return await self._dispatch_turn(lease, step)
        if step.activity == "agent_tool":
            return await self._dispatch_tool(lease, step)
        raise AgentLoopError("agent_activity_unsupported")

    async def _dispatch_turn(self, lease: ExecutionLease, step: AgentActivityStep) -> Any:
        inputs = await _resolve(self._resolve_turn_inputs, lease, step)
        if not isinstance(inputs, AgentTurnInputs):
            raise AgentLoopError("agent_turn_inputs_invalid")
        intent = await self._service.prepare_turn(
            lease,
            step.logical_key,
            context=inputs.initial_context,
            tool_schemas=inputs.tool_schemas,
            baseline_artifact_id=inputs.baseline_artifact_id,
            model_id=inputs.model_id,
        )
        receipt = await self._service.infer_or_reuse(lease, intent.turn_id, self._adapter)
        next_work = None
        if not receipt.invocations:
            next_work = self._candidate_validation_work(lease, step)
            if (
                not isinstance(next_work, NextWork)
                or next_work.activity != "candidate_validation"
                or next_work.revision != step.revision
                or next_work.replay_policy is not ReplayPolicy.REPLAY_SAFE
            ):
                raise AgentLoopError("agent_candidate_validation_work_invalid")
        return await self._service.complete_turn(
            lease,
            receipt.turn_id,
            next_work=next_work,
            target_run_status=RunStatus.RUNNING,
        )

    async def _dispatch_tool(self, lease: ExecutionLease, step: AgentActivityStep) -> Any:
        match = _TOOL_KEY.fullmatch(step.logical_key)
        if match is None:
            raise AgentLoopError("agent_tool_key_invalid")
        turn_id = UUID(hex=match.group(1))
        ordinal = int(match.group(2))
        invocations = await self._service.pending_invocations(lease, turn_id)
        selected = next((item for item in invocations if item.ordinal == ordinal), None)
        if selected is None or selected.logical_key != step.logical_key:
            raise AgentLoopError("agent_invocation_not_current")
        policy_context = await _resolve(self._resolve_policy_context, lease, step)
        if not isinstance(policy_context, ToolPolicyContext):
            raise AgentLoopError("agent_tool_policy_context_invalid")
        return await self._service.execute_tool_invocation(
            lease,
            turn_id,
            selected.invocation_id,
            policy_context=policy_context,
        )


async def _resolve(resolver: Callable[..., Any], *args: Any) -> Any:
    result = resolver(*args)
    return await result if inspect.isawaitable(result) else result


def _read_claimed_step(
    engine: Engine, principal: Principal, lease: ExecutionLease
) -> AgentActivityStep:
    try:
        with engine.begin() as connection:
            RuntimeRepository(connection, principal)
            lock_current_execution(connection, principal, lease)
            row = (
                connection.execute(
                    text(
                        "SELECT activity, logical_key, revision FROM runtime_private.steps "
                        "WHERE owner_id=:owner AND run_id=:run AND step_id=:step"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id, "step": lease.step_id},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise AgentLoopError("agent_step_not_found")
            return AgentActivityStep(row["activity"], row["logical_key"], row["revision"])
    except LeaseLost:
        raise AgentLoopError("agent_lease_lost") from None
    except SQLAlchemyError:
        raise AgentLoopError("agent_persistence_unavailable") from None
