from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from continuum_api.runtime import agent_activity
from continuum_api.runtime.agent_activity import (
    AgentActivityDispatcher,
    AgentActivityStep,
    AgentTurnInputs,
)
from continuum_api.runtime.agent_loop import AgentLoopError
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus
from continuum_api.runtime.transitions import NextWork
from continuum_api.tools.policy import ToolPolicyContext


def _lease() -> SimpleNamespace:
    return SimpleNamespace(owner_id=uuid4(), run_id=uuid4(), step_id=uuid4())


def _policy() -> ToolPolicyContext:
    from continuum_api.tools.contracts import ToolStage

    return ToolPolicyContext(
        capabilities=frozenset(),
        profile_id="python-pytest",
        profile_version="1",
        actor_can_write_repository=False,
        app_can_access_repository=True,
        stage=ToolStage.AGENT_WORK,
        run_budget_available=True,
        baseline_tree_sha256="0" * 64,
        baseline_source_paths=frozenset(),
        baseline_test_paths=frozenset(),
        baseline_paths_verified=True,
        protected_paths=frozenset(),
    )


class _Service:
    def __init__(self, *, calls: tuple[object, ...] = ()) -> None:
        self.calls = calls
        self.invocations: tuple[object, ...] = ()
        self.executed: list[tuple[UUID, UUID, ToolPolicyContext]] = []
        self.completed: list[tuple[UUID, NextWork | None, RunStatus]] = []

    async def prepare_turn(self, lease, logical_key, **kwargs):
        self.prepared = (lease, logical_key, kwargs)
        return SimpleNamespace(turn_id=UUID(int=55))

    async def infer_or_reuse(self, lease, turn_id, adapter):
        self.inferred = (lease, turn_id, adapter)
        return SimpleNamespace(turn_id=turn_id, invocations=self.calls)

    async def complete_turn(self, lease, turn_id, *, next_work, target_run_status):
        self.completed.append((turn_id, next_work, target_run_status))
        return "completed"

    async def pending_invocations(self, lease, turn_id):
        self.pending = (lease, turn_id)
        return self.invocations

    async def execute_tool_invocation(self, lease, turn_id, invocation_id, *, policy_context):
        self.executed.append((turn_id, invocation_id, policy_context))
        return "tool-accepted"


def test_dispatch_turn_resolves_inputs_and_routes_final_turn_to_validation(monkeypatch):
    lease = _lease()
    step = AgentActivityStep("agent_turn", "agent-turn:primary", 4)
    monkeypatch.setattr(agent_activity, "_read_claimed_step", lambda *_: step)
    service = _Service()
    baseline_id = uuid4()
    inputs = AgentTurnInputs({"task": "edit the source"}, (), baseline_id)
    next_work = NextWork(
        "candidate-validation:r4",
        "candidate_validation",
        revision=4,
        replay_policy=ReplayPolicy.REPLAY_SAFE,
    )
    dispatcher = AgentActivityDispatcher(
        object(),
        Principal(lease.owner_id),
        service,  # type: ignore[arg-type]
        resolve_turn_inputs=lambda _lease, _step: inputs,
        resolve_policy_context=lambda _lease, _step: _policy(),
        candidate_validation_work=lambda _lease, _step: next_work,
        adapter=object(),
    )

    result = asyncio.run(dispatcher(lease))  # type: ignore[arg-type]

    assert result == "completed"
    assert service.prepared[1] == step.logical_key
    assert service.prepared[2]["baseline_artifact_id"] == baseline_id
    assert service.completed == [(UUID(int=55), next_work, RunStatus.RUNNING)]


def test_dispatch_tool_uses_exact_persisted_invocation_and_policy(monkeypatch):
    lease = _lease()
    turn_id = UUID(int=23)
    step = AgentActivityStep("agent_tool", f"agent-tool:{turn_id.hex}:1", 2)
    monkeypatch.setattr(agent_activity, "_read_claimed_step", lambda *_: step)
    selected = SimpleNamespace(
        ordinal=1,
        logical_key=step.logical_key,
        invocation_id=UUID(int=91),
    )
    service = _Service()
    service.invocations = (SimpleNamespace(ordinal=0), selected)
    policy = _policy()
    dispatcher = AgentActivityDispatcher(
        object(),
        Principal(lease.owner_id),
        service,  # type: ignore[arg-type]
        resolve_turn_inputs=lambda *_: pytest.fail("tool activity resolved turn inputs"),
        resolve_policy_context=lambda _lease, _step: policy,
        candidate_validation_work=lambda *_: pytest.fail("tool activity completed a turn"),
        adapter=object(),
    )

    result = asyncio.run(dispatcher(lease))  # type: ignore[arg-type]

    assert result == "tool-accepted"
    assert service.pending == (lease, turn_id)
    assert service.executed == [(turn_id, selected.invocation_id, policy)]


def test_dispatch_tool_rejects_noncanonical_persisted_key(monkeypatch):
    lease = _lease()
    monkeypatch.setattr(
        agent_activity,
        "_read_claimed_step",
        lambda *_: AgentActivityStep("agent_tool", "agent-tool:not-a-uuid:1", 0),
    )
    service = _Service()
    dispatcher = AgentActivityDispatcher(
        object(),
        Principal(lease.owner_id),
        service,  # type: ignore[arg-type]
        resolve_turn_inputs=lambda *_: pytest.fail("invalid key resolved inputs"),
        resolve_policy_context=lambda *_: _policy(),
        candidate_validation_work=lambda *_: pytest.fail("invalid key completed a turn"),
        adapter=object(),
    )

    with pytest.raises(AgentLoopError, match="agent_tool_key_invalid"):
        asyncio.run(dispatcher(lease))  # type: ignore[arg-type]
