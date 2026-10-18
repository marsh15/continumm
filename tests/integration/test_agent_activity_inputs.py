"""PostgreSQL checks for owner-scoped persisted agent turn input resolution."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from test_agent_loop import (
    T22Database,
    _checkpoint_service,
    _seed_agent_run,
    _storage_fixture,
)
from test_agent_loop import t22_database as t22_database

from continuum_api.runtime import agent_activity
from continuum_api.runtime.agent_activity import (
    AgentLoopError,
    PersistedAgentTurnInputResolver,
)
from continuum_api.runtime.agent_loop import DurableAgentLoopService
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.transitions import TransitionService


def test_persisted_resolver_feeds_pinned_turn_and_fences_invalid_inputs(
    t22_database: T22Database, tmp_path: Path
) -> None:
    database = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(database, objects)
    lease = QueueService(database.runtime_engine, run.principal).claim_one(
        "t34-persisted-input-resolver"
    )
    assert lease is not None
    schemas = (
        {
            "name": "read_file",
            "parameters": {"type": "object", "properties": {"path": {"type": "string"}}},
        },
    )
    step = agent_activity._read_claimed_step(database.runtime_engine, run.principal, lease)
    resolver = PersistedAgentTurnInputResolver(
        database.runtime_engine, run.principal, tool_schemas=schemas
    )
    with pytest.raises(AgentLoopError, match="agent_turn_inputs_unpinned"):
        asyncio.run(resolver(lease, step))
    with pytest.raises(AgentLoopError, match="agent_turn_activity_required"):
        asyncio.run(resolver(lease, replace(step, activity="agent_tool")))

    # A prior trusted prepare_turn pins the persisted model, context, and schemas.
    storage, _requests = _storage_fixture(objects)
    persistence = ArtifactPersistence(database.runtime_engine)
    checkpoints = _checkpoint_service(database.runtime_engine, run.principal, persistence, storage)
    service = DurableAgentLoopService(
        database.runtime_engine,
        run.principal,
        TransitionService(database.runtime_engine, run.principal),
        checkpoints=checkpoints,
    )
    task_context = {"task": "update answer"}
    intent = asyncio.run(
        service.prepare_turn(
            lease,
            step.logical_key,
            context=task_context,
            tool_schemas=schemas,
            baseline_artifact_id=run.base_artifact_id,
            model_id="scripted-v1",
            output_tokens_reserved=1,
        )
    )
    inputs = asyncio.run(resolver(lease, step))
    assert inputs.initial_context == task_context
    assert inputs.tool_schemas == schemas
    assert inputs.baseline_artifact_id == run.base_artifact_id
    assert inputs.model_id == "scripted-v1"

    # The resolved values replay through the durable service's pin checks.
    try:
        resolved_intent = asyncio.run(
            service.prepare_turn(
                lease,
                step.logical_key,
                context=inputs.initial_context,
                tool_schemas=inputs.tool_schemas,
                baseline_artifact_id=inputs.baseline_artifact_id,
                model_id=inputs.model_id,
                output_tokens_reserved=1,
            )
        )
    finally:
        asyncio.run(storage.aclose())
    assert intent.baseline_artifact_id == run.base_artifact_id
    assert resolved_intent.turn_id == intent.turn_id
    inputs.tool_schemas[0]["name"] = "caller_mutation"
    assert asyncio.run(resolver(lease, step)).tool_schemas == schemas

    with pytest.raises(AgentLoopError, match="agent_lease_lost"):
        asyncio.run(
            PersistedAgentTurnInputResolver(
                database.runtime_engine, Principal(uuid4()), tool_schemas=schemas
            )(lease, step)
        )
    with pytest.raises(AgentLoopError, match="agent_lease_lost"):
        asyncio.run(resolver(replace(lease, generation=lease.generation + 1), step))
    with pytest.raises(AgentLoopError, match="agent_step_identity_conflict"):
        asyncio.run(resolver(lease, replace(step, revision=step.revision + 1)))
    with pytest.raises(AgentLoopError, match="agent_tool_schema_pinned"):
        asyncio.run(
            PersistedAgentTurnInputResolver(
                database.runtime_engine,
                run.principal,
                tool_schemas=({"name": "different"},),
            )(lease, step)
        )

    with pytest.raises(ValueError, match="agent_tool_schema_invalid"):
        PersistedAgentTurnInputResolver(
            database.runtime_engine,
            run.principal,
            tool_schemas=({"name": object()},),  # type: ignore[dict-item]
        )
