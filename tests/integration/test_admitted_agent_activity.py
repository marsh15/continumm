"""PostgreSQL checks for admission-bound first agent-turn initialization."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_agent_loop import T22Database, _checkpoint_service, _storage_fixture
from test_agent_loop import t22_database as t22_database
from test_source_preparation import (
    _PYTHON_FILES,
    _receipt,
    _start_source_run,
)

from continuum_api.runtime import agent_activity
from continuum_api.runtime.admitted_agent_activity import AdmittedAgentTurnInputResolver
from continuum_api.runtime.agent_activity import AgentLoopError
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RuntimeRepository
from continuum_api.runtime.source_preparation import SourcePreparationActivityHandler


def _prepared_first_turn(database: T22Database, tmp_path: Path):
    receipt = _receipt(_PYTHON_FILES, "python-pytest")
    principal, source_lease = _start_source_run(database, _PYTHON_FILES, "python-pytest", receipt)
    storage, _requests = _storage_fixture(tmp_path / str(source_lease.run_id))
    checkpoints = _checkpoint_service(
        database.runtime_engine,
        principal,
        ArtifactPersistence(database.runtime_engine),
        storage,
    )
    preparation = SourcePreparationActivityHandler(
        database.runtime_engine,
        principal,
        checkpoints,
        lambda _lease, _repository_id, _receipt: _PYTHON_FILES,
    )
    asyncio.run(preparation(source_lease))
    turn_lease = QueueService(database.runtime_engine, principal).claim_one(
        "t34-admitted-agent-turn",
        run_id=source_lease.run_id,
    )
    assert turn_lease is not None
    step = agent_activity._read_claimed_step(database.runtime_engine, principal, turn_lease)
    resolver = AdmittedAgentTurnInputResolver(
        database.runtime_engine,
        principal,
        checkpoints,
        tool_schemas=(),
    )
    return principal, turn_lease, step, resolver, storage, checkpoints


def _queue_revision_one_turn(database: T22Database, principal: Principal, run_id):
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET revision=1 WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": principal.account_id, "run": run_id},
        )
    with database.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        step_id = repository.create_step(run_id, "agent-turn:1", "agent_turn", revision=1)
        job_id = repository.create_job(run_id, step_id, replay_policy=ReplayPolicy.REPLAY_SAFE)
    lease = QueueService(database.runtime_engine, principal).claim_one(
        "t34-admitted-revision-one", run_id=run_id
    )
    assert lease is not None and lease.job_id == job_id
    return lease, agent_activity._read_claimed_step(database.runtime_engine, principal, lease)


def test_admitted_first_turn_pins_source_and_model_inputs_with_partial_recovery(
    t22_database: T22Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    principal, lease, step, resolver, storage, _checkpoints = _prepared_first_turn(
        t22_database, tmp_path
    )
    assert step.activity == "agent_turn"
    assert step.logical_key == "agent-turn:0"
    assert step.revision == 0

    actual_initialize = resolver._resolver.initialize
    failed_once = False

    async def interrupt_after_source_pin(*args, **kwargs):
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise AgentLoopError("simulated_initialization_interruption")
        await actual_initialize(*args, **kwargs)

    monkeypatch.setattr(resolver._resolver, "initialize", interrupt_after_source_pin)
    try:
        with pytest.raises(AgentLoopError, match="simulated_initialization_interruption"):
            asyncio.run(resolver(lease, step))
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            state = (
                connection.execute(
                    text(
                        "SELECT (SELECT count(*) FROM runtime_private.validation_source_pins "
                        "WHERE owner_id=:owner AND run_id=:run) AS source_pins, "
                        "(SELECT count(*) FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner AND run_id=:run) AS turns, "
                        "(SELECT model_id IS NULL AND model_task_context IS NULL "
                        "AND model_tool_schema_sha256 IS NULL AND model_requests_reserved=0 "
                        "FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run) AS model_uninitialized"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one()
            )
        assert state["source_pins"] == 1
        assert state["turns"] == 0
        assert state["model_uninitialized"] is True

        inputs = asyncio.run(resolver(lease, step))
        assert inputs.initial_context == {"task": "Implement the requested change."}
        assert inputs.model_id == "gpt-5.4-mini-2026-03-17"
        assert inputs.baseline_artifact_id is not None
        replayed = asyncio.run(resolver(lease, step))
        assert replayed == inputs
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            state = (
                connection.execute(
                    text(
                        "SELECT (SELECT count(*) FROM runtime_private.validation_source_pins "
                        "WHERE owner_id=:owner AND run_id=:run) AS source_pins, "
                        "(SELECT count(*) FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner AND run_id=:run) AS turns, "
                        "(SELECT model_id FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run) AS pinned_model, "
                        "(SELECT model_requests_reserved FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run) AS requests_reserved"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one()
            )
        assert state["source_pins"] == 1
        assert state["turns"] == 0
        assert state["pinned_model"] == inputs.model_id
        assert state["requests_reserved"] == 0

        later_lease, later_step = _queue_revision_one_turn(t22_database, principal, lease.run_id)
        assert later_step.logical_key == "agent-turn:1"
        assert asyncio.run(resolver(later_lease, later_step)) == inputs

        wrong_owner = AdmittedAgentTurnInputResolver(
            t22_database.runtime_engine,
            Principal(uuid4()),
            _checkpoints,
            tool_schemas=(),
        )
        with pytest.raises(AgentLoopError, match="agent_lease_owner_mismatch"):
            asyncio.run(wrong_owner(lease, step))
        with pytest.raises(AgentLoopError, match="agent_lease_lost"):
            asyncio.run(resolver(replace(lease, generation=lease.generation + 1), step))
    finally:
        asyncio.run(storage.aclose())


def test_admitted_first_turn_rejects_corrupt_source_result_binding(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    principal, lease, step, resolver, storage, _checkpoints = _prepared_first_turn(
        t22_database, tmp_path
    )
    try:
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE runtime_private.accepted_results DISABLE TRIGGER USER")
            )
            connection.execute(
                text(
                    "UPDATE runtime_private.accepted_results SET payload="
                    "jsonb_set(payload, '{commit_sha}', to_jsonb('wrong'::text)) "
                    "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            )
            connection.execute(
                text("ALTER TABLE runtime_private.accepted_results ENABLE TRIGGER USER")
            )
        with pytest.raises(AgentLoopError, match="agent_admission_binding_invalid"):
            asyncio.run(resolver(lease, step))
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.validation_source_pins "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                ).scalar_one()
                == 0
            )
    finally:
        asyncio.run(storage.aclose())


def test_later_turn_does_not_initialize_missing_admission_model_pins(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    principal, lease, _step, resolver, storage, _checkpoints = _prepared_first_turn(
        t22_database, tmp_path
    )
    try:
        later_lease, later_step = _queue_revision_one_turn(t22_database, principal, lease.run_id)
        with pytest.raises(AgentLoopError, match="agent_turn_inputs_unpinned"):
            asyncio.run(resolver(later_lease, later_step))
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            state = connection.execute(
                text(
                    "SELECT model_id IS NULL AND model_task_context IS NULL "
                    "AND model_tool_schema_sha256 IS NULL FROM runtime_private.runs "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one()
            source_pins = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.validation_source_pins "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one()
        assert state is True
        assert source_pins == 0
    finally:
        asyncio.run(storage.aclose())


def test_admitted_first_turn_rejects_provider_model_mismatch(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    principal, lease, step, resolver, storage, _checkpoints = _prepared_first_turn(
        t22_database, tmp_path
    )
    try:
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE runtime_private.run_admission_pins DISABLE TRIGGER USER")
            )
            connection.execute(
                text(
                    "UPDATE runtime_private.run_admission_pins SET provider='anthropic' "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            )
            connection.execute(
                text("ALTER TABLE runtime_private.run_admission_pins ENABLE TRIGGER USER")
            )
        with pytest.raises(AgentLoopError, match="agent_admission_binding_invalid"):
            asyncio.run(resolver(lease, step))
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.validation_source_pins "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                ).scalar_one()
                == 0
            )
    finally:
        asyncio.run(storage.aclose())
