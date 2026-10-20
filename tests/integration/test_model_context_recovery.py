"""Durable context integrity checks must precede new model dispatch."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from sqlalchemy import text
from test_agent_loop import T22Database, _checkpoint_service, _seed_agent_run, _storage_fixture
from test_agent_loop import t22_database as t22_database

from continuum_api.runtime.agent_loop import (
    AgentLoopError,
    DurableAgentLoopService,
    ScriptedModelResponse,
    ScriptedResponseQueue,
)
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.transitions import TransitionService


def _response() -> ScriptedModelResponse:
    return ScriptedModelResponse(
        complete=True,
        stop_reason="stop",
        model_id="scripted-v1",
        response_id="response_context",
        output_text="Inspected workspace",
        calls=(),
        input_tokens=10,
        output_tokens=2,
    )


@pytest.mark.parametrize("corruption", ["legacy_prepared", "request", "accepted_response"])
def test_corrupt_or_unverifiable_context_cannot_start_inference(
    t22_database: T22Database, tmp_path: Path, corruption: str
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    lease = QueueService(store.runtime_engine, run.principal).claim_one("t26-context-integrity")
    assert lease is not None
    storage, _ = _storage_fixture(objects)
    service = DurableAgentLoopService(
        store.runtime_engine,
        run.principal,
        TransitionService(store.runtime_engine, run.principal),
        checkpoints=_checkpoint_service(
            store.runtime_engine, run.principal, ArtifactPersistence(store.runtime_engine), storage
        ),
    )
    adapter = ScriptedResponseQueue((_response(),))

    async def exercise() -> None:
        intent = await service.prepare_turn(
            lease,
            "context-first",
            context={"task": "inspect"},
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
        )
        if corruption == "accepted_response":
            await service.infer_or_reuse(lease, intent.turn_id, adapter)
            with store.admin_engine.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE runtime_private.agent_turns SET response="
                        "jsonb_set(response,'{output_text}',CAST(:text AS jsonb)) "
                        "WHERE turn_id=:turn"
                    ),
                    {"text": json.dumps("forged accepted output"), "turn": intent.turn_id},
                )
            with pytest.raises(AgentLoopError, match="agent_persisted_response_corrupt"):
                await service.prepare_turn(
                    lease,
                    "context-second",
                    context={"task": "inspect"},
                    tool_schemas=(),
                    baseline_artifact_id=run.base_artifact_id,
                )
            assert adapter.call_count == 1
        else:
            with store.admin_engine.begin() as connection:
                if corruption == "legacy_prepared":
                    connection.execute(
                        text(
                            "UPDATE runtime_private.agent_turns SET source_sha256=NULL "
                            "WHERE turn_id=:turn"
                        ),
                        {"turn": intent.turn_id},
                    )
                else:
                    # Simulate privileged storage corruption; ordinary writes remain denied.
                    connection.execute(
                        text(
                            "ALTER TABLE runtime_private.agent_turns "
                            "DISABLE TRIGGER retention_only_redaction"
                        )
                    )
                    connection.execute(
                        text(
                            "UPDATE runtime_private.agent_turns SET context="
                            "jsonb_set(context,'{budget,requests_remaining}','1'::jsonb) "
                            "WHERE turn_id=:turn"
                        ),
                        {"turn": intent.turn_id},
                    )
                    connection.execute(
                        text(
                            "ALTER TABLE runtime_private.agent_turns "
                            "ENABLE TRIGGER retention_only_redaction"
                        )
                    )
            code = (
                "agent_context_provenance_missing"
                if corruption == "legacy_prepared"
                else "agent_persisted_request_corrupt"
            )
            with pytest.raises(AgentLoopError, match=code):
                await service.infer_or_reuse(lease, intent.turn_id, adapter)
            assert adapter.call_count == 0
        with store.admin_engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT count(*) FROM runtime_private.agent_turns WHERE run_id=:run"),
                    {"run": run.run_id},
                ).scalar_one()
                == 1
            )

    try:
        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())


def test_prepared_turn_replay_keeps_original_context_after_new_history(
    t22_database: T22Database, tmp_path: Path
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    lease = QueueService(store.runtime_engine, run.principal).claim_one("t26-context-replay")
    assert lease is not None
    storage, _ = _storage_fixture(objects)
    service = DurableAgentLoopService(
        store.runtime_engine,
        run.principal,
        TransitionService(store.runtime_engine, run.principal),
        checkpoints=_checkpoint_service(
            store.runtime_engine, run.principal, ArtifactPersistence(store.runtime_engine), storage
        ),
    )

    async def exercise() -> None:
        first = await service.prepare_turn(
            lease,
            "replay-first",
            context={"task": "inspect"},
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
        )
        adapter = ScriptedResponseQueue((_response(),))
        await service.infer_or_reuse(lease, first.turn_id, adapter)
        second = await service.prepare_turn(
            lease,
            "replay-second",
            context={"task": "inspect"},
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
        )
        assert second.turn_id != first.turn_id
        await service.infer_or_reuse(
            lease,
            second.turn_id,
            ScriptedResponseQueue(
                (_response().model_copy(update={"response_id": "response_context_second"}),)
            ),
        )
        original = await service.prepare_turn(
            lease,
            "replay-first",
            context={"task": "inspect"},
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
        )
        assert original.turn_id == first.turn_id
        assert original.request_sha256 == first.request_sha256
        adopted = await service.infer_or_reuse(lease, first.turn_id, ScriptedResponseQueue(()))
        assert adopted.adopted and adopted.response.output_text == "Inspected workspace"
        snapshot = service.read_model_budget(run.run_id)
        assert snapshot.requests_reserved == 2
        assert snapshot.requests_authorized == 2

    try:
        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())
