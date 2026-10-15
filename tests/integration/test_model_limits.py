"""PostgreSQL proofs for run-pinned model reservations and usage settlement."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import text
from test_agent_loop import (
    T22Database,
    T22Run,
    _checkpoint_service,
    _run_migration,
    _seed_agent_run,
    _storage_fixture,
)
from test_agent_loop import t22_database as t22_database

from continuum_api.runtime.agent_context import build_agent_context
from continuum_api.runtime.agent_loop import (
    MAX_AGENT_INPUT_TOKENS_PER_RUN,
    MAX_AGENT_OUTPUT_TOKENS_PER_RUN,
    OPENAI_MODEL_ID,
    AgentLoopError,
    DurableAgentLoopService,
    ScriptedModelResponse,
    ScriptedResponseQueue,
    _estimate_input_tokens,
)
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.transitions import TransitionService


def _new_service(
    store: T22Database, run: T22Run, objects: Path
) -> tuple[DurableAgentLoopService, Any, Any]:
    storage, _requests = _storage_fixture(objects)
    persistence = ArtifactPersistence(store.runtime_engine)
    checkpoints = _checkpoint_service(store.runtime_engine, run.principal, persistence, storage)
    service = DurableAgentLoopService(
        store.runtime_engine,
        run.principal,
        TransitionService(store.runtime_engine, run.principal),
        checkpoints=checkpoints,
    )
    lease = QueueService(store.runtime_engine, run.principal).claim_one("t26-model-limits")
    assert lease is not None
    return service, storage, lease


def _request_estimate(
    run: T22Run,
    logical_key: str,
    initial_context: dict[str, Any],
    budget: dict[str, Any],
    out: int,
) -> int:
    context = build_agent_context(
        initial_context=initial_context,
        accepted_turns=(),
        model_id="scripted-v1",
        budget=budget,
    )
    payload = {
        "baseline_artifact_id": str(run.base_artifact_id),
        "context": context,
        "logical_key": logical_key,
        "model_id": "scripted-v1",
        "output_tokens_reserved": out,
        "tool_schemas": [],
    }
    serialized = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return _estimate_input_tokens(serialized)


def _exact_remaining_estimate(
    run: T22Run, logical_key: str, initial_context: dict[str, Any], out: int
) -> int:
    estimate = 1
    for _ in range(12):
        next_estimate = _request_estimate(
            run,
            logical_key,
            initial_context,
            {
                "requests_remaining": 29,
                "input_tokens_remaining": estimate,
                "output_tokens_remaining": 1,
                "usage_uncertain": False,
            },
            out,
        )
        if next_estimate == estimate:
            return estimate
        estimate = next_estimate
    raise AssertionError("input budget estimate did not converge")


def test_model_pin_and_replay_reserve_once(t22_database: T22Database, tmp_path: Path) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    service, storage, lease = _new_service(store, run, objects)
    context = {"task": "inspect bounded source"}

    async def prepare() -> UUID:
        first = await service.prepare_turn(
            lease,
            "t26-primary",
            context=context,
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
        )
        replay = await service.prepare_turn(
            lease,
            "t26-primary",
            context=context,
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
        )
        assert replay.turn_id == first.turn_id
        return first.turn_id

    try:
        turn_id = asyncio.run(prepare())
        budget = service.read_model_budget(run.run_id)
        assert budget.model_id == "scripted-v1"
        assert budget.requests_reserved == 1
        assert budget.requests_authorized == 0
        assert budget.requests_received == 0
        assert budget.requests_ambiguous == 0
        assert budget.requests_in_flight == 0
        assert budget.requests_remaining == 29
        assert budget.input_tokens_reserved > len(json.dumps(context).encode("utf-8"))
        assert budget.output_tokens_reserved == 4096

        with pytest.raises(AgentLoopError, match="agent_model_switch_forbidden"):
            asyncio.run(
                service.prepare_turn(
                    lease,
                    "t26-switched-model",
                    context=context,
                    tool_schemas=(),
                    baseline_artifact_id=run.base_artifact_id,
                    model_id=OPENAI_MODEL_ID,
                    output_tokens_reserved=1,
                )
            )
        after_rejection = service.read_model_budget(run.run_id)
        assert after_rejection.requests_reserved == 1
        assert after_rejection.model_id == "scripted-v1"
        assert turn_id is not None
    finally:
        asyncio.run(storage.aclose())


def test_exact_input_and_output_caps_allow_the_last_reservation(
    t22_database: T22Database, tmp_path: Path
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    service, storage, lease = _new_service(store, run, objects)
    first_context = {"task": "first bounded request"}
    second_context = first_context

    async def exercise() -> None:
        await service.prepare_turn(
            lease,
            "t26-exact-first",
            context=first_context,
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
        )
        current = service.read_model_budget(run.run_id)
        estimate = _exact_remaining_estimate(run, "t26-exact-last", second_context, 1)
        with store.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET model_input_tokens_used=:input_used, "
                    "model_output_tokens_used=:output_used WHERE run_id=:run"
                ),
                {
                    "input_used": MAX_AGENT_INPUT_TOKENS_PER_RUN
                    - current.input_tokens_reserved
                    - estimate,
                    "output_used": MAX_AGENT_OUTPUT_TOKENS_PER_RUN
                    - current.output_tokens_reserved
                    - 1,
                    "run": run.run_id,
                },
            )
        await service.prepare_turn(
            lease,
            "t26-exact-last",
            context=second_context,
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
            output_tokens_reserved=1,
        )
        exact = service.read_model_budget(run.run_id)
        assert exact.input_tokens_remaining == 0
        assert exact.output_tokens_remaining == 0

    try:
        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())


def test_over_budget_input_or_output_fails_before_a_new_intent(
    t22_database: T22Database, tmp_path: Path
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    service, storage, lease = _new_service(store, run, objects)
    first_context = {"task": "first bounded request"}
    next_context = first_context

    async def exercise() -> None:
        await service.prepare_turn(
            lease,
            "t26-over-first",
            context=first_context,
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
        )
        current = service.read_model_budget(run.run_id)
        with store.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET model_input_tokens_used=:input_used "
                    "WHERE run_id=:run"
                ),
                {
                    "input_used": MAX_AGENT_INPUT_TOKENS_PER_RUN - current.input_tokens_reserved,
                    "run": run.run_id,
                },
            )
        with pytest.raises(AgentLoopError, match="agent_input_budget_exceeded"):
            await service.prepare_turn(
                lease,
                "t26-over-input",
                context=next_context,
                tool_schemas=(),
                baseline_artifact_id=run.base_artifact_id,
                output_tokens_reserved=1,
            )

        with store.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET model_input_tokens_used=0, "
                    "model_output_tokens_used=:output_used WHERE run_id=:run"
                ),
                {
                    "output_used": MAX_AGENT_OUTPUT_TOKENS_PER_RUN - current.output_tokens_reserved,
                    "run": run.run_id,
                },
            )
        with pytest.raises(AgentLoopError, match="agent_output_budget_exceeded"):
            await service.prepare_turn(
                lease,
                "t26-over-output",
                context=next_context,
                tool_schemas=(),
                baseline_artifact_id=run.base_artifact_id,
                output_tokens_reserved=1,
            )
        assert service.read_model_budget(run.run_id).requests_reserved == 1

    try:
        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())


@pytest.mark.parametrize("usage_known", [True, False])
def test_known_usage_settles_and_unknown_usage_keeps_reservations(
    t22_database: T22Database, tmp_path: Path, usage_known: bool
) -> None:
    store = t22_database
    objects = tmp_path / f"objects-{usage_known}"
    run = _seed_agent_run(store, objects)
    service, storage, lease = _new_service(store, run, objects)
    response = ScriptedModelResponse(
        complete=True,
        stop_reason="stop",
        model_id="scripted-v1",
        response_id="t26_response",
        output_text="done",
        calls=(),
        input_tokens=100 if usage_known else None,
        output_tokens=5 if usage_known else None,
    )
    adapter = ScriptedResponseQueue((response,))

    async def exercise() -> None:
        intent = await service.prepare_turn(
            lease,
            "t26-usage",
            context={"task": "settle usage"},
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
            output_tokens_reserved=1024,
        )
        before = service.read_model_budget(run.run_id)
        receipt = await service.infer_or_reuse(lease, intent.turn_id, adapter)
        assert receipt.response.input_tokens == response.input_tokens
        after = service.read_model_budget(run.run_id)
        assert after.requests_reserved == 1
        assert after.requests_authorized == 1
        assert after.requests_received == 1
        assert after.requests_in_flight == 0
        if usage_known:
            assert after.input_tokens_used == 100
            assert after.input_tokens_reserved == 0
            assert after.output_tokens_used == 5
            assert after.output_tokens_reserved == 0
            assert after.usage_unknown_requests == 0
        else:
            assert after.input_tokens_used == 0
            assert after.input_tokens_reserved == before.input_tokens_reserved
            assert after.output_tokens_used == 0
            assert after.output_tokens_reserved == before.output_tokens_reserved
            assert after.usage_unknown_requests == 1
        await service.infer_or_reuse(lease, intent.turn_id, adapter)
        replayed = service.read_model_budget(run.run_id)
        assert replayed.requests_reserved == 1
        assert replayed.requests_authorized == 1
        assert replayed.requests_received == 1
        assert replayed.usage_unknown_requests == after.usage_unknown_requests

    try:
        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())
        if not usage_known:
            with store.admin_engine.begin() as connection:
                connection.execute(
                    text(
                        "DELETE FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": run.owner_id, "run": run.run_id},
                )


def test_invalid_response_keeps_reservations_and_marks_usage_unknown(
    t22_database: T22Database, tmp_path: Path
) -> None:
    store = t22_database
    objects = tmp_path / "objects-invalid"
    run = _seed_agent_run(store, objects)
    service, storage, lease = _new_service(store, run, objects)
    incomplete = ScriptedModelResponse(
        complete=False,
        stop_reason="max_tokens",
        model_id="scripted-v1",
        response_id="t26_incomplete",
        output_text="partial",
        calls=(),
        input_tokens=None,
        output_tokens=None,
    )
    adapter = ScriptedResponseQueue((incomplete,))

    async def exercise() -> None:
        intent = await service.prepare_turn(
            lease,
            "t26-incomplete",
            context={"task": "incomplete response"},
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
            output_tokens_reserved=1024,
        )
        before = service.read_model_budget(run.run_id)
        with pytest.raises(AgentLoopError, match="agent_response_incomplete"):
            await service.infer_or_reuse(lease, intent.turn_id, adapter)
        after = service.read_model_budget(run.run_id)
        assert after.requests_authorized == 1
        assert after.requests_received == 1
        assert after.requests_in_flight == 0
        assert after.input_tokens_reserved == before.input_tokens_reserved
        assert after.output_tokens_reserved == before.output_tokens_reserved
        assert after.usage_unknown_requests == 1
        assert after.usage_uncertain

    try:
        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())


def test_reported_input_over_reservation_is_settled_and_blocks_tools(
    t22_database: T22Database, tmp_path: Path
) -> None:
    store = t22_database
    objects = tmp_path / "objects-input-overrun"
    run = _seed_agent_run(store, objects)
    service, storage, lease = _new_service(store, run, objects)
    response = ScriptedModelResponse(
        complete=True,
        stop_reason="tool_calls",
        model_id="scripted-v1",
        response_id="t26_input_overrun",
        output_text="",
        calls=(),
        input_tokens=50_000,
        output_tokens=4,
    )
    adapter = ScriptedResponseQueue((response,))

    async def exercise() -> None:
        intent = await service.prepare_turn(
            lease,
            "t26-input-overrun",
            context={"task": "input reservation overrun"},
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
            output_tokens_reserved=32,
        )
        before = service.read_model_budget(run.run_id)
        with pytest.raises(AgentLoopError, match="agent_input_reservation_exceeded"):
            await service.infer_or_reuse(lease, intent.turn_id, adapter)
        after = service.read_model_budget(run.run_id)
        assert after.input_tokens_used == 50_000
        assert before.input_tokens_reserved > 0
        assert after.input_tokens_reserved == 0
        assert after.output_tokens_used == 4
        assert after.output_tokens_reserved == before.output_tokens_reserved - 32
        assert after.exceeded is True
        assert after.requests_received == 1
        assert after.usage_unknown_requests == 0

    try:
        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())


def test_upgrade_backfills_existing_turn_reservations(
    t22_database: T22Database, tmp_path: Path
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    service, storage, lease = _new_service(store, run, objects)
    database_url = os.environ["CONTINUUM_T22_TEST_DATABASE_URL"]

    try:
        intent = asyncio.run(
            service.prepare_turn(
                lease,
                "t26-backfill",
                context={"task": "backfill old intent"},
                tool_schemas=(),
                baseline_artifact_id=run.base_artifact_id,
                output_tokens_reserved=512,
            )
        )
        before = service.read_model_budget(run.run_id)
        _run_migration("downgrade", "20261015_14", database_url)
        _run_migration("upgrade", "head", database_url)
        after = service.read_model_budget(run.run_id)
        assert after.model_id == "scripted-v1"
        assert after.requests_reserved == before.requests_reserved == 1
        assert after.requests_authorized == 0
        assert after.input_tokens_reserved == before.input_tokens_reserved
        assert after.output_tokens_reserved == before.output_tokens_reserved == 512
        assert after.usage_unknown_requests == 0
        assert intent.turn_id is not None
    finally:
        asyncio.run(storage.aclose())
