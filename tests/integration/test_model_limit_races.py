"""Competing preparations must share one atomic run model allowance."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

from sqlalchemy import text
from test_agent_loop import (
    T22Database,
    _checkpoint_service,
    _seed_agent_run,
    _storage_fixture,
)
from test_agent_loop import t22_database as t22_database

from continuum_api.runtime.agent_loop import AgentLoopError, DurableAgentLoopService
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.transitions import TransitionService


def test_competing_turns_cannot_reserve_the_same_remaining_output(
    t22_database: T22Database, tmp_path: Path
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    lease = QueueService(store.runtime_engine, run.principal).claim_one("t26-budget-race")
    assert lease is not None
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET model_output_tokens_used=25904 WHERE run_id=:run"
            ),
            {"run": run.run_id},
        )
    barrier = Barrier(2)

    def reserve(ordinal: int) -> str:
        # Separate clients/event loops, as independent worker callers would use.
        storage, _ = _storage_fixture(objects)
        service = DurableAgentLoopService(
            store.runtime_engine,
            run.principal,
            TransitionService(store.runtime_engine, run.principal),
            checkpoints=_checkpoint_service(
                store.runtime_engine,
                run.principal,
                ArtifactPersistence(store.runtime_engine),
                storage,
            ),
        )
        barrier.wait(timeout=10)

        async def exercise() -> str:
            try:
                await service.prepare_turn(
                    lease,
                    f"competing-{ordinal}",
                    context={"task": "inspect the same workspace"},
                    tool_schemas=(),
                    baseline_artifact_id=run.base_artifact_id,
                )
                return "reserved"
            except AgentLoopError as error:
                return error.code
            finally:
                await storage.aclose()

        return asyncio.run(exercise())

    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(reserve, (1, 2)))
    assert sorted(results) == ["agent_output_budget_exceeded", "reserved"]
    with store.admin_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT count(*) FROM runtime_private.agent_turns WHERE run_id=:run"),
                {"run": run.run_id},
            ).scalar_one()
            == 1
        )
        row = connection.execute(
            text(
                "SELECT model_output_tokens_used,model_output_tokens_reserved FROM "
                "runtime_private.runs WHERE run_id=:run"
            ),
            {"run": run.run_id},
        ).one()
        assert row.model_output_tokens_used + row.model_output_tokens_reserved == 30000
