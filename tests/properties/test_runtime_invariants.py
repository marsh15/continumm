"""Seeded competing transitions against production PostgreSQL services.

The fixture and source archive are shared with the durable-agent integration
proof; no alternate implementation of the runtime is under test.
"""

from __future__ import annotations

import json
import random
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text

from continuum_api.runtime.cancellation import CancellationService
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, QueueService
from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository
from continuum_api.runtime.transitions import InvalidTransition, TransitionService
from tests.integration.test_agent_loop import T22Database, _seed_agent_run
from tests.integration.test_agent_loop import t22_database as t22_database

SEEDS = (1, 7, 19, 43, 101, 997, 8191, 65537)
OPERATIONS = ("checkpoint", "reclaim", "stale_checkpoint", "cancel", "claim", "foreign_read")


def _exercise(store: T22Database, directory: Path, operations: list[str]) -> None:
    run = _seed_agent_run(store, directory)
    queue = QueueService(store.runtime_engine, run.principal)
    lease = queue.claim_one("property-current")
    assert lease is not None
    leases: list[ExecutionLease] = [lease]
    cancelled = False
    for index, operation in enumerate(operations):
        if operation == "cancel":
            receipt = CancellationService(store.runtime_engine, run.principal).cancel(run.run_id)
            assert receipt.run_status is RunStatus.CANCELLED
            cancelled = True
        elif operation == "reclaim" and not cancelled:
            with store.admin_engine.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE runtime_private.jobs SET lease_expires_at=clock_timestamp()-"
                        "interval '1 second' WHERE job_id=:job AND status='leased'"
                    ),
                    {"job": lease.job_id},
                )
            replacement = queue.claim_one(f"property-generation-{index}")
            assert replacement is not None
            assert replacement.generation > lease.generation
            leases.append(replacement)
            lease = replacement
        elif operation in {"checkpoint", "stale_checkpoint"}:
            target = leases[0] if operation == "stale_checkpoint" else lease
            stale = target.generation != lease.generation or cancelled
            if stale:
                with pytest.raises(LeaseLost):
                    queue.persist_checkpoint(target, f"generated-{index}", {"index": index})
            else:
                receipt = queue.persist_checkpoint(target, f"generated-{index}", {"index": index})
                replay = queue.persist_checkpoint(target, f"generated-{index}", {"index": index})
                assert replay.adopted and receipt.checkpoint_id == replay.checkpoint_id
        elif operation == "claim":
            assert queue.claim_one("property-competitor") is None
        elif operation == "foreign_read":
            with store.runtime_engine.begin() as connection:
                foreign = RuntimeRepository(connection, Principal(uuid4()))
                assert foreign.get_run(run.run_id) is None
        with store.admin_engine.connect() as connection:
            rows = (
                connection.execute(
                    text(
                        "SELECT sequence FROM runtime_private.events WHERE run_id=:run "
                        "ORDER BY sequence"
                    ),
                    {"run": run.run_id},
                )
                .scalars()
                .all()
            )
            assert rows == list(range(1, len(rows) + 1))
            status = connection.execute(
                text("SELECT status FROM runtime_private.runs WHERE run_id=:run"),
                {"run": run.run_id},
            ).scalar_one()
            assert status == ("cancelled" if cancelled else "running")
    # Compete a terminal cancellation with an activity result in real transactions.
    if not cancelled:

        def commit() -> None:
            try:
                TransitionService(store.runtime_engine, run.principal).commit_result(
                    lease=lease,
                    result={"seeded": True},
                    target_run_status=RunStatus.WAITING_CREDENTIALS,
                )
            except (LeaseLost, InvalidTransition):
                pass

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(commit),
                pool.submit(
                    CancellationService(store.runtime_engine, run.principal).cancel, run.run_id
                ),
            ]
            for future in futures:
                future.result(timeout=10)
    assert queue.claim_one("after-terminal") is None


@pytest.mark.parametrize("seed", SEEDS)
def test_generated_competing_transitions(
    t22_database: T22Database,
    tmp_path: Path,
    seed: int,
) -> None:
    randomizer = random.Random(seed)
    operations = [randomizer.choice(OPERATIONS) for _ in range(16)]
    try:
        _exercise(t22_database, tmp_path / "initial", operations)
    except Exception:
        # Delta-debug by deletion; rerun against a fresh owner/run each time.
        minimal = operations[:]
        for index in reversed(range(len(minimal))):
            candidate = minimal[:index] + minimal[index + 1 :]
            try:
                _exercise(t22_database, tmp_path / f"shrink-{index}", candidate)
            except Exception:
                minimal = candidate
        evidence = Path(".artifacts/property-failures")
        evidence.mkdir(parents=True, exist_ok=True)
        (evidence / f"seed-{seed}.json").write_text(
            json.dumps(
                {
                    "seed": seed,
                    "operations": operations,
                    "minimal_operations": minimal,
                },
                indent=2,
            )
            + "\n"
        )
        raise
