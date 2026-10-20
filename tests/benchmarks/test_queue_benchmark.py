"""Bounded local queue measurements; managed sandboxes/models are excluded."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import statistics
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import event, text

from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus, RuntimeRepository
from continuum_api.runtime.transitions import TransitionService
from tests.integration.test_agent_loop import T22Database
from tests.integration.test_agent_loop import t22_database as t22_database


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def test_scripted_queue_benchmark(t22_database: T22Database) -> None:
    if os.environ.get("CONTINUUM_RUN_BENCHMARK") != "1":
        pytest.skip("explicit CONTINUUM_RUN_BENCHMARK=1 required")
    store = t22_database
    jobs = 12
    trials = 3
    results = []
    for workers in (1, 2, 4):
        for trial in range(trials):
            results.append(_trial(store, workers, trial, jobs))
    with store.admin_engine.connect() as connection:
        database_version = connection.execute(text("SELECT version()")).scalar_one()
    destination = Path(os.environ.get("CONTINUUM_BENCHMARK_REPORT", ".artifacts/benchmark.json"))
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(
            {
                "created_at": datetime.now(UTC).isoformat(),
                "benchmark_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "working_tree_dirty": bool(
                    subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
                ),
                "worker_kind": "threads with separate PostgreSQL transactions",
                "naive_worker_count": 1,
                "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "machine": platform.platform(),
                "logical_cpus": os.cpu_count(),
                "python": platform.python_version(),
                "database": database_version,
                "workload": "one queue claim and accepted wait result per owner",
                "trials": trials,
                "seed": None,
                "jobs_per_trial": jobs,
                "limitations": [
                    "local loopback PostgreSQL",
                    "12 jobs per trial; no stable tail claim",
                    "no model or sandbox calls",
                    "in-memory baseline has no durability; restart simulated by discarding state",
                    "storage is cumulative relation size, includes fixture and indexes",
                    "dispatch latency measured from common batch start",
                    "recovery probe forces a database lease expiry; excludes natural lease wait",
                ],
                "results": results,
            },
            indent=2,
        )
        + "\n"
    )


def _trial(store: T22Database, workers: int, trial: int, jobs: int) -> dict[str, object]:
    owners = []
    for _ in range(jobs):
        principal = Principal(uuid4())
        with store.admin_engine.begin() as connection:
            connection.execute(
                text("INSERT INTO runtime_private.users (user_id,github_user_id) VALUES (:id,:gh)"),
                {"id": principal.account_id, "gh": principal.account_id.int % 10**15 + 1},
            )
        with store.runtime_engine.begin() as connection:
            repository = RuntimeRepository(connection, principal)
            run = repository.create_run()
            step = repository.create_step(run, "benchmark:one", "benchmark")
            repository.create_job(run, step, replay_policy=ReplayPolicy.REPLAY_SAFE)
        owners.append(principal)
    query_count = [0]
    count_lock = threading.Lock()

    def count(*_: object) -> None:
        with count_lock:
            query_count[0] += 1

    event.listen(store.runtime_engine, "before_cursor_execute", count)
    latencies: list[float] = []
    started = time.perf_counter()

    def durable(principal: Principal) -> None:
        lease = QueueService(store.runtime_engine, principal).claim_one("benchmark-worker")
        assert lease is not None
        with count_lock:
            latencies.append(time.perf_counter() - started)
        TransitionService(store.runtime_engine, principal).commit_result(
            lease=lease,
            result={"workload": "one-result"},
            target_run_status=RunStatus.WAITING_CREDENTIALS,
        )

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(durable, owners))
        elapsed = time.perf_counter() - started
    finally:
        event.remove(store.runtime_engine, "before_cursor_execute", count)
    naive_latencies = []
    naive_results: dict[str, str] = {}
    naive_started = time.perf_counter()

    def naive(principal: Principal) -> None:
        with count_lock:
            naive_latencies.append(time.perf_counter() - naive_started)
            naive_results[str(principal.account_id)] = "one-result"

    for principal in owners:
        naive(principal)
    naive_elapsed = time.perf_counter() - naive_started
    # A process restart loses the baseline's only authority.
    naive_results.clear()
    assert not naive_results
    with store.admin_engine.connect() as connection:
        committed = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.accepted_results "
                "WHERE payload->>'workload'='one-result'"
            )
        ).scalar_one()
        bytes_used = connection.execute(
            text(
                "SELECT sum(pg_total_relation_size(quote_ident(schemaname)||'.'||"
                "quote_ident(tablename))) FROM pg_tables WHERE schemaname='runtime_private'"
            )
        ).scalar_one()
    with store.admin_engine.connect() as connection:
        current_records = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.accepted_results "
                "WHERE owner_id=ANY(:owners) AND payload->>'workload'='one-result'"
            ),
            {"owners": [owner.account_id for owner in owners]},
        ).scalar_one()
    assert current_records == jobs
    recovery_seconds = _recovery_probe(store)
    return {
        "forced_expiry_reclaim_seconds": recovery_seconds,
        "workers": workers,
        "trial": trial,
        "jobs": jobs,
        "durable_seconds": elapsed,
        "durable_jobs_per_second": jobs / elapsed,
        "dispatch_p50_seconds": statistics.median(latencies),
        "dispatch_p95_seconds": _percentile(latencies, 0.95),
        "sql_statements": query_count[0],
        "runtime_total_relation_bytes": int(bytes_used),
        "naive_seconds": naive_elapsed,
        "naive_jobs_per_second": jobs / naive_elapsed,
        "naive_dispatch_p95_seconds": _percentile(naive_latencies, 0.95),
        "naive_after_simulated_restart_records": len(naive_results),
        "durable_committed_records_cumulative": committed,
        "durable_records_this_trial": current_records,
        "raw_dispatch_seconds": latencies,
    }


def _recovery_probe(store: T22Database) -> float:
    principal = Principal(uuid4())
    with store.admin_engine.begin() as connection:
        connection.execute(
            text("INSERT INTO runtime_private.users(user_id,github_user_id) VALUES (:id,:gh)"),
            {"id": principal.account_id, "gh": principal.account_id.int % 10**15 + 1},
        )
    with store.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run = repository.create_run()
        step = repository.create_step(run, "benchmark:recovery", "benchmark")
        repository.create_job(run, step, replay_policy=ReplayPolicy.REPLAY_SAFE)
    queue = QueueService(store.runtime_engine, principal)
    original = queue.claim_one("benchmark-before-loss")
    assert original is not None
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.jobs SET "
                "lease_expires_at=clock_timestamp()-interval '1 second' "
                "WHERE job_id=:job"
            ),
            {"job": original.job_id},
        )
    started = time.perf_counter()
    recovered = queue.claim_one("benchmark-after-loss")
    elapsed = time.perf_counter() - started
    assert recovered is not None and recovered.generation > original.generation
    TransitionService(store.runtime_engine, principal).commit_result(
        lease=recovered,
        result={"recovery": True},
        target_run_status=RunStatus.WAITING_CREDENTIALS,
    )
    return elapsed
