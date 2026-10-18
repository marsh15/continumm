"""Unit checks for bounded, run-scoped activity dispatch."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from continuum_api.runtime.activity_runner import (
    ActivityRunner,
    RunnerStopReason,
    UnknownActivity,
)
from continuum_api.runtime.leases import LEASE_DURATION_SECONDS, ExecutionLease, LeaseLost
from continuum_api.runtime.persistence import Principal


class FakeQueue:
    def __init__(self, leases: list[ExecutionLease], *, lose_heartbeat: bool = False) -> None:
        self.leases = iter(leases)
        self.claimed_runs = []
        self.heartbeat_calls = 0
        self.lose_heartbeat = lose_heartbeat

    def claim_one(self, lease_owner: str, *, run_id=None):
        self.claimed_runs.append(run_id)
        return next(self.leases, None)

    def heartbeat(self, lease: ExecutionLease) -> ExecutionLease:
        self.heartbeat_calls += 1
        if self.lose_heartbeat:
            raise LeaseLost("test_lease_lost")
        return lease


def _lease(run_id) -> ExecutionLease:
    now = datetime.now(UTC)
    return ExecutionLease(
        owner_id=uuid4(),
        run_id=run_id,
        step_id=uuid4(),
        job_id=uuid4(),
        attempt_id=uuid4(),
        generation=1,
        lease_owner="test-worker",
        lease_expires_at=now + timedelta(seconds=60),
    )


def test_runner_claims_only_admitted_run_and_stops_at_job_bound() -> None:
    run_id = uuid4()
    queue = FakeQueue([_lease(run_id), _lease(run_id), _lease(run_id)])
    calls = []

    async def handle(lease: ExecutionLease) -> None:
        calls.append(lease.run_id)

    runner = ActivityRunner(
        engine=None,  # type: ignore[arg-type]
        principal=Principal(uuid4()),
        handlers={"known": handle},
        lease_owner="test-worker",
        queue=queue,  # type: ignore[arg-type]
    )
    runner._next_due_activity = lambda selected: ("known", False)  # type: ignore[method-assign]
    runner._activity_for_lease = lambda lease: "known"  # type: ignore[method-assign]

    receipt = asyncio.run(runner.run_admitted(run_id, max_jobs=2, max_seconds=1))

    assert receipt.jobs_started == 2
    assert receipt.stop_reason is RunnerStopReason.JOB_LIMIT
    assert calls == [run_id, run_id]
    assert queue.claimed_runs == [run_id, run_id]


def test_unknown_activity_is_rejected_before_claim() -> None:
    queue = FakeQueue([])
    runner = ActivityRunner(
        engine=None,  # type: ignore[arg-type]
        principal=Principal(uuid4()),
        handlers={"known": _noop},
        lease_owner="test-worker",
        queue=queue,  # type: ignore[arg-type]
    )
    runner._next_due_activity = lambda selected: ("untrusted", False)  # type: ignore[method-assign]

    with pytest.raises(UnknownActivity, match="untrusted"):
        asyncio.run(runner.run_admitted(uuid4(), max_jobs=1, max_seconds=1))

    assert queue.claimed_runs == []


def test_lease_loss_cancels_active_handler() -> None:
    run_id = uuid4()
    queue = FakeQueue([_lease(run_id)], lose_heartbeat=True)
    cancelled = asyncio.Event()

    async def handle(lease: ExecutionLease) -> None:
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    runner = ActivityRunner(
        engine=None,  # type: ignore[arg-type]
        principal=Principal(uuid4()),
        handlers={"known": handle},
        lease_owner="test-worker",
        queue=queue,  # type: ignore[arg-type]
        heartbeat_interval_seconds=0.01,
    )
    runner._next_due_activity = lambda selected: ("known", False)  # type: ignore[method-assign]
    runner._activity_for_lease = lambda lease: "known"  # type: ignore[method-assign]
    runner._durable_result_exists = lambda lease: False  # type: ignore[method-assign]

    with pytest.raises(LeaseLost, match="test_lease_lost"):
        asyncio.run(runner.run_admitted(run_id, max_jobs=1, max_seconds=1))

    assert cancelled.is_set()
    assert queue.heartbeat_calls == 1


def test_lost_heartbeat_accepts_handler_result_already_committed() -> None:
    run_id = uuid4()
    queue = FakeQueue([_lease(run_id)], lose_heartbeat=True)
    cancelled = asyncio.Event()

    async def handle(lease: ExecutionLease) -> None:
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            cancelled.set()
            return

    runner = ActivityRunner(
        engine=None,  # type: ignore[arg-type]
        principal=Principal(uuid4()),
        handlers={"known": handle},
        lease_owner="test-worker",
        queue=queue,  # type: ignore[arg-type]
        heartbeat_interval_seconds=0.01,
    )
    runner._next_due_activity = lambda selected: ("known", False)  # type: ignore[method-assign]
    runner._activity_for_lease = lambda lease: "known"  # type: ignore[method-assign]
    runner._durable_result_exists = lambda lease: True  # type: ignore[method-assign]

    receipt = asyncio.run(runner.run_admitted(run_id, max_jobs=1, max_seconds=1))

    assert cancelled.is_set()
    assert receipt.jobs_started == 1
    assert receipt.stop_reason is RunnerStopReason.JOB_LIMIT


def test_lost_heartbeat_fails_if_cancelled_handler_returns_without_commit() -> None:
    run_id = uuid4()
    queue = FakeQueue([_lease(run_id)], lose_heartbeat=True)

    async def handle(lease: ExecutionLease) -> None:
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            return

    runner = ActivityRunner(
        engine=None,  # type: ignore[arg-type]
        principal=Principal(uuid4()),
        handlers={"known": handle},
        lease_owner="test-worker",
        queue=queue,  # type: ignore[arg-type]
        heartbeat_interval_seconds=0.01,
    )
    runner._next_due_activity = lambda selected: ("known", False)  # type: ignore[method-assign]
    runner._activity_for_lease = lambda lease: "known"  # type: ignore[method-assign]
    runner._durable_result_exists = lambda lease: False  # type: ignore[method-assign]

    with pytest.raises(LeaseLost, match="test_lease_lost"):
        asyncio.run(runner.run_admitted(run_id, max_jobs=1, max_seconds=1))


@pytest.mark.parametrize("seconds", [float("nan"), float("inf"), float("-inf"), True])
def test_runner_rejects_invalid_time_bounds(seconds: float) -> None:
    runner = ActivityRunner(
        engine=None,  # type: ignore[arg-type]
        principal=Principal(uuid4()),
        handlers={"known": _noop},
        lease_owner="test-worker",
        queue=FakeQueue([]),  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="runtime_runner_time_bound_invalid"):
        asyncio.run(runner.run_admitted(uuid4(), max_jobs=1, max_seconds=seconds))


def test_runner_rejects_boolean_job_count() -> None:
    runner = ActivityRunner(
        engine=None,  # type: ignore[arg-type]
        principal=Principal(uuid4()),
        handlers={"known": _noop},
        lease_owner="test-worker",
        queue=FakeQueue([]),  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="runtime_runner_job_bound_invalid"):
        asyncio.run(runner.run_admitted(uuid4(), max_jobs=True, max_seconds=1))


def test_runner_rejects_fractional_job_count() -> None:
    runner = ActivityRunner(
        engine=None,  # type: ignore[arg-type]
        principal=Principal(uuid4()),
        handlers={"known": _noop},
        lease_owner="test-worker",
        queue=FakeQueue([]),  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="runtime_runner_job_bound_invalid"):
        asyncio.run(runner.run_admitted(uuid4(), max_jobs=1.5, max_seconds=1))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "interval",
    [float("nan"), float("inf"), float("-inf"), LEASE_DURATION_SECONDS],
)
def test_runner_rejects_unsafe_heartbeat_interval(interval: float) -> None:
    with pytest.raises(ValueError, match="runtime_heartbeat_interval_invalid"):
        ActivityRunner(
            engine=None,  # type: ignore[arg-type]
            principal=Principal(uuid4()),
            handlers={"known": _noop},
            lease_owner="test-worker",
            queue=FakeQueue([]),  # type: ignore[arg-type]
            heartbeat_interval_seconds=interval,
        )


async def _noop(lease: ExecutionLease) -> None:
    return None
