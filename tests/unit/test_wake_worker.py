"""Focused tests for bounded wake work and its lease fence."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import UUID

import pytest

from continuum_api.runtime.wake_worker import (
    BoundedWakeWorker,
    DrainLease,
    InboxWork,
    RunWork,
    WakeDrainReceipt,
    WakeWorkerScheduler,
)
from continuum_api.wake import WakeInput


@dataclass
class FakeWakeStore:
    inbox: list[InboxWork] | None = None
    runs: list[RunWork] | None = None
    claim_busy: bool = False
    current: bool = True
    finish_result: bool = True
    claimed: list[tuple[UUID, UUID, int]] | None = None
    completed: list[tuple[UUID, str]] | None = None
    finish_calls: list[bool] | None = None
    heartbeat_calls: int = 0

    def claim(self, wake_id: UUID, attempt_id: UUID, generation: int) -> DrainLease | None:
        if self.claimed is None:
            self.claimed = []
        self.claimed.append((wake_id, attempt_id, generation))
        if self.claim_busy:
            return None
        return DrainLease(wake_id, UUID(int=1), generation)

    def heartbeat(self, lease: DrainLease) -> bool:
        self.heartbeat_calls += 1
        return self.current

    def next_inbox(self, lease: DrainLease) -> InboxWork | None:
        if self.inbox:
            return self.inbox.pop(0)
        return None

    def complete_inbox(self, lease: DrainLease, delivery_id: UUID, outcome: str) -> bool:
        if self.completed is None:
            self.completed = []
        if not self.current:
            return False
        self.completed.append((delivery_id, outcome))
        return True

    def next_run(self, lease: DrainLease) -> RunWork | None:
        if self.runs:
            return self.runs.pop(0)
        return None

    def finish(self, lease: DrainLease, complete: bool) -> bool:
        if self.finish_calls is None:
            self.finish_calls = []
        self.finish_calls.append(complete)
        return self.current and (
            not complete or (self.finish_result and not self.inbox and not self.runs)
        )


def _request() -> WakeInput:
    return WakeInput(
        attempt_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        generation="42",
        wake_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        reason="pending_inbox",
    )


def _worker(
    store: FakeWakeStore,
    *,
    process_inbox=None,
    run_workflow=None,
    **limits,
) -> BoundedWakeWorker:
    async def default_inbox(item: InboxWork) -> str:
        return "processed"

    async def default_workflow(run: RunWork, max_jobs: int, max_seconds: float) -> object:
        return SimpleNamespace(run_id=run.run_id, jobs_started=1, stop_reason="waiting")

    return BoundedWakeWorker(
        store,
        process_inbox or default_inbox,
        run_workflow or default_workflow,
        heartbeat_interval=limits.pop("heartbeat_interval", 0.02),
        **limits,
    )


def test_busy_duplicate_wake_does_not_report_completion() -> None:
    store = FakeWakeStore(claim_busy=True)

    receipt = asyncio.run(_worker(store).run(_request()))

    assert receipt == WakeDrainReceipt("busy", stop_reason="waiting")
    assert not receipt.completed
    assert store.finish_calls is None


def test_consumes_only_approved_inbox_outcome_and_finishes_authoritatively() -> None:
    delivery_id = UUID(int=10)
    store = FakeWakeStore(inbox=[InboxWork(delivery_id, "issue_comment", {}, None)])

    receipt = asyncio.run(_worker(store).run(_request()))

    assert receipt.completed
    assert receipt.deliveries_consumed == 1
    assert store.completed == [(delivery_id, "processed")]
    assert store.finish_calls == [True]


def test_unrecognized_inbox_outcome_is_left_unconsumed_and_partial() -> None:
    store = FakeWakeStore(inbox=[InboxWork(UUID(int=10), "issue_comment", {}, None)])

    async def process(item: InboxWork) -> str:
        return "maybe"

    receipt = asyncio.run(_worker(store, process_inbox=process).run(_request()))

    assert receipt.status == "partial"
    assert receipt.deliveries_consumed == 0
    assert store.completed is None
    assert store.finish_calls == [False]


def test_fence_loss_during_processing_cancels_before_inbox_completion() -> None:
    store = FakeWakeStore(inbox=[InboxWork(UUID(int=10), "issue_comment", {}, None)])
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def process(item: InboxWork) -> str:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def scenario() -> WakeDrainReceipt:
        worker = _worker(store, process_inbox=process)
        task = asyncio.create_task(worker.run(_request()))
        await started.wait()
        store.current = False
        return await task

    receipt = asyncio.run(scenario())

    assert receipt.status == "stale"
    assert cancelled.is_set()
    assert store.completed is None
    assert store.finish_calls is None


def test_workflow_receipt_must_match_selected_run_and_job_bound() -> None:
    store = FakeWakeStore(runs=[RunWork(UUID(int=20), UUID(int=30))])

    async def invalid_workflow(run: RunWork, max_jobs: int, max_seconds: float) -> object:
        return SimpleNamespace(run_id="other-run", jobs_started=999, stop_reason="idle")

    receipt = asyncio.run(_worker(store, run_workflow=invalid_workflow).run(_request()))

    assert receipt.status == "partial"
    assert receipt.runs_started == 0
    assert receipt.jobs_started == 0
    assert store.finish_calls == [False]


def test_run_and_delivery_caps_bound_one_wake_attempt() -> None:
    store = FakeWakeStore(
        inbox=[InboxWork(UUID(int=100 + index), "event", {}, None) for index in range(3)],
        runs=[RunWork(UUID(int=200), UUID(int=300 + index)) for index in range(3)],
    )
    workflow_args: list[tuple[int, float]] = []

    async def workflow(run: RunWork, max_jobs: int, max_seconds: float) -> object:
        workflow_args.append((max_jobs, max_seconds))
        return SimpleNamespace(run_id=run.run_id, jobs_started=max_jobs, stop_reason="job_limit")

    receipt = asyncio.run(
        _worker(
            store,
            run_workflow=workflow,
            max_deliveries=2,
            max_runs=2,
            max_jobs=3,
        ).run(_request())
    )

    assert receipt.status == "partial"  # Pending work remains after this bounded pass.
    assert receipt.deliveries_consumed == 2
    assert receipt.runs_started == 2
    assert receipt.jobs_started == 6
    assert len(workflow_args) == 2
    assert all(jobs == 3 for jobs, _ in workflow_args)
    assert len(store.inbox or []) == 1
    assert len(store.runs or []) == 1


def test_authoritative_finish_false_does_not_claim_drain() -> None:
    store = FakeWakeStore(finish_result=False)

    receipt = asyncio.run(_worker(store).run(_request()))

    assert receipt.status == "partial"
    assert not receipt.completed
    assert store.finish_calls == [True]


def test_worker_callback_failure_marks_only_partial() -> None:
    store = FakeWakeStore(inbox=[InboxWork(UUID(int=10), "event", {}, None)])

    async def failed(item: InboxWork) -> str:
        raise RuntimeError("private details must not escape")

    receipt = asyncio.run(_worker(store, process_inbox=failed).run(_request()))

    assert receipt.status == "partial"
    assert receipt.deliveries_consumed == 0
    assert store.completed is None
    assert store.finish_calls == [False]


def test_scheduler_keeps_one_strongly_referenced_task_and_closes_it() -> None:
    async def scenario() -> tuple[bool, bool, bool]:
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def drain(request: WakeInput) -> object:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        scheduler = WakeWorkerScheduler(drain)
        first = scheduler.schedule(_request())
        await started.wait()
        second = scheduler.schedule(_request())
        await scheduler.aclose()
        return first, second, cancelled.is_set()

    assert asyncio.run(scenario()) == (True, False, True)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_deliveries": 21},
        {"max_runs": 6},
        {"max_jobs": 21},
        {"max_seconds": 301},
        {"heartbeat_interval": 61},
    ],
)
def test_worker_rejects_unbounded_limits(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        _worker(FakeWakeStore(), **kwargs)


@dataclass
class FakeStatusStore(FakeWakeStore):
    statuses: list | None = None
    status_completed: list | None = None
    status_proof: bool = True

    def next_status(self, lease):
        return self.statuses.pop(0) if self.statuses else None

    def complete_status(self, lease, work):
        if not self.current or not self.status_proof:
            return False
        if self.status_completed is None:
            self.status_completed = []
        self.status_completed.append(work)
        return True

    def finish(self, lease, complete):
        result = super().finish(lease, complete)
        return result and (not complete or not self.statuses)


def _status_work(number=1):
    from continuum_api.runtime.wake_worker_store import StatusWork

    return StatusWork(UUID(int=number), UUID(int=number + 20), number, bytes([number]) * 32, False)


def test_status_failures_are_isolated_and_confirmed_only_with_database_proof():
    first, second = _status_work(1), _status_work(2)
    store = FakeStatusStore(statuses=[first, second], runs=[RunWork(UUID(int=1), UUID(int=2))])
    seen = []

    async def publish(work):
        seen.append(work)
        if work == first:
            raise RuntimeError("synthetic_unknown_comment_effect")
        return True

    receipt = asyncio.run(_worker(store, publish_status=publish).run(_request()))

    assert seen == [first, second]
    assert receipt.completed and receipt.jobs_started == 1
    assert receipt.status_comments_attempted == 2
    assert receipt.status_comments_confirmed == 1
    assert store.status_completed == [second]


def test_status_item_bound_does_not_claim_an_undrained_queue():
    store = FakeStatusStore(statuses=[_status_work(1), _status_work(2)])

    async def publish(work):
        return True

    receipt = asyncio.run(
        _worker(store, publish_status=publish, max_status_comments=1).run(_request())
    )

    assert receipt.status == "partial"
    assert receipt.status_comments_attempted == receipt.status_comments_confirmed == 1
    assert len(store.statuses) == 1


def test_unproved_status_confirmation_cannot_report_a_successful_drain():
    store = FakeStatusStore(statuses=[_status_work()], status_proof=False)

    async def publish(work):
        return True

    receipt = asyncio.run(_worker(store, publish_status=publish).run(_request()))

    assert not receipt.completed and receipt.status_comments_confirmed == 0
    assert store.finish_calls == [False]


def test_status_lookup_cancellation_obeys_outer_worker_deadline():
    store = FakeStatusStore(statuses=[_status_work()])
    cancelled = []

    async def publish(work):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(True)

    receipt = asyncio.run(_worker(store, publish_status=publish, max_seconds=0.05).run(_request()))

    assert receipt.status == "partial" and receipt.stop_reason == "time_limit"
    assert cancelled == [True] and store.finish_calls == [False]


def test_status_comment_bound_is_validated():
    with pytest.raises(ValueError, match="status_comments"):
        _worker(FakeWakeStore(), max_status_comments=11)
