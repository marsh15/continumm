"""Bounded async orchestration for durable wake receipts.

The worker owns no database policy. Its store methods are synchronous, fenced
operations, and its callbacks own the inbox transition and workflow execution.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from continuum_api.runtime.wake_worker_store import DrainLease, InboxWork, RunWork, StatusWork
from continuum_api.wake import WakeInput

logger = logging.getLogger(__name__)

MAX_WAKE_DELIVERIES = 20
MAX_WAKE_RUNS = 5
MAX_WAKE_JOBS = 20
MAX_WAKE_STATUS_COMMENTS = 10
MAX_WAKE_SECONDS = 300.0
DEFAULT_WAKE_DELIVERIES = 5
DEFAULT_WAKE_RUNS = 2
DEFAULT_WAKE_JOBS = 2
DEFAULT_WAKE_SECONDS = 60.0
DEFAULT_WAKE_HEARTBEAT_SECONDS = 10.0
_INBOX_OUTCOMES = frozenset({"processed", "inert", "rejected"})
_RUN_STOP_REASONS = frozenset({"idle", "waiting", "job_limit", "time_limit"})


class WakeWorkerStore(Protocol):
    """Synchronous store contract; each mutating method must enforce the lease fence."""

    def claim(self, wake_id: UUID, attempt_id: UUID, generation: int) -> DrainLease | None: ...

    def heartbeat(self, lease: DrainLease) -> bool: ...

    def next_inbox(self, lease: DrainLease) -> InboxWork | None: ...

    def complete_inbox(self, lease: DrainLease, delivery_id: UUID, outcome: str) -> bool: ...

    def next_run(self, lease: DrainLease) -> RunWork | None: ...

    def next_status(self, lease: DrainLease) -> StatusWork | None: ...

    def complete_status(self, lease: DrainLease, work: StatusWork) -> bool: ...

    def finish(self, lease: DrainLease, complete: bool) -> bool: ...


@dataclass(frozen=True, slots=True)
class WakeDrainReceipt:
    """Truthful outcome of one bounded attempt to drain a wake."""

    status: str
    deliveries_consumed: int = 0
    runs_started: int = 0
    jobs_started: int = 0
    stop_reason: str = "waiting"
    status_comments_attempted: int = 0
    status_comments_confirmed: int = 0

    @property
    def completed(self) -> bool:
        return self.status == "drained"


InboxProcessor = Callable[[InboxWork], Awaitable[str]]
WorkflowRunner = Callable[[RunWork, int, float], Awaitable[object]]
StatusPublisher = Callable[[StatusWork], Awaitable[bool]]


class BoundedWakeWorker:
    """Claim and process a wake under one renewable lease and strict work limits."""

    def __init__(
        self,
        store: WakeWorkerStore,
        process_inbox: InboxProcessor,
        run_workflow: WorkflowRunner,
        *,
        publish_status: StatusPublisher | None = None,
        max_status_comments: int = 3,
        max_deliveries: int = DEFAULT_WAKE_DELIVERIES,
        max_runs: int = DEFAULT_WAKE_RUNS,
        max_jobs: int = DEFAULT_WAKE_JOBS,
        max_seconds: float = DEFAULT_WAKE_SECONDS,
        heartbeat_interval: float = DEFAULT_WAKE_HEARTBEAT_SECONDS,
    ) -> None:
        _validate_bound("status_comments", max_status_comments, 1, MAX_WAKE_STATUS_COMMENTS)
        if publish_status is not None and not _is_async_callable(publish_status):
            raise ValueError("wake_worker_callback_must_be_async")
        self._publish_status = publish_status
        self._max_status_comments = max_status_comments
        _validate_bound("deliveries", max_deliveries, 1, MAX_WAKE_DELIVERIES)
        _validate_bound("runs", max_runs, 1, MAX_WAKE_RUNS)
        _validate_bound("jobs", max_jobs, 1, MAX_WAKE_JOBS)
        _validate_seconds("max_seconds", max_seconds, MAX_WAKE_SECONDS)
        _validate_seconds("heartbeat_interval", heartbeat_interval, 60.0)
        if heartbeat_interval >= max_seconds:
            raise ValueError("wake_heartbeat_interval_invalid")
        if not _is_async_callable(process_inbox) or not _is_async_callable(run_workflow):
            raise ValueError("wake_worker_callback_must_be_async")
        self._store = store
        self._process_inbox = process_inbox
        self._run_workflow = run_workflow
        self._max_deliveries = max_deliveries
        self._max_runs = max_runs
        self._max_jobs = max_jobs
        self._max_seconds = float(max_seconds)
        self._heartbeat_interval = float(heartbeat_interval)

    async def run(self, request: WakeInput) -> WakeDrainReceipt:
        """Attempt one bounded drain. No callback means no work or drain claim."""
        if not isinstance(request, WakeInput):
            raise ValueError("wake_input_invalid")
        try:
            lease = await asyncio.to_thread(
                self._store.claim,
                UUID(request.wake_id),
                UUID(request.attempt_id),
                int(request.generation),
            )
        except Exception as error:
            _log_failure("wake_claim", error)
            return WakeDrainReceipt("partial", stop_reason="waiting")
        if lease is None:
            return WakeDrainReceipt("busy", stop_reason="waiting")

        started_at = time.monotonic()
        work_task = asyncio.create_task(self._drain(lease, started_at), name="wake-drain")
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(lease), name="wake-heartbeat")
        try:
            async with asyncio.timeout(self._max_seconds):
                done, _ = await asyncio.wait(
                    {work_task, heartbeat_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if heartbeat_task in done:
                    # A heartbeat loop only exits early when the lease was lost.
                    work_task.cancel()
                    await _await_cancelled(work_task)
                    return WakeDrainReceipt("stale", stop_reason="waiting")
                receipt = await work_task
                return receipt
        except TimeoutError:
            work_task.cancel()
            await _await_cancelled(work_task)
            await self._try_finish(lease, complete=False)
            return WakeDrainReceipt("partial", stop_reason="time_limit")
        except asyncio.CancelledError:
            work_task.cancel()
            await _await_cancelled(work_task)
            await self._try_finish(lease, complete=False)
            raise
        except Exception as error:
            _log_failure("wake_drain", error)
            await self._try_finish(lease, complete=False)
            return WakeDrainReceipt("partial", stop_reason="waiting")
        finally:
            heartbeat_task.cancel()
            await _await_cancelled(heartbeat_task)

    async def _heartbeat_loop(self, lease: DrainLease) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                current = await asyncio.to_thread(self._store.heartbeat, lease)
            except Exception as error:
                _log_failure("wake_heartbeat", error)
                current = False
            if not current:
                return

    async def _drain(self, lease: DrainLease, started_at: float) -> WakeDrainReceipt:
        consumed = 0
        runs_started = 0
        jobs_started = 0
        stop_reason = "idle"
        seen_run_ids: set[str] = set()
        status_attempted = 0
        status_confirmed = 0

        try:
            for _ in range(self._max_deliveries):
                item = await asyncio.to_thread(self._store.next_inbox, lease)
                if item is None:
                    break
                outcome = await self._process_inbox(item)
                if outcome not in _INBOX_OUTCOMES:
                    raise _UnknownCallbackResult("wake_inbox_outcome_invalid")
                if not await self._is_current(lease):
                    return WakeDrainReceipt("stale", consumed, runs_started, jobs_started)
                completed = await asyncio.to_thread(
                    self._store.complete_inbox, lease, item.delivery_id, outcome
                )
                if not completed:
                    return WakeDrainReceipt("stale", consumed, runs_started, jobs_started)
                consumed += 1
            else:
                stop_reason = "job_limit"

            for _ in range(self._max_runs):
                run = await asyncio.to_thread(self._store.next_run, lease)
                if run is None:
                    break
                run_id = str(run.run_id)
                if run_id in seen_run_ids:
                    # The store must advance its cursor; stop to avoid an unbounded loop.
                    stop_reason = "waiting"
                    break
                seen_run_ids.add(run_id)
                if not await self._is_current(lease):
                    return WakeDrainReceipt("stale", consumed, runs_started, jobs_started)
                remaining = max(0.001, self._max_seconds - (time.monotonic() - started_at))
                receipt = await self._run_workflow(run, self._max_jobs, remaining)
                verified_jobs, verified_stop = _validate_runner_receipt(
                    receipt, run_id, self._max_jobs
                )
                runs_started += 1
                jobs_started += verified_jobs
                stop_reason = verified_stop
            else:
                stop_reason = "job_limit"

            if self._publish_status is not None:
                for _ in range(self._max_status_comments):
                    work = await asyncio.to_thread(self._store.next_status, lease)
                    if work is None:
                        break
                    if not await self._is_current(lease):
                        return WakeDrainReceipt("stale", consumed, runs_started, jobs_started)
                    status_attempted += 1
                    try:
                        published = await self._publish_status(work)
                    except Exception as error:
                        # The store persisted a retry cooldown before external I/O.
                        # A comment failure must not undo committed workflow progress.
                        _log_failure("wake_status_comment", error)
                        continue
                    if published is not True:
                        continue
                    if not await asyncio.to_thread(self._store.complete_status, lease, work):
                        await self._try_finish(lease, complete=False)
                        return WakeDrainReceipt(
                            "partial",
                            consumed,
                            runs_started,
                            jobs_started,
                            "waiting",
                            status_attempted,
                            status_confirmed,
                        )
                    status_confirmed += 1

            if not await self._is_current(lease):
                return WakeDrainReceipt("stale", consumed, runs_started, jobs_started)
            finished = await asyncio.to_thread(self._store.finish, lease, True)
            if finished:
                return WakeDrainReceipt(
                    "drained",
                    consumed,
                    runs_started,
                    jobs_started,
                    stop_reason,
                    status_attempted,
                    status_confirmed,
                )
            return WakeDrainReceipt(
                "partial",
                consumed,
                runs_started,
                jobs_started,
                "waiting",
                status_attempted,
                status_confirmed,
            )
        except asyncio.CancelledError:
            # The caller retains control of cancellation; no successful watermark is written.
            raise
        except Exception as error:
            _log_failure("wake_callback", error)
            await self._try_finish(lease, complete=False)
            return WakeDrainReceipt(
                "partial",
                consumed,
                runs_started,
                jobs_started,
                "waiting",
                status_attempted,
                status_confirmed,
            )

    async def _is_current(self, lease: DrainLease) -> bool:
        try:
            return await asyncio.to_thread(self._store.heartbeat, lease)
        except Exception as error:
            _log_failure("wake_fence_check", error)
            return False

    async def _try_finish(self, lease: DrainLease, *, complete: bool) -> None:
        try:
            await asyncio.to_thread(self._store.finish, lease, complete)
        except Exception as error:
            _log_failure("wake_finish", error)


class _UnknownCallbackResult(RuntimeError):
    pass


class WakeWorkerScheduler:
    """One in-process wake task at a time; distributed safety remains in the store."""

    def __init__(self, drain: Callable[[WakeInput], Coroutine[Any, Any, object]]) -> None:
        if not _is_async_callable(drain):
            raise ValueError("wake_scheduler_callback_must_be_async")
        self._drain = drain
        self._tasks: set[asyncio.Task[object]] = set()
        self._closed = False

    def schedule(self, request: WakeInput) -> bool:
        """Start a durable wake attempt without waiting for its result."""
        if self._closed or not isinstance(request, WakeInput):
            return False
        self._tasks = {task for task in self._tasks if not task.done()}
        if self._tasks:
            return False
        task = asyncio.create_task(self._drain(request), name="wake-scheduled-drain")
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return True

    async def aclose(self, *, cleanup_timeout: float = 95.0) -> None:
        """Cancel active work and wait a bounded time for callback cleanup."""
        if (
            isinstance(cleanup_timeout, bool)
            or not isinstance(cleanup_timeout, (int, float))
            or not math.isfinite(cleanup_timeout)
            or cleanup_timeout <= 0
            or cleanup_timeout > 95
        ):
            raise ValueError("wake_scheduler_cleanup_timeout_invalid")
        self._closed = True
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if not tasks:
            return
        try:
            async with asyncio.timeout(cleanup_timeout):
                await asyncio.gather(*tasks, return_exceptions=True)
        except TimeoutError:
            logger.warning("wake_scheduler_cleanup_timeout")

    def _task_done(self, task: asyncio.Task[object]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            _log_failure("wake_scheduler", error)


def _validate_runner_receipt(receipt: object, run_id: str, max_jobs: int) -> tuple[int, str]:
    actual_run_id = getattr(receipt, "run_id", None)
    jobs_started = getattr(receipt, "jobs_started", None)
    stop_reason = getattr(receipt, "stop_reason", None)
    if (
        str(actual_run_id) != run_id
        or isinstance(jobs_started, bool)
        or not isinstance(jobs_started, int)
        or not 0 <= jobs_started <= max_jobs
        or str(stop_reason) not in _RUN_STOP_REASONS
    ):
        raise _UnknownCallbackResult("wake_workflow_receipt_invalid")
    return jobs_started, str(stop_reason)


def _validate_bound(name: str, value: int, minimum: int, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"wake_{name}_bound_invalid")


def _validate_seconds(name: str, value: float, maximum: float) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0 < value <= maximum
    ):
        raise ValueError(f"wake_{name}_bound_invalid")


def _is_async_callable(value: object) -> bool:
    return callable(value) and inspect.iscoroutinefunction(value)


async def _await_cancelled(task: asyncio.Task[object] | asyncio.Task[None]) -> None:
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


def _log_failure(operation: str, error: BaseException) -> None:
    logger.warning("%s_failed error_type=%s", operation, type(error).__name__)
