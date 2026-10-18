"""Bounded, single-run dispatch for trusted runtime activity handlers."""

from __future__ import annotations

import asyncio
import inspect
import math
import time
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from continuum_api.runtime.leases import (
    HEARTBEAT_INTERVAL_SECONDS,
    LEASE_DURATION_SECONDS,
    ExecutionLease,
    QueueService,
)
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.runtime.scheduler import FailureClass, RuntimeRecoveryService

MAX_RUNNER_JOBS = 100
MAX_RUNNER_SECONDS = 300.0
ActivityHandler = Callable[[ExecutionLease], Coroutine[Any, Any, object]]


class RunnerStopReason(StrEnum):
    IDLE = "idle"
    WAITING = "waiting"
    JOB_LIMIT = "job_limit"
    TIME_LIMIT = "time_limit"


class UnknownActivity(RuntimeError):
    """A due activity has no trusted handler registered."""


class AdmittedRunNotFound(RuntimeError):
    """The admitted run is not visible to this principal."""

    code = "runtime_run_not_found"


@dataclass(frozen=True, slots=True)
class RunnerReceipt:
    run_id: UUID
    jobs_started: int
    stop_reason: RunnerStopReason


class ActivityRunner:
    """Run registered async handlers for one admitted run, one lease at a time.

    Handlers own durable result transitions. The runner only claims, heartbeats,
    and dispatches; it never retries a handler or interprets its return value.
    """

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        handlers: Mapping[str, ActivityHandler],
        *,
        lease_owner: str,
        queue: QueueService | None = None,
        heartbeat_interval_seconds: float = HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        if not handlers or any(
            not name.strip() or not _is_async_callable(handler)
            for name, handler in handlers.items()
        ):
            raise ValueError("runtime_activity_registry_invalid")
        if (
            isinstance(heartbeat_interval_seconds, bool)
            or not isinstance(heartbeat_interval_seconds, (int, float))
            or not math.isfinite(heartbeat_interval_seconds)
            or heartbeat_interval_seconds <= 0
            or heartbeat_interval_seconds >= LEASE_DURATION_SECONDS
        ):
            raise ValueError("runtime_heartbeat_interval_invalid")
        self._engine = engine
        self._principal = principal
        self._handlers = dict(handlers)
        self._lease_owner = lease_owner
        self._queue = queue or QueueService(engine, principal)
        self._heartbeat_interval = heartbeat_interval_seconds

    async def run_admitted(
        self, run_id: UUID, *, max_jobs: int, max_seconds: float
    ) -> RunnerReceipt:
        if not isinstance(run_id, UUID):
            raise ValueError("runtime_admitted_run_id_invalid")
        if (
            isinstance(max_jobs, bool)
            or not isinstance(max_jobs, int)
            or not 1 <= max_jobs <= MAX_RUNNER_JOBS
        ):
            raise ValueError("runtime_runner_job_bound_invalid")
        if (
            isinstance(max_seconds, bool)
            or not isinstance(max_seconds, (int, float))
            or not math.isfinite(max_seconds)
            or not 0 < max_seconds <= MAX_RUNNER_SECONDS
        ):
            raise ValueError("runtime_runner_time_bound_invalid")

        started_at = time.monotonic()
        jobs_started = 0
        while jobs_started < max_jobs:
            remaining = max_seconds - (time.monotonic() - started_at)
            if remaining <= 0:
                return RunnerReceipt(run_id, jobs_started, RunnerStopReason.TIME_LIMIT)

            activity, has_waiting_work = await asyncio.to_thread(self._next_due_activity, run_id)
            if activity is None:
                reason = RunnerStopReason.WAITING if has_waiting_work else RunnerStopReason.IDLE
                return RunnerReceipt(run_id, jobs_started, reason)
            if activity not in self._handlers:
                raise UnknownActivity(f"runtime_activity_unregistered:{activity}")

            lease = await asyncio.to_thread(self._queue.claim_one, self._lease_owner, run_id=run_id)
            if lease is None:
                # Another worker may have claimed the candidate between inspection and claim.
                continue

            jobs_started += 1
            claimed_activity = await asyncio.to_thread(self._activity_for_lease, lease)
            if claimed_activity not in self._handlers:
                await asyncio.to_thread(
                    RuntimeRecoveryService(self._engine, self._principal).fail_attempt,
                    lease=lease,
                    failure_class=FailureClass.UNKNOWN_EFFECT,
                )
                raise UnknownActivity(f"runtime_activity_unregistered:{claimed_activity}")
            activity = claimed_activity
            remaining = max_seconds - (time.monotonic() - started_at)
            if remaining <= 0:
                return RunnerReceipt(run_id, jobs_started, RunnerStopReason.TIME_LIMIT)
            try:
                async with asyncio.timeout(remaining):
                    await self._dispatch_with_heartbeat(lease, self._handlers[activity])
            except TimeoutError:
                return RunnerReceipt(run_id, jobs_started, RunnerStopReason.TIME_LIMIT)

        return RunnerReceipt(run_id, jobs_started, RunnerStopReason.JOB_LIMIT)

    async def _dispatch_with_heartbeat(
        self, lease: ExecutionLease, handler: ActivityHandler
    ) -> object:
        handler_awaitable = handler(lease)
        if not inspect.isawaitable(handler_awaitable):
            raise TypeError("runtime_activity_handler_must_be_async")
        handler_task = asyncio.create_task(handler_awaitable)
        heartbeat_error: Exception | None = None

        async def heartbeat() -> None:
            nonlocal heartbeat_error
            while True:
                await asyncio.sleep(self._heartbeat_interval)
                try:
                    await asyncio.to_thread(self._queue.heartbeat, lease)
                except Exception as error:
                    if handler_task.done():
                        return
                    heartbeat_error = error
                    handler_task.cancel()
                    return

        heartbeat_task = asyncio.create_task(heartbeat())
        try:
            result = await handler_task
            if heartbeat_error is not None:
                if not await asyncio.to_thread(self._durable_result_exists, lease):
                    raise heartbeat_error
            return result
        except asyncio.CancelledError:
            if heartbeat_error is not None:
                if await asyncio.to_thread(self._durable_result_exists, lease):
                    return None
                raise heartbeat_error from None
            raise
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)

    def _next_due_activity(self, run_id: UUID) -> tuple[str | None, bool]:
        """Return the next activity and whether the run has work waiting for later."""
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            owner_id = self._principal.account_id
            activity = connection.execute(
                text(
                    """
                    SELECT step.activity
                    FROM runtime_private.runs AS run
                    JOIN runtime_private.steps AS step
                      ON step.owner_id=run.owner_id AND step.run_id=run.run_id
                    JOIN runtime_private.jobs AS job
                      ON job.owner_id=step.owner_id AND job.run_id=step.run_id
                     AND job.step_id=step.step_id
                    WHERE run.owner_id=:owner_id AND run.run_id=:run_id
                      AND run.status IN ('queued', 'running')
                      AND (run.deadline_at IS NULL OR run.deadline_at > clock_timestamp())
                      AND (
                        (step.status='queued' AND job.status='queued'
                         AND job.available_at <= clock_timestamp())
                        OR (run.status='running' AND step.status='running'
                            AND job.status='leased'
                            AND job.lease_expires_at <= clock_timestamp()
                            AND job.replay_policy='replay_safe')
                      )
                    ORDER BY step.step_id, job.job_id
                    LIMIT 1
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ).scalar_one_or_none()
            if activity is not None:
                return str(activity), False

            waiting = connection.execute(
                text(
                    """
                    SELECT run.status IN ('waiting_approval', 'waiting_credentials',
                                          'waiting_capacity', 'needs_attention')
                           OR EXISTS (
                               SELECT 1 FROM runtime_private.steps AS step
                               JOIN runtime_private.jobs AS job
                                 ON job.owner_id=step.owner_id AND job.run_id=step.run_id
                                AND job.step_id=step.step_id
                               WHERE step.owner_id=run.owner_id AND step.run_id=run.run_id
                                 AND step.status IN ('queued', 'running')
                                 AND job.status IN ('queued', 'leased')
                           )
                    FROM runtime_private.runs AS run
                    WHERE run.owner_id=:owner_id AND run.run_id=:run_id
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ).scalar_one_or_none()
            if waiting is None:
                # Missing and cross-principal runs intentionally share one response.
                raise AdmittedRunNotFound("runtime_run_not_found")
            return None, bool(waiting)

    def _activity_for_lease(self, lease: ExecutionLease) -> str | None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            return connection.execute(
                text(
                    "SELECT activity FROM runtime_private.steps "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
                ),
                {
                    "owner_id": lease.owner_id,
                    "run_id": lease.run_id,
                    "step_id": lease.step_id,
                },
            ).scalar_one_or_none()

    def _durable_result_exists(self, lease: ExecutionLease) -> bool:
        """Recognize a result committed just before a heartbeat lost its lease race."""
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            return bool(
                connection.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1 FROM runtime_private.accepted_results
                            WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id
                              AND generation=:generation AND attempt_id=:attempt_id
                        ) OR EXISTS (
                            SELECT 1
                            FROM runtime_private.jobs AS job
                            JOIN runtime_private.attempts AS attempt
                              ON attempt.owner_id=job.owner_id AND attempt.run_id=job.run_id
                             AND attempt.job_id=job.job_id
                            JOIN runtime_private.runs AS run
                              ON run.owner_id=job.owner_id AND run.run_id=job.run_id
                            WHERE job.owner_id=:owner_id AND job.run_id=:run_id
                              AND job.job_id=:job_id AND job.generation=:generation
                              AND job.status='succeeded' AND attempt.attempt_id=:attempt_id
                              AND attempt.status='succeeded' AND run.status='succeeded'
                        )
                        """
                    ),
                    {
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                        "job_id": lease.job_id,
                        "generation": lease.generation,
                        "attempt_id": lease.attempt_id,
                    },
                ).scalar_one()
            )


def _is_async_callable(handler: object) -> bool:
    call_method = inspect.getattr_static(type(handler), "__call__", None)
    return callable(handler) and (
        inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(call_method)
    )
