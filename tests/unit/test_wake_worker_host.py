"""Hosted wake scheduling is explicit, bounded and owned by app lifetime."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from continuum_api.app import create_app
from continuum_api.runtime.wake_worker_host import ConfiguredWakeWorkerScheduler
from continuum_api.wake import WakeInput


def _request():
    return WakeInput(
        attempt_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        wake_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        generation="1",
        reason="pending_inbox",
    )


def test_disabled_worker_does_not_construct_resources():
    factory = AsyncMock()
    host = ConfiguredWakeWorkerScheduler(lambda: SimpleNamespace(enable_wake_worker=False), factory)
    assert host.schedule(_request()) is False
    factory.assert_not_called()
    asyncio.run(host.aclose())


def test_scheduled_work_owns_resources_until_completion():
    async def check():
        started, release = asyncio.Event(), asyncio.Event()
        events = []

        class Worker:
            async def run(self, request):
                assert request == _request()
                events.append("work")
                started.set()
                await release.wait()

        class Resources:
            worker = Worker()

            async def aclose(self):
                events.append("close")

        factory = AsyncMock(return_value=Resources())
        host = ConfiguredWakeWorkerScheduler(
            lambda: SimpleNamespace(enable_wake_worker=True), factory
        )
        assert host.schedule(_request()) is True
        await asyncio.wait_for(started.wait(), 5)
        assert host.schedule(_request()) is False
        assert events == ["work"]
        release.set()
        await asyncio.sleep(0)
        await host.aclose()
        assert events == ["work", "close"]
        assert factory.await_count == 1

    asyncio.run(check())


def test_shutdown_cancels_work_and_closes_resources():
    async def check():
        started = asyncio.Event()
        events = []

        class Worker:
            async def run(self, request):
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    events.append("cancel")

        resources = SimpleNamespace(worker=Worker(), aclose=AsyncMock())
        host = ConfiguredWakeWorkerScheduler(
            lambda: SimpleNamespace(enable_wake_worker=True),
            AsyncMock(return_value=resources),
        )
        host.schedule(_request())
        await asyncio.wait_for(started.wait(), 5)
        await host.aclose()
        assert events == ["cancel"]
        resources.aclose.assert_awaited_once()

    asyncio.run(check())


def test_app_lifespan_closes_injected_wake_scheduler():
    async def check():
        scheduler = SimpleNamespace(schedule=lambda _: False, aclose=AsyncMock())
        app = create_app(wake_worker_scheduler=scheduler)
        async with app.router.lifespan_context(app):
            assert app.state.wake_worker_scheduler is scheduler
        scheduler.aclose.assert_awaited_once()

    asyncio.run(check())
