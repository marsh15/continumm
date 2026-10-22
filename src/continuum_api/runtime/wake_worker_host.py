"""Application-lifetime scheduler for explicitly enabled wake execution."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from continuum_api.runtime.wake_worker import WakeWorkerScheduler
from continuum_api.settings import get_settings
from continuum_api.wake import WakeInput


class ConfiguredWakeWorkerScheduler:
    """Construct no hosted clients until an authenticated committed wake arrives."""

    def __init__(
        self,
        settings_provider: Callable[[], Any] = get_settings,
        resources_factory: Callable[[Any], Awaitable[Any]] | None = None,
    ) -> None:
        self._settings_provider = settings_provider
        self._resources_factory = resources_factory
        self._scheduler = WakeWorkerScheduler(self._drain)

    def schedule(self, request: WakeInput) -> bool:
        settings = self._settings_provider()
        if getattr(settings, "enable_wake_worker", False) is not True:
            return False
        return self._scheduler.schedule(request)

    async def _drain(self, request: WakeInput) -> Any:
        settings = self._settings_provider()
        if getattr(settings, "enable_wake_worker", False) is not True:
            return None
        factory = self._resources_factory
        if factory is None:
            from continuum_api.runtime.wake_worker_resources import build_wake_worker_resources

            factory = build_wake_worker_resources
        resources = await factory(settings)
        try:
            return await resources.worker.run(request)
        finally:
            await resources.aclose()

    async def aclose(self) -> None:
        await self._scheduler.aclose()
