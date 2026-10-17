"""Read-only authenticated run snapshot, event page, and replayable SSE routes."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Annotated, Any, cast
from uuid import UUID

import anyio
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import ValidationError
from sqlalchemy import Engine

from continuum_api.api_models import ErrorResponse
from continuum_api.runtime.database import create_runtime_engine
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.run_queries import (
    EventPageResponse,
    EventStreamErrorPayload,
    HistoryResetPayload,
    RunEventPage,
    RunSnapshotResponse,
    event_page_response,
    parse_event_cursor,
    read_event_page,
    read_run,
    run_snapshot_response,
)
from continuum_api.settings import get_settings

PrincipalResolver = Callable[[Request], Principal | Awaitable[Principal]]
_MAX_PAGE_SIZE = 100
_DEFAULT_PAGE_SIZE = 50
_POLL_SECONDS = 1.0
_HEARTBEAT_SECONDS = 15.0
_MAX_STREAM_SECONDS = 60.0


class _StreamLimiter:
    """Bound active SSE bodies to two per principal in this API process."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._active: dict[UUID, int] = {}

    async def acquire(self, owner_id: UUID) -> bool:
        async with self._lock:
            active = self._active.get(owner_id, 0)
            if active >= 2:
                return False
            self._active[owner_id] = active + 1
            return True

    async def release(self, owner_id: UUID) -> None:
        async with self._lock:
            active = self._active.get(owner_id, 0)
            if active <= 1:
                self._active.pop(owner_id, None)
            else:
                self._active[owner_id] = active - 1


def get_runtime_engine(request: Request) -> Engine:
    configured = cast(Engine | None, request.app.state.runtime_engine)
    if configured is not None:
        return configured
    with request.app.state.runtime_engine_lock:
        configured = cast(Engine | None, request.app.state.runtime_engine)
        if configured is not None:
            return configured
        try:
            database_url = get_settings().runtime_database_url
            if database_url is None or not database_url.get_secret_value().strip():
                raise ValueError("runtime_database_url_missing")
            configured = create_runtime_engine(database_url.get_secret_value())
        except (ValueError, ValidationError):
            raise HTTPException(status_code=503, detail="runtime_database_unavailable") from None
        request.app.state.runtime_engine = configured
        request.app.state.owns_runtime_engine = True
        return configured


def create_run_router(principal_resolver: PrincipalResolver | None) -> APIRouter:
    router = APIRouter(prefix="/api/v1/runs", tags=["runs"])

    async def authenticated_principal(request: Request) -> Principal:
        if principal_resolver is None:
            raise HTTPException(status_code=401, detail="authentication_required")
        try:
            resolved = principal_resolver(request)
            principal = await resolved if inspect.isawaitable(resolved) else resolved
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=401, detail="authentication_required") from None
        if not isinstance(principal, Principal) or not isinstance(principal.account_id, UUID):
            raise HTTPException(status_code=401, detail="authentication_required")
        return principal

    @router.get(
        "/{run_id}",
        response_model=RunSnapshotResponse,
        responses={
            422: {"model": ErrorResponse},
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    def get_run_snapshot(
        run_id: UUID,
        response: Response,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
        engine: Engine = Depends(get_runtime_engine),  # noqa: B008
    ) -> RunSnapshotResponse:
        response.headers["Cache-Control"] = "private, no-store"
        try:
            run = read_run(engine, principal, run_id)
        except Exception:
            raise HTTPException(status_code=503, detail="runtime_database_unavailable") from None
        if run is None:
            raise HTTPException(status_code=404, detail="not_found")
        return run_snapshot_response(run)

    @router.get(
        "/{run_id}/events",
        response_model=EventPageResponse,
        responses={
            200: {
                "content": {"text/event-stream": {"schema": {"type": "string"}}},
                "description": "JSON event page or replayable server-sent event stream.",
            },
            422: {"model": ErrorResponse},
            400: {"model": ErrorResponse},
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            429: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    async def get_run_events(
        run_id: UUID,
        request: Request,
        response: Response,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
        engine: Engine = Depends(get_runtime_engine),  # noqa: B008
        after: Annotated[str | None, Query(json_schema_extra={"maxLength": 19})] = None,
        last_event_id: Annotated[
            str | None,
            Header(alias="Last-Event-ID", json_schema_extra={"maxLength": 19}),
        ] = None,
        limit: Annotated[int, Query(ge=1, le=_MAX_PAGE_SIZE)] = _DEFAULT_PAGE_SIZE,
    ) -> Any:
        is_sse = "text/event-stream" in request.headers.get("accept", "").lower()
        try:
            after_sequence = _select_cursor(
                after,
                last_event_id,
                is_sse=is_sse,
            )
        except ValueError:
            raise HTTPException(status_code=400, detail="event_cursor_invalid") from None

        if not is_sse:
            response.headers["Cache-Control"] = "private, no-store"
            try:
                page = await anyio.to_thread.run_sync(
                    lambda: read_event_page(
                        engine,
                        principal,
                        run_id,
                        after_sequence=after_sequence,
                        limit=limit,
                    )
                )
            except Exception:
                raise HTTPException(
                    status_code=503, detail="runtime_database_unavailable"
                ) from None
            if page.run is None:
                raise HTTPException(status_code=404, detail="not_found")
            if after_sequence > page.run.event_watermark:
                raise HTTPException(status_code=400, detail="event_cursor_ahead")
            return event_page_response(run_id, after_sequence, page)

        limiter: _StreamLimiter = request.app.state.sse_stream_limiter
        if not await limiter.acquire(principal.account_id):
            raise HTTPException(status_code=429, detail="event_stream_limit_reached")
        try:
            initial = await anyio.to_thread.run_sync(
                lambda: read_event_page(
                    engine,
                    principal,
                    run_id,
                    after_sequence=after_sequence,
                    limit=limit,
                )
            )
        except BaseException as error:
            await asyncio.shield(limiter.release(principal.account_id))
            if isinstance(error, Exception):
                raise HTTPException(
                    status_code=503, detail="runtime_database_unavailable"
                ) from None
            raise
        if initial.run is None:
            await limiter.release(principal.account_id)
            raise HTTPException(status_code=404, detail="not_found")

        async def stream() -> Any:
            cursor = after_sequence
            page = initial
            last_heartbeat = time.monotonic()
            stream_started = last_heartbeat
            try:
                yield ": connected\n\n"
                while True:
                    if time.monotonic() - stream_started >= _MAX_STREAM_SECONDS:
                        return
                    try:
                        current_principal = await authenticated_principal(request)
                    except HTTPException:
                        yield _sse_control(
                            "stream.error",
                            EventStreamErrorPayload(
                                code="authentication_required", retryable=False
                            ).model_dump_json(),
                        )
                        return
                    if current_principal.account_id != principal.account_id:
                        yield _sse_control(
                            "stream.error",
                            EventStreamErrorPayload(
                                code="authentication_required", retryable=False
                            ).model_dump_json(),
                        )
                        return
                    if page.run is None:
                        yield _sse_control(
                            "stream.error",
                            EventStreamErrorPayload(
                                code="event_stream_unavailable", retryable=True
                            ).model_dump_json(),
                        )
                        return
                    if cursor > page.run.event_watermark:
                        payload = HistoryResetPayload(
                            reason="cursor_ahead",
                            high_watermark=str(page.run.event_watermark),
                        )
                        yield _sse_control("history.reset", payload.model_dump_json())
                        return
                    if page.history_gap:
                        payload = HistoryResetPayload(
                            reason="history_gap",
                            high_watermark=str(page.run.event_watermark),
                        )
                        yield _sse_control("history.reset", payload.model_dump_json())
                        return

                    for event in page.events:
                        try:
                            event_principal = await authenticated_principal(request)
                        except HTTPException:
                            yield _sse_control(
                                "stream.error",
                                EventStreamErrorPayload(
                                    code="authentication_required", retryable=False
                                ).model_dump_json(),
                            )
                            return
                        if event_principal.account_id != principal.account_id:
                            yield _sse_control(
                                "stream.error",
                                EventStreamErrorPayload(
                                    code="authentication_required", retryable=False
                                ).model_dump_json(),
                            )
                            return
                        response = event_page_response(
                            run_id, cursor, RunEventPage(page.run, (event,), None, False)
                        ).events[0]
                        yield _sse_event(response.sequence, "run.event", response.model_dump_json())
                        cursor = event.sequence
                    if page.events:
                        last_heartbeat = time.monotonic()
                    elif time.monotonic() - last_heartbeat >= _HEARTBEAT_SECONDS:
                        yield ": heartbeat\n\n"
                        last_heartbeat = time.monotonic()

                    if await request.is_disconnected():
                        return
                    if not page.events:
                        await asyncio.sleep(_POLL_SECONDS)
                    try:
                        page = await anyio.to_thread.run_sync(
                            partial(
                                read_event_page,
                                engine,
                                principal,
                                run_id,
                                after_sequence=cursor,
                                limit=limit,
                            )
                        )
                    except Exception:
                        yield _sse_control(
                            "stream.error",
                            EventStreamErrorPayload(
                                code="event_stream_unavailable", retryable=True
                            ).model_dump_json(),
                        )
                        return
            finally:
                await limiter.release(principal.account_id)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "private, no-store, no-cache, no-transform",
                "X-Accel-Buffering": "no",
            },
        )

    return router


def _select_cursor(query: str | None, last_event_id: str | None, *, is_sse: bool) -> int:
    """Honor EventSource's advancing Last-Event-ID over its fixed initial URL."""
    if is_sse and last_event_id is not None:
        return parse_event_cursor(last_event_id)
    query_cursor = parse_event_cursor(query)
    if last_event_id is None:
        return query_cursor
    header_cursor = parse_event_cursor(last_event_id)
    if query_cursor != header_cursor:
        raise ValueError("event_cursor_conflict")
    return header_cursor


def _sse_event(sequence: str, event_name: str, data: str) -> str:
    return f"id: {sequence}\nevent: {event_name}\ndata: {data}\n\n"


def _sse_control(event_name: str, data: str) -> str:
    return f"event: {event_name}\ndata: {data}\n\n"


def install_sse_openapi(application: Any) -> None:
    """Document the stable SSE event names and their data contracts."""
    original_openapi = application.openapi

    def custom_openapi() -> dict[str, Any]:
        existing_schema = cast(dict[str, Any] | None, application.openapi_schema)
        if existing_schema:
            return existing_schema
        schema = cast(dict[str, Any], original_openapi())
        components = schema.setdefault("components", {}).setdefault("schemas", {})
        components["HistoryResetPayload"] = HistoryResetPayload.model_json_schema()
        components["EventStreamErrorPayload"] = EventStreamErrorPayload.model_json_schema()
        response = schema["paths"]["/api/v1/runs/{run_id}/events"]["get"]["responses"]["200"]
        event_stream = response.setdefault("content", {}).setdefault(
            "text/event-stream", {"schema": {"type": "string"}}
        )
        event_stream["x-event-payloads"] = {
            "run.event": {"$ref": "#/components/schemas/TimelineEventResponse"},
            "history.reset": {"$ref": "#/components/schemas/HistoryResetPayload"},
            "stream.error": {"$ref": "#/components/schemas/EventStreamErrorPayload"},
        }
        application.openapi_schema = schema
        return schema

    application.openapi = custom_openapi
