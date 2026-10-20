"""Owner-scoped, privacy-filtered run snapshots and event pages."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine

from continuum_api.runtime.persistence import (
    Principal,
    RunRecord,
    RunStatus,
    RuntimeEvent,
    RuntimeRepository,
)

MAX_POSTGRES_BIGINT = 9_223_372_036_854_775_807
_CURSOR_PATTERN = re.compile(r"^(?:0|[1-9][0-9]{0,18})$")
_EVENT_TYPE_PATTERN = re.compile(r"^[a-z][a-z0-9_.]{0,127}$")
_ID_KEYS = frozenset({"attempt_id", "job_id", "step_id", "next_job_id"})
_COUNT_KEYS = frozenset({"failure_count", "generation", "cancelled_jobs", "invalidated_attempts"})
_DATE_KEYS = frozenset({"retry_at", "capacity_resume_at"})
_STATUS_VALUES = frozenset(
    {
        "queued",
        "running",
        "waiting_approval",
        "waiting_credentials",
        "waiting_capacity",
        "needs_attention",
        "succeeded",
        "failed",
        "cancelled",
        "expired",
    }
)
_FAILURE_VALUES = frozenset(
    {
        "invalid_input",
        "missing_credential",
        "transient_read",
        "unsupported_environment",
        "resource_exhaustion",
        "model_response_lost",
        "unknown_effect",
        "remote_conflict",
        "corrupt_artifact",
    }
)


class RunSnapshotResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: UUID
    status: RunStatus
    revision: int
    created_at: datetime
    deadline_at: datetime | None
    event_watermark: str
    content_expires_at: datetime | None = None


class EventData(BaseModel):
    """Only metadata the timeline is allowed to expose from stored event JSON."""

    model_config = ConfigDict(extra="forbid")

    attempt_id: UUID | None = None
    job_id: UUID | None = None
    step_id: UUID | None = None
    next_job_id: UUID | None = None
    failure_count: str | None = None
    generation: str | None = None
    cancelled_jobs: str | None = None
    invalidated_attempts: str | None = None
    retry_at: datetime | None = None
    capacity_resume_at: datetime | None = None
    target_run_status: RunStatus | None = None
    status: RunStatus | None = None
    failure_class: (
        Literal[
            "invalid_input",
            "missing_credential",
            "transient_read",
            "unsupported_environment",
            "resource_exhaustion",
            "model_response_lost",
            "unknown_effect",
            "remote_conflict",
            "corrupt_artifact",
        ]
        | None
    ) = None
    replay_policy: Literal["replay_safe", "attention_on_unknown"] | None = None
    retry_scheduled: bool | None = None


class TimelineEventResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    run_id: UUID
    sequence: str
    type: str
    occurred_at: datetime
    data: EventData


class EventPageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: UUID
    after_sequence: str
    events: list[TimelineEventResponse]
    next_cursor: str | None
    high_watermark: str
    history_gap: bool


class HistoryResetPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: Literal["cursor_ahead", "history_gap"]
    high_watermark: str


class EventStreamErrorPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: Literal["authentication_required", "event_stream_unavailable"]
    retryable: bool


@dataclass(frozen=True, slots=True)
class RunEventPage:
    """Read result captured with run metadata in one repeatable-read snapshot."""

    run: RunRecord | None
    events: tuple[RuntimeEvent, ...]
    next_cursor: int | None
    history_gap: bool


def parse_event_cursor(value: str | None) -> int:
    """Parse a nonnegative PostgreSQL int8 cursor represented as decimal text."""
    if value is None:
        return 0
    if not _CURSOR_PATTERN.fullmatch(value):
        raise ValueError("event_cursor_invalid")
    parsed = int(value)
    if parsed > MAX_POSTGRES_BIGINT:
        raise ValueError("event_cursor_invalid")
    return parsed


def run_snapshot_response(run: RunRecord) -> RunSnapshotResponse:
    return RunSnapshotResponse(
        run_id=run.run_id,
        status=run.status,
        revision=run.revision,
        created_at=run.created_at,
        deadline_at=run.deadline_at,
        content_expires_at=run.content_expires_at,
        event_watermark=str(run.event_watermark),
    )


def timeline_event_response(event: RuntimeEvent) -> TimelineEventResponse:
    event_type = (
        event.event_type if _EVENT_TYPE_PATTERN.fullmatch(event.event_type) else "runtime.event"
    )
    return TimelineEventResponse(
        event_id=event.event_id,
        run_id=event.run_id,
        sequence=str(event.sequence),
        type=event_type,
        occurred_at=event.created_at,
        data=project_event_data(event.payload),
    )


def project_event_data(payload: dict[str, Any]) -> EventData:
    """Expose only bounded event metadata with fixed keys and validated values."""
    projected: dict[str, Any] = {}
    for key in _ID_KEYS:
        value = payload.get(key)
        if isinstance(value, str):
            try:
                projected[key] = str(UUID(value))
            except ValueError:
                pass
    for key in _COUNT_KEYS:
        value = payload.get(key)
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and 0 <= value <= MAX_POSTGRES_BIGINT
        ):
            projected[key] = str(value)
    for key in _DATE_KEYS:
        value = payload.get(key)
        if isinstance(value, str) and len(value) <= 40:
            try:
                datetime.fromisoformat(value)
            except ValueError:
                continue
            projected[key] = value
    for key in ("target_run_status", "status"):
        value = payload.get(key)
        if isinstance(value, str) and value in _STATUS_VALUES:
            projected[key] = value
    failure_class = payload.get("failure_class")
    if isinstance(failure_class, str) and failure_class in _FAILURE_VALUES:
        projected["failure_class"] = failure_class
    replay_policy = payload.get("replay_policy")
    if isinstance(replay_policy, str) and replay_policy in {
        "replay_safe",
        "attention_on_unknown",
    }:
        projected["replay_policy"] = replay_policy
    retry_scheduled = payload.get("retry_scheduled")
    if isinstance(retry_scheduled, bool):
        projected["retry_scheduled"] = retry_scheduled
    return EventData.model_validate(projected)


def read_run(engine: Engine, principal: Principal, run_id: UUID) -> RunRecord | None:
    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="REPEATABLE READ")
        with connection.begin():
            repository = RuntimeRepository(connection, principal)
            return repository.get_run(run_id)


def read_event_page(
    engine: Engine,
    principal: Principal,
    run_id: UUID,
    *,
    after_sequence: int,
    limit: int,
) -> RunEventPage:
    if not 1 <= limit <= 100:
        raise ValueError("runtime_event_limit_invalid")
    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="REPEATABLE READ")
        with connection.begin():
            repository = RuntimeRepository(connection, principal)
            run = repository.get_run(run_id)
            if run is None:
                return RunEventPage(None, (), None, False)
            selected = repository.list_events(
                run_id, after_sequence=after_sequence, limit=limit + 1
            )
            has_more = len(selected) > limit
            events = selected[:limit]
            next_cursor = events[-1].sequence if has_more and events else None
            expected = after_sequence + 1
            history_gap = False
            for event in events:
                if event.sequence != expected:
                    history_gap = True
                    break
                expected += 1
            if not events and run.event_watermark > after_sequence:
                history_gap = True
            return RunEventPage(run, events, next_cursor, history_gap)


def event_page_response(run_id: UUID, after_sequence: int, page: RunEventPage) -> EventPageResponse:
    if page.run is None:
        raise ValueError("runtime_run_not_found")
    return EventPageResponse(
        run_id=run_id,
        after_sequence=str(after_sequence),
        events=[timeline_event_response(event) for event in page.events],
        next_cursor=None if page.next_cursor is None else str(page.next_cursor),
        high_watermark=str(page.run.event_watermark),
        history_gap=page.history_gap,
    )
