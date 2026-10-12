"""Pure contract tests for owner-scoped run timeline projection."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from continuum_api.runtime.run_queries import (
    MAX_POSTGRES_BIGINT,
    EventData,
    parse_event_cursor,
    project_event_data,
)
from continuum_api.runtime.run_routes import _select_cursor


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, 0), ("0", 0), ("17", 17), (str(MAX_POSTGRES_BIGINT), MAX_POSTGRES_BIGINT)],
)
def test_event_cursor_is_decimal_text_with_postgres_int8_bounds(
    value: str | None, expected: int
) -> None:
    assert parse_event_cursor(value) == expected


@pytest.mark.parametrize("value", ["", "-1", "+1", "01", "1.0", "1e2", "9" * 20])
def test_invalid_event_cursors_are_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="event_cursor_invalid"):
        parse_event_cursor(value)


def test_event_payload_projection_exposes_only_validated_metadata() -> None:
    identifier = uuid4()
    projected = project_event_data(
        {
            "attempt_id": str(identifier),
            "generation": 9_007_199_254_741_000,
            "failure_class": "unknown_effect",
            "retry_scheduled": False,
            "observation": {"provider_token": "do-not-return"},
            "result_sha256": "private-digest",
            "operation_key": "provider-operation",
            "unreviewed_field": "arbitrary input",
        }
    )

    assert projected.model_dump(mode="json", exclude_none=True) == {
        "attempt_id": str(identifier),
        "generation": "9007199254741000",
        "failure_class": "unknown_effect",
        "retry_scheduled": False,
    }
    with pytest.raises(ValidationError):
        EventData.model_validate({"provider_token": "do-not-return"})


def test_sse_reconnect_header_overrides_the_fixed_initial_query_cursor() -> None:
    assert _select_cursor("1", "3", is_sse=True) == 3
    with pytest.raises(ValueError, match="event_cursor_conflict"):
        _select_cursor("1", "3", is_sse=False)
