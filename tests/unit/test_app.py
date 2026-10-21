"""HTTP and OpenAPI contracts for the initial health endpoints."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from pathlib import Path
from uuid import UUID

import httpx
import pytest

from continuum_api.app import app, create_app
from continuum_api.runtime.persistence import Principal
from continuum_api.settings import get_settings


def get(path: str, *, application=app) -> httpx.Response:
    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(path)

    return asyncio.run(send())


def test_live_endpoint_reports_process_liveness() -> None:
    response = get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "checks": None}
    UUID(response.headers["x-request-id"])


def test_ready_endpoint_reports_database_check_as_unavailable() -> None:
    response = get("/health/ready")

    request_id = response.headers["x-request-id"]
    assert response.status_code == 503
    assert response.json() == {
        "code": "not_ready",
        "message": "Database readiness has not been verified.",
        "retryable": True,
        "request_id": request_id,
    }
    UUID(request_id)


def test_openapi_documents_health_and_error_response_models() -> None:
    schema = app.openapi()

    live_schema = schema["paths"]["/health/live"]["get"]["responses"]["200"]
    ready_responses = schema["paths"]["/health/ready"]["get"]["responses"]
    assert live_schema["content"]["application/json"]["schema"]["$ref"] == (
        "#/components/schemas/HealthResponse"
    )
    assert ready_responses["200"]["content"]["application/json"]["schema"]["$ref"] == (
        "#/components/schemas/HealthResponse"
    )
    assert ready_responses["503"]["content"]["application/json"]["schema"]["$ref"] == (
        "#/components/schemas/ErrorResponse"
    )
    assert set(schema["components"]["schemas"]["ErrorResponse"]["properties"]) == {
        "code",
        "message",
        "retryable",
        "request_id",
    }
    run_snapshot = schema["paths"]["/api/v1/runs/{run_id}"]["get"]
    snapshot_schema = run_snapshot["responses"]["200"]["content"]["application/json"]["schema"]
    assert snapshot_schema["$ref"] == "#/components/schemas/RunSnapshotResponse"
    status_schema = schema["components"]["schemas"]["RunSnapshotResponse"]["properties"]["status"]
    assert status_schema["$ref"] == "#/components/schemas/RunStatus"
    events_response = schema["paths"]["/api/v1/runs/{run_id}/events"]["get"]["responses"]["200"]
    assert "application/json" in events_response["content"]
    assert "text/event-stream" in events_response["content"]
    assert (
        events_response["content"]["text/event-stream"]["x-event-payloads"]["history.reset"]["$ref"]
        == "#/components/schemas/HistoryResetPayload"
    )
    assert (
        events_response["content"]["application/json"]["schema"]["$ref"]
        == "#/components/schemas/EventPageResponse"
    )
    event_data = schema["components"]["schemas"]["EventData"]
    assert event_data["additionalProperties"] is False
    assert "observation" not in event_data["properties"]
    assert (
        events_response["content"]["text/event-stream"]["x-event-payloads"]["run.event"]["$ref"]
        == "#/components/schemas/TimelineEventResponse"
    )
    assert (
        schema["paths"]["/api/v1/runs/{run_id}"]["get"]["responses"]["422"]["content"][
            "application/json"
        ]["schema"]["$ref"]
        == "#/components/schemas/ErrorResponse"
    )
    events_parameters = schema["paths"]["/api/v1/runs/{run_id}/events"]["get"]["parameters"]
    assert any(parameter["name"] == "Last-Event-ID" for parameter in events_parameters)
    assert (
        schema["paths"]["/api/v1/runs/{run_id}/events"]["get"]["responses"]["422"]["content"][
            "application/json"
        ]["schema"]["$ref"]
        == "#/components/schemas/ErrorResponse"
    )


def test_run_api_is_closed_without_an_injected_identity_resolver() -> None:
    application = create_app()

    response = get("/api/v1/runs/00000000-0000-4000-8000-000000000001", application=application)

    assert response.status_code == 401
    assert response.json()["code"] == "authentication_required"


def test_run_api_reports_missing_runtime_database_as_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv("CONTINUUM_RUNTIME_DATABASE_URL", "")
    application = create_app(principal_resolver=lambda _request: Principal(UUID(int=1)))

    try:
        response = get("/api/v1/runs/00000000-0000-4000-8000-000000000001", application=application)
    finally:
        get_settings.cache_clear()

    assert response.status_code == 503
    assert response.json()["code"] == "runtime_database_unavailable"


def test_run_routes_can_be_imported_before_app_construction() -> None:
    project_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import continuum_api.runtime.run_routes; import continuum_api.app",
        ],
        cwd=project_root,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )

    assert result.returncode == 0, "standalone route import failed"
