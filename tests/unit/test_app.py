"""HTTP and OpenAPI contracts for the initial health endpoints."""

from __future__ import annotations

import asyncio
from uuid import UUID

import httpx

from continuum_api.app import app


def get(path: str) -> httpx.Response:
    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
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
