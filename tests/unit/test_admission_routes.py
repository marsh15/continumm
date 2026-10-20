"""HTTP admission stays closed before service setup unless explicitly enabled."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from continuum_api.runtime.admission import (
    AdmissionError,
    AdmissionResult,
    AdmissionService,
    IssueSource,
)
from continuum_api.runtime.admission_routes import create_admission_router
from continuum_api.runtime.persistence import Principal


def test_public_admission_flag_blocks_service_factory(monkeypatch) -> None:
    owner = uuid4()
    calls = 0

    def service_factory(request, principal):
        nonlocal calls
        calls += 1
        raise AssertionError("disabled admission must stop before service construction")

    monkeypatch.setattr(
        "continuum_api.runtime.admission_routes.get_settings",
        lambda: type("SettingsStub", (), {"enable_public_admission": False})(),
    )
    app = FastAPI()
    app.include_router(
        create_admission_router(
            lambda request: Principal(owner), service_factory, lambda request: True
        )
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/runs",
            headers={
                "Origin": "https://console.example",
                "X-CSRF-Token": "valid-token",
                "Idempotency-Key": "request-key-00000001",
            },
            json={
                "source": {
                    "kind": "free_text",
                    "registration_id": 1,
                    "request_id": str(uuid4()),
                    "task": "A bounded task.",
                }
            },
        )
    assert response.status_code == 503
    assert response.json()["detail"] == "public_admission_disabled"
    assert calls == 0


def test_admission_service_is_closed_by_default_before_database_or_github() -> None:
    class ForbiddenGithub:
        async def current(self, *_args):
            raise AssertionError("disabled admission must not contact GitHub")

    service = AdmissionService(None, Principal(uuid4()), ForbiddenGithub(), None)
    with pytest.raises(AdmissionError, match="public_admission_disabled"):
        import asyncio

        asyncio.run(
            service.admit(
                IssueSource(
                    kind="issue",
                    registration_id=1,
                    issue_number=1,
                    request_id=uuid4(),
                    task="A bounded task.",
                ),
                "request-key-00000003",
            )
        )


def test_admission_request_schema_is_documented() -> None:
    app = FastAPI()
    app.include_router(
        create_admission_router(lambda request: Principal(uuid4()), lambda *_: None, lambda _: True)
    )
    schema = app.openapi()
    operation = schema["paths"]["/api/v1/runs"]["post"]
    assert "requestBody" in operation
    assert "202" in operation["responses"]
    header = next(item for item in operation["parameters"] if item["name"] == "Idempotency-Key")
    assert header["required"] is True
    request_schema = operation["requestBody"]["content"]["application/json"]["schema"]

    def assert_refs_resolve(value) -> None:
        if isinstance(value, dict):
            assert "$ref" not in value
            for nested in value.values():
                assert_refs_resolve(nested)
        elif isinstance(value, list):
            for nested in value:
                assert_refs_resolve(nested)

    assert_refs_resolve(request_schema)


def test_enabled_admission_returns_typed_pinned_snapshot(monkeypatch) -> None:
    owner = uuid4()
    calls: list[tuple[object, str]] = []
    run_id = uuid4()
    now = datetime.now(UTC)

    class Service:
        async def admit(self, source, idempotency_key):
            calls.append((source, idempotency_key))
            return AdmissionResult(
                run_id=run_id,
                status="waiting_credentials",
                created_at=now,
                deadline_at=now + timedelta(days=7),
                profile_id="python-pytest",
                profile_version="1",
                provider="openai",
                model_id="gpt-5.4-mini-2026-03-17",
                source_sha="a" * 40,
                issue_id=789,
                issue_sha="b" * 64,
            )

    monkeypatch.setattr(
        "continuum_api.runtime.admission_routes.get_settings",
        lambda: type("SettingsStub", (), {"enable_public_admission": True})(),
    )
    app = FastAPI()
    app.include_router(
        create_admission_router(
            lambda request: Principal(owner), lambda request, principal: Service(), lambda _: True
        )
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/runs",
            headers={
                "Origin": "https://console.example",
                "X-CSRF-Token": "valid-token",
                "Idempotency-Key": "request-key-00000001",
            },
            json={
                "source": {
                    "kind": "issue",
                    "registration_id": 1,
                    "issue_number": 7,
                    "request_id": str(uuid4()),
                    "task": "Fix the bounded issue.",
                }
            },
        )
    assert response.status_code == 202
    payload = response.json()
    assert payload["run_id"] == str(run_id)
    assert payload["status"] == "waiting_credentials"
    assert payload["profile_id"] == "python-pytest"
    assert payload["issue_id"] == 789
    assert payload["issue_sha"] == "b" * 64
    assert payload["model_id"] == "gpt-5.4-mini-2026-03-17"
    assert calls and calls[0][1] == "request-key-00000001"


def test_csrf_required_idempotency_and_bounded_strict_body(monkeypatch) -> None:
    calls = 0
    factory_calls = 0

    class Service:
        async def admit(self, source, idempotency_key):
            nonlocal calls
            calls += 1
            raise AssertionError("invalid requests must stop before admission")

    monkeypatch.setattr(
        "continuum_api.runtime.admission_routes.get_settings",
        lambda: type("SettingsStub", (), {"enable_public_admission": True})(),
    )
    app = FastAPI()

    def service_factory(request, principal):
        nonlocal factory_calls
        factory_calls += 1
        return Service()

    app.include_router(
        create_admission_router(
            lambda request: Principal(uuid4()),
            service_factory,
            lambda request: request.headers.get("x-csrf-token") == "valid-token",
        )
    )
    base_headers = {
        "Origin": "https://console.example",
        "X-CSRF-Token": "invalid-token",
        "Idempotency-Key": "request-key-00000002",
    }
    with TestClient(app) as client:
        csrf = client.post("/api/v1/runs", headers=base_headers, json={})
        missing_key = client.post(
            "/api/v1/runs",
            headers={"Origin": base_headers["Origin"], "X-CSRF-Token": "valid-token"},
            json={},
        )
        unknown = client.post(
            "/api/v1/runs",
            headers={**base_headers, "X-CSRF-Token": "valid-token"},
            json={"source": {"kind": "free_text", "unknown": True}},
        )
        oversized = client.post(
            "/api/v1/runs",
            headers={**base_headers, "X-CSRF-Token": "valid-token"},
            content=b"x" * 65_537,
        )
    assert csrf.status_code == 403
    assert missing_key.status_code == 400
    assert unknown.status_code == 422
    assert oversized.status_code == 413
    assert calls == 0
    assert factory_calls == 0
