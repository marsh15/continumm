"""HTTP boundary tests for the disabled-by-default T06 wake receiver."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, Request, Response
from starlette.middleware.base import RequestResponseEndpoint

from continuum_api.wake import (
    MAX_WAKE_REQUEST_BYTES,
    ReceiptResult,
    StatusResult,
    WakeInput,
    WakeReceiverConfig,
    WakeStoreUnavailable,
    create_wake_receiver_engine,
    create_wake_router,
)

TOKEN = "local-wake-backend-token-for-tests-000000000000"
ATTEMPT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
WAKE_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


@dataclass
class FakeStore:
    receipt_id: str = WAKE_ID
    outcome: str = "accepted"
    drain_status: str = "worker_not_started"
    failure: bool = False
    inputs: list[WakeInput] | None = None

    def accept(self, request: WakeInput) -> ReceiptResult:
        if self.failure:
            raise WakeStoreUnavailable("redacted")
        if self.inputs is None:
            self.inputs = []
        self.inputs.append(request)
        return ReceiptResult(
            outcome=self.outcome,
            receipt_id=self.receipt_id,
            accepted_at=datetime(2026, 10, 11, tzinfo=UTC),
            drain_status=self.drain_status,
            last_successful_drain_at=None,
        )

    def status(self) -> StatusResult:
        if self.failure:
            raise WakeStoreUnavailable("redacted")
        return StatusResult(None, "worker_not_started", None)


def _app(config_provider: Any) -> FastAPI:
    application = FastAPI()
    application.include_router(create_wake_router(config_provider))

    @application.middleware("http")
    async def request_id(request: Request, call_next: RequestResponseEndpoint) -> Response:
        request.state.request_id = str(uuid4())
        response = await call_next(request)
        response.headers["x-request-id"] = request.state.request_id
        return response

    return application


def _send(app: FastAPI, method: str, path: str, **kwargs: Any) -> httpx.Response:
    async def request() -> httpx.Response:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            return await client.request(method, path, **kwargs)

    return asyncio.run(request())


def _valid_payload() -> dict[str, str]:
    return {
        "attempt_id": ATTEMPT_ID,
        "generation": "42",
        "wake_id": WAKE_ID,
        "reason": "pending_inbox",
    }


def _auth() -> dict[str, str]:
    return {"authorization": f"Bearer {TOKEN}"}


def test_committed_receipt_contract_does_not_claim_a_drain() -> None:
    store = FakeStore()
    config = WakeReceiverConfig(TOKEN, store)
    response = _send(
        _app(lambda: config),
        "POST",
        "/internal/v1/wake",
        headers={**_auth(), "content-type": "application/json"},
        json=_valid_payload(),
    )

    assert response.status_code == 202
    assert response.json() == {
        "accepted": True,
        "receipt_id": WAKE_ID,
        "status": "worker_not_started",
        "drain_status": "not_started",
        "last_successful_drain_at": None,
    }
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-request-id"]
    assert store.inputs is not None
    assert store.inputs[0].model_dump() == _valid_payload()
    assert "wake" not in response.headers.get("content-type", "")


def test_duplicate_committed_receipt_remains_idempotent() -> None:
    store = FakeStore(outcome="duplicate")
    response = _send(
        _app(lambda: WakeReceiverConfig(TOKEN, store)),
        "POST",
        "/internal/v1/wake",
        headers={**_auth(), "content-type": "application/json"},
        json=_valid_payload(),
    )

    assert response.status_code == 202
    assert response.json()["receipt_id"] == WAKE_ID
    assert response.json()["last_successful_drain_at"] is None


def test_authentication_precedes_body_parsing() -> None:
    store = FakeStore()
    response = _send(
        _app(lambda: WakeReceiverConfig(TOKEN, store)),
        "POST",
        "/internal/v1/wake",
        headers={"content-type": "application/json"},
        content=b"not-json",
    )

    assert response.status_code == 401
    assert response.json()["code"] == "unauthorized"
    assert store.inputs is None


def test_missing_receiver_config_is_safely_disabled() -> None:
    response = _send(_app(lambda: None), "GET", "/internal/v1/wake/status")

    assert response.status_code == 503
    assert response.json()["code"] == "worker_not_started"
    assert response.json()["request_id"] == response.headers["x-request-id"]
    assert response.headers["cache-control"] == "no-store"
    assert "token" not in response.text


def test_oversized_body_is_rejected_before_json_parsing() -> None:
    store = FakeStore()
    response = _send(
        _app(lambda: WakeReceiverConfig(TOKEN, store)),
        "POST",
        "/internal/v1/wake",
        headers={**_auth(), "content-type": "application/json"},
        content=b"{" + b" " * MAX_WAKE_REQUEST_BYTES + b"}",
    )

    assert response.status_code == 413
    assert store.inputs is None


@pytest.mark.parametrize(
    ("store", "expected_status", "expected_code"),
    [
        (FakeStore(outcome="stale"), 409, "wake_not_accepted"),
        (FakeStore(receipt_id=ATTEMPT_ID), 503, "wake_receipt_invalid"),
        (FakeStore(drain_status="drained"), 503, "wake_receipt_invalid"),
        (FakeStore(failure=True), 503, "wake_receiver_unavailable"),
    ],
)
def test_noncommitted_or_mismatched_receipt_is_never_accepted(
    store: FakeStore, expected_status: int, expected_code: str
) -> None:
    response = _send(
        _app(lambda: WakeReceiverConfig(TOKEN, store)),
        "POST",
        "/internal/v1/wake",
        headers={**_auth(), "content-type": "application/json"},
        json=_valid_payload(),
    )

    assert response.status_code == expected_status
    assert response.json()["code"] == expected_code


def test_status_auth_is_required_and_never_claims_drain_success() -> None:
    store = FakeStore()
    app = _app(lambda: WakeReceiverConfig(TOKEN, store))
    denied = _send(app, "GET", "/internal/v1/wake/status")
    allowed = _send(app, "GET", "/internal/v1/wake/status", headers=_auth())

    assert denied.status_code == 401
    assert allowed.status_code == 200
    assert allowed.json() == {
        "worker_status": "worker_not_started",
        "drain_status": "not_started",
        "last_successful_drain_at": None,
        "receipt_accepted_at": None,
    }


def test_database_configuration_errors_are_redacted() -> None:
    secret = "receiver-password-must-not-appear"
    with pytest.raises(ValueError) as error:
        create_wake_receiver_engine(
            f"postgresql://user:{secret}@db.example.test/continuum?sslmode=disable",
            production=True,
        )

    assert str(error.value) == "wake_receiver_database_configuration_invalid"
    assert secret not in str(error.value)


def test_local_cleartext_database_url_must_be_loopback() -> None:
    with pytest.raises(ValueError, match="wake_receiver_database_configuration_invalid"):
        create_wake_receiver_engine(
            "postgresql://user:password@db.example.test/continuum?sslmode=disable",
            production=False,
        )


def test_nonlocal_receiver_url_requires_an_explicit_tls_mode() -> None:
    with pytest.raises(ValueError, match="wake_receiver_database_configuration_invalid"):
        create_wake_receiver_engine(
            "postgresql://user:password@db.example.test/continuum",
            production=False,
        )


def test_generation_above_postgres_bigint_is_rejected_before_store_call() -> None:
    store = FakeStore()
    payload = _valid_payload()
    payload["generation"] = "9223372036854775808"
    response = _send(
        _app(lambda: WakeReceiverConfig(TOKEN, store)),
        "POST",
        "/internal/v1/wake",
        headers={**_auth(), "content-type": "application/json"},
        json=payload,
    )

    assert response.status_code == 400
    assert store.inputs is None
