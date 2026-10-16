from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import UUID

import httpx

from continuum_api.app import create_app
from continuum_api.runtime.persistence import Principal


def _client(application) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://test")


def test_review_decision_requires_origin_and_csrf_guard(tmp_path: Path) -> None:
    application = create_app(
        tmp_path,
        principal_resolver=lambda _request: Principal(UUID(int=1)),
        csrf_guard=lambda _request: True,
    )

    async def send() -> httpx.Response:
        async with _client(application) as client:
            return await client.post(
                "/api/v1/approvals/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/decision",
                headers={
                    "Idempotency-Key": "route-unit-1",
                    "X-CSRF-Token": "provided-by-session-boundary",
                },
                json={"decision": "reject", "manifest_sha256": "a" * 64},
            )

    response = asyncio.run(send())
    assert response.status_code == 403
    assert response.json()["code"] == "csrf_failed"


def test_decision_body_is_capped_before_json_parsing(tmp_path: Path) -> None:
    application = create_app(
        tmp_path,
        principal_resolver=lambda _request: Principal(UUID(int=1)),
        csrf_guard=lambda _request: True,
    )

    async def chunks() -> AsyncIterator[bytes]:
        yield b'{"decision":"changes","feedback":"'
        yield b"x" * (64 * 1024)
        yield b'","manifest_sha256":"' + b"a" * 64 + b'"}'

    async def send() -> httpx.Response:
        async with _client(application) as client:
            return await client.post(
                "/api/v1/approvals/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa/decision",
                headers={
                    "Content-Type": "application/json",
                    "Origin": "http://test",
                    "X-CSRF-Token": "provided-by-session-boundary",
                    "Idempotency-Key": "route-unit-2",
                },
                content=chunks(),
            )

    response = asyncio.run(send())
    assert response.status_code == 413
    assert response.json()["code"] == "request_too_large"
