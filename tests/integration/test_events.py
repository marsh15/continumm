"""PostgreSQL proof for owner-scoped run snapshots, replay, and SSE cursors."""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlencode
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url

from continuum_api.app import create_app
from continuum_api.runtime.persistence import EventActor, Principal, RuntimeRepository

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t12_test"
LATEST_REVISION = "20261011_07"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)


def test_run_snapshot_event_pages_and_sse_reconnect_are_owner_scoped() -> None:
    database_url = os.environ.get("CONTINUUM_T12_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T12_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T12 integration requires loopback PostgreSQL named continuum_t12_test")

    admin_engine = create_engine(
        url.set(drivername="postgresql+psycopg"), hide_parameters=True, pool_size=8, max_overflow=0
    )
    runtime_password = secrets.token_urlsafe(32)
    runtime_url = url.set(
        drivername="postgresql+psycopg", username=RUNTIME_ROLE, password=runtime_password
    )
    runtime_engine: Engine | None = None
    migration_applied = False

    try:
        _reset_database(admin_engine, runtime_password)
        _assert_postgres_17(admin_engine)
        _run_migration("upgrade", LATEST_REVISION, database_url)
        migration_applied = True
        runtime_engine = create_engine(
            runtime_url, hide_parameters=True, pool_size=2, max_overflow=0
        )
        owner_id, other_owner_id = uuid4(), uuid4()
        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO runtime_private.users (user_id, github_user_id) "
                    "VALUES (:owner_id, 810001), (:other_owner_id, 810002)"
                ),
                {"owner_id": owner_id, "other_owner_id": other_owner_id},
            )

        secret_sentinel = "private-provider-observation-sentinel"
        primary_run = _seed_run(
            runtime_engine,
            owner_id,
            [
                (1, "run.admitted", {"source": "task", "attempt_id": str(uuid4())}),
                (2, "job.claimed", {"generation": 3, "job_id": str(uuid4())}),
                (3, "run.cancelled", {"cancelled_jobs": 1}),
                (
                    4,
                    "dispatch.observed",
                    {
                        "operation_key": "private-operation-key",
                        "observation": {"token": secret_sentinel},
                    },
                ),
            ],
            status="cancelled",
        )
        page_run = _seed_run(
            runtime_engine,
            owner_id,
            [(sequence, "job.claimed", {"generation": sequence}) for sequence in range(1, 104)],
            status="cancelled",
        )
        large_sequence = 9_007_199_254_740_993
        large_cursor_run = _seed_run(
            runtime_engine,
            owner_id,
            [(large_sequence, "job.claimed", {"generation": 1})],
            status="cancelled",
        )
        gap_run = _seed_run(
            runtime_engine,
            owner_id,
            [(1, "run.admitted", {}), (3, "run.cancelled", {})],
            status="cancelled",
        )

        owner_app = create_app(
            principal_resolver=lambda _request: Principal(owner_id), runtime_engine=runtime_engine
        )
        stranger_app = create_app(
            principal_resolver=lambda _request: Principal(other_owner_id),
            runtime_engine=runtime_engine,
        )
        snapshot = _get(owner_app, f"/api/v1/runs/{primary_run}")
        assert snapshot.status_code == 200
        assert snapshot.headers["cache-control"] == "private, no-store"
        assert snapshot.json()["status"] == "cancelled"
        assert snapshot.json()["event_watermark"] == "4"
        assert _get(stranger_app, f"/api/v1/runs/{primary_run}").status_code == 404
        assert _get(stranger_app, f"/api/v1/runs/{primary_run}/events").status_code == 404

        first_page = _get(owner_app, f"/api/v1/runs/{primary_run}/events?after=0&limit=2")
        assert first_page.status_code == 200
        assert first_page.headers["cache-control"] == "private, no-store"
        assert [event["sequence"] for event in first_page.json()["events"]] == ["1", "2"]
        assert first_page.json()["next_cursor"] == "2"
        full_page = _get(owner_app, f"/api/v1/runs/{page_run}/events?after=0&limit=100")
        assert full_page.status_code == 200
        assert len(full_page.json()["events"]) == 100
        assert full_page.json()["next_cursor"] == "100"
        assert full_page.json()["high_watermark"] == "103"
        assert full_page.json()["history_gap"] is False

        exact_int8 = str(large_sequence)
        large_snapshot = _get(owner_app, f"/api/v1/runs/{large_cursor_run}").json()
        assert large_snapshot["event_watermark"] == exact_int8
        large_page = _get(
            owner_app,
            f"/api/v1/runs/{large_cursor_run}/events?after={large_sequence - 1}",
        )
        assert large_page.json()["events"][0]["sequence"] == exact_int8
        assert _get(owner_app, f"/api/v1/runs/{primary_run}/events?after=5").status_code == 400
        assert (
            _get(
                owner_app,
                f"/api/v1/runs/{primary_run}/events?after=1",
                headers={"Last-Event-ID": "2"},
            ).status_code
            == 400
        )

        history_page = _get(owner_app, f"/api/v1/runs/{gap_run}/events?after=0")
        assert history_page.json()["history_gap"] is True
        assert [event["sequence"] for event in history_page.json()["events"]] == ["1", "3"]
        serialized_events = json.dumps(first_page.json(), sort_keys=True)
        assert secret_sentinel not in serialized_events
        assert "private-operation-key" not in serialized_events
        assert "observation" not in serialized_events

        sse_headers: dict[str, str] = {}
        reconnect_body = _run_sse_and_disconnect(
            owner_app,
            f"/api/v1/runs/{primary_run}/events?{urlencode({'after': '1'})}",
            headers={"accept": "text/event-stream", "last-event-id": "3"},
            stop_marker=b"id: 4\n",
            disconnect_on_marker=True,
            captured_headers=sse_headers,
        )
        assert b"id: 4\nevent: run.event\n" in reconnect_body
        assert b'"type":"dispatch.observed"' in reconnect_body
        assert secret_sentinel not in reconnect_body.decode()
        assert sse_headers["cache-control"] == "private, no-store, no-cache, no-transform"
        assert runtime_engine.pool.checkedout() == 0

        reset_body = _run_sse_and_disconnect(
            owner_app,
            f"/api/v1/runs/{gap_run}/events?after=0",
            headers={"accept": "text/event-stream"},
            stop_marker=b"event: history.reset\n",
            disconnect_on_marker=False,
        )
        assert b'"reason":"history_gap"' in reset_body
        assert b'"high_watermark":"3"' in reset_body
        _append_event(runtime_engine, owner_id, gap_run, 4, "dispatch.observed", {})
        resumed_after_gap = _run_sse_and_disconnect(
            owner_app,
            f"/api/v1/runs/{gap_run}/events?after=3",
            headers={"accept": "text/event-stream"},
            stop_marker=b"id: 4\n",
            disconnect_on_marker=True,
        )
        assert b"id: 4\nevent: run.event\n" in resumed_after_gap
        assert b"event: history.reset\n" not in resumed_after_gap

        future_cursor_body = _run_sse_and_disconnect(
            owner_app,
            f"/api/v1/runs/{primary_run}/events?after=9",
            headers={"accept": "text/event-stream"},
            stop_marker=b"event: history.reset\n",
            disconnect_on_marker=False,
        )
        assert b'"reason":"cursor_ahead"' in future_cursor_body
        assert b'"high_watermark":"4"' in future_cursor_body
        assert runtime_engine.pool.checkedout() == 0

        principal_is_valid = True

        def resolve_revocable_principal(_request: object) -> Principal:
            if not principal_is_valid:
                raise HTTPException(status_code=401, detail="authentication_required")
            return Principal(owner_id)

        revocable_app = create_app(
            principal_resolver=resolve_revocable_principal,
            runtime_engine=runtime_engine,
        )

        def revoke_after_first_event(chunk: bytes) -> None:
            nonlocal principal_is_valid
            if b"id: 1\n" in chunk:
                principal_is_valid = False

        revoked_body = _run_sse_and_disconnect(
            revocable_app,
            f"/api/v1/runs/{page_run}/events?after=0",
            headers={"accept": "text/event-stream"},
            stop_marker=b"event: stream.error\n",
            disconnect_on_marker=False,
            on_chunk=revoke_after_first_event,
        )
        assert b"id: 1\nevent: run.event\n" in revoked_body
        emitted_event_ids = [
            frame.splitlines()[0]
            for frame in revoked_body.split(b"\n\n")
            if frame.startswith(b"id: ")
        ]
        assert emitted_event_ids == [b"id: 1"]
        assert b'"code":"authentication_required","retryable":false' in revoked_body
        assert runtime_engine.pool.checkedout() == 0

        openapi = owner_app.openapi()
        assert "HistoryResetPayload" in openapi["components"]["schemas"]
    finally:
        if runtime_engine is not None:
            runtime_engine.dispose()
        try:
            if migration_applied:
                _run_migration("downgrade", "base", database_url)
        finally:
            _cleanup_database(admin_engine)
            admin_engine.dispose()


def _seed_run(
    engine: Engine,
    owner_id: UUID,
    events: list[tuple[int, str, dict[str, object]]],
    *,
    status: str,
) -> UUID:
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, Principal(owner_id))
        run_id = repository.create_run()
        for sequence, event_type, payload in events:
            repository.append_event(
                run_id, sequence, event_type, EventActor.SYSTEM, payload=payload
            )
        connection.execute(
            text("UPDATE runtime_private.runs SET status=:status WHERE run_id=:run_id"),
            {"status": status, "run_id": run_id},
        )
        return run_id


def _append_event(
    engine: Engine,
    owner_id: UUID,
    run_id: UUID,
    sequence: int,
    event_type: str,
    payload: dict[str, object],
) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, Principal(owner_id)).append_event(
            run_id, sequence, event_type, EventActor.SYSTEM, payload=payload
        )


def _get(
    application: object, path: str, *, headers: dict[str, str] | None = None
) -> httpx.Response:
    async def send() -> httpx.Response:
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get(path, headers=headers)

    return asyncio.run(send())


def _run_sse_and_disconnect(
    application: object,
    target: str,
    *,
    headers: dict[str, str],
    stop_marker: bytes,
    disconnect_on_marker: bool,
    on_chunk: Callable[[bytes], None] | None = None,
    captured_headers: dict[str, str] | None = None,
) -> bytes:
    async def consume() -> bytes:
        outgoing: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        incoming: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        await incoming.put({"type": "http.request", "body": b"", "more_body": False})
        scope = _http_scope(target, headers)

        async def receive() -> dict[str, object]:
            return await incoming.get()

        async def send(message: dict[str, object]) -> None:
            await outgoing.put(message)
            if message.get("type") == "http.response.start" and captured_headers is not None:
                response_headers = message.get("headers", [])
                if isinstance(response_headers, list):
                    captured_headers.update(
                        {
                            name.decode("latin1").lower(): value.decode("latin1")
                            for name, value in response_headers
                            if isinstance(name, bytes) and isinstance(value, bytes)
                        }
                    )
            body = message.get("body")
            if isinstance(body, bytes) and on_chunk is not None:
                on_chunk(body)
            if disconnect_on_marker and isinstance(body, bytes) and stop_marker in body:
                await incoming.put({"type": "http.disconnect"})

        task = asyncio.create_task(application(scope, receive, send))
        body_chunks: list[bytes] = []
        saw_marker = False
        try:
            while True:
                message = await asyncio.wait_for(outgoing.get(), timeout=5)
                if message.get("type") == "http.response.body":
                    body = message.get("body", b"")
                    if isinstance(body, bytes):
                        body_chunks.append(body)
                        saw_marker = saw_marker or stop_marker in body
                    if saw_marker and not disconnect_on_marker:
                        break
                    if saw_marker and disconnect_on_marker:
                        break
                    if message.get("more_body") is False:
                        break
            if disconnect_on_marker and saw_marker:
                await asyncio.wait_for(task, timeout=5)
            else:
                await asyncio.wait_for(task, timeout=5)
        finally:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if not saw_marker:
            pytest.fail("SSE response did not emit the expected event")
        return b"".join(body_chunks)

    return asyncio.run(consume())


def _http_scope(target: str, headers: dict[str, str]) -> dict[str, object]:
    path, separator, query = target.partition("?")
    encoded_headers = [(key.lower().encode(), value.encode()) for key, value in headers.items()]
    encoded_headers.append((b"host", b"test"))
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": query.encode("ascii") if separator else b"",
        "root_path": "",
        "headers": encoded_headers,
        "client": ("127.0.0.1", 12_345),
        "server": ("test", 80),
        "state": {},
    }


def _role_url(url: URL, password: str) -> URL:
    return url.set(drivername="postgresql+psycopg", username=RUNTIME_ROLE, password=password)


def _reset_database(engine: Engine, runtime_password: str) -> None:
    with engine.begin() as connection:
        for schema_name in (
            "runtime_private",
            "wake_private",
            "webhook_private",
            "continuum_migrations",
        ):
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        for role_name in ROLE_NAMES:
            if connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role_name)"),
                {"role_name": role_name},
            ).scalar_one():
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE {role_name}"))
        for role_name, login_mode, password in (
            ("continuum_ingress", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_issuer", "NOLOGIN", None),
            ("continuum_wake", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_receiver", "LOGIN", secrets.token_urlsafe(32)),
            (RUNTIME_ROLE, "LOGIN", runtime_password),
        ):
            password_sql = "" if password is None else f"PASSWORD '{password}'"
            connection.exec_driver_sql(
                f"CREATE ROLE {role_name} {login_mode} NOSUPERUSER NOCREATEDB "
                f"NOCREATEROLE NOINHERIT NOBYPASSRLS {password_sql}"
            )


def _assert_postgres_17(engine: Engine) -> None:
    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT current_setting('server_version_num')::integer")
        ).scalar_one()
    if not 170000 <= version < 180000:
        pytest.fail("T12 integration requires PostgreSQL 17")


def _run_migration(action: str, revision: str, database_url: str) -> None:
    environment = os.environ.copy()
    environment["CONTINUUM_MIGRATION_DATABASE_URL"] = database_url
    result = subprocess.run(
        [sys.executable, "-m", "continuum_api.migrations", action, revision],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(f"T12 migration {action} {revision} failed; output redacted")


def _cleanup_database(engine: Engine) -> None:
    with engine.begin() as connection:
        for schema_name in (
            "runtime_private",
            "wake_private",
            "webhook_private",
            "continuum_migrations",
        ):
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        for role_name in ROLE_NAMES:
            if connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role_name)"),
                {"role_name": role_name},
            ).scalar_one():
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE {role_name}"))
