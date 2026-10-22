"""Prove a killed PostgreSQL wake worker is fenced before lease replacement."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import select
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from test_agent_loop import T22Database  # noqa: F401
from test_agent_loop import t22_database as t22_database

from continuum_api.runtime.wake_worker_store import DrainLease, PostgresWakeWorkerStore

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WORKER_ROLE = "continuum_wake_worker"
APP_ID = 7_777_777_777

_CLAIM_AND_BLOCK = r"""
import json
import os
import sys
from uuid import UUID

from sqlalchemy import create_engine
from continuum_api.runtime.wake_worker_store import PostgresWakeWorkerStore

engine = create_engine(
    os.environ["CONTINUUM_TEST_WAKE_WORKER_DATABASE_URL"],
    hide_parameters=True,
    pool_size=1,
    max_overflow=0,
    connect_args={
        "connect_timeout": 5,
        "options": "-c statement_timeout=5000 -c lock_timeout=2000",
    },
)
try:
    store = PostgresWakeWorkerStore(
        engine, github_app_id=int(os.environ["CONTINUUM_TEST_WAKE_APP_ID"])
    )
    lease = store.claim(
        UUID(os.environ["CONTINUUM_TEST_WAKE_ID"]),
        UUID(os.environ["CONTINUUM_TEST_WAKE_ATTEMPT_ID"]),
        int(os.environ["CONTINUUM_TEST_WAKE_DISPATCH_GENERATION"]),
    )
    if lease is None:
        print(json.dumps({"kind": "claim_failed"}), flush=True)
        sys.exit(2)
    print(json.dumps({
        "kind": "lease_acquired",
        "wake_id": str(lease.wake_id),
        "token": str(lease.token),
        "generation": lease.generation,
    }), flush=True)
    sys.stdin.readline()
finally:
    engine.dispose()
"""


def test_killed_worker_cannot_consume_or_finish_after_replacement_claim(
    t22_database: T22Database,
) -> None:
    database = t22_database
    worker_password = secrets.token_urlsafe(32)
    with database.admin_engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER ROLE {WORKER_ROLE} PASSWORD '{worker_password}'")

    worker_url: URL = database.runtime_url.set(
        username=WORKER_ROLE,
        password=worker_password,
        query={**database.runtime_url.query, "sslmode": "disable"},
    )
    worker_engine = create_engine(
        worker_url,
        hide_parameters=True,
        pool_size=1,
        max_overflow=0,
        connect_args={
            "connect_timeout": 5,
            "options": "-c statement_timeout=5000 -c lock_timeout=2000",
        },
    )
    store = PostgresWakeWorkerStore(worker_engine, github_app_id=APP_ID)
    owner_id, wake_id, old_attempt, new_attempt, delivery_id = (uuid4() for _ in range(5))
    old_dispatch_generation = 8_810_001
    new_dispatch_generation = old_dispatch_generation + 1
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE wake_private.worker_lease "
                "SET expires_at=clock_timestamp()-interval '1 second' WHERE singleton"
            )
        )
    _seed_wake(database, owner_id, wake_id, old_attempt, old_dispatch_generation)
    _seed_delivery(database, delivery_id, "push", {})
    previous_watermark = _watermark(database)

    worker_environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": os.pathsep.join(
            part
            for part in (
                str(PROJECT_ROOT / "src"),
                str(PROJECT_ROOT),
                os.environ.get("PYTHONPATH", ""),
            )
            if part
        ),
        "PYTHONUNBUFFERED": "1",
        "CONTINUUM_TEST_WAKE_WORKER_DATABASE_URL": worker_url.render_as_string(hide_password=False),
        "CONTINUUM_TEST_WAKE_APP_ID": str(APP_ID),
        "CONTINUUM_TEST_WAKE_ID": str(wake_id),
        "CONTINUUM_TEST_WAKE_ATTEMPT_ID": str(old_attempt),
        "CONTINUUM_TEST_WAKE_DISPATCH_GENERATION": str(old_dispatch_generation),
    }
    process = subprocess.Popen(
        [sys.executable, "-c", _CLAIM_AND_BLOCK],
        cwd=PROJECT_ROOT,
        env=worker_environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )
    old_lease: dict[str, Any] | None = None
    try:
        old_lease = _read_message(process, timeout=20)
        assert set(old_lease) == {"kind", "wake_id", "token", "generation"}
        assert old_lease["kind"] == "lease_acquired"
        assert old_lease["wake_id"] == str(wake_id)
        assert UUID(old_lease["token"])
        assert type(old_lease["generation"]) is int
        assert _watermark(database) == previous_watermark
        assert _delivery_is_pending(database, delivery_id)
        process.kill()
        process.wait(timeout=10)
        assert process.returncode is not None

        old_token = UUID(old_lease["token"])
        old_lease_generation = old_lease["generation"]
        with database.admin_engine.begin() as connection:
            expired = connection.execute(
                text(
                    "UPDATE wake_private.worker_lease "
                    "SET expires_at=clock_timestamp()-interval '1 second' "
                    "WHERE singleton AND wake_id=:wake AND token=:token AND generation=:generation"
                ),
                {
                    "wake": wake_id,
                    "token": old_token,
                    "generation": old_lease_generation,
                },
            ).rowcount
            assert expired == 1
            connection.execute(
                text(
                    "UPDATE wake_private.dispatch_control SET generation=:generation,"
                    "active_attempt_id=:attempt,"
                    "lease_expires_at=clock_timestamp()+interval '120 seconds' WHERE singleton"
                ),
                {"generation": new_dispatch_generation, "attempt": new_attempt},
            )
            adopted = (
                connection.execute(
                    text(
                        "SELECT * FROM wake_private.accept_wake_receipt("
                        "CAST(:attempt AS uuid),CAST(:generation AS bigint),"
                        "CAST(:wake AS uuid),'pending_inbox')"
                    ),
                    {
                        "attempt": new_attempt,
                        "generation": new_dispatch_generation,
                        "wake": wake_id,
                    },
                )
                .mappings()
                .one()
            )
            assert adopted["outcome"] == "duplicate"

        replacement = store.claim(wake_id, new_attempt, new_dispatch_generation)
        assert replacement is not None
        assert replacement.generation == old_lease_generation + 1
        assert replacement.token != old_token
        item = store.next_inbox(replacement)
        assert item is not None and item.delivery_id == delivery_id
        assert store.complete_inbox(replacement, delivery_id, "inert") is True
        assert not _delivery_is_pending(database, delivery_id)
        assert _watermark(database) == previous_watermark

        stale_lease = DrainLease(wake_id, old_token, old_lease_generation)
        assert store.complete_inbox(stale_lease, delivery_id, "inert") is False
        assert store.finish(stale_lease, complete=True) is False
        assert _watermark(database) == previous_watermark

        assert store.finish(replacement, complete=True) is True
        new_watermark = _watermark(database)
        assert new_watermark is not None and new_watermark != previous_watermark
        with database.admin_engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT drain_status FROM wake_private.wake_receipt WHERE wake_id=:wake"),
                    {"wake": wake_id},
                ).scalar_one()
                == "drained"
            )
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        if process.stdin is not None:
            process.stdin.close()
        if process.stdout is not None:
            process.stdout.close()
        store.close()


def _read_message(process: subprocess.Popen[str], *, timeout: float) -> dict[str, Any]:
    if process.stdout is None:
        pytest.fail("wake_worker_child_stdout_missing")
    ready, _, _ = select.select([process.stdout], [], [], timeout)
    if not ready:
        pytest.fail("wake_worker_child_claim_timeout")
    line = process.stdout.readline()
    if not line:
        pytest.fail("wake_worker_child_exited_before_claim")
    try:
        message = json.loads(line)
    except json.JSONDecodeError:
        pytest.fail("wake_worker_child_message_invalid")
    if not isinstance(message, dict) or message.get("kind") != "lease_acquired":
        pytest.fail("wake_worker_child_claim_failed")
    return message


def _delivery_is_pending(database: T22Database, delivery_id: UUID) -> bool:
    with database.admin_engine.connect() as connection:
        return bool(
            connection.execute(
                text(
                    "SELECT consumed_at IS NULL FROM webhook_private.github_delivery_inbox "
                    "WHERE github_app_id=:app AND delivery_id=:delivery"
                ),
                {"app": APP_ID, "delivery": delivery_id},
            ).scalar_one()
        )


def _seed_wake(
    database: T22Database,
    owner_id: UUID,
    wake_id: UUID,
    attempt_id: UUID,
    generation: int,
) -> None:
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users(user_id,github_user_id) VALUES (:owner,39001) "
                "ON CONFLICT (user_id) DO NOTHING"
            ),
            {"owner": owner_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.repository_registrations "
                "(owner_id,github_repository_id,repository_owner,repository_name,profile_id) "
                "VALUES (:owner,99001,'owner','repo','python-pytest')"
            ),
            {"owner": owner_id},
        )
        connection.execute(
            text(
                "UPDATE wake_private.dispatch_control SET generation=:generation,"
                "active_wake_id=:wake,active_attempt_id=:attempt,lease_expires_at=NULL,"
                "last_dispatch_outcome='claimed' WHERE singleton"
            ),
            {"generation": generation, "wake": wake_id, "attempt": attempt_id},
        )
        connection.execute(
            text(
                "INSERT INTO wake_private.wake_receipt "
                "(wake_id,reason,accepted_attempt_id,accepted_generation,"
                "last_accepted_attempt_id,last_accepted_generation) "
                "VALUES (:wake,'pending_inbox',:attempt,:generation,:attempt,:generation)"
            ),
            {"wake": wake_id, "attempt": attempt_id, "generation": generation},
        )


def _seed_delivery(
    database: T22Database,
    delivery_id: UUID,
    event_type: str,
    payload: dict[str, object],
) -> None:
    raw_body = json.dumps(payload).encode()
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO webhook_private.github_delivery_inbox "
                "(github_app_id,delivery_id,event_type,action,payload_sha256,raw_body) "
                "VALUES (:app,:delivery,:event,:action,:digest,:body)"
            ),
            {
                "app": APP_ID,
                "delivery": delivery_id,
                "event": event_type,
                "action": payload.get("action"),
                "digest": hashlib.sha256(raw_body).digest(),
                "body": raw_body,
            },
        )


def _watermark(database: T22Database) -> datetime | None:
    with database.admin_engine.connect() as connection:
        return connection.execute(
            text(
                "SELECT last_successful_drain_at FROM wake_private.drain_watermark WHERE singleton"
            )
        ).scalar_one()
