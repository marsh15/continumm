"""T42 PostgreSQL and HTTP-boundary proof for operator controls."""

from __future__ import annotations

import fcntl
import os
import secrets
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.runtime.leases import LeaseLost, QueueService
from continuum_api.runtime.operator_actions import (
    OperatorActionError,
    OperatorActionService,
    OperatorReason,
    create_operator_engine,
    parse_operator_allowlist,
    require_operator_actor,
)
from continuum_api.runtime.operator_routes import create_operator_router
from continuum_api.runtime.persistence import Principal, RuntimeRepository

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t42_test"
ROLE_NAMES = (
    "continuum_ingress",
    "continuum_wake_issuer",
    "continuum_wake",
    "continuum_wake_receiver",
    "continuum_wake_worker",
    "continuum_wake_worker_owner",
    "continuum_runtime",
    "continuum_admission",
    "continuum_recovery",
    "continuum_operator",
)


@dataclass(frozen=True)
class _Identity:
    principal: Principal
    github_user_id: int


def test_operator_read_api_and_audited_actions_use_separate_database_role() -> None:
    database_url = os.environ.get("CONTINUUM_T42_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T42_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T42 integration requires loopback PostgreSQL named continuum_t42_test")

    with Path("/tmp/continuum-remaining-pg-test.lock").open("w", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        admin = create_engine(url.set(drivername="postgresql+psycopg"), hide_parameters=True)
        operator_password = secrets.token_urlsafe(32)
        runtime_password = secrets.token_urlsafe(32)
        operator_engine: Engine | None = None
        migrated = False
        try:
            _assert_pg17(admin)
            _reset_database(admin, operator_password, runtime_password)
            _migrate("upgrade", "head", database_url)
            migrated = True
            owner_id, github_id = uuid4(), 88_001_224
            credential_id = _seed_sensitive_rows(admin, owner_id, github_id)
            operator_url = url.set(
                drivername="postgresql+psycopg",
                username="continuum_operator",
                password=operator_password,
            ).render_as_string(hide_password=False)
            operator_engine = create_operator_engine(operator_url)
            service = OperatorActionService(operator_engine)
            _assert_direct_sensitive_reads_denied(operator_engine)

            overview = service.overview()
            assert overview.pending_inbox_deliveries == 1
            assert overview.artifact_capacity_limit_bytes > 0
            assert service.pending_inbox()[0].event_type == "issue_comment"
            _assert_least_privilege(admin)
            assert service.revoke_provider_credential(
                credential_id, 99_001, OperatorReason.SECURITY
            )
            with admin.connect() as connection:
                revoked = connection.execute(
                    text(
                        "SELECT status, nonce, ciphertext, ended_at FROM "
                        "runtime_private.provider_credentials WHERE credential_id=:id"
                    ),
                    {"id": credential_id},
                ).one()
            assert revoked.status == "revoked" and revoked.nonce is None
            assert revoked.ciphertext is None and revoked.ended_at is not None

            runtime = create_engine(
                url.set(
                    drivername="postgresql+psycopg",
                    username="continuum_runtime",
                    password=runtime_password,
                ),
                hide_parameters=True,
            )
            try:
                stop_started = threading.Event()

                def stop_admission() -> bool:
                    stop_started.set()
                    return service.set_admission_stopped(
                        True, 99_001, OperatorReason.DATABASE_PAUSE
                    )

                with ThreadPoolExecutor(max_workers=1) as executor:
                    with runtime.begin() as connection:
                        assert connection.execute(
                            text("SELECT operator_private.admission_allowed(:owner)"),
                            {"owner": owner_id},
                        ).scalar_one()
                        stopped = executor.submit(stop_admission)
                        assert stop_started.wait(timeout=1)
                        time.sleep(0.05)
                        assert not stopped.done(), (
                            "stop did not wait for admission transaction lock"
                        )
                    assert stopped.result(timeout=2)
                with runtime.begin() as connection:
                    assert not connection.execute(
                        text("SELECT operator_private.admission_allowed(:owner)"),
                        {"owner": owner_id},
                    ).scalar_one()
            finally:
                runtime.dispose()

            assert service.set_admission_stopped(False, 99_001, OperatorReason.DATABASE_PAUSE)
            with runtime.begin() as connection:
                repository = RuntimeRepository(connection, Principal(owner_id))
                run_id = repository.create_run()
                first_step = repository.create_step(run_id, "suspension:leased", "probe")
                repository.create_job(run_id, first_step)
            queue = QueueService(runtime, Principal(owner_id))
            lease = queue.claim_one("suspension-probe")
            assert lease is not None
            with runtime.begin() as connection:
                repository = RuntimeRepository(connection, Principal(owner_id))
                queued_step = repository.create_step(run_id, "suspension:queued", "probe")
                repository.create_job(run_id, queued_step)
            assert service.set_account_suspended(github_id, True, 99_001, OperatorReason.SECURITY)
            assert queue.claim_one("suspended-new-work") is None
            with pytest.raises(LeaseLost):
                queue.persist_checkpoint(lease, "after-suspension", {"must_not_commit": True})
            with admin.begin() as connection:
                assert (
                    connection.execute(
                        text("SELECT status FROM runtime_private.users WHERE user_id=:owner"),
                        {"owner": owner_id},
                    ).scalar_one()
                    == "suspended"
                )
                token = connection.execute(
                    text(
                        "SELECT status, nonce, ciphertext FROM runtime_private.github_user_tokens "
                        "WHERE owner_id=:owner"
                    ),
                    {"owner": owner_id},
                ).one()
                assert (
                    token.status == "revoked" and token.nonce is None and token.ciphertext is None
                )
                assert connection.execute(
                    text(
                        "SELECT revoked_at IS NOT NULL FROM runtime_private.browser_sessions "
                        "WHERE user_id=:owner"
                    ),
                    {"owner": owner_id},
                ).scalar_one()
            assert service.set_account_suspended(github_id, False, 99_001, OperatorReason.SECURITY)
            with admin.connect() as connection:
                assert connection.execute(
                    text("SELECT operator_private.admission_allowed(:owner)"),
                    {"owner": owner_id},
                ).scalar_one()
            assert {item.action for item in service.audit_events()} == {
                "admission_stop",
                "admission_resume",
                "account_suspend",
                "account_reactivate",
                "credential_revoke",
            }

            _assert_allowlisted_api(service, owner_id)
            with pytest.raises(OperatorActionError, match="operator_actor_not_allowlisted"):
                require_operator_actor(99_002, frozenset({99_001}))
            with pytest.raises(OperatorActionError, match="operator_allowlist_invalid"):
                parse_operator_allowlist("99,,100")
        finally:
            if operator_engine is not None:
                operator_engine.dispose()
            if migrated:
                _migrate("downgrade", "20261020_27", database_url)
            _cleanup_database(admin)
            admin.dispose()
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _assert_allowlisted_api(service: OperatorActionService, owner_id: UUID) -> None:
    actor_id = 99_001
    principal = Principal(owner_id)

    def resolver(request):
        request.state.github_identity = _Identity(principal, actor_id)
        return principal

    app = FastAPI()
    app.include_router(
        create_operator_router(
            resolver, lambda _request, _principal: service, lambda: frozenset({actor_id})
        )
    )
    with TestClient(app) as client:
        assert client.get("/api/v1/operator/overview").status_code == 200
        assert client.post("/api/v1/operator/admission/stop").status_code == 404

    def missing_identity(_request):
        return principal

    unauthenticated = FastAPI()
    unauthenticated.include_router(
        create_operator_router(
            missing_identity,
            lambda _request, _principal: service,
            lambda: frozenset({actor_id}),
        )
    )
    with TestClient(unauthenticated) as client:
        assert (
            client.get(
                "/api/v1/operator/overview",
                headers={"x-github-user-id": str(actor_id)},
            ).status_code
            == 401
        )


def _assert_least_privilege(engine: Engine) -> None:
    with engine.connect() as connection:
        values = connection.execute(
            text(
                """
                SELECT
                    has_table_privilege('continuum_operator','operator_private.global_control','UPDATE'),
                    has_table_privilege('continuum_operator','operator_private.audit_events','UPDATE'),
                    has_table_privilege('continuum_operator','operator_private.audit_events','DELETE'),
                    has_table_privilege('continuum_operator','runtime_private.github_user_tokens','SELECT'),
                    has_table_privilege('continuum_operator','runtime_private.browser_sessions','SELECT'),
                    has_column_privilege('continuum_operator','webhook_private.github_delivery_inbox','raw_body','SELECT'),
                    has_column_privilege('continuum_operator','runtime_private.runs','model_task_context','SELECT'),
                    has_table_privilege('continuum_runtime','operator_private.global_control','UPDATE'),
                    has_table_privilege('continuum_runtime','operator_private.audit_events','SELECT')
                """
            )
        ).one()
    assert all(value is False for value in values)


def _assert_direct_sensitive_reads_denied(engine: Engine) -> None:
    for statement in (
        "SELECT ciphertext FROM runtime_private.github_user_tokens LIMIT 1",
        "SELECT csrf_hash FROM runtime_private.browser_sessions LIMIT 1",
        "SELECT raw_body FROM webhook_private.github_delivery_inbox LIMIT 1",
        "SELECT model_task_context FROM runtime_private.runs LIMIT 1",
        "UPDATE operator_private.global_control SET admissions_stopped=true",
        "UPDATE operator_private.audit_events SET action='admission_stop'",
    ):
        with engine.connect() as connection:
            with pytest.raises(SQLAlchemyError):
                with connection.begin():
                    connection.execute(text(statement))


def _seed_sensitive_rows(engine: Engine, owner_id: UUID, github_id: int) -> UUID:
    now = datetime.now(UTC)
    credential_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users(user_id,github_user_id,github_login) "
                "VALUES (:owner,:github,'t42-fixture')"
            ),
            {"owner": owner_id, "github": github_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.github_user_tokens "
                "(owner_id,github_user_id,token_id,key_version,nonce,ciphertext,expires_at) "
                "VALUES (:owner,:github,:token,'v1',:nonce,:ciphertext,:expires)"
            ),
            {
                "owner": owner_id,
                "github": github_id,
                "token": uuid4(),
                "nonce": b"n" * 12,
                "ciphertext": b"c" * 32,
                "expires": now + timedelta(days=1),
            },
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.browser_sessions "
                "(session_hash,user_id,csrf_hash,created_at,expires_at) "
                "VALUES (:session,:owner,:csrf,:created,:expires)"
            ),
            {
                "session": b"s" * 32,
                "owner": owner_id,
                "csrf": b"x" * 32,
                "created": now,
                "expires": now + timedelta(days=1),
            },
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.provider_credentials "
                "(credential_id,owner_id,provider,scope,key_version,nonce,ciphertext,status) "
                "VALUES (:credential,:owner,'openai','saved','v1',:nonce,:ciphertext,'active')"
            ),
            {
                "credential": credential_id,
                "owner": owner_id,
                "nonce": b"p" * 12,
                "ciphertext": b"provider-secret-fixture",
            },
        )
        connection.execute(
            text(
                "INSERT INTO webhook_private.github_delivery_inbox "
                "(github_app_id,delivery_id,event_type,action,payload_sha256,raw_body) "
                "VALUES (1,:delivery,'issue_comment','created',:digest,:body)"
            ),
            {"delivery": uuid4(), "digest": b"d" * 32, "body": b"private comment body"},
        )
    return credential_id


def _assert_pg17(engine: Engine) -> None:
    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT current_setting('server_version_num')::integer")
        ).scalar_one()
    if not 170000 <= version < 180000:
        pytest.fail("T42 integration requires PostgreSQL 17")


def _reset_database(engine: Engine, operator_password: str, runtime_password: str) -> None:
    with engine.begin() as connection:
        for schema in (
            "operator_private",
            "runtime_private",
            "wake_private",
            "webhook_private",
            "recovery_private",
            "continuum_migrations",
        ):
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        for role in ROLE_NAMES:
            if connection.execute(
                text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
            ).scalar_one():
                connection.execute(text(f"DROP OWNED BY {role}"))
                connection.execute(text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role}"))
                connection.execute(text(f"DROP ROLE {role}"))
        passwords = {"continuum_operator": operator_password, "continuum_runtime": runtime_password}
        definitions = (
            ("continuum_ingress", "LOGIN"),
            ("continuum_wake_issuer", "NOLOGIN"),
            ("continuum_wake", "LOGIN"),
            ("continuum_wake_receiver", "LOGIN"),
            ("continuum_wake_worker", "LOGIN"),
            ("continuum_wake_worker_owner", "NOLOGIN"),
            ("continuum_runtime", "LOGIN"),
            ("continuum_admission", "NOLOGIN"),
            ("continuum_recovery", "LOGIN"),
            ("continuum_operator", "LOGIN"),
        )
        for role, mode in definitions:
            password = f" PASSWORD '{passwords[role]}'" if role in passwords else ""
            connection.exec_driver_sql(
                f"CREATE ROLE {role} {mode} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT "
                f"NOBYPASSRLS{password}"
            )


def _migrate(operation: str, revision: str, database_url: str) -> None:
    environment = os.environ.copy()
    environment["CONTINUUM_MIGRATION_DATABASE_URL"] = database_url
    result = subprocess.run(
        [sys.executable, "-m", "continuum_api.migrations", operation, revision],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        pytest.fail("T42 migration command failed; output redacted")


def _cleanup_database(engine: Engine) -> None:
    with engine.begin() as connection:
        for schema in (
            "operator_private",
            "runtime_private",
            "wake_private",
            "webhook_private",
            "recovery_private",
            "continuum_migrations",
        ):
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        for role in ROLE_NAMES:
            if connection.execute(
                text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
            ).scalar_one():
                connection.execute(text(f"DROP OWNED BY {role}"))
                connection.execute(text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role}"))
                connection.execute(text(f"DROP ROLE {role}"))
