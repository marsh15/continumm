"""PostgreSQL proof for immediate retention gates and durable deletion cleanup."""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError

from continuum_api.app import create_app
from continuum_api.runtime.persistence import EventActor, Principal, RuntimeRepository
from continuum_api.runtime.retention import (
    DeleteOutcome,
    RetentionService,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t40_test"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    "continuum_ingress",
    "continuum_wake_issuer",
    "continuum_admission",
    "continuum_wake",
    "continuum_wake_receiver",
    "continuum_recovery",
    RUNTIME_ROLE,
)
SCHEMAS = (
    "runtime_private",
    "wake_private",
    "webhook_private",
    "recovery_private",
    "continuum_migrations",
)


@dataclass(slots=True)
class T40Database:
    admin_engine: Engine
    runtime_engine: Engine


@pytest.fixture(scope="module")
def t40_database() -> Iterator[T40Database]:
    database_url = os.environ.get("CONTINUUM_T40_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T40_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T40 integration requires loopback PostgreSQL named continuum_t40_test")
    admin_engine = create_engine(url.set(drivername="postgresql+psycopg"), hide_parameters=True)
    runtime_password = secrets.token_urlsafe(32)
    runtime_engine: Engine | None = None
    migrated = False
    try:
        _reset_database(admin_engine, runtime_password)
        with admin_engine.connect() as connection:
            version = connection.execute(text("SHOW server_version_num")).scalar_one()
        if int(version) // 10000 != 17:
            pytest.fail("T40 integration requires PostgreSQL 17")
        _run_migration("upgrade", "head", database_url)
        migrated = True
        runtime_engine = create_engine(
            _role_url(url, runtime_password), hide_parameters=True, pool_size=4, max_overflow=0
        )
        yield T40Database(admin_engine, runtime_engine)
    finally:
        if runtime_engine is not None:
            runtime_engine.dispose()
        try:
            if migrated:
                _run_migration("downgrade", "base", database_url)
        finally:
            _cleanup_database(admin_engine)
            admin_engine.dispose()


class UnsupportedRemoteDeletion:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None]] = []

    def delete(self, sink: str, reference_id: str, provider: str | None) -> DeleteOutcome:
        self.calls.append((sink, reference_id, provider))
        return DeleteOutcome.UNSUPPORTED


def test_expiry_deletion_and_account_revoke_are_immediate_and_idempotent(
    t40_database: T40Database,
) -> None:
    store = t40_database
    owner = uuid4()
    principal = Principal(owner)
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id,github_user_id,github_login) "
                "VALUES (:owner,:github,'retention-test')"
            ),
            {"owner": owner, "github": 900_000_000 + owner.int % 10_000_000},
        )
    with store.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run(github_repository_id=400_000_000, issue_number=7)
        repository.append_event(
            run_id,
            1,
            "task.accepted",
            EventActor.ACCOUNT,
            actor_id=str(owner),
            payload={"body": "sensitive issue text"},
        )
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.provider_credentials "
                "(credential_id,owner_id,provider,scope,scope_run_id,key_version,nonce,"
                "ciphertext,expires_at) "
                "VALUES (:id,:owner,'openai','run',:run,'test-key-version',:nonce,:ciphertext,"
                "clock_timestamp()+interval '14 days')"
            ),
            {
                "id": uuid4(),
                "owner": owner,
                "run": run_id,
                "nonce": secrets.token_bytes(12),
                "ciphertext": secrets.token_bytes(32),
            },
        )

    service = RetentionService(store.runtime_engine, principal)
    first = service.delete_run(run_id)
    second = service.delete_run(run_id)
    assert first.accepted and second.accepted
    assert second.cleanup_pending == 0
    with store.admin_engine.connect() as connection:
        run_credential = connection.execute(
            text(
                "SELECT status,nonce,ciphertext FROM runtime_private.provider_credentials "
                "WHERE owner_id=:owner AND scope_run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        ).one()
        assert run_credential.status == "deleted"
        assert run_credential.nonce is None and run_credential.ciphertext is None
    with store.runtime_engine.begin() as connection:
        assert RuntimeRepository(connection, principal).get_run(run_id) is None
    with store.admin_engine.connect() as connection:
        assert connection.execute(
            text("SELECT payload FROM runtime_private.events WHERE run_id=:run"),
            {"run": run_id},
        ).scalars().all() == [{}, {}]
        assert (
            connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.retention_receipts "
                    "WHERE owner_id=:owner AND scope='run' AND subject_id=:run AND action='delete'"
                ),
                {"owner": owner, "run": run_id},
            ).scalar_one()
            == 1
        )
    with pytest.raises(DBAPIError):
        with store.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            connection.execute(
                text(
                    "UPDATE runtime_private.retention_receipts SET action='expire' "
                    "WHERE owner_id=:owner AND subject_id=:run"
                ),
                {"owner": owner, "run": run_id},
            )

    # Expiry is a database-time read gate even before the bounded scrubber runs.
    expiring_owner = uuid4()
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id,github_user_id,github_login) "
                "VALUES (:owner,:github,'expiry-test')"
            ),
            {"owner": expiring_owner, "github": 900_000_000 + expiring_owner.int % 10_000_000},
        )
    expiring = Principal(expiring_owner)
    with store.runtime_engine.begin() as connection:
        run = RuntimeRepository(connection, expiring).create_run()
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET created_at=clock_timestamp()-interval '2 days', "
                "content_expires_at=clock_timestamp()-interval '1 second' WHERE run_id=:run"
            ),
            {"run": run},
        )
    with store.runtime_engine.begin() as connection:
        assert RuntimeRepository(connection, expiring).get_run(run) is None
    assert RetentionService(store.runtime_engine, expiring).expire_due(limit=1) == 1

    # Account deletion revokes auth/key material before asynchronous cleanup.
    account = uuid4()
    account_github_id = 910_000_000 + account.int % 10_000_000
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id,github_user_id,github_login) "
                "VALUES (:owner,:github,'account-delete')"
            ),
            {"owner": account, "github": account_github_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.github_user_tokens "
                "(owner_id,github_user_id,token_id,key_version,nonce,ciphertext,status,expires_at) "
                "VALUES (:owner,:github,:id,'test-key-version',:nonce,:ciphertext,'active',"
                "clock_timestamp()+interval '1 hour')"
            ),
            {
                "owner": account,
                "github": account_github_id,
                "id": uuid4(),
                "nonce": secrets.token_bytes(12),
                "ciphertext": secrets.token_bytes(32),
            },
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.browser_sessions "
                "(session_hash,user_id,csrf_hash,created_at,expires_at) "
                "VALUES (:session,:owner,:csrf,clock_timestamp(),"
                "clock_timestamp()+interval '1 hour')"
            ),
            {"session": secrets.token_bytes(32), "owner": account, "csrf": secrets.token_bytes(32)},
        )
    account_principal = Principal(account)
    with store.runtime_engine.begin() as connection:
        account_run = RuntimeRepository(connection, account_principal).create_run(
            github_repository_id=400_000_001, issue_number=8
        )
    account_service = RetentionService(store.runtime_engine, account_principal)
    receipt = account_service.delete_account()
    assert receipt.accepted and receipt.cleanup_pending == 1
    with store.admin_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT status FROM runtime_private.users WHERE user_id=:owner"),
                {"owner": account},
            ).scalar_one()
            == "deleting"
        )
        github_token = connection.execute(
            text(
                "SELECT status,nonce,ciphertext FROM runtime_private.github_user_tokens "
                "WHERE owner_id=:owner"
            ),
            {"owner": account},
        ).one()
        assert github_token == ("revoked", None, None)
        assert connection.execute(
            text(
                "SELECT revoked_at IS NOT NULL FROM runtime_private.browser_sessions "
                "WHERE user_id=:owner"
            ),
            {"owner": account},
        ).scalar_one()
    with store.runtime_engine.begin() as connection:
        assert RuntimeRepository(connection, account_principal).get_run(account_run) is None
    drained = RetentionService.drain_due(store.runtime_engine, limit=10)
    assert drained.scrubbed_runs == 1
    with store.admin_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT status FROM runtime_private.users WHERE user_id=:owner"),
                {"owner": account},
            ).scalar_one()
            == "deleted"
        )
        assert connection.execute(
            text(
                "SELECT status,content_scrubbed_at IS NOT NULL "
                "FROM runtime_private.runs WHERE run_id=:run"
            ),
            {"run": account_run},
        ).one() == ("cancelled", True)


def test_cleanup_adapter_records_unsupported_without_claiming_erasure(
    t40_database: T40Database,
) -> None:
    owner = uuid4()
    with t40_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id,github_user_id,github_login) "
                "VALUES (:owner,:github,'cleanup-test')"
            ),
            {"owner": owner, "github": 920_000_000 + owner.int % 10_000_000},
        )
    principal = Principal(owner)
    cleanup_id = uuid4()
    with t40_database.runtime_engine.begin() as connection:
        RuntimeRepository(connection, principal)
        connection.execute(
            text(
                "INSERT INTO runtime_private.retention_cleanup_items "
                "(cleanup_id,owner_id,subject_id,scope,sink,provider,reference_id) "
                "VALUES (:id,:owner,:subject,'run','provider','openai','provider-copy-opaque')"
            ),
            {"id": cleanup_id, "owner": owner, "subject": uuid4()},
        )
    adapter = UnsupportedRemoteDeletion()
    summary = RetentionService(
        t40_database.runtime_engine, principal, deletion_adapter=adapter
    ).cleanup_due(limit=1)
    assert summary.unsupported == 1 and summary.deleted == 0
    assert adapter.calls == [("provider", "provider-copy-opaque", "openai")]
    with t40_database.runtime_engine.begin() as connection:
        RuntimeRepository(connection, principal)
        status = connection.execute(
            text("SELECT status FROM runtime_private.retention_cleanup_items WHERE cleanup_id=:id"),
            {"id": cleanup_id},
        ).scalar_one()
    assert status == "unsupported"


def test_delete_routes_require_origin_csrf_and_return_durable_receipt(
    t40_database: T40Database,
) -> None:
    owner = uuid4()
    with t40_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id,github_user_id,github_login) "
                "VALUES (:owner,:github,'route-test')"
            ),
            {"owner": owner, "github": 930_000_000 + owner.int % 10_000_000},
        )
    principal = Principal(owner)
    with t40_database.runtime_engine.begin() as connection:
        run_id = RuntimeRepository(connection, principal).create_run()
    app = create_app(
        principal_resolver=lambda _request: principal,
        retention_service_factory=lambda _request, identity: RetentionService(
            t40_database.runtime_engine, identity
        ),
        csrf_guard=lambda request: request.headers.get("x-csrf-token") == "valid",
        runtime_engine=t40_database.runtime_engine,
    )
    with TestClient(app) as client:
        denied = client.delete(f"/api/v1/runs/{run_id}", headers={"origin": "https://local"})
        assert denied.status_code == 403
        response = client.delete(
            f"/api/v1/runs/{run_id}",
            headers={"origin": "https://local", "x-csrf-token": "valid"},
        )
    assert response.status_code == 202
    assert response.json()["accepted"] is True


def _run_migration(operation: str, revision: str, database_url: str) -> None:
    env = os.environ.copy()
    env["CONTINUUM_MIGRATION_DATABASE_URL"] = database_url
    result = subprocess.run(
        [sys.executable, "-m", "continuum_api.migrations", operation, revision],
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        pytest.fail(
            f"T40 migration {operation} failed: {result.stdout[-4000:]} {result.stderr[-4000:]}"
        )


def _role_url(url: URL, password: str) -> URL:
    return url.set(
        drivername="postgresql+psycopg",
        username=RUNTIME_ROLE,
        password=password,
        query={"sslmode": "disable"},
    )


def _reset_database(engine: Engine, runtime_password: str) -> None:
    with engine.begin() as connection:
        _drop_schemas(connection)
        for role in ROLE_NAMES:
            if connection.execute(
                text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
            ).scalar_one():
                connection.exec_driver_sql(
                    f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role}"
                )
                connection.exec_driver_sql(f"DROP ROLE {role}")
        definitions = (
            ("continuum_ingress", "LOGIN", secrets.token_urlsafe(24)),
            ("continuum_wake_issuer", "NOLOGIN", None),
            ("continuum_admission", "NOLOGIN", None),
            ("continuum_wake", "LOGIN", secrets.token_urlsafe(24)),
            ("continuum_wake_receiver", "LOGIN", secrets.token_urlsafe(24)),
            ("continuum_recovery", "LOGIN", secrets.token_urlsafe(24)),
            (RUNTIME_ROLE, "LOGIN", runtime_password),
        )
        for role, mode, password in definitions:
            password_sql = "" if password is None else f"PASSWORD '{password}'"
            connection.exec_driver_sql(
                f"CREATE ROLE {role} {mode} NOSUPERUSER NOCREATEDB NOCREATEROLE "
                f"NOINHERIT NOBYPASSRLS {password_sql}"
            )


def _drop_schemas(connection: Connection) -> None:
    for schema in SCHEMAS:
        connection.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))


def _cleanup_database(engine: Engine) -> None:
    with engine.begin() as connection:
        _drop_schemas(connection)
        for role in ROLE_NAMES:
            if connection.execute(
                text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
            ).scalar_one():
                connection.exec_driver_sql(
                    f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role}"
                )
                connection.exec_driver_sql(f"DROP ROLE {role}")
