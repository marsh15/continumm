"""PostgreSQL proof for T39 ticket, lease, watermark, and redelivery dedup state."""

from __future__ import annotations

import fcntl
import os
import secrets
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t39_test"
REVISION = "20261020_26"
ROLES = (
    "continuum_ingress",
    "continuum_wake_issuer",
    "continuum_wake",
    "continuum_wake_receiver",
    "continuum_runtime",
    "continuum_admission",
    "continuum_recovery",
)


def test_ticket_claim_and_watermark_are_durable_in_postgres() -> None:
    database_url = os.environ.get("CONTINUUM_T39_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T39_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T39 integration requires loopback PostgreSQL named continuum_t39_test")

    lock_path = Path("/tmp/continuum-remaining-pg-test.lock")
    with lock_path.open("w", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        admin_engine = create_engine(
            url.set(drivername="postgresql+psycopg"),
            hide_parameters=True,
            pool_size=2,
            max_overflow=0,
        )
        applied = False
        try:
            _assert_postgres_17(admin_engine)
            _reset_database(admin_engine)
            _run_migration("upgrade", REVISION, database_url)
            applied = True
            _assert_recovery_privileges(admin_engine)
            _run_deno_integration(database_url)
        finally:
            try:
                if applied:
                    _run_migration("downgrade", "20261020_25", database_url)
            finally:
                _cleanup_database(admin_engine)
                admin_engine.dispose()
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _assert_postgres_17(engine: Engine) -> None:
    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT current_setting('server_version_num')::integer")
        ).scalar_one()
    if not 170000 <= version < 180000:
        pytest.fail("T39 integration requires PostgreSQL 17")


def _assert_recovery_privileges(engine: Engine) -> None:
    with engine.connect() as connection:
        grants = connection.execute(
            text(
                """
                SELECT
                    has_schema_privilege('continuum_ingress', 'recovery_private', 'USAGE'),
                    has_schema_privilege('continuum_recovery', 'recovery_private', 'USAGE'),
                    has_table_privilege('continuum_recovery',
                        'recovery_private.delivery_redelivery', 'SELECT'),
                    has_table_privilege('continuum_recovery',
                        'recovery_private.delivery_redelivery', 'INSERT'),
                    has_table_privilege('continuum_recovery',
                        'recovery_private.delivery_redelivery', 'UPDATE'),
                    has_table_privilege('continuum_recovery',
                        'recovery_private.delivery_redelivery', 'DELETE'),
                    (SELECT pg_get_userbyid(relowner) FROM pg_class
                     WHERE oid='recovery_private.delivery_redelivery'::regclass)
                """
            )
        ).one()
    if grants[0] or not all(grants[1:5]) or grants[5] or grants[6] == "continuum_recovery":
        pytest.fail("T39 recovery database grants are not isolated")


def _reset_database(engine: Engine) -> None:
    with engine.begin() as connection:
        for schema in (
            "runtime_private",
            "wake_private",
            "webhook_private",
            "recovery_private",
            "continuum_migrations",
        ):
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        for role in ROLES:
            exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role)"),
                {"role": role},
            ).scalar_one()
            if exists:
                connection.execute(text(f"DROP OWNED BY {role}"))
                connection.execute(text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role}"))
                connection.execute(text(f"DROP ROLE {role}"))
        for role, mode in (
            ("continuum_ingress", "LOGIN"),
            ("continuum_wake_issuer", "NOLOGIN"),
            ("continuum_wake", "LOGIN"),
            ("continuum_wake_receiver", "LOGIN"),
            ("continuum_runtime", "LOGIN"),
            ("continuum_admission", "NOLOGIN"),
            ("continuum_recovery", "LOGIN"),
        ):
            password = f" PASSWORD '{secrets.token_urlsafe(32)}'" if mode == "LOGIN" else ""
            connection.exec_driver_sql(
                f"CREATE ROLE {role} {mode} NOSUPERUSER NOCREATEDB "
                f"NOCREATEROLE NOINHERIT NOBYPASSRLS{password}"
            )


def _run_migration(operation: str, revision: str, database_url: str) -> None:
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
    if result.returncode != 0:
        pytest.fail("T39 migration command failed; output redacted")


def _run_deno_integration(database_url: str) -> None:
    environment = os.environ.copy()
    environment["CONTINUUM_T39_TEST_DATABASE_URL"] = database_url
    result = subprocess.run(
        [
            "npm",
            "--prefix",
            "tools/edge",
            "exec",
            "--",
            "deno",
            "test",
            "--allow-env=CONTINUUM_T39_TEST_DATABASE_URL",
            f"--allow-net=localhost:{make_url(database_url).port},127.0.0.1:{make_url(database_url).port}",
            "--config",
            "supabase/functions/github-delivery-recovery/deno.json",
            "supabase/functions/github-delivery-recovery/integration_test.ts",
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        diagnostic = (result.stdout + result.stderr).replace(database_url, "<database>")
        pytest.fail(f"T39 database adapter integration failed: {diagnostic[-1500:]}")


def _cleanup_database(engine: Engine) -> None:
    with engine.begin() as connection:
        for schema in (
            "runtime_private",
            "wake_private",
            "webhook_private",
            "recovery_private",
            "continuum_migrations",
        ):
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        for role in ROLES:
            exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role)"),
                {"role": role},
            ).scalar_one()
            if exists:
                connection.execute(text(f"DROP OWNED BY {role}"))
                connection.execute(text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role}"))
                connection.execute(text(f"DROP ROLE {role}"))
