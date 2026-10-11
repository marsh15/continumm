"""Real PostgreSQL and Deno-driver proof for the T05 durable inbox."""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t05_test"
T05_REVISION = "20261011_01"
INGRESS_ROLE = "continuum_ingress"
APP_ID = 9_223_372_036_854_775_000
DELIVERY_IDS = (
    "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
)


def test_signed_deliveries_commit_once_and_migrations_roll_back() -> None:
    database_url = os.environ.get("CONTINUUM_T05_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T05_TEST_DATABASE_URL to disposable local PostgreSQL 17")
    url = make_url(database_url)
    sqlalchemy_url = url.set(drivername="postgresql+psycopg")
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T05 integration requires loopback PostgreSQL named continuum_t05_test")

    engine = create_engine(sqlalchemy_url, hide_parameters=True)
    ingress_password = secrets.token_urlsafe(32)
    ingress_url = _ingress_url(url, ingress_password)
    role_created = False
    migration_applied = False

    try:
        with engine.begin() as connection:
            server_version = connection.execute(
                text("SELECT current_setting('server_version_num')::integer")
            ).scalar_one()
            if not 170000 <= server_version < 180000:
                pytest.fail("T05 integration requires PostgreSQL 17")
            connection.execute(text("DROP SCHEMA IF EXISTS webhook_private CASCADE"))
            connection.execute(text("DROP SCHEMA IF EXISTS continuum_migrations CASCADE"))
            connection.execute(text("DROP TABLE IF EXISTS public.t05_unrelated"))
            connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
            role_exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :role_name)"),
                {"role_name": INGRESS_ROLE},
            ).scalar_one()
            if role_exists:
                connection.execute(
                    text("REVOKE CONNECT ON DATABASE continuum_t05_test FROM continuum_ingress")
                )
            connection.execute(text("DROP ROLE IF EXISTS continuum_ingress"))
            connection.exec_driver_sql(
                "CREATE ROLE continuum_ingress LOGIN NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOINHERIT NOBYPASSRLS "
                f"PASSWORD '{ingress_password}'"
            )
            connection.execute(text("CREATE TABLE public.t05_unrelated (secret text NOT NULL)"))
            connection.execute(
                text("INSERT INTO public.t05_unrelated (secret) VALUES ('private-fixture')")
            )
        role_created = True

        with engine.begin() as connection:
            connection.execute(text("CREATE ROLE t05_membership_test NOLOGIN"))
            connection.execute(text("GRANT t05_membership_test TO continuum_ingress"))
        _run_migration("upgrade", T05_REVISION, database_url, expect_success=False)
        _assert_inbox_schema_absent(engine)

        with engine.begin() as connection:
            connection.execute(text("REVOKE t05_membership_test FROM continuum_ingress"))
            connection.execute(text("DROP ROLE t05_membership_test"))
            connection.execute(text("ALTER ROLE continuum_ingress BYPASSRLS"))
        _run_migration("upgrade", T05_REVISION, database_url, expect_success=False)
        _assert_inbox_schema_absent(engine)
        with engine.begin() as connection:
            connection.execute(text("ALTER ROLE continuum_ingress NOBYPASSRLS"))

        _run_migration("upgrade", T05_REVISION, database_url)
        migration_applied = True
        _assert_ingress_privileges(engine, ingress_url)
        _assert_migration_metadata_is_private(engine)
        _run_actual_typescript_integration(ingress_url)
        _assert_committed_raw_payloads(engine)

        with engine.begin() as connection:
            connection.execute(
                text(
                    "DELETE FROM webhook_private.github_delivery_inbox "
                    "WHERE github_app_id = :app_id"
                ),
                {"app_id": APP_ID},
            )
        _run_migration("downgrade", "base", database_url)
        migration_applied = False
        with engine.connect() as connection:
            schema_exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'webhook_private')")
            ).scalar_one()
        assert schema_exists is False

        _run_migration("upgrade", T05_REVISION, database_url)
        migration_applied = True
        with engine.connect() as connection:
            table_exists = connection.execute(
                text(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'webhook_private' "
                    "AND table_name = 'github_delivery_inbox')"
                )
            ).scalar_one()
        assert table_exists is True
    finally:
        try:
            if migration_applied:
                try:
                    _run_migration("downgrade", "base", database_url)
                except Exception:
                    pass
            if role_created:
                with engine.begin() as connection:
                    connection.execute(text("DROP SCHEMA IF EXISTS webhook_private CASCADE"))
                    connection.execute(text("DROP SCHEMA IF EXISTS continuum_migrations CASCADE"))
                    membership_role_exists = connection.execute(
                        text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :role_name)"),
                        {"role_name": "t05_membership_test"},
                    ).scalar_one()
                    if membership_role_exists:
                        connection.execute(
                            text("REVOKE t05_membership_test FROM continuum_ingress")
                        )
                        connection.execute(text("DROP ROLE t05_membership_test"))
                    connection.execute(text("DROP TABLE IF EXISTS public.t05_unrelated"))
                    connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
                    connection.execute(
                        text("REVOKE CONNECT ON DATABASE continuum_t05_test FROM continuum_ingress")
                    )
                    connection.execute(text("DROP ROLE IF EXISTS continuum_ingress"))
        finally:
            engine.dispose()


def _run_migration(
    operation: str,
    revision: str,
    database_url: str,
    *,
    expect_success: bool = True,
) -> None:
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
    if (result.returncode == 0) is not expect_success:
        pytest.fail("migration_command_failed; output redacted")


def _assert_inbox_schema_absent(engine: Engine) -> None:
    with engine.connect() as connection:
        schema_exists = connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'webhook_private')")
        ).scalar_one()
    assert schema_exists is False


def _assert_migration_metadata_is_private(engine: Engine) -> None:
    with engine.connect() as connection:
        private_table_exists = connection.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'continuum_migrations' "
                "AND table_name = 'alembic_version')"
            )
        ).scalar_one()
        public_table_exists = connection.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'alembic_version')"
            )
        ).scalar_one()
    assert private_table_exists is True
    assert public_table_exists is False


def _ingress_url(url: URL, password: str) -> str:
    query = dict(url.query)
    query["sslmode"] = "disable"
    return url.set(username=INGRESS_ROLE, password=password, query=query).render_as_string(
        hide_password=False
    )


def _assert_ingress_privileges(engine: Engine, ingress_url: str) -> None:
    with engine.connect() as connection:
        privileges = connection.execute(
            text(
                """
                SELECT
                    has_schema_privilege(:role_name, 'webhook_private', 'USAGE'),
                    has_schema_privilege(:role_name, 'webhook_private', 'CREATE'),
                    has_column_privilege(
                        :role_name, 'webhook_private.github_delivery_inbox', 'raw_body', 'INSERT'
                    ),
                    has_column_privilege(
                        :role_name, 'webhook_private.github_delivery_inbox', 'received_at', 'INSERT'
                    ),
                    has_column_privilege(
                        :role_name,
                        'webhook_private.github_delivery_inbox',
                        'payload_sha256',
                        'SELECT'
                    ),
                    has_column_privilege(
                        :role_name, 'webhook_private.github_delivery_inbox', 'raw_body', 'SELECT'
                    ),
                    has_table_privilege(
                        :role_name, 'webhook_private.github_delivery_inbox', 'UPDATE'
                    ),
                    has_table_privilege(
                        :role_name, 'webhook_private.github_delivery_inbox', 'DELETE'
                    ),
                    has_database_privilege(:role_name, current_database(), 'CREATE'),
                    pg_has_role(:role_name, 'postgres', 'MEMBER'),
                    has_schema_privilege(:role_name, 'continuum_migrations', 'USAGE'),
                    pg_get_userbyid((SELECT relowner FROM pg_class WHERE
                        oid = 'webhook_private.github_delivery_inbox'::regclass)) <> :role_name
                """
            ),
            {"role_name": INGRESS_ROLE},
        ).one()
        rls = connection.execute(
            text(
                """
                SELECT relrowsecurity, relforcerowsecurity
                FROM pg_class
                WHERE oid = 'webhook_private.github_delivery_inbox'::regclass
                """
            )
        ).one()

    assert tuple(privileges) == (
        True,
        False,
        True,
        False,
        True,
        False,
        False,
        False,
        False,
        False,
        False,
        True,
    )
    assert tuple(rls) == (True, True)

    ingress_engine = create_engine(
        make_url(ingress_url).set(drivername="postgresql+psycopg"),
        hide_parameters=True,
    )
    try:
        with ingress_engine.connect() as connection:
            assert connection.execute(text("SELECT current_user")).scalar_one() == INGRESS_ROLE
            with pytest.raises(DBAPIError):
                connection.execute(text("SELECT secret FROM public.t05_unrelated"))
        with ingress_engine.connect() as connection:
            with pytest.raises(DBAPIError):
                connection.execute(
                    text("SELECT version_num FROM continuum_migrations.alembic_version")
                )
        with ingress_engine.connect() as connection:
            with pytest.raises(DBAPIError):
                connection.execute(
                    text("SELECT raw_body FROM webhook_private.github_delivery_inbox LIMIT 1")
                )
        with ingress_engine.connect() as connection:
            with pytest.raises(DBAPIError):
                connection.execute(
                    text(
                        "UPDATE webhook_private.github_delivery_inbox SET action = NULL WHERE false"
                    )
                )
        with ingress_engine.connect() as connection:
            with pytest.raises(DBAPIError):
                connection.execute(
                    text("DELETE FROM webhook_private.github_delivery_inbox WHERE false")
                )
        with ingress_engine.connect() as connection:
            with pytest.raises(DBAPIError):
                connection.execute(
                    text(
                        "CREATE TABLE webhook_private.ingress_must_not_ddl (value integer NOT NULL)"
                    )
                )
        with ingress_engine.connect() as connection:
            with pytest.raises(DBAPIError):
                connection.execute(
                    text(
                        "INSERT INTO webhook_private.github_delivery_inbox "
                        "(github_app_id, delivery_id, event_type, action, payload_sha256, "
                        "raw_body, received_at) "
                        "VALUES (1, gen_random_uuid(), 'issue_comment', NULL, "
                        "decode(repeat('00', 32), 'hex'), '\\x7b7d', now())"
                    )
                )
    finally:
        ingress_engine.dispose()


def _run_actual_typescript_integration(ingress_url: str) -> None:
    environment = os.environ.copy()
    environment["CONTINUUM_TEST_INGRESS_DATABASE_URL"] = ingress_url
    result = subprocess.run(
        ["npm", "--prefix", "tools/edge", "run", "test-integration"],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail("deno_webhook_integration_failed; output redacted")


def _assert_committed_raw_payloads(engine: Engine) -> None:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT delivery_id::text, action, raw_body
                FROM webhook_private.github_delivery_inbox
                WHERE github_app_id = :app_id
                ORDER BY delivery_id
                """
            ),
            {"app_id": APP_ID},
        ).all()
    expected_bodies = {
        DELIVERY_IDS[0]: b'{"action":"created","issue":{"number":7}}',
        DELIVERY_IDS[1]: b'{"action":"created","issue":{"number":8}}',
        DELIVERY_IDS[2]: b'{"action":"created","issue":{"number":9}}',
    }
    assert len(rows) == len(expected_bodies)
    assert {row.delivery_id: row.raw_body for row in rows} == expected_bodies
    assert {row.action for row in rows} == {"created"}
