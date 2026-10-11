"""PostgreSQL and Edge-TypeScript proof for work-triggered wake boundaries."""

from __future__ import annotations

import os
import re
import secrets
import socket
import subprocess
import sys
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from typing import Any
from urllib.request import urlopen

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, Connection, make_url
from sqlalchemy.exc import DBAPIError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t06_test"
T05_REVISION = "20261011_01"
T06_REVISION = "20261011_02"
APP_ID = 7_777_777_777
DELIVERY_ID = "77777777-7777-4777-8777-777777777777"
ISSUER_ROLE = "continuum_wake_issuer"
EDGE_ROLE = "continuum_wake"
RECEIVER_ROLE = "continuum_wake_receiver"
INGRESS_ROLE = "continuum_ingress"
MIGRATION_ROLE = "continuum_t06_migrator"


def test_dispatch_is_fenced_and_receipts_do_not_claim_a_drain() -> None:
    database_url = os.environ.get("CONTINUUM_T06_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T06_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T06 integration requires loopback PostgreSQL named continuum_t06_test")

    engine = create_engine(
        url.set(drivername="postgresql+psycopg"),
        hide_parameters=True,
        pool_size=4,
        max_overflow=0,
    )
    edge_password = secrets.token_urlsafe(32)
    receiver_password = secrets.token_urlsafe(32)
    migration_password = secrets.token_urlsafe(32)
    edge_url = _role_url(url, EDGE_ROLE, edge_password)
    receiver_url = _role_url(url, RECEIVER_ROLE, receiver_password)
    migration_url = _role_url(url, MIGRATION_ROLE, migration_password)
    roles_created = False

    try:
        _reset_database(engine)
        _assert_postgres_17(engine)
        _create_roles(engine, edge_password, receiver_password, migration_password)
        roles_created = True
        _run_migration("upgrade", T05_REVISION, migration_url)

        _assert_migration_rejects_edge_membership(engine, migration_url)
        _run_migration("upgrade", T06_REVISION, migration_url)
        _assert_schema_and_role_grants(engine)
        _assert_no_dispatch_when_inbox_is_idle(engine)
        _seed_pending_delivery(engine)
        _assert_ingress_cannot_write_consumed_marker(engine)
        _assert_ticket_fencing_receipt_and_backoff(engine, edge_url, receiver_url)
        _run_actual_edge_integration(edge_url, receiver_url, engine)
        _run_migration("downgrade", T05_REVISION, migration_url)
        _assert_t06_downgrade_preserved_t05(engine)
        _run_migration("upgrade", T06_REVISION, migration_url)
        _assert_schema_and_role_grants(engine)
    finally:
        try:
            if roles_created:
                _drop_database_fixture(engine)
        finally:
            engine.dispose()


def _reset_database(engine: Engine) -> None:
    with engine.begin() as connection:
        for schema_name in ("wake_private", "webhook_private", "continuum_migrations"):
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.t06_unrelated"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        _revoke_test_database_grants(connection)
        issuer_exists = connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)"),
            {"name": ISSUER_ROLE},
        ).scalar_one()
        if issuer_exists:
            issuer_members = (
                connection.execute(
                    text(
                        "SELECT pg_get_userbyid(member) FROM pg_auth_members "
                        "WHERE roleid=(SELECT oid FROM pg_roles WHERE rolname=:name)"
                    ),
                    {"name": ISSUER_ROLE},
                )
                .scalars()
                .all()
            )
            for member in issuer_members:
                quoted_member = connection.dialect.identifier_preparer.quote(member)
                connection.execute(text(f"REVOKE {ISSUER_ROLE} FROM {quoted_member}"))
        for role_name in (ISSUER_ROLE, EDGE_ROLE, RECEIVER_ROLE, INGRESS_ROLE, MIGRATION_ROLE):
            role_exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)"),
                {"name": role_name},
            ).scalar_one()
            if role_exists:
                connection.execute(text(f"DROP OWNED BY {role_name}"))
                database_name = connection.execute(text("SELECT current_database()")).scalar_one()
                quoted_database = connection.dialect.identifier_preparer.quote(database_name)
                connection.execute(
                    text(f"REVOKE ALL PRIVILEGES ON DATABASE {quoted_database} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE IF EXISTS {role_name}"))


def _assert_postgres_17(engine: Engine) -> None:
    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT current_setting('server_version_num')::integer")
        ).scalar_one()
    if not 170000 <= version < 180000:
        pytest.fail("T06 integration requires PostgreSQL 17")


def _create_roles(
    engine: Engine,
    edge_password: str,
    receiver_password: str,
    migration_password: str,
) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE ROLE continuum_ingress LOGIN NOINHERIT NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        )
        connection.exec_driver_sql(
            "CREATE ROLE continuum_wake_issuer NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        )
        connection.exec_driver_sql(
            "CREATE ROLE continuum_wake LOGIN NOINHERIT NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE NOREPLICATION NOBYPASSRLS "
            f"PASSWORD '{edge_password}'"
        )
        connection.exec_driver_sql(
            "CREATE ROLE continuum_wake_receiver LOGIN NOINHERIT NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE NOREPLICATION NOBYPASSRLS "
            f"PASSWORD '{receiver_password}'"
        )
        connection.exec_driver_sql(
            "CREATE ROLE continuum_t06_migrator LOGIN NOINHERIT NOSUPERUSER NOCREATEDB "
            "NOCREATEROLE NOREPLICATION NOBYPASSRLS "
            f"PASSWORD '{migration_password}'"
        )
        connection.execute(
            text(
                "GRANT CONNECT, CREATE ON DATABASE continuum_t06_test "
                "TO continuum_t06_migrator WITH GRANT OPTION"
            )
        )
        connection.execute(text("GRANT continuum_wake_issuer TO continuum_t06_migrator"))
        connection.execute(text("CREATE TABLE public.t06_unrelated (secret text NOT NULL)"))
        connection.execute(
            text("INSERT INTO public.t06_unrelated (secret) VALUES ('private-fixture')")
        )


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
        raise RuntimeError("T06_migration_command_failed; output redacted")


def _assert_migration_rejects_edge_membership(engine: Engine, migration_url: str) -> None:
    with engine.begin() as connection:
        connection.execute(text("CREATE ROLE t06_forbidden_group NOLOGIN"))
        connection.execute(text(f"GRANT t06_forbidden_group TO {EDGE_ROLE}"))
    _run_migration("upgrade", T06_REVISION, migration_url, expect_success=False)
    with engine.connect() as connection:
        wake_schema_exists = connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='wake_private')")
        ).scalar_one()
        consumed_marker_exists = connection.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_schema='webhook_private' "
                "AND table_name='github_delivery_inbox' AND column_name='consumed_at')"
            )
        ).scalar_one()
    assert wake_schema_exists is False
    assert consumed_marker_exists is False
    with engine.begin() as connection:
        connection.execute(text(f"REVOKE t06_forbidden_group FROM {EDGE_ROLE}"))
        connection.execute(text("DROP ROLE t06_forbidden_group"))


def _role_url(url: URL, role_name: str, password: str) -> str:
    query = dict(url.query)
    query["sslmode"] = "disable"
    return url.set(username=role_name, password=password, query=query).render_as_string(
        hide_password=False
    )


def _assert_schema_and_role_grants(engine: Engine) -> None:
    with engine.connect() as connection:
        roles = connection.execute(
            text(
                """
                SELECT
                    (SELECT rolcanlogin FROM pg_roles WHERE rolname=:issuer),
                    (SELECT rolcanlogin FROM pg_roles WHERE rolname=:edge),
                    (SELECT rolcanlogin FROM pg_roles WHERE rolname=:receiver),
                    has_column_privilege(:issuer, 'webhook_private.github_delivery_inbox',
                                         'consumed_at', 'SELECT'),
                    has_column_privilege(:issuer, 'webhook_private.github_delivery_inbox',
                                         'raw_body', 'SELECT'),
                    has_column_privilege(:issuer, 'webhook_private.github_delivery_inbox',
                                         'consumed_at', 'UPDATE'),
                    has_column_privilege(:edge, 'webhook_private.github_delivery_inbox',
                                         'consumed_at', 'SELECT'),
                    has_column_privilege(:ingress, 'webhook_private.github_delivery_inbox',
                                         'consumed_at', 'INSERT'),
                    has_column_privilege(:ingress, 'webhook_private.github_delivery_inbox',
                                         'consumed_at', 'UPDATE'),
                    has_table_privilege(:edge, 'wake_private.dispatch_control', 'SELECT'),
                    has_table_privilege(:receiver, 'wake_private.wake_receipt', 'INSERT'),
                    has_function_privilege(:edge,
                        'wake_private.consume_dispatch_ticket(uuid,text)', 'EXECUTE'),
                    has_function_privilege(:edge,
                        'wake_private.issue_dispatch_ticket()', 'EXECUTE'),
                    has_function_privilege(:receiver,
                        'wake_private.accept_wake_receipt(uuid,bigint,uuid,text)', 'EXECUTE'),
                    has_function_privilege(:receiver,
                        'wake_private.read_wake_status()', 'EXECUTE'),
                    (SELECT relrowsecurity AND relforcerowsecurity FROM pg_class
                     WHERE oid='wake_private.dispatch_control'::regclass),
                    (SELECT last_successful_drain_at IS NULL FROM wake_private.drain_watermark
                     WHERE singleton=true)
                """
            ),
            {
                "issuer": ISSUER_ROLE,
                "edge": EDGE_ROLE,
                "ingress": INGRESS_ROLE,
                "receiver": RECEIVER_ROLE,
            },
        ).one()
        inbox_columns = (
            connection.execute(
                text(
                    """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='webhook_private' AND table_name='github_delivery_inbox'
                """
                )
            )
            .scalars()
            .all()
        )
        migration_schemas = (
            connection.execute(
                text(
                    "SELECT table_schema FROM information_schema.tables "
                    "WHERE table_name='alembic_version'"
                )
            )
            .scalars()
            .all()
        )
        function_security = connection.execute(
            text(
                """
                SELECT p.prosecdef, p.proconfig
                FROM pg_proc p
                WHERE p.oid='wake_private.accept_wake_receipt(uuid,bigint,uuid,text)'::regprocedure
                """
            )
        ).one()

    assert tuple(roles) == (
        False,
        True,
        True,
        True,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
        True,
        False,
        True,
        True,
        True,
        True,
    )
    assert "consumed_at" in inbox_columns
    assert migration_schemas == ["continuum_migrations"]
    assert function_security.prosecdef is True
    assert function_security.proconfig == ["search_path=pg_catalog"]

    _assert_role_connection_privileges(engine, EDGE_ROLE)
    _assert_role_connection_privileges(engine, RECEIVER_ROLE)


def _assert_role_connection_privileges(engine: Engine, role_name: str) -> None:
    with engine.connect() as connection:
        connection.execute(text(f"SET ROLE {role_name}"))
        assert connection.execute(text("SELECT current_user")).scalar_one() == role_name
        with pytest.raises(DBAPIError):
            connection.execute(text("SELECT raw_body FROM webhook_private.github_delivery_inbox"))
        connection.rollback()
        connection.execute(text(f"SET ROLE {role_name}"))
        with pytest.raises(DBAPIError):
            connection.execute(text("SELECT * FROM wake_private.dispatch_control"))
        connection.rollback()
        connection.execute(text(f"SET ROLE {role_name}"))
        with pytest.raises(DBAPIError):
            connection.execute(text("SELECT secret FROM public.t06_unrelated"))


def _assert_ingress_cannot_write_consumed_marker(engine: Engine) -> None:
    with engine.connect() as connection:
        connection.execute(text(f"SET ROLE {INGRESS_ROLE}"))
        with pytest.raises(DBAPIError):
            connection.execute(
                text(
                    "UPDATE webhook_private.github_delivery_inbox "
                    "SET consumed_at=clock_timestamp() "
                    "WHERE github_app_id=:app_id AND delivery_id=:delivery_id"
                ),
                {"app_id": APP_ID, "delivery_id": DELIVERY_ID},
            )
        connection.rollback()
        connection.execute(text(f"SET ROLE {INGRESS_ROLE}"))
        with pytest.raises(DBAPIError):
            connection.execute(
                text(
                    "INSERT INTO webhook_private.github_delivery_inbox "
                    "(github_app_id, delivery_id, event_type, payload_sha256, raw_body, "
                    "consumed_at) "
                    "VALUES (:app_id, :delivery_id, 'issue_comment', "
                    "decode(repeat('11', 32), 'hex'), "
                    "decode('7b7d', 'hex'), clock_timestamp())"
                ),
                {"app_id": APP_ID, "delivery_id": "77777777-7777-4777-8777-777777777778"},
            )
        connection.rollback()


def _assert_t06_downgrade_preserved_t05(engine: Engine) -> None:
    with engine.connect() as connection:
        wake_schema_exists = connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='wake_private')")
        ).scalar_one()
        consumed_marker_exists = connection.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_schema='webhook_private' "
                "AND table_name='github_delivery_inbox' AND column_name='consumed_at')"
            )
        ).scalar_one()
        t05_table_exists = connection.execute(
            text("SELECT to_regclass('webhook_private.github_delivery_inbox') IS NOT NULL")
        ).scalar_one()
        revision = connection.execute(
            text("SELECT version_num FROM continuum_migrations.alembic_version")
        ).scalar_one()
    assert wake_schema_exists is False
    assert consumed_marker_exists is False
    assert t05_table_exists is True
    assert revision == T05_REVISION


def _assert_no_dispatch_when_inbox_is_idle(engine: Engine) -> None:
    with engine.connect() as connection:
        ticket = (
            connection.execute(text("SELECT * FROM wake_private.issue_dispatch_ticket()"))
            .mappings()
            .all()
        )
    assert ticket == []


def _seed_pending_delivery(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO webhook_private.github_delivery_inbox
                    (github_app_id, delivery_id, event_type, action, payload_sha256, raw_body)
                VALUES (:app_id, :delivery_id, 'issue_comment', 'created',
                        decode(repeat('11', 32), 'hex'), decode('7b7d', 'hex'))
                """
            ),
            {"app_id": APP_ID, "delivery_id": DELIVERY_ID},
        )
        consumed_at = connection.execute(
            text(
                "SELECT consumed_at FROM webhook_private.github_delivery_inbox "
                "WHERE github_app_id=:app_id AND delivery_id=:delivery_id"
            ),
            {"app_id": APP_ID, "delivery_id": DELIVERY_ID},
        ).scalar_one()
    assert consumed_at is None


def _issue_ticket(engine: Engine) -> dict[str, Any] | None:
    with engine.begin() as connection:
        return (
            connection.execute(text("SELECT * FROM wake_private.issue_dispatch_ticket()"))
            .mappings()
            .one_or_none()
        )


def _consume_ticket(engine: Engine, ticket: dict[str, Any]) -> dict[str, Any]:
    with engine.begin() as connection:
        return (
            connection.execute(
                text(
                    "SELECT * FROM wake_private.consume_dispatch_ticket "
                    "(CAST(:ticket_id AS uuid), CAST(:ticket_value AS text))"
                ),
                {"ticket_id": ticket["ticket_id"], "ticket_value": ticket["ticket_value"]},
            )
            .mappings()
            .one()
        )


def _record_dispatch(
    engine: Engine,
    ticket: dict[str, Any],
    *,
    outcome: str,
    http_status: int | None,
) -> bool:
    with engine.begin() as connection:
        return connection.execute(
            text(
                "SELECT recorded FROM wake_private.record_dispatch_result "
                "(CAST(:attempt_id AS uuid), CAST(:generation AS bigint), "
                "CAST(:outcome AS text), CAST(:http_status AS smallint))"
            ),
            {
                "attempt_id": ticket["attempt_id"],
                "generation": ticket["generation"],
                "outcome": outcome,
                "http_status": http_status,
            },
        ).scalar_one()


def _accept_receipt(
    engine: Engine,
    ticket: dict[str, Any],
    *,
    reason: str = "pending_inbox",
) -> dict[str, Any]:
    with engine.begin() as connection:
        return (
            connection.execute(
                text(
                    "SELECT * FROM wake_private.accept_wake_receipt "
                    "(CAST(:attempt_id AS uuid), CAST(:generation AS bigint), "
                    "CAST(:wake_id AS uuid), CAST(:reason AS text))"
                ),
                {
                    "attempt_id": ticket["attempt_id"],
                    "generation": ticket["generation"],
                    "wake_id": ticket["wake_id"],
                    "reason": reason,
                },
            )
            .mappings()
            .one()
        )


def _assert_ticket_fencing_receipt_and_backoff(
    admin_engine: Engine,
    edge_url: str,
    receiver_url: str,
) -> None:
    edge_engine = create_engine(
        make_url(edge_url).set(drivername="postgresql+psycopg"),
        hide_parameters=True,
        pool_size=2,
        max_overflow=0,
    )
    receiver_engine = create_engine(
        make_url(receiver_url).set(drivername="postgresql+psycopg"),
        hide_parameters=True,
        pool_size=2,
        max_overflow=0,
    )
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            ticket_results = list(executor.map(lambda _: _issue_ticket(admin_engine), range(2)))
        issued = [ticket for ticket in ticket_results if ticket is not None]
        assert len(issued) == 1
        expired_ticket = issued[0]

        with admin_engine.begin() as connection:
            stored = connection.execute(
                text(
                    "SELECT ticket_sha256 = sha256(convert_to(:ticket_value, 'UTF8')) "
                    "FROM wake_private.dispatch_ticket WHERE ticket_id=:ticket_id"
                ),
                {
                    "ticket_value": expired_ticket["ticket_value"],
                    "ticket_id": expired_ticket["ticket_id"],
                },
            ).scalar_one()
            connection.execute(
                text(
                    "UPDATE wake_private.dispatch_ticket SET expires_at=clock_timestamp() "
                    "- interval '1 second' WHERE ticket_id=:ticket_id"
                ),
                {"ticket_id": expired_ticket["ticket_id"]},
            )
        assert stored is True
        assert _consume_ticket(edge_engine, expired_ticket)["authorized"] is False

        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE wake_private.dispatch_control SET active_attempt_id=NULL, "
                    "lease_expires_at=NULL, next_attempt_at=NULL, "
                    "known_capacity_hold_until=NULL WHERE singleton=true"
                )
            )
        first_ticket = _issue_ticket(admin_engine)
        assert first_ticket is not None
        consumed = _consume_ticket(edge_engine, first_ticket)
        assert consumed["authorized"] is True
        assert consumed["wake_id"] == first_ticket["wake_id"]
        assert _consume_ticket(edge_engine, first_ticket)["authorized"] is False

        first_receipt = _accept_receipt(receiver_engine, first_ticket)
        assert first_receipt["outcome"] == "accepted"
        assert first_receipt["drain_status"] == "worker_not_started"
        assert first_receipt["last_successful_drain_at"] is None
        replayed_receipt = _accept_receipt(receiver_engine, first_ticket)
        assert replayed_receipt["outcome"] == "duplicate"
        assert replayed_receipt["receipt_id"] == first_receipt["receipt_id"]

        assert (
            _record_dispatch(edge_engine, first_ticket, outcome="accepted", http_status=202) is True
        )
        assert (
            _record_dispatch(edge_engine, first_ticket, outcome="unknown", http_status=503) is False
        )
        with admin_engine.connect() as connection:
            next_attempt_at = connection.execute(
                text("SELECT next_attempt_at FROM wake_private.dispatch_control")
            ).scalar_one()
            watermark = connection.execute(
                text("SELECT last_successful_drain_at FROM wake_private.drain_watermark")
            ).scalar_one()
            inbox_consumed_at = connection.execute(
                text(
                    "SELECT consumed_at FROM webhook_private.github_delivery_inbox "
                    "WHERE github_app_id=:app_id AND delivery_id=:delivery_id"
                ),
                {"app_id": APP_ID, "delivery_id": DELIVERY_ID},
            ).scalar_one()
        assert next_attempt_at is not None
        assert watermark is None
        assert inbox_consumed_at is None
        assert _issue_ticket(admin_engine) is None

        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE wake_private.dispatch_control SET next_attempt_at=clock_timestamp() "
                    "- interval '1 second' WHERE singleton=true"
                )
            )
        second_ticket = _issue_ticket(admin_engine)
        assert second_ticket is not None
        assert second_ticket["wake_id"] == first_ticket["wake_id"]
        assert second_ticket["generation"] > first_ticket["generation"]
        assert (
            _record_dispatch(edge_engine, first_ticket, outcome="unknown", http_status=503) is False
        )
        stale_receipt = _accept_receipt(receiver_engine, first_ticket)
        assert stale_receipt["outcome"] == "stale"
        assert _consume_ticket(edge_engine, second_ticket)["authorized"] is True
        duplicate_receipt = _accept_receipt(receiver_engine, second_ticket)
        assert duplicate_receipt["outcome"] == "duplicate"
        assert duplicate_receipt["receipt_id"] == first_receipt["receipt_id"]
        assert (
            _record_dispatch(edge_engine, second_ticket, outcome="accepted", http_status=202)
            is True
        )

        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE wake_private.dispatch_control SET next_attempt_at=clock_timestamp() "
                    "- interval '1 second' WHERE singleton=true"
                )
            )
        third_ticket = _issue_ticket(admin_engine)
        assert third_ticket is not None
        assert _consume_ticket(edge_engine, third_ticket)["authorized"] is True
        assert (
            _record_dispatch(
                edge_engine, third_ticket, outcome="retryable_failure", http_status=503
            )
            is True
        )
        with admin_engine.connect() as connection:
            backoff = connection.execute(
                text(
                    "SELECT failure_count, next_attempt_at > clock_timestamp() "
                    "FROM wake_private.dispatch_control WHERE singleton=true"
                )
            ).one()
        assert tuple(backoff) == (1, True)
        assert _issue_ticket(admin_engine) is None

        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE wake_private.dispatch_control SET next_attempt_at=NULL, "
                    "known_capacity_hold_until=clock_timestamp() + interval '10 minutes', "
                    "active_attempt_id=NULL, lease_expires_at=NULL WHERE singleton=true"
                )
            )
        assert _issue_ticket(admin_engine) is None
        with receiver_engine.connect() as connection:
            status = (
                connection.execute(text("SELECT * FROM wake_private.read_wake_status()"))
                .mappings()
                .one()
            )
        assert status["last_successful_drain_at"] is None
        assert status["receipt_state"] == "worker_not_started"
        _assert_database_clock_after_lock_wait(admin_engine, edge_engine, receiver_engine)
    finally:
        edge_engine.dispose()
        receiver_engine.dispose()


def _assert_database_clock_after_lock_wait(
    admin_engine: Engine,
    edge_engine: Engine,
    receiver_engine: Engine,
) -> None:
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE wake_private.dispatch_control SET active_attempt_id=NULL, "
                "lease_expires_at=NULL, next_attempt_at=NULL, known_capacity_hold_until=NULL "
                "WHERE singleton=true"
            )
        )
    consume_ticket = _issue_ticket(admin_engine)
    assert consume_ticket is not None
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE wake_private.dispatch_ticket SET expires_at=clock_timestamp() "
                "+ interval '1 second' WHERE ticket_id=:ticket_id"
            ),
            {"ticket_id": consume_ticket["ticket_id"]},
        )
        connection.execute(
            text(
                "UPDATE wake_private.dispatch_control SET lease_expires_at=clock_timestamp() "
                "+ interval '1 second' WHERE singleton=true"
            )
        )
    consume_result = _run_while_control_locked(
        admin_engine,
        edge_engine,
        application_name="t06-consume-ticket-wait",
        invoke=lambda connection: connection.execute(
            text(
                "SELECT authorized FROM wake_private.consume_dispatch_ticket "
                "(CAST(:ticket_id AS uuid), CAST(:ticket_value AS text))"
            ),
            {
                "ticket_id": consume_ticket["ticket_id"],
                "ticket_value": consume_ticket["ticket_value"],
            },
        ).scalar_one(),
    )
    assert consume_result is False
    with admin_engine.connect() as connection:
        consumed_at = connection.execute(
            text("SELECT consumed_at FROM wake_private.dispatch_ticket WHERE ticket_id=:ticket_id"),
            {"ticket_id": consume_ticket["ticket_id"]},
        ).scalar_one()
    assert consumed_at is None

    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE wake_private.dispatch_control SET active_attempt_id=NULL, "
                "lease_expires_at=NULL, next_attempt_at=NULL WHERE singleton=true"
            )
        )
    result_ticket = _issue_ticket(admin_engine)
    assert result_ticket is not None
    assert _consume_ticket(edge_engine, result_ticket)["authorized"] is True
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE wake_private.dispatch_control SET lease_expires_at=clock_timestamp() "
                "+ interval '1 second', last_dispatch_outcome='claimed' WHERE singleton=true"
            )
        )
    result_recorded = _run_while_control_locked(
        admin_engine,
        edge_engine,
        application_name="t06-record-result-wait",
        invoke=lambda connection: connection.execute(
            text(
                "SELECT recorded FROM wake_private.record_dispatch_result "
                "(CAST(:attempt_id AS uuid), CAST(:generation AS bigint), 'accepted', "
                "CAST(202 AS smallint))"
            ),
            {
                "attempt_id": result_ticket["attempt_id"],
                "generation": result_ticket["generation"],
            },
        ).scalar_one(),
    )
    assert result_recorded is False
    with admin_engine.connect() as connection:
        dispatch_state = connection.execute(
            text(
                "SELECT last_dispatch_outcome, last_successful_drain_at "
                "FROM wake_private.dispatch_control AS c "
                "CROSS JOIN wake_private.drain_watermark AS w WHERE c.singleton=true"
            )
        ).one()
    assert tuple(dispatch_state) == ("claimed", None)

    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE wake_private.dispatch_control SET active_attempt_id=NULL, "
                "lease_expires_at=NULL, next_attempt_at=NULL WHERE singleton=true"
            )
        )
    receipt_ticket = _issue_ticket(admin_engine)
    assert receipt_ticket is not None
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE wake_private.dispatch_control SET lease_expires_at=clock_timestamp() "
                "+ interval '1 second' WHERE singleton=true"
            )
        )
    receipt = _run_while_control_locked(
        admin_engine,
        receiver_engine,
        application_name="t06-accept-receipt-wait",
        invoke=lambda connection: connection.execute(
            text(
                "SELECT outcome FROM wake_private.accept_wake_receipt "
                "(CAST(:attempt_id AS uuid), CAST(:generation AS bigint), "
                "CAST(:wake_id AS uuid), 'pending_inbox')"
            ),
            {
                "attempt_id": receipt_ticket["attempt_id"],
                "generation": receipt_ticket["generation"],
                "wake_id": receipt_ticket["wake_id"],
            },
        ).scalar_one(),
    )
    assert receipt == "stale"
    with admin_engine.connect() as connection:
        receipt_count = connection.execute(
            text("SELECT count(*) FROM wake_private.wake_receipt")
        ).scalar_one()
        watermark = connection.execute(
            text("SELECT last_successful_drain_at FROM wake_private.drain_watermark")
        ).scalar_one()
    assert receipt_count == 1
    assert watermark is None


def _run_while_control_locked(
    admin_engine: Engine,
    caller_engine: Engine,
    *,
    application_name: str,
    invoke: Callable[[Connection], Any],
) -> Any:
    started = Event()

    def call() -> Any:
        with caller_engine.begin() as connection:
            connection.execute(
                text("SELECT set_config('application_name', :name, true)"),
                {"name": application_name},
            )
            started.set()
            return invoke(connection)

    blocker = admin_engine.connect()
    transaction = blocker.begin()
    try:
        blocker.execute(
            text(
                "SELECT singleton FROM wake_private.dispatch_control "
                "WHERE singleton=true FOR UPDATE"
            )
        )
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(call)
            assert started.wait(timeout=2)
            deadline = time.monotonic() + 3
            blocked = False
            while time.monotonic() < deadline:
                with admin_engine.connect() as monitor:
                    wait_type = monitor.execute(
                        text(
                            "SELECT wait_event_type FROM pg_stat_activity "
                            "WHERE application_name=:name"
                        ),
                        {"name": application_name},
                    ).scalar_one_or_none()
                if wait_type == "Lock":
                    blocked = True
                    break
                time.sleep(0.02)
            if blocked:
                time.sleep(1.25)
            transaction.commit()
            result = future.result(timeout=3)
            assert blocked is True
            return result
    finally:
        if transaction.is_active:
            transaction.rollback()
        blocker.close()


def _run_actual_edge_integration(
    edge_url: str,
    receiver_url: str,
    admin_engine: Engine,
) -> None:
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE wake_private.dispatch_control SET next_attempt_at=NULL, "
                "known_capacity_hold_until=NULL, active_attempt_id=NULL, "
                "lease_expires_at=NULL WHERE singleton=true"
            )
        )
    ticket = _issue_ticket(admin_engine)
    if ticket is None:
        pytest.fail("T06_edge_fixture_ticket_missing")

    backend_token = "local-only-wake-token-0000000000000000"
    backend_process, backend_url = _start_local_wake_api(receiver_url, backend_token)
    environment = os.environ.copy()
    environment.update(
        {
            "CONTINUUM_TEST_WAKE_DATABASE_URL": edge_url,
            "CONTINUUM_TEST_WAKE_BACKEND_FIXTURE_URL": backend_url,
            "CONTINUUM_TEST_WAKE_TICKET_ID": str(ticket["ticket_id"]),
            "CONTINUUM_TEST_WAKE_TICKET_VALUE": ticket["ticket_value"],
            "CONTINUUM_TEST_WAKE_BACKEND_TOKEN": backend_token,
        }
    )
    try:
        result = subprocess.run(
            ["npm", "--prefix", "tools/edge", "run", "test-wake-integration"],
            cwd=PROJECT_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            diagnostic = (result.stdout + "\n" + result.stderr)[-2_000:]
            diagnostic = re.sub(r"postgres(?:ql)?://\S+", "<database-url>", diagnostic)
            diagnostic = diagnostic.replace(ticket["ticket_value"], "<one-use-ticket>")
            diagnostic = diagnostic.replace(backend_token, "<wake-token>")
            with admin_engine.connect() as connection:
                dispatch_state = connection.execute(
                    text(
                        "SELECT last_dispatch_outcome, last_http_status "
                        "FROM wake_private.dispatch_control WHERE singleton=true"
                    )
                ).one()
            pytest.fail(
                f"deno_wake_integration_failed; state={tuple(dispatch_state)}; {diagnostic}"
            )
    finally:
        _stop_local_wake_api(backend_process)

    with admin_engine.connect() as connection:
        status = connection.execute(
            text(
                "SELECT c.last_dispatch_outcome, r.drain_status, "
                "w.last_successful_drain_at, i.consumed_at "
                "FROM wake_private.dispatch_control AS c "
                "JOIN wake_private.wake_receipt AS r ON r.wake_id=c.active_wake_id "
                "JOIN wake_private.drain_watermark AS w ON w.singleton=true "
                "JOIN webhook_private.github_delivery_inbox AS i "
                "ON i.github_app_id=:app_id AND i.delivery_id=:delivery_id"
            ),
            {"app_id": APP_ID, "delivery_id": DELIVERY_ID},
        ).one()
    assert tuple(status) == ("accepted", "worker_not_started", None, None)


def _start_local_wake_api(
    receiver_url: str, backend_token: str
) -> tuple[subprocess.Popen[bytes], str]:
    with socket.socket() as server_socket:
        server_socket.bind(("127.0.0.1", 0))
        port = server_socket.getsockname()[1]
    backend_url = f"http://127.0.0.1:{port}"
    environment = os.environ.copy()
    environment.update(
        {
            "CONTINUUM_ENVIRONMENT": "development",
            "CONTINUUM_WAKE_RECEIVER_DATABASE_URL": receiver_url,
            "CONTINUUM_WAKE_BACKEND_TOKEN": backend_token,
        }
    )
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "continuum_api.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "critical",
        ],
        cwd=PROJECT_ROOT,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail("T06_local_api_failed_to_start")
        try:
            with urlopen(f"{backend_url}/health/live", timeout=0.5) as response:
                if response.status == 200:
                    return process, backend_url
        except Exception:
            time.sleep(0.05)
    _stop_local_wake_api(process)
    pytest.fail("T06_local_api_start_timeout")


def _stop_local_wake_api(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _drop_database_fixture(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS wake_private CASCADE"))
        connection.execute(text("DROP SCHEMA IF EXISTS webhook_private CASCADE"))
        connection.execute(text("DROP SCHEMA IF EXISTS continuum_migrations CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.t06_unrelated"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        _revoke_test_database_grants(connection)
        issuer_members = (
            connection.execute(
                text(
                    "SELECT pg_get_userbyid(member) FROM pg_auth_members "
                    "WHERE roleid=(SELECT oid FROM pg_roles WHERE rolname=:name)"
                ),
                {"name": ISSUER_ROLE},
            )
            .scalars()
            .all()
        )
        for member in issuer_members:
            quoted_member = connection.dialect.identifier_preparer.quote(member)
            connection.execute(text(f"REVOKE {ISSUER_ROLE} FROM {quoted_member}"))
        for role_name in (RECEIVER_ROLE, EDGE_ROLE, INGRESS_ROLE, ISSUER_ROLE):
            role_exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)"),
                {"name": role_name},
            ).scalar_one()
            if role_exists:
                connection.execute(text(f"DROP OWNED BY {role_name}"))
                database_name = connection.execute(text("SELECT current_database()")).scalar_one()
                quoted_database = connection.dialect.identifier_preparer.quote(database_name)
                connection.execute(
                    text(f"REVOKE ALL PRIVILEGES ON DATABASE {quoted_database} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE IF EXISTS {role_name}"))
        migration_exists = connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)"),
            {"name": MIGRATION_ROLE},
        ).scalar_one()
        if migration_exists:
            connection.execute(text(f"DROP OWNED BY {MIGRATION_ROLE}"))
            database_name = connection.execute(text("SELECT current_database()")).scalar_one()
            quoted_database = connection.dialect.identifier_preparer.quote(database_name)
            connection.execute(
                text(f"REVOKE ALL PRIVILEGES ON DATABASE {quoted_database} FROM {MIGRATION_ROLE}")
            )
            connection.execute(text(f"DROP ROLE IF EXISTS {MIGRATION_ROLE}"))


def _revoke_test_database_grants(connection: Connection) -> None:
    migration_exists = connection.execute(
        text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)"),
        {"name": MIGRATION_ROLE},
    ).scalar_one()
    database_name = connection.execute(text("SELECT current_database()")).scalar_one()
    quoted_database = connection.dialect.identifier_preparer.quote(database_name)
    if migration_exists:
        connection.execute(text(f"SET ROLE {MIGRATION_ROLE}"))
        for role_name in (INGRESS_ROLE, EDGE_ROLE, RECEIVER_ROLE):
            role_exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)"),
                {"name": role_name},
            ).scalar_one()
            if role_exists:
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {quoted_database} FROM {role_name}")
                )
        connection.execute(text("RESET ROLE"))
    for role_name in (INGRESS_ROLE, EDGE_ROLE, RECEIVER_ROLE, MIGRATION_ROLE):
        role_exists = connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)"),
            {"name": role_name},
        ).scalar_one()
        if role_exists:
            connection.execute(
                text(f"REVOKE ALL PRIVILEGES ON DATABASE {quoted_database} FROM {role_name}")
            )
