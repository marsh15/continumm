"""Real PostgreSQL proof for T07 runtime persistence and account isolation."""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, IntegrityError

from continuum_api.runtime.persistence import EventActor, Principal, RuntimeRepository

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t07_test"
T06_REVISION = "20261011_02"
T07_REVISION = "20261011_03"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)


def test_runtime_schema_enforces_ownership_and_logical_keys() -> None:
    database_url = os.environ.get("CONTINUUM_T07_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T07_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T07 integration requires loopback PostgreSQL named continuum_t07_test")

    engine = create_engine(
        url.set(drivername="postgresql+psycopg"), hide_parameters=True, pool_size=4, max_overflow=0
    )
    runtime_password = secrets.token_urlsafe(32)
    runtime_url = _role_url(url, RUNTIME_ROLE, runtime_password)
    runtime_engine: Engine | None = None
    migration_applied = False

    try:
        _reset_database(engine, runtime_password)
        _assert_postgres_17(engine)
        _run_migration("upgrade", T07_REVISION, database_url)
        migration_applied = True
        runtime_engine = create_engine(
            runtime_url, hide_parameters=True, pool_size=4, max_overflow=0
        )
        _assert_private_schema_and_grants(engine)
        owners = _seed_users(engine)
        run_id, step_id, job_id = _exercise_repository(runtime_engine, owners)
        _assert_database_constraints(runtime_engine, owners[0], run_id, step_id, job_id)
        _assert_cross_account_denial(runtime_engine, owners[1], run_id)
        _assert_principal_is_transaction_scoped(runtime_engine, owners[0], owners[1])

        runtime_engine.dispose()
        runtime_engine = None
        _run_migration("downgrade", T06_REVISION, database_url)
        migration_applied = False
        _assert_schema_absent(engine, "runtime_private")
        _assert_schema_present(engine, "wake_private")
        _run_migration("upgrade", T07_REVISION, database_url)
        migration_applied = True
        _assert_schema_and_table_names(engine)
        _run_migration("downgrade", "base", database_url)
        migration_applied = False
        for schema_name in ("runtime_private", "wake_private", "webhook_private"):
            _assert_schema_absent(engine, schema_name)
    finally:
        if runtime_engine is not None:
            runtime_engine.dispose()
        try:
            if migration_applied:
                _run_migration("downgrade", "base", database_url)
        finally:
            _cleanup_database(engine)
            engine.dispose()


def _role_url(url: URL, role_name: str, password: str) -> URL:
    return url.set(drivername="postgresql+psycopg", username=role_name, password=password)


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
            exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role_name)"),
                {"role_name": role_name},
            ).scalar_one()
            if exists:
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE {role_name}"))

        role_definitions = (
            ("continuum_ingress", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_issuer", "NOLOGIN", None),
            ("continuum_wake", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_receiver", "LOGIN", secrets.token_urlsafe(32)),
            (RUNTIME_ROLE, "LOGIN", runtime_password),
        )
        for role_name, login_mode, password in role_definitions:
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
        pytest.fail("T07 integration requires PostgreSQL 17")


def _run_migration(
    operation: str, revision: str, database_url: str, *, expect_success: bool = True
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
    if expect_success and result.returncode != 0:
        pytest.fail("T07 migration command failed; output redacted")
    if not expect_success and result.returncode == 0:
        pytest.fail("T07 migration unexpectedly succeeded")


def _assert_private_schema_and_grants(engine: Engine) -> None:
    _assert_schema_and_table_names(engine)
    with engine.connect() as connection:
        state = connection.execute(
            text(
                """
                SELECT
                    has_schema_privilege('public', 'runtime_private', 'USAGE'),
                    has_schema_privilege(:runtime_role, 'runtime_private', 'USAGE'),
                    has_table_privilege(:runtime_role, 'runtime_private.runs', 'SELECT'),
                    has_table_privilege(:runtime_role, 'runtime_private.runs', 'INSERT'),
                    has_table_privilege(:runtime_role, 'runtime_private.users', 'INSERT')
                """
            ),
            {"runtime_role": RUNTIME_ROLE},
        ).one()
    assert tuple(state) == (False, True, True, True, False)


def _assert_schema_and_table_names(engine: Engine) -> None:
    with engine.connect() as connection:
        tables = (
            connection.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='runtime_private' AND table_type='BASE TABLE' "
                    "ORDER BY table_name"
                )
            )
            .scalars()
            .all()
        )
        public_runtime_tables = (
            connection.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='public' AND table_name IN "
                    "('users', 'runs', 'steps', 'jobs', 'attempts', 'events')"
                )
            )
            .scalars()
            .all()
        )
    assert tables == ["attempts", "events", "jobs", "runs", "steps", "users"]
    assert public_runtime_tables == []


def _seed_users(engine: Engine) -> tuple[UUID, UUID]:
    first_owner, second_owner = uuid4(), uuid4()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:user_id, :github_user_id)"
            ),
            [
                {"user_id": first_owner, "github_user_id": 710_001},
                {"user_id": second_owner, "github_user_id": 710_002},
            ],
        )
    return first_owner, second_owner


def _exercise_repository(engine: Engine, owners: tuple[UUID, UUID]) -> tuple[UUID, UUID, UUID]:
    first_owner, _second_owner = owners
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, Principal(first_owner))
        run_id = repository.create_run(github_repository_id=9_001, issue_number=42)
        run = repository.get_run(run_id)
        assert run is not None
        assert run.owner_id == first_owner
        assert run.status.value == "queued"
        assert run.revision == 0

        step_id = repository.create_step(run_id, "prepare:base", "prepare")
        job_id = repository.create_job(run_id, step_id)
        connection.execute(
            text(
                "UPDATE runtime_private.jobs SET status='leased', lease_owner='fixture-worker', "
                "lease_expires_at=clock_timestamp() + interval '60 seconds', generation=1 "
                "WHERE owner_id=:owner_id AND job_id=:job_id"
            ),
            {"owner_id": first_owner, "job_id": job_id},
        )
        repository.create_attempt(run_id, job_id, generation=1)
        repository.append_event(
            run_id,
            1,
            "run.admitted",
            EventActor.SYSTEM,
            payload={"source": "integration-test"},
        )
        events = repository.list_events(run_id)
        assert [(event.sequence, event.event_type) for event in events] == [(1, "run.admitted")]
        assert events[0].payload == {"source": "integration-test"}
    return run_id, step_id, job_id


def _assert_database_constraints(
    engine: Engine, owner_id: UUID, run_id: UUID, step_id: UUID, job_id: UUID
) -> None:
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, Principal(owner_id))
        _expect_constraint(
            connection,
            lambda: repository.create_run(),
            "uq_runtime_runs_one_active_account",
        )
        _expect_constraint(
            connection,
            lambda: repository.create_step(run_id, "prepare:base", "prepare"),
            "uq_runtime_steps_logical_key",
        )
        assert repository.create_step(run_id, "prepare:base", "prepare", revision=1) != step_id
        _expect_constraint(
            connection,
            lambda: repository.create_job(run_id, step_id),
            "uq_runtime_jobs_step",
        )
        _expect_constraint(
            connection,
            lambda: repository.create_attempt(run_id, job_id, generation=1),
            "uq_runtime_attempts_job_generation",
        )
        _expect_constraint(
            connection,
            lambda: repository.append_event(run_id, 1, "run.admitted", EventActor.SYSTEM),
            "uq_runtime_events_run_sequence",
        )
        _expect_constraint(
            connection,
            lambda: repository.append_event(run_id, 0, "run.invalid", EventActor.SYSTEM),
            "ck_runtime_events_sequence_positive",
        )
        _expect_constraint(
            connection,
            lambda: repository.append_event(
                run_id,
                2,
                "run.too_large",
                EventActor.SYSTEM,
                payload={"large": "x" * 9_000},
            ),
            "ck_runtime_events_payload_limit",
        )
        _expect_permission_denied(
            connection,
            lambda: connection.execute(
                text(
                    "UPDATE runtime_private.events SET event_type='run.edited' "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND sequence=1"
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ),
        )
        _expect_constraint(
            connection,
            lambda: connection.execute(
                text(
                    "INSERT INTO runtime_private.runs "
                    "(run_id, owner_id, github_repository_id, status) "
                    "VALUES (:run_id, :owner_id, 9002, 'succeeded')"
                ),
                {"run_id": uuid4(), "owner_id": owner_id},
            ),
            "ck_runtime_runs_repository_issue_pair",
        )
        _expect_constraint(
            connection,
            lambda: connection.execute(
                text(
                    "INSERT INTO runtime_private.runs "
                    "(run_id, owner_id, issue_number, status) "
                    "VALUES (:run_id, :owner_id, 44, 'succeeded')"
                ),
                {"run_id": uuid4(), "owner_id": owner_id},
            ),
            "ck_runtime_runs_repository_issue_pair",
        )


def _expect_constraint(connection, operation, constraint_name: str) -> None:
    savepoint = connection.begin_nested()
    try:
        operation()
    except IntegrityError as error:
        assert getattr(error.orig, "diag", None).constraint_name == constraint_name
        savepoint.rollback()
    else:
        savepoint.rollback()
        pytest.fail(f"expected database constraint {constraint_name}")


def _expect_permission_denied(connection, operation) -> None:
    savepoint = connection.begin_nested()
    try:
        operation()
    except DBAPIError as error:
        assert getattr(error.orig, "sqlstate", None) == "42501"
        savepoint.rollback()
    else:
        savepoint.rollback()
        pytest.fail("runtime role unexpectedly updated an append-only event")


def _assert_cross_account_denial(engine: Engine, owner_id: UUID, run_id: UUID) -> None:
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, Principal(owner_id))
        assert repository.get_run(run_id) is None
        assert repository.list_events(run_id) == ()
        _expect_constraint(
            connection,
            lambda: repository.create_step(run_id, "foreign:step", "prepare"),
            "fk_runtime_steps_owner_run",
        )

    with engine.connect() as connection, connection.begin():
        visible_runs = (
            connection.execute(text("SELECT run_id FROM runtime_private.runs")).scalars().all()
        )
    assert visible_runs == []


def _assert_principal_is_transaction_scoped(
    engine: Engine, first_owner: UUID, second_owner: UUID
) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, Principal(first_owner))
        with pytest.raises(ValueError, match="runtime_transaction_principal_mismatch"):
            RuntimeRepository(connection, Principal(second_owner))


def _assert_schema_absent(engine: Engine, schema_name: str) -> None:
    with engine.connect() as connection:
        exists = connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=:schema_name)"),
            {"schema_name": schema_name},
        ).scalar_one()
    assert exists is False


def _assert_schema_present(engine: Engine, schema_name: str) -> None:
    with engine.connect() as connection:
        exists = connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=:schema_name)"),
            {"schema_name": schema_name},
        ).scalar_one()
    assert exists is True


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
            exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role_name)"),
                {"role_name": role_name},
            ).scalar_one()
            if exists:
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE {role_name}"))
