"""PostgreSQL proof for atomic T08 activity transitions and replay."""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import sys
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import IntegrityError

from continuum_api.runtime.leases import ExecutionLease, QueueService
from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository
from continuum_api.runtime.transitions import (
    MAX_RESULT_BYTES,
    NextWork,
    ResultIdentityConflict,
    TransitionService,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t08_test"
T07_REVISION = "20261011_03"
T08_REVISION = "20261011_06"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)


def test_activity_result_commits_atomically_and_recovers_lost_ack() -> None:
    database_url = os.environ.get("CONTINUUM_T08_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T08_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T08 integration requires loopback PostgreSQL named continuum_t08_test")

    admin_engine = create_engine(
        url.set(drivername="postgresql+psycopg"),
        hide_parameters=True,
        pool_size=4,
        max_overflow=0,
    )
    runtime_password = secrets.token_urlsafe(32)
    runtime_url = _role_url(url, RUNTIME_ROLE, runtime_password)
    runtime_engine: Engine | None = None
    migration_applied = False

    try:
        _reset_database(admin_engine, runtime_password)
        _assert_postgres_17(admin_engine)
        _run_migration("upgrade", T08_REVISION, database_url)
        migration_applied = True
        runtime_engine = create_engine(
            runtime_url, hide_parameters=True, pool_size=4, max_overflow=0
        )
        owner_id = _seed_user(admin_engine)
        run_id, source_step_id, source_job_id, duplicate_next_step_id = _seed_work(
            runtime_engine, owner_id
        )
        queue = QueueService(runtime_engine, Principal(owner_id))
        source_lease = queue.claim_one("fixture-worker")
        assert source_lease is not None
        service = TransitionService(runtime_engine, Principal(owner_id))
        result = {"accepted": True, "patch": {"files": 2, "sha": "abc123"}}
        valid_next = NextWork("validate:patch", "validate", revision=0)

        _assert_rollback_after_state_writes(
            service,
            runtime_engine,
            owner_id,
            source_lease,
            duplicate_next_step_id,
            result,
        )
        receipt = service.commit_result(
            lease=source_lease,
            result=result,
            target_run_status=RunStatus.RUNNING,
            next_work=valid_next,
        )
        assert receipt.adopted is False
        assert receipt.event_sequence == 2
        assert receipt.next_step_id is not None
        assert receipt.next_job_id is not None
        _assert_committed_transition(
            runtime_engine, owner_id, receipt, source_step_id, source_job_id, result
        )

        replay = service.commit_result(
            lease=source_lease,
            result=result,
            target_run_status=RunStatus.RUNNING,
            next_work=valid_next,
        )
        assert replay.adopted is True
        assert replay.result_id == receipt.result_id
        assert replay.result_sha256 == receipt.result_sha256
        assert replay.event_sequence == receipt.event_sequence
        assert replay.next_step_id == receipt.next_step_id
        assert replay.next_job_id == receipt.next_job_id
        _assert_result_identity_conflicts(service, source_lease, result)

        assert receipt.next_step_id is not None and receipt.next_job_id is not None
        next_lease = queue.claim_one("fixture-worker-2")
        assert next_lease is not None
        assert next_lease.step_id == receipt.next_step_id
        assert next_lease.job_id == receipt.next_job_id
        waiting_receipt = service.commit_result(
            lease=next_lease,
            result={"validated": True},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )
        assert waiting_receipt.run_status is RunStatus.WAITING_APPROVAL
        assert waiting_receipt.next_job_id is None
        assert waiting_receipt.event_sequence == 4
        _mark_run_failed(runtime_engine, owner_id, run_id)
        _assert_terminal_run_does_not_reopen(
            service, queue, runtime_engine, owner_id, run_id, next_lease
        )

        runtime_engine.dispose()
        runtime_engine = None
        _run_migration("downgrade", T07_REVISION, database_url)
        migration_applied = False
        _assert_table_absent(admin_engine, "accepted_results")
        _run_migration("upgrade", T08_REVISION, database_url)
        migration_applied = True
        _assert_table_present(admin_engine, "accepted_results")
        _run_migration("downgrade", "base", database_url)
        migration_applied = False
    finally:
        if runtime_engine is not None:
            runtime_engine.dispose()
        try:
            if migration_applied:
                _run_migration("downgrade", "base", database_url)
        finally:
            _cleanup_database(admin_engine)
            admin_engine.dispose()


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
        pytest.fail("T08 integration requires PostgreSQL 17")


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
        pytest.fail("T08 migration command failed; output redacted")


def _seed_user(engine: Engine) -> UUID:
    owner_id = uuid4()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:user_id, :github_user_id)"
            ),
            {"user_id": owner_id, "github_user_id": 810_001},
        )
    return owner_id


def _seed_work(engine: Engine, owner_id: UUID) -> tuple[UUID, UUID, UUID, UUID]:
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, Principal(owner_id))
        run_id = repository.create_run()
        step_id = repository.create_step(run_id, "prepare:patch", "prepare")
        job_id = repository.create_job(run_id, step_id)
        duplicate_next_step_id = repository.create_step(run_id, "blocked:duplicate", "validate")
    return run_id, step_id, job_id, duplicate_next_step_id


def _assert_rollback_after_state_writes(
    service: TransitionService,
    engine: Engine,
    owner_id: UUID,
    lease: ExecutionLease,
    duplicate_next_step_id: UUID,
    result: dict[str, object],
) -> None:
    with pytest.raises(IntegrityError) as error:
        service.commit_result(
            lease=lease,
            result=result,
            target_run_status=RunStatus.RUNNING,
            next_work=NextWork("blocked:duplicate", "validate", revision=0),
        )
    assert error.value.orig.diag.constraint_name == "uq_runtime_steps_logical_key"
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, Principal(owner_id))
        run = repository.get_run(lease.run_id)
        assert run is not None and run.status is RunStatus.RUNNING
        states = connection.execute(
            text(
                """
                SELECT step.status, job.status, job.lease_owner,
                       (SELECT count(*) FROM runtime_private.accepted_results
                        WHERE owner_id=:owner_id AND run_id=:run_id) AS result_count,
                       (SELECT count(*) FROM runtime_private.events
                        WHERE owner_id=:owner_id AND run_id=:run_id) AS event_count
                FROM runtime_private.steps AS step
                JOIN runtime_private.jobs AS job
                  ON job.owner_id=step.owner_id AND job.run_id=step.run_id
                 AND job.step_id=step.step_id
                WHERE step.owner_id=:owner_id AND step.run_id=:run_id AND step.step_id=:step_id
                """
            ),
            {"owner_id": owner_id, "run_id": lease.run_id, "step_id": lease.step_id},
        ).one()
        blocked_step_state = connection.execute(
            text("SELECT status FROM runtime_private.steps WHERE step_id=:step_id"),
            {"step_id": duplicate_next_step_id},
        ).scalar_one()
    assert tuple(states) == ("running", "leased", lease.lease_owner, 0, 1)
    assert blocked_step_state == "queued"

    late_result = {f"k{index:08d}": 0 for index in range(4_600)}
    assert len(json.dumps(late_result, separators=(",", ":")).encode("utf-8")) <= MAX_RESULT_BYTES
    with pytest.raises(IntegrityError) as late_error:
        service.commit_result(
            lease=lease,
            result=late_result,
            target_run_status=RunStatus.RUNNING,
            next_work=NextWork("failed:late-result", "validate", revision=0),
        )
    assert late_error.value.orig.diag.constraint_name == "ck_runtime_accepted_results_payload_limit"
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, Principal(owner_id))
        run = repository.get_run(lease.run_id)
        late_work_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.steps "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND logical_key='failed:late-result'"
            ),
            {"owner_id": owner_id, "run_id": lease.run_id},
        ).scalar_one()
        counts = connection.execute(
            text(
                "SELECT "
                "(SELECT count(*) FROM runtime_private.accepted_results "
                " WHERE owner_id=:owner_id AND run_id=:run_id), "
                "(SELECT count(*) FROM runtime_private.events "
                " WHERE owner_id=:owner_id AND run_id=:run_id)"
            ),
            {"owner_id": owner_id, "run_id": lease.run_id},
        ).one()
    assert run is not None and run.status is RunStatus.RUNNING
    assert late_work_count == 0
    assert tuple(counts) == (0, 1)


def _assert_committed_transition(
    engine: Engine,
    owner_id: UUID,
    receipt,
    source_step_id: UUID,
    source_job_id: UUID,
    result: dict[str, object],
) -> None:
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, Principal(owner_id))
        run = repository.get_run(receipt.run_id)
        assert run is not None and run.status is RunStatus.RUNNING
        source_state = connection.execute(
            text(
                """
                SELECT step.status, job.status, job.lease_owner, job.lease_expires_at
                FROM runtime_private.steps AS step
                JOIN runtime_private.jobs AS job
                  ON job.owner_id=step.owner_id AND job.run_id=step.run_id
                 AND job.step_id=step.step_id
                WHERE step.owner_id=:owner_id AND step.run_id=:run_id
                  AND step.step_id=:step_id AND job.job_id=:job_id
                """
            ),
            {
                "owner_id": owner_id,
                "run_id": receipt.run_id,
                "step_id": source_step_id,
                "job_id": source_job_id,
            },
        ).one()
        persisted_result = connection.execute(
            text(
                "SELECT result_id, result_sha256, payload, event_sequence, next_job_id "
                "FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
            ),
            {"owner_id": owner_id, "run_id": receipt.run_id, "step_id": source_step_id},
        ).one()
        events = repository.list_events(receipt.run_id)
        next_job_step_id = connection.execute(
            text(
                "SELECT step_id FROM runtime_private.jobs "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id"
            ),
            {
                "owner_id": owner_id,
                "run_id": receipt.run_id,
                "job_id": receipt.next_job_id,
            },
        ).scalar_one()
    assert tuple(source_state) == ("succeeded", "succeeded", None, None)
    assert persisted_result.result_id == receipt.result_id
    assert persisted_result.result_sha256 == receipt.result_sha256
    assert persisted_result.payload == result
    assert persisted_result.event_sequence == 2
    assert persisted_result.next_job_id == receipt.next_job_id
    assert receipt.next_step_id == next_job_step_id
    assert [(event.sequence, event.event_type) for event in events] == [
        (1, "job.claimed"),
        (2, "activity.result_accepted"),
    ]


def _assert_result_identity_conflicts(
    service: TransitionService,
    lease: ExecutionLease,
    result: dict[str, object],
) -> None:
    with pytest.raises(ResultIdentityConflict, match="accepted_result_intent_conflict"):
        service.commit_result(
            lease=lease,
            result=result,
            target_run_status=RunStatus.RUNNING,
            next_work=NextWork("validate:different", "validate", revision=0),
        )
    with pytest.raises(ResultIdentityConflict, match="accepted_result_intent_conflict"):
        service.commit_result(
            lease=lease,
            result=result,
            target_run_status=RunStatus.WAITING_APPROVAL,
        )
    with pytest.raises(ResultIdentityConflict, match="accepted_result_intent_conflict"):
        service.commit_result(
            lease=lease,
            result={"accepted": False},
            target_run_status=RunStatus.RUNNING,
            next_work=NextWork("validate:patch", "validate", revision=0),
        )


def _mark_run_failed(engine: Engine, owner_id: UUID, run_id: UUID) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, Principal(owner_id))
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET status='failed' "
                "WHERE owner_id=:owner_id AND run_id=:run_id"
            ),
            {"owner_id": owner_id, "run_id": run_id},
        )


def _assert_terminal_run_does_not_reopen(
    service: TransitionService,
    queue: QueueService,
    engine: Engine,
    owner_id: UUID,
    run_id: UUID,
    lease: ExecutionLease,
) -> None:
    receipt = service.commit_result(
        lease=lease,
        result={"validated": True},
        target_run_status=RunStatus.WAITING_APPROVAL,
    )
    assert receipt.adopted is True
    assert queue.claim_one("late-worker") is None
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, Principal(owner_id))
        run = repository.get_run(run_id)
        accepted_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner_id AND run_id=:run_id"
            ),
            {"owner_id": owner_id, "run_id": run_id},
        ).scalar_one()
    assert run is not None and run.status is RunStatus.FAILED
    assert accepted_count == 2


def _assert_table_present(engine: Engine, table_name: str) -> None:
    with engine.connect() as connection:
        exists = connection.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='runtime_private' AND table_name=:table_name)"
            ),
            {"table_name": table_name},
        ).scalar_one()
    assert exists is True


def _assert_table_absent(engine: Engine, table_name: str) -> None:
    with engine.connect() as connection:
        exists = connection.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='runtime_private' AND table_name=:table_name)"
            ),
            {"table_name": table_name},
        ).scalar_one()
    assert exists is False


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
