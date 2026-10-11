"""Real PostgreSQL proof for T09 claims, heartbeat, and execution fencing."""

from __future__ import annotations

import multiprocessing
import os
import secrets
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import IntegrityError

from continuum_api.runtime.leases import (
    HEARTBEAT_INTERVAL_SECONDS,
    LEASE_DURATION_SECONDS,
    CheckpointIdentityConflict,
    ExecutionLease,
    LeaseLost,
    QueueService,
)
from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository
from continuum_api.runtime.transitions import (
    NextWork,
    ResultIdentityConflict,
    TransitionService,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t09_test"
T04_REVISION = "20261011_04"
T09_REVISION = "20261011_05"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)


def test_claims_heartbeat_and_fenced_results_use_database_ownership() -> None:
    database_url = os.environ.get("CONTINUUM_T09_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T09_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T09 integration requires loopback PostgreSQL named continuum_t09_test")

    admin_engine = create_engine(
        url.set(drivername="postgresql+psycopg"), hide_parameters=True, pool_size=8, max_overflow=0
    )
    runtime_password = secrets.token_urlsafe(32)
    runtime_url = _role_url(url, RUNTIME_ROLE, runtime_password)
    runtime_url_text = runtime_url.render_as_string(hide_password=False)
    runtime_engine: Engine | None = None
    migration_applied = False

    try:
        _reset_database(admin_engine, runtime_password)
        _assert_postgres_17(admin_engine)
        _run_migration("upgrade", T04_REVISION, database_url)
        legacy_owner, legacy_run, legacy_step, legacy_job, legacy_attempt = _seed_legacy_t08_rows(
            admin_engine
        )
        _run_migration("upgrade", T09_REVISION, database_url)
        migration_applied = True
        _assert_legacy_rows_survive_upgrade(
            admin_engine, legacy_owner, legacy_run, legacy_step, legacy_job, legacy_attempt
        )

        _run_migration("downgrade", T04_REVISION, database_url)
        migration_applied = False
        _assert_legacy_rows_survive_downgrade(
            admin_engine, legacy_owner, legacy_run, legacy_step, legacy_job, legacy_attempt
        )
        _run_migration("upgrade", T09_REVISION, database_url)
        migration_applied = True
        _assert_legacy_rows_survive_upgrade(
            admin_engine, legacy_owner, legacy_run, legacy_step, legacy_job, legacy_attempt
        )

        runtime_engine = create_engine(
            runtime_url, hide_parameters=True, pool_size=8, max_overflow=0
        )
        owner_id, waiting_owner, terminal_owner = _seed_accounts(admin_engine)
        first_run, first_step, first_job = _seed_work(admin_engine, owner_id, RunStatus.QUEUED)
        _seed_work(admin_engine, waiting_owner, RunStatus.WAITING_CAPACITY)
        _seed_work(admin_engine, terminal_owner, RunStatus.FAILED)
        _install_one_shot_deadlock_trigger(admin_engine)

        _assert_run_first_skip_locked(admin_engine, runtime_url_text, owner_id)
        competing_claims = _compete_claims(runtime_url_text, owner_id)
        leases = [
            result
            for result in competing_claims
            if result is not None and result["lease"] is not None
        ]
        assert len(leases) == 1
        assert len({result["process_id"] for result in competing_claims}) == 2
        first_lease = _lease_from_data(leases[0]["lease"])
        assert first_lease.generation == 1
        assert first_lease.run_id == first_run
        assert first_lease.step_id == first_step
        assert first_lease.job_id == first_job
        _assert_one_shot_retry_and_attempt(admin_engine, owner_id, first_lease)

        queue = QueueService(runtime_engine, Principal(owner_id))
        assert queue.claim_one("same-account-second-worker") is None
        assert (
            QueueService(runtime_engine, Principal(waiting_owner)).claim_one("waiting-worker")
            is None
        )
        assert (
            QueueService(runtime_engine, Principal(terminal_owner)).claim_one("terminal-worker")
            is None
        )
        _assert_owner_isolation(runtime_engine, owner_id, waiting_owner, first_run)

        competing_results = _compete_results(runtime_url_text, first_lease)
        assert sorted(result["status"] for result in competing_results) == ["accepted", "conflict"]
        assert len({result["process_id"] for result in competing_results}) == 2
        accepted_result = next(
            result for result in competing_results if result["status"] == "accepted"
        )
        next_step_id = UUID(accepted_result["next_step_id"])
        next_job_id = UUID(accepted_result["next_job_id"])
        _assert_first_result_and_next_job(
            admin_engine, first_run, first_step, first_job, first_lease, accepted_result
        )

        next_lease = queue.claim_one("next-activity-worker")
        assert next_lease is not None
        assert next_lease.step_id == next_step_id and next_lease.job_id == next_job_id
        assert next_lease.generation == 1
        assert LEASE_DURATION_SECONDS == 60 and HEARTBEAT_INTERVAL_SECONDS == 15
        _exercise_checkpoint_and_heartbeat(queue, runtime_engine, admin_engine, next_lease)

        after_wait_lease = _reclaim_expired_job(queue, admin_engine, next_lease)
        assert after_wait_lease.generation == 2
        _assert_stale_writes_rejected(queue, runtime_engine, owner_id, after_wait_lease, next_lease)

        _assert_heartbeat_update_does_not_revive(admin_engine, runtime_url_text, after_wait_lease)
        third_lease = queue.claim_one("post-heartbeat-worker")
        assert third_lease is not None and third_lease.generation == 3
        _assert_checkpoint_write_checks_time_after_database_write(
            admin_engine, runtime_url_text, owner_id, third_lease
        )
        fourth_lease = queue.claim_one("post-checkpoint-worker")
        assert fourth_lease is not None and fourth_lease.generation == 4

        _assert_final_write_checks_time_after_database_writes(
            admin_engine, runtime_url_text, owner_id, fourth_lease
        )
        final_lease = queue.claim_one("final-worker")
        assert final_lease is not None and final_lease.generation == 5
        _persist_checkpoint_and_finish(queue, runtime_engine, owner_id, final_lease, next_job_id)

        _run_migration("downgrade", T04_REVISION, database_url)
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
        pytest.fail("T09 integration requires PostgreSQL 17")


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
        pytest.fail("T09 migration command failed; output redacted")


def _seed_legacy_t08_rows(engine: Engine) -> tuple[UUID, UUID, UUID, UUID, UUID]:
    owner_id, run_id, step_id, job_id, attempt_id = uuid4(), uuid4(), uuid4(), uuid4(), uuid4()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:user_id, 9_200_000)"
            ),
            {"user_id": owner_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.runs (run_id, owner_id, status) "
                "VALUES (:run_id, :owner_id, 'waiting_approval')"
            ),
            {"run_id": run_id, "owner_id": owner_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.steps "
                "(step_id, owner_id, run_id, logical_key, activity, status) "
                "VALUES (:step_id, :owner_id, :run_id, 'legacy:step', 'prepare', 'succeeded')"
            ),
            {"step_id": step_id, "owner_id": owner_id, "run_id": run_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.jobs "
                "(job_id, owner_id, run_id, step_id, status, generation) "
                "VALUES (:job_id, :owner_id, :run_id, :step_id, 'succeeded', 1)"
            ),
            {"job_id": job_id, "owner_id": owner_id, "run_id": run_id, "step_id": step_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.attempts "
                "(attempt_id, owner_id, run_id, job_id, generation, status, started_at, "
                "finished_at) "
                "VALUES (:attempt_id, :owner_id, :run_id, :job_id, 1, 'succeeded', "
                "clock_timestamp() - interval '1 second', clock_timestamp())"
            ),
            {"attempt_id": attempt_id, "owner_id": owner_id, "run_id": run_id, "job_id": job_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.events "
                "(event_id, owner_id, run_id, sequence, event_type, actor_type, payload) "
                "VALUES (:event_id, :owner_id, :run_id, 1, 'legacy.accepted', 'system', "
                "'{}'::jsonb)"
            ),
            {"event_id": uuid4(), "owner_id": owner_id, "run_id": run_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.accepted_results "
                "(result_id, owner_id, run_id, step_id, job_id, result_sha256, intent_sha256, "
                "run_status, event_sequence, payload) "
                "VALUES (:result_id, :owner_id, :run_id, :step_id, :job_id, :digest, :intent, "
                "'waiting_approval', 1, CAST(:payload AS jsonb))"
            ),
            {
                "result_id": uuid4(),
                "owner_id": owner_id,
                "run_id": run_id,
                "step_id": step_id,
                "job_id": job_id,
                "digest": b"l" * 32,
                "intent": b"i" * 32,
                "payload": '{"legacy":true}',
            },
        )
    return owner_id, run_id, step_id, job_id, attempt_id


def _assert_legacy_rows_survive_upgrade(
    engine: Engine, owner_id: UUID, run_id: UUID, step_id: UUID, job_id: UUID, attempt_id: UUID
) -> None:
    with engine.connect() as connection:
        result = connection.execute(
            text(
                "SELECT payload, generation, attempt_id, lease_owner "
                "FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
            ),
            {"owner_id": owner_id, "run_id": run_id, "step_id": step_id},
        ).one()
        attempt = connection.execute(
            text(
                "SELECT status, lease_owner FROM runtime_private.attempts "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id "
                "AND generation=1 AND attempt_id=:attempt_id"
            ),
            {
                "owner_id": owner_id,
                "run_id": run_id,
                "job_id": job_id,
                "attempt_id": attempt_id,
            },
        ).one()
    assert result.payload == {"legacy": True}
    assert tuple(result[1:]) == (None, None, None)
    assert tuple(attempt) == ("succeeded", None)
    with engine.begin() as connection:
        savepoint = connection.begin_nested()
        try:
            connection.execute(
                text(
                    "UPDATE runtime_private.accepted_results "
                    "SET generation=NULL, attempt_id=:attempt_id, lease_owner='partial-legacy' "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
                ),
                {
                    "attempt_id": uuid4(),
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "step_id": step_id,
                },
            )
        except IntegrityError as error:
            assert (
                error.orig.diag.constraint_name == "ck_runtime_accepted_results_execution_identity"
            )
            savepoint.rollback()
        else:
            savepoint.rollback()
            pytest.fail("T09 accepted-result identity check allowed a partial-null tuple")


def _assert_legacy_rows_survive_downgrade(
    engine: Engine, owner_id: UUID, run_id: UUID, step_id: UUID, job_id: UUID, attempt_id: UUID
) -> None:
    with engine.connect() as connection:
        result = connection.execute(
            text(
                "SELECT payload FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
            ),
            {"owner_id": owner_id, "run_id": run_id, "step_id": step_id},
        ).scalar_one()
        attempt = connection.execute(
            text(
                "SELECT status FROM runtime_private.attempts "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id "
                "AND generation=1 AND attempt_id=:attempt_id"
            ),
            {
                "owner_id": owner_id,
                "run_id": run_id,
                "job_id": job_id,
                "attempt_id": attempt_id,
            },
        ).scalar_one()
    assert result == {"legacy": True}
    assert attempt == "succeeded"


def _seed_accounts(engine: Engine) -> tuple[UUID, UUID, UUID]:
    owners = (uuid4(), uuid4(), uuid4())
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:first, 9_200_001), (:second, 9_200_002), (:third, 9_200_003)"
            ),
            {"first": owners[0], "second": owners[1], "third": owners[2]},
        )
    return owners


def _seed_work(engine: Engine, owner_id: UUID, run_status: RunStatus) -> tuple[UUID, UUID, UUID]:
    run_id, step_id, job_id = uuid4(), uuid4(), uuid4()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.runs (run_id, owner_id, status) "
                "VALUES (:run_id, :owner_id, :status)"
            ),
            {"run_id": run_id, "owner_id": owner_id, "status": run_status.value},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.steps "
                "(step_id, owner_id, run_id, logical_key, activity) "
                "VALUES (:step_id, :owner_id, :run_id, :logical_key, 'prepare')"
            ),
            {
                "step_id": step_id,
                "owner_id": owner_id,
                "run_id": run_id,
                "logical_key": f"prepare:{run_id}",
            },
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.jobs (job_id, owner_id, run_id, step_id) "
                "VALUES (:job_id, :owner_id, :run_id, :step_id)"
            ),
            {"job_id": job_id, "owner_id": owner_id, "run_id": run_id, "step_id": step_id},
        )
    return run_id, step_id, job_id


def _lease_data(lease: ExecutionLease) -> dict[str, str | int]:
    return {
        "owner_id": str(lease.owner_id),
        "run_id": str(lease.run_id),
        "step_id": str(lease.step_id),
        "job_id": str(lease.job_id),
        "attempt_id": str(lease.attempt_id),
        "generation": lease.generation,
        "lease_owner": lease.lease_owner,
        "lease_expires_at": lease.lease_expires_at.isoformat(),
    }


def _lease_from_data(data: dict[str, str | int]) -> ExecutionLease:
    return ExecutionLease(
        owner_id=UUID(str(data["owner_id"])),
        run_id=UUID(str(data["run_id"])),
        step_id=UUID(str(data["step_id"])),
        job_id=UUID(str(data["job_id"])),
        attempt_id=UUID(str(data["attempt_id"])),
        generation=int(data["generation"]),
        lease_owner=str(data["lease_owner"]),
        lease_expires_at=datetime.fromisoformat(str(data["lease_expires_at"])),
    )


def _claim_process(
    database_url: str,
    owner_id: str,
    lease_owner: str,
    start_gate: Any,
    ready_pids: Any,
) -> dict[str, Any] | None:
    ready_pids.append(os.getpid())
    if not start_gate.wait(timeout=20):
        raise TimeoutError("claim_process_start_gate_timeout")
    engine = create_engine(database_url, hide_parameters=True, pool_size=1, max_overflow=0)
    try:
        lease = QueueService(engine, Principal(UUID(owner_id))).claim_one(lease_owner)
        if lease is None:
            return {"process_id": os.getpid(), "lease": None}
        return {"process_id": os.getpid(), "lease": _lease_data(lease)}
    finally:
        engine.dispose()


def _result_process(
    database_url: str,
    lease_data: dict[str, str | int],
    result_key: str,
    start_gate: Any,
    ready_pids: Any,
) -> dict[str, Any]:
    ready_pids.append(os.getpid())
    if not start_gate.wait(timeout=20):
        raise TimeoutError("result_process_start_gate_timeout")
    lease = _lease_from_data(lease_data)
    engine = create_engine(database_url, hide_parameters=True, pool_size=1, max_overflow=0)
    try:
        try:
            receipt = TransitionService(engine, Principal(lease.owner_id)).commit_result(
                lease=lease,
                result={"competitor": result_key},
                target_run_status=RunStatus.RUNNING,
                next_work=NextWork("validate:competing", "validate", revision=0),
            )
        except ResultIdentityConflict:
            return {"process_id": os.getpid(), "status": "conflict"}
        return {
            "process_id": os.getpid(),
            "status": "accepted",
            "result_id": str(receipt.result_id),
            "event_sequence": receipt.event_sequence,
            "next_step_id": str(receipt.next_step_id),
            "next_job_id": str(receipt.next_job_id),
        }
    finally:
        engine.dispose()


def _run_competing_processes(target, database_url: str, *target_args: Any) -> list[Any]:
    context = multiprocessing.get_context("spawn")
    with (
        context.Manager() as manager,
        ProcessPoolExecutor(max_workers=2, mp_context=context) as pool,
    ):
        start_gate = manager.Event()
        ready_pids = manager.list()
        futures = [
            pool.submit(target, database_url, *target_args[index], start_gate, ready_pids)
            for index in range(2)
        ]
        deadline = time.monotonic() + 20
        while len(ready_pids) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(ready_pids) == 2
        assert len(set(ready_pids)) == 2
        start_gate.set()
        return [future.result(timeout=40) for future in futures]


def _compete_claims(database_url: str, owner_id: UUID) -> list[dict[str, Any] | None]:
    return _run_competing_processes(
        _claim_process,
        database_url,
        (str(owner_id), "worker-claim-a"),
        (str(owner_id), "worker-claim-b"),
    )


def _compete_results(database_url: str, lease: ExecutionLease) -> list[dict[str, Any]]:
    lease_data = _lease_data(lease)
    return _run_competing_processes(
        _result_process,
        database_url,
        (lease_data, "result-a"),
        (lease_data, "result-b"),
    )


def _install_one_shot_deadlock_trigger(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE SEQUENCE public.t09_deadlock_once START 1")
        connection.exec_driver_sql(
            """
            CREATE FUNCTION public.t09_deadlock_before_attempt() RETURNS trigger
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
            BEGIN
                IF pg_catalog.nextval('public.t09_deadlock_once'::regclass) = 1 THEN
                    RAISE EXCEPTION 'T09 retry fixture' USING ERRCODE='40P01';
                END IF;
                RETURN NEW;
            END
            $$
            """
        )
        connection.exec_driver_sql(
            "CREATE TRIGGER t09_deadlock_once BEFORE INSERT ON runtime_private.attempts "
            "FOR EACH ROW EXECUTE FUNCTION public.t09_deadlock_before_attempt()"
        )
        connection.exec_driver_sql(
            "REVOKE ALL ON FUNCTION public.t09_deadlock_before_attempt() FROM PUBLIC"
        )


def _assert_one_shot_retry_and_attempt(
    engine: Engine, owner_id: UUID, lease: ExecutionLease
) -> None:
    with engine.connect() as connection:
        retry_count = connection.execute(
            text("SELECT last_value FROM public.t09_deadlock_once")
        ).scalar_one()
        current = connection.execute(
            text(
                "SELECT job.generation, job.status, count(attempt.attempt_id) "
                "FROM runtime_private.jobs AS job "
                "LEFT JOIN runtime_private.attempts AS attempt "
                "ON attempt.owner_id=job.owner_id AND attempt.run_id=job.run_id "
                "AND attempt.job_id=job.job_id "
                "WHERE job.owner_id=:owner_id AND job.job_id=:job_id "
                "GROUP BY job.generation, job.status"
            ),
            {"owner_id": owner_id, "job_id": lease.job_id},
        ).one()
    assert retry_count >= 2
    assert tuple(current) == (1, "leased", 1)


def _assert_run_first_skip_locked(admin_engine: Engine, runtime_url: str, owner_id: UUID) -> None:
    blocker = admin_engine.connect()
    transaction = blocker.begin()
    try:
        run_id = blocker.execute(
            text("SELECT run_id FROM runtime_private.runs WHERE owner_id=:owner_id FOR UPDATE"),
            {"owner_id": owner_id},
        ).scalar_one()
        result = _run_single_process(
            runtime_url,
            str(owner_id),
            "skip-locked-probe",
        )
        assert result["lease"] is None
        assert run_id is not None
    finally:
        transaction.rollback()
        blocker.close()


def _run_single_process(database_url: str, owner_id: str, lease_owner: str) -> dict[str, Any]:
    with ProcessPoolExecutor(
        max_workers=1, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        manager_context = multiprocessing.get_context("spawn")
        with manager_context.Manager() as manager:
            gate = manager.Event()
            ready = manager.list()
            future = pool.submit(_claim_process, database_url, owner_id, lease_owner, gate, ready)
            deadline = time.monotonic() + 20
            while not ready and time.monotonic() < deadline:
                time.sleep(0.01)
            assert len(ready) == 1
            gate.set()
            result = future.result(timeout=20)
    assert result is not None
    return result


def _assert_owner_isolation(
    engine: Engine, owner_id: UUID, other_owner_id: UUID, run_id: UUID
) -> None:
    with engine.begin() as connection:
        other = RuntimeRepository(connection, Principal(other_owner_id))
        assert other.get_run(run_id) is None
        visible = (
            connection.execute(
                text("SELECT run_id FROM runtime_private.runs WHERE owner_id=:owner_id"),
                {"owner_id": owner_id},
            )
            .scalars()
            .all()
        )
        assert visible == []


def _assert_first_result_and_next_job(
    engine: Engine,
    run_id: UUID,
    step_id: UUID,
    job_id: UUID,
    lease: ExecutionLease,
    accepted: dict[str, Any],
) -> None:
    with engine.connect() as connection:
        accepted_row = connection.execute(
            text(
                "SELECT generation, attempt_id, lease_owner, event_sequence, next_job_id "
                "FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
            ),
            {"owner_id": lease.owner_id, "run_id": run_id, "step_id": step_id},
        ).one()
        attempt = connection.execute(
            text("SELECT status FROM runtime_private.attempts WHERE attempt_id=:attempt_id"),
            {"attempt_id": lease.attempt_id},
        ).scalar_one()
        job_count = connection.execute(
            text("SELECT count(*) FROM runtime_private.jobs WHERE job_id=:job_id"),
            {"job_id": UUID(accepted["next_job_id"])},
        ).scalar_one()
    assert tuple(accepted_row) == (
        lease.generation,
        lease.attempt_id,
        lease.lease_owner,
        2,
        UUID(accepted["next_job_id"]),
    )
    assert attempt == "succeeded"
    assert job_count == 1
    assert UUID(accepted["next_step_id"]) != step_id
    assert job_id == lease.job_id


def _exercise_checkpoint_and_heartbeat(
    queue: QueueService, runtime_engine: Engine, admin_engine: Engine, lease: ExecutionLease
) -> None:
    checkpoint = queue.persist_checkpoint(lease, "model:response", {"text": "bounded"})
    assert checkpoint.adopted is False
    adopted = queue.persist_checkpoint(lease, "model:response", {"text": "bounded"})
    assert adopted.adopted is True and adopted.checkpoint_id == checkpoint.checkpoint_id
    with pytest.raises(CheckpointIdentityConflict):
        queue.persist_checkpoint(lease, "model:response", {"text": "changed"})

    renewed = queue.heartbeat(lease)
    with admin_engine.connect() as connection:
        database_now = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
    assert renewed.lease_expires_at > lease.lease_expires_at
    assert renewed.lease_expires_at - database_now <= timedelta(seconds=LEASE_DURATION_SECONDS)


def _reclaim_expired_job(
    queue: QueueService, admin_engine: Engine, lease: ExecutionLease
) -> ExecutionLease:
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "SELECT run_id FROM runtime_private.runs "
                "WHERE owner_id=:owner_id AND run_id=:run_id FOR UPDATE"
            ),
            {"owner_id": lease.owner_id, "run_id": lease.run_id},
        ).one()
        connection.execute(
            text(
                "UPDATE runtime_private.jobs "
                "SET lease_expires_at=clock_timestamp() - interval '1 ms' "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id"
            ),
            {"owner_id": lease.owner_id, "run_id": lease.run_id, "job_id": lease.job_id},
        )
    new_lease = queue.claim_one("recovery-worker")
    assert new_lease is not None
    with admin_engine.connect() as connection:
        previous_status = connection.execute(
            text("SELECT status FROM runtime_private.attempts WHERE attempt_id=:attempt_id"),
            {"attempt_id": lease.attempt_id},
        ).scalar_one()
    assert previous_status == "unknown"
    return new_lease


def _assert_stale_writes_rejected(
    queue: QueueService,
    engine: Engine,
    owner_id: UUID,
    current_lease: ExecutionLease,
    stale_lease: ExecutionLease,
) -> None:
    with pytest.raises(LeaseLost):
        queue.heartbeat(stale_lease)
    with pytest.raises(LeaseLost):
        queue.persist_checkpoint(stale_lease, "stale:write", {"must_not_commit": True})
    with pytest.raises(LeaseLost):
        TransitionService(engine, Principal(owner_id)).commit_result(
            lease=stale_lease,
            result={"stale": True},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )
    with engine.begin() as connection:
        RuntimeRepository(connection, Principal(owner_id))
        state = connection.execute(
            text(
                "SELECT job.generation, job.lease_owner, job.status, attempt.status "
                "FROM runtime_private.jobs AS job JOIN runtime_private.attempts AS attempt "
                "ON attempt.owner_id=job.owner_id AND attempt.run_id=job.run_id "
                "AND attempt.job_id=job.job_id AND attempt.generation=job.generation "
                "WHERE job.owner_id=:owner_id AND job.job_id=:job_id"
            ),
            {"owner_id": owner_id, "job_id": current_lease.job_id},
        ).one()
        checkpoint_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.attempt_checkpoints "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND attempt_id=:attempt_id "
                "AND checkpoint_key='stale:write'"
            ),
            {
                "owner_id": owner_id,
                "run_id": current_lease.run_id,
                "attempt_id": stale_lease.attempt_id,
            },
        ).scalar_one()
    assert tuple(state) == (
        current_lease.generation,
        current_lease.lease_owner,
        "leased",
        "running",
    )
    assert checkpoint_count == 0


def _assert_heartbeat_update_does_not_revive(
    admin_engine: Engine, runtime_url: str, lease: ExecutionLease
) -> None:
    _install_delayed_heartbeat_trigger(admin_engine)
    expires_at = _set_lease_expiry(admin_engine, lease, seconds=1.5)
    heartbeat_engine = create_engine(
        runtime_url,
        hide_parameters=True,
        connect_args={"application_name": "t09-delayed-heartbeat"},
    )
    try:
        service = QueueService(heartbeat_engine, Principal(lease.owner_id))
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(service.heartbeat, lease)
            _wait_for_pg_wait(admin_engine, "t09-delayed-heartbeat", "PgSleep")
            _wait_until_database_time(admin_engine, expires_at)
            with pytest.raises(LeaseLost):
                future.result(timeout=10)
        with admin_engine.connect() as connection:
            persisted_expiry = connection.execute(
                text("SELECT lease_expires_at FROM runtime_private.jobs WHERE job_id=:job_id"),
                {"job_id": lease.job_id},
            ).scalar_one()
        assert persisted_expiry == expires_at
    finally:
        heartbeat_engine.dispose()
        _drop_delayed_heartbeat_trigger(admin_engine)


def _assert_final_write_checks_time_after_database_writes(
    admin_engine: Engine,
    runtime_url: str,
    owner_id: UUID,
    lease: ExecutionLease,
) -> None:
    _install_delayed_result_trigger(admin_engine)
    expires_at = _set_lease_expiry(admin_engine, lease, seconds=1.5)
    result_engine = create_engine(
        runtime_url,
        hide_parameters=True,
        connect_args={"application_name": "t09-delayed-result"},
    )
    try:
        service = TransitionService(result_engine, Principal(owner_id))
        with admin_engine.connect() as connection:
            before = connection.execute(
                text(
                    "SELECT "
                    "(SELECT count(*) FROM runtime_private.accepted_results "
                    " WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id), "
                    "(SELECT count(*) FROM runtime_private.events "
                    " WHERE owner_id=:owner_id AND run_id=:run_id)"
                ),
                {"owner_id": owner_id, "run_id": lease.run_id, "step_id": lease.step_id},
            ).one()
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                service.commit_result,
                lease=lease,
                result={"late_write": True},
                target_run_status=RunStatus.WAITING_APPROVAL,
            )
            _wait_for_pg_wait(admin_engine, "t09-delayed-result", "PgSleep")
            _wait_until_database_time(admin_engine, expires_at)
            with pytest.raises(LeaseLost, match="runtime_lease_expired_before_commit"):
                future.result(timeout=10)
        with admin_engine.connect() as connection:
            current = connection.execute(
                text(
                    "SELECT run.status, step.status, job.status, job.generation, "
                    "job.lease_owner, attempt.status "
                    "FROM runtime_private.runs AS run "
                    "JOIN runtime_private.steps AS step ON step.owner_id=run.owner_id "
                    "AND step.run_id=run.run_id "
                    "JOIN runtime_private.jobs AS job ON job.owner_id=step.owner_id "
                    "AND job.run_id=step.run_id AND job.step_id=step.step_id "
                    "JOIN runtime_private.attempts AS attempt ON attempt.owner_id=job.owner_id "
                    "AND attempt.run_id=job.run_id AND attempt.job_id=job.job_id "
                    "AND attempt.generation=job.generation "
                    "WHERE run.owner_id=:owner_id AND run.run_id=:run_id "
                    "AND step.step_id=:step_id AND job.job_id=:job_id"
                ),
                {
                    "owner_id": owner_id,
                    "run_id": lease.run_id,
                    "step_id": lease.step_id,
                    "job_id": lease.job_id,
                },
            ).one()
            after = connection.execute(
                text(
                    "SELECT "
                    "(SELECT count(*) FROM runtime_private.accepted_results "
                    " WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id), "
                    "(SELECT count(*) FROM runtime_private.events "
                    " WHERE owner_id=:owner_id AND run_id=:run_id)"
                ),
                {"owner_id": owner_id, "run_id": lease.run_id, "step_id": lease.step_id},
            ).one()
        assert tuple(current) == (
            "running",
            "running",
            "leased",
            lease.generation,
            lease.lease_owner,
            "running",
        )
        assert tuple(after) == tuple(before)
    finally:
        result_engine.dispose()
        _drop_delayed_result_trigger(admin_engine)


def _assert_checkpoint_write_checks_time_after_database_write(
    admin_engine: Engine,
    runtime_url: str,
    owner_id: UUID,
    lease: ExecutionLease,
) -> None:
    _install_delayed_checkpoint_trigger(admin_engine)
    expires_at = _set_lease_expiry(admin_engine, lease, seconds=1.5)
    checkpoint_engine = create_engine(
        runtime_url,
        hide_parameters=True,
        connect_args={"application_name": "t09-delayed-checkpoint"},
    )
    try:
        queue = QueueService(checkpoint_engine, Principal(owner_id))
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                queue.persist_checkpoint,
                lease,
                "expired:checkpoint",
                {"progress": "must_roll_back"},
            )
            _wait_for_pg_wait(admin_engine, "t09-delayed-checkpoint", "PgSleep")
            _wait_until_database_time(admin_engine, expires_at)
            with pytest.raises(LeaseLost, match="runtime_lease_expired_before_commit"):
                future.result(timeout=10)
        with admin_engine.connect() as connection:
            current = connection.execute(
                text(
                    "SELECT job.generation, job.status, job.lease_owner, attempt.status "
                    "FROM runtime_private.jobs AS job JOIN runtime_private.attempts AS attempt "
                    "ON attempt.owner_id=job.owner_id AND attempt.run_id=job.run_id "
                    "AND attempt.job_id=job.job_id AND attempt.generation=job.generation "
                    "WHERE job.owner_id=:owner_id AND job.job_id=:job_id"
                ),
                {"owner_id": owner_id, "job_id": lease.job_id},
            ).one()
            checkpoint_count = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.attempt_checkpoints "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND attempt_id=:attempt_id "
                    "AND checkpoint_key='expired:checkpoint'"
                ),
                {
                    "owner_id": owner_id,
                    "run_id": lease.run_id,
                    "attempt_id": lease.attempt_id,
                },
            ).scalar_one()
        assert tuple(current) == (
            lease.generation,
            "leased",
            lease.lease_owner,
            "running",
        )
        assert checkpoint_count == 0
    finally:
        checkpoint_engine.dispose()
        _drop_delayed_checkpoint_trigger(admin_engine)


def _persist_checkpoint_and_finish(
    queue: QueueService,
    engine: Engine,
    owner_id: UUID,
    lease: ExecutionLease,
    job_id: UUID,
) -> None:
    checkpoint = queue.persist_checkpoint(lease, "final:checkpoint", {"tree": "bounded"})
    assert checkpoint.adopted is False
    renewed = queue.heartbeat(lease)
    assert renewed.lease_expires_at > lease.lease_expires_at
    service = TransitionService(engine, Principal(owner_id))
    receipt = service.commit_result(
        lease=renewed,
        result={"final": True},
        target_run_status=RunStatus.WAITING_APPROVAL,
    )
    assert receipt.adopted is False
    adopted = service.commit_result(
        lease=lease,
        result={"final": True},
        target_run_status=RunStatus.WAITING_APPROVAL,
    )
    assert adopted.adopted is True and adopted.result_id == receipt.result_id
    with engine.begin() as connection:
        RuntimeRepository(connection, Principal(owner_id))
        result_identity = connection.execute(
            text(
                "SELECT generation, attempt_id, lease_owner FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id"
            ),
            {"owner_id": owner_id, "run_id": lease.run_id, "job_id": job_id},
        ).one()
        attempts = connection.execute(
            text(
                "SELECT generation, status FROM runtime_private.attempts "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id "
                "ORDER BY generation"
            ),
            {"owner_id": owner_id, "run_id": lease.run_id, "job_id": job_id},
        ).all()
    assert tuple(result_identity) == (lease.generation, lease.attempt_id, lease.lease_owner)
    assert [tuple(attempt) for attempt in attempts] == [
        (1, "unknown"),
        (2, "unknown"),
        (3, "unknown"),
        (4, "unknown"),
        (5, "succeeded"),
    ]
    assert queue.claim_one("after-terminal-worker") is None


def _set_lease_expiry(admin_engine: Engine, lease: ExecutionLease, *, seconds: float) -> datetime:
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "SELECT run_id FROM runtime_private.runs "
                "WHERE owner_id=:owner_id AND run_id=:run_id FOR UPDATE"
            ),
            {"owner_id": lease.owner_id, "run_id": lease.run_id},
        ).one()
        expires_at = connection.execute(
            text("SELECT clock_timestamp() + make_interval(secs => :seconds)"),
            {"seconds": seconds},
        ).scalar_one()
        connection.execute(
            text(
                "UPDATE runtime_private.jobs SET lease_expires_at=:expires_at "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id"
            ),
            {
                "expires_at": expires_at,
                "owner_id": lease.owner_id,
                "run_id": lease.run_id,
                "job_id": lease.job_id,
            },
        )
    return expires_at


def _wait_for_pg_wait(engine: Engine, application_name: str, wait_event: str) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with engine.connect() as connection:
            waiting = connection.execute(
                text(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                    "WHERE application_name=:application_name AND state='active' "
                    "AND wait_event=:wait_event)"
                ),
                {"application_name": application_name, "wait_event": wait_event},
            ).scalar_one()
        if waiting:
            return
        time.sleep(0.01)
    pytest.fail(f"T09 worker did not reach database wait {wait_event}")


def _wait_until_database_time(engine: Engine, deadline_at: datetime) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with engine.connect() as connection:
            elapsed = connection.execute(
                text("SELECT clock_timestamp() >= :deadline_at"),
                {"deadline_at": deadline_at},
            ).scalar_one()
        if elapsed:
            return
        time.sleep(0.01)
    pytest.fail("T09 database-time boundary did not pass")


def _install_delayed_heartbeat_trigger(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE FUNCTION public.t09_delay_heartbeat_update() RETURNS trigger
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
            BEGIN
                IF OLD.lease_expires_at <= clock_timestamp() + interval '3 seconds'
                   AND NEW.lease_expires_at > OLD.lease_expires_at THEN
                    PERFORM pg_sleep(2.2);
                END IF;
                RETURN NEW;
            END
            $$
            """
        )
        connection.exec_driver_sql(
            "CREATE TRIGGER t09_delay_heartbeat_update "
            "BEFORE UPDATE OF lease_expires_at ON runtime_private.jobs "
            "FOR EACH ROW EXECUTE FUNCTION public.t09_delay_heartbeat_update()"
        )
        connection.exec_driver_sql(
            "REVOKE ALL ON FUNCTION public.t09_delay_heartbeat_update() FROM PUBLIC"
        )


def _drop_delayed_heartbeat_trigger(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "DROP TRIGGER IF EXISTS t09_delay_heartbeat_update ON runtime_private.jobs"
        )
        connection.exec_driver_sql("DROP FUNCTION IF EXISTS public.t09_delay_heartbeat_update()")


def _install_delayed_checkpoint_trigger(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE SEQUENCE public.t09_checkpoint_delay_once START 1")
        connection.exec_driver_sql(
            """
            CREATE FUNCTION public.t09_delay_first_checkpoint_insert() RETURNS trigger
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
            BEGIN
                IF pg_catalog.nextval('public.t09_checkpoint_delay_once'::regclass) = 1 THEN
                    PERFORM pg_catalog.pg_sleep(2.2);
                END IF;
                RETURN NEW;
            END
            $$
            """
        )
        connection.exec_driver_sql(
            "CREATE TRIGGER t09_delay_first_checkpoint_insert "
            "BEFORE INSERT ON runtime_private.attempt_checkpoints "
            "FOR EACH ROW EXECUTE FUNCTION public.t09_delay_first_checkpoint_insert()"
        )
        connection.exec_driver_sql(
            "REVOKE ALL ON FUNCTION public.t09_delay_first_checkpoint_insert() FROM PUBLIC"
        )


def _drop_delayed_checkpoint_trigger(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "DROP TRIGGER IF EXISTS t09_delay_first_checkpoint_insert "
            "ON runtime_private.attempt_checkpoints"
        )
        connection.exec_driver_sql(
            "DROP FUNCTION IF EXISTS public.t09_delay_first_checkpoint_insert()"
        )


def _install_delayed_result_trigger(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE SEQUENCE public.t09_result_delay_once START 1")
        connection.exec_driver_sql(
            """
            CREATE FUNCTION public.t09_delay_first_result_insert() RETURNS trigger
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
            BEGIN
                IF pg_catalog.nextval('public.t09_result_delay_once'::regclass) = 1 THEN
                    PERFORM pg_catalog.pg_sleep(2.2);
                END IF;
                RETURN NEW;
            END
            $$
            """
        )
        connection.exec_driver_sql(
            "CREATE TRIGGER t09_delay_first_result_insert "
            "BEFORE INSERT ON runtime_private.accepted_results "
            "FOR EACH ROW EXECUTE FUNCTION public.t09_delay_first_result_insert()"
        )
        connection.exec_driver_sql(
            "REVOKE ALL ON FUNCTION public.t09_delay_first_result_insert() FROM PUBLIC"
        )


def _drop_delayed_result_trigger(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "DROP TRIGGER IF EXISTS t09_delay_first_result_insert "
            "ON runtime_private.accepted_results"
        )
        connection.exec_driver_sql("DROP FUNCTION IF EXISTS public.t09_delay_first_result_insert()")


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
        for function_name in (
            "t09_deadlock_before_attempt",
            "t09_delay_heartbeat_update",
            "t09_delay_first_checkpoint_insert",
            "t09_delay_first_result_insert",
        ):
            connection.exec_driver_sql(f"DROP FUNCTION IF EXISTS public.{function_name}()")
        connection.exec_driver_sql("DROP SEQUENCE IF EXISTS public.t09_deadlock_once")
        connection.exec_driver_sql("DROP SEQUENCE IF EXISTS public.t09_checkpoint_delay_once")
        connection.exec_driver_sql("DROP SEQUENCE IF EXISTS public.t09_result_delay_once")
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
