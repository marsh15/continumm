"""PostgreSQL proof for cancellation fencing and independent late observations."""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import IntegrityError

from continuum_api.runtime.cancellation import (
    CancellationService,
    DispatchIntentConflict,
    DispatchIntentService,
    RuntimeRunNotFound,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, QueueService
from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository
from continuum_api.runtime.scheduler import FailureClass, RuntimeRecoveryService
from continuum_api.runtime.transitions import TransitionService

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t11_test"
T06_REVISION = "20261011_06"
T11_REVISION = "20261011_07"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)


def test_cancellation_fences_dispatch_and_preserves_unresolved_observations() -> None:
    database_url = os.environ.get("CONTINUUM_T11_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T11_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T11 integration requires loopback PostgreSQL named continuum_t11_test")

    admin_engine = create_engine(
        url.set(drivername="postgresql+psycopg"), hide_parameters=True, pool_size=8, max_overflow=0
    )
    runtime_password = secrets.token_urlsafe(32)
    runtime_url = _role_url(url, runtime_password)
    runtime_engine: Engine | None = None
    migration_applied = False

    try:
        _reset_database(admin_engine, runtime_password)
        _assert_postgres_17(admin_engine)
        _run_migration("upgrade", T11_REVISION, database_url)
        migration_applied = True
        runtime_engine = create_engine(
            runtime_url, hide_parameters=True, pool_size=8, max_overflow=0
        )

        cancel_owner, cancel_run, cancel_lease = _seed_claimed_run(admin_engine, runtime_engine)
        cancel_principal = Principal(cancel_owner)
        queue = QueueService(runtime_engine, cancel_principal)
        recovery = RuntimeRecoveryService(runtime_engine, cancel_principal)
        dispatcher = DispatchIntentService(runtime_engine, cancel_principal)
        cancellation = CancellationService(runtime_engine, cancel_principal)
        dispatch = dispatcher.authorize(
            cancel_lease,
            "activity:call-1",
            {"activity": "test", "target": "remote-effect"},
        )
        assert not dispatch.adopted and not dispatch.observed
        _seed_unknown_reservation(admin_engine, cancel_owner, cancel_run)
        _assert_ledger_owner_isolation(runtime_engine, cancel_owner, cancel_run)
        _assert_metadata_bounds(admin_engine, dispatcher, cancel_owner, cancel_run, cancel_lease)

        # All callers contend on the run lock. Exactly one commits cancellation;
        # the others adopt its terminal event without adding duplicate cleanup.
        with ThreadPoolExecutor(max_workers=4) as pool:
            cancel_results = list(pool.map(lambda _: cancellation.cancel(cancel_run), range(4)))
        assert sum(not receipt.adopted for receipt in cancel_results) == 1
        assert all(receipt.run_status is RunStatus.CANCELLED for receipt in cancel_results)
        _assert_cancelled_state(admin_engine, cancel_owner, cancel_run, cancel_lease)

        # Replaying a previously committed dispatch key is read-only. A new
        # dispatch key cannot be authorized after the cancellation commit.
        adopted_dispatch = dispatcher.authorize(
            cancel_lease,
            "activity:call-1",
            {"activity": "test", "target": "remote-effect"},
        )
        assert adopted_dispatch.adopted and adopted_dispatch.dispatch_id == dispatch.dispatch_id
        with pytest.raises(LeaseLost):
            dispatcher.authorize(
                cancel_lease,
                "activity:call-2",
                {"activity": "test", "target": "second-effect"},
            )
        with pytest.raises(DispatchIntentConflict):
            dispatcher.authorize(
                cancel_lease,
                "activity:call-1",
                {"activity": "changed", "target": "remote-effect"},
            )

        # A read-only late observation can update the prior intent and event
        # history while the run remains terminal. Replaying it adds no event.
        observation = dispatcher.observe(
            run_id=cancel_run,
            operation_key="activity:call-1",
            observation={"remote_id": "r-17", "state": "completed"},
        )
        assert observation.observed and not observation.adopted
        assert dispatcher.observe(
            run_id=cancel_run,
            operation_key="activity:call-1",
            observation={"remote_id": "r-17", "state": "completed"},
        ).adopted
        with pytest.raises(DispatchIntentConflict):
            dispatcher.observe(
                run_id=cancel_run,
                operation_key="activity:call-1",
                observation={"remote_id": "other", "state": "completed"},
            )
        _assert_late_observation_keeps_terminal_and_reservation(
            admin_engine, cancel_owner, cancel_run
        )

        with pytest.raises(LeaseLost):
            queue.heartbeat(cancel_lease)
        with pytest.raises(LeaseLost):
            queue.persist_checkpoint(cancel_lease, "after-cancel", {"checkpoint": 1})
        with pytest.raises(LeaseLost):
            TransitionService(runtime_engine, cancel_principal).commit_result(
                lease=cancel_lease,
                result={"accepted": True},
                target_run_status=RunStatus.WAITING_CREDENTIALS,
            )
        with pytest.raises(LeaseLost):
            recovery.fail_attempt(
                lease=cancel_lease,
                failure_class=FailureClass.TRANSIENT_READ,
            )

        # Cancellation releases only the active run slot. The owner can admit
        # and claim another run while the old resource reservation remains.
        next_run = _create_queued_run(admin_engine, cancel_owner)
        next_lease = queue.claim_one("after-cancellation")
        assert next_lease is not None and next_lease.run_id == next_run

        # Owner isolation is enforced by principal-scoped repository access.
        stranger = CancellationService(runtime_engine, Principal(uuid4()))
        with pytest.raises(RuntimeRunNotFound):
            stranger.cancel(cancel_run)

        _assert_result_cancel_orderings(admin_engine, runtime_engine)
        _assert_deadline_expiry(admin_engine, runtime_engine)
        _assert_concurrent_dispatch_cancel_race(admin_engine, runtime_engine)
        _assert_concurrent_result_cancel_race(admin_engine, runtime_engine)
        _assert_other_terminal_state_is_preserved(admin_engine, runtime_engine)

        # Roll back 07 with populated cancelled state, then reapply it. The
        # legacy labels map to their closest pre-T11 values; the cancellation
        # run state and immutable event remain in the 06 schema.
        _run_migration("downgrade", T06_REVISION, database_url)
        migration_applied = False
        _assert_downgrade_kept_cancel_event(admin_engine, cancel_owner, cancel_run, cancel_lease)
        _run_migration("upgrade", T11_REVISION, database_url)
        migration_applied = True
        _assert_revision_07_objects_exist(admin_engine)
    finally:
        if runtime_engine is not None:
            runtime_engine.dispose()
        try:
            if migration_applied:
                _run_migration("downgrade", "base", database_url)
        finally:
            _cleanup_database(admin_engine)
            admin_engine.dispose()


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
            exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role_name)"),
                {"role_name": role_name},
            ).scalar_one()
            if exists:
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
        pytest.fail("T11 integration requires PostgreSQL 17")


def _run_migration(action: str, revision: str, database_url: str) -> None:
    _migration_command(action, revision, database_url)


def _migration_command(action: str, revision: str, database_url: str) -> None:
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
        pytest.fail(f"migration {action} {revision} failed: {result.stdout}\n{result.stderr}")


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


def _seed_claimed_run(
    admin_engine: Engine, runtime_engine: Engine
) -> tuple[UUID, UUID, ExecutionLease]:
    owner_id = uuid4()
    principal = Principal(owner_id)
    with admin_engine.begin() as connection:
        connection.execute(
            text("INSERT INTO runtime_private.users (user_id, github_user_id) VALUES (:id, :gh)"),
            {"id": owner_id, "gh": owner_id.int % 2_000_000_000 + 1},
        )
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run()
        step_id = repository.create_step(run_id, "cancel-test", "test")
        repository.create_job(run_id, step_id)
    lease = QueueService(runtime_engine, principal).claim_one("before-cancel")
    assert lease is not None and lease.run_id == run_id
    return owner_id, run_id, lease


def _create_queued_run(admin_engine: Engine, owner_id: UUID) -> UUID:
    with admin_engine.begin() as connection:
        repository = RuntimeRepository(connection, Principal(owner_id))
        run_id = repository.create_run()
        step_id = repository.create_step(run_id, "after-cancel", "test")
        repository.create_job(run_id, step_id)
        return run_id


def _seed_unknown_reservation(admin_engine: Engine, owner_id: UUID, run_id: UUID) -> UUID:
    reservation_id = uuid4()
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.resource_reservations "
                "(reservation_id, owner_id, run_id, logical_key, resource_kind, status, metadata) "
                "VALUES (:id, :owner_id, :run_id, 'sandbox:primary', 'sandbox', 'unknown', "
                'CAST(\'{"allocation":"response-lost"}\' AS jsonb))'
            ),
            {"id": reservation_id, "owner_id": owner_id, "run_id": run_id},
        )
    return reservation_id


def _assert_cancelled_state(
    engine: Engine, owner_id: UUID, run_id: UUID, lease: ExecutionLease
) -> None:
    with engine.connect() as connection:
        run_status = connection.execute(
            text(
                "SELECT status FROM runtime_private.runs "
                "WHERE owner_id=:owner_id AND run_id=:run_id"
            ),
            {"owner_id": owner_id, "run_id": run_id},
        ).scalar_one()
        job = (
            connection.execute(
                text(
                    "SELECT status, generation, lease_owner, lease_expires_at "
                    "FROM runtime_private.jobs WHERE owner_id=:owner_id AND run_id=:run_id"
                ),
                {"owner_id": owner_id, "run_id": run_id},
            )
            .mappings()
            .one()
        )
        (step_status,) = connection.execute(
            text(
                "SELECT status FROM runtime_private.steps "
                "WHERE owner_id=:owner_id AND run_id=:run_id"
            ),
            {"owner_id": owner_id, "run_id": run_id},
        ).one()
        attempt = (
            connection.execute(
                text(
                    "SELECT status, failure_class, finished_at FROM runtime_private.attempts "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND attempt_id=:attempt_id"
                ),
                {"owner_id": owner_id, "run_id": run_id, "attempt_id": lease.attempt_id},
            )
            .mappings()
            .one()
        )
        cleanup = connection.execute(
            text(
                "SELECT cleanup_kind, status FROM runtime_private.cleanup_intents "
                "WHERE owner_id=:owner_id AND run_id=:run_id ORDER BY cleanup_kind"
            ),
            {"owner_id": owner_id, "run_id": run_id},
        ).all()
        event_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.events WHERE owner_id=:owner_id "
                "AND run_id=:run_id AND event_type='run.cancelled'"
            ),
            {"owner_id": owner_id, "run_id": run_id},
        ).scalar_one()
    assert run_status == "cancelled"
    assert job["status"] == "cancelled" and job["generation"] == lease.generation + 1
    assert job["lease_owner"] is None and job["lease_expires_at"] is None
    assert step_status == "cancelled"
    assert attempt["status"] == "unknown" and attempt["failure_class"] == "cancelled"
    assert attempt["finished_at"] is not None
    assert cleanup == [("observe_effects", "pending"), ("stop_execution", "pending")]
    assert event_count == 1


def _assert_late_observation_keeps_terminal_and_reservation(
    engine: Engine, owner_id: UUID, run_id: UUID
) -> None:
    with engine.connect() as connection:
        run_status = connection.execute(
            text("SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
        reservation = (
            connection.execute(
                text(
                    "SELECT status, metadata FROM runtime_private.resource_reservations "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": owner_id, "run": run_id},
            )
            .mappings()
            .one()
        )
        intent = connection.execute(
            text(
                "SELECT observation FROM runtime_private.dispatch_intents "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
        observation_events = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.events WHERE owner_id=:owner "
                "AND run_id=:run AND event_type='dispatch.observed'"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
    assert run_status == "cancelled"
    assert reservation["status"] == "unknown"
    assert reservation["metadata"] == {"allocation": "response-lost"}
    assert intent == {"remote_id": "r-17", "state": "completed"}
    assert observation_events == 1


def _assert_result_cancel_orderings(admin_engine: Engine, runtime_engine: Engine) -> None:
    result_owner, result_run, result_lease = _seed_claimed_run(admin_engine, runtime_engine)
    principal = Principal(result_owner)
    transitions = TransitionService(runtime_engine, principal)
    original = transitions.commit_result(
        lease=result_lease,
        result={"accepted": "before-cancel"},
        target_run_status=RunStatus.WAITING_CREDENTIALS,
    )
    assert not original.adopted
    cancelled = CancellationService(runtime_engine, principal).cancel(result_run)
    assert cancelled.run_status is RunStatus.CANCELLED
    replay = transitions.commit_result(
        lease=result_lease,
        result={"accepted": "before-cancel"},
        target_run_status=RunStatus.WAITING_CREDENTIALS,
    )
    assert replay.adopted and replay.result_id == original.result_id

    after_owner, after_run, after_lease = _seed_claimed_run(admin_engine, runtime_engine)
    after_service = TransitionService(runtime_engine, Principal(after_owner))
    CancellationService(runtime_engine, Principal(after_owner)).cancel(after_run)
    with pytest.raises(LeaseLost):
        after_service.commit_result(
            lease=after_lease,
            result={"accepted": "after-cancel"},
            target_run_status=RunStatus.WAITING_CREDENTIALS,
        )


def _assert_deadline_expiry(admin_engine: Engine, runtime_engine: Engine) -> None:
    before_owner, before_run = _seed_waiting_run(admin_engine, overdue=False)
    before_expiry = CancellationService(runtime_engine, Principal(before_owner))
    assert before_expiry.expire_due(limit=5) == 0
    with admin_engine.connect() as connection:
        assert (
            connection.execute(
                text(
                    "SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": before_owner, "run": before_run},
            ).scalar_one()
            == "waiting_capacity"
        )

    waiting_owner, waiting_run = _seed_waiting_run(admin_engine, overdue=True)
    waiting_service = CancellationService(runtime_engine, Principal(waiting_owner))
    assert waiting_service.expire_due(limit=5) == 1
    assert waiting_service.expire_due(limit=5) == 0
    _assert_expired_state(admin_engine, waiting_owner, waiting_run, expected_attempts=0)
    waiting_queue = QueueService(runtime_engine, Principal(waiting_owner))
    assert waiting_queue.claim_one("expired-waiting-run") is None
    replacement_run = _create_queued_run(admin_engine, waiting_owner)
    replacement_lease = waiting_queue.claim_one("after-expiry-slot-release")
    assert replacement_lease is not None and replacement_lease.run_id == replacement_run

    running_owner, running_run, running_lease = _seed_claimed_run(admin_engine, runtime_engine)
    _set_run_overdue(admin_engine, running_owner, running_run)
    running_service = CancellationService(runtime_engine, Principal(running_owner))
    assert running_service.expire_due(limit=5) == 1
    _assert_expired_state(admin_engine, running_owner, running_run, expected_attempts=1)
    with pytest.raises(LeaseLost):
        QueueService(runtime_engine, Principal(running_owner)).heartbeat(running_lease)

    race_owner, race_run = _seed_waiting_run(admin_engine, overdue=True)
    race_service = CancellationService(runtime_engine, Principal(race_owner))
    barrier = threading.Barrier(3)

    def expire() -> int:
        barrier.wait()
        return race_service.expire_due(limit=1)

    def cancel() -> RunStatus:
        barrier.wait()
        return race_service.cancel(race_run).run_status

    with ThreadPoolExecutor(max_workers=2) as pool:
        expiry_future = pool.submit(expire)
        cancel_future = pool.submit(cancel)
        barrier.wait()
        expiry_count = expiry_future.result(timeout=15)
        cancel_status = cancel_future.result(timeout=15)
    with admin_engine.connect() as connection:
        run_status = connection.execute(
            text("SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
            {"owner": race_owner, "run": race_run},
        ).scalar_one()
        terminal_events = connection.execute(
            text(
                "SELECT event_type, actor_type FROM runtime_private.events "
                "WHERE owner_id=:owner AND run_id=:run "
                "AND event_type IN ('run.cancelled', 'run.expired')"
            ),
            {"owner": race_owner, "run": race_run},
        ).all()
    assert run_status in {"cancelled", "expired"}
    assert len(terminal_events) == 1
    if run_status == "cancelled":
        assert cancel_status is RunStatus.CANCELLED and expiry_count == 0
        assert terminal_events == [("run.cancelled", "account")]
    else:
        assert cancel_status is RunStatus.EXPIRED and expiry_count == 1
        assert terminal_events == [("run.expired", "system")]


def _seed_waiting_run(admin_engine: Engine, *, overdue: bool) -> tuple[UUID, UUID]:
    owner_id, run_id, step_id, job_id = uuid4(), uuid4(), uuid4(), uuid4()
    if overdue:
        run_sql = (
            "INSERT INTO runtime_private.runs "
            "(run_id, owner_id, status, created_at, deadline_at) "
            "VALUES (:run, :owner, 'waiting_capacity', clock_timestamp() - interval '2 hours', "
            "clock_timestamp() - interval '1 hour')"
        )
    else:
        run_sql = (
            "INSERT INTO runtime_private.runs "
            "(run_id, owner_id, status, created_at, deadline_at) "
            "VALUES (:run, :owner, 'waiting_capacity', clock_timestamp() - interval '1 minute', "
            "clock_timestamp() + interval '10 minutes')"
        )
    with admin_engine.begin() as connection:
        connection.execute(
            text("INSERT INTO runtime_private.users (user_id, github_user_id) VALUES (:id, :gh)"),
            {"id": owner_id, "gh": owner_id.int % 2_000_000_000 + 1},
        )
        connection.execute(text(run_sql), {"run": run_id, "owner": owner_id})
        connection.execute(
            text(
                "INSERT INTO runtime_private.steps "
                "(step_id, owner_id, run_id, logical_key, activity) "
                "VALUES (:step, :owner, :run, 'waiting', 'test')"
            ),
            {"step": step_id, "owner": owner_id, "run": run_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.jobs (job_id, owner_id, run_id, step_id) "
                "VALUES (:job, :owner, :run, :step)"
            ),
            {"job": job_id, "owner": owner_id, "run": run_id, "step": step_id},
        )
    return owner_id, run_id


def _set_run_overdue(engine: Engine, owner_id: UUID, run_id: UUID) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs "
                "SET created_at=clock_timestamp() - interval '2 hours', "
                "deadline_at=clock_timestamp() - interval '1 hour' "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": run_id},
        )


def _assert_expired_state(
    engine: Engine, owner_id: UUID, run_id: UUID, *, expected_attempts: int
) -> None:
    with engine.connect() as connection:
        run_status = connection.execute(
            text("SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
        job = (
            connection.execute(
                text(
                    "SELECT status, generation, lease_owner, lease_expires_at "
                    "FROM runtime_private.jobs WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": owner_id, "run": run_id},
            )
            .mappings()
            .one()
        )
        attempts = (
            connection.execute(
                text(
                    "SELECT status, failure_class, finished_at FROM runtime_private.attempts "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": owner_id, "run": run_id},
            )
            .mappings()
            .all()
        )
        expired_events = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.events WHERE owner_id=:owner "
                "AND run_id=:run AND event_type='run.expired' AND actor_type='system'"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
        cleanup_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.cleanup_intents "
                "WHERE owner_id=:owner AND run_id=:run AND status='pending'"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
    assert run_status == "expired"
    assert job["status"] == "cancelled" and job["lease_owner"] is None
    assert job["lease_expires_at"] is None
    assert job["generation"] == (2 if expected_attempts else 1)
    assert len(attempts) == expected_attempts
    assert all(
        attempt["status"] == "unknown"
        and attempt["failure_class"] is None
        and attempt["finished_at"] is not None
        for attempt in attempts
    )
    assert expired_events == 1 and cleanup_count == 2


def _assert_other_terminal_state_is_preserved(admin_engine: Engine, runtime_engine: Engine) -> None:
    owner_id = uuid4()
    run_id = uuid4()
    with admin_engine.begin() as connection:
        connection.execute(
            text("INSERT INTO runtime_private.users (user_id, github_user_id) VALUES (:id, :gh)"),
            {"id": owner_id, "gh": owner_id.int % 2_000_000_000 + 1},
        )
        repository = RuntimeRepository(connection, Principal(owner_id))
        run_id = repository.create_run()
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET status='succeeded' "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": run_id},
        )
    receipt = CancellationService(runtime_engine, Principal(owner_id)).cancel(run_id)
    assert receipt.run_status is RunStatus.SUCCEEDED and not receipt.adopted
    with admin_engine.connect() as connection:
        assert (
            connection.execute(
                text(
                    "SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": owner_id, "run": run_id},
            ).scalar_one()
            == "succeeded"
        )


def _assert_ledger_owner_isolation(engine: Engine, owner_id: UUID, run_id: UUID) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, Principal(uuid4()))
        reservations = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.resource_reservations "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
        intents = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.dispatch_intents "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
        cleanup = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.cleanup_intents "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
    assert reservations == intents == cleanup == 0


def _assert_metadata_bounds(
    admin_engine: Engine,
    dispatcher: DispatchIntentService,
    owner_id: UUID,
    run_id: UUID,
    lease: ExecutionLease,
) -> None:
    with pytest.raises(ValueError, match="runtime_dispatch_metadata_too_large"):
        dispatcher.authorize(lease, "oversized-client-intent", {"large": "x" * 5000})
    with pytest.raises(ValueError, match="runtime_observation_too_large"):
        dispatcher.observe(
            run_id=run_id,
            operation_key="activity:call-1",
            observation={"large": "x" * 5000},
        )
    with pytest.raises(IntegrityError):
        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO runtime_private.resource_reservations "
                    "(reservation_id, owner_id, run_id, logical_key, resource_kind, "
                    "status, metadata) "
                    "VALUES (:id, :owner, :run, 'oversized', 'sandbox', 'unknown', "
                    "CAST(:metadata AS jsonb))"
                ),
                {
                    "id": uuid4(),
                    "owner": owner_id,
                    "run": run_id,
                    "metadata": '{"large":"' + ("x" * 5000) + '"}',
                },
            )


def _assert_concurrent_dispatch_cancel_race(admin_engine: Engine, runtime_engine: Engine) -> None:
    owner_id, run_id, lease = _seed_claimed_run(admin_engine, runtime_engine)
    principal = Principal(owner_id)
    barrier = threading.Barrier(3)
    dispatcher = DispatchIntentService(runtime_engine, principal)
    cancellation = CancellationService(runtime_engine, principal)

    def authorize() -> str:
        barrier.wait()
        try:
            receipt = dispatcher.authorize(lease, "racing-dispatch", {"target": "remote"})
            return "committed" if not receipt.adopted else "adopted"
        except LeaseLost:
            return "fenced"

    def cancel() -> RunStatus:
        barrier.wait()
        return cancellation.cancel(run_id).run_status

    with ThreadPoolExecutor(max_workers=2) as pool:
        authorize_future = pool.submit(authorize)
        cancel_future = pool.submit(cancel)
        barrier.wait()
        authorize_result = authorize_future.result(timeout=15)
        cancel_result = cancel_future.result(timeout=15)
    assert cancel_result is RunStatus.CANCELLED
    assert authorize_result in {"committed", "fenced"}
    with admin_engine.connect() as connection:
        intent_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.dispatch_intents "
                "WHERE owner_id=:owner AND run_id=:run AND operation_key='racing-dispatch'"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
        cancel_event_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.events WHERE owner_id=:owner "
                "AND run_id=:run AND event_type='run.cancelled'"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
    assert intent_count == (1 if authorize_result == "committed" else 0)
    assert cancel_event_count == 1


def _assert_concurrent_result_cancel_race(admin_engine: Engine, runtime_engine: Engine) -> None:
    owner_id, run_id, lease = _seed_claimed_run(admin_engine, runtime_engine)
    principal = Principal(owner_id)
    barrier = threading.Barrier(3)
    transitions = TransitionService(runtime_engine, principal)
    cancellation = CancellationService(runtime_engine, principal)

    def commit_result() -> str:
        barrier.wait()
        try:
            receipt = transitions.commit_result(
                lease=lease,
                result={"race": "result"},
                target_run_status=RunStatus.WAITING_CREDENTIALS,
            )
            return "committed" if not receipt.adopted else "adopted"
        except LeaseLost:
            return "fenced"

    def cancel() -> RunStatus:
        barrier.wait()
        return cancellation.cancel(run_id).run_status

    with ThreadPoolExecutor(max_workers=2) as pool:
        result_future = pool.submit(commit_result)
        cancel_future = pool.submit(cancel)
        barrier.wait()
        result = result_future.result(timeout=15)
        cancel_result = cancel_future.result(timeout=15)
    assert cancel_result is RunStatus.CANCELLED
    assert result in {"committed", "fenced"}
    with admin_engine.connect() as connection:
        accepted_results = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
    assert accepted_results == (1 if result == "committed" else 0)


def _assert_downgrade_kept_cancel_event(
    engine: Engine, owner_id: UUID, run_id: UUID, lease: ExecutionLease
) -> None:
    with engine.connect() as connection:
        (run_status,) = connection.execute(
            text("SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
            {"owner": owner_id, "run": run_id},
        ).one()
        (step_status,) = connection.execute(
            text("SELECT status FROM runtime_private.steps WHERE owner_id=:owner AND run_id=:run"),
            {"owner": owner_id, "run": run_id},
        ).one()
        failure_class, attempt_status = connection.execute(
            text(
                "SELECT failure_class, status FROM runtime_private.attempts "
                "WHERE owner_id=:owner AND run_id=:run AND attempt_id=:attempt"
            ),
            {"owner": owner_id, "run": run_id, "attempt": lease.attempt_id},
        ).one()
        cancelled_events = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.events WHERE owner_id=:owner "
                "AND run_id=:run AND event_type='run.cancelled'"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
    assert run_status == "cancelled" and step_status == "skipped"
    assert attempt_status == "unknown" and failure_class is None
    assert cancelled_events == 1


def _assert_revision_07_objects_exist(engine: Engine) -> None:
    with engine.connect() as connection:
        tables = (
            connection.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='runtime_private' AND table_name IN "
                    "('resource_reservations', 'dispatch_intents', 'cleanup_intents')"
                )
            )
            .scalars()
            .all()
        )
    assert set(tables) == {"resource_reservations", "dispatch_intents", "cleanup_intents"}
