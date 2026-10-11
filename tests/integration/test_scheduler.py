"""PostgreSQL proof for T10 retries, persisted waits, and lease recovery."""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url

from continuum_api.runtime.leases import ExecutionLease, LeaseLost, QueueService
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus
from continuum_api.runtime.scheduler import FailureClass, RuntimeRecoveryService

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t10_test"
T10_REVISION = "20261011_06"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)


def test_retry_waits_and_expired_lease_recovery_are_durable() -> None:
    database_url = os.environ.get("CONTINUUM_T10_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T10_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T10 integration requires loopback PostgreSQL named continuum_t10_test")

    admin_engine = create_engine(
        url.set(drivername="postgresql+psycopg"), hide_parameters=True, pool_size=8, max_overflow=0
    )
    runtime_password = secrets.token_urlsafe(32)
    runtime_url = _role_url(url, runtime_password)
    runtime_url_text = runtime_url.render_as_string(hide_password=False)
    runtime_engine: Engine | None = None
    migration_applied = False

    try:
        _reset_database(admin_engine, runtime_password)
        _assert_postgres_17(admin_engine)
        _run_migration(database_url)
        migration_applied = True
        runtime_engine = create_engine(
            runtime_url, hide_parameters=True, pool_size=4, max_overflow=0
        )

        (
            safe_owner,
            deadline_owner,
            unsafe_owner,
            race_owner,
            capacity_manual_owner,
            capacity_timed_owner,
            expired_owner,
            other_owner,
        ) = _seed_accounts(admin_engine, 8)
        safe_ids = _seed_work(admin_engine, safe_owner, ReplayPolicy.REPLAY_SAFE)
        deadline_ids = _seed_work(admin_engine, deadline_owner, ReplayPolicy.REPLAY_SAFE)
        unsafe_ids = _seed_work(admin_engine, unsafe_owner, ReplayPolicy.ATTENTION_ON_UNKNOWN)
        race_ids = _seed_work(admin_engine, race_owner, ReplayPolicy.REPLAY_SAFE)
        capacity_manual_ids = _seed_work(
            admin_engine, capacity_manual_owner, ReplayPolicy.REPLAY_SAFE
        )
        capacity_timed_ids = _seed_work(
            admin_engine, capacity_timed_owner, ReplayPolicy.REPLAY_SAFE
        )
        expired_ids = _seed_work(admin_engine, expired_owner, ReplayPolicy.REPLAY_SAFE)

        safe_queue = QueueService(runtime_engine, Principal(safe_owner))
        safe_recovery = RuntimeRecoveryService(runtime_engine, Principal(safe_owner))
        first_lease = _claim(safe_queue, safe_ids)
        _expire_lease(admin_engine, first_lease)
        assert safe_recovery.recover_due(limit=1) == 1
        assert safe_recovery.recover_due(limit=1) == 0
        _assert_crash_was_not_retry_budget(admin_engine, first_lease)
        second_lease = safe_queue.claim_one("after-crash-worker")
        assert second_lease is not None and second_lease.generation == 2
        _assert_failure_owner_isolation(runtime_engine, other_owner, second_lease)
        with pytest.raises(LeaseLost):
            RuntimeRecoveryService(runtime_engine, Principal(safe_owner)).fail_attempt(
                lease=first_lease, failure_class=FailureClass.TRANSIENT_READ
            )

        retry_after = _database_now(admin_engine) + timedelta(seconds=1.5)
        scheduled = safe_recovery.fail_attempt(
            lease=second_lease,
            failure_class=FailureClass.TRANSIENT_READ,
            retry_after=retry_after,
        )
        assert scheduled.retry_scheduled and scheduled.failure_count == 1
        assert scheduled.available_at is not None and scheduled.available_at >= retry_after
        _assert_retry_wait(admin_engine, second_lease, scheduled.available_at)

        # A fresh engine models process restart: eligibility remains in PostgreSQL.
        runtime_engine.dispose()
        runtime_engine = create_engine(
            runtime_url, hide_parameters=True, pool_size=4, max_overflow=0
        )
        safe_queue = QueueService(runtime_engine, Principal(safe_owner))
        assert safe_queue.claim_one("before-durable-wait") is None
        _wait_until_database_time(admin_engine, scheduled.available_at)
        third_lease = safe_queue.claim_one("after-durable-wait")
        assert third_lease is not None and third_lease.generation == 3
        _assert_failure_and_attempt_counts(admin_engine, third_lease)

        deadline_queue = QueueService(runtime_engine, Principal(deadline_owner))
        deadline_recovery = RuntimeRecoveryService(runtime_engine, Principal(deadline_owner))
        deadline_lease = _claim(deadline_queue, deadline_ids)
        deadline_at = _database_now(admin_engine) + timedelta(seconds=10)
        _set_run_deadline(admin_engine, deadline_lease, deadline_at)
        no_time_retry = deadline_recovery.fail_attempt(
            lease=deadline_lease,
            failure_class=FailureClass.TRANSIENT_READ,
            retry_after=deadline_at + timedelta(seconds=1),
        )
        assert not no_time_retry.retry_scheduled
        assert no_time_retry.run_status is RunStatus.FAILED
        assert deadline_queue.claim_one("past-retry-deadline") is None

        unsafe_queue = QueueService(runtime_engine, Principal(unsafe_owner))
        unsafe_recovery = RuntimeRecoveryService(runtime_engine, Principal(unsafe_owner))
        unsafe_lease = _claim(unsafe_queue, unsafe_ids)
        _expire_lease(admin_engine, unsafe_lease)
        assert unsafe_queue.claim_one("unsafe-must-not-reclaim") is None
        assert unsafe_recovery.recover_due(limit=5) == 1
        assert unsafe_recovery.recover_due(limit=5) == 0
        _assert_unknown_expiry_needs_attention(admin_engine, unsafe_lease)
        assert unsafe_queue.claim_one("attention-is-not-replayed") is None

        _assert_concurrent_recovery(admin_engine, runtime_url_text, race_owner, race_ids)
        _assert_capacity_wait_is_explicit(
            admin_engine,
            runtime_engine,
            runtime_url_text,
            capacity_manual_owner,
            capacity_manual_ids,
            capacity_timed_owner,
            capacity_timed_ids,
        )
        _assert_expired_deadline_is_not_recovered(
            admin_engine, runtime_engine, expired_owner, expired_ids
        )

        _run_migration_down(database_url)
        migration_applied = False
    finally:
        if runtime_engine is not None:
            runtime_engine.dispose()
        try:
            if migration_applied:
                _run_migration_down(database_url)
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
        pytest.fail("T10 integration requires PostgreSQL 17")


def _run_migration(database_url: str) -> None:
    _migration_command("upgrade", T10_REVISION, database_url)


def _run_migration_down(database_url: str) -> None:
    _migration_command("downgrade", "base", database_url)


def _migration_command(operation: str, revision: str, database_url: str) -> None:
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
        pytest.fail("T10 migration command failed; output redacted")


def _seed_accounts(engine: Engine, count: int) -> tuple[UUID, ...]:
    owners = tuple(uuid4() for _ in range(count))
    with engine.begin() as connection:
        for github_id, owner_id in enumerate(owners, start=9_300_000):
            connection.execute(
                text(
                    "INSERT INTO runtime_private.users (user_id, github_user_id) "
                    "VALUES (:owner_id, :github_id)"
                ),
                {"owner_id": owner_id, "github_id": github_id},
            )
    return owners


def _seed_work(
    engine: Engine, owner_id: UUID, replay_policy: ReplayPolicy
) -> tuple[UUID, UUID, UUID]:
    run_id, step_id, job_id = uuid4(), uuid4(), uuid4()
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO runtime_private.runs (run_id, owner_id) VALUES (:run, :owner)"),
            {"run": run_id, "owner": owner_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.steps "
                "(step_id, owner_id, run_id, logical_key, activity) "
                "VALUES (:step, :owner, :run, :key, 'prepare')"
            ),
            {"step": step_id, "owner": owner_id, "run": run_id, "key": f"prepare:{run_id}"},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.jobs "
                "(job_id, owner_id, run_id, step_id, replay_policy) "
                "VALUES (:job, :owner, :run, :step, :replay_policy)"
            ),
            {
                "job": job_id,
                "owner": owner_id,
                "run": run_id,
                "step": step_id,
                "replay_policy": replay_policy.value,
            },
        )
    return run_id, step_id, job_id


def _claim(queue: QueueService, identity: tuple[UUID, UUID, UUID]) -> ExecutionLease:
    lease = queue.claim_one(f"t10-{uuid4()}")
    assert lease is not None
    assert (lease.run_id, lease.step_id, lease.job_id) == identity
    return lease


def _expire_lease(engine: Engine, lease: ExecutionLease) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "SELECT run_id FROM runtime_private.runs "
                "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
            ),
            {"owner": lease.owner_id, "run": lease.run_id},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.jobs "
                "SET lease_expires_at=clock_timestamp() - interval '1 ms' "
                "WHERE owner_id=:owner AND run_id=:run AND job_id=:job"
            ),
            {"owner": lease.owner_id, "run": lease.run_id, "job": lease.job_id},
        )


def _assert_crash_was_not_retry_budget(engine: Engine, lease: ExecutionLease) -> None:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT job.generation, job.failure_count, job.status, step.status, "
                "attempt.status, attempt.failure_class FROM runtime_private.jobs AS job "
                "JOIN runtime_private.steps AS step ON step.owner_id=job.owner_id "
                "AND step.run_id=job.run_id AND step.step_id=job.step_id "
                "JOIN runtime_private.attempts AS attempt ON attempt.owner_id=job.owner_id "
                "AND attempt.run_id=job.run_id AND attempt.job_id=job.job_id "
                "AND attempt.generation=job.generation "
                "WHERE job.owner_id=:owner AND job.job_id=:job"
            ),
            {"owner": lease.owner_id, "job": lease.job_id},
        ).one()
    assert tuple(row) == (1, 0, "queued", "queued", "unknown", "worker_lost")


def _assert_failure_owner_isolation(
    engine: Engine, other_owner: UUID, lease: ExecutionLease
) -> None:
    service = RuntimeRecoveryService(engine, Principal(other_owner))
    with pytest.raises(LeaseLost, match="runtime_lease_owner_mismatch"):
        service.fail_attempt(lease=lease, failure_class=FailureClass.TRANSIENT_READ)
    assert service.recover_due(limit=10) == 0


def _assert_retry_wait(engine: Engine, lease: ExecutionLease, available_at: datetime) -> None:
    with engine.connect() as connection:
        state = connection.execute(
            text(
                "SELECT job.generation, job.failure_count, job.status, job.available_at, "
                "step.status, attempt.status, attempt.failure_class "
                "FROM runtime_private.jobs AS job "
                "JOIN runtime_private.steps AS step ON step.owner_id=job.owner_id "
                "AND step.run_id=job.run_id AND step.step_id=job.step_id "
                "JOIN runtime_private.attempts AS attempt ON attempt.owner_id=job.owner_id "
                "AND attempt.run_id=job.run_id AND attempt.job_id=job.job_id "
                "AND attempt.generation=job.generation "
                "WHERE job.owner_id=:owner AND job.job_id=:job"
            ),
            {"owner": lease.owner_id, "job": lease.job_id},
        ).one()
    assert tuple(state) == (
        lease.generation,
        1,
        "queued",
        available_at,
        "queued",
        "retryable_failure",
        "transient_read",
    )


def _assert_failure_and_attempt_counts(engine: Engine, lease: ExecutionLease) -> None:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT generation, failure_count, status FROM runtime_private.jobs "
                "WHERE owner_id=:owner AND job_id=:job"
            ),
            {"owner": lease.owner_id, "job": lease.job_id},
        ).one()
        attempts = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.attempts "
                "WHERE owner_id=:owner AND run_id=:run AND job_id=:job"
            ),
            {"owner": lease.owner_id, "run": lease.run_id, "job": lease.job_id},
        ).scalar_one()
    assert tuple(row) == (3, 1, "leased")
    assert attempts == 3


def _set_run_deadline(engine: Engine, lease: ExecutionLease, deadline_at: datetime) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET deadline_at=:deadline "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"deadline": deadline_at, "owner": lease.owner_id, "run": lease.run_id},
        )


def _assert_unknown_expiry_needs_attention(engine: Engine, lease: ExecutionLease) -> None:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT run.status, step.status, job.status, job.failure_count, "
                "attempt.status, attempt.failure_class FROM runtime_private.runs AS run "
                "JOIN runtime_private.steps AS step ON step.owner_id=run.owner_id "
                "AND step.run_id=run.run_id JOIN runtime_private.jobs AS job "
                "ON job.owner_id=step.owner_id AND job.run_id=step.run_id "
                "AND job.step_id=step.step_id JOIN runtime_private.attempts AS attempt "
                "ON attempt.owner_id=job.owner_id AND attempt.run_id=job.run_id "
                "AND attempt.job_id=job.job_id AND attempt.generation=job.generation "
                "WHERE run.owner_id=:owner AND run.run_id=:run AND job.job_id=:job"
            ),
            {"owner": lease.owner_id, "run": lease.run_id, "job": lease.job_id},
        ).one()
    assert tuple(row) == (
        "needs_attention",
        "failed",
        "failed",
        0,
        "unknown",
        "unknown_effect",
    )


def _assert_concurrent_recovery(
    admin_engine: Engine,
    runtime_url: str,
    owner_id: UUID,
    identity: tuple[UUID, UUID, UUID],
) -> None:
    claim_engine = create_engine(runtime_url, pool_size=1, max_overflow=0)
    try:
        lease = _claim(QueueService(claim_engine, Principal(owner_id)), identity)
    finally:
        claim_engine.dispose()
    _expire_lease(admin_engine, lease)
    engines = [create_engine(runtime_url, pool_size=1, max_overflow=0) for _ in range(2)]
    try:
        services = [RuntimeRecoveryService(engine, Principal(owner_id)) for engine in engines]
        with ThreadPoolExecutor(max_workers=2) as executor:
            recovered = list(executor.map(lambda service: service.recover_due(limit=10), services))
        assert sorted(recovered) == [0, 1]
        with admin_engine.connect() as connection:
            counts = connection.execute(
                text(
                    "SELECT job.generation, job.failure_count, job.status, step.status, "
                    "(SELECT count(*) FROM runtime_private.events WHERE owner_id=job.owner_id "
                    "AND run_id=job.run_id AND event_type='job.lease_expired_requeued') "
                    "FROM runtime_private.jobs AS job JOIN runtime_private.steps AS step "
                    "ON step.owner_id=job.owner_id AND step.run_id=job.run_id "
                    "AND step.step_id=job.step_id WHERE job.owner_id=:owner AND job.job_id=:job"
                ),
                {"owner": owner_id, "job": lease.job_id},
            ).one()
        assert tuple(counts) == (1, 0, "queued", "queued", 1)
    finally:
        for engine in engines:
            engine.dispose()


def _assert_capacity_wait_is_explicit(
    admin_engine: Engine,
    runtime_engine: Engine,
    runtime_url: str,
    manual_owner: UUID,
    manual_identity: tuple[UUID, UUID, UUID],
    timed_owner: UUID,
    timed_identity: tuple[UUID, UUID, UUID],
) -> None:
    queue = QueueService(runtime_engine, Principal(manual_owner))
    recovery = RuntimeRecoveryService(runtime_engine, Principal(manual_owner))
    lease = _claim(queue, manual_identity)
    assert (
        recovery.fail_attempt(
            lease=lease, failure_class=FailureClass.RESOURCE_EXHAUSTION
        ).capacity_resume_at
        is None
    )
    assert recovery.recover_due(limit=10) == 0
    assert queue.claim_one("capacity-do-not-spin") is None

    timed_queue = QueueService(runtime_engine, Principal(timed_owner))
    timed_recovery = RuntimeRecoveryService(runtime_engine, Principal(timed_owner))
    timed_lease = _claim(timed_queue, timed_identity)
    resume_at = _database_now(admin_engine) + timedelta(seconds=1.2)
    receipt = timed_recovery.fail_attempt(
        lease=timed_lease,
        failure_class=FailureClass.RESOURCE_EXHAUSTION,
        retry_after=resume_at,
    )
    assert receipt.capacity_resume_at == resume_at
    assert timed_recovery.recover_due(limit=10) == 0
    _wait_until_database_time(admin_engine, resume_at)
    assert timed_recovery.recover_due(limit=10) == 1
    resumed = timed_queue.claim_one("after-capacity-window")
    assert resumed is not None and resumed.generation == 2


def _assert_expired_deadline_is_not_recovered(
    admin_engine: Engine,
    runtime_engine: Engine,
    owner_id: UUID,
    identity: tuple[UUID, UUID, UUID],
) -> None:
    queue = QueueService(runtime_engine, Principal(owner_id))
    recovery = RuntimeRecoveryService(runtime_engine, Principal(owner_id))
    lease = _claim(queue, identity)
    _expire_lease(admin_engine, lease)
    _set_run_deadline(admin_engine, lease, _database_now(admin_engine) - timedelta(seconds=1))
    assert recovery.recover_due(limit=10) == 0
    assert queue.claim_one("expired-run-must-not-claim") is None
    with admin_engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT run.status, job.status, job.generation, attempt.status "
                "FROM runtime_private.runs AS run JOIN runtime_private.jobs AS job "
                "ON job.owner_id=run.owner_id AND job.run_id=run.run_id "
                "JOIN runtime_private.attempts AS attempt ON attempt.owner_id=job.owner_id "
                "AND attempt.run_id=job.run_id AND attempt.job_id=job.job_id "
                "AND attempt.generation=job.generation "
                "WHERE run.owner_id=:owner AND run.run_id=:run"
            ),
            {"owner": owner_id, "run": lease.run_id},
        ).one()
    assert tuple(row) == ("running", "leased", 1, "running")


def _database_now(engine: Engine) -> datetime:
    with engine.connect() as connection:
        return connection.execute(text("SELECT clock_timestamp()")).scalar_one()


def _wait_until_database_time(engine: Engine, deadline_at: datetime) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if _database_now(engine) >= deadline_at:
            return
        time.sleep(0.01)
    pytest.fail("T10 durable availability timestamp did not arrive")


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
