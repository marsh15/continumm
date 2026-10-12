"""PostgreSQL proof for durable repository preflight quotas and cache fencing."""

from __future__ import annotations

import os
import secrets
import subprocess
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, Connection, make_url

from continuum_api.repositories.preflight import (
    EligibilityReceipt,
    RepositoryPreflightError,
)
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.repository_preflight import RepositoryPreflightStore

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t13_test"
LATEST_REVISION = "20261012_08"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)


def test_repository_preflight_reservations_quota_cache_and_rls() -> None:
    database_url = os.environ.get("CONTINUUM_T13_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T13_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T13 integration requires loopback PostgreSQL named continuum_t13_test")

    admin_engine = create_engine(
        url.set(drivername="postgresql+psycopg"), hide_parameters=True, pool_size=8, max_overflow=0
    )
    runtime_password = secrets.token_urlsafe(32)
    runtime_engine: Engine | None = None
    migration_applied = False
    try:
        _reset_database(admin_engine, runtime_password)
        _assert_postgres_17(admin_engine)
        _run_migration("upgrade", LATEST_REVISION, database_url)
        migration_applied = True
        runtime_engine = create_engine(
            _role_url(url, runtime_password), hide_parameters=True, pool_size=8, max_overflow=0
        )

        owners = _seed_users(admin_engine, 4)
        store = RepositoryPreflightStore(runtime_engine)
        _assert_cache_and_stale_check_fencing(admin_engine, store, owners[0], owners[1])
        _assert_sliding_quota_and_failure_rollback(admin_engine, store, owners[2])
        _assert_concurrent_reservation_is_serialized(store, owners[3])
        _assert_account_rls(runtime_engine, store, owners[0], owners[1])

        runtime_engine.dispose()
        runtime_engine = None
        _run_migration("downgrade", "base", database_url)
        migration_applied = False
        _assert_schema_absent(admin_engine, "runtime_private")
    finally:
        if runtime_engine is not None:
            runtime_engine.dispose()
        try:
            if migration_applied:
                _run_migration("downgrade", "base", database_url)
        finally:
            _cleanup_database(admin_engine)
            admin_engine.dispose()


def _assert_cache_and_stale_check_fencing(
    admin_engine: Engine,
    store: RepositoryPreflightStore,
    owner_id: UUID,
    other_owner_id: UUID,
) -> None:
    principal = Principal(owner_id)
    other_principal = Principal(other_owner_id)
    repository_id = 91001
    commit_sha = "a" * 40
    profile_id = "python-pytest"
    profile_version = "1"
    receipt = _receipt(repository_id, commit_sha, profile_id, profile_version)

    reservation = store.reserve(principal, repository_id, commit_sha, profile_id, profile_version)
    assert reservation.github_user_id == owner_id.int % 2_000_000_000 + 1
    assert reservation.deadline_at is not None
    with pytest.raises(RepositoryPreflightError) as active_error:
        store.reserve(principal, repository_id + 1, commit_sha, profile_id, profile_version)
    assert active_error.value.code == "repository_preflight_in_progress"

    key = (repository_id, commit_sha, profile_id, profile_version)
    assert store.get_cached(principal, reservation.check_id, *key) is None
    oversized_receipt = EligibilityReceipt(
        repository_id=repository_id,
        commit_sha=commit_sha,
        tree_sha="d" * 40,
        profile_id=profile_id,
        profile_version=profile_version,
        file_count=4,
        source_bytes=256,
        manifest_blobs=tuple((f"manifest-{index}-" + "x" * 240, "e" * 40) for index in range(20)),
        package_count=1,
    )
    with pytest.raises(RepositoryPreflightError) as oversized_error:
        store.put_cached(principal, reservation.check_id, oversized_receipt)
    assert oversized_error.value.code == "repository_preflight_cache_corrupt"
    store.put_cached(principal, reservation.check_id, receipt)
    cached = store.get_cached(principal, reservation.check_id, *key)
    assert cached is not None and cached.cache_hit
    assert cached.manifest_blobs == receipt.manifest_blobs
    with admin_engine.connect() as connection:
        payload: dict[str, object] = connection.execute(
            text(
                "SELECT receipt FROM runtime_private.repository_eligibility_cache "
                "WHERE owner_id=:owner AND github_repository_id=:repo"
            ),
            {"owner": owner_id, "repo": repository_id},
        ).scalar_one()
    assert "cache_hit" not in payload
    assert "content" not in payload

    with pytest.raises(RepositoryPreflightError) as wrong_owner:
        store.get_cached(other_principal, reservation.check_id, *key)
    assert wrong_owner.value.code == "repository_preflight_reservation_lost"

    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.repository_eligibility_cache "
                "SET cached_at=clock_timestamp() - interval '2 minutes', "
                "expires_at=clock_timestamp() - interval '1 second' "
                "WHERE owner_id=:owner AND github_repository_id=:repo"
            ),
            {"owner": owner_id, "repo": repository_id},
        )
    assert store.get_cached(principal, reservation.check_id, *key) is None

    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.repository_preflight_checks "
                "SET created_at=clock_timestamp() - interval '20 minutes', "
                "deadline_at=clock_timestamp() - interval '1 second' "
                "WHERE check_id=:check"
            ),
            {"check": reservation.check_id},
        )
    with pytest.raises(RepositoryPreflightError) as stale_read:
        store.get_cached(principal, reservation.check_id, *key)
    assert stale_read.value.code == "repository_preflight_reservation_lost"

    replacement = store.reserve(principal, repository_id, commit_sha, profile_id, profile_version)
    assert replacement.check_id != reservation.check_id
    stale_actions: tuple[Callable[[], object], ...] = (
        lambda: store.release(principal, reservation.check_id, "failed"),
        lambda: store.put_cached(principal, reservation.check_id, receipt),
    )
    for stale_action in stale_actions:
        with pytest.raises(RepositoryPreflightError) as stale_error:
            stale_action()
        assert stale_error.value.code == "repository_preflight_reservation_lost"
    store.release(principal, replacement.check_id, "completed")

    with admin_engine.connect() as connection:
        old_status: str
        replacement_status: str
        old_status, replacement_status = (
            connection.execute(
                text(
                    "SELECT status FROM runtime_private.repository_preflight_checks "
                    "WHERE owner_id=:owner ORDER BY created_at"
                ),
                {"owner": owner_id},
            )
            .scalars()
            .all()
        )
    assert old_status == "expired"
    assert replacement_status == "completed"


def _assert_sliding_quota_and_failure_rollback(
    admin_engine: Engine, store: RepositoryPreflightStore, owner_id: UUID
) -> None:
    principal = Principal(owner_id)
    for index in range(4):
        reservation = store.reserve(principal, 92000 + index, "b" * 40, "python-pytest", "1")
        store.release(principal, reservation.check_id, "completed")

    fifth = store.reserve(principal, 92010, "b" * 40, "python-pytest", "1")
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.repository_preflight_checks "
                "SET created_at=clock_timestamp() - interval '20 minutes', "
                "deadline_at=clock_timestamp() - interval '1 second' "
                "WHERE check_id=:check"
            ),
            {"check": fifth.check_id},
        )

    with pytest.raises(RepositoryPreflightError) as quota_error:
        store.reserve(principal, 92011, "b" * 40, "python-pytest", "1")
    assert quota_error.value.code == "repository_preflight_rate_limited"
    # Expiring the crashed reservation and then rejecting on quota are one
    # transaction: the failed admission cannot leave a partial state change.
    with admin_engine.connect() as connection:
        status: str = connection.execute(
            text(
                "SELECT status FROM runtime_private.repository_preflight_checks "
                "WHERE check_id=:check"
            ),
            {"check": fifth.check_id},
        ).scalar_one()
    assert status == "active"


def _assert_concurrent_reservation_is_serialized(
    store: RepositoryPreflightStore, owner_id: UUID
) -> None:
    principal = Principal(owner_id)

    def reserve() -> tuple[UUID | None, str | None]:
        try:
            reservation = store.reserve(principal, 93001, "c" * 40, "node-npm", "1")
            return reservation.check_id, None
        except RepositoryPreflightError as error:
            return None, error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: reserve(), range(2)))
    assert sum(check_id is not None for check_id, _ in results) == 1
    assert [code for _, code in results if code is not None] == ["repository_preflight_in_progress"]


def _assert_account_rls(
    runtime_engine: Engine,
    store: RepositoryPreflightStore,
    owner_id: UUID,
    other_owner_id: UUID,
) -> None:
    reservation = store.reserve(Principal(owner_id), 91999, "f" * 40, "python-pytest", "1")
    store.put_cached(
        Principal(owner_id),
        reservation.check_id,
        _receipt(91999, "f" * 40, "python-pytest", "1"),
    )
    with runtime_engine.begin() as connection:
        connection.execute(
            text("SELECT set_config('continuum.principal_id', :owner, true)"),
            {"owner": str(owner_id)},
        )
        own_count: int = connection.execute(
            text("SELECT count(*) FROM runtime_private.repository_preflight_checks")
        ).scalar_one()
        own_cache_count: int = connection.execute(
            text("SELECT count(*) FROM runtime_private.repository_eligibility_cache")
        ).scalar_one()
        connection.execute(
            text("SELECT set_config('continuum.principal_id', :owner, true)"),
            {"owner": str(other_owner_id)},
        )
        other_count: int = connection.execute(
            text("SELECT count(*) FROM runtime_private.repository_preflight_checks")
        ).scalar_one()
        other_cache_count: int = connection.execute(
            text("SELECT count(*) FROM runtime_private.repository_eligibility_cache")
        ).scalar_one()
    assert own_count > 0
    assert own_cache_count > 0
    assert other_count == 0
    assert other_cache_count == 0


def _receipt(
    repository_id: int, commit_sha: str, profile_id: str, profile_version: str
) -> EligibilityReceipt:
    return EligibilityReceipt(
        repository_id=repository_id,
        commit_sha=commit_sha,
        tree_sha="d" * 40,
        profile_id=profile_id,
        profile_version=profile_version,
        file_count=4,
        source_bytes=256,
        manifest_blobs=(("requirements.lock", "e" * 40),),
        package_count=1,
    )


def _seed_users(engine: Engine, count: int) -> tuple[UUID, ...]:
    owners = tuple(uuid4() for _ in range(count))
    with engine.begin() as connection:
        for owner_id in owners:
            connection.execute(
                text(
                    "INSERT INTO runtime_private.users (user_id, github_user_id) "
                    "VALUES (:id, :github_id)"
                ),
                {"id": owner_id, "github_id": owner_id.int % 2_000_000_000 + 1},
            )
    return owners


def _role_url(url: URL, password: str) -> URL:
    return url.set(drivername="postgresql+psycopg", username=RUNTIME_ROLE, password=password)


def _reset_database(engine: Engine, runtime_password: str) -> None:
    with engine.begin() as connection:
        _drop_schemas(connection)
        for role_name in ROLE_NAMES:
            exists: bool = connection.execute(
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


def _drop_schemas(connection: Connection) -> None:
    for schema_name in (
        "runtime_private",
        "wake_private",
        "webhook_private",
        "continuum_migrations",
    ):
        connection.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
    connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))


def _assert_postgres_17(engine: Engine) -> None:
    with engine.connect() as connection:
        version: int = connection.execute(
            text("SELECT current_setting('server_version_num')::integer")
        ).scalar_one()
    if not 170000 <= version < 180000:
        pytest.fail("T13 integration requires PostgreSQL 17")


def _run_migration(action: str, revision: str, database_url: str) -> None:
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


def _assert_schema_absent(engine: Engine, schema_name: str) -> None:
    with engine.connect() as connection:
        assert not connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=:schema)"),
            {"schema": schema_name},
        ).scalar_one()


def _cleanup_database(engine: Engine) -> None:
    with engine.begin() as connection:
        _drop_schemas(connection)
        for role_name in ROLE_NAMES:
            exists: bool = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role_name)"),
                {"role_name": role_name},
            ).scalar_one()
            if exists:
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE {role_name}"))
