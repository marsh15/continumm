"""PG17 proof for sandbox reservations, fencing, and conservative quota ledgers."""

from __future__ import annotations

import asyncio
import hashlib
import os
import secrets
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.engine import URL, make_url

from continuum_api.runtime.leases import ExecutionLease, LeaseLost, QueueService
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.runtime.sandbox_allocations import (
    AllocationReservation,
    SandboxAllocationError,
    SandboxAllocationIdentityConflict,
    SandboxAllocationObservationConflict,
    SandboxAllocationStore,
)
from continuum_api.sandbox.allocation_service import SandboxAllocationService
from continuum_api.sandbox.contracts import (
    EffectState,
    SandboxAction,
    SandboxEffectError,
    SandboxObservation,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t16_test"
LATEST_REVISION = "20261012_10"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)


class _FakeAllocation:
    def __init__(self, name: str, observation: SandboxObservation) -> None:
        self.name = name
        self._observation = observation
        self.session_id = observation.session_id
        self.current_session_id = observation.session_id
        self.current_session_status = observation.session_status

    def observation(self) -> SandboxObservation:
        return self._observation


class _FakeProvider:
    def __init__(self) -> None:
        self.create_calls = 0
        self.observe_calls = 0
        self.name: str | None = None
        self.observation: SandboxObservation | None = None

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[_FakeProvider]:
        yield self

    async def create(self, name: str, *, tags: dict[str, str]) -> _FakeAllocation:
        self.create_calls += 1
        self.name = name
        assert tags
        raise SandboxEffectError(
            SandboxAction.CREATE, EffectState.UNKNOWN, "fixture_outcome_unknown"
        )

    async def observe(self, name: str) -> _FakeAllocation | None:
        self.observe_calls += 1
        assert name == self.name
        if self.observation is None:
            return None
        return _FakeAllocation(name, self.observation)

    async def observe_exact_session(
        self, name: str, expected_session_id: str
    ) -> _FakeAllocation | None:
        self.observe_calls += 1
        assert name == self.name
        assert self.observation is None or self.observation.session_id == expected_session_id
        if self.observation is None:
            return None
        return _FakeAllocation(name, self.observation)


def test_sandbox_reservations_hold_capacity_and_charge_across_periods() -> None:
    database_url = os.environ.get("CONTINUUM_T16_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T16_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T16 integration requires loopback PostgreSQL named continuum_t16_test")

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
        owners = _seed_users(admin_engine, 5)
        _seed_periods(admin_engine)
        store = SandboxAllocationStore(runtime_engine)
        leases = _seed_leases(runtime_engine, owners)

        first, first_lease = Principal(owners[0]), leases[0]
        second, second_lease = Principal(owners[1]), leases[1]
        fake_provider = _FakeProvider()
        service = SandboxAllocationService(
            store,
            image=f"registry.example/continuum@sha256:{'a' * 64}",
            provider_period_id="period-a",
            provider_factory=lambda limits: fake_provider,
        )
        first_reservation = asyncio.run(service.create(first, first_lease, "python:main", 300))
        assert not first_reservation.is_new
        assert first_reservation.status == "unknown"
        assert first_reservation.provider_name == f"continuum-{first_reservation.allocation_id.hex}"
        assert first_reservation.max_requested_seconds == 300
        assert fake_provider.create_calls == 1
        assert fake_provider.observe_calls == 1
        replay = asyncio.run(service.create(first, first_lease, "python:main", 300))
        assert not replay.is_new and replay.status == "unknown"
        assert fake_provider.create_calls == 1
        assert fake_provider.observe_calls == 2

        with pytest.raises(SandboxAllocationError) as busy:
            store.reserve(
                second, second_lease, "python:main", _digest("second"), "period-a", "public", 120
            )
        assert busy.value.code == "sandbox_allocation_capacity_busy"
        _assert_active_charge(admin_engine, first_reservation.allocation_id, "period-a", 600)

        # A provider 404 represented as no row is not proof that delayed create
        # can never arrive. Only positive STOPPED for the exact never-bound name
        # releases this persistent=False allocation.
        store.record_observation(first, first_reservation.allocation_id, None, "period-a")
        assert _active_allocation(admin_engine) == first_reservation.allocation_id
        stopped_without_session = SandboxObservation(
            first_reservation.provider_name, "stopped", None, None
        )
        fake_provider.observation = stopped_without_session
        stopped_reservation = asyncio.run(service.observe(first, first_reservation.allocation_id))
        assert stopped_reservation.status == "stopped"
        assert _active_allocation(admin_engine) is None
        assert _period_usage(admin_engine, "period-a", "public") == 600

        # A second account cannot read another owner's row through forced RLS.
        with pytest.raises(SandboxAllocationError) as hidden:
            store.get_for_observation(second, first_reservation.allocation_id)
        assert hidden.value.code == "sandbox_allocation_reservation_lost"
        with runtime_engine.begin() as connection:
            RuntimeRepository(connection, second)
            visible = connection.execute(
                text("SELECT count(*) FROM runtime_private.sandbox_allocations")
            ).scalar_one()
        assert visible == 0

        _assert_period_fence_before_create(admin_engine, store, Principal(owners[2]), leases[2])

        # An owner-scoped runtime-role create in a later run is charged against
        # its own class pool; public and operator pools cannot borrow.
        operator_reservation = store.reserve(
            second,
            second_lease,
            "python:operator",
            _digest("operator"),
            "period-a",
            "operator",
            300,
        )
        assert store.authorize_create(second, second_lease, operator_reservation.allocation_id)
        store.mark_create_not_applied(second, operator_reservation.allocation_id)
        assert _active_allocation(admin_engine) is None
        assert _period_usage(admin_engine, "period-a", "operator") == 600
        assert _period_usage(admin_engine, "period-a", "public") == 600

        _assert_run_and_period_quotas(
            admin_engine,
            store,
            second,
            second_lease,
            Principal(owners[2]),
            leases[2],
        )

        _assert_period_rollover_carries_unknown_allocation(
            admin_engine,
            store,
            first,
            first_lease,
            second,
            second_lease,
            Principal(owners[2]),
            leases[2],
        )
        _assert_exact_stop_and_concurrent_capacity(
            admin_engine, store, Principal(owners[3]), leases[3], Principal(owners[4]), leases[4]
        )
        _assert_final_lease_fence_under_allocation_lock(
            admin_engine, store, runtime_engine, Principal(owners[2]), leases[2]
        )

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


def _assert_period_rollover_carries_unknown_allocation(
    admin_engine: Engine,
    store: SandboxAllocationStore,
    first: Principal,
    first_lease: ExecutionLease,
    second: Principal,
    second_lease: ExecutionLease,
    next_principal: Principal,
    next_lease: ExecutionLease,
) -> None:
    image = f"registry.example/continuum@sha256:{'c' * 64}"
    period_a_provider = _FakeProvider()
    period_a_service = SandboxAllocationService(
        store,
        image=image,
        provider_period_id="period-a",
        provider_factory=lambda limits: period_a_provider,
    )
    active = asyncio.run(period_a_service.create(first, first_lease, "python:rollover", 240))
    assert active.status == "unknown"
    assert period_a_provider.create_calls == 1
    _switch_to_period_b(admin_engine)
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_period_usage SET charged_cpu_seconds=14000 "
                "WHERE provider_period_id='period-b' AND allocation_class='public'"
            )
        )

    period_b_provider = _FakeProvider()
    period_b_provider.name = active.provider_name
    period_b_service = SandboxAllocationService(
        store,
        image=image,
        provider_period_id="period-b",
        provider_factory=lambda limits: period_b_provider,
    )
    period_b_replay = asyncio.run(
        period_b_service.create(first, first_lease, "python:rollover", 240)
    )
    assert period_b_replay.allocation_id == active.allocation_id
    assert period_b_replay.provider_name == active.provider_name
    assert period_b_replay.status == "unknown"
    assert period_b_provider.create_calls == 0
    assert period_b_provider.observe_calls == 1
    _assert_active_charge(admin_engine, active.allocation_id, "period-a", 480)

    with pytest.raises(SandboxAllocationError) as blocked:
        store.reserve(
            second,
            second_lease,
            "python:busy-during-rollover",
            _digest("busy-rollover"),
            "period-b",
            "operator",
            60,
        )
    assert blocked.value.code == "sandbox_allocation_period_blocked"
    with pytest.raises(SandboxAllocationError) as terminal_blocked:
        store.record_observation(
            first,
            active.allocation_id,
            SandboxObservation(active.provider_name, "stopped", None, None),
            "period-b",
        )
    assert terminal_blocked.value.code == "sandbox_allocation_period_blocked"
    with admin_engine.connect() as connection:
        block_period, block_reason, stored_status = connection.execute(
            text(
                "SELECT capacity.period_blocked_period_id, capacity.period_blocked_reason, "
                "allocation.status FROM runtime_private.sandbox_global_capacity AS capacity "
                "JOIN runtime_private.sandbox_allocations AS allocation "
                "ON allocation.allocation_id=capacity.active_allocation_id "
                "WHERE capacity.capacity_id=1"
            )
        ).one()
        assert (
            connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.sandbox_allocation_period_charges "
                    "WHERE allocation_id=:allocation_id AND provider_period_id='period-b'"
                ),
                {"allocation_id": active.allocation_id},
            ).scalar_one()
            == 0
        )
    assert block_period == "period-b" and block_reason == "carry_exceeds_pool"
    assert stored_status == "unknown"
    assert _active_allocation(admin_engine) == active.allocation_id

    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_period_usage SET charged_cpu_seconds=0 "
                "WHERE provider_period_id='period-b' AND allocation_class='public'"
            )
        )
    carried_replay = asyncio.run(
        period_b_service.create(first, first_lease, "python:rollover", 240)
    )
    assert carried_replay.allocation_id == active.allocation_id
    assert period_b_provider.create_calls == 0
    assert period_b_provider.observe_calls == 2
    _assert_active_charge(admin_engine, active.allocation_id, "period-a", 480)
    assert _period_usage(admin_engine, "period-b", "public") == 480
    with admin_engine.connect() as connection:
        origin_period = connection.execute(
            text(
                "SELECT provider_period_id FROM runtime_private.sandbox_allocations "
                "WHERE allocation_id=:allocation_id"
            ),
            {"allocation_id": active.allocation_id},
        ).scalar_one()
    assert origin_period == "period-a"
    with pytest.raises(SandboxAllocationError) as busy:
        store.reserve(
            second,
            second_lease,
            "python:busy-during-rollover",
            _digest("busy-rollover"),
            "period-b",
            "operator",
            60,
        )
    assert busy.value.code == "sandbox_allocation_capacity_busy"
    _assert_active_charge(admin_engine, active.allocation_id, "period-b", 480)
    assert _period_usage(admin_engine, "period-b", "public") == 480

    # A stale owner-only observation remains admissible after its lease expires.
    _expire_lease(admin_engine, first_lease)
    terminal = SandboxObservation(active.provider_name, "stopped", None, None)
    store.record_observation(first, active.allocation_id, terminal, "period-b")
    assert _active_allocation(admin_engine) is None
    assert _period_usage(admin_engine, "period-b", "public") == 480

    # A second account's independent lease can now reserve, but stays within
    # the operator pool even though the public pool also has unused capacity.
    next_reservation = store.reserve(
        next_principal,
        next_lease,
        "python:operator-after-rollover",
        _digest("operator-after-rollover"),
        "period-b",
        "operator",
        60,
    )
    assert next_reservation.is_new
    store.release_undispatched(next_principal, next_lease, next_reservation.allocation_id)


def _assert_period_fence_before_create(
    admin_engine: Engine,
    store: SandboxAllocationStore,
    principal: Principal,
    lease: ExecutionLease,
) -> None:
    reservation = store.reserve(
        principal,
        lease,
        "python:period-boundary",
        _digest("period-boundary"),
        "period-a",
        "public",
        90,
    )
    _switch_to_period_b(admin_engine)
    with pytest.raises(SandboxAllocationError) as expired_period:
        store.authorize_create(principal, lease, reservation.allocation_id)
    assert expired_period.value.code == "sandbox_allocation_period_unavailable"
    with admin_engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT status, create_authorized_at FROM runtime_private.sandbox_allocations "
                "WHERE allocation_id=:allocation_id"
            ),
            {"allocation_id": reservation.allocation_id},
        ).one()
    assert row.status == "reserved" and row.create_authorized_at is None
    store.release_undispatched(principal, lease, reservation.allocation_id)
    _switch_to_period_a(admin_engine)


def _assert_run_and_period_quotas(
    admin_engine: Engine,
    store: SandboxAllocationStore,
    charged_principal: Principal,
    charged_lease: ExecutionLease,
    other_principal: Principal,
    other_lease: ExecutionLease,
) -> None:
    # Dispatched known-not-applied work remains part of the run's conservative
    # 900-second cap, so another 601-second reservation cannot be admitted.
    with pytest.raises(SandboxAllocationError) as run_budget:
        store.reserve(
            charged_principal,
            charged_lease,
            "python:run-budget-over",
            _digest("run-budget-over"),
            "period-a",
            "public",
            601,
        )
    assert run_budget.value.code == "sandbox_allocation_run_budget_exceeded"

    original_hash = _digest("rearm-exact-intent")
    first_reservation = store.reserve(
        charged_principal,
        charged_lease,
        "python:rearm",
        original_hash,
        "period-a",
        "public",
        60,
    )
    charged_before_release = _period_usage(admin_engine, "period-a", "public")
    store.release_undispatched(charged_principal, charged_lease, first_reservation.allocation_id)
    charged_after_release = _period_usage(admin_engine, "period-a", "public")
    assert charged_after_release == charged_before_release - 120
    replayed_reservation = store.reserve(
        charged_principal,
        charged_lease,
        "python:rearm",
        original_hash,
        "period-a",
        "public",
        60,
    )
    assert replayed_reservation.is_new
    assert replayed_reservation.allocation_id == first_reservation.allocation_id
    with pytest.raises(SandboxAllocationIdentityConflict):
        store.reserve(
            charged_principal,
            charged_lease,
            "python:rearm",
            _digest("changed-rearm-intent"),
            "period-a",
            "operator",
            60,
        )
    charged_before_final_release = _period_usage(admin_engine, "period-a", "public")
    store.release_undispatched(charged_principal, charged_lease, first_reservation.allocation_id)
    assert _period_usage(admin_engine, "period-a", "public") == (charged_before_final_release - 120)

    maximum = store.reserve(
        other_principal,
        other_lease,
        "python:max-session",
        _digest("max-session"),
        "period-a",
        "operator",
        900,
    )
    assert maximum.max_requested_seconds == 900
    store.release_undispatched(other_principal, other_lease, maximum.allocation_id)

    prior_public = _period_usage(admin_engine, "period-a", "public")
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_period_usage "
                "SET charged_cpu_seconds=14400 "
                "WHERE provider_period_id='period-a' AND allocation_class='public'"
            )
        )
    with pytest.raises(SandboxAllocationError) as public_pool:
        store.reserve(
            other_principal,
            other_lease,
            "python:public-pool-full",
            _digest("public-pool-full"),
            "period-a",
            "public",
            1,
        )
    assert public_pool.value.code == "sandbox_allocation_quota_exceeded"

    # The exhausted public pool cannot borrow from or block the independent
    # operator pool; its successful reservation is then genuinely undispatched.
    operator = store.reserve(
        other_principal,
        other_lease,
        "python:operator-pool-independent",
        _digest("operator-pool-independent"),
        "period-a",
        "operator",
        1,
    )
    store.release_undispatched(other_principal, other_lease, operator.allocation_id)
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_period_usage "
                "SET charged_cpu_seconds=:charged "
                "WHERE provider_period_id='period-a' AND allocation_class='public'"
            ),
            {"charged": prior_public},
        )


def _assert_final_lease_fence_under_allocation_lock(
    admin_engine: Engine,
    store: SandboxAllocationStore,
    runtime_engine: Engine,
    principal: Principal,
    lease: ExecutionLease,
) -> None:
    usage_before = _period_usage(admin_engine, "period-b", "public")
    reservation = store.reserve(
        principal,
        lease,
        "python:lease-expiry-fence",
        _digest("lease-expiry-fence"),
        "period-b",
        "public",
        30,
    )
    started = threading.Event()

    async def authorize() -> BaseException | None:
        started.set()
        try:
            await asyncio.to_thread(
                store.authorize_create, principal, lease, reservation.allocation_id
            )
        except BaseException as error:
            return error
        return None

    def run_authorize() -> BaseException | None:
        return asyncio.run(authorize())

    with runtime_engine.connect() as connection:
        transaction = connection.begin()
        RuntimeRepository(connection, principal)
        connection.execute(
            text(
                "SELECT allocation_id FROM runtime_private.sandbox_allocations "
                "WHERE owner_id=:owner AND allocation_id=:allocation FOR UPDATE"
            ),
            {"owner": principal.account_id, "allocation": reservation.allocation_id},
        ).one()
        with admin_engine.begin() as admin:
            admin.execute(
                text(
                    "UPDATE runtime_private.jobs SET lease_expires_at=clock_timestamp() "
                    "+ interval '2 seconds' WHERE owner_id=:owner AND run_id=:run "
                    "AND job_id=:job"
                ),
                {"owner": lease.owner_id, "run": lease.run_id, "job": lease.job_id},
            )
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(run_authorize)
            assert started.wait(timeout=2)
            time.sleep(3)
            transaction.commit()
            result = future.result(timeout=5)
    assert isinstance(result, LeaseLost)
    with admin_engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT status, create_authorized_at FROM runtime_private.sandbox_allocations "
                "WHERE allocation_id=:allocation_id"
            ),
            {"allocation_id": reservation.allocation_id},
        ).one()
    assert row.status == "reserved" and row.create_authorized_at is None
    assert _active_allocation(admin_engine) == reservation.allocation_id
    fake_provider = _FakeProvider()
    provider_constructions = 0

    def provider_factory(limits: object) -> _FakeProvider:
        nonlocal provider_constructions
        del limits
        provider_constructions += 1
        return fake_provider

    service = SandboxAllocationService(
        store,
        image=f"registry.example/continuum@sha256:{'b' * 64}",
        provider_period_id="period-b",
        provider_factory=provider_factory,
    )
    recovered = asyncio.run(service.stop(principal, reservation.allocation_id))
    assert recovered.status == "released"
    assert provider_constructions == 0
    assert fake_provider.observe_calls == 0
    assert _active_allocation(admin_engine) is None
    assert _period_usage(admin_engine, "period-b", "public") == usage_before


def _assert_exact_stop_and_concurrent_capacity(
    admin_engine: Engine,
    store: SandboxAllocationStore,
    first_racer: Principal,
    first_lease: ExecutionLease,
    second_racer: Principal,
    second_lease: ExecutionLease,
) -> None:
    # Use two fresh identities to race the global singleton.
    racers = (first_racer, second_racer)
    leases = (first_lease, second_lease)

    def reserve(index: int) -> tuple[AllocationReservation | None, str | None]:
        try:
            result = store.reserve(
                racers[index],
                leases[index],
                "python:race",
                _digest(f"race-{index}"),
                "period-b",
                "public",
                100,
            )
            return result, None
        except SandboxAllocationError as error:
            return None, error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(reserve, (0, 1)))
    assert sum(result is not None for result, _ in results) == 1
    assert sum(code == "sandbox_allocation_capacity_busy" for _, code in results) == 1
    winner_index = next(index for index, (result, _) in enumerate(results) if result is not None)
    winner = results[winner_index][0]
    assert winner is not None

    # A session-bound allocation can only be released by a matching session
    # STOPPED observation. A different session ID is a hard conflict.
    assert store.authorize_create(racers[winner_index], leases[winner_index], winner.allocation_id)
    authorized_snapshot = store.get_for_observation(racers[winner_index], winner.allocation_id)
    assert not store.reconcile_undispatched(
        racers[winner_index], authorized_snapshot, winner.allocation_id
    )
    assert _active_allocation(admin_engine) == winner.allocation_id
    observed = SandboxObservation(winner.provider_name, "running", "session-17", "running")
    store.record_observation(racers[winner_index], winner.allocation_id, observed, "period-b")
    action_budget = store.authorize_profile_action(
        racers[winner_index],
        leases[winner_index],
        winner.allocation_id,
        "session-17",
        "period-b",
    )
    assert 0 < action_budget <= 100
    with pytest.raises(SandboxAllocationObservationConflict):
        store.authorize_profile_action(
            racers[winner_index],
            leases[winner_index],
            winner.allocation_id,
            "other-session",
            "period-b",
        )
    assert store.authorize_stop(racers[winner_index], winner.allocation_id, "session-17")
    assert not store.authorize_stop(racers[winner_index], winner.allocation_id, "session-17")
    store.mark_stop_unknown(racers[winner_index], winner.allocation_id)
    with pytest.raises(SandboxAllocationObservationConflict):
        store.record_observation(
            racers[winner_index],
            winner.allocation_id,
            SandboxObservation(winner.provider_name, "stopped", "wrong-session", "stopped"),
            "period-b",
        )
    store.record_observation(
        racers[winner_index],
        winner.allocation_id,
        SandboxObservation(winner.provider_name, "running", "session-17", "stopped"),
        "period-b",
    )
    assert _active_allocation(admin_engine) == winner.allocation_id
    loser_index = 1 - winner_index
    with pytest.raises(SandboxAllocationError) as still_busy:
        store.reserve(
            racers[loser_index],
            leases[loser_index],
            "python:race",
            _digest(f"race-{loser_index}"),
            "period-b",
            "public",
            100,
        )
    assert still_busy.value.code == "sandbox_allocation_capacity_busy"
    store.record_observation(
        racers[winner_index],
        winner.allocation_id,
        SandboxObservation(winner.provider_name, "stopped", "session-17", "stopped"),
        "period-b",
    )
    assert _active_allocation(admin_engine) is None


def _seed_users(engine: Engine, count: int) -> tuple[UUID, ...]:
    owners = tuple(UUID(int=2_000_000 + index) for index in range(count))
    with engine.begin() as connection:
        for index, owner_id in enumerate(owners):
            connection.execute(
                text(
                    "INSERT INTO runtime_private.users (user_id, github_user_id) "
                    "VALUES (:id, :github_id)"
                ),
                {"id": owner_id, "github_id": 820_001 + index},
            )
    return owners


def _seed_leases(runtime_engine: Engine, owners: tuple[UUID, ...]) -> tuple[ExecutionLease, ...]:
    return tuple(
        _seed_new_lease(runtime_engine, Principal(owner), f"sandbox-run-{index}")
        for index, owner in enumerate(owners)
    )


def _seed_new_lease(runtime_engine: Engine, principal: Principal, key: str) -> ExecutionLease:
    with runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run()
        step_id = repository.create_step(run_id, key, "sandbox")
        repository.create_job(run_id, step_id)
    lease = QueueService(runtime_engine, principal).claim_one(f"worker-{key}")
    assert lease is not None
    return lease


def _seed_periods(engine: Engine) -> None:
    now = _database_now(engine)
    with engine.begin() as connection:
        _insert_period(connection, "period-a", now - timedelta(days=1), now + timedelta(hours=1))
        _insert_period(connection, "period-b", now + timedelta(hours=1), now + timedelta(days=1))


def _insert_period(
    connection: Connection, period_id: str, starts_at: datetime, ends_at: datetime
) -> None:
    connection.execute(
        text(
            "INSERT INTO runtime_private.sandbox_provider_periods "
            "(provider_period_id, starts_at, ends_at, public_cpu_seconds_limit, "
            "operator_cpu_seconds_limit) VALUES (:period_id, :starts_at, :ends_at, 14400, 3600)"
        ),
        {"period_id": period_id, "starts_at": starts_at, "ends_at": ends_at},
    )
    connection.execute(
        text(
            "INSERT INTO runtime_private.sandbox_period_usage "
            "(provider_period_id, allocation_class) VALUES (:period_id, 'public'), "
            "(:period_id, 'operator')"
        ),
        {"period_id": period_id},
    )


def _switch_to_period_b(engine: Engine) -> None:
    now = _database_now(engine)
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_provider_periods "
                "SET ends_at=:ended WHERE provider_period_id='period-a'"
            ),
            {"ended": now - timedelta(seconds=1)},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_provider_periods "
                "SET starts_at=:started, ends_at=:ends "
                "WHERE provider_period_id='period-b'"
            ),
            {"started": now - timedelta(seconds=1), "ends": now + timedelta(days=1)},
        )


def _switch_to_period_a(engine: Engine) -> None:
    now = _database_now(engine)
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_provider_periods "
                "SET starts_at=:started, ends_at=:ended "
                "WHERE provider_period_id='period-b'"
            ),
            {
                "started": now - timedelta(days=1),
                "ended": now - timedelta(seconds=1),
            },
        )
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_provider_periods "
                "SET starts_at=:started, ends_at=:ends "
                "WHERE provider_period_id='period-a'"
            ),
            {"started": now - timedelta(days=1), "ends": now + timedelta(hours=1)},
        )


def _expire_lease(engine: Engine, lease: ExecutionLease) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.jobs "
                "SET lease_expires_at=clock_timestamp() - interval '1 second' "
                "WHERE owner_id=:owner AND run_id=:run AND job_id=:job"
            ),
            {"owner": lease.owner_id, "run": lease.run_id, "job": lease.job_id},
        )


def _assert_active_charge(
    engine: Engine, allocation_id: UUID, period_id: str, cpu_seconds: int
) -> None:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT cpu_seconds FROM runtime_private.sandbox_allocation_period_charges "
                "WHERE allocation_id=:allocation_id AND provider_period_id=:period_id"
            ),
            {"allocation_id": allocation_id, "period_id": period_id},
        ).scalar_one()
    assert row == cpu_seconds


def _active_allocation(engine: Engine) -> UUID | None:
    with engine.connect() as connection:
        return connection.execute(
            text(
                "SELECT active_allocation_id FROM runtime_private.sandbox_global_capacity "
                "WHERE capacity_id=1"
            )
        ).scalar_one()


def _period_usage(engine: Engine, period_id: str, allocation_class: str) -> int:
    with engine.connect() as connection:
        return connection.execute(
            text(
                "SELECT charged_cpu_seconds FROM runtime_private.sandbox_period_usage "
                "WHERE provider_period_id=:period_id AND allocation_class=:allocation_class"
            ),
            {"period_id": period_id, "allocation_class": allocation_class},
        ).scalar_one()


def _database_now(engine: Engine) -> datetime:
    with engine.connect() as connection:
        return connection.execute(text("SELECT clock_timestamp()")).scalar_one()


def _digest(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


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
        definitions = (
            ("continuum_ingress", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_issuer", "NOLOGIN", None),
            ("continuum_wake", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_receiver", "LOGIN", secrets.token_urlsafe(32)),
            (RUNTIME_ROLE, "LOGIN", runtime_password),
        )
        for role_name, login_mode, password in definitions:
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
        pytest.fail("T16 integration requires PostgreSQL 17")


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
        pytest.fail(f"T16 migration {action} {revision} failed: {result.stdout}\n{result.stderr}")


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
