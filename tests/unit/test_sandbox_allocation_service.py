"""T16 allocation lifecycle tests use fake persistence and provider objects."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from continuum_api.runtime.leases import ExecutionLease, LeaseLost
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.sandbox_allocations import (
    AllocationReservation,
    SandboxAllocationError,
)
from continuum_api.sandbox.allocation_service import (
    SandboxAllocationService,
    SandboxAllocationServiceError,
)
from continuum_api.sandbox.config import SandboxAllocationLimits
from continuum_api.sandbox.contracts import (
    EffectState,
    SandboxAction,
    SandboxEffectError,
    SandboxObservation,
)


class _FakeStore:
    def __init__(
        self,
        *,
        reservation: AllocationReservation | None = None,
        reserve_error: Exception | None = None,
        create_allowed: bool = True,
        release_error: Exception | None = None,
        authorize_started: threading.Event | None = None,
        authorize_continue: threading.Event | None = None,
        authorize_error: Exception | None = None,
    ) -> None:
        self.reservation = reservation or _reservation()
        self.reserve_error = reserve_error
        self.create_allowed = create_allowed
        self.release_error = release_error
        self.authorize_started = authorize_started
        self.authorize_continue = authorize_continue
        self.authorize_error = authorize_error
        self.calls: list[tuple[Any, ...]] = []

    def reserve(self, *args: Any) -> AllocationReservation:
        self.calls.append(("reserve", *args))
        if self.reserve_error is not None:
            raise self.reserve_error
        return self.reservation

    def authorize_create(self, *args: Any) -> bool:
        self.calls.append(("authorize_create", *args))
        if self.authorize_started is not None:
            self.authorize_started.set()
        if self.authorize_continue is not None:
            self.authorize_continue.wait(timeout=3)
        if self.authorize_error is not None:
            raise self.authorize_error
        if self.create_allowed:
            self.reservation = replace(self.reservation, status="creating")
        return self.create_allowed

    def release_undispatched(self, *args: Any) -> None:
        self.calls.append(("release_undispatched", *args))
        if self.release_error is not None:
            raise self.release_error
        self.reservation = replace(self.reservation, status="released", is_new=False)

    def reconcile_undispatched(self, *args: Any) -> bool:
        self.calls.append(("reconcile_undispatched", *args))
        if self.reservation.status == "reserved":
            self.reservation = replace(self.reservation, status="released", is_new=False)
            return True
        return False

    def mark_create_not_applied(self, *args: Any) -> None:
        self.calls.append(("mark_create_not_applied", *args))
        self.reservation = replace(self.reservation, status="not_applied", is_new=False)

    def mark_create_unknown(self, *args: Any) -> None:
        self.calls.append(("mark_create_unknown", *args))
        self.reservation = replace(self.reservation, status="unknown", is_new=False)

    def get_for_observation(self, *_args: Any) -> AllocationReservation:
        self.calls.append(("get_for_observation", *_args))
        return self.reservation

    def record_observation(
        self,
        _principal: Principal,
        _allocation_id: UUID,
        observation: SandboxObservation | None,
        _period_id: str | None,
    ) -> AllocationReservation:
        self.calls.append(("record_observation", observation, _period_id))
        if observation is None:
            if self.reservation.status == "destroy_unknown":
                self.reservation = replace(self.reservation, status="destroyed")
            return self.reservation
        if observation.name != self.reservation.provider_name:
            raise SandboxAllocationError("sandbox_allocation_observation_conflict")
        if (
            self.reservation.provider_session_id is not None
            and observation.session_id != self.reservation.provider_session_id
        ):
            raise SandboxAllocationError("sandbox_allocation_observation_conflict")
        status = self.reservation.status
        if (observation.session_status or "").casefold() == "stopped":
            status = "destroy_unknown" if status == "destroy_unknown" else "stopped"
        elif (
            observation.session_status or ""
        ).casefold() == "running" and status != "stop_unknown":
            status = "running"
        self.reservation = replace(
            self.reservation,
            status=status,
            provider_session_id=(self.reservation.provider_session_id or observation.session_id),
        )
        return self.reservation

    def mark_stop_unknown(self, *args: Any) -> None:
        self.calls.append(("mark_stop_unknown", *args))
        self.reservation = replace(self.reservation, status="stop_unknown")

    def mark_observation_unknown(self, *args: Any) -> None:
        self.calls.append(("mark_observation_unknown", *args))
        self.reservation = replace(self.reservation, status="unknown")

    def authorize_stop(self, *args: Any) -> bool:
        self.calls.append(("authorize_stop", *args))
        return self.reservation.status == "running"

    def authorize_destroy(self, *args: Any) -> bool:
        self.calls.append(("authorize_destroy", *args))
        if self.reservation.status != "stopped":
            return False
        self.reservation = replace(self.reservation, status="destroy_unknown")
        return True


class _FakeAllocation:
    def __init__(
        self,
        name: str,
        session_id: str | None,
        *,
        sandbox_status: str | None = "RUNNING",
        session_status: str | None = "RUNNING",
        current_session_id: str | None = None,
        current_session_status: str | None = None,
    ) -> None:
        self.name = name
        self.session_id = session_id
        self.sandbox_status = sandbox_status
        self.session_status = session_status
        self.current_session_id = current_session_id or session_id
        self.current_session_status = current_session_status or session_status
        self.stop_calls = 0
        self.destroy_calls = 0
        self.stop_error: Exception | None = None
        self.destroy_error: Exception | None = None
        self.on_destroy: Callable[[], None] | None = None

    def observation(self) -> SandboxObservation:
        return SandboxObservation(
            name=self.name,
            sandbox_status=self.sandbox_status,
            session_id=self.session_id,
            session_status=self.session_status,
        )

    async def stop(self) -> None:
        self.stop_calls += 1
        if self.stop_error is not None:
            raise self.stop_error
        self.session_status = "STOPPED"
        self.current_session_status = "STOPPED"
        self.sandbox_status = "STOPPED"

    async def destroy(self) -> None:
        self.destroy_calls += 1
        if self.destroy_error is not None:
            raise self.destroy_error
        if self.on_destroy is not None:
            self.on_destroy()


class _FakeProvider:
    def __init__(
        self,
        *,
        create_error: Exception | None = None,
        observed: _FakeAllocation | None = None,
        created: _FakeAllocation | None = None,
        connection_entered: asyncio.Event | None = None,
        connection_gate: asyncio.Event | None = None,
        connection_error: Exception | None = None,
    ) -> None:
        self.create_error = create_error
        self.observed = observed
        self.created = created
        self.connection_entered = connection_entered
        self.connection_gate = connection_gate
        self.connection_error = connection_error
        self.create_calls: list[tuple[str, dict[str, str] | None]] = []
        self.observe_calls: list[str] = []
        self.observe_exact_calls: list[tuple[str, str]] = []

    @asynccontextmanager
    async def connection(self):
        if self.connection_entered is not None:
            self.connection_entered.set()
        if self.connection_gate is not None:
            await self.connection_gate.wait()
        if self.connection_error is not None:
            raise self.connection_error
        yield self

    async def create(self, name: str, *, tags: dict[str, str] | None = None) -> _FakeAllocation:
        self.create_calls.append((name, tags))
        if self.create_error is not None:
            raise self.create_error
        if self.created is None:
            self.created = _FakeAllocation(name, "provider-session-fixture")
        return self.created

    async def observe(self, name: str) -> _FakeAllocation | None:
        self.observe_calls.append(name)
        return self.observed

    async def observe_exact_session(
        self, name: str, expected_session_id: str
    ) -> _FakeAllocation | None:
        self.observe_exact_calls.append((name, expected_session_id))
        if self.observed is not None and self.observed.session_id == expected_session_id:
            return self.observed
        return None


def _principal_lease() -> tuple[Principal, ExecutionLease]:
    owner_id = uuid4()
    return Principal(owner_id), ExecutionLease(
        owner_id=owner_id,
        run_id=uuid4(),
        step_id=uuid4(),
        job_id=uuid4(),
        attempt_id=uuid4(),
        generation=1,
        lease_owner="sandbox-allocation-worker",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )


def _reservation(
    *,
    status: str = "reserved",
    is_new: bool = True,
    provider_session_id: str | None = None,
) -> AllocationReservation:
    return AllocationReservation(
        allocation_id=uuid4(),
        provider_name=f"continuum-{uuid4().hex}",
        status=status,
        is_new=is_new,
        max_requested_seconds=120,
        vcpus=2,
        memory_mb=4096,
        provider_period_id="operator-period-fixture",
        allocation_class="public",
        provider_session_id=provider_session_id,
        owner_id=uuid4(),
        run_id=uuid4(),
        step_id=uuid4(),
        job_id=uuid4(),
        generation=1,
        attempt_id=uuid4(),
        lease_owner="sandbox-allocation-worker",
    )


def _service(
    store: _FakeStore,
    provider: _FakeProvider,
    *,
    period_id: str | None = "operator-period-fixture",
    deadline_seconds: float = 90.0,
    factory_calls: list[SandboxAllocationLimits] | None = None,
) -> SandboxAllocationService:
    def factory(limits: SandboxAllocationLimits) -> _FakeProvider:
        if factory_calls is not None:
            factory_calls.append(limits)
        return provider

    service = SandboxAllocationService(
        cast(Any, store),
        image=f"vercel/sandbox/python:3.14@sha256:{'a' * 64}",
        provider_period_id=period_id,
        provider_factory=cast(Callable[[SandboxAllocationLimits], Any], factory),
        action_deadline_seconds=deadline_seconds,
    )
    return service


def test_allocation_reserves_before_one_bounded_create_and_records_session() -> None:
    async def scenario() -> None:
        store = _FakeStore()
        reservation = store.reservation
        provider = _FakeProvider()
        service = _service(store, provider)
        principal, lease = _principal_lease()

        result = await service.create(
            principal,
            lease,
            "runtime:initial",
            900,
        )

        assert result.allocation_id == reservation.allocation_id
        assert result.status == "running"
        assert result.max_requested_seconds == 120
        assert result.provider_session_id == "provider-session-fixture"
        assert [call[0] for call in store.calls] == [
            "reserve",
            "authorize_create",
            "record_observation",
        ]
        assert len(provider.create_calls) == 1
        assert provider.create_calls[0][0] == reservation.provider_name
        assert provider.create_calls[0][1] == {
            "continuum-allocation-id": str(reservation.allocation_id),
            "continuum-attempt-id": str(reservation.attempt_id),
            "continuum-generation": "1",
            "continuum-provider-period-id": reservation.provider_period_id,
        }
        limits = service._limits_for(result)
        assert (limits.session_seconds, limits.vcpus, limits.memory_mb) == (120, 2, 4096)

    asyncio.run(scenario())


def test_unknown_create_and_404_remain_charged_and_replay_observes_only() -> None:
    async def scenario() -> None:
        store = _FakeStore()
        provider = _FakeProvider(
            create_error=SandboxEffectError(
                SandboxAction.CREATE,
                EffectState.UNKNOWN,
                "provider_response_uncertain",
            )
        )
        service = _service(store, provider)
        principal, lease = _principal_lease()

        first = await service.create(principal, lease, "runtime:initial", 120)
        second = await service.create(principal, lease, "runtime:initial", 120)

        assert first.status == "unknown"
        assert second.status == "unknown"
        assert len(provider.create_calls) == 1
        assert provider.observe_calls == [store.reservation.provider_name] * 2
        assert [call[0] for call in store.calls].count("mark_create_unknown") == 1
        assert [call[0] for call in store.calls].count("record_observation") == 2
        assert not any(call[0] == "release_undispatched" for call in store.calls)

    asyncio.run(scenario())


def test_unknown_allocation_replays_across_provider_period_without_a_second_create() -> None:
    async def scenario() -> None:
        store = _FakeStore()
        provider = _FakeProvider(
            create_error=SandboxEffectError(
                SandboxAction.CREATE,
                EffectState.UNKNOWN,
                "provider_response_uncertain",
            )
        )
        principal, lease = _principal_lease()
        period_zero = _service(store, provider, period_id="provider-period-zero")
        period_one = _service(store, provider, period_id="provider-period-one")

        first = await period_zero.create(principal, lease, "runtime:initial", 120)
        replay = await period_one.create(principal, lease, "runtime:initial", 120)

        reserve_calls = [call for call in store.calls if call[0] == "reserve"]
        assert first.allocation_id == replay.allocation_id
        assert first.status == replay.status == "unknown"
        assert reserve_calls[0][4] == reserve_calls[1][4]
        assert reserve_calls[0][5] == "provider-period-zero"
        assert reserve_calls[1][5] == "provider-period-one"
        assert provider.create_calls == [
            (
                first.provider_name,
                {
                    "continuum-allocation-id": str(first.allocation_id),
                    "continuum-attempt-id": str(first.attempt_id),
                    "continuum-generation": "1",
                    "continuum-provider-period-id": first.provider_period_id,
                },
            )
        ]
        assert store.calls[-1] == ("record_observation", None, "provider-period-one")

    asyncio.run(scenario())


def test_period_is_required_before_reserve_or_provider_construction() -> None:
    async def scenario() -> None:
        store = _FakeStore()
        provider = _FakeProvider()
        service = _service(store, provider, period_id=None)
        principal, lease = _principal_lease()

        with pytest.raises(
            SandboxAllocationServiceError,
            match="sandbox_allocation_period_unconfigured",
        ):
            await service.create(principal, lease, "runtime:initial", 120)

        assert store.calls == []
        assert provider.create_calls == []

    asyncio.run(scenario())


def test_invalid_local_duration_never_reserves_or_calls_provider() -> None:
    async def scenario() -> None:
        store = _FakeStore()
        provider = _FakeProvider()
        service = _service(store, provider)
        principal, lease = _principal_lease()

        with pytest.raises(SandboxAllocationServiceError, match="duration_invalid"):
            await service.create(principal, lease, "runtime:initial", 901)

        assert store.calls == []
        assert provider.create_calls == []

    asyncio.run(scenario())


def test_existing_allocation_never_authorizes_another_create() -> None:
    async def scenario() -> None:
        reservation = _reservation(status="unknown", is_new=False)
        store = _FakeStore(reservation=reservation)
        provider = _FakeProvider(observed=None)
        service = _service(store, provider)
        principal, lease = _principal_lease()

        result = await service.create(principal, lease, "runtime:initial", 120)

        assert result.status == "unknown"
        assert provider.create_calls == []
        assert provider.observe_calls == [reservation.provider_name]
        assert [call[0] for call in store.calls] == ["reserve", "record_observation"]

    asyncio.run(scenario())


def test_definitive_create_rejection_frees_only_global_slot_no_repost() -> None:
    async def scenario() -> None:
        store = _FakeStore()
        provider = _FakeProvider(
            create_error=SandboxEffectError(
                SandboxAction.CREATE,
                EffectState.NOT_APPLIED,
                "provider_rejected",
            )
        )
        service = _service(store, provider)
        principal, lease = _principal_lease()

        with pytest.raises(
            SandboxAllocationServiceError,
            match="sandbox_create_not_applied",
        ):
            await service.create(principal, lease, "runtime:initial", 120)

        assert len(provider.create_calls) == 1
        assert [call[0] for call in store.calls] == [
            "reserve",
            "authorize_create",
            "mark_create_not_applied",
        ]

    asyncio.run(scenario())


def test_running_allocation_dispatches_one_exact_stop_and_replay_only_observes() -> None:
    async def scenario() -> None:
        reservation = _reservation(status="running", is_new=False, provider_session_id="session-1")
        store = _FakeStore(reservation=reservation)
        allocation = _FakeAllocation(
            reservation.provider_name,
            "session-1",
            sandbox_status="RUNNING",
            session_status="RUNNING",
        )
        provider = _FakeProvider(observed=allocation)
        service = _service(store, provider)
        principal, _lease = _principal_lease()

        stopped = await service.stop(principal, reservation.allocation_id)
        replay = await service.stop(principal, reservation.allocation_id)

        assert stopped.status == "stopped"
        assert replay.status == "stopped"
        assert allocation.stop_calls == 1
        assert [call[0] for call in store.calls].count("authorize_stop") == 1
        assert provider.observe_exact_calls == [(reservation.provider_name, "session-1")] * 3

    asyncio.run(scenario())


def test_unknown_stop_keeps_slot_and_does_not_repeat_provider_stop() -> None:
    async def scenario() -> None:
        reservation = _reservation(status="running", is_new=False, provider_session_id="session-1")
        store = _FakeStore(reservation=reservation)
        allocation = _FakeAllocation(
            reservation.provider_name,
            "session-1",
            sandbox_status="RUNNING",
            session_status="RUNNING",
        )
        allocation.stop_error = SandboxEffectError(
            SandboxAction.STOP, EffectState.UNKNOWN, "provider_response_uncertain"
        )
        provider = _FakeProvider(observed=allocation)
        service = _service(store, provider)
        principal, _lease = _principal_lease()

        first = await service.stop(principal, reservation.allocation_id)
        second = await service.stop(principal, reservation.allocation_id)

        assert first.status == "stop_unknown"
        assert second.status == "stop_unknown"
        assert allocation.stop_calls == 1
        assert [call[0] for call in store.calls].count("authorize_stop") == 1
        assert not any(call[0] == "record_observation" and call[1] is None for call in store.calls)

    asyncio.run(scenario())


def test_stopped_old_session_with_new_current_session_never_frees_slot_or_destroys_box() -> None:
    async def scenario() -> None:
        reservation = _reservation(
            status="stopped", is_new=False, provider_session_id="old-session"
        )
        store = _FakeStore(reservation=reservation)
        allocation = _FakeAllocation(
            reservation.provider_name,
            "old-session",
            sandbox_status="RUNNING",
            session_status="STOPPED",
            current_session_id="new-session",
            current_session_status="RUNNING",
        )
        provider = _FakeProvider(observed=allocation)
        service = _service(store, provider)
        principal, _lease = _principal_lease()

        result = await service.destroy(principal, reservation.allocation_id)

        assert result.status == "unknown"
        assert allocation.destroy_calls == 0
        assert [call[0] for call in store.calls].count("authorize_destroy") == 0

    asyncio.run(scenario())


def test_contradictory_box_and_session_status_never_dispatches_stop() -> None:
    async def scenario() -> None:
        reservation = _reservation(status="running", is_new=False, provider_session_id="session-1")
        store = _FakeStore(reservation=reservation)
        allocation = _FakeAllocation(
            reservation.provider_name,
            "session-1",
            sandbox_status="STOPPED",
            session_status="RUNNING",
            current_session_id="session-1",
            current_session_status="RUNNING",
        )
        provider = _FakeProvider(observed=allocation)
        service = _service(store, provider)
        principal, _lease = _principal_lease()

        result = await service.stop(principal, reservation.allocation_id)

        assert result.status == "unknown"
        assert allocation.stop_calls == 0
        assert [call[0] for call in store.calls].count("authorize_stop") == 0

    asyncio.run(scenario())


def test_destroy_requires_terminal_proof_and_replay_does_not_destroy_twice() -> None:
    async def scenario() -> None:
        reservation = _reservation(status="stopped", is_new=False, provider_session_id="session-1")
        store = _FakeStore(reservation=reservation)
        allocation = _FakeAllocation(
            reservation.provider_name,
            "session-1",
            sandbox_status="STOPPED",
            session_status="STOPPED",
        )
        provider = _FakeProvider(observed=allocation)
        allocation.on_destroy = lambda: setattr(provider, "observed", None)
        service = _service(store, provider)
        principal, _lease = _principal_lease()

        destroyed = await service.destroy(principal, reservation.allocation_id)
        replay = await service.destroy(principal, reservation.allocation_id)

        assert destroyed.status == "destroyed"
        assert replay.status == "destroyed"
        assert allocation.destroy_calls == 1
        assert [call[0] for call in store.calls].count("authorize_destroy") == 1

    asyncio.run(scenario())


def test_cancel_during_provider_connection_reconciles_unclaimed_reservation() -> None:
    async def scenario() -> None:
        store = _FakeStore()
        connection_entered = asyncio.Event()
        connection_gate = asyncio.Event()
        provider = _FakeProvider(
            connection_entered=connection_entered,
            connection_gate=connection_gate,
        )
        service = _service(store, provider)
        principal, lease = _principal_lease()

        task = asyncio.create_task(service.create(principal, lease, "runtime:initial", 120))
        await asyncio.wait_for(connection_entered.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert store.reservation.status == "released"
        assert [call[0] for call in store.calls].count("reconcile_undispatched") == 1
        assert not any(call[0] == "authorize_create" for call in store.calls)
        assert provider.create_calls == []

    asyncio.run(scenario())


def test_cancel_while_create_claim_finishes_never_posts_and_keeps_charge() -> None:
    async def scenario() -> None:
        authorize_started = threading.Event()
        authorize_continue = threading.Event()
        store = _FakeStore(
            authorize_started=authorize_started,
            authorize_continue=authorize_continue,
        )
        provider = _FakeProvider()
        service = _service(store, provider)
        principal, lease = _principal_lease()

        task = asyncio.create_task(service.create(principal, lease, "runtime:initial", 120))
        assert await asyncio.to_thread(authorize_started.wait, 1)
        task.cancel()
        authorize_continue.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert store.reservation.status == "unknown"
        assert [call[0] for call in store.calls].count("reconcile_undispatched") == 1
        assert [call[0] for call in store.calls].count("mark_create_unknown") == 1
        assert provider.create_calls == []

    asyncio.run(scenario())


def test_stale_pre_auth_connection_failure_uses_protective_reconciliation() -> None:
    async def scenario() -> None:
        store = _FakeStore(release_error=LeaseLost("fixture_lease_expired"))
        provider = _FakeProvider(connection_error=RuntimeError("untrusted provider text"))
        service = _service(store, provider)
        principal, lease = _principal_lease()

        with pytest.raises(
            SandboxAllocationServiceError,
            match="sandbox_allocation_provider_configuration_invalid",
        ):
            await service.create(principal, lease, "runtime:initial", 120)

        assert store.reservation.status == "released"
        assert [call[0] for call in store.calls].count("release_undispatched") == 1
        assert [call[0] for call in store.calls].count("reconcile_undispatched") == 1
        assert provider.create_calls == []

    asyncio.run(scenario())


def test_lost_create_authorization_releases_before_any_provider_get_or_post() -> None:
    async def scenario() -> None:
        store = _FakeStore(create_allowed=False)
        provider = _FakeProvider()
        service = _service(store, provider)
        principal, lease = _principal_lease()

        result = await service.create(principal, lease, "runtime:initial", 120)

        assert result.status == "released"
        assert provider.create_calls == []
        assert provider.observe_calls == []
        assert [call[0] for call in store.calls].count("reconcile_undispatched") == 1

    asyncio.run(scenario())


def test_lease_lost_from_create_authorization_is_not_misreported_as_provider_error() -> None:
    async def scenario() -> None:
        store = _FakeStore(authorize_error=LeaseLost("fixture_lease_expired"))
        provider = _FakeProvider()
        service = _service(store, provider)
        principal, lease = _principal_lease()

        with pytest.raises(SandboxAllocationServiceError, match="sandbox_allocation_lease_lost"):
            await service.create(principal, lease, "runtime:initial", 120)

        assert store.reservation.status == "released"
        assert provider.create_calls == []
        assert [call[0] for call in store.calls].count("reconcile_undispatched") == 1

    asyncio.run(scenario())


def test_explicit_stop_reconciles_reserved_unclaimed_row_without_provider_creation() -> None:
    async def scenario() -> None:
        reservation = _reservation(status="reserved", is_new=False)
        store = _FakeStore(reservation=reservation)
        provider = _FakeProvider()
        factory_calls: list[SandboxAllocationLimits] = []
        service = _service(store, provider, factory_calls=factory_calls)
        principal, _lease = _principal_lease()

        result = await service.stop(principal, reservation.allocation_id)

        assert result.status == "released"
        assert store.reservation.status == "released"
        assert factory_calls == []
        assert provider.create_calls == []
        assert provider.observe_calls == []
        assert [call[0] for call in store.calls].count("reconcile_undispatched") == 1

    asyncio.run(scenario())


def test_require_new_refuses_prior_identity_before_any_provider_observation() -> None:
    async def scenario() -> None:
        store = _FakeStore(reservation=_reservation(status="running", is_new=False))
        provider = _FakeProvider()
        factory_calls = []
        service = _service(store, provider, factory_calls=factory_calls)
        principal, lease = _principal_lease()
        with pytest.raises(
            SandboxAllocationServiceError, match="sandbox_allocation_reuse_forbidden"
        ):
            await service.create(
                principal,
                lease,
                "hidden:fresh",
                120,
                allocation_class="operator",
                require_new=True,
            )
        assert [call[0] for call in store.calls] == ["reserve"]
        assert factory_calls == []
        assert provider.observe_calls == []
        assert provider.create_calls == []

    asyncio.run(scenario())


def test_require_new_creates_once_and_refuses_replay() -> None:
    async def scenario() -> None:
        store = _FakeStore()
        provider = _FakeProvider()
        service = _service(store, provider)
        principal, lease = _principal_lease()
        first = await service.create(principal, lease, "hidden:fresh", 120, require_new=True)
        assert first.status == "running"
        # The persisted reservation signals that this identity already existed.
        store.reservation = replace(store.reservation, is_new=False)
        with pytest.raises(
            SandboxAllocationServiceError, match="sandbox_allocation_reuse_forbidden"
        ):
            await service.create(principal, lease, "hidden:fresh", 120, require_new=True)
        assert len(provider.create_calls) == 1
        assert provider.observe_calls == []

    asyncio.run(scenario())
