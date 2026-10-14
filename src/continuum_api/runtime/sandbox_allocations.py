"""Durable, fenced reservations for the managed sandbox provider."""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.runtime.leases import (
    ExecutionLease,
    LeaseLost,
    LockedExecution,
    lock_current_execution,
)
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.sandbox.contracts import SandboxObservation

AllocationClass = Literal["public", "operator"]
Row = Mapping[str, Any]

PUBLIC_CPU_SECONDS = 14_400
OPERATOR_CPU_SECONDS = 3_600
MAX_RUN_SECONDS = 900
MAX_SESSION_SECONDS = 900
VCPUS = 2
MEMORY_MB = 4096


class SandboxAllocationError(RuntimeError):
    """A safe-to-display persistence failure with a stable code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class SandboxAllocationIdentityConflict(SandboxAllocationError):
    def __init__(self) -> None:
        super().__init__("sandbox_allocation_identity_conflict")


class SandboxAllocationReservationLost(SandboxAllocationError):
    def __init__(self) -> None:
        super().__init__("sandbox_allocation_reservation_lost")


class SandboxAllocationObservationConflict(SandboxAllocationError):
    def __init__(self) -> None:
        super().__init__("sandbox_allocation_observation_conflict")


@dataclass(frozen=True, slots=True)
class AllocationReservation:
    allocation_id: UUID
    provider_name: str
    status: str
    is_new: bool
    max_requested_seconds: int
    vcpus: int
    memory_mb: int
    provider_period_id: str
    allocation_class: AllocationClass
    provider_session_id: str | None
    owner_id: UUID
    run_id: UUID
    step_id: UUID
    job_id: UUID
    generation: int
    attempt_id: UUID
    lease_owner: str


CreatorIdentity = ExecutionLease | AllocationReservation


class SandboxAllocationStore:
    """Persist sandbox identity, conservative compute charges, and one slot.

    The provider period and quota ledger are locked before the singleton
    capacity row; current run, step, job, and attempt locks follow that row.
    Callers must perform all provider I/O after these short transactions end.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def reserve(
        self,
        principal: Principal,
        lease: ExecutionLease,
        logical_key: str,
        request_sha256: bytes,
        provider_period_id: str | None,
        allocation_class: AllocationClass,
        requested_seconds: int,
        vcpus: int = VCPUS,
        memory_mb: int = MEMORY_MB,
    ) -> AllocationReservation:
        self._validate_request(
            principal,
            lease,
            logical_key,
            request_sha256,
            provider_period_id,
            allocation_class,
            requested_seconds,
            vcpus,
            memory_mb,
        )

        # Replays never re-authorize creation. A released row is the only
        # reusable identity: release_undispatched proves no POST was claimed.
        with self._connection(principal) as connection:
            prior = self._find_identity(connection, principal, lease, logical_key, for_update=False)
            if prior is not None and prior["status"] != "released":
                self._assert_identity(prior, principal, lease)
                self._assert_same_request(
                    prior,
                    logical_key,
                    request_sha256,
                    allocation_class,
                    requested_seconds,
                    vcpus,
                    memory_mb,
                )
                return self._from_row(prior, is_new=False)

        deferred_error: SandboxAllocationError | None = None
        reservation: AllocationReservation | None = None
        with self._connection(principal) as connection:
            period = self._lock_current_period(connection, provider_period_id)
            usage = self._lock_period_usage(connection, provider_period_id)
            assert provider_period_id is not None
            capacity = self._lock_capacity(connection)

            # Carry the active allocation before considering any new request.
            # The marker commits even when the carry cannot fit, so operators
            # can see why admission is fail-closed while the slot stays held.
            if capacity["active_allocation_id"] is not None:
                carry_error = self._carry_active_charge(
                    connection,
                    capacity,
                    provider_period_id,
                    usage,
                    period,
                )
                if carry_error is not None:
                    deferred_error = carry_error
                else:
                    deferred_error = SandboxAllocationError("sandbox_allocation_capacity_busy")
                # Do not raise here: a successful carry or period-block marker
                # is durable even though this admission is rejected.
            else:
                if set(usage) != {"operator", "public"}:
                    raise SandboxAllocationError("sandbox_allocation_period_unavailable")
                locked = lock_current_execution(connection, principal, lease)
                self._assert_live_again(connection, locked)
                run = (
                    connection.execute(
                        text(
                            "SELECT deadline_at FROM runtime_private.runs "
                            "WHERE owner_id=:owner_id AND run_id=:run_id"
                        ),
                        {"owner_id": lease.owner_id, "run_id": lease.run_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if run is None:
                    raise LeaseLost("runtime_run_not_found")

                existing = self._find_identity(
                    connection, principal, lease, logical_key, for_update=True
                )
                if existing is not None and existing["status"] != "released":
                    self._assert_identity(existing, principal, lease)
                    self._assert_same_request(
                        existing,
                        logical_key,
                        request_sha256,
                        allocation_class,
                        requested_seconds,
                        vcpus,
                        memory_mb,
                    )
                    reservation = self._from_row(existing, is_new=False)
                else:
                    if existing is not None:
                        self._assert_identity(existing, principal, lease)
                        self._assert_same_request(
                            existing,
                            logical_key,
                            request_sha256,
                            allocation_class,
                            requested_seconds,
                            vcpus,
                            memory_mb,
                        )
                    seconds = min(requested_seconds, MAX_RUN_SECONDS)
                    if run["deadline_at"] is not None:
                        remaining: int = connection.execute(
                            text(
                                "SELECT floor(extract(epoch FROM "
                                ":deadline_at - clock_timestamp()))::integer"
                            ),
                            {"deadline_at": run["deadline_at"]},
                        ).scalar_one()
                        seconds = min(seconds, max(0, remaining))
                    if seconds <= 0:
                        raise SandboxAllocationError("sandbox_allocation_run_deadline_exhausted")

                    charged: int = connection.execute(
                        text(
                            "SELECT coalesce(sum(requested_seconds), 0) "
                            "FROM runtime_private.sandbox_allocations "
                            "WHERE owner_id=:owner_id AND run_id=:run_id AND status <> 'released'"
                        ),
                        {"owner_id": lease.owner_id, "run_id": lease.run_id},
                    ).scalar_one()
                    if charged + seconds > MAX_RUN_SECONDS:
                        raise SandboxAllocationError("sandbox_allocation_run_budget_exceeded")

                    cpu_seconds = seconds * vcpus
                    period_limit = self._pool_limit(period, allocation_class)
                    used = usage[allocation_class]
                    if used + cpu_seconds > period_limit:
                        raise SandboxAllocationError("sandbox_allocation_quota_exceeded")

                    now: datetime = connection.execute(
                        text("SELECT clock_timestamp()")
                    ).scalar_one()
                    allocation_id = existing["allocation_id"] if existing is not None else uuid4()
                    provider_name = f"continuum-{allocation_id.hex}"
                    values = {
                        "allocation_id": allocation_id,
                        "owner_id": principal.account_id,
                        "run_id": lease.run_id,
                        "step_id": lease.step_id,
                        "job_id": lease.job_id,
                        "generation": lease.generation,
                        "attempt_id": lease.attempt_id,
                        "lease_owner": lease.lease_owner,
                        "logical_key": logical_key,
                        "request_sha256": request_sha256,
                        "provider_name": provider_name,
                        "provider_period_id": provider_period_id,
                        "allocation_class": allocation_class,
                        "requested_seconds": seconds,
                        "vcpus": vcpus,
                        "memory_mb": memory_mb,
                        "reserved_cpu_seconds": cpu_seconds,
                        "now": now,
                    }
                    if existing is None:
                        connection.execute(
                            text(
                                "INSERT INTO runtime_private.sandbox_allocations ("
                                "allocation_id, owner_id, run_id, step_id, job_id, generation, "
                                "attempt_id, lease_owner, logical_key, request_sha256, "
                                "provider_name, "
                                "provider_period_id, allocation_class, requested_seconds, vcpus, "
                                "memory_mb, reserved_cpu_seconds, status, created_at, updated_at) "
                                "VALUES (:allocation_id, :owner_id, :run_id, :step_id, :job_id, "
                                ":generation, :attempt_id, :lease_owner, :logical_key, "
                                ":request_sha256, :provider_name, :provider_period_id, "
                                ":allocation_class, :requested_seconds, :vcpus, :memory_mb, "
                                ":reserved_cpu_seconds, 'reserved', :now, :now)"
                            ),
                            values,
                        )
                    else:
                        # This row has never authorized a provider dispatch.
                        # Re-arm it after a proven local/undispatched refund.
                        connection.execute(
                            text(
                                "UPDATE runtime_private.sandbox_allocations SET "
                                "provider_period_id=:provider_period_id, "
                                "requested_seconds=:requested_seconds, "
                                "vcpus=:vcpus, memory_mb=:memory_mb, "
                                "reserved_cpu_seconds=:reserved_cpu_seconds, "
                                "status='reserved', create_authorized_at=NULL, "
                                "create_observed_at=NULL, "
                                "stop_authorized_at=NULL, destroy_authorized_at=NULL, "
                                "provider_session_id=NULL, "
                                "last_observed_at=NULL, last_sandbox_status=NULL, "
                                "last_session_status=NULL, "
                                "updated_at=:now WHERE allocation_id=:allocation_id "
                                "AND owner_id=:owner_id "
                                "AND status='released' AND create_authorized_at IS NULL"
                            ),
                            values,
                        )

                    connection.execute(
                        text(
                            "UPDATE runtime_private.sandbox_period_usage "
                            "SET charged_cpu_seconds=charged_cpu_seconds + :cpu_seconds "
                            "WHERE provider_period_id=:provider_period_id "
                            "AND allocation_class=:allocation_class"
                        ),
                        {
                            "cpu_seconds": cpu_seconds,
                            "provider_period_id": provider_period_id,
                            "allocation_class": allocation_class,
                        },
                    )
                    connection.execute(
                        text(
                            "INSERT INTO runtime_private.sandbox_allocation_period_charges "
                            "(allocation_id, provider_period_id, allocation_class, cpu_seconds) "
                            "VALUES (:allocation_id, :provider_period_id, "
                            ":allocation_class, :cpu_seconds)"
                        ),
                        {
                            "allocation_id": allocation_id,
                            "provider_period_id": provider_period_id,
                            "allocation_class": allocation_class,
                            "cpu_seconds": cpu_seconds,
                        },
                    )
                    connection.execute(
                        text(
                            "UPDATE runtime_private.sandbox_global_capacity SET "
                            "active_allocation_id=:allocation_id, "
                            "active_provider_name=:provider_name, "
                            "active_allocation_class=:allocation_class, "
                            "active_origin_period_id=:provider_period_id, "
                            "active_cpu_seconds=:cpu_seconds, "
                            "period_blocked_at=NULL, period_blocked_period_id=NULL, "
                            "period_blocked_reason=NULL WHERE capacity_id=1"
                        ),
                        {
                            "allocation_id": allocation_id,
                            "provider_name": provider_name,
                            "allocation_class": allocation_class,
                            "provider_period_id": provider_period_id,
                            "cpu_seconds": cpu_seconds,
                        },
                    )
                    row = self._get_by_id(connection, principal, allocation_id)
                    assert row is not None
                    self._assert_creation_fence(connection, locked, row, provider_period_id)
                    reservation = self._from_row(row, is_new=True)

        if deferred_error is not None:
            raise deferred_error
        if reservation is None:
            raise SandboxAllocationError("sandbox_allocation_store_unavailable")
        return reservation

    def authorize_create(
        self, principal: Principal, lease: ExecutionLease, allocation_id: UUID
    ) -> bool:
        with self._connection(principal) as connection:
            candidate = self._get_by_id(connection, principal, allocation_id)
            if candidate is None:
                raise SandboxAllocationReservationLost()
            self._assert_identity(candidate, principal, lease)
            period_id = str(candidate["provider_period_id"])
            self._lock_current_period(connection, period_id)
            self._lock_period_rows_for_allocation(connection, principal, lease, allocation_id)
            self._lock_capacity(connection)
            locked = lock_current_execution(connection, principal, lease)
            self._assert_live_again(connection, locked)
            row = self._lock_by_id(connection, principal, lease, allocation_id)
            if row["status"] != "reserved" or row["create_authorized_at"] is not None:
                return False
            capacity: UUID = connection.execute(
                text(
                    "SELECT active_allocation_id FROM runtime_private.sandbox_global_capacity "
                    "WHERE capacity_id=1"
                )
            ).scalar_one()
            if capacity != allocation_id:
                raise SandboxAllocationReservationLost()
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_allocations SET status='creating', "
                    "create_authorized_at=clock_timestamp(), updated_at=clock_timestamp() "
                    "WHERE allocation_id=:allocation_id AND owner_id=:owner_id "
                    "AND status='reserved' AND create_authorized_at IS NULL"
                ),
                {"allocation_id": allocation_id, "owner_id": principal.account_id},
            )
            self._assert_creation_fence(connection, locked, row, period_id)
            return True

    def authorize_profile_action(
        self,
        principal: Principal,
        lease: ExecutionLease,
        allocation_id: UUID,
        expected_session_id: str,
        provider_period_id: str | None,
    ) -> int:
        """Fence one sandbox mutation and return its DB-computed time budget."""
        if principal.account_id != lease.owner_id:
            raise LeaseLost("runtime_lease_owner_mismatch")
        if not expected_session_id:
            raise SandboxAllocationObservationConflict()

        deferred_error: SandboxAllocationError | LeaseLost | None = None
        remaining_seconds: int | None = None
        with self._connection(principal) as connection:
            period = self._lock_current_period(connection, provider_period_id)
            assert provider_period_id is not None
            usage = self._lock_period_usage(connection, provider_period_id)
            capacity = self._lock_capacity(connection)

            # The current billing pool receives the full conservative charge
            # before any source, policy, setup, or test mutation is authorized.
            if capacity["active_allocation_id"] is not None:
                carry_error = self._carry_active_charge(
                    connection, capacity, provider_period_id, usage, period
                )
                if carry_error is not None:
                    deferred_error = carry_error
            elif set(usage) != {"operator", "public"}:
                deferred_error = SandboxAllocationError("sandbox_allocation_period_unavailable")

            if deferred_error is None:
                if capacity["active_allocation_id"] != allocation_id:
                    deferred_error = SandboxAllocationReservationLost()
                else:
                    try:
                        locked = lock_current_execution(connection, principal, lease)
                        self._assert_live_again(connection, locked)
                        row = self._lock_by_id(connection, principal, lease, allocation_id)
                        if row["status"] != "running" or row["create_authorized_at"] is None:
                            raise SandboxAllocationReservationLost()
                        if row["provider_session_id"] != expected_session_id:
                            raise SandboxAllocationObservationConflict()
                        self._assert_live_again(connection, locked)

                        current_period_count: int
                        current_period_id: str | None
                        current_period_count, current_period_id = cast(
                            tuple[int, str | None],
                            connection.execute(
                                text(
                                    "WITH db_clock AS (SELECT clock_timestamp() AS now) "
                                    "SELECT count(*), min(provider_period_id) "
                                    "FROM runtime_private.sandbox_provider_periods, db_clock "
                                    "WHERE starts_at <= db_clock.now "
                                    "AND ends_at > db_clock.now"
                                )
                            ).one(),
                        )
                        if current_period_count != 1 or current_period_id != provider_period_id:
                            raise SandboxAllocationError("sandbox_allocation_period_unavailable")

                        session_deadline = row["create_authorized_at"] + timedelta(
                            seconds=int(row["requested_seconds"])
                        )
                        remaining_seconds = int(
                            connection.execute(
                                text(
                                    "WITH db_clock AS (SELECT clock_timestamp() AS now) "
                                    "SELECT CASE WHEN db_clock.now >= :lease_deadline THEN 0 "
                                    "ELSE floor(extract(epoch FROM LEAST("
                                    ":session_deadline, "
                                    "coalesce(:run_deadline, :session_deadline), "
                                    ":period_deadline) - db_clock.now))::integer END "
                                    "FROM db_clock"
                                ),
                                {
                                    "session_deadline": session_deadline,
                                    "run_deadline": locked.run_deadline_at,
                                    "lease_deadline": locked.lease_expires_at,
                                    "period_deadline": period["ends_at"],
                                },
                            ).scalar_one()
                        )
                        if remaining_seconds <= 0:
                            raise SandboxAllocationError(
                                "sandbox_allocation_action_deadline_exhausted"
                            )
                    except (SandboxAllocationError, LeaseLost) as error:
                        deferred_error = error

        if deferred_error is not None:
            raise deferred_error
        if remaining_seconds is None or remaining_seconds <= 0:
            raise SandboxAllocationError("sandbox_allocation_action_deadline_exhausted")
        return remaining_seconds

    def release_undispatched(
        self, principal: Principal, lease: ExecutionLease, allocation_id: UUID
    ) -> None:
        with self._connection(principal) as connection:
            self._lock_period_rows_for_allocation(connection, principal, lease, allocation_id)
            self._lock_capacity(connection)
            locked = lock_current_execution(connection, principal, lease)
            self._assert_live_again(connection, locked)
            row = self._lock_by_id(connection, principal, lease, allocation_id)
            if row["status"] == "released":
                return
            if row["status"] != "reserved" or row["create_authorized_at"] is not None:
                raise SandboxAllocationReservationLost()
            self._refund_undispatched_locked(connection, principal, allocation_id)
            self._assert_live_again(connection, locked)

    def reconcile_undispatched(
        self, principal: Principal, creator: CreatorIdentity, allocation_id: UUID
    ) -> bool:
        """Release only a persisted reservation that never won create authorization.

        This cleanup path is safe after lease loss: the exact immutable creator
        identity or its owner-scoped reservation snapshot is checked, and
        `authorize_create` and this refund serialize on the capacity row.
        """
        with self._connection(principal) as connection:
            self._lock_period_rows_for_allocation(connection, principal, creator, allocation_id)
            self._lock_capacity(connection)
            row = self._lock_by_id(connection, principal, creator, allocation_id)
            if row["status"] == "released":
                return True
            if row["status"] != "reserved" or row["create_authorized_at"] is not None:
                return False
            self._refund_undispatched_locked(connection, principal, allocation_id)
            return True

    def _refund_undispatched_locked(
        self, connection: Connection, principal: Principal, allocation_id: UUID
    ) -> None:
        charges = (
            connection.execute(
                text(
                    "SELECT provider_period_id, allocation_class, cpu_seconds "
                    "FROM runtime_private.sandbox_allocation_period_charges "
                    "WHERE allocation_id=:allocation_id "
                    "ORDER BY provider_period_id, allocation_class"
                ),
                {"allocation_id": allocation_id},
            )
            .mappings()
            .all()
        )
        if not charges:
            raise SandboxAllocationError("sandbox_allocation_ledger_corrupt")
        for charge in charges:
            result = connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_period_usage SET "
                    "charged_cpu_seconds=charged_cpu_seconds - :cpu_seconds "
                    "WHERE provider_period_id=:period_id "
                    "AND allocation_class=:allocation_class "
                    "AND charged_cpu_seconds >= :cpu_seconds"
                ),
                {
                    "cpu_seconds": charge["cpu_seconds"],
                    "period_id": charge["provider_period_id"],
                    "allocation_class": charge["allocation_class"],
                },
            )
            if result.rowcount != 1:
                raise SandboxAllocationError("sandbox_allocation_ledger_corrupt")
        connection.execute(
            text(
                "DELETE FROM runtime_private.sandbox_allocation_period_charges "
                "WHERE allocation_id=:allocation_id"
            ),
            {"allocation_id": allocation_id},
        )
        released = connection.execute(
            text(
                "UPDATE runtime_private.sandbox_allocations SET status='released', "
                "updated_at=clock_timestamp() WHERE allocation_id=:allocation_id "
                "AND owner_id=:owner_id AND status='reserved' "
                "AND create_authorized_at IS NULL"
            ),
            {"allocation_id": allocation_id, "owner_id": principal.account_id},
        )
        if released.rowcount != 1:
            raise SandboxAllocationReservationLost()
        self._clear_capacity(connection, allocation_id)

    def mark_create_not_applied(self, principal: Principal, allocation_id: UUID) -> None:
        with self._connection(principal) as connection:
            self._lock_capacity(connection)
            row = self._lock_by_id_without_lease(connection, principal, allocation_id)
            if row["status"] == "not_applied":
                return
            if row["status"] != "creating" or row["create_authorized_at"] is None:
                raise SandboxAllocationReservationLost()
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_allocations SET status='not_applied', "
                    "updated_at=clock_timestamp() WHERE allocation_id=:allocation_id "
                    "AND owner_id=:owner_id AND status='creating'"
                ),
                {"allocation_id": allocation_id, "owner_id": principal.account_id},
            )
            # The provider was called. Compute remains charged; only the slot
            # is freed after the adapter proves the create had no effect.
            self._clear_capacity(connection, allocation_id)

    def mark_create_unknown(self, principal: Principal, allocation_id: UUID) -> None:
        with self._connection(principal) as connection:
            row = self._lock_by_id_without_lease(connection, principal, allocation_id)
            if row["status"] == "unknown":
                return
            if row["status"] != "creating" or row["create_authorized_at"] is None:
                raise SandboxAllocationReservationLost()
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_allocations SET status='unknown', "
                    "updated_at=clock_timestamp() WHERE allocation_id=:allocation_id "
                    "AND owner_id=:owner_id AND status='creating'"
                ),
                {"allocation_id": allocation_id, "owner_id": principal.account_id},
            )

    def get_for_observation(
        self, principal: Principal, allocation_id: UUID
    ) -> AllocationReservation:
        with self._connection(principal) as connection:
            row = self._get_by_id(connection, principal, allocation_id)
            if row is None:
                raise SandboxAllocationReservationLost()
            return self._from_row(row, is_new=False)

    def list_recovery_allocations(
        self, principal: Principal, lease: ExecutionLease
    ) -> tuple[AllocationReservation, ...]:
        """Find this run's allocations that are not proven inactive and terminal."""
        if principal.account_id != lease.owner_id:
            raise LeaseLost("runtime_lease_owner_mismatch")
        with self._connection(principal) as connection:
            lock_current_execution(connection, principal, lease)
            rows = (
                connection.execute(
                    text(
                        "SELECT * FROM runtime_private.sandbox_allocations "
                        "WHERE owner_id=:owner_id AND run_id=:run_id "
                        "AND status NOT IN ('stopped', 'destroyed', 'not_applied', 'released') "
                        "ORDER BY allocation_id"
                    ),
                    {"owner_id": lease.owner_id, "run_id": lease.run_id},
                )
                .mappings()
                .all()
            )
            return tuple(self._from_row(dict(row), is_new=False) for row in rows)

    def authorize_stop(
        self, principal: Principal, allocation_id: UUID, expected_session_id: str
    ) -> bool:
        with self._connection(principal) as connection:
            capacity = self._lock_capacity(connection)
            row = self._lock_by_id_without_lease(connection, principal, allocation_id)
            self._assert_session(row, expected_session_id)
            if row["status"] != "running" or row["stop_authorized_at"] is not None:
                return False
            if capacity["active_allocation_id"] != allocation_id:
                raise SandboxAllocationReservationLost()
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_allocations SET "
                    "stop_authorized_at=clock_timestamp(), updated_at=clock_timestamp() "
                    "WHERE allocation_id=:allocation_id AND owner_id=:owner_id "
                    "AND status='running' AND stop_authorized_at IS NULL"
                ),
                {"allocation_id": allocation_id, "owner_id": principal.account_id},
            )
            return True

    def mark_stop_unknown(self, principal: Principal, allocation_id: UUID) -> None:
        with self._connection(principal) as connection:
            row = self._lock_by_id_without_lease(connection, principal, allocation_id)
            if row["status"] == "stop_unknown":
                return
            if row["status"] != "running" or row["stop_authorized_at"] is None:
                raise SandboxAllocationReservationLost()
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_allocations SET status='stop_unknown', "
                    "updated_at=clock_timestamp() WHERE allocation_id=:allocation_id "
                    "AND owner_id=:owner_id AND status='running'"
                ),
                {"allocation_id": allocation_id, "owner_id": principal.account_id},
            )

    def mark_observation_unknown(self, principal: Principal, allocation_id: UUID) -> None:
        """Retain capacity when a passive observation contradicts bound identity."""
        with self._connection(principal) as connection:
            row = self._lock_by_id_without_lease(connection, principal, allocation_id)
            if row["status"] == "unknown":
                return
            if row["status"] not in {
                "reserved",
                "creating",
                "running",
                "stop_unknown",
                "stopped",
                "destroy_unknown",
                "destroyed",
            }:
                raise SandboxAllocationReservationLost()
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_allocations SET status='unknown', "
                    "last_observed_at=clock_timestamp(), updated_at=clock_timestamp() "
                    "WHERE allocation_id=:allocation_id AND owner_id=:owner_id"
                ),
                {"allocation_id": allocation_id, "owner_id": principal.account_id},
            )

    def authorize_destroy(
        self, principal: Principal, allocation_id: UUID, expected_session_id: str
    ) -> bool:
        with self._connection(principal) as connection:
            row = self._lock_by_id_without_lease(connection, principal, allocation_id)
            self._assert_session(row, expected_session_id)
            if row["status"] != "stopped" or row["destroy_authorized_at"] is not None:
                return False
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_allocations SET status='destroy_unknown', "
                    "destroy_authorized_at=clock_timestamp(), updated_at=clock_timestamp() "
                    "WHERE allocation_id=:allocation_id AND owner_id=:owner_id "
                    "AND status='stopped' AND destroy_authorized_at IS NULL"
                ),
                {"allocation_id": allocation_id, "owner_id": principal.account_id},
            )
            return True

    def record_observation(
        self,
        principal: Principal,
        allocation_id: UUID,
        observation: SandboxObservation | None,
        provider_period_id: str | None,
    ) -> AllocationReservation:
        if observation is not None:
            _validate_observation(observation)

        # For a termination observed in a later configured period, carry the
        # full conservative charge before freeing concurrency. With no current
        # period configured, exact termination may still release the slot;
        # all future admission remains fail-closed.
        deferred_error: SandboxAllocationError | None = None
        reservation: AllocationReservation | None = None
        with self._connection(principal) as connection:
            if provider_period_id is not None:
                period = self._lock_current_period(connection, provider_period_id)
                usage = self._lock_period_usage(connection, provider_period_id)
            else:
                period = None
                usage = None
            capacity = self._lock_capacity(connection)
            row = self._lock_by_id_without_lease(connection, principal, allocation_id)
            if observation is not None and observation.name != row["provider_name"]:
                raise SandboxAllocationObservationConflict()
            observed_session = None if observation is None else observation.session_id
            if (
                observed_session is not None
                and row["provider_session_id"] is not None
                and observed_session != row["provider_session_id"]
            ):
                raise SandboxAllocationObservationConflict()

            terminal = _is_terminal_observation(observation, row)
            if capacity["active_allocation_id"] == allocation_id and provider_period_id is not None:
                assert period is not None
                carry_error = self._carry_active_charge(
                    connection, capacity, provider_period_id, usage or {}, period
                )
                if carry_error is not None:
                    # Persist the blocked marker and observed status, but do not
                    # release the singleton until the charge is carried.
                    deferred_error = carry_error
                else:
                    capacity = self._lock_capacity(connection)

            now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if observation is None:
                new_status = (
                    "destroyed" if row["destroy_authorized_at"] is not None else row["status"]
                )
                connection.execute(
                    text(
                        "UPDATE runtime_private.sandbox_allocations SET status=:status, "
                        "last_observed_at=:now, updated_at=:now "
                        "WHERE allocation_id=:allocation_id AND owner_id=:owner_id"
                    ),
                    {
                        "status": new_status,
                        "now": now,
                        "allocation_id": allocation_id,
                        "owner_id": principal.account_id,
                    },
                )
            else:
                new_session = row["provider_session_id"] or observed_session
                session_status = _normalize_status(observation.session_status)
                sandbox_status = _normalize_status(observation.sandbox_status)
                new_status = row["status"]
                if (session_status == "stopped" and sandbox_status == "stopped") or (
                    row["provider_session_id"] is None
                    and observation.session_id is None
                    and sandbox_status == "stopped"
                ):
                    new_status = "stopped"
                elif session_status == "stopped" or sandbox_status == "stopped":
                    new_status = "unknown"
                elif row["status"] in {"creating", "unknown"} and (
                    session_status == "running"
                    or (observation.session_id is not None and sandbox_status == "running")
                ):
                    new_status = "running"
                elif row["status"] == "stop_unknown" and session_status == "running":
                    new_status = "stop_unknown"
                if row["create_authorized_at"] is None:
                    # A passive observation can discover an out-of-band object,
                    # but only authorize_create may establish an app-dispatched
                    # lifecycle state. Preserve the anomaly as unknown.
                    new_status = "unknown"
                if row["destroy_authorized_at"] is not None:
                    # A post-destroy readback that still sees the stopped box
                    # preserves uncertainty and never authorizes a second call.
                    new_status = "destroy_unknown"
                if deferred_error is not None:
                    new_status = "unknown"
                connection.execute(
                    text(
                        "UPDATE runtime_private.sandbox_allocations "
                        "SET provider_session_id=:session_id, "
                        "status=:status, create_observed_at=coalesce(create_observed_at, :now), "
                        "last_observed_at=:now, last_sandbox_status=:sandbox_status, "
                        "last_session_status=:session_status, updated_at=:now "
                        "WHERE allocation_id=:allocation_id AND owner_id=:owner_id"
                    ),
                    {
                        "session_id": new_session,
                        "status": new_status,
                        "now": now,
                        "sandbox_status": sandbox_status,
                        "session_status": session_status,
                        "allocation_id": allocation_id,
                        "owner_id": principal.account_id,
                    },
                )

            if (
                terminal
                and deferred_error is None
                and capacity["active_allocation_id"] == allocation_id
            ):
                self._clear_capacity(connection, allocation_id)
            updated = self._get_by_id(connection, principal, allocation_id)
            assert updated is not None
            reservation = self._from_row(updated, is_new=False)
        if deferred_error is not None:
            raise deferred_error
        if reservation is None:
            raise SandboxAllocationError("sandbox_allocation_store_unavailable")
        return reservation

    @contextmanager
    def _connection(self, principal: Principal) -> Iterator[Connection]:
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, principal)
                yield connection
        except (SandboxAllocationError, LeaseLost):
            raise
        except SQLAlchemyError:
            raise SandboxAllocationError("sandbox_allocation_store_unavailable") from None

    @staticmethod
    def _find_identity(
        connection: Connection,
        principal: Principal,
        lease: ExecutionLease,
        logical_key: str,
        *,
        for_update: bool,
    ) -> Row | None:
        suffix = " FOR UPDATE" if for_update else ""
        row = (
            connection.execute(
                text(
                    "SELECT * FROM runtime_private.sandbox_allocations "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id "
                    "AND job_id=:job_id AND generation=:generation AND attempt_id=:attempt_id "
                    "AND logical_key=:logical_key" + suffix
                ),
                {
                    "owner_id": principal.account_id,
                    "run_id": lease.run_id,
                    "step_id": lease.step_id,
                    "job_id": lease.job_id,
                    "generation": lease.generation,
                    "attempt_id": lease.attempt_id,
                    "logical_key": logical_key,
                },
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else dict(row)

    @staticmethod
    def _get_by_id(connection: Connection, principal: Principal, allocation_id: UUID) -> Row | None:
        row = (
            connection.execute(
                text(
                    "SELECT * FROM runtime_private.sandbox_allocations "
                    "WHERE owner_id=:owner_id AND allocation_id=:allocation_id"
                ),
                {"owner_id": principal.account_id, "allocation_id": allocation_id},
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else dict(row)

    @staticmethod
    def _lock_by_id(
        connection: Connection,
        principal: Principal,
        lease: CreatorIdentity,
        allocation_id: UUID,
    ) -> Row:
        row = (
            connection.execute(
                text(
                    "SELECT * FROM runtime_private.sandbox_allocations "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND allocation_id=:allocation_id "
                    "FOR UPDATE"
                ),
                {
                    "owner_id": principal.account_id,
                    "run_id": lease.run_id,
                    "allocation_id": allocation_id,
                },
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise SandboxAllocationReservationLost()
        result = dict(row)
        SandboxAllocationStore._assert_identity(result, principal, lease)
        return result

    @staticmethod
    def _lock_by_id_without_lease(
        connection: Connection, principal: Principal, allocation_id: UUID
    ) -> Row:
        row = (
            connection.execute(
                text(
                    "SELECT * FROM runtime_private.sandbox_allocations "
                    "WHERE owner_id=:owner_id AND allocation_id=:allocation_id FOR UPDATE"
                ),
                {"owner_id": principal.account_id, "allocation_id": allocation_id},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise SandboxAllocationReservationLost()
        return dict(row)

    @staticmethod
    def _lock_capacity(connection: Connection) -> Row:
        row = (
            connection.execute(
                text(
                    "SELECT * FROM runtime_private.sandbox_global_capacity "
                    "WHERE capacity_id=1 FOR UPDATE"
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise SandboxAllocationError("sandbox_allocation_capacity_unavailable")
        return dict(row)

    @staticmethod
    def _clear_capacity(connection: Connection, allocation_id: UUID) -> None:
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_global_capacity SET active_allocation_id=NULL, "
                "active_provider_name=NULL, active_allocation_class=NULL, "
                "active_origin_period_id=NULL, "
                "active_cpu_seconds=NULL, period_blocked_at=NULL, period_blocked_period_id=NULL, "
                "period_blocked_reason=NULL WHERE capacity_id=1 "
                "AND active_allocation_id=:allocation_id"
            ),
            {"allocation_id": allocation_id},
        )

    @staticmethod
    def _lock_current_period(connection: Connection, provider_period_id: str | None) -> Row:
        if provider_period_id is None:
            raise SandboxAllocationError("sandbox_allocation_period_unavailable")
        rows = (
            connection.execute(
                text(
                    "WITH db_clock AS (SELECT clock_timestamp() AS now) "
                    "SELECT provider_period_id, starts_at, ends_at, public_cpu_seconds_limit, "
                    "operator_cpu_seconds_limit "
                    "FROM runtime_private.sandbox_provider_periods, db_clock "
                    "WHERE starts_at <= db_clock.now AND ends_at > db_clock.now "
                    "ORDER BY starts_at"
                )
            )
            .mappings()
            .all()
        )
        if len(rows) != 1 or rows[0]["provider_period_id"] != provider_period_id:
            raise SandboxAllocationError("sandbox_allocation_period_unavailable")
        return dict(rows[0])

    @staticmethod
    def _lock_period_usage(
        connection: Connection, provider_period_id: str | None
    ) -> dict[str, int]:
        if provider_period_id is None:
            raise SandboxAllocationError("sandbox_allocation_period_unavailable")
        rows = (
            connection.execute(
                text(
                    "SELECT allocation_class, charged_cpu_seconds "
                    "FROM runtime_private.sandbox_period_usage WHERE provider_period_id=:period_id "
                    "ORDER BY allocation_class FOR UPDATE"
                ),
                {"period_id": provider_period_id},
            )
            .mappings()
            .all()
        )
        usage = {str(row["allocation_class"]): int(row["charged_cpu_seconds"]) for row in rows}
        return usage

    def _lock_period_rows_for_allocation(
        self,
        connection: Connection,
        principal: Principal,
        lease: CreatorIdentity,
        allocation_id: UUID,
    ) -> None:
        row = self._get_by_id(connection, principal, allocation_id)
        if row is None:
            raise SandboxAllocationReservationLost()
        self._assert_identity(row, principal, lease)
        raw_period_ids = cast(
            list[str],
            connection.execute(
                text(
                    "SELECT DISTINCT provider_period_id FROM "
                    "runtime_private.sandbox_allocation_period_charges "
                    "WHERE allocation_id=:allocation_id ORDER BY provider_period_id"
                ),
                {"allocation_id": allocation_id},
            )
            .scalars()
            .all(),
        )
        period_ids = [str(value) for value in raw_period_ids]
        if not period_ids:
            return
        # All period/pool locks precede the singleton and execution locks.
        connection.execute(
            text(
                "SELECT provider_period_id, allocation_class "
                "FROM runtime_private.sandbox_period_usage "
                "WHERE provider_period_id = ANY(:period_ids) "
                "ORDER BY provider_period_id, allocation_class FOR UPDATE"
            ),
            {"period_ids": period_ids},
        ).all()

    @staticmethod
    def _carry_active_charge(
        connection: Connection,
        capacity: Row,
        provider_period_id: str,
        usage: Mapping[str, int],
        period: Row,
    ) -> SandboxAllocationError | None:
        allocation_id = capacity["active_allocation_id"]
        assert isinstance(allocation_id, UUID)
        active_period = capacity["active_origin_period_id"]
        active_class = str(capacity["active_allocation_class"])
        cpu_seconds = int(capacity["active_cpu_seconds"])
        if active_period == provider_period_id:
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_global_capacity SET period_blocked_at=NULL, "
                    "period_blocked_period_id=NULL, period_blocked_reason=NULL WHERE capacity_id=1"
                )
            )
            return None
        already_charged = connection.execute(
            text(
                "SELECT 1 FROM runtime_private.sandbox_allocation_period_charges "
                "WHERE allocation_id=:allocation_id AND provider_period_id=:period_id"
            ),
            {"allocation_id": allocation_id, "period_id": provider_period_id},
        ).scalar_one_or_none()
        if already_charged is not None:
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_global_capacity SET period_blocked_at=NULL, "
                    "period_blocked_period_id=NULL, period_blocked_reason=NULL WHERE capacity_id=1"
                )
            )
            return None

        limit = SandboxAllocationStore._pool_limit(period, active_class)
        current_usage = usage.get(active_class)
        if current_usage is None:
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_global_capacity "
                    "SET period_blocked_at=clock_timestamp(), "
                    "period_blocked_period_id=:period_id, "
                    "period_blocked_reason='period_ledger_unavailable' "
                    "WHERE capacity_id=1"
                ),
                {"period_id": provider_period_id},
            )
            return SandboxAllocationError("sandbox_allocation_period_blocked")
        if current_usage + cpu_seconds > limit:
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_global_capacity "
                    "SET period_blocked_at=clock_timestamp(), "
                    "period_blocked_period_id=:period_id, "
                    "period_blocked_reason='carry_exceeds_pool' "
                    "WHERE capacity_id=1"
                ),
                {"period_id": provider_period_id},
            )
            return SandboxAllocationError("sandbox_allocation_period_blocked")
        result = connection.execute(
            text(
                "UPDATE runtime_private.sandbox_period_usage SET "
                "charged_cpu_seconds=charged_cpu_seconds + :cpu_seconds "
                "WHERE provider_period_id=:period_id AND allocation_class=:allocation_class"
            ),
            {
                "cpu_seconds": cpu_seconds,
                "period_id": provider_period_id,
                "allocation_class": active_class,
            },
        )
        if result.rowcount != 1:
            return SandboxAllocationError("sandbox_allocation_period_blocked")
        connection.execute(
            text(
                "INSERT INTO runtime_private.sandbox_allocation_period_charges "
                "(allocation_id, provider_period_id, allocation_class, cpu_seconds) "
                "VALUES (:allocation_id, :period_id, :allocation_class, :cpu_seconds)"
            ),
            {
                "allocation_id": allocation_id,
                "period_id": provider_period_id,
                "allocation_class": active_class,
                "cpu_seconds": cpu_seconds,
            },
        )
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_global_capacity SET period_blocked_at=NULL, "
                "period_blocked_period_id=NULL, period_blocked_reason=NULL WHERE capacity_id=1"
            )
        )
        return None

    @staticmethod
    def _pool_limit(period: Row, allocation_class: str) -> int:
        if allocation_class == "public":
            return int(period["public_cpu_seconds_limit"])
        return int(period["operator_cpu_seconds_limit"])

    @staticmethod
    def _assert_identity(row: Row, principal: Principal, lease: CreatorIdentity) -> None:
        if (
            principal.account_id != lease.owner_id
            or row["owner_id"] != lease.owner_id
            or row["run_id"] != lease.run_id
            or row["step_id"] != lease.step_id
            or row["job_id"] != lease.job_id
            or row["generation"] != lease.generation
            or row["attempt_id"] != lease.attempt_id
            or row["lease_owner"] != lease.lease_owner
        ):
            raise SandboxAllocationReservationLost()

    @staticmethod
    def _assert_same_request(
        row: Row,
        logical_key: str,
        request_sha256: bytes,
        allocation_class: AllocationClass,
        requested_seconds: int,
        vcpus: int,
        memory_mb: int,
    ) -> None:
        if (
            row["logical_key"] != logical_key
            or row["request_sha256"] != request_sha256
            or row["allocation_class"] != allocation_class
            or int(row["requested_seconds"]) > requested_seconds
            or int(row["requested_seconds"]) <= 0
            or row["vcpus"] != vcpus
            or row["memory_mb"] != memory_mb
        ):
            raise SandboxAllocationIdentityConflict()

    @staticmethod
    def _assert_session(row: Row, expected_session_id: str) -> None:
        if (
            not expected_session_id
            or row["provider_session_id"] is None
            or row["provider_session_id"] != expected_session_id
        ):
            raise SandboxAllocationObservationConflict()

    @staticmethod
    def _assert_live_again(connection: Connection, locked: LockedExecution) -> None:
        now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
        lease_expires_at = locked.lease_expires_at
        run_deadline_at = locked.run_deadline_at
        if lease_expires_at is None or lease_expires_at <= now:
            raise LeaseLost("runtime_lease_expired_before_commit")
        if run_deadline_at is not None and run_deadline_at <= now:
            raise LeaseLost("runtime_run_expired_before_commit")

    @staticmethod
    def _assert_creation_fence(
        connection: Connection,
        locked: LockedExecution,
        row: Row,
        provider_period_id: str,
    ) -> None:
        SandboxAllocationStore._assert_live_again(connection, locked)
        current_period_count: int
        current_period_id: str | None
        current_period_count, current_period_id = cast(
            tuple[int, str | None],
            connection.execute(
                text(
                    "WITH db_clock AS (SELECT clock_timestamp() AS now) "
                    "SELECT count(*), min(provider_period_id) "
                    "FROM runtime_private.sandbox_provider_periods, db_clock "
                    "WHERE starts_at <= db_clock.now AND ends_at > db_clock.now"
                ),
            ).one(),
        )
        if current_period_count != 1 or current_period_id != provider_period_id:
            raise SandboxAllocationError("sandbox_allocation_period_unavailable")
        if locked.run_deadline_at is not None:
            remaining: int = connection.execute(
                text("SELECT floor(extract(epoch FROM :deadline_at - clock_timestamp()))::integer"),
                {"deadline_at": locked.run_deadline_at},
            ).scalar_one()
            if int(row["requested_seconds"]) > remaining:
                raise SandboxAllocationError("sandbox_allocation_run_deadline_exhausted")

    @staticmethod
    def _validate_request(
        principal: Principal,
        lease: ExecutionLease,
        logical_key: str,
        request_sha256: bytes,
        provider_period_id: str | None,
        allocation_class: AllocationClass,
        requested_seconds: int,
        vcpus: int,
        memory_mb: int,
    ) -> None:
        if principal.account_id != lease.owner_id:
            raise LeaseLost("runtime_lease_owner_mismatch")
        if not logical_key or logical_key.strip() != logical_key or len(logical_key) > 128:
            raise ValueError("sandbox_allocation_logical_key_invalid")
        if len(request_sha256) != 32:
            raise ValueError("sandbox_allocation_request_digest_invalid")
        if provider_period_id is not None and (
            not provider_period_id.strip() or len(provider_period_id) > 64
        ):
            raise ValueError("sandbox_allocation_period_id_invalid")
        if allocation_class not in {"public", "operator"}:
            raise ValueError("sandbox_allocation_class_invalid")
        if not 1 <= requested_seconds <= MAX_SESSION_SECONDS:
            raise ValueError("sandbox_allocation_duration_invalid")
        if vcpus != VCPUS or memory_mb != MEMORY_MB:
            raise ValueError("sandbox_allocation_resources_invalid")

    @staticmethod
    def _from_row(row: Row, *, is_new: bool) -> AllocationReservation:
        return AllocationReservation(
            allocation_id=row["allocation_id"],
            provider_name=str(row["provider_name"]),
            status=str(row["status"]),
            is_new=is_new,
            max_requested_seconds=int(row["requested_seconds"]),
            vcpus=int(row["vcpus"]),
            memory_mb=int(row["memory_mb"]),
            provider_period_id=str(row["provider_period_id"]),
            allocation_class=cast(AllocationClass, str(row["allocation_class"])),
            provider_session_id=(
                None if row["provider_session_id"] is None else str(row["provider_session_id"])
            ),
            owner_id=row["owner_id"],
            run_id=row["run_id"],
            step_id=row["step_id"],
            job_id=row["job_id"],
            generation=int(row["generation"]),
            attempt_id=row["attempt_id"],
            lease_owner=str(row["lease_owner"]),
        )


def _normalize_status(status: str | None) -> str | None:
    if status is None:
        return None
    normalized = status.strip().lower()
    if not _STATUS.fullmatch(normalized):
        raise SandboxAllocationObservationConflict()
    return normalized


def _validate_observation(observation: SandboxObservation) -> None:
    if (
        not observation.name
        or len(observation.name) > 64
        or observation.name.strip() != observation.name
    ):
        raise SandboxAllocationObservationConflict()
    if observation.session_id is not None and (
        not observation.session_id
        or len(observation.session_id) > 128
        or observation.session_id.strip() != observation.session_id
        or any(ord(character) < 32 for character in observation.session_id)
    ):
        raise SandboxAllocationObservationConflict()
    _normalize_status(observation.sandbox_status)
    _normalize_status(observation.session_status)


def _is_terminal_observation(observation: SandboxObservation | None, row: Row) -> bool:
    if observation is None:
        # Absence is conclusive only after the exact creator authorized one
        # destroy request. A create-side 404 never releases capacity.
        return row["destroy_authorized_at"] is not None
    if (
        row["provider_session_id"] is None
        and row["status"] in {"creating", "unknown"}
        and observation.session_id is None
        and _normalize_status(observation.sandbox_status) == "stopped"
        and observation.session_status is None
    ):
        # On the never-bound name, provider STOPPED is positive proof because
        # that sandbox was created with persistent=False and has no session.
        return True
    return (
        observation.session_id is not None
        and observation.session_id == row["provider_session_id"]
        and _normalize_status(observation.sandbox_status) == "stopped"
        and _normalize_status(observation.session_status) == "stopped"
    )


_STATUS = re.compile(r"^[a-z0-9_.:-]{1,32}$")
