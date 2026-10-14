"""Focused ownership checks for T21 allocation quiescence."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine

from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.sandbox_allocations import (
    AllocationReservation,
    SandboxAllocationStore,
)
from continuum_api.runtime.transitions import TransitionService
from continuum_api.runtime.workspace_recovery import (
    WorkspaceRecoveryError,
    WorkspaceRecoveryService,
)


class _RecoveryStore:
    def __init__(self, allocation: AllocationReservation) -> None:
        self.allocation = allocation

    def list_recovery_allocations(
        self, principal: Principal, lease: ExecutionLease
    ) -> tuple[AllocationReservation, ...]:
        assert principal.account_id == lease.owner_id
        return (self.allocation,)


class _Reconciler:
    def __init__(self, observed: AllocationReservation) -> None:
        self.observed = observed
        self.stop_calls = 0

    async def observe(self, principal: Principal, allocation_id: UUID) -> AllocationReservation:
        return self.observed

    async def stop(self, principal: Principal, allocation_id: UUID) -> AllocationReservation:
        self.stop_calls += 1
        return self.observed


def _fixture(
    observed_change: dict[str, object],
) -> tuple[WorkspaceRecoveryService, Principal, ExecutionLease, _Reconciler]:
    owner_id = uuid4()
    run_id = uuid4()
    step_id = uuid4()
    job_id = uuid4()
    attempt_id = uuid4()
    principal = Principal(owner_id)
    lease = ExecutionLease(
        owner_id=owner_id,
        run_id=run_id,
        step_id=step_id,
        job_id=job_id,
        attempt_id=attempt_id,
        generation=3,
        lease_owner="worker-current",
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=30),
    )
    allocation = AllocationReservation(
        allocation_id=uuid4(),
        provider_name="continuum-fixture",
        status="unknown",
        is_new=False,
        max_requested_seconds=120,
        vcpus=2,
        memory_mb=4096,
        provider_period_id="period-1",
        allocation_class="public",
        provider_session_id=None,
        owner_id=owner_id,
        run_id=run_id,
        step_id=step_id,
        job_id=job_id,
        generation=2,
        attempt_id=uuid4(),
        lease_owner="worker-expired",
    )
    observed = replace(allocation, status="stopped", **cast(Any, observed_change))
    reconciler = _Reconciler(observed)
    service = WorkspaceRecoveryService(
        cast(Engine, object()),
        principal,
        cast(CheckpointStorageService, object()),
        cast(TransitionService, object()),
        allocation_store=cast(SandboxAllocationStore, _RecoveryStore(allocation)),
        allocation_reconciler=reconciler,
    )
    return service, principal, lease, reconciler


@pytest.mark.parametrize(
    "observed_change",
    [
        {"owner_id": uuid4()},
        {"allocation_id": uuid4()},
        {"generation": 3},
    ],
)
def test_recovery_rejects_mismatched_allocation_identity(
    observed_change: dict[str, object],
) -> None:
    service, principal, lease, reconciler = _fixture(observed_change)

    with pytest.raises(WorkspaceRecoveryError) as error:
        asyncio.run(service._reconcile_prior_allocations(principal, lease))

    assert error.value.code == "workspace_allocation_identity_mismatch"
    assert reconciler.stop_calls == 0
