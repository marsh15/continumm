"""Fenced accounting and reconciliation for Vercel sandbox allocations."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import Callable
from typing import Any
from uuid import UUID

from continuum_api.runtime.leases import ExecutionLease, LeaseLost
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.sandbox_allocations import (
    AllocationClass,
    AllocationReservation,
    SandboxAllocationError,
    SandboxAllocationStore,
)
from continuum_api.sandbox.config import (
    ALLOCATION_MEMORY_MB,
    ALLOCATION_VCPUS,
    MAX_ALLOCATION_SECONDS,
    SandboxAllocationLimits,
)
from continuum_api.sandbox.contracts import (
    EffectState,
    SandboxAction,
    SandboxEffectError,
)
from continuum_api.sandbox.vercel import VercelSandboxAllocation, VercelSandboxProvider

_LOGGER = logging.getLogger(__name__)
ALLOCATION_ACTION_DEADLINE_SECONDS = 90.0
_LOGICAL_KEY_MAX_BYTES = 128
_CLEANUP_DEADLINE_SECONDS = 5.0


class SandboxAllocationServiceError(RuntimeError):
    """A sanitized allocation-service error with a stable code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class SandboxAllocationService:
    """Reserve before one create, then only observe existing provider names."""

    def __init__(
        self,
        store: SandboxAllocationStore,
        *,
        image: str,
        provider_period_id: str | None,
        provider_factory: Callable[[SandboxAllocationLimits], VercelSandboxProvider],
        action_deadline_seconds: float = ALLOCATION_ACTION_DEADLINE_SECONDS,
    ) -> None:
        if action_deadline_seconds <= 0:
            raise ValueError("sandbox allocation deadline must be positive")
        self._store = store
        self._image = image
        self._provider_period_id = provider_period_id
        self._provider_factory = provider_factory
        self._action_deadline_seconds = action_deadline_seconds

    async def create(
        self,
        principal: Principal,
        lease: ExecutionLease,
        logical_key: str,
        requested_seconds: int,
        *,
        allocation_class: AllocationClass = "public",
    ) -> AllocationReservation:
        """Reserve all limits, authorize one POST, and never blindly repeat it."""
        if principal.account_id != lease.owner_id:
            raise SandboxAllocationServiceError("sandbox_allocation_owner_mismatch")
        if not logical_key or logical_key.strip() != logical_key:
            raise SandboxAllocationServiceError("sandbox_allocation_logical_key_invalid")
        if len(logical_key.encode("utf-8")) > _LOGICAL_KEY_MAX_BYTES:
            raise SandboxAllocationServiceError("sandbox_allocation_logical_key_invalid")
        if (
            not isinstance(requested_seconds, int)
            or isinstance(requested_seconds, bool)
            or not 1 <= requested_seconds <= MAX_ALLOCATION_SECONDS
        ):
            raise SandboxAllocationServiceError("sandbox_allocation_duration_invalid")
        if self._provider_period_id is None:
            raise SandboxAllocationServiceError("sandbox_allocation_period_unconfigured")
        if allocation_class not in {"public", "operator"}:
            raise SandboxAllocationServiceError("sandbox_allocation_class_invalid")

        try:
            request_sha256 = _request_sha256(
                logical_key,
                self._image,
                allocation_class,
                requested_seconds,
            )
            async with asyncio.timeout(self._action_deadline_seconds):
                reservation = await self._reserve_shielded(
                    principal,
                    lease,
                    logical_key,
                    request_sha256,
                    allocation_class,
                    requested_seconds,
                )
                if not reservation.is_new:
                    return await self._observe_existing(principal, reservation)

                create_authorized = False
                reconciliation_attempted = False
                try:
                    limits = self._limits_for(reservation)
                    provider = self._provider_factory(limits)
                    async with provider.connection() as connected_provider:
                        authorized = await self._shielded_store_call(
                            self._store.authorize_create,
                            principal,
                            lease,
                            reservation.allocation_id,
                        )
                        if not authorized:
                            latest = await self._finalize_undispatched(
                                principal, lease, reservation
                            )
                            reconciliation_attempted = True
                            if latest.status == "released":
                                return latest
                            return await self._observe_with_provider(
                                principal, latest, connected_provider
                            )
                        create_authorized = True
                        try:
                            allocation = await connected_provider.create(
                                reservation.provider_name,
                                tags=_allocation_tags(reservation),
                            )
                        except SandboxEffectError as error:
                            if (
                                error.action is SandboxAction.CREATE
                                and error.effect is EffectState.NOT_APPLIED
                            ):
                                await asyncio.to_thread(
                                    self._store.mark_create_not_applied,
                                    principal,
                                    reservation.allocation_id,
                                )
                                raise SandboxAllocationServiceError(
                                    "sandbox_create_not_applied"
                                ) from None
                            await asyncio.to_thread(
                                self._store.mark_create_unknown,
                                principal,
                                reservation.allocation_id,
                            )
                            return await self._observe_with_provider(
                                principal, reservation, connected_provider
                            )
                        except asyncio.CancelledError:
                            await self._mark_create_unknown(principal, reservation)
                            raise
                        except Exception:
                            await self._mark_create_unknown(principal, reservation)
                            raise SandboxAllocationServiceError(
                                "sandbox_create_outcome_unknown"
                            ) from None

                        if not _valid_allocation_handle(reservation, allocation):
                            await self._mark_create_unknown(principal, reservation)
                            return await self._observe_with_provider(
                                principal, reservation, connected_provider
                            )
                        return await self._record_observation(
                            principal,
                            reservation,
                            allocation,
                            provider_period_id=self._provider_period_id,
                        )
                except asyncio.CancelledError:
                    if create_authorized:
                        await self._mark_create_unknown(principal, reservation)
                    raise
                except LeaseLost:
                    raise SandboxAllocationServiceError("sandbox_allocation_lease_lost") from None
                except SandboxEffectError as error:
                    if not create_authorized:
                        raise SandboxAllocationServiceError(
                            "sandbox_allocation_provider_configuration_invalid"
                        ) from None
                    await self._mark_create_unknown(principal, reservation)
                    _log_provider_failure("create_connection", error)
                    return await self._observe_existing(principal, reservation)
                except SandboxAllocationServiceError:
                    raise
                except SandboxAllocationError:
                    raise
                except Exception as error:
                    if not create_authorized:
                        await self._release_undispatched(principal, lease, reservation)
                        raise SandboxAllocationServiceError(
                            "sandbox_allocation_provider_configuration_invalid"
                        ) from None
                    await self._mark_create_unknown(principal, reservation)
                    _log_exception("create_connection", error)
                    return await self._observe_existing(principal, reservation)
                finally:
                    if not create_authorized and not reconciliation_attempted:
                        await self._finalize_undispatched(principal, lease, reservation)
        except TimeoutError:
            raise SandboxAllocationServiceError("sandbox_allocation_deadline_elapsed") from None
        except asyncio.CancelledError:
            raise
        except SandboxAllocationServiceError:
            raise
        except SandboxAllocationError as error:
            raise SandboxAllocationServiceError(error.code) from None
        except LeaseLost:
            raise SandboxAllocationServiceError("sandbox_allocation_lease_lost") from None

    async def _reserve_shielded(
        self,
        principal: Principal,
        lease: ExecutionLease,
        logical_key: str,
        request_sha256: bytes,
        allocation_class: AllocationClass,
        requested_seconds: int,
    ) -> AllocationReservation:
        task = asyncio.create_task(
            asyncio.to_thread(
                self._store.reserve,
                principal,
                lease,
                logical_key,
                request_sha256,
                self._provider_period_id,
                allocation_class,
                requested_seconds,
                ALLOCATION_VCPUS,
                ALLOCATION_MEMORY_MB,
            )
        )
        try:
            async with asyncio.timeout(self._action_deadline_seconds):
                return await asyncio.shield(task)
        except (asyncio.CancelledError, TimeoutError):
            reservation = await self._settle_thread_task(task, "reserve")
            if isinstance(reservation, AllocationReservation) and reservation.is_new:
                await self._finalize_undispatched(principal, lease, reservation)
            raise

    async def _shielded_store_call(self, method: Callable[..., Any], *args: Any) -> Any:
        task = asyncio.create_task(asyncio.to_thread(method, *args))
        try:
            async with asyncio.timeout(self._action_deadline_seconds):
                return await asyncio.shield(task)
        except (asyncio.CancelledError, TimeoutError):
            await self._settle_thread_task(task, getattr(method, "__name__", "store_call"))
            raise

    async def _settle_thread_task(self, task: asyncio.Task[Any], operation: str) -> Any | None:
        try:
            async with asyncio.timeout(_CLEANUP_DEADLINE_SECONDS):
                return await asyncio.shield(task)
        except TimeoutError:
            _LOGGER.warning("sandbox_allocation_thread_call_unsettled operation=%s", operation)
        except Exception as error:
            _log_error_type(operation, error)
        return None

    async def _reconcile_undispatched(
        self,
        principal: Principal,
        creator: ExecutionLease | AllocationReservation,
        reservation: AllocationReservation,
    ) -> bool:
        """Refund only if PostgreSQL proves that no create claim was recorded."""
        task = asyncio.create_task(
            asyncio.to_thread(
                self._store.reconcile_undispatched,
                principal,
                creator,
                reservation.allocation_id,
            )
        )
        try:
            async with asyncio.timeout(_CLEANUP_DEADLINE_SECONDS):
                return bool(await asyncio.shield(task))
        except asyncio.CancelledError:
            await self._settle_thread_task(task, "reconcile_undispatched")
            raise
        except TimeoutError:
            await self._settle_thread_task(task, "reconcile_undispatched")
            _LOGGER.warning("sandbox_allocation_cleanup_deadline_elapsed")
        except SandboxAllocationError as error:
            _log_persistence_failure("reconcile_undispatched", error)
        except Exception as error:
            _log_error_type("reconcile_undispatched", error)
        return False

    async def _finalize_undispatched(
        self,
        principal: Principal,
        creator: ExecutionLease | AllocationReservation,
        reservation: AllocationReservation,
    ) -> AllocationReservation:
        released = await self._reconcile_undispatched(principal, creator, reservation)
        latest = await self._get_for_observation_or_default(principal, reservation)
        if not released and latest.status == "creating":
            await self._mark_create_unknown(principal, latest)
            latest = await self._get_for_observation_or_default(principal, latest)
        return latest

    async def observe(self, principal: Principal, allocation_id: UUID) -> AllocationReservation:
        """Passively reconcile one owner-scoped allocation by its stable name."""
        try:
            async with asyncio.timeout(self._action_deadline_seconds):
                reservation = await asyncio.to_thread(
                    self._store.get_for_observation,
                    principal,
                    allocation_id,
                )
                if reservation.status in {"destroyed", "not_applied", "released"}:
                    return reservation
                return await self._observe_existing(principal, reservation)
        except TimeoutError:
            raise SandboxAllocationServiceError("sandbox_observation_deadline_elapsed") from None
        except SandboxAllocationError as error:
            raise SandboxAllocationServiceError(error.code) from None

    async def stop(self, principal: Principal, allocation_id: UUID) -> AllocationReservation:
        try:
            async with asyncio.timeout(self._action_deadline_seconds):
                return await self._stop_once(principal, allocation_id)
        except TimeoutError:
            raise SandboxAllocationServiceError("sandbox_stop_outcome_unknown") from None

    async def _stop_once(self, principal: Principal, allocation_id: UUID) -> AllocationReservation:
        """Stop only the persisted session ID once, then prove the whole box stopped."""
        reservation = await self._get_for_observation(principal, allocation_id)
        if reservation.status == "reserved":
            reservation = await self._finalize_undispatched(principal, reservation, reservation)
            if reservation.status in {"released", "not_applied"}:
                return reservation
        if reservation.status in {"destroyed", "not_applied", "released"}:
            return reservation
        if reservation.provider_session_id is None:
            reservation = await self.observe(principal, allocation_id)
            if reservation.provider_session_id is None:
                return reservation
        expected_session_id = reservation.provider_session_id
        if expected_session_id is None:
            return reservation

        provider = self._provider_for(reservation)
        try:
            async with asyncio.timeout(self._action_deadline_seconds):
                async with provider.connection() as connected_provider:
                    allocation = await connected_provider.observe_exact_session(
                        reservation.provider_name,
                        expected_session_id,
                    )
                    if allocation is None:
                        return await self._record_empty_observation(principal, reservation)
                    if not _matches_persisted_identity(reservation, allocation):
                        await self._mark_observation_unknown(principal, reservation)
                        raise SandboxAllocationServiceError(
                            "sandbox_allocation_session_identity_conflict"
                        )

                    if _is_whole_box_terminal(reservation, allocation):
                        return await self._record_observation(
                            principal,
                            reservation,
                            allocation,
                            provider_period_id=self._provider_period_id,
                        )

                    # A stopped historical session does not prove that the
                    # named sandbox is stopped. Likewise, an exact old session
                    # must not be actuated when the box now points at another
                    # session. Preserve the slot until a whole-box terminal
                    # observation resolves the discrepancy.
                    if not _is_exact_current_session(reservation, allocation):
                        await self._mark_observation_unknown(principal, reservation)
                        return await self._get_for_observation_or_default(principal, reservation)

                    reservation = await self._record_observation(
                        principal,
                        reservation,
                        allocation,
                        provider_period_id=self._provider_period_id,
                    )
                    if reservation.status != "running":
                        return reservation
                    authorized = await asyncio.to_thread(
                        self._store.authorize_stop,
                        principal,
                        allocation_id,
                        expected_session_id,
                    )
                    if not authorized:
                        return await self._observe_with_provider(
                            principal, reservation, connected_provider
                        )
                    try:
                        await allocation.stop()
                    except SandboxEffectError as error:
                        _log_provider_failure("stop", error)
                        await self._mark_stop_unknown(principal, reservation)
                        return await self._observe_with_provider(
                            principal, reservation, connected_provider
                        )
                    except Exception as error:
                        _log_exception("stop", error)
                        await self._mark_stop_unknown(principal, reservation)
                        return reservation

                    # Session stop is safe on the exact historical handle, but
                    # only an exact terminal box observation frees concurrency.
                    exact = await connected_provider.observe_exact_session(
                        reservation.provider_name,
                        expected_session_id,
                    )
                    if exact is None or not _matches_persisted_identity(reservation, exact):
                        await self._mark_stop_unknown(principal, reservation)
                        return reservation
                    if not _is_whole_box_terminal(reservation, exact):
                        await self._mark_observation_unknown(principal, reservation)
                        return await self._get_for_observation_or_default(principal, reservation)
                    return await self._record_observation(
                        principal,
                        reservation,
                        exact,
                        provider_period_id=self._provider_period_id,
                    )
        except TimeoutError:
            await self._mark_stop_unknown(principal, reservation)
            raise SandboxAllocationServiceError("sandbox_stop_outcome_unknown") from None
        except SandboxAllocationServiceError:
            raise
        except SandboxAllocationError as error:
            raise SandboxAllocationServiceError(error.code) from None
        except SandboxEffectError as error:
            _log_provider_failure("stop", error)
            await self._mark_stop_unknown(principal, reservation)
            raise SandboxAllocationServiceError("sandbox_stop_outcome_unknown") from None

    async def destroy(self, principal: Principal, allocation_id: UUID) -> AllocationReservation:
        try:
            async with asyncio.timeout(self._action_deadline_seconds):
                return await self._destroy_once(principal, allocation_id)
        except TimeoutError:
            raise SandboxAllocationServiceError("sandbox_destroy_outcome_unknown") from None

    async def _destroy_once(
        self, principal: Principal, allocation_id: UUID
    ) -> AllocationReservation:
        """Destroy only a stopped sandbox whose current session identity matches."""
        reservation = await self._get_for_observation(principal, allocation_id)
        if reservation.status in {"destroyed", "not_applied", "released"}:
            return reservation
        if reservation.status in {"running", "unknown", "creating", "stop_unknown"}:
            reservation = await self.stop(principal, allocation_id)
        if reservation.status == "destroy_unknown":
            return await self.observe(principal, allocation_id)
        if reservation.status != "stopped" or reservation.provider_session_id is None:
            return reservation
        expected_session_id = reservation.provider_session_id

        provider = self._provider_for(reservation)
        try:
            async with asyncio.timeout(self._action_deadline_seconds):
                async with provider.connection() as connected_provider:
                    allocation = await connected_provider.observe_exact_session(
                        reservation.provider_name,
                        expected_session_id,
                    )
                    if allocation is None:
                        return await self._record_empty_observation(principal, reservation)
                    if not _matches_persisted_identity(reservation, allocation):
                        await self._mark_observation_unknown(principal, reservation)
                        raise SandboxAllocationServiceError(
                            "sandbox_allocation_session_identity_conflict"
                        )
                    if not _is_whole_box_terminal(reservation, allocation):
                        await self._mark_observation_unknown(principal, reservation)
                        return await self._get_for_observation_or_default(principal, reservation)
                    reservation = await self._record_observation(
                        principal,
                        reservation,
                        allocation,
                        provider_period_id=self._provider_period_id,
                    )
                    if reservation.status != "stopped":
                        return reservation
                    authorized = await asyncio.to_thread(
                        self._store.authorize_destroy,
                        principal,
                        allocation_id,
                        expected_session_id,
                    )
                    if not authorized:
                        return await self._observe_with_provider(
                            principal, reservation, connected_provider
                        )
                    try:
                        await allocation.destroy()
                    except SandboxEffectError as error:
                        _log_provider_failure("destroy", error)
                        return await self._observe_after_destroy(
                            principal, reservation, connected_provider
                        )
                    except Exception as error:
                        _log_exception("destroy", error)
                        return await self._observe_after_destroy(
                            principal, reservation, connected_provider
                        )
                    return await self._observe_after_destroy(
                        principal, reservation, connected_provider
                    )
        except TimeoutError:
            raise SandboxAllocationServiceError("sandbox_destroy_outcome_unknown") from None
        except SandboxAllocationServiceError:
            raise
        except SandboxAllocationError as error:
            raise SandboxAllocationServiceError(error.code) from None
        except SandboxEffectError as error:
            _log_provider_failure("destroy", error)
            raise SandboxAllocationServiceError("sandbox_destroy_outcome_unknown") from None

    async def _observe_after_destroy(
        self,
        principal: Principal,
        reservation: AllocationReservation,
        provider: VercelSandboxProvider,
    ) -> AllocationReservation:
        try:
            allocation = await provider.observe(reservation.provider_name)
        except SandboxEffectError as error:
            _log_provider_failure("observe_after_destroy", error)
            return reservation
        if allocation is None:
            return await self._record_empty_observation(principal, reservation)
        if not _valid_allocation_handle(reservation, allocation):
            await self._mark_observation_unknown(principal, reservation)
            return reservation
        return await self._record_observation(
            principal,
            reservation,
            allocation,
            provider_period_id=self._provider_period_id,
        )

    async def _observe_existing(
        self, principal: Principal, reservation: AllocationReservation
    ) -> AllocationReservation:
        limits = self._limits_for(reservation)
        provider = self._provider_factory(limits)
        async with provider.connection() as connected_provider:
            return await self._observe_with_provider(principal, reservation, connected_provider)

    async def _get_for_observation(
        self, principal: Principal, allocation_id: UUID
    ) -> AllocationReservation:
        try:
            return await asyncio.to_thread(
                self._store.get_for_observation,
                principal,
                allocation_id,
            )
        except SandboxAllocationError as error:
            raise SandboxAllocationServiceError(error.code) from None

    async def _get_for_observation_or_default(
        self, principal: Principal, reservation: AllocationReservation
    ) -> AllocationReservation:
        try:
            return await asyncio.to_thread(
                self._store.get_for_observation,
                principal,
                reservation.allocation_id,
            )
        except SandboxAllocationError:
            return reservation

    def _provider_for(self, reservation: AllocationReservation) -> VercelSandboxProvider:
        try:
            return self._provider_factory(self._limits_for(reservation))
        except SandboxAllocationServiceError:
            raise
        except Exception:
            raise SandboxAllocationServiceError(
                "sandbox_allocation_provider_configuration_invalid"
            ) from None

    async def _observe_with_provider(
        self,
        principal: Principal,
        reservation: AllocationReservation,
        provider: VercelSandboxProvider,
    ) -> AllocationReservation:
        try:
            if reservation.provider_session_id is None:
                allocation = await provider.observe(reservation.provider_name)
            else:
                allocation = await provider.observe_exact_session(
                    reservation.provider_name,
                    reservation.provider_session_id,
                )
        except SandboxEffectError as error:
            _log_provider_failure("observe", error)
            return reservation
        if allocation is None:
            observed = await self._record_empty_observation(principal, reservation)
            if reservation.provider_session_id is not None and observed.status not in {
                "destroyed",
                "not_applied",
                "released",
            }:
                await self._mark_observation_unknown(principal, reservation)
                return await self._get_for_observation_or_default(principal, reservation)
            return observed
        if not _valid_allocation_handle(reservation, allocation):
            _LOGGER.warning("sandbox_allocation_provider_identity_mismatch")
            await self._mark_observation_unknown(principal, reservation)
            return await self._get_for_observation_or_default(principal, reservation)
        if (
            reservation.provider_session_id is not None
            and allocation.session_id != reservation.provider_session_id
        ):
            _LOGGER.warning("sandbox_allocation_session_identity_mismatch")
            await self._mark_observation_unknown(principal, reservation)
            return await self._get_for_observation_or_default(principal, reservation)
        return await self._record_observation(
            principal,
            reservation,
            allocation,
            provider_period_id=self._provider_period_id,
        )

    async def _record_empty_observation(
        self, principal: Principal, reservation: AllocationReservation
    ) -> AllocationReservation:
        try:
            return await asyncio.to_thread(
                self._store.record_observation,
                principal,
                reservation.allocation_id,
                None,
                self._provider_period_id,
            )
        except SandboxAllocationError as error:
            _log_persistence_failure("record_empty_observation", error)
            return reservation

    async def _record_observation(
        self,
        principal: Principal,
        reservation: AllocationReservation,
        allocation: VercelSandboxAllocation,
        *,
        provider_period_id: str | None,
    ) -> AllocationReservation:
        observation = allocation.observation()
        if not _is_whole_box_terminal(reservation, allocation):
            if (observation.session_status or "").casefold() == "stopped":
                _LOGGER.warning("sandbox_allocation_other_session_may_be_active")
                await self._mark_observation_unknown(principal, reservation)
                return await self._get_for_observation_or_default(principal, reservation)
        try:
            return await asyncio.to_thread(
                self._store.record_observation,
                principal,
                reservation.allocation_id,
                observation,
                provider_period_id,
            )
        except SandboxAllocationError as error:
            _log_persistence_failure("record_observation", error)
            if error.code == "sandbox_allocation_observation_conflict":
                await self._mark_observation_unknown(principal, reservation)
                return await self._get_for_observation_or_default(principal, reservation)
            return reservation

    async def _mark_create_unknown(
        self, principal: Principal, reservation: AllocationReservation
    ) -> None:
        try:
            await self._shielded_store_call(
                self._store.mark_create_unknown,
                principal,
                reservation.allocation_id,
            )
        except SandboxAllocationError as error:
            _log_persistence_failure("mark_create_unknown", error)

    async def _mark_observation_unknown(
        self, principal: Principal, reservation: AllocationReservation
    ) -> None:
        try:
            await asyncio.to_thread(
                self._store.mark_observation_unknown,
                principal,
                reservation.allocation_id,
            )
        except SandboxAllocationError as error:
            _log_persistence_failure("mark_observation_unknown", error)

    async def _mark_stop_unknown(
        self, principal: Principal, reservation: AllocationReservation
    ) -> None:
        try:
            await asyncio.to_thread(
                self._store.mark_stop_unknown,
                principal,
                reservation.allocation_id,
            )
        except SandboxAllocationError as error:
            _log_persistence_failure("mark_stop_unknown", error)

    async def _release_undispatched(
        self,
        principal: Principal,
        lease: ExecutionLease,
        reservation: AllocationReservation,
    ) -> None:
        try:
            await asyncio.to_thread(
                self._store.release_undispatched,
                principal,
                lease,
                reservation.allocation_id,
            )
        except (SandboxAllocationError, LeaseLost) as error:
            _log_error_type("release_undispatched", error)

    def _limits_for(self, reservation: AllocationReservation) -> SandboxAllocationLimits:
        if reservation.vcpus != ALLOCATION_VCPUS or reservation.memory_mb != ALLOCATION_MEMORY_MB:
            raise SandboxAllocationServiceError("sandbox_allocation_resource_envelope_invalid")
        try:
            return SandboxAllocationLimits(
                image=self._image,
                session_seconds=reservation.max_requested_seconds,
                vcpus=reservation.vcpus,
                memory_mb=reservation.memory_mb,
            )
        except ValueError:
            raise SandboxAllocationServiceError(
                "sandbox_allocation_runtime_limits_invalid"
            ) from None


def _request_sha256(
    logical_key: str,
    image: str,
    allocation_class: AllocationClass,
    requested_seconds: int,
) -> bytes:
    payload = {
        "allocation_class": allocation_class,
        "image": image,
        "logical_key": logical_key,
        "memory_mb": ALLOCATION_MEMORY_MB,
        "requested_seconds": requested_seconds,
        "vcpus": ALLOCATION_VCPUS,
    }
    content = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(content).digest()


def _allocation_tags(reservation: AllocationReservation) -> dict[str, str]:
    return {
        "continuum-allocation-id": str(reservation.allocation_id),
        "continuum-attempt-id": str(reservation.attempt_id),
        "continuum-generation": str(reservation.generation),
        "continuum-provider-period-id": reservation.provider_period_id,
    }


def _valid_allocation_handle(
    reservation: AllocationReservation, allocation: VercelSandboxAllocation
) -> bool:
    return allocation.name == reservation.provider_name


def _matches_persisted_identity(
    reservation: AllocationReservation, allocation: VercelSandboxAllocation
) -> bool:
    return (
        allocation.name == reservation.provider_name
        and allocation.session_id == reservation.provider_session_id
    )


def _is_whole_box_terminal(
    reservation: AllocationReservation, allocation: VercelSandboxAllocation
) -> bool:
    """Require stopped session and stopped box before freeing global capacity."""
    observation = allocation.observation()
    if (observation.session_status or "").casefold() != "stopped":
        return False
    if (observation.sandbox_status or "").casefold() != "stopped":
        return False
    current_session_id = allocation.current_session_id
    if reservation.provider_session_id is not None:
        if observation.session_id != reservation.provider_session_id:
            return False
        if current_session_id not in {None, reservation.provider_session_id}:
            return False
        current_status = (allocation.current_session_status or "").casefold()
        return current_session_id is None or current_status in {"", "stopped"}
    if observation.session_id is None:
        return current_session_id is None
    return current_session_id == observation.session_id


def _is_exact_current_session(
    reservation: AllocationReservation, allocation: VercelSandboxAllocation
) -> bool:
    """Allow a stop only when the persisted session is still the box's current one."""
    observation = allocation.observation()
    return (
        observation.session_id == reservation.provider_session_id
        and observation.session_id is not None
        and (observation.sandbox_status or "").casefold() == "running"
        and (observation.session_status or "").casefold() != "stopped"
        and allocation.current_session_id == reservation.provider_session_id
        and (allocation.current_session_status or "").casefold() != "stopped"
    )


def _log_provider_failure(operation: str, error: SandboxEffectError) -> None:
    _LOGGER.warning(
        "sandbox_provider_operation_uncertain operation=%s action=%s effect=%s reason=%s",
        operation,
        error.action.value,
        error.effect.value,
        error.reason,
    )


def _log_persistence_failure(operation: str, error: SandboxAllocationError) -> None:
    _LOGGER.warning(
        "sandbox_allocation_persistence_failed operation=%s code=%s",
        operation,
        error.code,
    )


def _log_exception(operation: str, error: Exception) -> None:
    _LOGGER.warning(
        "sandbox_provider_operation_failed operation=%s error_type=%s",
        operation,
        type(error).__name__,
    )


def _log_error_type(operation: str, error: Exception) -> None:
    _LOGGER.warning(
        "sandbox_allocation_persistence_failed operation=%s error_type=%s",
        operation,
        type(error).__name__,
    )
