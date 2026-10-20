"""Authenticated, allowlisted read-only operator metadata API."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from datetime import datetime
from typing import Any, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine

from continuum_api.api_models import ErrorResponse
from continuum_api.runtime.operator_actions import (
    OperatorActionError,
    OperatorActionService,
    OperatorAuditEvent,
    OperatorOverview,
    PendingInboxDelivery,
    SuspendedAccount,
    UncertainAllocation,
    UnknownEffect,
    create_operator_engine,
    parse_operator_allowlist,
    require_operator_actor,
)
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.run_routes import PrincipalResolver

OperatorServiceFactory = Callable[[Request, Principal], OperatorActionService]
AllowlistResolver = Callable[[], frozenset[int]]


class OperatorOverviewResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    admissions_stopped: bool
    active_runs: int
    unknown_effects: int
    uncertain_allocations: int
    pending_inbox_deliveries: int
    oldest_pending_inbox_at: datetime | None
    artifact_charged_bytes: int
    artifact_capacity_limit_bytes: int
    suspended_accounts: int


class UnknownEffectResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    effect_id: UUID
    owner_id: UUID
    run_id: UUID
    status: str
    created_at: datetime
    updated_at: datetime


class UncertainAllocationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    allocation_id: UUID
    owner_id: UUID
    run_id: UUID
    status: str
    provider_name: str
    created_at: datetime
    last_observed_at: datetime | None


class PendingInboxResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    delivery_id: UUID
    event_type: str
    received_at: datetime


class SuspendedAccountResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    user_id: UUID
    github_user_id: int
    created_at: datetime


class OperatorAuditResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    audit_id: UUID
    actor_github_user_id: int
    action: str
    target_user_id: UUID | None
    reason_code: str | None
    created_at: datetime


def create_operator_router(
    principal_resolver: PrincipalResolver | None,
    service_factory: OperatorServiceFactory | None,
    allowlist_resolver: AllowlistResolver,
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/operator", tags=["operator"])
    errors: dict[int | str, dict[str, Any]] = {
        code: {"model": ErrorResponse} for code in (401, 404, 503)
    }

    async def authorized_service(request: Request) -> OperatorActionService:
        if principal_resolver is None:
            raise HTTPException(401, "authentication_required")
        try:
            result = principal_resolver(request)
            principal = await result if inspect.isawaitable(result) else result
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(401, "authentication_required") from None
        if not isinstance(principal, Principal) or not isinstance(principal.account_id, UUID):
            raise HTTPException(401, "authentication_required")
        identity = getattr(request.state, "github_identity", None)
        actor_id = getattr(identity, "github_user_id", None)
        if (
            not isinstance(actor_id, int)
            or isinstance(actor_id, bool)
            or actor_id <= 0
            or getattr(getattr(identity, "principal", None), "account_id", None)
            != principal.account_id
        ):
            raise HTTPException(401, "authentication_required")
        try:
            allowlist = allowlist_resolver()
        except Exception:
            raise HTTPException(503, "operator_unavailable") from None
        if actor_id not in allowlist:
            # Do not reveal operator routes to ordinary signed-in accounts.
            raise HTTPException(404, "not_found")
        try:
            require_operator_actor(actor_id, allowlist)
        except OperatorActionError:
            raise HTTPException(404, "not_found") from None
        if service_factory is None:
            raise HTTPException(503, "operator_unavailable")
        try:
            return service_factory(request, principal)
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(503, "operator_unavailable") from None

    async def invoke(method: Callable[..., Any], *args: Any) -> Any:
        try:
            return await asyncio.to_thread(method, *args)
        except OperatorActionError as error:
            if error.code == "operator_limit_invalid":
                raise HTTPException(422, error.code) from None
            raise HTTPException(503, "operator_unavailable") from None

    @router.get("/overview", response_model=OperatorOverviewResponse, responses=errors)
    async def overview(request: Request) -> OperatorOverviewResponse:
        service = await authorized_service(request)
        value: OperatorOverview = await invoke(service.overview)
        return OperatorOverviewResponse(**_fields(value))

    @router.get(
        "/unknown-effects", response_model=tuple[UnknownEffectResponse, ...], responses=errors
    )
    async def unknown_effects(
        request: Request, limit: int = Query(50, ge=1, le=100)
    ) -> tuple[UnknownEffectResponse, ...]:
        service = await authorized_service(request)
        items: tuple[UnknownEffect, ...] = await invoke(service.unknown_effects, limit)
        return tuple(UnknownEffectResponse(**_fields(item)) for item in items)

    @router.get(
        "/uncertain-allocations",
        response_model=tuple[UncertainAllocationResponse, ...],
        responses=errors,
    )
    async def uncertain_allocations(
        request: Request, limit: int = Query(50, ge=1, le=100)
    ) -> tuple[UncertainAllocationResponse, ...]:
        service = await authorized_service(request)
        items: tuple[UncertainAllocation, ...] = await invoke(service.uncertain_allocations, limit)
        return tuple(UncertainAllocationResponse(**_fields(item)) for item in items)

    @router.get("/inbox-lag", response_model=tuple[PendingInboxResponse, ...], responses=errors)
    async def inbox_lag(
        request: Request, limit: int = Query(50, ge=1, le=100)
    ) -> tuple[PendingInboxResponse, ...]:
        service = await authorized_service(request)
        items: tuple[PendingInboxDelivery, ...] = await invoke(service.pending_inbox, limit)
        return tuple(PendingInboxResponse(**_fields(item)) for item in items)

    @router.get(
        "/suspended-accounts", response_model=tuple[SuspendedAccountResponse, ...], responses=errors
    )
    async def suspended_accounts(
        request: Request, limit: int = Query(50, ge=1, le=100)
    ) -> tuple[SuspendedAccountResponse, ...]:
        service = await authorized_service(request)
        items: tuple[SuspendedAccount, ...] = await invoke(service.suspended_accounts, limit)
        return tuple(SuspendedAccountResponse(**_fields(item)) for item in items)

    @router.get("/audit", response_model=tuple[OperatorAuditResponse, ...], responses=errors)
    async def audit_events(
        request: Request, limit: int = Query(50, ge=1, le=100)
    ) -> tuple[OperatorAuditResponse, ...]:
        service = await authorized_service(request)
        items: tuple[OperatorAuditEvent, ...] = await invoke(service.audit_events, limit)
        return tuple(OperatorAuditResponse(**_fields(item)) for item in items)

    return router


def settings_allowlist() -> frozenset[int]:
    from continuum_api.settings import get_settings

    return parse_operator_allowlist(get_settings().operator_github_user_ids)


def get_operator_engine(request: Request) -> Engine:
    """Create the separate operator connection only when an allowlisted request arrives."""
    configured = getattr(request.app.state, "operator_engine", None)
    if configured is not None:
        return cast(Engine, configured)
    lock = request.app.state.operator_engine_lock
    with lock:
        configured = getattr(request.app.state, "operator_engine", None)
        if configured is not None:
            return cast(Engine, configured)
        from continuum_api.settings import get_settings

        database_url = get_settings().operator_database_url
        if database_url is None or not database_url.get_secret_value().strip():
            raise HTTPException(503, "operator_unavailable")
        try:
            configured = create_operator_engine(database_url.get_secret_value())
        except OperatorActionError:
            raise HTTPException(503, "operator_unavailable") from None
        request.app.state.operator_engine = configured
        request.app.state.owns_operator_engine = True
        return configured


def _fields(value: Any) -> dict[str, Any]:
    return {field: getattr(value, field) for field in value.__dataclass_fields__}
