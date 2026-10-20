"""CSRF-protected owner controls for run and account content deletion."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict

from continuum_api.api_models import ErrorResponse
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.retention import DeletionReceipt, RetentionError, RetentionService
from continuum_api.runtime.run_routes import PrincipalResolver

RetentionServiceFactory = Callable[[Request, Principal], RetentionService]
CsrfGuard = Callable[[Request], bool | Awaitable[bool]]


class DeletionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    scope: str
    subject_id: UUID
    accepted: bool
    state: str
    cleanup_pending: int


def _response(receipt: DeletionReceipt) -> DeletionResponse:
    return DeletionResponse(
        scope=receipt.scope,
        subject_id=receipt.subject_id,
        accepted=receipt.accepted,
        state=receipt.state,
        cleanup_pending=receipt.cleanup_pending,
    )


def create_retention_router(
    principal_resolver: PrincipalResolver | None,
    service_factory: RetentionServiceFactory | None,
    csrf_guard: CsrfGuard | None,
) -> APIRouter:
    router = APIRouter(tags=["retention"])

    async def authenticated(request: Request) -> Principal:
        if principal_resolver is None:
            raise HTTPException(401, "authentication_required")
        try:
            principal = principal_resolver(request)
            principal = await principal if inspect.isawaitable(principal) else principal
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(401, "authentication_required") from None
        if not isinstance(principal, Principal):
            raise HTTPException(401, "authentication_required")
        return principal

    async def require_csrf(request: Request) -> None:
        # Origin is checked by the injected browser CSRF boundary; requiring
        # both headers here makes the endpoint fail closed with test adapters.
        if not request.headers.get("origin") or not request.headers.get("x-csrf-token"):
            raise HTTPException(403, "csrf_failed")
        if csrf_guard is None:
            raise HTTPException(403, "csrf_failed")
        try:
            allowed = csrf_guard(request)
            allowed = await allowed if inspect.isawaitable(allowed) else allowed
        except Exception:
            allowed = False
        if allowed is not True:
            raise HTTPException(403, "csrf_failed")

    def service(request: Request, principal: Principal) -> RetentionService:
        if service_factory is None:
            raise HTTPException(503, "retention_service_unavailable")
        try:
            return service_factory(request, principal)
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(503, "retention_service_unavailable") from None

    errors: dict[int | str, dict[str, Any]] = {
        code: {"model": ErrorResponse} for code in (401, 403, 404, 503)
    }

    @router.delete(
        "/api/v1/runs/{run_id}",
        status_code=202,
        response_model=DeletionResponse,
        responses=errors,
    )
    async def delete_run(
        run_id: UUID,
        request: Request,
        response: Response,
        principal: Principal = Depends(authenticated),  # noqa: B008
    ) -> DeletionResponse:
        await require_csrf(request)
        response.headers["Cache-Control"] = "private, no-store"
        try:
            receipt = await asyncio.to_thread(service(request, principal).delete_run, run_id)
        except RetentionError as error:
            if error.code == "run_not_found":
                raise HTTPException(404, "not_found") from None
            raise HTTPException(503, "retention_storage_unavailable") from None
        return _response(receipt)

    @router.delete("/api/v1/me", status_code=202, response_model=DeletionResponse, responses=errors)
    async def delete_account(
        request: Request,
        response: Response,
        principal: Principal = Depends(authenticated),  # noqa: B008
    ) -> DeletionResponse:
        await require_csrf(request)
        response.headers["Cache-Control"] = "private, no-store"
        try:
            receipt = await asyncio.to_thread(service(request, principal).delete_account)
        except RetentionError:
            raise HTTPException(503, "retention_storage_unavailable") from None
        return _response(receipt)

    return router
