"""Authenticated API routes for current review snapshots and decisions."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Coroutine
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field

from continuum_api.api_models import ErrorResponse
from continuum_api.runtime.approvals import ApprovalError, ApprovalNotFound
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.review import (
    MAX_REVIEW_BODY_BYTES,
    CsrfGuard,
    ReviewDecisionReceipt,
    ReviewResponse,
    ReviewService,
)
from continuum_api.runtime.run_routes import PrincipalResolver

ReviewServiceFactory = Callable[[Request, Principal], ReviewService]


class ReviewDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    decision: Literal["approve", "reject", "changes"]
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    feedback: str | None = Field(default=None, max_length=16_384)


class _BoundedDecisionRoute(APIRoute):
    """Reject oversized decision bodies before FastAPI decodes JSON."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original = super().get_route_handler()

        async def bounded_handler(request: Request) -> Response:
            if request.method == "POST" and request.url.path.endswith("/decision"):
                content_length = request.headers.get("content-length")
                if content_length is not None:
                    try:
                        declared_size = int(content_length)
                    except ValueError:
                        raise HTTPException(
                            status_code=400, detail="content_length_invalid"
                        ) from None
                    if declared_size < 0:
                        raise HTTPException(status_code=400, detail="content_length_invalid")
                    if declared_size > MAX_REVIEW_BODY_BYTES:
                        raise HTTPException(status_code=413, detail="request_too_large")
                body = bytearray()
                async for chunk in request.stream():
                    if len(body) + len(chunk) > MAX_REVIEW_BODY_BYTES:
                        raise HTTPException(status_code=413, detail="request_too_large")
                    body.extend(chunk)
                request._body = bytes(body)
            return await original(request)

        return bounded_handler


def create_review_router(
    principal_resolver: PrincipalResolver | None,
    service_factory: ReviewServiceFactory | None,
    csrf_guard: CsrfGuard | None,
) -> APIRouter:
    router = APIRouter(tags=["review"], route_class=_BoundedDecisionRoute)

    async def authenticated_principal(request: Request) -> Principal:
        if principal_resolver is None:
            raise HTTPException(status_code=401, detail="authentication_required")
        try:
            resolved = principal_resolver(request)
            principal = await resolved if inspect.isawaitable(resolved) else resolved
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=401, detail="authentication_required") from None
        if not isinstance(principal, Principal) or not isinstance(principal.account_id, UUID):
            raise HTTPException(status_code=401, detail="authentication_required")
        return principal

    def review_service(request: Request, principal: Principal) -> ReviewService:
        if service_factory is None:
            raise HTTPException(status_code=503, detail="review_service_unavailable")
        try:
            return service_factory(request, principal)
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=503, detail="review_service_unavailable") from None

    async def require_csrf(request: Request) -> None:
        token = request.headers.get("x-csrf-token")
        if not token or len(token) > 512 or not request.headers.get("origin") or csrf_guard is None:
            raise HTTPException(status_code=403, detail="csrf_failed")
        try:
            allowed = csrf_guard(request)
            allowed = await allowed if inspect.isawaitable(allowed) else allowed
        except Exception:
            allowed = False
        if allowed is not True:
            raise HTTPException(status_code=403, detail="csrf_failed")

    @router.get(
        "/api/v1/runs/{run_id}/review",
        response_model=ReviewResponse,
        responses={
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            409: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    async def get_review(
        run_id: UUID,
        request: Request,
        response: Response,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
    ) -> ReviewResponse:
        response.headers["Cache-Control"] = "private, no-store"
        try:
            return await review_service(request, principal).get_review(run_id)
        except ApprovalNotFound:
            raise HTTPException(status_code=404, detail="not_found") from None
        except ApprovalError as error:
            raise _approval_http_error(error) from None

    @router.post(
        "/api/v1/approvals/{approval_id}/decision",
        response_model=ReviewDecisionReceipt,
        status_code=202,
        responses={
            400: {"model": ErrorResponse},
            401: {"model": ErrorResponse},
            403: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            409: {"model": ErrorResponse},
            413: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    async def decide_review(
        approval_id: UUID,
        request: Request,
        body: ReviewDecisionRequest,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
        idempotency_key: str | None = Header(
            default=None,
            alias="Idempotency-Key",
            max_length=128,
            pattern=r"^[A-Za-z0-9._:-]{1,128}$",
        ),
    ) -> ReviewDecisionReceipt:
        if idempotency_key is None:
            raise HTTPException(status_code=400, detail="idempotency_key_required")
        await require_csrf(request)
        try:
            service = review_service(request, principal)
            return await service.decide(
                approval_id,
                body.manifest_sha256,
                body.decision,
                feedback=body.feedback,
                idempotency_key=idempotency_key,
            )
        except ApprovalNotFound:
            raise HTTPException(status_code=404, detail="not_found") from None
        except ApprovalError as error:
            raise _approval_http_error(error) from None

    return router


def _approval_http_error(error: ApprovalError) -> HTTPException:
    code = error.code
    status = {
        "approval_expired": 410,
        "review_content_expired": 410,
        "repository_authorization_pending": 503,
        "review_candidate_restore_failed": 503,
        "repository_access_denied": 403,
        "github_user_token_unavailable": 403,
        "github_identity_mismatch": 403,
        "repository_actor_write_access_required": 403,
        "github_installation_user_access_required": 403,
        "github_authorization_timeout": 503,
        "github_api_unavailable": 503,
        "github_api_request_failed": 503,
        "approval_owner_mismatch": 404,
        "approval_not_found": 404,
        "approval_digest_invalid": 400,
        "approval_feedback_required": 400,
        "approval_feedback_invalid": 400,
        "approval_feedback_unexpected": 400,
        "approval_idempotency_key_invalid": 400,
    }.get(code, 409)
    return HTTPException(status_code=status, detail=code)
