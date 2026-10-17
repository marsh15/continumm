"""Authenticated API for unresolved external effects and safe PR reconciliation."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import datetime
from typing import Any, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Engine

from continuum_api.api_models import ErrorResponse
from continuum_api.runtime.external_attention import (
    ExternalAttentionItem,
    ExternalAttentionService,
    PullRequestState,
)
from continuum_api.runtime.github_pr_publication import (
    GitHubPullRequestPublicationError,
    GitHubPullRequestService,
)
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.run_routes import PrincipalResolver, get_runtime_engine

ReconcileStatus = Literal[
    "adopted",
    "closed",
    "merged",
    "unknown",
    "conflict",
    "unavailable",
    "not_dispatched",
    "not_started",
    "definitely_failed",
]
CsrfGuard = Callable[[Request], bool | Any]
ExternalAttentionServiceFactory = Callable[
    [Request, Principal], tuple[ExternalAttentionService, GitHubPullRequestService]
]


class AttentionEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    pr_number: int | None = None
    pr_id: int | None = None
    pr_url: str | None = None
    pr_state: Literal["open", "closed", "merged"] | None = None


class AttentionEffect(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    kind: Literal["external_operation", "dispatch_intent", "resource_reservation", "cleanup_intent"]
    effect_id: UUID
    status: Literal["prepared", "dispatching", "unknown", "conflict", "reserved", "pending"]
    created_at: datetime
    updated_at: datetime | None = None
    can_reconcile: bool
    evidence: AttentionEvidence


class ExternalAttentionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    run_id: UUID
    effects: tuple[AttentionEffect, ...]
    truncated: bool


class ReconcileResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    effect_id: UUID
    status: ReconcileStatus
    pr_number: int | None = None
    pr_id: int | None = None
    pr_url: str | None = None
    pr_state: Literal["open", "closed", "merged"] | None = None


def create_external_attention_router(
    principal_resolver: PrincipalResolver | None,
    csrf_guard: CsrfGuard | None,
    service_factory: ExternalAttentionServiceFactory | None,
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/runs", tags=["external-effects"])

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

    async def require_csrf(request: Request) -> None:
        if (
            csrf_guard is None
            or not request.headers.get("origin")
            or not request.headers.get("x-csrf-token")
            or len(request.headers.get("x-csrf-token", "")) > 512
        ):
            raise HTTPException(status_code=403, detail="csrf_failed")
        try:
            allowed = csrf_guard(request)
            allowed = await allowed if inspect.isawaitable(allowed) else allowed
        except Exception:
            allowed = False
        if allowed is not True:
            raise HTTPException(status_code=403, detail="csrf_failed")

    def services(
        request: Request, principal: Principal
    ) -> tuple[ExternalAttentionService, GitHubPullRequestService]:
        if service_factory is None:
            raise HTTPException(status_code=503, detail="external_attention_unavailable")
        try:
            return service_factory(request, principal)
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=503, detail="external_attention_unavailable") from None

    @router.get(
        "/{run_id}/external-effects",
        response_model=ExternalAttentionResponse,
        responses={
            401: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    def list_external_effects(
        run_id: UUID,
        response: Response,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
        engine: Engine = Depends(get_runtime_engine),  # noqa: B008
    ) -> ExternalAttentionResponse:
        response.headers["Cache-Control"] = "private, no-store"
        try:
            page = ExternalAttentionService(engine, principal).list_for_run(run_id)
        except Exception:
            raise HTTPException(status_code=503, detail="runtime_database_unavailable") from None
        if page is None:
            raise HTTPException(status_code=404, detail="not_found")
        return ExternalAttentionResponse(
            run_id=run_id,
            effects=tuple(_effect_response(item) for item in page.effects),
            truncated=page.has_more,
        )

    @router.post(
        "/{run_id}/external-effects/{effect_id}/reconcile",
        response_model=ReconcileResponse,
        responses={
            401: {"model": ErrorResponse},
            403: {"model": ErrorResponse},
            404: {"model": ErrorResponse},
            409: {"model": ErrorResponse},
            503: {"model": ErrorResponse},
        },
    )
    async def reconcile_external_effect(
        run_id: UUID,
        effect_id: UUID,
        request: Request,
        response: Response,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
    ) -> ReconcileResponse:
        response.headers["Cache-Control"] = "private, no-store"
        await require_csrf(request)
        async for chunk in request.stream():
            if chunk:
                raise HTTPException(status_code=400, detail="request_body_invalid")
        attention, publication = services(request, principal)
        try:
            binding = attention.get_pr_operation(run_id, effect_id)
        except Exception:
            raise HTTPException(status_code=503, detail="runtime_database_unavailable") from None
        if binding is None:
            raise HTTPException(status_code=404, detail="not_found")
        approval_id, digest = binding
        try:
            receipt = await publication.reconcile(approval_id, digest)
        except GitHubPullRequestPublicationError as error:
            if error.code.endswith("not_found") or error.code.endswith("owner_mismatch"):
                raise HTTPException(status_code=404, detail="not_found") from None
            if "conflict" in error.code:
                raise HTTPException(status_code=409, detail="external_effect_conflict") from None
            raise HTTPException(
                status_code=503, detail="external_reconciliation_unavailable"
            ) from None
        return ReconcileResponse(
            effect_id=effect_id,
            status=cast(ReconcileStatus, receipt.status),
            pr_number=receipt.pr_number,
            pr_id=receipt.pr_id,
            pr_url=receipt.html_url if _safe_pr_url(receipt.html_url, receipt.pr_number) else None,
            pr_state=(
                cast(PullRequestState, receipt.remote_state)
                if receipt.remote_state in {"open", "closed", "merged"}
                else None
            ),
        )

    return router


def _effect_response(item: ExternalAttentionItem) -> AttentionEffect:
    return AttentionEffect(
        kind=item.kind,
        effect_id=item.effect_id,
        status=item.status,
        created_at=item.created_at,
        updated_at=item.updated_at,
        can_reconcile=item.can_reconcile,
        evidence=AttentionEvidence(
            pr_number=item.pr_number,
            pr_id=item.pr_id,
            pr_url=item.pr_url,
            pr_state=item.pr_state,
        ),
    )


def _safe_pr_url(url: str | None, number: int | None) -> bool:
    if not url or type(number) is not int or number <= 0:
        return False
    from urllib.parse import urlsplit

    parsed = urlsplit(url)
    return (
        parsed.scheme == "https"
        and parsed.netloc == "github.com"
        and parsed.query == ""
        and parsed.fragment == ""
        and parsed.path.endswith(f"/pull/{number}")
        and len(parsed.path.split("/")) == 5
    )
