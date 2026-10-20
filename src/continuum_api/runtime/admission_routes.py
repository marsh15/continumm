"""Authenticated HTTP boundary for public run admission."""

from __future__ import annotations

import copy
import inspect
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Annotated, Any, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from continuum_api.api_models import ErrorResponse
from continuum_api.runtime.admission import (
    AdmissionError,
    AdmissionResult,
    AdmissionService,
    FreeTextSource,
    IssueSource,
)
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.run_routes import PrincipalResolver
from continuum_api.settings import get_settings

ServiceFactory = Callable[[Request, Principal], AdmissionService]
CsrfGuard = Callable[[Request], bool | Awaitable[bool]]
_MAX_BODY_BYTES = 65_536


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _RequestModel(_StrictModel):
    source: Annotated[IssueSource | FreeTextSource, Field(discriminator="kind")]


class AdmissionResponse(_StrictModel):
    run_id: UUID
    status: Literal[
        "queued",
        "running",
        "waiting_approval",
        "waiting_credentials",
        "waiting_capacity",
        "needs_attention",
        "succeeded",
        "failed",
        "cancelled",
        "expired",
    ]
    created_at: datetime
    deadline_at: datetime
    profile_id: Literal["python-pytest", "node-npm"]
    profile_version: Literal["1"]
    provider: Literal["openai", "anthropic"]
    model_id: Literal["gpt-5.4-mini-2026-03-17", "claude-sonnet-5-5"]
    source_sha: str
    credential_id: UUID | None = None
    issue_id: int | None = None
    issue_sha: str | None = None
    replayed: bool


async def _read_body(request: Request) -> _RequestModel:
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > _MAX_BODY_BYTES:
            raise HTTPException(413, "request_body_too_large")
        body.extend(chunk)
    try:
        return _RequestModel.model_validate_json(bytes(body))
    except ValidationError:
        raise HTTPException(422, "validation_error") from None


def _request_schema() -> dict[str, Any]:
    schema = _RequestModel.model_json_schema()
    definitions = schema.pop("$defs", {})

    def inline(value: Any) -> Any:
        if isinstance(value, list):
            return [inline(item) for item in value]
        if not isinstance(value, dict):
            return value
        reference = value.get("$ref")
        if isinstance(reference, str) and reference.startswith("#/$defs/"):
            key = reference.removeprefix("#/$defs/")
            definition = definitions.get(key)
            if not isinstance(definition, dict):
                raise ValueError("admission_request_schema_reference_invalid")
            return inline(copy.deepcopy(definition))
        return {key: inline(item) for key, item in value.items() if key != "discriminator"}

    return cast(dict[str, Any], inline(schema))


def _response(result: AdmissionResult) -> AdmissionResponse:
    return AdmissionResponse(
        run_id=result.run_id,
        status=cast(
            Literal[
                "queued",
                "running",
                "waiting_approval",
                "waiting_credentials",
                "waiting_capacity",
                "needs_attention",
                "succeeded",
                "failed",
                "cancelled",
                "expired",
            ],
            result.status,
        ),
        created_at=result.created_at,
        deadline_at=result.deadline_at,
        profile_id=cast(Literal["python-pytest", "node-npm"], result.profile_id),
        profile_version=cast(Literal["1"], result.profile_version),
        provider=cast(Literal["openai", "anthropic"], result.provider),
        model_id=cast(Literal["gpt-5.4-mini-2026-03-17", "claude-sonnet-5-5"], result.model_id),
        source_sha=result.source_sha,
        credential_id=result.credential_id,
        issue_id=result.issue_id,
        issue_sha=result.issue_sha,
        replayed=result.replayed,
    )


def _status(code: str) -> int:
    if code in {"repository_not_found", "account_unavailable", "idempotency_result_expired"}:
        return 404 if code == "repository_not_found" else 409
    if code in {
        "github_user_token_unavailable",
        "github_identity_mismatch",
        "repository_actor_write_access_required",
        "github_installation_user_access_required",
        "github_authorization_invalid",
        "repository_must_be_public",
    }:
        return 403
    if code in {
        "admission_source_invalid",
        "admission_task_invalid",
        "idempotency_key_invalid",
    }:
        return 400
    if code in {
        "repository_provider_default_required",
        "repository_profile_default_required",
        "repository_not_eligible",
        "repository_eligibility_corrupt",
        "repository_default_branch_invalid",
        "credential_unavailable",
        "github_issue_verification_unavailable",
        "setup_continuation_time_unavailable",
    }:
        return 422
    if code in {
        "idempotency_key_conflict",
        "submission_request_id_conflict",
        "repository_defaults_changed",
        "repository_eligibility_stale",
        "active_issue_input_conflict",
        "active_issue_exists",
        "account_active_run_exists",
        "setup_continuation_identity_conflict",
        "setup_continuation_already_claimed",
    }:
        return 409
    if code == "global_queue_full":
        return 429
    if code == "public_admission_disabled":
        return 503
    return 503


def create_admission_router(
    principal_resolver: PrincipalResolver,
    service_factory: ServiceFactory,
    csrf_guard: CsrfGuard,
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/runs", tags=["runs"])

    async def authenticated_principal(request: Request) -> Principal:
        try:
            resolved = principal_resolver(request)
            principal = await resolved if inspect.isawaitable(resolved) else resolved
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(401, "authentication_required") from None
        if not isinstance(principal, Principal) or not isinstance(principal.account_id, UUID):
            raise HTTPException(401, "authentication_required")
        return principal

    async def require_csrf(request: Request) -> None:
        token = request.headers.get("x-csrf-token")
        origin = request.headers.get("origin")
        if not token or len(token) > 512 or not origin:
            raise HTTPException(403, "csrf_failed")
        try:
            allowed = csrf_guard(request)
            if inspect.isawaitable(allowed):
                allowed = await allowed
        except Exception:
            allowed = False
        if allowed is not True:
            raise HTTPException(403, "csrf_failed")

    @router.post(
        "",
        status_code=202,
        response_model=AdmissionResponse,
        responses={
            code: {"model": ErrorResponse} for code in (400, 401, 403, 404, 409, 413, 422, 429, 503)
        },
        openapi_extra={
            "parameters": [
                {
                    "name": "Idempotency-Key",
                    "in": "header",
                    "required": True,
                    "schema": {"type": "string", "minLength": 16, "maxLength": 128},
                }
            ],
            "requestBody": {
                "required": True,
                "content": {"application/json": {"schema": _request_schema()}},
            },
        },
    )
    async def create_run(
        request: Request,
        response: Response,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
    ) -> AdmissionResponse:
        await require_csrf(request)
        if get_settings().enable_public_admission is not True:
            raise HTTPException(503, "public_admission_disabled")
        idempotency_key = request.headers.get("idempotency-key")
        if idempotency_key is None:
            raise HTTPException(400, "idempotency_key_required")
        try:
            key_bytes = idempotency_key.encode("ascii")
        except UnicodeEncodeError:
            raise HTTPException(400, "idempotency_key_invalid") from None
        if not 16 <= len(key_bytes) <= 128 or any(byte < 0x21 or byte > 0x7E for byte in key_bytes):
            raise HTTPException(400, "idempotency_key_invalid")
        body = await _read_body(request)
        try:
            result = await service_factory(request, principal).admit(body.source, idempotency_key)
        except AdmissionError as error:
            raise HTTPException(_status(error.code), error.code) from None
        response.headers["Cache-Control"] = "private, no-store"
        return _response(result)

    return router
