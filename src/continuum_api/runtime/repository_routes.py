"""Owner-scoped repository onboarding and saved credential metadata routes."""

from __future__ import annotations

import asyncio
import builtins
import inspect
import json
import re
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import Engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.credentials import CredentialScope
from continuum_api.repositories.preflight import (
    BUILD_PROFILES,
    EligibilityReceipt,
    RepositoryPreflightError,
    RepositoryPreflightService,
)
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.runtime.provider_credentials import CredentialMetadata, CredentialService
from continuum_api.runtime.setup_continuations import (
    SetupContinuationError,
    SetupContinuationService,
)

PrincipalResolver = Callable[[Request], Principal | Awaitable[Principal]]
CsrfGuard = Callable[[Request], bool | Awaitable[bool]]
AuthorizationFactory = Callable[[Principal], Any]
_MAX_BODY_BYTES = 8192
_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _RegisterBody(_StrictModel):
    github_repository_id: Annotated[int, Field(gt=0, le=9_223_372_036_854_775_807)]
    profile_id: Annotated[str, Field(min_length=1, max_length=32)]
    provider: Literal["openai", "anthropic"] | None = None
    credential_id: UUID | None = None


class _PatchBody(_StrictModel):
    profile_id: Annotated[str, Field(min_length=1, max_length=32)] | None = None
    provider: Literal["openai", "anthropic"] | None = None
    credential_id: UUID | None = None


class _CheckBody(_StrictModel):
    commit_sha: Annotated[str, Field(min_length=40, max_length=64)]


class EligibilitySummary(_StrictModel):
    status: Literal["unchecked", "eligible", "ineligible", "error"]
    checked_sha: str | None
    checked_at: datetime | None
    error: str | None


class RepositoryRegistrationResponse(_StrictModel):
    id: int
    github_repository_id: int
    owner: str
    name: str
    full_name: str
    profile_id: Literal["python-pytest", "node-npm"]
    profile_version: Literal["1"]
    provider: Literal["openai", "anthropic"] | None
    credential_id: UUID | None
    eligibility: EligibilitySummary


class RepositoryListResponse(_StrictModel):
    repositories: list[RepositoryRegistrationResponse]


class SavedCredentialResponse(_StrictModel):
    credential_id: UUID
    provider: Literal["openai", "anthropic"]
    scope: Literal["saved"]
    status: Literal["active"]
    created_at: datetime
    expires_at: datetime | None


class CredentialListResponse(_StrictModel):
    credentials: list[SavedCredentialResponse]


class EligibilityCheckResponse(_StrictModel):
    status: Literal["eligible"]
    checked_sha: str
    checked_at: datetime
    profile_id: Literal["python-pytest", "node-npm"]
    profile_version: Literal["1"]
    package_count: int
    file_count: int
    source_bytes: int
    cache_hit: bool


async def _bounded_model(request: Request, model: type[BaseModel]) -> BaseModel:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
            if declared_length < 0:
                raise HTTPException(400, "request_body_invalid")
            if declared_length > _MAX_BODY_BYTES:
                raise HTTPException(413, "request_body_too_large")
        except ValueError:
            raise HTTPException(400, "request_body_invalid") from None
    body = bytearray()
    async for chunk in request.stream():
        if len(chunk) > _MAX_BODY_BYTES - len(body):
            raise HTTPException(413, "request_body_too_large")
        body.extend(chunk)
    try:
        return model.model_validate_json(bytes(body))
    except ValidationError:
        raise HTTPException(422, "validation_error") from None


class RepositoryOnboardingService:
    """Persist registrations and defaults with explicit principal predicates and RLS."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        authorization: Any,
        preflight: RepositoryPreflightService,
        credentials: CredentialService,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._authorization = authorization
        self._preflight = preflight
        self._credentials = credentials

    @contextmanager
    def _connection(self) -> Iterator[Connection]:
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                yield connection
        except SQLAlchemyError:
            raise HTTPException(503, "repository_storage_unavailable") from None

    def list(self) -> list[dict[str, Any]]:
        try:
            SetupContinuationService(self._engine, self._principal).sweep_expired()
        except SetupContinuationError:
            raise HTTPException(503, "repository_storage_unavailable") from None
        with self._connection() as connection:
            rows = connection.execute(
                text(
                    "SELECT registration_id, github_repository_id, repository_owner, "
                    "repository_name, profile_id, profile_version, provider, credential_id, "
                    "eligibility_status, checked_sha, eligibility_checked_at, eligibility_error "
                    "FROM runtime_private.repository_registrations WHERE owner_id=:owner "
                    "ORDER BY updated_at DESC, registration_id"
                ),
                {"owner": self._principal.account_id},
            ).mappings()
            return [_registration_response(dict(row)) for row in rows]

    def list_credentials(self) -> builtins.list[dict[str, Any]]:
        return [
            _credential_response(item)
            for item in self._credentials.list_metadata()
            if item.scope is CredentialScope.SAVED and item.status == "active"
        ]

    async def register(
        self,
        *,
        repository_id: int,
        profile_id: str,
        provider: str | None,
        credential_id: UUID | None,
    ) -> dict[str, Any]:
        profile = BUILD_PROFILES.get(profile_id)
        if profile is None:
            raise HTTPException(422, "repository_profile_unsupported")
        await asyncio.to_thread(self._validate_credential, provider, credential_id)
        github_user_id = await asyncio.to_thread(self._github_user_id)
        facts = await self._current_repository(repository_id, github_user_id)
        self._validate_facts(facts, repository_id, github_user_id)
        row = await asyncio.to_thread(
            self._upsert_registration,
            repository_id,
            facts.owner,
            facts.name,
            profile.profile_id,
            profile.version,
            provider,
            credential_id,
        )
        return _registration_response(dict(row))

    def _upsert_registration(
        self,
        repository_id: int,
        owner: str,
        name: str,
        profile_id: str,
        profile_version: str,
        provider: str | None,
        credential_id: UUID | None,
    ) -> Any:
        with self._connection() as connection:
            self._lock_saved_credential(connection, provider, credential_id)
            return (
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.repository_registrations "
                        "(owner_id, github_repository_id, repository_owner, repository_name, "
                        "profile_id, profile_version, provider, credential_id) VALUES "
                        "(:owner, :repo, :repo_owner, :repo_name, :profile, :version, "
                        ":provider, :credential) "
                        "ON CONFLICT (owner_id, github_repository_id) DO UPDATE SET "
                        "repository_owner=EXCLUDED.repository_owner, "
                        "repository_name=EXCLUDED.repository_name, profile_id=EXCLUDED.profile_id, "
                        "profile_version=EXCLUDED.profile_version, provider=EXCLUDED.provider, "
                        "credential_id=EXCLUDED.credential_id, "
                        "defaults_version=repository_registrations.defaults_version + 1, "
                        "eligibility_status='unchecked', checked_sha=NULL, "
                        "eligibility_checked_at=NULL, eligibility_error=NULL, "
                        "eligibility_receipt=NULL, updated_at=clock_timestamp() "
                        "RETURNING registration_id, github_repository_id, repository_owner, "
                        "repository_name, profile_id, profile_version, provider, credential_id, "
                        "eligibility_status, checked_sha, eligibility_checked_at, eligibility_error"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "repo": repository_id,
                        "repo_owner": owner,
                        "repo_name": name,
                        "profile": profile_id,
                        "version": profile_version,
                        "provider": provider,
                        "credential": credential_id,
                    },
                )
                .mappings()
                .one()
            )

    async def update(
        self,
        registration_id: int,
        values: _PatchBody,
    ) -> dict[str, Any]:
        if not values.model_fields_set:
            raise HTTPException(422, "validation_error")
        existing = await asyncio.to_thread(self._select_registration, registration_id)
        if existing is None:
            raise HTTPException(404, "not_found")

        github_user_id = await asyncio.to_thread(self._github_user_id)
        facts = await self._current_repository(existing["github_repository_id"], github_user_id)
        self._validate_facts(facts, existing["github_repository_id"], github_user_id)

        provider = (
            values.provider if "provider" in values.model_fields_set else existing["provider"]
        )
        credential_id = (
            values.credential_id
            if "credential_id" in values.model_fields_set
            else existing["credential_id"]
        )
        await asyncio.to_thread(self._validate_credential, provider, credential_id)
        profile_id = (
            values.profile_id if "profile_id" in values.model_fields_set else existing["profile_id"]
        )
        profile = BUILD_PROFILES.get(profile_id) if profile_id is not None else None
        if profile is None:
            raise HTTPException(422, "repository_profile_unsupported")
        row = await asyncio.to_thread(
            self._update_registration,
            registration_id,
            facts.owner,
            facts.name,
            profile.profile_id,
            profile.version,
            provider,
            credential_id,
        )
        if row is None:
            raise HTTPException(404, "not_found")
        return _registration_response(dict(row))

    def _select_registration(self, registration_id: int) -> Any:
        with self._connection() as connection:
            return (
                connection.execute(
                    text(
                        "SELECT registration_id, github_repository_id, repository_owner, "
                        "repository_name, profile_id, profile_version, defaults_version, "
                        "provider, credential_id, eligibility_status, checked_sha, "
                        "eligibility_checked_at, eligibility_error "
                        "FROM runtime_private.repository_registrations "
                        "WHERE owner_id=:owner AND registration_id=:registration"
                    ),
                    {"owner": self._principal.account_id, "registration": registration_id},
                )
                .mappings()
                .one_or_none()
            )

    def _update_registration(
        self,
        registration_id: int,
        owner_name: str,
        repository_name: str,
        profile_id: str,
        profile_version: str,
        provider: str | None,
        credential_id: UUID | None,
    ) -> Any:
        with self._connection() as connection:
            self._lock_saved_credential(connection, provider, credential_id)
            return (
                connection.execute(
                    text(
                        "UPDATE runtime_private.repository_registrations SET "
                        "repository_owner=:repo_owner, repository_name=:repo_name, "
                        "profile_id=:profile, profile_version=:version, "
                        "defaults_version=defaults_version+1, provider=:provider, "
                        "credential_id=:credential, eligibility_status='unchecked', "
                        "checked_sha=NULL, eligibility_checked_at=NULL, eligibility_error=NULL, "
                        "eligibility_receipt=NULL, updated_at=clock_timestamp() "
                        "WHERE owner_id=:owner AND registration_id=:registration "
                        "RETURNING registration_id, github_repository_id, repository_owner, "
                        "repository_name, profile_id, profile_version, provider, credential_id, "
                        "eligibility_status, checked_sha, eligibility_checked_at, eligibility_error"
                    ),
                    {
                        "repo_owner": owner_name,
                        "repo_name": repository_name,
                        "profile": profile_id,
                        "version": profile_version,
                        "provider": provider,
                        "credential": credential_id,
                        "owner": self._principal.account_id,
                        "registration": registration_id,
                    },
                )
                .mappings()
                .one_or_none()
            )

    def _lock_saved_credential(
        self,
        connection: Connection,
        provider: str | None,
        credential_id: UUID | None,
    ) -> None:
        if credential_id is None:
            return
        row = connection.execute(
            text(
                "SELECT credential_id FROM runtime_private.provider_credentials "
                "WHERE owner_id=:owner AND credential_id=:credential AND provider=:provider "
                "AND scope='saved' AND status='active' FOR SHARE"
            ),
            {
                "owner": self._principal.account_id,
                "credential": credential_id,
                "provider": provider,
            },
        ).scalar_one_or_none()
        if row is None:
            raise HTTPException(422, "credential_default_invalid")

    async def check(self, registration_id: int, commit_sha: str) -> dict[str, Any]:
        if _SHA_RE.fullmatch(commit_sha) is None:
            raise HTTPException(422, "repository_commit_invalid")
        row = await asyncio.to_thread(self._select_registration, registration_id)
        if row is None:
            raise HTTPException(404, "not_found")
        repo_id = row["github_repository_id"]
        expected_defaults_version = row["defaults_version"]
        github_user_id = await asyncio.to_thread(self._github_user_id)
        facts = await self._current_repository(repo_id, github_user_id)
        self._validate_facts(facts, repo_id, github_user_id)
        try:
            receipt = await self._preflight.check(
                self._principal,
                repository_id=repo_id,
                commit_sha=commit_sha,
                profile_id=row["profile_id"],
                profile_version=row["profile_version"],
            )
        except RepositoryPreflightError as error:
            status = (
                "ineligible"
                if error.code.startswith("repository_")
                and error.code
                not in {
                    "repository_preflight_storage_unavailable",
                    "repository_preflight_state_unavailable",
                    "repository_preflight_in_progress",
                    "repository_preflight_rate_limited",
                    "repository_preflight_timeout",
                    "repository_authorization_pending",
                    "repository_authorization_unavailable",
                    "repository_access_denied",
                }
                else "error"
            )
            saved = await asyncio.to_thread(
                self._save_check_error,
                registration_id,
                expected_defaults_version,
                commit_sha,
                error.code,
                status,
            )
            if not saved:
                raise HTTPException(409, "repository_defaults_changed_during_check") from None
            raise HTTPException(_preflight_status(error.code), error.code) from None
        checked_at = await asyncio.to_thread(
            self._save_check_success,
            registration_id,
            expected_defaults_version,
            receipt,
        )
        if checked_at is None:
            raise HTTPException(409, "repository_defaults_changed_during_check")
        return {
            "status": "eligible",
            "checked_sha": receipt.commit_sha,
            "checked_at": checked_at,
            "profile_id": receipt.profile_id,
            "profile_version": receipt.profile_version,
            "package_count": receipt.package_count,
            "file_count": receipt.file_count,
            "source_bytes": receipt.source_bytes,
            "cache_hit": receipt.cache_hit,
        }

    def _github_user_id(self) -> int:
        with self._connection() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT github_user_id, status FROM runtime_private.users "
                        "WHERE user_id=:owner"
                    ),
                    {"owner": self._principal.account_id},
                )
                .mappings()
                .one_or_none()
            )
        if (
            row is None
            or row["status"] != "active"
            or type(row["github_user_id"]) is not int
            or row["github_user_id"] <= 0
        ):
            raise HTTPException(401, "authentication_required")
        return row["github_user_id"]

    async def _current_repository(self, repository_id: int, github_user_id: int) -> Any:
        try:
            return await self._authorization.current(repository_id, github_user_id)
        except Exception as error:
            code = getattr(error, "code", "github_authorization_failed")
            raise HTTPException(_authorization_status(code), code) from None

    @staticmethod
    def _validate_facts(facts: Any, repository_id: int, github_user_id: int) -> None:
        proof = getattr(facts, "access_proof", None)
        if (
            getattr(facts, "repository_id", None) != repository_id
            or getattr(facts, "github_user_id", None) != github_user_id
            or getattr(proof, "repository_id", None) != repository_id
            or getattr(proof, "github_user_id", None) != github_user_id
            or getattr(proof, "actor_can_write", None) is not True
            or getattr(proof, "app_can_access", None) is not True
        ):
            raise HTTPException(403, "repository_access_denied")

    def _validate_credential(self, provider: str | None, credential_id: UUID | None) -> None:
        if credential_id is None:
            return
        if provider is None:
            raise HTTPException(422, "credential_provider_mismatch")
        metadata = next(
            (
                item
                for item in self._credentials.list_metadata()
                if item.credential_id == credential_id
            ),
            None,
        )
        if (
            metadata is None
            or metadata.provider.value != provider
            or metadata.scope is not CredentialScope.SAVED
            or metadata.status != "active"
        ):
            raise HTTPException(422, "credential_default_invalid")

    def _save_check_success(
        self,
        registration_id: int,
        defaults_version: int,
        receipt: EligibilityReceipt,
    ) -> datetime | None:
        payload = json.dumps(receipt.to_cache_payload(), separators=(",", ":"), sort_keys=True)
        with self._connection() as connection:
            checked_at = connection.execute(
                text(
                    "UPDATE runtime_private.repository_registrations "
                    "SET eligibility_status='eligible', checked_sha=:sha, "
                    "eligibility_checked_at=clock_timestamp(), eligibility_error=NULL, "
                    "eligibility_receipt=CAST(:receipt AS jsonb), updated_at=clock_timestamp() "
                    "WHERE owner_id=:owner AND registration_id=:registration "
                    "AND defaults_version=:defaults_version "
                    "RETURNING eligibility_checked_at"
                ),
                {
                    "sha": receipt.commit_sha,
                    "receipt": payload,
                    "owner": self._principal.account_id,
                    "registration": registration_id,
                    "defaults_version": defaults_version,
                },
            ).scalar_one_or_none()
            return checked_at

    def _save_check_error(
        self,
        registration_id: int,
        defaults_version: int,
        commit_sha: str,
        code: str,
        status: str,
    ) -> bool:
        with self._connection() as connection:
            result = connection.execute(
                text(
                    "UPDATE runtime_private.repository_registrations "
                    "SET eligibility_status=:status, checked_sha=:sha, "
                    "eligibility_checked_at=clock_timestamp(), eligibility_error=:error, "
                    "eligibility_receipt=NULL, updated_at=clock_timestamp() "
                    "WHERE owner_id=:owner AND registration_id=:registration "
                    "AND defaults_version=:defaults_version"
                ),
                {
                    "status": status,
                    "sha": commit_sha,
                    "error": code,
                    "owner": self._principal.account_id,
                    "registration": registration_id,
                    "defaults_version": defaults_version,
                },
            )
            return result.rowcount == 1


def create_repository_router(
    principal_resolver: PrincipalResolver,
    service_factory: Callable[[Request, Principal], RepositoryOnboardingService],
    csrf_guard: CsrfGuard,
) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["repositories", "credentials"])

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

    @router.get("/repositories", response_model=RepositoryListResponse)
    def list_repositories(
        request: Request,
        response: Response,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
    ) -> dict[str, Any]:
        response.headers["Cache-Control"] = "private, no-store"
        return {"repositories": service_factory(request, principal).list()}

    @router.post(
        "/repositories",
        response_model=RepositoryRegistrationResponse,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {"application/json": {"schema": _RegisterBody.model_json_schema()}},
            }
        },
    )
    async def register_repository(
        request: Request,
        response: Response,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
    ) -> dict[str, Any]:
        await require_csrf(request)
        body = await _bounded_model(request, _RegisterBody)
        assert isinstance(body, _RegisterBody)
        result = await service_factory(request, principal).register(
            repository_id=body.github_repository_id,
            profile_id=body.profile_id,
            provider=body.provider,
            credential_id=body.credential_id,
        )
        response.headers["Cache-Control"] = "private, no-store"
        return result

    @router.patch(
        "/repositories/{registration_id}",
        response_model=RepositoryRegistrationResponse,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {"application/json": {"schema": _PatchBody.model_json_schema()}},
            }
        },
    )
    async def update_repository(
        registration_id: Annotated[int, Path(gt=0)],
        request: Request,
        response: Response,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
    ) -> dict[str, Any]:
        await require_csrf(request)
        body = await _bounded_model(request, _PatchBody)
        assert isinstance(body, _PatchBody)
        result = await service_factory(request, principal).update(registration_id, body)
        response.headers["Cache-Control"] = "private, no-store"
        return result

    @router.post(
        "/repositories/{registration_id}/check",
        response_model=EligibilityCheckResponse,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {"application/json": {"schema": _CheckBody.model_json_schema()}},
            }
        },
    )
    async def check_repository(
        registration_id: Annotated[int, Path(gt=0)],
        request: Request,
        response: Response,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
    ) -> dict[str, Any]:
        await require_csrf(request)
        body = await _bounded_model(request, _CheckBody)
        assert isinstance(body, _CheckBody)
        result = await service_factory(request, principal).check(registration_id, body.commit_sha)
        response.headers["Cache-Control"] = "private, no-store"
        return result

    @router.get("/credentials", response_model=CredentialListResponse)
    def list_credentials(
        request: Request,
        response: Response,
        principal: Principal = Depends(authenticated_principal),  # noqa: B008
    ) -> dict[str, Any]:
        response.headers["Cache-Control"] = "private, no-store"
        return {"credentials": service_factory(request, principal).list_credentials()}

    return router


def _registration_response(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["registration_id"],
        "github_repository_id": row["github_repository_id"],
        "owner": row["repository_owner"],
        "name": row["repository_name"],
        "full_name": f"{row['repository_owner']}/{row['repository_name']}",
        "profile_id": row["profile_id"],
        "profile_version": row["profile_version"],
        "provider": row["provider"],
        "credential_id": row["credential_id"],
        "eligibility": {
            "status": row["eligibility_status"],
            "checked_sha": row["checked_sha"],
            "checked_at": row["eligibility_checked_at"],
            "error": row["eligibility_error"],
        },
    }


def _credential_response(item: CredentialMetadata) -> dict[str, Any]:
    return {
        "credential_id": item.credential_id,
        "provider": item.provider.value,
        "scope": item.scope.value,
        "status": item.status,
        "created_at": item.created_at,
        "expires_at": item.expires_at,
    }


def _authorization_status(code: str) -> int:
    if code in {
        "repository_actor_write_access_required",
        "github_installation_user_access_required",
        "repository_must_be_public",
    }:
        return 403
    if code in {"github_user_token_unavailable", "github_identity_mismatch"}:
        return 401
    if "timeout" in code:
        return 504
    return 503


def _preflight_status(code: str) -> int:
    if code in {"repository_access_denied", "repository_actor_unavailable"}:
        return 403
    if "invalid" in code or "unsupported" in code or "ineligible" in code:
        return 422
    if "rate_limited" in code:
        return 429
    if "in_progress" in code:
        return 409
    return 503
