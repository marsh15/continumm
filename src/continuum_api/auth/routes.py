"""Fixed-origin GitHub login, session inspection and CSRF-protected logout."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, ConfigDict

from continuum_api.api_models import ErrorResponse
from continuum_api.auth.boundary import (
    CSRF_COOKIE,
    SESSION_COOKIE,
    STATE_COOKIE,
    GitHubAuthBoundary,
)
from continuum_api.auth.oauth import GitHubOAuthError
from continuum_api.auth.sessions import OAUTH_STATE_SECONDS, SESSION_SECONDS, AuthStorageError


class SessionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    user_id: UUID
    github_user_id: int
    login: str
    expires_at: datetime


def create_auth_router(
    boundary_for: Callable[[Request], GitHubAuthBoundary],
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/auth", tags=["authentication"])
    errors: dict[int | str, dict[str, Any]] = {
        code: {"model": ErrorResponse} for code in (400, 401, 403, 429, 503)
    }

    async def rate_limit(boundary: GitHubAuthBoundary, request: Request) -> None:
        address = request.client.host if request.client is not None else ""
        try:
            allowed = await asyncio.to_thread(boundary.limiter.allow, address)
        except AuthStorageError:
            raise HTTPException(503, "github_auth_unavailable") from None
        if not allowed:
            raise HTTPException(429, "github_auth_rate_limited", headers={"Retry-After": "900"})

    @router.get("/github/start", status_code=303, response_class=RedirectResponse, responses=errors)
    async def start_login(request: Request) -> RedirectResponse:
        boundary = boundary_for(request)
        boundary.check_origin(request)
        await rate_limit(boundary, request)
        try:
            state = await asyncio.to_thread(boundary.states.issue)
            url = boundary.oauth.authorization_url(
                state.state, state.code_verifier.get_secret_value()
            )
        except (AuthStorageError, GitHubOAuthError):
            raise HTTPException(503, "github_auth_unavailable") from None
        response = RedirectResponse(url, status_code=303)
        response.set_cookie(
            STATE_COOKIE,
            state.browser_binding,
            max_age=OAUTH_STATE_SECONDS,
            secure=True,
            httponly=True,
            samesite="lax",
            path="/",
        )
        return response

    @router.get(
        "/github/callback", status_code=303, response_class=RedirectResponse, responses=errors
    )
    async def finish_login(request: Request) -> Response:
        boundary = boundary_for(request)
        boundary.check_origin(request)
        await rate_limit(boundary, request)
        code_values = request.query_params.getlist("code")
        state_values = request.query_params.getlist("state")
        binding = request.cookies.get(STATE_COOKIE)
        if len(code_values) != 1 or len(state_values) != 1 or binding is None:
            raise HTTPException(400, "github_oauth_state_invalid")
        code, state = code_values[0], state_values[0]
        if not 1 <= len(code) <= 1024 or not 32 <= len(state) <= 128:
            raise HTTPException(400, "github_oauth_state_invalid")
        try:
            verifier = await asyncio.to_thread(boundary.states.consume, state, binding)
            if verifier is None:
                raise HTTPException(400, "github_oauth_state_invalid")
            identity = await boundary.oauth.exchange_code(code, verifier.get_secret_value())
            session = await asyncio.to_thread(
                boundary.sessions.login,
                identity.github_user_id,
                identity.login,
                identity.access_token,
                identity.expires_in,
                request.cookies.get(SESSION_COOKIE),
            )
        except GitHubOAuthError:
            raise HTTPException(400, "github_oauth_failed") from None
        except AuthStorageError:
            raise HTTPException(503, "github_auth_unavailable") from None
        if session is None:
            raise HTTPException(403, "account_unavailable")
        response = RedirectResponse("/", status_code=303)
        response.delete_cookie(STATE_COOKIE, path="/", secure=True, httponly=True, samesite="lax")
        response.set_cookie(
            SESSION_COOKIE,
            session.session_token,
            max_age=SESSION_SECONDS,
            expires=session.principal.expires_at.astimezone(UTC),
            secure=True,
            httponly=True,
            samesite="lax",
            path="/",
        )
        response.set_cookie(
            CSRF_COOKIE,
            session.csrf_token,
            max_age=SESSION_SECONDS,
            expires=session.principal.expires_at.astimezone(UTC),
            secure=True,
            httponly=False,
            samesite="strict",
            path="/",
        )
        return response

    @router.get("/session", response_model=SessionResponse, responses=errors)
    async def current_session(request: Request, response: Response) -> SessionResponse:
        boundary = boundary_for(request)
        try:
            await boundary.principal(request)
        except AuthStorageError:
            raise HTTPException(503, "github_auth_unavailable") from None
        identity = request.state.github_identity
        response.headers["Cache-Control"] = "private, no-store"
        return SessionResponse(
            user_id=identity.principal.account_id,
            github_user_id=identity.github_user_id,
            login=identity.login,
            expires_at=identity.expires_at,
        )

    @router.post("/logout", status_code=204, responses=errors)
    async def logout(request: Request) -> Response:
        boundary = boundary_for(request)
        try:
            if not await boundary.csrf(request):
                raise HTTPException(403, "csrf_failed")
            await asyncio.to_thread(boundary.sessions.revoke, request.cookies[SESSION_COOKIE])
        except AuthStorageError:
            raise HTTPException(503, "github_auth_unavailable") from None
        response = Response(status_code=204)
        response.delete_cookie(SESSION_COOKIE, path="/", secure=True, httponly=True, samesite="lax")
        response.delete_cookie(CSRF_COOKIE, path="/", secure=True, samesite="strict")
        response.delete_cookie(STATE_COOKIE, path="/", secure=True, httponly=True, samesite="lax")
        return response

    return router
