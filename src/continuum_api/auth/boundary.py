"""Trusted browser identity, CSRF, and repository authorization composition."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING

from fastapi import HTTPException, Request
from sqlalchemy import Engine

from continuum_api.auth.github import GitHubAuthorizationClient, GitHubPublicationFactsProvider
from continuum_api.connectivity.storage import SupabaseStorage
from continuum_api.credentials import CredentialCipher
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.review import ReviewService

if TYPE_CHECKING:
    from continuum_api.auth.config import AuthConfig
    from continuum_api.auth.oauth import GitHubOAuthClient
    from continuum_api.auth.sessions import OAuthRateLimiter, OAuthStateStore, SessionService

SESSION_COOKIE = "__Host-continumm_session"
STATE_COOKIE = "__Host-continumm_oauth"
CSRF_COOKIE = "csrf_token"


class GitHubAuthBoundary:
    """Keep server credentials outside the browser and resource identity resolvers."""

    def __init__(
        self,
        *,
        engine: Engine,
        config: AuthConfig,
        sessions: SessionService,
        states: OAuthStateStore,
        oauth: GitHubOAuthClient,
        limiter: OAuthRateLimiter,
        authorization_factory: Callable[[Principal], GitHubAuthorizationClient],
        storage: SupabaseStorage | None = None,
    ) -> None:
        self.engine = engine
        self.config = config
        self.sessions = sessions
        self.states = states
        self.oauth = oauth
        self.limiter = limiter
        self.authorization_factory = authorization_factory
        self.storage = storage

    def check_origin(self, request: Request, *, require_header: bool = False) -> None:
        actual_origin = f"{request.url.scheme}://{request.url.netloc}"
        origin = request.headers.get("origin")
        if (
            actual_origin != self.config.public_origin
            or (require_header and origin != self.config.public_origin)
            or (origin is not None and origin != self.config.public_origin)
        ):
            raise HTTPException(403, "auth_origin_invalid")

    async def principal(self, request: Request) -> Principal:
        self.check_origin(request)
        token = request.cookies.get(SESSION_COOKIE)
        if token is None:
            raise HTTPException(401, "authentication_required")
        resolved = await asyncio.to_thread(self.sessions.resolve, token)
        if resolved is None:
            raise HTTPException(401, "authentication_required")
        request.state.github_identity = resolved
        return resolved.principal

    async def csrf(self, request: Request) -> bool:
        self.check_origin(request, require_header=True)
        token = request.cookies.get(SESSION_COOKIE)
        csrf_cookie = request.cookies.get(CSRF_COOKIE)
        csrf_header = request.headers.get("x-csrf-token")
        if token is None or csrf_cookie is None or csrf_header != csrf_cookie:
            return False
        return await asyncio.to_thread(self.sessions.validate_csrf, token, csrf_header)

    def review(self, request: Request, principal: Principal) -> ReviewService:
        del request
        if self.storage is None:
            raise HTTPException(503, "review_service_unavailable")
        return ReviewService(
            self.engine,
            principal,
            self.storage,
            publication_facts=GitHubPublicationFactsProvider(self.authorization_factory(principal)),
        )

    async def aclose(self) -> None:
        if self.storage is not None:
            await self.storage.aclose()


def configured_boundary(engine: Engine) -> GitHubAuthBoundary:
    """Construct only when explicitly enabled and every server setting is present."""
    from continuum_api.auth.config import AuthConfig
    from continuum_api.auth.oauth import GitHubOAuthClient
    from continuum_api.auth.sessions import (
        GitHubUserTokenStore,
        OAuthRateLimiter,
        OAuthStateStore,
        SessionService,
    )
    from continuum_api.settings import get_settings

    settings = get_settings()
    if not settings.enable_github_auth:
        raise HTTPException(503, "github_auth_unavailable")
    if (
        not settings.github_client_id
        or not settings.github_client_secret
        or not settings.github_public_origin
        or not settings.github_state_key
        or settings.github_app_id is None
        or not settings.github_app_private_key
    ):
        raise HTTPException(503, "github_auth_unavailable")
    config = AuthConfig(
        client_id=settings.github_client_id,
        client_secret=settings.github_client_secret,
        public_origin=settings.github_public_origin,
        redirect_uri=settings.github_public_origin.rstrip("/") + "/api/v1/auth/github/callback",
        pkce_key=settings.github_state_key,
        environment=settings.environment,
    )
    cipher = CredentialCipher.from_settings(settings)
    sessions = SessionService(engine, cipher)
    storage = None
    if settings.supabase_url and settings.supabase_secret_key:
        from continuum_api.artifacts.archives import MAX_COMPRESSED_BYTES

        storage = SupabaseStorage(
            settings.supabase_url,
            settings.supabase_secret_key.get_secret_value(),
            settings.supabase_bucket,
            fixture_byte_limit=MAX_COMPRESSED_BYTES,
        )
    app_id = settings.github_app_id
    app_key = settings.github_app_private_key
    return GitHubAuthBoundary(
        engine=engine,
        config=config,
        sessions=sessions,
        states=OAuthStateStore(engine, config.pkce_key),
        oauth=GitHubOAuthClient(config),
        limiter=OAuthRateLimiter(engine, config.pkce_key),
        authorization_factory=lambda principal: GitHubAuthorizationClient(
            app_id=app_id,
            app_private_key=app_key,
            user_tokens=GitHubUserTokenStore(engine, cipher, principal.account_id),
        ),
        storage=storage,
    )
