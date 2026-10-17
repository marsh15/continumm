"""FastAPI application and process health endpoints."""

from __future__ import annotations

import logging
import os
import re
import secrets
import threading
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import Engine
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import RequestResponseEndpoint

from continuum_api.api_models import ErrorResponse
from continuum_api.auth.boundary import GitHubAuthBoundary, configured_boundary
from continuum_api.wake import create_wake_router

logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    """Documented successful health response shape."""

    status: Literal["ok", "ready"]
    checks: dict[str, Literal["ok"]] | None = None


def create_app(
    static_dir: Path | None = None,
    *,
    principal_resolver: Callable[[Request], Any] | None = None,
    review_service_factory: Callable[[Request, Any], Any] | None = None,
    csrf_guard: Callable[[Request], Any] | None = None,
    runtime_engine: Engine | None = None,
    github_auth_boundary: GitHubAuthBoundary | None = None,
) -> FastAPI:
    """Build the API with an injected identity boundary and runtime database."""
    if static_dir is None:
        configured_static_dir = os.environ.get("CONTINUUM_STATIC_DIR")
        static_dir = (
            Path(configured_static_dir)
            if configured_static_dir
            else Path(__file__).resolve().parents[2] / "frontend" / "dist"
        )
    static_root = static_dir.resolve()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            auth = application.state.github_auth_boundary
            if auth is not None:
                await auth.aclose()
            if application.state.owns_runtime_engine:
                application.state.runtime_engine.dispose()

    application = FastAPI(
        title="Continumm API",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )

    @application.get("/docs", include_in_schema=False)
    def documentation() -> HTMLResponse:
        # Preserve the existing documentation UI without allowing arbitrary inline scripts.
        nonce = secrets.token_urlsafe(24)
        html = get_swagger_ui_html(openapi_url="/openapi.json", title="Continumm API - Swagger UI")
        body = bytes(html.body).decode("utf-8").replace("<script", f'<script nonce="{nonce}"')
        return HTMLResponse(
            body,
            headers={
                "Content-Security-Policy": (
                    f"default-src 'self'; script-src 'self' 'nonce-{nonce}'; "
                    "style-src 'self' https://cdn.jsdelivr.net; object-src 'none'; "
                    "base-uri 'none'; frame-ancestors 'none'; "
                    "connect-src 'self'; form-action 'self'"
                )
            },
        )

    application.include_router(create_wake_router())
    application.state.github_auth_boundary = github_auth_boundary
    application.state.github_auth_lock = threading.Lock()
    application.state.runtime_engine = runtime_engine
    application.state.owns_runtime_engine = False
    application.state.runtime_engine_lock = threading.Lock()
    from continuum_api.auth.routes import create_auth_router
    from continuum_api.auth.sessions import AuthStorageError
    from continuum_api.runtime.review_routes import create_review_router
    from continuum_api.runtime.run_routes import (
        _StreamLimiter,
        create_run_router,
        get_runtime_engine,
        install_sse_openapi,
    )

    def auth_boundary(request: Request) -> GitHubAuthBoundary:
        existing = request.app.state.github_auth_boundary
        if isinstance(existing, GitHubAuthBoundary):
            return existing
        with request.app.state.github_auth_lock:
            existing = request.app.state.github_auth_boundary
            if isinstance(existing, GitHubAuthBoundary):
                return existing
            try:
                from continuum_api.settings import get_settings

                if not get_settings().enable_github_auth:
                    raise StarletteHTTPException(503, "github_auth_unavailable")
                configured = configured_boundary(get_runtime_engine(request))
            except StarletteHTTPException:
                raise
            except Exception:
                raise StarletteHTTPException(503, "github_auth_unavailable") from None
            request.app.state.github_auth_boundary = configured
            return configured

    async def browser_principal(request: Request) -> Any:
        try:
            return await auth_boundary(request).principal(request)
        except StarletteHTTPException as error:
            if error.detail == "github_auth_unavailable":
                raise StarletteHTTPException(401, "authentication_required") from None
            raise
        except AuthStorageError:
            raise StarletteHTTPException(503, "github_auth_unavailable") from None

    def browser_review(request: Request, principal: Any) -> Any:
        return auth_boundary(request).review(request, principal)

    async def browser_csrf(request: Request) -> bool:
        try:
            return await auth_boundary(request).csrf(request)
        except AuthStorageError:
            return False

    principal_resolver = principal_resolver or browser_principal
    review_service_factory = review_service_factory or browser_review
    csrf_guard = csrf_guard or browser_csrf
    application.include_router(create_auth_router(auth_boundary))
    application.state.sse_stream_limiter = _StreamLimiter()
    application.include_router(create_run_router(principal_resolver))
    application.include_router(
        create_review_router(principal_resolver, review_service_factory, csrf_guard)
    )
    install_sse_openapi(application)

    @application.middleware("http")
    async def structured_request_log(
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        request_id = str(uuid4())
        request.state.request_id = request_id
        try:
            response = await call_next(request)
        except Exception:
            logger.exception("request failed", extra={"request_id": request_id})
            raise
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers.setdefault(
            "Content-Security-Policy",
            (
                "default-src 'self'; script-src 'self'; style-src 'self'; "
                "object-src 'none'; base-uri 'none'; frame-ancestors 'none'; "
                "form-action 'self'; connect-src 'self'"
            ),
        )
        if request.url.scheme == "https":
            response.headers["Strict-Transport-Security"] = "max-age=31536000"
        if request.url.path.startswith("/api/v1/auth/"):
            response.headers["Cache-Control"] = "private, no-store"
        logger.info(
            "http request completed",
            extra={
                "request_id": request_id,
                "method": request.method,
                "status": response.status_code,
            },
        )
        return response

    def error_response(
        request: Request,
        *,
        status_code: int,
        code: str,
        message: str,
        retryable: bool = False,
        headers: dict[str, str] | None = None,
    ) -> JSONResponse:
        """Create the stable error envelope used by browser API callers."""
        response_headers = dict(headers or {})
        response_headers["X-Request-ID"] = request.state.request_id
        return JSONResponse(
            status_code=status_code,
            content=ErrorResponse(
                code=code,
                message=message,
                retryable=retryable,
                request_id=request.state.request_id,
            ).model_dump(),
            headers=response_headers,
        )

    @application.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        messages = {
            "authentication_required": ("authentication_required", "Authentication is required."),
            "event_cursor_invalid": ("event_cursor_invalid", "The event cursor is invalid."),
            "event_cursor_ahead": ("event_cursor_ahead", "The cursor is newer than run history."),
            "event_stream_limit_reached": (
                "event_stream_limit_reached",
                "The event stream limit has been reached.",
            ),
            "runtime_database_unavailable": (
                "runtime_database_unavailable",
                "Run history is temporarily unavailable.",
            ),
            "not_found": ("not_found", "The requested API resource was not found."),
        }
        auth_messages = {
            "github_auth_unavailable": "GitHub sign-in is temporarily unavailable.",
            "github_auth_rate_limited": "Too many sign-in attempts. Try again later.",
            "auth_origin_invalid": "The browser origin is not allowed.",
            "github_oauth_state_invalid": (
                "The sign-in request expired or did not match this browser."
            ),
            "github_oauth_failed": "GitHub sign-in could not be completed. Start again.",
            "account_unavailable": "This account is unavailable.",
            "github_user_token_unavailable": (
                "Reconnect GitHub to verify current repository access."
            ),
            "github_identity_mismatch": "Reconnect GitHub with the linked account.",
            "repository_actor_write_access_required": "GitHub write access is required.",
            "github_installation_user_access_required": "GitHub App access is required.",
            "github_authorization_timeout": "GitHub access verification timed out.",
            "github_api_unavailable": "GitHub access verification is temporarily unavailable.",
            "github_api_request_failed": "Current GitHub access could not be verified.",
        }
        messages.update({code: (code, message) for code, message in auth_messages.items()})
        mapped = messages.get(exc.detail) if isinstance(exc.detail, str) else None
        message = (
            exc.detail if isinstance(exc.detail, str) else "The request could not be completed."
        )
        if mapped is None:
            if exc.status_code == 404:
                code = "not_found"
            elif isinstance(exc.detail, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,95}", exc.detail):
                code = exc.detail
            else:
                code = "http_error"
        else:
            code, message = mapped
        retryable = exc.status_code in (408, 425, 429) or exc.status_code >= 500
        return error_response(
            request,
            status_code=exc.status_code,
            code=code,
            message=message,
            retryable=retryable,
            headers=dict(exc.headers or {}),
        )

    @application.exception_handler(RequestValidationError)
    async def validation_error(request: Request, _exc: RequestValidationError) -> JSONResponse:
        return error_response(
            request,
            status_code=422,
            code="validation_error",
            message="The request did not match the required format.",
        )

    @application.exception_handler(Exception)
    async def internal_error(request: Request, _exc: Exception) -> JSONResponse:
        return error_response(
            request,
            status_code=500,
            code="internal_error",
            message="The service could not complete this request.",
            retryable=True,
        )

    @application.get("/health/live", tags=["health"], response_model=HealthResponse)
    async def live() -> HealthResponse:
        """Report that the process can serve requests."""
        return HealthResponse(status="ok")

    @application.get(
        "/health/ready",
        tags=["health"],
        response_model=HealthResponse,
        responses={503: {"model": ErrorResponse}},
    )
    async def ready(request: Request) -> Response:
        """Report readiness without claiming an unimplemented database check."""
        return JSONResponse(
            status_code=503,
            content=ErrorResponse(
                code="not_ready",
                message="Database readiness has not been verified.",
                retryable=True,
                request_id=request.state.request_id,
            ).model_dump(),
        )

    @application.get("/{asset_path:path}", include_in_schema=False)
    async def frontend_or_not_found(asset_path: str, request: Request) -> Response:
        """Serve built assets and SPA routes without exposing other application files."""
        normalized_path = asset_path.lstrip("/")
        reserved = ("api", "auth", "health", "internal", "openapi.json", "docs")
        if any(
            normalized_path == prefix or normalized_path.startswith(f"{prefix}/")
            for prefix in reserved
        ):
            return error_response(
                request,
                status_code=404,
                code="not_found",
                message="The requested API resource was not found.",
            )

        try:
            candidate = (static_root / normalized_path).resolve()
            if candidate.is_relative_to(static_root) and candidate.is_file():
                cache_control = (
                    "public, max-age=31536000, immutable"
                    if normalized_path.startswith("assets/")
                    else "no-cache"
                )
                return FileResponse(candidate, headers={"Cache-Control": cache_control})
        except (OSError, RuntimeError):
            # A missing file or an invalid symlink is an ordinary not-found result.
            pass

        accepts_html = "text/html" in request.headers.get("accept", "")
        is_extensionless_route = not Path(normalized_path).suffix
        try:
            index_file = (static_root / "index.html").resolve()
            if (
                accepts_html
                and is_extensionless_route
                and index_file.is_relative_to(static_root)
                and index_file.is_file()
            ):
                return FileResponse(index_file, headers={"Cache-Control": "no-cache"})
        except (OSError, RuntimeError):
            pass

        return error_response(
            request,
            status_code=404,
            code="not_found",
            message="The requested page or asset was not found.",
        )

    return application


app = create_app()
