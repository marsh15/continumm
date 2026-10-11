"""FastAPI application and process health endpoints."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import RequestResponseEndpoint

logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    """Documented successful health response shape."""

    status: Literal["ok", "ready"]
    checks: dict[str, Literal["ok"]] | None = None


class ErrorResponse(BaseModel):
    """Uniform machine-readable API error shape."""

    code: str
    message: str
    retryable: bool
    request_id: str


def create_app(static_dir: Path | None = None) -> FastAPI:
    """Build the API application and serve only files from the built frontend."""
    if static_dir is None:
        configured_static_dir = os.environ.get("CONTINUUM_STATIC_DIR")
        static_dir = (
            Path(configured_static_dir)
            if configured_static_dir
            else Path(__file__).resolve().parents[2] / "frontend" / "dist"
        )
    static_root = static_dir.resolve()

    application = FastAPI(
        title="Continumm API",
        version="0.1.0",
        docs_url="/docs",
        redoc_url=None,
    )

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
        message = (
            exc.detail if isinstance(exc.detail, str) else "The request could not be completed."
        )
        code = "not_found" if exc.status_code == 404 else "http_error"
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
        reserved = ("api", "auth", "health", "openapi.json", "docs")
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
