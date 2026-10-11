"""FastAPI application and process health endpoints."""

from __future__ import annotations

import logging
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel
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


def create_app() -> FastAPI:
    """Build the API application."""
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

    return application


app = create_app()
