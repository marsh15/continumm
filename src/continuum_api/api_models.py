"""Shared HTTP schemas that do not depend on app construction order."""

from __future__ import annotations

from pydantic import BaseModel


class ErrorResponse(BaseModel):
    """Uniform machine-readable API error shape."""

    code: str
    message: str
    retryable: bool
    request_id: str
