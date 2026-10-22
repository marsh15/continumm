"""Authenticated durable wake receipts and opt-in bounded execution scheduling."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import json
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from typing import Literal, Protocol
from urllib.parse import urlsplit

from fastapi import APIRouter, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url

MAX_WAKE_REQUEST_BYTES = 2_048
WAKE_REQUEST_BODY_TIMEOUT_SECONDS = 5
WAKE_DATABASE_OPERATION_TIMEOUT_SECONDS = 8
MAX_POSTGRES_BIGINT = 9_223_372_036_854_775_807


class WakeInput(BaseModel):
    """Exact Edge-to-backend request. Generation remains decimal text on wire."""

    model_config = ConfigDict(extra="forbid", strict=True)

    attempt_id: str = Field(
        pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    )
    generation: str = Field(pattern=r"^[1-9][0-9]{0,18}$")
    wake_id: str = Field(
        pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    )
    reason: Literal["pending_inbox"]

    @field_validator("generation")
    @classmethod
    def generation_fits_postgres_bigint(cls, value: str) -> str:
        if int(value) > MAX_POSTGRES_BIGINT:
            raise ValueError("generation_out_of_range")
        return value


class WakeStatus(BaseModel):
    """Operational status does not claim inbox consumption or drain success."""

    worker_status: Literal["worker_not_started", "running", "partial", "drained"]
    drain_status: Literal["not_started", "running", "partial", "completed"]
    last_successful_drain_at: datetime | None
    receipt_accepted_at: datetime | None


class WakeAccepted(BaseModel):
    accepted: Literal[True]
    receipt_id: str
    status: Literal["worker_not_started", "scheduled", "running", "partial", "drained"]
    drain_status: Literal["not_started", "running", "partial", "completed"]
    last_successful_drain_at: datetime | None


@dataclass(frozen=True, slots=True)
class ReceiptResult:
    outcome: str
    receipt_id: str | None
    accepted_at: datetime | None
    drain_status: str | None
    last_successful_drain_at: datetime | None


@dataclass(frozen=True, slots=True)
class StatusResult:
    last_successful_drain_at: datetime | None
    receipt_state: str | None
    receipt_accepted_at: datetime | None


class WakeStore(Protocol):
    def accept(self, request: WakeInput) -> ReceiptResult: ...

    def status(self) -> StatusResult: ...


class WakeScheduler(Protocol):
    def schedule(self, request: WakeInput) -> bool: ...


class WakeStoreUnavailable(RuntimeError):
    """Redacted wake database failure."""


class PostgresWakeStore:
    """Receiver-role SQL adapter; every accepted wake is committed before return."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def close(self) -> None:
        self._engine.dispose()

    def accept(self, request: WakeInput) -> ReceiptResult:
        try:
            with self._engine.begin() as connection:
                row = (
                    connection.execute(
                        text(
                            """
                            SELECT * FROM wake_private.accept_wake_receipt(
                                CAST(:attempt_id AS uuid), CAST(:generation AS bigint),
                                CAST(:wake_id AS uuid), CAST(:reason AS text)
                            )
                            """
                        ),
                        request.model_dump(),
                    )
                    .mappings()
                    .one()
                )
                result = ReceiptResult(
                    outcome=row["outcome"],
                    receipt_id=str(row["receipt_id"]) if row["receipt_id"] else None,
                    accepted_at=row["accepted_at"],
                    drain_status=row["drain_status"],
                    last_successful_drain_at=row["last_successful_drain_at"],
                )
            return result
        except Exception:
            raise WakeStoreUnavailable("wake_receiver_database_unavailable") from None

    def status(self) -> StatusResult:
        try:
            with self._engine.connect() as connection:
                row = (
                    connection.execute(text("SELECT * FROM wake_private.read_wake_status()"))
                    .mappings()
                    .one()
                )
                return StatusResult(
                    last_successful_drain_at=row["last_successful_drain_at"],
                    receipt_state=row["receipt_state"],
                    receipt_accepted_at=row["receipt_accepted_at"],
                )
        except Exception:
            raise WakeStoreUnavailable("wake_receiver_database_unavailable") from None


@dataclass(frozen=True, slots=True)
class WakeReceiverConfig:
    backend_token: str = field(repr=False)
    store: WakeStore


@lru_cache(maxsize=1)
def load_wake_receiver_config() -> WakeReceiverConfig | None:
    """Fail closed when either receiver credential or its database URL is invalid."""
    token = os.environ.get("CONTINUUM_WAKE_BACKEND_TOKEN", "")
    database_url = os.environ.get("CONTINUUM_WAKE_RECEIVER_DATABASE_URL", "")
    if len(token.encode("utf-8")) < 32 or not database_url:
        return None
    try:
        engine = create_wake_receiver_engine(database_url)
    except Exception:
        return None
    return WakeReceiverConfig(token, PostgresWakeStore(engine))


def create_wake_receiver_engine(database_url: str, *, production: bool | None = None) -> Engine:
    """Validate a single-purpose receiver URL before constructing a small pool."""
    try:
        url = make_url(database_url)
        parts = urlsplit(database_url)
        if url.drivername not in {"postgres", "postgresql", "postgresql+psycopg"}:
            raise ValueError("unsupported_driver")
        if not url.host or not url.username or not url.password or not url.database:
            raise ValueError("incomplete_url")
        if url.query.keys() - {"sslmode"}:
            raise ValueError("unsupported_query")
        environment_is_production = (
            os.environ.get("CONTINUUM_ENVIRONMENT") == "production"
            if production is None
            else production
        )
        sslmode = url.query.get("sslmode")
        try:
            local_host = ipaddress.ip_address(url.host).is_loopback
        except ValueError:
            local_host = url.host.rstrip(".").lower() == "localhost"
        if environment_is_production:
            if parts.scheme not in {"postgres", "postgresql"}:
                raise ValueError("unsupported_scheme")
            if sslmode not in {"require", "verify-ca", "verify-full"}:
                raise ValueError("tls_required")
        elif sslmode not in {None, "disable", "require", "verify-ca", "verify-full"}:
            raise ValueError("invalid_sslmode")
        if (
            not environment_is_production
            and not local_host
            and sslmode
            not in {
                "require",
                "verify-ca",
                "verify-full",
            }
        ):
            raise ValueError("nonlocal_tls_required")
        if parts.fragment or parts.username is None or parts.password is None:
            raise ValueError("invalid_url")
        connect_args: dict[str, object] = {
            "application_name": "continumm-wake-receiver",
            "connect_timeout": 5,
            "options": "-c statement_timeout=5000 -c lock_timeout=2000",
        }
        if sslmode is not None:
            connect_args["sslmode"] = sslmode
        engine = create_engine(
            url.set(drivername="postgresql+psycopg"),
            pool_size=1,
            max_overflow=0,
            pool_timeout=3,
            pool_pre_ping=True,
            hide_parameters=True,
            connect_args=connect_args,
        )
        return engine
    except Exception:
        raise ValueError("wake_receiver_database_configuration_invalid") from None


def create_wake_router(
    config_provider: Callable[[], WakeReceiverConfig | None] = load_wake_receiver_config,
    *,
    scheduler: WakeScheduler | None = None,
) -> APIRouter:
    """Build hidden internal routes with authentication before body parsing."""
    router = APIRouter(include_in_schema=False)

    @router.post("/internal/v1/wake")
    async def receive_wake(request: Request) -> Response:
        config = config_provider()
        if config is None:
            return _wake_error(request, 503, "worker_not_started", retryable=True)
        if not _authorized(request, config.backend_token):
            return _wake_error(request, 401, "unauthorized", headers={"WWW-Authenticate": "Bearer"})
        if not _is_json_content_type(request.headers.get("content-type")):
            return _wake_error(request, 415, "unsupported_content_type")
        body_kind, raw = await _read_bounded_json(request)
        if body_kind == "invalid":
            return _wake_error(request, 400, "invalid_request")
        if body_kind == "too_large":
            return _wake_error(request, 413, "request_too_large")
        if body_kind == "timeout":
            return _wake_error(request, 408, "request_timeout", retryable=True)
        assert raw is not None
        try:
            payload = WakeInput.model_validate_json(raw)
        except (ValidationError, ValueError, UnicodeDecodeError):
            return _wake_error(request, 400, "invalid_request")

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(config.store.accept, payload),
                timeout=WAKE_DATABASE_OPERATION_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            return _wake_error(request, 503, "wake_receiver_unavailable", retryable=True)
        except WakeStoreUnavailable:
            return _wake_error(request, 503, "wake_receiver_unavailable", retryable=True)
        if result.outcome not in {"accepted", "duplicate"}:
            return _wake_error(request, 409, "wake_not_accepted")
        if (
            result.receipt_id != payload.wake_id
            or result.drain_status not in {"worker_not_started", "running", "partial", "drained"}
            or (result.drain_status == "drained" and result.last_successful_drain_at is None)
            or result.accepted_at is None
        ):
            return _wake_error(request, 503, "wake_receipt_invalid", retryable=True)
        status = result.drain_status
        if result.drain_status != "drained" and scheduler is not None:
            try:
                scheduled = scheduler.schedule(payload)
            except Exception:
                return _wake_error(request, 503, "wake_worker_unavailable", retryable=True)
            if scheduled and result.drain_status == "worker_not_started":
                status = "scheduled"
        receipt = WakeAccepted(
            accepted=True,
            receipt_id=result.receipt_id,
            status=status,  # type: ignore[arg-type]
            drain_status=_drain_status(result.drain_status),
            last_successful_drain_at=result.last_successful_drain_at,
        )
        return Response(
            content=receipt.model_dump_json(),
            status_code=202,
            media_type="application/json",
            headers=_request_headers(request),
        )

    @router.get("/internal/v1/wake/status")
    async def get_wake_status(request: Request) -> Response:
        config = config_provider()
        if config is None:
            return _wake_error(request, 503, "worker_not_started", retryable=True)
        if not _authorized(request, config.backend_token):
            return _wake_error(request, 401, "unauthorized", headers={"WWW-Authenticate": "Bearer"})
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(config.store.status),
                timeout=WAKE_DATABASE_OPERATION_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            return _wake_error(request, 503, "wake_receiver_unavailable", retryable=True)
        except WakeStoreUnavailable:
            return _wake_error(request, 503, "wake_receiver_unavailable", retryable=True)
        if result.receipt_state not in {
            None,
            "worker_not_started",
            "running",
            "partial",
            "drained",
        }:
            return _wake_error(request, 503, "wake_status_invalid", retryable=True)
        status = WakeStatus(
            worker_status=result.receipt_state or "worker_not_started",  # type: ignore[arg-type]
            drain_status=_drain_status(result.receipt_state),
            last_successful_drain_at=result.last_successful_drain_at,
            receipt_accepted_at=result.receipt_accepted_at,
        )
        return Response(
            content=status.model_dump_json(),
            media_type="application/json",
            headers=_request_headers(request),
        )

    return router


def _drain_status(state: str | None) -> Literal["not_started", "running", "partial", "completed"]:
    if state == "drained":
        return "completed"
    if state == "running":
        return "running"
    if state == "partial":
        return "partial"
    return "not_started"


def _authorized(request: Request, expected: str) -> bool:
    authorization = request.headers.get("authorization", "")
    scheme, separator, supplied = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not supplied or len(supplied) > 1_024:
        return False
    supplied_digest = hashlib.sha256(supplied.encode("utf-8", errors="replace")).digest()
    expected_digest = hashlib.sha256(expected.encode("utf-8")).digest()
    return hmac.compare_digest(supplied_digest, expected_digest)


async def _read_bounded_json(request: Request) -> tuple[str, bytes | None]:
    length = request.headers.get("content-length")
    if length is not None:
        if not length.isascii() or not length.isdecimal() or (len(length) > 1 and length[0] == "0"):
            return "invalid", None
        if int(length) > MAX_WAKE_REQUEST_BYTES:
            return "too_large", None
    chunks: list[bytes] = []
    total = 0
    try:
        async with asyncio.timeout(WAKE_REQUEST_BODY_TIMEOUT_SECONDS):
            async for chunk in request.stream():
                total += len(chunk)
                if total > MAX_WAKE_REQUEST_BYTES:
                    return "too_large", None
                chunks.append(chunk)
    except TimeoutError:
        return "timeout", None
    except Exception:
        return "invalid", None
    return "ok", b"".join(chunks)


def _is_json_content_type(value: str | None) -> bool:
    if value is None:
        return False
    parts = value.split(";")
    if parts[0].strip().lower() != "application/json":
        return False
    return all(
        len(parameter.split("=", 1)) == 2
        and parameter.split("=", 1)[0].strip().lower() == "charset"
        and parameter.split("=", 1)[1].strip().strip('"').lower() == "utf-8"
        for parameter in parts[1:]
    )


def _wake_error(
    request: Request,
    status: int,
    code: str,
    *,
    retryable: bool = False,
    headers: dict[str, str] | None = None,
) -> Response:
    message = {
        "unauthorized": "The wake request is not authorized.",
        "worker_not_started": "Wake receipt storage is not configured.",
        "wake_receiver_unavailable": "The wake receipt could not be stored.",
        "wake_receipt_invalid": "The wake receipt could not be verified.",
        "wake_not_accepted": "The wake request is stale or no longer actionable.",
        "wake_status_invalid": "Wake status is unavailable.",
        "wake_worker_unavailable": "Wake execution could not be scheduled.",
        "unsupported_content_type": "The request must use application/json.",
        "request_too_large": "The request body is too large.",
        "request_timeout": "The request body timed out.",
        "invalid_request": "The wake request did not match the required format.",
    }.get(code, "The wake request could not be completed.")
    return Response(
        content=json.dumps(
            {
                "code": code,
                "message": message,
                "retryable": retryable,
                "request_id": request.state.request_id,
            }
        ),
        status_code=status,
        media_type="application/json",
        headers={**_request_headers(request), **(headers or {})},
    )


def _request_headers(request: Request) -> dict[str, str]:
    return {
        "cache-control": "no-store",
        "x-request-id": request.state.request_id,
    }
