"""Restricted operator diagnostics and audited emergency controls.

Only an allowlisted operator may use the local CLI or server-side operator API.
This module exposes metadata projections and explicit state changes; it has no
force-complete, replay-all, or external mutation retry operation.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import SQLAlchemyError


class OperatorActionError(RuntimeError):
    """Sanitized operator-action error code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class OperatorReason(StrEnum):
    QUOTA_EXHAUSTED = "quota_exhausted"
    DATABASE_PAUSE = "database_pause"
    FAILED_DELIVERY = "failed_delivery"
    LOST_ALLOCATION = "lost_allocation"
    UNCERTAIN_PUBLICATION = "uncertain_publication"
    SECURITY = "security"
    STORAGE_PRESSURE = "storage_pressure"
    OTHER = "other"


@dataclass(frozen=True, slots=True)
class OperatorOverview:
    admissions_stopped: bool
    active_runs: int
    unknown_effects: int
    uncertain_allocations: int
    pending_inbox_deliveries: int
    oldest_pending_inbox_at: datetime | None
    artifact_charged_bytes: int
    artifact_capacity_limit_bytes: int
    suspended_accounts: int


@dataclass(frozen=True, slots=True)
class UnknownEffect:
    effect_id: UUID
    owner_id: UUID
    run_id: UUID
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class UncertainAllocation:
    allocation_id: UUID
    owner_id: UUID
    run_id: UUID
    status: str
    provider_name: str
    created_at: datetime
    last_observed_at: datetime | None


@dataclass(frozen=True, slots=True)
class PendingInboxDelivery:
    delivery_id: UUID
    event_type: str
    received_at: datetime


@dataclass(frozen=True, slots=True)
class SuspendedAccount:
    user_id: UUID
    github_user_id: int
    created_at: datetime


@dataclass(frozen=True, slots=True)
class OperatorAuditEvent:
    audit_id: UUID
    actor_github_user_id: int
    action: str
    target_user_id: UUID | None
    reason_code: str | None
    created_at: datetime


class OperatorActionService:
    """Read sanitized operational metadata and apply audited controls."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def overview(self) -> OperatorOverview:
        row = self._one("SELECT operator_private.read_overview() AS overview")
        value = row["overview"]
        oldest_pending = value["oldest_pending_inbox_at"]
        if isinstance(oldest_pending, str):
            oldest_pending = datetime.fromisoformat(oldest_pending.replace("Z", "+00:00"))
        return OperatorOverview(
            admissions_stopped=bool(value["admissions_stopped"]),
            active_runs=int(value["active_runs"]),
            unknown_effects=int(value["unknown_effects"]),
            uncertain_allocations=int(value["uncertain_allocations"]),
            pending_inbox_deliveries=int(value["pending_inbox_deliveries"]),
            oldest_pending_inbox_at=oldest_pending,
            artifact_charged_bytes=int(value["artifact_charged_bytes"]),
            artifact_capacity_limit_bytes=int(value["artifact_capacity_limit_bytes"]),
            suspended_accounts=int(value["suspended_accounts"]),
        )

    def unknown_effects(self, limit: int = 50) -> tuple[UnknownEffect, ...]:
        return tuple(
            UnknownEffect(**dict(row))
            for row in self._rows(
                "SELECT * FROM operator_private.read_unknown_effects(:limit)", limit
            )
        )

    def uncertain_allocations(self, limit: int = 50) -> tuple[UncertainAllocation, ...]:
        return tuple(
            UncertainAllocation(**dict(row))
            for row in self._rows(
                "SELECT * FROM operator_private.read_unknown_allocations(:limit)", limit
            )
        )

    def pending_inbox(self, limit: int = 50) -> tuple[PendingInboxDelivery, ...]:
        return tuple(
            PendingInboxDelivery(**dict(row))
            for row in self._rows(
                "SELECT * FROM operator_private.read_pending_inbox(:limit)", limit
            )
        )

    def suspended_accounts(self, limit: int = 50) -> tuple[SuspendedAccount, ...]:
        return tuple(
            SuspendedAccount(**dict(row))
            for row in self._rows(
                "SELECT * FROM operator_private.read_suspended_accounts(:limit)", limit
            )
        )

    def audit_events(self, limit: int = 50) -> tuple[OperatorAuditEvent, ...]:
        return tuple(
            OperatorAuditEvent(**dict(row))
            for row in self._rows("SELECT * FROM operator_private.read_audit_events(:limit)", limit)
        )

    def set_admission_stopped(
        self, stopped: bool, actor_github_user_id: int, reason: OperatorReason
    ) -> bool:
        _validate_actor(actor_github_user_id)
        try:
            with self._engine.begin() as connection:
                changed: bool = connection.execute(
                    text("SELECT operator_private.set_admission_stopped(:stopped,:actor,:reason)"),
                    {
                        "stopped": stopped,
                        "actor": actor_github_user_id,
                        "reason": reason.value,
                    },
                ).scalar_one()
                return bool(changed)
        except SQLAlchemyError:
            raise OperatorActionError("operator_storage_unavailable") from None

    def set_account_suspended(
        self,
        github_user_id: int,
        suspended: bool,
        actor_github_user_id: int,
        reason: OperatorReason,
    ) -> bool:
        _validate_actor(actor_github_user_id)
        if type(github_user_id) is not int or github_user_id <= 0:
            raise OperatorActionError("operator_account_id_invalid")
        try:
            with self._engine.begin() as connection:
                changed: bool = connection.execute(
                    text(
                        "SELECT operator_private.set_account_suspended("
                        ":github_user_id,:suspended,:actor,:reason)"
                    ),
                    {
                        "github_user_id": github_user_id,
                        "suspended": suspended,
                        "actor": actor_github_user_id,
                        "reason": reason.value,
                    },
                ).scalar_one()
                return bool(changed)
        except SQLAlchemyError as error:
            code = getattr(getattr(error, "orig", None), "sqlstate", None)
            if code == "P0002":
                raise OperatorActionError("operator_account_not_found") from None
            if code == "55000":
                raise OperatorActionError("operator_account_not_reactivatable") from None
            raise OperatorActionError("operator_storage_unavailable") from None

    def revoke_provider_credential(
        self, credential_id: UUID, actor_github_user_id: int, reason: OperatorReason
    ) -> bool:
        _validate_actor(actor_github_user_id)
        if not isinstance(credential_id, UUID):
            raise OperatorActionError("operator_credential_id_invalid")
        try:
            with self._engine.begin() as connection:
                changed: bool = connection.execute(
                    text(
                        "SELECT operator_private.revoke_provider_credential("
                        ":credential,:actor,:reason)"
                    ),
                    {
                        "credential": credential_id,
                        "actor": actor_github_user_id,
                        "reason": reason.value,
                    },
                ).scalar_one()
                return bool(changed)
        except SQLAlchemyError as error:
            if getattr(getattr(error, "orig", None), "sqlstate", None) == "P0002":
                raise OperatorActionError("operator_credential_not_found") from None
            raise OperatorActionError("operator_storage_unavailable") from None

    def _one(self, statement: str) -> Any:
        try:
            with self._engine.begin() as connection:
                row = connection.execute(text(statement)).mappings().one_or_none()
            if row is None:
                raise OperatorActionError("operator_metadata_unavailable")
            return row
        except OperatorActionError:
            raise
        except SQLAlchemyError:
            raise OperatorActionError("operator_storage_unavailable") from None

    def _rows(self, statement: str, limit: int) -> list[Any]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise OperatorActionError("operator_limit_invalid")
        try:
            with self._engine.begin() as connection:
                return list(connection.execute(text(statement), {"limit": limit}).mappings())
        except SQLAlchemyError:
            raise OperatorActionError("operator_storage_unavailable") from None


def parse_operator_allowlist(value: str | None) -> frozenset[int]:
    """Parse comma-separated immutable GitHub account IDs; empty disables operators."""
    if value is None or not value.strip():
        return frozenset()
    identifiers: set[int] = set()
    for raw in value.split(","):
        item = raw.strip()
        if not item.isascii() or not item.isdecimal() or item.startswith("0"):
            raise OperatorActionError("operator_allowlist_invalid")
        identifier = int(item)
        _validate_actor(identifier)
        identifiers.add(identifier)
    return frozenset(identifiers)


def require_operator_actor(actor_github_user_id: int, allowlist: frozenset[int]) -> int:
    """Return the server-sourced actor ID only when deployment configuration permits it."""
    _validate_actor(actor_github_user_id)
    if actor_github_user_id not in allowlist:
        raise OperatorActionError("operator_actor_not_allowlisted")
    return actor_github_user_id


def create_operator_engine(database_url: str) -> Engine:
    """Build a bounded connection using only the dedicated operator login."""
    url = _validate_operator_url(database_url)
    return create_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=3,
        pool_pre_ping=True,
        pool_recycle=300,
        hide_parameters=True,
        connect_args={
            "application_name": "continumm-operator",
            "connect_timeout": 5,
            "options": "-c statement_timeout=5000 -c lock_timeout=2000",
        },
    )


def _validate_operator_url(database_url: str) -> URL:
    try:
        url = make_url(database_url)
        parts = urlsplit(database_url)
        if url.drivername not in {"postgres", "postgresql", "postgresql+psycopg"}:
            raise ValueError
        if (
            url.username != "continuum_operator"
            or not url.password
            or not url.host
            or not url.database
        ):
            raise ValueError
        if parts.fragment or url.query.keys() - {"sslmode", "sslrootcert"}:
            raise ValueError
        try:
            loopback = ipaddress.ip_address(url.host).is_loopback
        except ValueError:
            loopback = url.host.rstrip(".").lower() == "localhost"
        production = os.environ.get("CONTINUUM_ENVIRONMENT") == "production"
        sslmode = url.query.get("sslmode")
        if production or not loopback:
            if sslmode not in {None, "verify-full"}:
                raise ValueError
            url = url.set(query={"sslmode": "verify-full", "sslrootcert": "system"})
        elif sslmode not in {None, "disable", "verify-full"}:
            raise ValueError
        elif sslmode == "verify-full":
            url = url.set(query={"sslmode": "verify-full", "sslrootcert": "system"})
        else:
            url = url.set(query={"sslmode": "disable"})
        return url.set(drivername="postgresql+psycopg")
    except Exception:
        raise OperatorActionError("operator_database_url_invalid_or_unsafe") from None


def _validate_actor(actor_github_user_id: int) -> None:
    if (
        type(actor_github_user_id) is not int
        or not 1 <= actor_github_user_id <= 9_223_372_036_854_775_807
    ):
        raise OperatorActionError("operator_actor_id_invalid")


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="continumm-operator")
    parser.add_argument("--actor-github-user-id", type=int, required=True)
    subparsers = parser.add_subparsers(dest="group", required=True)

    inspect_parser = subparsers.add_parser("inspect")
    inspect_parser.add_argument(
        "view",
        choices=(
            "overview",
            "unknown-effects",
            "uncertain-allocations",
            "inbox-lag",
            "suspended-accounts",
            "audit",
        ),
    )
    inspect_parser.add_argument("--limit", type=int, default=50)

    admission = subparsers.add_parser("admission")
    admission.add_argument("action", choices=("stop", "resume"))
    admission.add_argument(
        "--reason", choices=tuple(item.value for item in OperatorReason), required=True
    )

    account = subparsers.add_parser("account")
    account.add_argument("action", choices=("suspend", "reactivate"))
    account.add_argument("github_user_id", type=int)
    account.add_argument(
        "--reason", choices=tuple(item.value for item in OperatorReason), required=True
    )

    credential = subparsers.add_parser("credential")
    credential.add_argument("action", choices=("revoke",))
    credential.add_argument("credential_id", type=UUID)
    credential.add_argument(
        "--reason", choices=tuple(item.value for item in OperatorReason), required=True
    )

    parsed = parser.parse_args(arguments)
    try:
        allowlist = parse_operator_allowlist(os.environ.get("CONTINUUM_OPERATOR_GITHUB_USER_IDS"))
        require_operator_actor(parsed.actor_github_user_id, allowlist)
        database_url = os.environ.get("CONTINUUM_OPERATOR_DATABASE_URL", "")
        if not database_url:
            raise OperatorActionError("operator_database_url_required")
        engine = create_operator_engine(database_url)
        try:
            service = OperatorActionService(engine)
            if parsed.group == "inspect":
                methods: dict[str, Any] = {
                    "overview": service.overview,
                    "unknown-effects": lambda: service.unknown_effects(parsed.limit),
                    "uncertain-allocations": lambda: service.uncertain_allocations(parsed.limit),
                    "inbox-lag": lambda: service.pending_inbox(parsed.limit),
                    "suspended-accounts": lambda: service.suspended_accounts(parsed.limit),
                    "audit": lambda: service.audit_events(parsed.limit),
                }
                _print_json(methods[parsed.view]())
            elif parsed.group == "admission":
                changed = service.set_admission_stopped(
                    parsed.action == "stop",
                    parsed.actor_github_user_id,
                    OperatorReason(parsed.reason),
                )
                _print_json({"changed": changed})
            else:
                if parsed.group == "account":
                    changed = service.set_account_suspended(
                        parsed.github_user_id,
                        parsed.action == "suspend",
                        parsed.actor_github_user_id,
                        OperatorReason(parsed.reason),
                    )
                else:
                    changed = service.revoke_provider_credential(
                        parsed.credential_id,
                        parsed.actor_github_user_id,
                        OperatorReason(parsed.reason),
                    )
                _print_json({"changed": changed})
        finally:
            engine.dispose()
        return 0
    except OperatorActionError as error:
        print(error.code, file=sys.stderr)
        return 2


def _print_json(value: Any) -> None:
    if hasattr(value, "__dataclass_fields__"):
        value = asdict(value)
    elif isinstance(value, tuple):
        value = [asdict(item) if hasattr(item, "__dataclass_fields__") else item for item in value]
    print(
        json.dumps(
            value,
            default=lambda item: item.isoformat() if isinstance(item, datetime) else str(item),
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
