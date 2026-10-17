"""Safe owner-scoped projection of effects that still need attention."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import Engine, text

from continuum_api.runtime.persistence import Principal, RuntimeRepository

ExternalAttentionStatus = Literal[
    "prepared", "dispatching", "unknown", "conflict", "reserved", "pending"
]
PullRequestState = Literal["open", "closed", "merged"]


@dataclass(frozen=True, slots=True)
class ExternalAttentionItem:
    kind: Literal["external_operation", "dispatch_intent", "resource_reservation", "cleanup_intent"]
    effect_id: UUID
    status: ExternalAttentionStatus
    created_at: datetime
    updated_at: datetime | None = None
    pr_number: int | None = None
    pr_id: int | None = None
    pr_url: str | None = None
    pr_state: PullRequestState | None = None
    can_reconcile: bool = False


@dataclass(frozen=True, slots=True)
class ExternalAttentionPage:
    effects: tuple[ExternalAttentionItem, ...]
    has_more: bool


class ExternalAttentionNotFound(Exception):
    """The owner-scoped run or effect does not exist."""


class ExternalAttentionService:
    """Read safe summaries and reconcile the existing PR operation only."""

    def __init__(self, engine: Engine, principal: Principal) -> None:
        self._engine = engine
        self._principal = principal

    def list_for_run(self, run_id: UUID) -> ExternalAttentionPage | None:
        """Return unresolved effects, or None when the run is outside this owner."""
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            owner_id = self._principal.account_id
            exists = connection.execute(
                text("SELECT 1 FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
                {"owner": owner_id, "run": run_id},
            ).scalar_one_or_none()
            if exists is None:
                return None

            rows = (
                connection.execute(
                    text(
                        "SELECT operation.operation_id, operation.operation_key, operation.status, "
                        "operation.created_at, operation.updated_at, "
                        "operation.approval_id, operation.approval_sha256, "
                        "pr_ref.details AS reference, pr_state_observation.details AS remote_state "
                        "FROM runtime_private.external_operations AS operation "
                        "LEFT JOIN LATERAL (SELECT details FROM "
                        "runtime_private.external_operation_observations "
                        "WHERE owner_id=operation.owner_id AND run_id=operation.run_id "
                        "AND operation_id=operation.operation_id "
                        "AND observation_key='pull-request-reference' "
                        "ORDER BY observed_at DESC, observation_id DESC LIMIT 1) AS pr_ref ON true "
                        "LEFT JOIN LATERAL (SELECT details FROM "
                        "runtime_private.external_operation_observations "
                        "WHERE owner_id=operation.owner_id AND run_id=operation.run_id "
                        "AND operation_id=operation.operation_id "
                        "AND observation_key LIKE 'pull-request-state-%' "
                        "ORDER BY observed_at DESC, observation_id DESC LIMIT 1) "
                        "AS pr_state_observation ON true "
                        "WHERE operation.owner_id=:owner AND operation.run_id=:run "
                        "AND operation.status IN ('prepared','dispatching','unknown','conflict') "
                        "ORDER BY operation.created_at, operation.operation_id LIMIT 101"
                    ),
                    {"owner": owner_id, "run": run_id},
                )
                .mappings()
                .all()
            )
            dispatches = (
                connection.execute(
                    text(
                        "SELECT dispatch_id, authorized_at FROM runtime_private.dispatch_intents "
                        "WHERE owner_id=:owner AND run_id=:run AND (observation IS NULL OR "
                        "coalesce(observation->>'state', '') NOT IN "
                        "('completed','failed','confirmed')) "
                        "ORDER BY authorized_at, dispatch_id LIMIT 101"
                    ),
                    {"owner": owner_id, "run": run_id},
                )
                .mappings()
                .all()
            )
            reservations = (
                connection.execute(
                    text(
                        "SELECT reservation_id, status, created_at FROM "
                        "runtime_private.resource_reservations WHERE owner_id=:owner "
                        "AND run_id=:run "
                        "AND status IN ('reserved','unknown') "
                        "ORDER BY created_at, reservation_id LIMIT 101"
                    ),
                    {"owner": owner_id, "run": run_id},
                )
                .mappings()
                .all()
            )
            cleanups = (
                connection.execute(
                    text(
                        "SELECT cleanup_id, cleanup_kind, created_at FROM "
                        "runtime_private.cleanup_intents WHERE owner_id=:owner AND run_id=:run "
                        "AND status='pending' ORDER BY created_at, cleanup_id LIMIT 101"
                    ),
                    {"owner": owner_id, "run": run_id},
                )
                .mappings()
                .all()
            )

        has_more = any(len(group) > 100 for group in (rows, dispatches, reservations, cleanups))
        result: list[ExternalAttentionItem] = []
        for row in rows[:100]:
            reference = row["reference"] or {}
            remote_state = row["remote_state"] or {}
            number = reference.get("number") if isinstance(reference, dict) else None
            identifier = reference.get("id") if isinstance(reference, dict) else None
            url = reference.get("url") if isinstance(reference, dict) else None
            state = remote_state.get("state") if isinstance(remote_state, dict) else None
            if type(number) is not int or number <= 0:
                number = None
            if type(identifier) is not int or identifier <= 0:
                identifier = None
            if not _valid_github_pr_url(url, number):
                url = None
            if state not in {"open", "closed", "merged"}:
                state = None
            result.append(
                ExternalAttentionItem(
                    kind="external_operation",
                    effect_id=row["operation_id"],
                    status=cast(ExternalAttentionStatus, row["status"]),
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                    pr_number=number,
                    pr_id=identifier,
                    pr_url=url,
                    pr_state=cast(PullRequestState | None, state),
                    can_reconcile=(
                        row["operation_key"] == "github:pull-request"
                        and row["approval_id"] is not None
                        and row["approval_sha256"] is not None
                    ),
                )
            )
        result.extend(
            ExternalAttentionItem(
                "dispatch_intent", row["dispatch_id"], "unknown", row["authorized_at"]
            )
            for row in dispatches[:100]
        )
        result.extend(
            ExternalAttentionItem(
                "resource_reservation",
                row["reservation_id"],
                cast(ExternalAttentionStatus, row["status"]),
                row["created_at"],
            )
            for row in reservations[:100]
        )
        result.extend(
            ExternalAttentionItem("cleanup_intent", row["cleanup_id"], "pending", row["created_at"])
            for row in cleanups[:100]
        )
        return ExternalAttentionPage(tuple(result), has_more)

    def get_pr_operation(self, run_id: UUID, operation_id: UUID) -> tuple[UUID, str] | None:
        """Return the stored approval binding for this run's PR operation."""
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = (
                connection.execute(
                    text(
                        "SELECT approval_id, encode(approval_sha256, 'hex') AS approval_sha256 "
                        "FROM runtime_private.external_operations WHERE owner_id=:owner "
                        "AND run_id=:run AND operation_id=:operation AND operation_key="
                        "'github:pull-request'"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "run": run_id,
                        "operation": operation_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
        if row is None or row["approval_id"] is None or row["approval_sha256"] is None:
            return None
        return row["approval_id"], row["approval_sha256"]


def _valid_github_pr_url(url: object, number: int | None) -> bool:
    if not isinstance(url, str) or number is None:
        return False
    pattern = rf"https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/{number}"
    return bool(re.fullmatch(pattern, url))
