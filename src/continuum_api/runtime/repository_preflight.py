"""Durable, principal-scoped reservations and receipts for repository preflight."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import Engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.repositories.preflight import (
    EligibilityReceipt,
    PreflightReservation,
    RepositoryPreflightError,
)
from continuum_api.runtime.persistence import Principal

_RESERVATION_SECONDS = 600
_HOURLY_CHECK_LIMIT = 5


class RepositoryPreflightStore:
    """Persist preflight admission and cached eligibility metadata in PostgreSQL.

    Each method uses a short engine-owned transaction. Repository access,
    manifest reads, and other network operations belong outside this store.
    The principal is installed in a transaction-local GUC so forced RLS and
    explicit owner predicates enforce the same account boundary.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def reserve(
        self,
        principal: Principal,
        repo_id: int,
        commit_sha: str,
        profile_id: str,
        profile_version: str,
    ) -> PreflightReservation:
        owner_id = principal.account_id
        check_id = uuid4()
        try:
            with self._connection(principal) as connection:
                # Serialize admission per account across API processes. The lock
                # is transaction-scoped and never held across GitHub requests.
                connection.execute(
                    text(
                        "SELECT pg_advisory_xact_lock(hashtextextended(CAST(:owner_id AS text), 0))"
                    ),
                    {"owner_id": owner_id},
                )
                user = (
                    connection.execute(
                        text(
                            "SELECT github_user_id, status FROM runtime_private.users "
                            "WHERE user_id = :owner_id"
                        ),
                        {"owner_id": owner_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if user is None or user["status"] != "active":
                    raise RepositoryPreflightError("repository_actor_unavailable")

                connection.execute(
                    text(
                        "UPDATE runtime_private.repository_preflight_checks "
                        "SET status = 'expired', finished_at = clock_timestamp() "
                        "WHERE owner_id = :owner_id AND status = 'active' "
                        "AND deadline_at <= clock_timestamp()"
                    ),
                    {"owner_id": owner_id},
                )
                has_active: bool = connection.execute(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM "
                        "runtime_private.repository_preflight_checks "
                        "WHERE owner_id = :owner_id AND status = 'active')"
                    ),
                    {"owner_id": owner_id},
                ).scalar_one()
                if has_active:
                    raise RepositoryPreflightError("repository_preflight_in_progress")

                checks_last_hour: int = connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.repository_preflight_checks "
                        "WHERE owner_id = :owner_id "
                        "AND created_at > clock_timestamp() - interval '1 hour'"
                    ),
                    {"owner_id": owner_id},
                ).scalar_one()
                if checks_last_hour >= _HOURLY_CHECK_LIMIT:
                    raise RepositoryPreflightError("repository_preflight_rate_limited")

                self._delete_expired_cache(connection, owner_id)
                # Keep only the rolling quota ledger; completed records older
                # than its window have no remaining authorization purpose.
                connection.execute(
                    text(
                        "DELETE FROM runtime_private.repository_preflight_checks "
                        "WHERE owner_id = :owner_id AND status <> 'active' "
                        "AND created_at <= clock_timestamp() - interval '1 hour'"
                    ),
                    {"owner_id": owner_id},
                )
                row = connection.execute(
                    text(
                        "INSERT INTO runtime_private.repository_preflight_checks "
                        "(check_id, owner_id, github_repository_id, commit_sha, "
                        "profile_id, profile_version, deadline_at) VALUES "
                        "(:check_id, :owner_id, :repo_id, :commit_sha, :profile_id, "
                        ":profile_version, clock_timestamp() + "
                        "(:reservation_seconds * interval '1 second')) "
                        "RETURNING deadline_at"
                    ),
                    {
                        "check_id": check_id,
                        "owner_id": owner_id,
                        "repo_id": repo_id,
                        "commit_sha": commit_sha,
                        "profile_id": profile_id,
                        "profile_version": profile_version,
                        "reservation_seconds": _RESERVATION_SECONDS,
                    },
                ).one()
                return PreflightReservation(
                    check_id=check_id,
                    github_user_id=user["github_user_id"],
                    deadline_at=row.deadline_at,
                )
        except RepositoryPreflightError:
            raise
        except SQLAlchemyError:
            raise RepositoryPreflightError("repository_preflight_storage_unavailable") from None

    def get_cached(
        self,
        principal: Principal,
        check_id: UUID,
        repo_id: int,
        commit_sha: str,
        profile_id: str,
        profile_version: str,
    ) -> EligibilityReceipt | None:
        try:
            with self._connection(principal) as connection:
                check = self._lock_active_check(connection, principal, check_id)
                self._assert_key_matches(check, repo_id, commit_sha, profile_id, profile_version)
                self._delete_expired_cache(connection, principal.account_id)
                row = connection.execute(
                    text(
                        "SELECT receipt FROM runtime_private.repository_eligibility_cache "
                        "WHERE owner_id = :owner_id AND github_repository_id = :repo_id "
                        "AND commit_sha = :commit_sha AND profile_id = :profile_id "
                        "AND profile_version = :profile_version "
                        "AND expires_at > clock_timestamp()"
                    ),
                    {
                        "owner_id": principal.account_id,
                        "repo_id": repo_id,
                        "commit_sha": commit_sha,
                        "profile_id": profile_id,
                        "profile_version": profile_version,
                    },
                ).one_or_none()
                self._assert_unexpired(connection, check)
                if row is None:
                    return None
                try:
                    receipt = EligibilityReceipt.from_cache_payload(row.receipt)
                except RepositoryPreflightError:
                    raise
                except Exception:
                    raise RepositoryPreflightError(
                        "repository_preflight_storage_unavailable"
                    ) from None
                self._assert_unexpired(connection, check)
                return receipt
        except RepositoryPreflightError:
            raise
        except SQLAlchemyError:
            raise RepositoryPreflightError("repository_preflight_storage_unavailable") from None

    def put_cached(
        self,
        principal: Principal,
        check_id: UUID,
        receipt: EligibilityReceipt,
    ) -> None:
        try:
            with self._connection(principal) as connection:
                check = self._lock_active_check(connection, principal, check_id)
                self._assert_key_matches(
                    check,
                    receipt.repository_id,
                    receipt.commit_sha,
                    receipt.profile_id,
                    receipt.profile_version,
                )
                payload = receipt.to_cache_payload()
                # Validate the model's serialized shape and the exact JSONB
                # size before attempting the write; the table constraint is a
                # second guard against oversized metadata.
                EligibilityReceipt.from_cache_payload(payload)
                receipt_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
                jsonb_size: int = connection.execute(
                    text("SELECT octet_length(CAST(:receipt AS jsonb)::text)"),
                    {"receipt": receipt_json},
                ).scalar_one()
                if jsonb_size > 4096:
                    raise RepositoryPreflightError("repository_preflight_cache_corrupt")
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.repository_eligibility_cache "
                        "(owner_id, github_repository_id, commit_sha, profile_id, "
                        "profile_version, receipt, cached_at, expires_at) VALUES "
                        "(:owner_id, :repo_id, :commit_sha, :profile_id, :profile_version, "
                        "CAST(:receipt AS jsonb), clock_timestamp(), "
                        "clock_timestamp() + interval '10 minutes') "
                        "ON CONFLICT (owner_id, github_repository_id, commit_sha, "
                        "profile_id, profile_version) DO UPDATE SET "
                        "receipt = EXCLUDED.receipt, cached_at = EXCLUDED.cached_at, "
                        "expires_at = EXCLUDED.expires_at"
                    ),
                    {
                        "owner_id": principal.account_id,
                        "repo_id": receipt.repository_id,
                        "commit_sha": receipt.commit_sha,
                        "profile_id": receipt.profile_id,
                        "profile_version": receipt.profile_version,
                        "receipt": receipt_json,
                    },
                )
                self._assert_unexpired(connection, check)
        except RepositoryPreflightError:
            raise
        except SQLAlchemyError:
            raise RepositoryPreflightError("repository_preflight_storage_unavailable") from None

    def release(
        self,
        principal: Principal,
        check_id: UUID,
        outcome: Literal["completed", "failed"],
    ) -> None:
        if outcome not in {"completed", "failed"}:
            raise ValueError("repository_preflight_outcome_invalid")
        try:
            with self._connection(principal) as connection:
                self._lock_active_check(connection, principal, check_id)
                self._delete_expired_cache(connection, principal.account_id)
                updated = connection.execute(
                    text(
                        "UPDATE runtime_private.repository_preflight_checks "
                        "SET status = :outcome, finished_at = clock_timestamp() "
                        "WHERE owner_id = :owner_id AND check_id = :check_id "
                        "AND status = 'active' AND deadline_at > clock_timestamp()"
                    ),
                    {
                        "outcome": outcome,
                        "owner_id": principal.account_id,
                        "check_id": check_id,
                    },
                )
                if updated.rowcount != 1:
                    raise RepositoryPreflightError("repository_preflight_reservation_lost")
        except RepositoryPreflightError:
            raise
        except SQLAlchemyError:
            raise RepositoryPreflightError("repository_preflight_storage_unavailable") from None

    @contextmanager
    def _connection(self, principal: Principal) -> Iterator[Connection]:
        with self._engine.begin() as connection:
            connection.execute(
                text("SELECT set_config('continuum.principal_id', :principal_id, true)"),
                {"principal_id": str(principal.account_id)},
            )
            yield connection

    @staticmethod
    def _lock_active_check(
        connection: Connection, principal: Principal, check_id: UUID
    ) -> Mapping[str, Any]:
        row = (
            connection.execute(
                text(
                    "SELECT check_id, github_repository_id, commit_sha, profile_id, "
                    "profile_version, status, deadline_at "
                    "FROM runtime_private.repository_preflight_checks "
                    "WHERE owner_id = :owner_id AND check_id = :check_id FOR UPDATE"
                ),
                {"owner_id": principal.account_id, "check_id": check_id},
            )
            .mappings()
            .one_or_none()
        )
        if row is None or row["status"] != "active":
            raise RepositoryPreflightError("repository_preflight_reservation_lost")
        check = dict(row)
        RepositoryPreflightStore._assert_unexpired(connection, check)
        return check

    @staticmethod
    def _assert_unexpired(connection: Connection, check: Mapping[str, Any]) -> None:
        # Read the database clock after taking the row lock and again after
        # cache I/O, so a request that crosses its deadline cannot return or
        # commit a receipt as if it still owned the reservation.
        now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
        if check["deadline_at"] <= now:
            raise RepositoryPreflightError("repository_preflight_reservation_lost")

    @staticmethod
    def _delete_expired_cache(connection: Connection, owner_id: UUID) -> None:
        connection.execute(
            text(
                "DELETE FROM runtime_private.repository_eligibility_cache "
                "WHERE owner_id = :owner_id AND expires_at <= clock_timestamp()"
            ),
            {"owner_id": owner_id},
        )

    @staticmethod
    def _assert_key_matches(
        row: Mapping[str, Any],
        repo_id: int,
        commit_sha: str,
        profile_id: str,
        profile_version: str,
    ) -> None:
        if (
            row["github_repository_id"] != repo_id
            or row["commit_sha"] != commit_sha
            or row["profile_id"] != profile_id
            or row["profile_version"] != profile_version
        ):
            raise RepositoryPreflightError("repository_preflight_reservation_lost")
