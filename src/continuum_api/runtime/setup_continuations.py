"""Trusted, owner-scoped, claim-once handoff for issue-comment setup flows.

This module deliberately exposes no HTTP route and creates no run. A webhook or
command-ingress adapter may capture the verified original comment, and a later
setup flow may claim its exact body after repository defaults are configured.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.runtime.persistence import Principal, RuntimeRepository

_MAX_BODY_BYTES = 16_384
_MAX_FUTURE_SKEW = timedelta(minutes=5)
_MAX_AGE = timedelta(hours=72)


class SetupContinuationError(RuntimeError):
    """Stable failure code for an invalid, stale, or already-used continuation."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class SetupContinuation:
    continuation_id: UUID
    github_repository_id: int
    github_comment_id: int
    github_issue_number: int
    comment_created_at: datetime
    submission_body: str = field(repr=False)
    submission_sha256: str


class SetupContinuationService:
    """Capture trusted comments and atomically return their original content once."""

    def __init__(self, engine: Engine, principal: Principal) -> None:
        self._engine = engine
        self._principal = principal

    @contextmanager
    def _connection(self) -> Iterator[Connection]:
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                yield connection
        except SQLAlchemyError:
            raise SetupContinuationError("setup_continuation_storage_unavailable") from None

    def capture(
        self,
        *,
        github_repository_id: int,
        github_comment_id: int,
        github_issue_number: int,
        comment_author_github_id: int,
        comment_created_at: datetime,
        submission_body: str,
    ) -> SetupContinuation:
        """Persist an exact webhook-verified submission only for an existing registration."""
        if (
            type(github_repository_id) is not int
            or github_repository_id <= 0
            or type(github_comment_id) is not int
            or github_comment_id <= 0
            or type(github_issue_number) is not int
            or github_issue_number <= 0
            or type(comment_author_github_id) is not int
            or comment_author_github_id <= 0
            or not isinstance(comment_created_at, datetime)
            or comment_created_at.tzinfo is None
            or not isinstance(submission_body, str)
        ):
            raise SetupContinuationError("setup_continuation_invalid")
        encoded_body = submission_body.encode("utf-8")
        if not encoded_body or len(encoded_body) > _MAX_BODY_BYTES:
            raise SetupContinuationError("setup_continuation_invalid")
        digest = _submission_digest(github_issue_number, encoded_body)
        comment_created_at = comment_created_at.astimezone(UTC)

        with self._connection() as connection:
            eligibility = (
                connection.execute(
                    text(
                        "SELECT u.github_user_id, u.status, "
                        "u.created_at AS account_created_at, "
                        "r.created_at AS registration_created_at, clock_timestamp() AS now "
                        "FROM runtime_private.users AS u JOIN "
                        "runtime_private.repository_registrations AS r "
                        "ON r.owner_id=u.user_id WHERE u.user_id=:owner "
                        "AND r.github_repository_id=:repo_id"
                    ),
                    {"owner": self._principal.account_id, "repo_id": github_repository_id},
                )
                .mappings()
                .one_or_none()
            )
            if eligibility is None:
                raise SetupContinuationError("setup_continuation_repository_unregistered")
            if eligibility["status"] != "active":
                raise SetupContinuationError("setup_continuation_account_inactive")
            if comment_author_github_id != eligibility["github_user_id"]:
                raise SetupContinuationError("setup_continuation_author_mismatch")
            now = eligibility["now"]
            if (
                comment_created_at < eligibility["account_created_at"]
                or comment_created_at < eligibility["registration_created_at"]
                or comment_created_at > now + _MAX_FUTURE_SKEW
                or comment_created_at < now - _MAX_AGE
            ):
                raise SetupContinuationError("setup_continuation_stale")

            continuation_id = uuid4()
            row = (
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.setup_continuations "
                        "(continuation_id, owner_id, github_user_id, github_repository_id, "
                        "github_comment_id, github_issue_number, comment_created_at, "
                        "submission_body, "
                        "submission_sha256, "
                        "expires_at) VALUES (:id, :owner, :github_user_id, :repo_id, :comment_id, "
                        ":issue_number, :created_at, :body, :digest, "
                        ":created_at + interval '72 hours') "
                        "ON CONFLICT (owner_id, github_repository_id, github_comment_id) "
                        "DO NOTHING "
                        "RETURNING continuation_id, github_repository_id, github_comment_id, "
                        "github_issue_number, "
                        "comment_created_at, submission_body, submission_sha256"
                    ),
                    {
                        "id": continuation_id,
                        "owner": self._principal.account_id,
                        "github_user_id": comment_author_github_id,
                        "repo_id": github_repository_id,
                        "comment_id": github_comment_id,
                        "issue_number": github_issue_number,
                        "created_at": comment_created_at,
                        "body": submission_body,
                        "digest": digest,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                row = (
                    connection.execute(
                        text(
                            "SELECT continuation_id, github_repository_id, github_comment_id, "
                            "github_issue_number, "
                            "comment_created_at, submission_body, submission_sha256 "
                            "FROM runtime_private.setup_continuations WHERE owner_id=:owner "
                            "AND github_repository_id=:repo_id AND github_comment_id=:comment_id"
                        ),
                        {
                            "owner": self._principal.account_id,
                            "repo_id": github_repository_id,
                            "comment_id": github_comment_id,
                        },
                    )
                    .mappings()
                    .one()
                )
                if (
                    row["submission_sha256"] != digest
                    or row["submission_body"] != submission_body
                    or row["github_issue_number"] != github_issue_number
                    or row["comment_created_at"] != comment_created_at
                ):
                    raise SetupContinuationError("setup_continuation_identity_conflict")
            return _continuation(row)

    def claim(self, continuation_id: UUID) -> SetupContinuation:
        """Claim the exact captured submission once, after defaults are ready."""
        if not isinstance(continuation_id, UUID):
            raise SetupContinuationError("setup_continuation_invalid")
        with self._connection() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT c.continuation_id, c.github_user_id, c.github_repository_id, "
                        "c.github_comment_id, c.github_issue_number, c.comment_created_at, "
                        "c.submission_body, c.submission_sha256, c.expires_at, c.claimed_at, "
                        "u.status, "
                        "u.created_at AS account_created_at, "
                        "r.created_at AS registration_created_at, r.provider, r.profile_id, "
                        "r.credential_id, clock_timestamp() AS now "
                        "FROM runtime_private.setup_continuations AS c "
                        "JOIN runtime_private.users AS u ON u.user_id=c.owner_id "
                        "JOIN runtime_private.repository_registrations AS r "
                        "ON r.owner_id=c.owner_id "
                        "AND r.github_repository_id=c.github_repository_id "
                        "WHERE c.owner_id=:owner AND c.continuation_id=:id FOR UPDATE OF c"
                    ),
                    {"owner": self._principal.account_id, "id": continuation_id},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise SetupContinuationError("setup_continuation_not_found")
            if row["claimed_at"] is not None:
                raise SetupContinuationError("setup_continuation_already_claimed")
            if row["status"] != "active":
                raise SetupContinuationError("setup_continuation_account_inactive")
            if (
                row["expires_at"] <= row["now"]
                or row["comment_created_at"] < row["account_created_at"]
                or row["comment_created_at"] < row["registration_created_at"]
                or row["comment_created_at"] > row["now"] + _MAX_FUTURE_SKEW
            ):
                raise SetupContinuationError("setup_continuation_stale")
            if row["provider"] is None or row["profile_id"] is None:
                raise SetupContinuationError("setup_continuation_setup_incomplete")
            body = row["submission_body"]
            if (
                not isinstance(body, str)
                or _submission_digest(row["github_issue_number"], body.encode("utf-8"))
                != row["submission_sha256"]
            ):
                raise SetupContinuationError("setup_continuation_content_expired")
            if row["credential_id"] is not None:
                usable_credential: bool = connection.execute(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM runtime_private.provider_credentials "
                        "WHERE owner_id=:owner AND credential_id=:credential "
                        "AND provider=:provider AND scope='saved' AND status='active')"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "credential": row["credential_id"],
                        "provider": row["provider"],
                    },
                ).scalar_one()
                if not usable_credential:
                    raise SetupContinuationError("setup_continuation_setup_incomplete")
            updated = (
                connection.execute(
                    text(
                        "UPDATE runtime_private.setup_continuations "
                        "SET claimed_at=clock_timestamp() "
                        "WHERE owner_id=:owner AND continuation_id=:id AND claimed_at IS NULL "
                        "RETURNING continuation_id, github_repository_id, github_comment_id, "
                        "github_issue_number, "
                        "comment_created_at, submission_body, submission_sha256"
                    ),
                    {"owner": self._principal.account_id, "id": continuation_id},
                )
                .mappings()
                .one_or_none()
            )
            if updated is None:
                raise SetupContinuationError("setup_continuation_already_claimed")
            return _continuation(updated)

    def sweep_expired(self, *, limit: int = 100) -> int:
        """Scrub expired comment text while retaining its replay-prevention tombstone."""
        if type(limit) is not int or not 1 <= limit <= 500:
            raise SetupContinuationError("setup_continuation_limit_invalid")
        with self._connection() as connection:
            ids: Sequence[UUID] = (
                connection.execute(
                    text(
                        "SELECT continuation_id FROM runtime_private.setup_continuations "
                        "WHERE owner_id=:owner AND expires_at<=clock_timestamp() "
                        "AND submission_body IS NOT NULL ORDER BY expires_at, continuation_id "
                        "LIMIT :limit FOR UPDATE SKIP LOCKED"
                    ),
                    {"owner": self._principal.account_id, "limit": limit},
                )
                .scalars()
                .all()
            )
            if not ids:
                return 0
            result = connection.execute(
                text(
                    "UPDATE runtime_private.setup_continuations SET submission_body=NULL "
                    "WHERE owner_id=:owner AND continuation_id = ANY(:ids) "
                    "AND expires_at<=clock_timestamp() AND submission_body IS NOT NULL"
                ),
                {"owner": self._principal.account_id, "ids": ids},
            )
            return result.rowcount


def _continuation(row: Mapping[Any, Any]) -> SetupContinuation:
    values = dict(row)  # SQLAlchemy RowMapping
    if not isinstance(values["submission_body"], str):
        raise SetupContinuationError("setup_continuation_content_expired")
    return SetupContinuation(
        continuation_id=values["continuation_id"],
        github_repository_id=values["github_repository_id"],
        github_comment_id=values["github_comment_id"],
        github_issue_number=values["github_issue_number"],
        comment_created_at=values["comment_created_at"],
        submission_body=values["submission_body"],
        submission_sha256=bytes(values["submission_sha256"]).hex(),
    )


def _submission_digest(issue_number: int, body: bytes) -> bytes:
    return hashlib.sha256(
        issue_number.to_bytes(8, byteorder="big", signed=False) + b"\0" + body
    ).digest()
