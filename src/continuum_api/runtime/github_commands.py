"""Fresh, owner-scoped intake for commands on ordinary GitHub issue comments.

Intake records intent for later command handlers. It never admits a run or
executes a decision itself.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol, cast

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.auth.github import GitHubIssueCommentSnapshot
from continuum_api.runtime.persistence import Principal, RuntimeRepository

_MAX_BODY_BYTES = 16_384
_MAX_FUTURE_SKEW = timedelta(minutes=5)
_MAX_AGE = timedelta(hours=72)
_UUID = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
CommandKind = Literal["run", "status", "cancel", "approve", "reject", "changes"]
_COMMANDS = {
    "run": re.compile(r"^/continumm run$"),
    "status": re.compile(rf"^/continumm status ({_UUID})$"),
    "cancel": re.compile(rf"^/continumm cancel ({_UUID})$"),
    "approve": re.compile(rf"^/continumm approve ({_UUID}) ([0-9a-fA-F]{{64}})$"),
    "reject": re.compile(rf"^/continumm reject ({_UUID})$"),
    "changes": re.compile(rf"^/continumm changes ({_UUID})$"),
}


class GitHubCommandError(RuntimeError):
    """Stable command-intake error code."""

    def __init__(self, code: str, *, retryable: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True, slots=True)
class NormalizedGitHubCommand:
    kind: CommandKind
    target_id: str | None = None
    action_sha256: str | None = None
    feedback: str | None = field(default=None, repr=False)

    def metadata(self) -> dict[str, str]:
        """Return only command identity fields; feedback is short-lived content."""
        values: dict[str, str] = {"kind": self.kind}
        if self.target_id is not None:
            values["target_id"] = self.target_id
        if self.action_sha256 is not None:
            values["action_sha256"] = self.action_sha256
        return values


def parse_github_command(body: str) -> NormalizedGitHubCommand | None:
    """Parse an exact first-line command; quoted/code/natural-language text is inert."""
    if not isinstance(body, str):
        return None
    try:
        encoded = body.encode("utf-8")
    except UnicodeEncodeError:
        return None
    if not encoded or len(encoded) > _MAX_BODY_BYTES:
        return None
    first, separator, remainder = body.partition("\n")
    # A carriage return is not accepted as a command-line normalizer.
    if first.endswith("\r"):
        first = first[:-1]
    if first.startswith((">", "`", "~", " ", "\t", '"', "'")):
        return None
    for raw_kind, pattern in _COMMANDS.items():
        match = pattern.fullmatch(first)
        if match is None:
            continue
        kind = cast(CommandKind, raw_kind)
        feedback = remainder if separator else None
        if feedback is not None and feedback.endswith("\r"):
            feedback = feedback[:-1]
        if kind == "changes":
            if not feedback or not feedback.strip():
                return None
            if len(feedback.encode("utf-8")) > _MAX_BODY_BYTES:
                return None
        elif feedback is not None and feedback.strip():
            # Only changes carries user-supplied review feedback.
            return None
        captures = match.groups()
        target = captures[0].lower() if captures else None
        action_sha = captures[1].lower() if len(captures) > 1 else None
        return NormalizedGitHubCommand(kind, target, action_sha, feedback)
    return None


@dataclass(frozen=True, slots=True)
class IssueCommentCommandEvent:
    action: str
    repository_id: int
    issue_id: int
    issue_number: int
    comment_id: int
    author_github_id: int
    author_type: str
    created_at: datetime
    updated_at: datetime
    body: str = field(repr=False)
    is_pull_request: bool = False


class CurrentCommentReader(Protocol):
    """Reader must recheck current user write access and App read access first."""

    async def current_issue_comment(
        self, *, repository_id: int, github_user_id: int, comment_id: int, issue_number: int
    ) -> GitHubIssueCommentSnapshot: ...


@dataclass(frozen=True, slots=True)
class GitHubCommandReceipt:
    repository_id: int
    comment_id: int
    issue_number: int
    accepted_at: datetime
    expires_at: datetime
    command: NormalizedGitHubCommand = field(repr=False)
    duplicate: bool = False


class GitHubCommandService:
    """Verify a fresh owner command and persist its dedup receipt atomically."""

    def __init__(
        self, engine: Engine, principal: Principal, comment_reader: CurrentCommentReader
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._comment_reader = comment_reader

    async def accept_created_comment(
        self, event: IssueCommentCommandEvent
    ) -> GitHubCommandReceipt | None:
        """Accept one unchanged owner-authored issue comment; return None for inert input."""
        _validate_event(event)
        if event.action != "created" or event.author_type.casefold() != "user":
            return None
        if event.is_pull_request:
            return None

        registration = await asyncio.to_thread(self._registration, event.repository_id)
        if (
            registration is None
            or registration["status"] != "active"
            or registration["github_user_id"] != event.author_github_id
        ):
            return None
        command = parse_github_command(event.body)
        if command is None:
            return None

        try:
            current = await self._comment_reader.current_issue_comment(
                repository_id=event.repository_id,
                github_user_id=event.author_github_id,
                comment_id=event.comment_id,
                issue_number=event.issue_number,
            )
        except Exception:
            raise GitHubCommandError("github_comment_unverifiable", retryable=True) from None
        if not _same_current_comment(event, current):
            return None

        created_at = event.created_at.astimezone(UTC)
        try:
            return await asyncio.to_thread(
                self._persist,
                event=event,
                created_at=created_at,
                command=command,
            )
        except GitHubCommandError:
            raise
        except SQLAlchemyError:
            raise GitHubCommandError("github_command_storage_unavailable", retryable=True) from None

    def _registration(self, repository_id: int) -> Mapping[Any, Any] | None:
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                return cast(
                    Mapping[Any, Any] | None,
                    connection.execute(
                        text(
                            "SELECT u.github_user_id, u.status "
                            "FROM runtime_private.users AS u JOIN "
                            "runtime_private.repository_registrations AS r ON r.owner_id=u.user_id "
                            "WHERE u.user_id=:owner AND r.github_repository_id=:repo_id"
                        ),
                        {"owner": self._principal.account_id, "repo_id": repository_id},
                    )
                    .mappings()
                    .one_or_none(),
                )
        except SQLAlchemyError:
            raise GitHubCommandError("github_command_storage_unavailable", retryable=True) from None

    def _persist(
        self,
        *,
        event: IssueCommentCommandEvent,
        created_at: datetime,
        command: NormalizedGitHubCommand,
    ) -> GitHubCommandReceipt:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            authorization = (
                connection.execute(
                    text(
                        "SELECT u.github_user_id, u.status, u.created_at AS account_created_at, "
                        "r.created_at AS registration_created_at, clock_timestamp() AS now "
                        "FROM runtime_private.users AS u JOIN "
                        "runtime_private.repository_registrations AS r ON r.owner_id=u.user_id "
                        "WHERE u.user_id=:owner AND r.github_repository_id=:repo "
                        "FOR SHARE OF r"
                    ),
                    {"owner": self._principal.account_id, "repo": event.repository_id},
                )
                .mappings()
                .one_or_none()
            )
            if (
                authorization is None
                or authorization["status"] != "active"
                or authorization["github_user_id"] != event.author_github_id
            ):
                raise GitHubCommandError("github_command_authorization_changed")
            now = authorization["now"]
            if (
                created_at < authorization["account_created_at"]
                or created_at < authorization["registration_created_at"]
                or created_at > now + _MAX_FUTURE_SKEW
                or created_at < now - _MAX_AGE
            ):
                raise GitHubCommandError("github_comment_stale")
            metadata = command.metadata()
            canonical = json.dumps(metadata, sort_keys=True, separators=(",", ":"))
            digest_material = json.dumps(
                {
                    **metadata,
                    "repository_id": event.repository_id,
                    "comment_id": event.comment_id,
                    "issue_id": event.issue_id,
                    "issue_number": event.issue_number,
                    "author_id": event.author_github_id,
                    "comment_created_at": created_at.isoformat(),
                    "original_body": event.body,
                },
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            command_digest = hashlib.sha256(digest_material.encode("utf-8")).digest()
            payload_json = None
            if command.feedback is not None:
                payload_json = json.dumps({"feedback": command.feedback}, ensure_ascii=False)
            result = (
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.github_command_receipts "
                        "(owner_id, repository_id, comment_id, issue_id, issue_number, author_id, "
                        "comment_created_at, command_kind, command_metadata, command_sha256, "
                        "expires_at) "
                        "VALUES (:owner, :repo, :comment, :issue, :number, :author, "
                        ":created, :kind, "
                        "CAST(:metadata AS jsonb), :digest, :created + interval '90 days') "
                        "ON CONFLICT (repository_id, comment_id) DO NOTHING "
                        "RETURNING repository_id, comment_id, issue_number, accepted_at, expires_at"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "repo": event.repository_id,
                        "comment": event.comment_id,
                        "issue": event.issue_id,
                        "number": event.issue_number,
                        "author": event.author_github_id,
                        "created": created_at,
                        "kind": command.kind,
                        "metadata": canonical,
                        "digest": command_digest,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if result is None:
                existing = (
                    connection.execute(
                        text(
                            "SELECT owner_id, issue_id, issue_number, author_id, "
                            "comment_created_at, "
                            "command_sha256, accepted_at, expires_at FROM "
                            "runtime_private.github_command_receipts WHERE repository_id=:repo "
                            "AND comment_id=:comment"
                        ),
                        {"repo": event.repository_id, "comment": event.comment_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if existing is None:
                    raise GitHubCommandError("github_command_identity_conflict")
                if (
                    existing["owner_id"] != self._principal.account_id
                    or existing["issue_id"] != event.issue_id
                    or existing["issue_number"] != event.issue_number
                    or existing["author_id"] != event.author_github_id
                    or existing["comment_created_at"] != created_at
                    or bytes(existing["command_sha256"]) != command_digest
                ):
                    raise GitHubCommandError("github_command_identity_conflict")
                return GitHubCommandReceipt(
                    event.repository_id,
                    event.comment_id,
                    event.issue_number,
                    existing["accepted_at"],
                    existing["expires_at"],
                    command,
                    True,
                )
            if payload_json is not None:
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.github_command_payloads "
                        "(owner_id, repository_id, comment_id, accepted_at, payload, expires_at) "
                        "VALUES (:owner, :repo, :comment, :accepted, CAST(:payload AS jsonb), "
                        ":accepted + interval '72 hours')"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "repo": event.repository_id,
                        "comment": event.comment_id,
                        "accepted": result["accepted_at"],
                        "payload": payload_json,
                    },
                )
            return GitHubCommandReceipt(
                result["repository_id"],
                result["comment_id"],
                result["issue_number"],
                result["accepted_at"],
                result["expires_at"],
                command,
            )

    def pending(self, *, limit: int = 100) -> tuple[GitHubCommandReceipt, ...]:
        """Read unexpired normalized commands for a future T38 command handler."""
        if type(limit) is not int or not 1 <= limit <= 500:
            raise GitHubCommandError("github_command_limit_invalid")
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                rows = (
                    connection.execute(
                        text(
                            "SELECT r.repository_id, r.comment_id, r.issue_number, r.accepted_at, "
                            "r.expires_at, r.command_metadata, p.payload FROM "
                            "runtime_private.github_command_receipts AS r LEFT JOIN "
                            "runtime_private.github_command_payloads AS p USING "
                            "(owner_id, repository_id, comment_id) WHERE r.owner_id=:owner "
                            "AND r.expires_at>clock_timestamp() "
                            "AND r.accepted_at>=clock_timestamp()-interval '72 hours' "
                            "AND (r.command_kind<>'changes' OR "
                            "(p.payload IS NOT NULL AND p.expires_at>clock_timestamp())) "
                            "ORDER BY r.accepted_at, r.comment_id "
                            "LIMIT :limit"
                        ),
                        {"owner": self._principal.account_id, "limit": limit},
                    )
                    .mappings()
                    .all()
                )
            return tuple(_receipt_from_row(row) for row in rows)
        except SQLAlchemyError:
            raise GitHubCommandError("github_command_storage_unavailable", retryable=True) from None

    def sweep_expired(self, *, limit: int = 100) -> int:
        """Scrub feedback after 72 hours while leaving the 90-day dedup tombstone."""
        if type(limit) is not int or not 1 <= limit <= 500:
            raise GitHubCommandError("github_command_limit_invalid")
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            purged_payloads = connection.execute(
                text(
                    "DELETE FROM runtime_private.github_command_payloads WHERE owner_id=:owner "
                    "AND (repository_id, comment_id) IN "
                    "(SELECT repository_id, comment_id FROM "
                    "runtime_private.github_command_payloads "
                    "WHERE owner_id=:owner AND expires_at<=clock_timestamp() "
                    "ORDER BY expires_at, comment_id LIMIT :limit)"
                ),
                {"owner": self._principal.account_id, "limit": limit},
            ).rowcount
            purged_receipts = connection.execute(
                text(
                    "DELETE FROM runtime_private.github_command_receipts WHERE owner_id=:owner "
                    "AND (repository_id, comment_id) IN "
                    "(SELECT repository_id, comment_id FROM "
                    "runtime_private.github_command_receipts "
                    "WHERE owner_id=:owner AND expires_at<=clock_timestamp() "
                    "ORDER BY expires_at, comment_id LIMIT :limit)"
                ),
                {"owner": self._principal.account_id, "limit": limit},
            ).rowcount
            return purged_payloads + purged_receipts


def _receipt_from_row(row: Mapping[Any, Any]) -> GitHubCommandReceipt:
    metadata = row["command_metadata"]
    command = NormalizedGitHubCommand(
        kind=metadata["kind"],
        target_id=metadata.get("target_id"),
        action_sha256=metadata.get("action_sha256"),
        feedback=(row["payload"] or {}).get("feedback"),
    )
    return GitHubCommandReceipt(
        row["repository_id"],
        row["comment_id"],
        row["issue_number"],
        row["accepted_at"],
        row["expires_at"],
        command,
    )


def _same_current_comment(
    event: IssueCommentCommandEvent, current: GitHubIssueCommentSnapshot
) -> bool:
    return (
        current.repository_id == event.repository_id
        and current.issue_id == event.issue_id
        and current.issue_number == event.issue_number
        and current.comment_id == event.comment_id
        and current.author_github_id == event.author_github_id
        and current.author_type == event.author_type
        and current.created_at == event.created_at
        and current.updated_at == event.updated_at
        and event.updated_at == event.created_at
        and current.body == event.body
        and current.is_pull_request is False
    )


def _validate_event(event: IssueCommentCommandEvent) -> None:
    if (
        not isinstance(event, IssueCommentCommandEvent)
        or event.action not in {"created", "edited", "deleted"}
        or any(
            type(value) is not int or value <= 0
            for value in (
                event.repository_id,
                event.issue_id,
                event.issue_number,
                event.comment_id,
                event.author_github_id,
            )
        )
        or not isinstance(event.author_type, str)
        or not event.author_type
        or not isinstance(event.created_at, datetime)
        or event.created_at.tzinfo is None
        or not isinstance(event.updated_at, datetime)
        or event.updated_at.tzinfo is None
        or not isinstance(event.body, str)
    ):
        raise GitHubCommandError("github_comment_event_invalid")
