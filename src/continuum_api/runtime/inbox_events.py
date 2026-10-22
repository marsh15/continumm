"""Strict parser for the public GitHub issue-comment webhook fields we consume."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from continuum_api.runtime.github_commands import IssueCommentCommandEvent

MAX_PUBLIC_WEBHOOK_BYTES = 250 * 1024
MAX_COMMENT_BODY_BYTES = 16_384
MAX_POSTGRES_BIGINT = 9_223_372_036_854_775_807
MAX_ISSUE_NUMBER = 2_147_483_647


def parse_public_issue_comment_identity(payload: object) -> tuple[int, int, int] | None:
    """Extract the exact numeric identity used by the inbox completion fence."""
    if not isinstance(payload, dict):
        return None
    repository = payload.get("repository")
    comment = payload.get("comment")
    user = comment.get("user") if isinstance(comment, dict) else None
    if (
        not isinstance(repository, dict)
        or not isinstance(comment, dict)
        or not isinstance(user, dict)
    ):
        return None
    repository_id = _positive_id(repository.get("id"), MAX_POSTGRES_BIGINT)
    comment_id = _positive_id(comment.get("id"), MAX_POSTGRES_BIGINT)
    author_id = _positive_id(user.get("id"), MAX_POSTGRES_BIGINT)
    if repository_id is None or comment_id is None or author_id is None:
        return None
    return repository_id, comment_id, author_id


def parse_public_issue_comment(payload: object) -> IssueCommentCommandEvent | None:
    """Return a normalized created-comment event, or ``None`` for inert input.

    The inbox payload is public GitHub data. This function deliberately extracts
    only the fields needed by the existing owner-scoped command intake and never
    serializes or logs the original payload.
    """
    identity = parse_public_issue_comment_identity(payload)
    if identity is None or not isinstance(payload, dict):
        return None
    action = payload.get("action")
    repository = payload.get("repository")
    issue = payload.get("issue")
    comment = payload.get("comment")
    if (
        action != "created"
        or not isinstance(repository, dict)
        or not isinstance(issue, dict)
        or not isinstance(comment, dict)
        or "pull_request" in issue
    ):
        return None

    repository_id = identity[0]
    issue_id = _positive_id(issue.get("id"), MAX_POSTGRES_BIGINT)
    issue_number = _positive_id(issue.get("number"), MAX_ISSUE_NUMBER)
    comment_id = identity[1]
    user = comment.get("user")
    if not isinstance(user, dict):
        return None
    author_id = identity[2]
    author_type = user.get("type")
    created_at = _timestamp(comment.get("created_at"))
    updated_at = _timestamp(comment.get("updated_at"))
    body = comment.get("body")
    if (
        repository_id is None
        or issue_id is None
        or issue_number is None
        or comment_id is None
        or author_id is None
        or author_type != "User"
        or created_at is None
        or updated_at is None
        or not isinstance(body, str)
    ):
        return None
    try:
        if len(body.encode("utf-8")) > MAX_COMMENT_BODY_BYTES:
            return None
    except UnicodeEncodeError:
        return None

    return IssueCommentCommandEvent(
        action="created",
        repository_id=repository_id,
        issue_id=issue_id,
        issue_number=issue_number,
        comment_id=comment_id,
        author_github_id=author_id,
        author_type=author_type,
        created_at=created_at,
        updated_at=updated_at,
        body=body,
        is_pull_request=False,
    )


def _positive_id(value: Any, maximum: int) -> int | None:
    if type(value) is not int or not 1 <= value <= maximum:
        return None
    return value


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not 1 <= len(value) <= 40:
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if result.tzinfo is None or result.utcoffset() is None:
        return None
    return result.astimezone(UTC)
