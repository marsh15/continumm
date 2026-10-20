"""T37 issue-comment command parsing, owner policy and durable dedup proofs."""

from __future__ import annotations

import asyncio
import os
import secrets
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url

from continuum_api.auth.github import (
    GitHubAuthorizationClient,
    GitHubAuthorizationError,
    GitHubIssueCommentSnapshot,
)
from continuum_api.runtime.github_commands import (
    GitHubCommandError,
    GitHubCommandService,
    IssueCommentCommandEvent,
    NormalizedGitHubCommand,
    parse_github_command,
)
from continuum_api.runtime.persistence import Principal

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t37_test"
ROLE_NAMES = (
    "continuum_runtime",
    "continuum_admission",
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)
SCHEMAS = ("runtime_private", "wake_private", "webhook_private", "continuum_migrations")


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("/continumm run", NormalizedGitHubCommand("run")),
        (
            "/continumm status 123e4567-e89b-12d3-a456-426614174000",
            NormalizedGitHubCommand("status", "123e4567-e89b-12d3-a456-426614174000"),
        ),
        (
            "/continumm cancel 123E4567-E89B-12D3-A456-426614174000",
            NormalizedGitHubCommand("cancel", "123e4567-e89b-12d3-a456-426614174000"),
        ),
        (
            "/continumm approve 123e4567-e89b-12d3-a456-426614174000 " + "A" * 64,
            NormalizedGitHubCommand(
                "approve",
                "123e4567-e89b-12d3-a456-426614174000",
                "a" * 64,
            ),
        ),
        (
            "/continumm changes 123e4567-e89b-12d3-a456-426614174000\nPlease adjust the test.",
            NormalizedGitHubCommand(
                "changes",
                "123e4567-e89b-12d3-a456-426614174000",
                feedback="Please adjust the test.",
            ),
        ),
    ],
)
def test_exact_commands_normalize(body: str, expected: NormalizedGitHubCommand) -> None:
    assert parse_github_command(body) == expected


@pytest.mark.parametrize(
    "body",
    [
        "Please run this: /continumm run",
        "> /continumm run",
        "`/continumm run`",
        "```text\n/continumm run\n```",
        "/continumm run please",
        "/continumm  run",
        "/continumm status not-a-uuid",
        "/continumm approve 123e4567-e89b-12d3-a456-426614174000 abc",
        "/continumm changes 123e4567-e89b-12d3-a456-426614174000",
        "/continumm status 123e4567-e89b-12d3-a456-426614174000\nextra feedback",
    ],
)
def test_quoted_code_or_non_exact_commands_are_inert(body: str) -> None:
    assert parse_github_command(body) is None


class _CurrentComment:
    def __init__(self) -> None:
        self.value: GitHubIssueCommentSnapshot | None = None
        self.fail = False
        self.calls = 0
        self.after_read: Any = None

    async def current_issue_comment(
        self,
        *,
        repository_id: int,
        github_user_id: int,
        comment_id: int,
        issue_number: int,
    ) -> GitHubIssueCommentSnapshot:
        self.calls += 1
        if self.fail:
            raise RuntimeError("synthetic_unavailable")
        assert self.value is not None
        if self.after_read is not None:
            self.after_read()
        return self.value


@contextmanager
def _database() -> Any:
    database_url = os.environ.get("CONTINUUM_T37_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T37_TEST_DATABASE_URL to a disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail(f"T37 requires the dedicated loopback database {DATABASE_NAME}")
    admin = create_engine(url.set(drivername="postgresql+psycopg"), hide_parameters=True)
    runtime_password = secrets.token_urlsafe(32)
    runtime: Engine | None = None

    def clean() -> None:
        with admin.begin() as connection:
            for schema in SCHEMAS:
                connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
            for role in ROLE_NAMES:
                exists: bool = connection.execute(
                    text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role)"),
                    {"role": role},
                ).scalar_one()
                if exists:
                    connection.execute(
                        text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role}")
                    )
                    connection.execute(text(f"DROP ROLE {role}"))

    def migrate(operation: str, revision: str) -> None:
        env = {**os.environ, "CONTINUUM_MIGRATION_DATABASE_URL": database_url}
        result = subprocess.run(
            [sys.executable, "-m", "continuum_api.migrations", operation, revision],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )
        if result.returncode:
            pytest.fail(
                f"T37 migration {operation} failed: {result.stdout[-500:]} {result.stderr[-2000:]}"
            )

    try:
        clean()
        with admin.begin() as connection:
            version: int = connection.execute(
                text("SELECT current_setting('server_version_num')::integer")
            ).scalar_one()
            assert 170000 <= version < 180000
            for role in reversed(ROLE_NAMES):
                password = (
                    runtime_password if role == "continuum_runtime" else secrets.token_urlsafe(32)
                )
                login = (
                    "NOLOGIN"
                    if role in {"continuum_wake_issuer", "continuum_admission"}
                    else "LOGIN"
                )
                password_clause = "" if login == "NOLOGIN" else f"PASSWORD '{password}'"
                connection.exec_driver_sql(
                    f"CREATE ROLE {role} {login} NOSUPERUSER NOCREATEDB "
                    f"NOCREATEROLE NOINHERIT NOBYPASSRLS {password_clause}"
                )
        migrate("upgrade", "head")
        runtime = create_engine(
            url.set(
                drivername="postgresql+psycopg",
                username="continuum_runtime",
                password=runtime_password,
            ),
            hide_parameters=True,
        )
        yield admin, runtime
        runtime.dispose()
        runtime = None
        migrate("downgrade", "20261019_23")
    finally:
        if runtime is not None:
            runtime.dispose()
        clean()
        admin.dispose()


def _event(
    body: str,
    *,
    now: datetime,
    comment_id: int = 900_001,
    author_id: int = 8_700_001,
    action: str = "created",
    author_type: str = "User",
    is_pull_request: bool = False,
) -> IssueCommentCommandEvent:
    return IssueCommentCommandEvent(
        action=action,
        repository_id=8_800_001,
        issue_id=8_900_001,
        issue_number=17,
        comment_id=comment_id,
        author_github_id=author_id,
        author_type=author_type,
        created_at=now,
        updated_at=now,
        body=body,
        is_pull_request=is_pull_request,
    )


def _snapshot(event: IssueCommentCommandEvent, **changes: Any) -> GitHubIssueCommentSnapshot:
    values: dict[str, Any] = {
        "repository_id": event.repository_id,
        "issue_id": event.issue_id,
        "issue_number": event.issue_number,
        "comment_id": event.comment_id,
        "author_github_id": event.author_github_id,
        "author_type": event.author_type,
        "created_at": event.created_at,
        "updated_at": event.updated_at,
        "body": event.body,
        "is_pull_request": event.is_pull_request,
    }
    values.update(changes)
    return GitHubIssueCommentSnapshot(**values)


def test_owner_command_intake_is_fresh_deduplicated_and_content_bounded() -> None:
    with _database() as (admin, runtime):
        owner, other = Principal(uuid4()), Principal(uuid4())
        with admin.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO runtime_private.users (user_id, github_user_id, created_at) "
                    "VALUES (:owner, 8700001, clock_timestamp()-interval '80 hours'), "
                    "(:other, 8700002, clock_timestamp()-interval '80 hours')"
                ),
                {"owner": owner.account_id, "other": other.account_id},
            )
            connection.execute(
                text(
                    "INSERT INTO runtime_private.repository_registrations "
                    "(owner_id, github_repository_id, repository_owner, repository_name, "
                    "profile_id, created_at) "
                    "VALUES (:owner, 8800001, 'owner', 'repo', 'python-pytest', "
                    "clock_timestamp()-interval '80 hours')"
                ),
                {"owner": owner.account_id},
            )

        reader = _CurrentComment()
        service = GitHubCommandService(runtime, owner, reader)
        timestamp = datetime.now(UTC) + timedelta(seconds=1)
        changes = _event(
            "/continumm changes 123e4567-e89b-12d3-a456-426614174000\nPlease update tests.",
            now=timestamp,
        )
        reader.value = _snapshot(changes)
        first = asyncio.run(service.accept_created_comment(changes))
        assert first is not None and first.duplicate is False
        assert first.command.feedback == "Please update tests."
        replay = asyncio.run(service.accept_created_comment(changes))
        assert replay is not None and replay.duplicate is True
        assert reader.calls == 2

        changed_body = _event(
            "/continumm changes 123e4567-e89b-12d3-a456-426614174000\nDifferent feedback.",
            now=timestamp,
        )
        reader.value = _snapshot(changed_body)
        with pytest.raises(GitHubCommandError, match="github_command_identity_conflict"):
            asyncio.run(service.accept_created_comment(changed_body))

        pending = asyncio.run(asyncio.to_thread(service.pending))
        assert len(pending) == 1
        assert pending[0].command.feedback == "Please update tests."
        with admin.begin() as connection:
            stored = (
                connection.execute(
                    text(
                        "SELECT command_metadata, command_sha256 FROM "
                        "runtime_private.github_command_receipts WHERE repository_id=8800001 "
                        "AND comment_id=900001"
                    )
                )
                .mappings()
                .one()
            )
            payload = connection.execute(
                text(
                    "SELECT payload FROM runtime_private.github_command_payloads "
                    "WHERE repository_id=8800001 AND comment_id=900001"
                )
            ).scalar_one()
            assert "feedback" not in stored["command_metadata"]
            assert payload == {"feedback": "Please update tests."}

        with admin.begin() as connection:
            connection.execute(
                text(
                    "ALTER TABLE runtime_private.github_command_payloads "
                    "DROP CONSTRAINT ck_github_command_payload_expiry"
                )
            )
            connection.execute(
                text(
                    "UPDATE runtime_private.github_command_payloads SET "
                    "expires_at=clock_timestamp()-interval '1 second' "
                    "WHERE repository_id=8800001 AND comment_id=900001"
                )
            )
            connection.execute(
                text(
                    "ALTER TABLE runtime_private.github_command_payloads "
                    "ADD CONSTRAINT ck_github_command_payload_expiry CHECK "
                    "(expires_at = accepted_at + interval '72 hours') NOT VALID"
                )
            )
        assert asyncio.run(asyncio.to_thread(service.pending)) == ()
        assert asyncio.run(asyncio.to_thread(service.sweep_expired)) == 1
        with admin.begin() as connection:
            connection.execute(
                text(
                    "ALTER TABLE runtime_private.github_command_payloads "
                    "VALIDATE CONSTRAINT ck_github_command_payload_expiry"
                )
            )

        # A command accepted near the 72-hour freshness boundary remains pending
        # for 72 hours from acceptance, including its feedback payload.
        near_boundary = _event(
            "/continumm changes 123e4567-e89b-12d3-a456-426614174000\nBoundary feedback.",
            now=datetime.now(UTC) - timedelta(hours=71, minutes=59),
            comment_id=900_014,
        )
        reader.value = _snapshot(near_boundary)
        accepted_boundary = asyncio.run(service.accept_created_comment(near_boundary))
        assert accepted_boundary is not None
        pending_boundary = asyncio.run(asyncio.to_thread(service.pending))
        assert any(
            item.comment_id == near_boundary.comment_id
            and item.command.feedback == "Boundary feedback."
            for item in pending_boundary
        )
        with admin.begin() as connection:
            ttl = connection.execute(
                text(
                    "SELECT p.expires_at-r.accepted_at FROM "
                    "runtime_private.github_command_payloads AS p JOIN "
                    "runtime_private.github_command_receipts AS r USING "
                    "(owner_id, repository_id, comment_id) "
                    "WHERE p.repository_id=8800001 AND p.comment_id=900014"
                )
            ).scalar_one()
            assert ttl == timedelta(hours=72)

        # If the owner is suspended during the current-comment network read, the
        # transactional authorization recheck rejects intake before a receipt.
        suspended_event = _event(
            "/continumm run",
            now=datetime.now(UTC) + timedelta(seconds=1),
            comment_id=900_015,
        )
        reader.value = _snapshot(suspended_event)

        def suspend_owner() -> None:
            with admin.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE runtime_private.users SET status='suspended' WHERE user_id=:owner"
                    ),
                    {"owner": owner.account_id},
                )

        reader.after_read = suspend_owner
        with pytest.raises(GitHubCommandError, match="github_command_authorization_changed"):
            asyncio.run(service.accept_created_comment(suspended_event))
        reader.after_read = None
        with admin.begin() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.github_command_receipts "
                        "WHERE repository_id=8800001 AND comment_id=900015"
                    )
                ).scalar_one()
                == 0
            )
            connection.execute(
                text("UPDATE runtime_private.users SET status='active' WHERE user_id=:owner"),
                {"owner": owner.account_id},
            )

        # JSON escaping must not reject feedback within the original body byte cap.
        escaped_feedback = "\\" * 9000
        escaped = _event(
            "/continumm changes 123e4567-e89b-12d3-a456-426614174000\n" + escaped_feedback,
            now=datetime.now(UTC) - timedelta(seconds=2),
            comment_id=900_020,
        )
        reader.value = _snapshot(escaped)
        escaped_receipt = asyncio.run(service.accept_created_comment(escaped))
        assert escaped_receipt is not None
        assert escaped_receipt.command.feedback == escaped_feedback

        # Concurrent webhook deliveries of one newly created command share a receipt.
        run_event = _event(
            "/continumm run",
            now=datetime.now(UTC) + timedelta(seconds=1),
            comment_id=900_012,
        )
        reader.value = _snapshot(run_event)
        with ThreadPoolExecutor(max_workers=4) as pool:
            concurrent = list(
                pool.map(
                    lambda _: asyncio.run(service.accept_created_comment(run_event)),
                    range(4),
                )
            )
        assert sum(receipt is not None and not receipt.duplicate for receipt in concurrent) == 1
        assert all(receipt is not None for receipt in concurrent)
        with admin.begin() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.github_command_receipts "
                        "WHERE repository_id=8800001 AND comment_id=900012"
                    )
                ).scalar_one()
                == 1
            )

        # Creation freshness and registration time are rechecked inside the commit.
        stale = _event(
            "/continumm run",
            now=datetime.now(UTC) - timedelta(hours=73),
            comment_id=900_009,
        )
        reader.value = _snapshot(stale)
        with pytest.raises(GitHubCommandError, match="github_comment_stale"):
            asyncio.run(service.accept_created_comment(stale))

        with admin.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.repository_registrations SET "
                    "created_at=clock_timestamp() WHERE owner_id=:owner "
                    "AND github_repository_id=8800001"
                ),
                {"owner": owner.account_id},
            )
        pre_registration = _event(
            "/continumm run",
            now=datetime.now(UTC) - timedelta(seconds=1),
            comment_id=900_013,
        )
        reader.value = _snapshot(pre_registration)
        with pytest.raises(GitHubCommandError, match="github_comment_stale"):
            asyncio.run(service.accept_created_comment(pre_registration))

        future = _event(
            "/continumm run",
            now=datetime.now(UTC) + timedelta(minutes=6),
            comment_id=900_010,
        )
        reader.value = _snapshot(future)
        with pytest.raises(GitHubCommandError, match="github_comment_stale"):
            asyncio.run(service.accept_created_comment(future))

        inert = _event(
            "/continumm run",
            now=datetime.now(UTC) - timedelta(seconds=2),
            comment_id=900_002,
        )
        for skipped in (
            _event(inert.body, now=inert.created_at, comment_id=900_003, author_type="Bot"),
            _event(inert.body, now=inert.created_at, comment_id=900_004, action="edited"),
            _event(inert.body, now=inert.created_at, comment_id=900_011, action="deleted"),
            _event(inert.body, now=inert.created_at, comment_id=900_005, is_pull_request=True),
            _event(inert.body, now=inert.created_at, comment_id=900_006, author_id=8_700_002),
        ):
            reader.value = _snapshot(skipped)
            assert asyncio.run(service.accept_created_comment(skipped)) is None

        # The exact issue comment must still match GitHub's current body and timestamps.
        edited = _event(inert.body, now=inert.created_at, comment_id=900_007)
        reader.value = _snapshot(edited, body="edited after delivery")
        assert asyncio.run(service.accept_created_comment(edited)) is None

        # Current GitHub access errors are retryable and never create a receipt.
        unreadable = _event(inert.body, now=inert.created_at, comment_id=900_008)
        reader.fail = True
        with pytest.raises(GitHubCommandError) as error:
            asyncio.run(service.accept_created_comment(unreadable))
        assert error.value.code == "github_comment_unverifiable" and error.value.retryable

        # Another account cannot see this owner's command through the runtime RLS role.
        other_service = GitHubCommandService(runtime, other, reader)
        assert asyncio.run(asyncio.to_thread(other_service.pending)) == ()


def test_current_comment_timestamp_comparison_includes_creation_and_update() -> None:
    timestamp = datetime.now(UTC)
    event = _event("/continumm run", now=timestamp)
    current = _snapshot(event, updated_at=timestamp + timedelta(seconds=1))
    from continuum_api.runtime.github_commands import _same_current_comment

    assert not _same_current_comment(event, current)


class _UserToken:
    def get_for_user(self, github_user_id: int) -> SecretStr | None:
        return SecretStr("synthetic-user-token") if github_user_id == 8_700_001 else None


class _CommentAPI:
    def __init__(self, *, changes: dict[str, Any] | None = None) -> None:
        self.changes = changes or {}
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path == "/user":
            return httpx.Response(200, json={"id": 8_700_001})
        if path == "/repositories/8800001":
            if request.headers.get("authorization") == "Bearer synthetic-user-token":
                return httpx.Response(
                    200,
                    json={
                        "id": 8_800_001,
                        "private": False,
                        "name": "repo",
                        "owner": {"login": "acme"},
                        "default_branch": "main",
                        "permissions": {"push": not self.changes.get("no_push", False)},
                    },
                )
            return httpx.Response(
                200,
                json={
                    "id": 8_800_001,
                    "private": False,
                    "name": "repo",
                    "owner": {"login": "acme"},
                },
            )
        if path == "/repos/acme/repo/installation":
            return httpx.Response(200, json={"id": 77})
        if path == "/repos/acme/repo/git/ref/heads/main":
            return httpx.Response(200, json={"object": {"sha": "a" * 40}})
        if path == "/user/installations/77/repositories":
            repos = [] if self.changes.get("no_app_access") else [{"id": 8_800_001}]
            return httpx.Response(200, json={"repositories": repos})
        if path == "/app/installations/77/access_tokens":
            permissions = __import__("json").loads(request.content)["permissions"]
            return httpx.Response(
                201,
                json={
                    "token": "synthetic-installation-token",
                    "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    "permissions": permissions,
                    "repository_selection": "selected",
                    "repositories": [{"id": 8_800_001}],
                },
            )
        if path == "/repos/acme/repo/issues/comments/900001":
            if self.changes.get("redirect"):
                return httpx.Response(302, headers={"location": "https://attacker.invalid"})
            return httpx.Response(
                200,
                json={
                    "id": 900_001,
                    "body": self.changes.get("body", "/continumm run"),
                    "created_at": "2026-10-20T12:00:00Z",
                    "updated_at": self.changes.get("updated_at", "2026-10-20T12:00:00Z"),
                    "issue_url": self.changes.get(
                        "issue_url", "https://api.github.com/repos/acme/repo/issues/17"
                    ),
                    "user": {
                        "id": self.changes.get("author_id", 8_700_001),
                        "type": self.changes.get("author_type", "User"),
                    },
                },
            )
        if path == "/repos/acme/repo/issues/17":
            return httpx.Response(
                200,
                json={
                    "id": self.changes.get("issue_id", 8_900_001),
                    "number": 17,
                    **({"pull_request": {"url": "ignored"}} if self.changes.get("is_pr") else {}),
                },
            )
        return httpx.Response(404, json={})


def _github_comment_client(
    api: _CommentAPI, monkeypatch: pytest.MonkeyPatch
) -> GitHubAuthorizationClient:
    monkeypatch.setattr("continuum_api.auth.github._create_app_jwt", lambda *_: "synthetic-app-jwt")
    return GitHubAuthorizationClient(
        app_id=77,
        app_private_key=SecretStr("synthetic-pem-is-not-used"),
        user_tokens=_UserToken(),
        transport=httpx.MockTransport(api),
    )


def test_current_comment_adapter_rechecks_write_and_app_access_and_compares_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = _CommentAPI()
    client = _github_comment_client(api, monkeypatch)
    snapshot = asyncio.run(
        client.current_issue_comment(
            repository_id=8_800_001,
            github_user_id=8_700_001,
            comment_id=900_001,
            issue_number=17,
        )
    )
    assert snapshot.repository_id == 8_800_001
    assert snapshot.issue_id == 8_900_001
    assert snapshot.author_type == "User"
    token_request = next(request for request in api.requests if request.method == "POST")
    assert __import__("json").loads(token_request.content)["permissions"]["issues"] == "read"
    assert all(
        request.url.scheme == "https" and request.url.host == "api.github.com"
        for request in api.requests
    )

    event = _event("/continumm run", now=snapshot.created_at)
    current = GitHubIssueCommentSnapshot(
        repository_id=snapshot.repository_id,
        issue_id=snapshot.issue_id,
        issue_number=snapshot.issue_number,
        comment_id=snapshot.comment_id,
        author_github_id=snapshot.author_github_id,
        author_type=snapshot.author_type,
        created_at=snapshot.created_at,
        updated_at=snapshot.updated_at + timedelta(seconds=1),
        body=snapshot.body,
    )
    from continuum_api.runtime.github_commands import _same_current_comment

    assert not _same_current_comment(event, current)


@pytest.mark.parametrize(
    ("changes", "expected_code"),
    [
        ({"no_push": True}, "repository_actor_write_access_required"),
        ({"no_app_access": True}, "github_installation_user_access_required"),
        ({"author_id": 8_700_002}, "github_comment_identity_invalid"),
        ({"author_type": "Bot"}, "github_comment_identity_invalid"),
        (
            {"issue_url": "https://attacker.invalid/repos/acme/repo/issues/17"},
            "github_comment_identity_invalid",
        ),
        ({"is_pr": True}, "github_comment_pull_request_rejected"),
        ({"redirect": True}, "github_api_redirect_rejected"),
    ],
)
def test_current_comment_adapter_fails_closed(
    changes: dict[str, Any], expected_code: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = _CommentAPI(changes=changes)
    client = _github_comment_client(api, monkeypatch)
    with pytest.raises(GitHubAuthorizationError, match=expected_code):
        asyncio.run(
            client.current_issue_comment(
                repository_id=8_800_001,
                github_user_id=8_700_001,
                comment_id=900_001,
                issue_number=17,
            )
        )
