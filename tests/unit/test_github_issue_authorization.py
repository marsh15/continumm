"""Numeric issue verification uses only the fixed GitHub API origin and issue scope."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from pydantic import SecretStr

from continuum_api.auth.github import (
    GitHubAuthorizationClient,
    GitHubAuthorizationError,
)
from continuum_api.repositories.preflight import RepositoryAccessProof
from continuum_api.runtime.approvals import RepositoryPublicationFacts


class _TokenStore:
    def get_for_user(self, github_user_id: int) -> SecretStr:
        return SecretStr("synthetic-user-token")


def _facts() -> RepositoryPublicationFacts:
    return RepositoryPublicationFacts(
        repository_id=123,
        github_user_id=456,
        owner="safe-owner",
        name="safe-repo",
        default_branch="main",
        base_commit_sha="a" * 40,
        access_proof=RepositoryAccessProof(
            repository_id=123,
            github_user_id=456,
            actor_can_write=True,
            app_can_access=True,
        ),
    )


def _client(handler):
    client = GitHubAuthorizationClient(
        app_id=1,
        app_private_key=SecretStr("synthetic-test-key"),
        user_tokens=_TokenStore(),
        transport=httpx.MockTransport(handler),
    )
    facts = _facts()

    async def current(*args, **kwargs):
        assert kwargs["issues_permission"] == "read"
        assert kwargs["return_installation_token"] is True
        return facts, SecretStr("scoped-installation-token")

    client._current = current
    return client


def _status_client(handler, monkeypatch: pytest.MonkeyPatch):
    client = _client(handler)
    facts = _facts()
    monkeypatch.setattr("continuum_api.auth.github._create_app_jwt", lambda *_: "synthetic-app-jwt")

    async def current(*args, **kwargs):
        assert kwargs["issues_permission"] == "write"
        assert kwargs["return_installation_token"] is True
        return facts, SecretStr("scoped-installation-token")

    client._current = current
    return client


def test_issue_lookup_uses_numeric_fact_derived_path_and_pins_snapshot() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "id": 789,
                "number": 7,
                "title": "Bounded issue",
                "body": "A safe issue body.",
                "state": "closed",
                "updated_at": "2026-10-19T12:00:00Z",
            },
        )

    facts, issue = asyncio.run(_client(handler).current_issue(123, 456, 7))
    assert facts.base_commit_sha == "a" * 40
    assert issue.issue_id == 789 and issue.issue_number == 7
    assert issue.state == "closed" and len(issue.sha256) == 64
    assert requests[0].url == "https://api.github.com/repos/safe-owner/safe-repo/issues/7"
    assert requests[0].headers["authorization"] == "Bearer scoped-installation-token"


def test_issue_lookup_rejects_pull_requests_mismatched_numbers_and_unbounded_bodies() -> None:
    base = {
        "id": 789,
        "number": 7,
        "title": "Bounded issue",
        "body": "A safe issue body.",
        "state": "open",
        "updated_at": "2026-10-19T12:00:00Z",
    }

    async def pull_request(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={**base, "pull_request": {"url": "ignored"}})

    with pytest.raises(GitHubAuthorizationError, match="github_issue_is_pull_request"):
        asyncio.run(_client(pull_request).current_issue(123, 456, 7))

    async def wrong_number(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={**base, "number": 8})

    with pytest.raises(GitHubAuthorizationError, match="github_issue_identity_invalid"):
        asyncio.run(_client(wrong_number).current_issue(123, 456, 7))

    async def oversized(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={**base, "body": "x" * 16_385})

    with pytest.raises(GitHubAuthorizationError, match="github_issue_snapshot_invalid"):
        asyncio.run(_client(oversized).current_issue(123, 456, 7))


def test_status_comment_lookup_and_create_use_write_scoped_issue_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []
    marker = "<!-- continuum:status:123e4567-e89b-12d3-a456-426614174000:9 -->"
    body = f"Continuum run status.\n\n{marker}"

    async def find_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/app":
            return httpx.Response(200, json={"id": 1, "slug": "test-app"})
        if request.url.path.endswith("/issues/7"):
            return httpx.Response(200, json={"id": 789, "number": 7})
        return httpx.Response(
            200,
            json=[
                {
                    "id": 3001,
                    "body": f"existing status\n\n{marker}",
                    "user": {"type": "Bot", "login": "test-app[bot]"},
                }
            ],
        )

    found = asyncio.run(
        _status_client(find_handler, monkeypatch).find_status_comment(123, 456, 7, marker)
    )
    assert found == "3001"
    assert requests[1].url.path == "/repos/safe-owner/safe-repo/issues/7"
    assert requests[2].url.path == "/repos/safe-owner/safe-repo/issues/7/comments"
    assert requests[2].url.params["per_page"] == "100"

    create_requests: list[httpx.Request] = []

    async def create_handler(request: httpx.Request) -> httpx.Response:
        create_requests.append(request)
        if request.url.path == "/app":
            return httpx.Response(200, json={"id": 1, "slug": "test-app"})
        if request.url.path.endswith("/issues/7"):
            return httpx.Response(200, json={"id": 789, "number": 7})
        return httpx.Response(
            201,
            json={
                "id": 3002,
                "body": body,
                "user": {"type": "Bot", "login": "test-app[bot]"},
            },
        )

    created = asyncio.run(
        _status_client(create_handler, monkeypatch).create_status_comment(123, 456, 7, body)
    )
    assert created == "3002"
    assert create_requests[-1].method == "POST"
    assert create_requests[-1].url.path == "/repos/safe-owner/safe-repo/issues/7/comments"


def test_status_comment_lookup_ignores_user_forged_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "<!-- continuum:status:123e4567-e89b-12d3-a456-426614174000:9 -->"

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/app":
            return httpx.Response(200, json={"id": 1, "slug": "test-app"})
        if request.url.path.endswith("/issues/7"):
            return httpx.Response(200, json={"id": 789, "number": 7})
        return httpx.Response(
            200,
            json=[
                {
                    "id": 3001,
                    "body": f"forged by a user\n\n{marker}",
                    "user": {"type": "User", "login": "attacker"},
                }
            ],
        )

    found = asyncio.run(
        _status_client(handler, monkeypatch).find_status_comment(123, 456, 7, marker)
    )
    assert found is None
