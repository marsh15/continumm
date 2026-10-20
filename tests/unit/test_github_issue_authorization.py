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
