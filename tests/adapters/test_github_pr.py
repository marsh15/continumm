"""HTTP-level PR transport tests; fixtures never contact GitHub or a database."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import SecretStr

from continuum_api.auth.github import GitHubAuthorizationClient
from continuum_api.publication.github_pr import (
    GitHubPRTransport,
    GitHubPRTransportError,
)

USER_ID = 12345
REPOSITORY_ID = 67890
BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40


class _Tokens:
    def get_for_user(self, github_user_id: int) -> SecretStr:
        assert github_user_id == USER_ID
        return SecretStr("fixture-user-token")


def _pr(*, state: str = "open", merged_at: str | None = None) -> dict[str, object]:
    return {
        "number": 17,
        "id": 1700,
        "state": state,
        "merged_at": merged_at,
        "head": {
            "ref": "continumm/run/00000000-0000-0000-0000-000000000001",
            "sha": HEAD_SHA,
            "repo": {"id": REPOSITORY_ID, "owner": {"login": "acme"}},
        },
        "base": {
            "ref": "main",
            "sha": BASE_SHA,
            "repo": {"id": REPOSITORY_ID},
        },
        "title": "Approved change",
        "body": "Details\n\n<!-- continumm:run=00000000-0000-0000-0000-000000000001 -->",
        "html_url": "https://github.com/acme/widget/pull/17",
    }


class _GitHub:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.pull_pages: dict[int, list[dict[str, object]]] = {1: [_pr()]}
        self.page_count = 1
        self.next_link_mode = "none"
        self.permissions: list[dict[str, str]] = []
        self.detail = _pr()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        auth = request.headers.get("authorization")
        if path == "/user":
            return self._json({"id": USER_ID})
        if path == f"/repositories/{REPOSITORY_ID}" and auth == "Bearer fixture-user-token":
            return self._json(
                {
                    "id": REPOSITORY_ID,
                    "private": False,
                    "name": "widget",
                    "owner": {"login": "acme"},
                    "default_branch": "main",
                    "permissions": {"push": True},
                }
            )
        if path == "/repos/acme/widget/installation":
            return self._json({"id": 54321})
        if path == "/user/installations/54321/repositories":
            return self._json({"repositories": [{"id": REPOSITORY_ID}]})
        if path == "/app/installations/54321/access_tokens":
            requested = json.loads(request.content)["permissions"]
            self.permissions.append(requested)
            return self._json(
                {
                    "token": "fixture-installation-token",
                    "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    "permissions": requested,
                    "repository_selection": "selected",
                    "repositories": [{"id": REPOSITORY_ID}],
                }
            )
        if path == f"/repositories/{REPOSITORY_ID}" and auth == "Bearer fixture-installation-token":
            return self._json(
                {
                    "id": REPOSITORY_ID,
                    "private": False,
                    "name": "widget",
                    "owner": {"login": "acme"},
                }
            )
        if path == "/repos/acme/widget/git/ref/heads/main":
            return self._json(
                {"ref": "refs/heads/main", "object": {"sha": BASE_SHA, "type": "commit"}}
            )
        if (
            path
            == "/repos/acme/widget/git/ref/heads/continumm/run/00000000-0000-0000-0000-000000000001"
        ):
            return self._json(
                {
                    "ref": "refs/heads/continumm/run/00000000-0000-0000-0000-000000000001",
                    "object": {"sha": HEAD_SHA, "type": "commit"},
                }
            )
        if path == "/repos/acme/widget/pulls" and request.method == "GET":
            page = int(request.url.params.get("page", "1"))
            headers: dict[str, str] = {}
            if self.next_link_mode == "normal" and page < self.page_count:
                headers["link"] = (
                    f"<https://api.github.com{path}?state=all&per_page=100&page={page + 1}>"
                    '; rel="next"'
                )
            elif self.next_link_mode == "foreign":
                headers["link"] = '<https://attacker.invalid/pulls?page=2>; rel="next"'
            return self._json(self.pull_pages.get(page, []), headers=headers)
        if path == "/repos/acme/widget/pulls/17" and request.method == "GET":
            return self._json(self.detail)
        if path == "/repos/acme/widget/pulls" and request.method == "POST":
            payload = json.loads(request.content)
            result = _pr()
            result["title"] = payload["title"]
            result["body"] = payload["body"]
            return self._json(result, status=201)
        return self._json({"message": "not found"}, status=404)

    @staticmethod
    def _json(
        body: object,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        return httpx.Response(status, json=body, headers=headers)


def _transport(mock: _GitHub) -> GitHubPRTransport:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    authorization = GitHubAuthorizationClient(
        app_id=77,
        app_private_key=SecretStr(pem.decode()),
        user_tokens=_Tokens(),
        transport=httpx.MockTransport(mock),
    )
    return GitHubPRTransport(authorization, transport=httpx.MockTransport(mock))


def test_list_scans_all_states_and_parses_merged_state() -> None:
    mock = _GitHub()
    mock.pull_pages[1] = [_pr(state="closed", merged_at="2026-10-16T10:00:00Z")]
    records = asyncio.run(
        _transport(mock).list_pull_requests(
            REPOSITORY_ID,
            USER_ID,
            expected_owner="acme",
            expected_name="widget",
        )
    )
    assert records[0].state == "closed"
    assert records[0].merged is True
    assert any(request.url.params.get("state") == "all" for request in mock.requests)
    assert mock.permissions == [{"contents": "read", "metadata": "read", "pull_requests": "read"}]


def test_list_accepts_historical_unicode_refs_and_deleted_head_repository() -> None:
    mock = _GitHub()
    historical = _pr()
    historical_head = historical["head"]
    assert isinstance(historical_head, dict)
    historical_head["ref"] = "feature/café"
    historical_head["repo"] = None
    mock.pull_pages[1] = [historical]

    records = asyncio.run(
        _transport(mock).list_pull_requests(
            REPOSITORY_ID,
            USER_ID,
            expected_owner="acme",
            expected_name="widget",
        )
    )

    assert records[0].head_ref == "feature/café"
    assert records[0].head_repository_id is None
    assert records[0].head_owner is None


def test_list_follows_same_origin_pagination_until_complete() -> None:
    mock = _GitHub()
    mock.page_count = 2
    mock.next_link_mode = "normal"
    mock.pull_pages = {1: [_pr()], 2: [_pr()]}
    records = asyncio.run(
        _transport(mock).list_pull_requests(
            REPOSITORY_ID,
            USER_ID,
            expected_owner="acme",
            expected_name="widget",
        )
    )
    assert len(records) == 2
    assert [
        request.url.params.get("page")
        for request in mock.requests
        if request.url.path.endswith("/pulls")
    ] == ["1", "2"]


def test_list_rejects_foreign_pagination_link() -> None:
    mock = _GitHub()
    mock.next_link_mode = "foreign"
    with pytest.raises(GitHubPRTransportError, match="pagination_invalid"):
        asyncio.run(
            _transport(mock).list_pull_requests(
                REPOSITORY_ID,
                USER_ID,
                expected_owner="acme",
                expected_name="widget",
            )
        )


def test_list_fails_closed_when_page_limit_leaves_more_results() -> None:
    mock = _GitHub()
    mock.page_count = 11
    mock.next_link_mode = "normal"
    mock.pull_pages = {page: [_pr()] for page in range(1, 12)}
    with pytest.raises(GitHubPRTransportError, match="list_incomplete"):
        asyncio.run(
            _transport(mock).list_pull_requests(
                REPOSITORY_ID,
                USER_ID,
                expected_owner="acme",
                expected_name="widget",
            )
        )


def test_create_checks_fresh_refs_and_sends_once_with_pull_request_write_scope() -> None:
    mock = _GitHub()
    transport = _transport(mock)
    body = "Details\n\n<!-- continumm:run=00000000-0000-0000-0000-000000000001 -->"
    result = asyncio.run(
        transport.create_pull_request(
            REPOSITORY_ID,
            USER_ID,
            expected_owner="acme",
            expected_name="widget",
            expected_base_branch="main",
            expected_base_sha=BASE_SHA,
            expected_head_branch="continumm/run/00000000-0000-0000-0000-000000000001",
            expected_head_sha=HEAD_SHA,
            title="Approved change",
            body=body,
            marker="<!-- continumm:run=00000000-0000-0000-0000-000000000001 -->",
        )
    )
    assert result.number == 17
    assert mock.permissions == [{"contents": "read", "metadata": "read", "pull_requests": "write"}]
    posts = [
        request
        for request in mock.requests
        if request.method == "POST" and request.url.path.endswith("/pulls")
    ]
    assert len(posts) == 1
    assert json.loads(posts[0].content)["body"] == body


def test_create_stops_if_base_ref_moved_before_post() -> None:
    mock = _GitHub()
    original = mock.__call__

    def moved_base(request: httpx.Request) -> httpx.Response:
        if (
            request.url.path == "/repos/acme/widget/git/ref/heads/main"
            and request.headers.get("authorization") == "Bearer fixture-installation-token"
        ):
            mock.requests.append(request)
            return mock._json(
                {"ref": "refs/heads/main", "object": {"sha": "c" * 40, "type": "commit"}}
            )
        return original(request)

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    authorization = GitHubAuthorizationClient(
        app_id=77,
        app_private_key=SecretStr(pem.decode()),
        user_tokens=_Tokens(),
        transport=httpx.MockTransport(moved_base),
    )
    transport = GitHubPRTransport(authorization, transport=httpx.MockTransport(moved_base))
    with pytest.raises(GitHubPRTransportError, match="base_changed"):
        asyncio.run(
            transport.create_pull_request(
                REPOSITORY_ID,
                USER_ID,
                expected_owner="acme",
                expected_name="widget",
                expected_base_branch="main",
                expected_base_sha=BASE_SHA,
                expected_head_branch="continumm/run/00000000-0000-0000-0000-000000000001",
                expected_head_sha=HEAD_SHA,
                title="Approved change",
                body="Details\n\n<!-- continumm:run=00000000-0000-0000-0000-000000000001 -->",
                marker="<!-- continumm:run=00000000-0000-0000-0000-000000000001 -->",
            )
        )
    assert not any(
        request.method == "POST" and request.url.path.endswith("/pulls")
        for request in mock.requests
    )
