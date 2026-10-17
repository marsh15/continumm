"""HTTP-mock proofs for GitHub identity and dual repository access."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import SecretStr

from continuum_api.auth.authorization import (
    GitHubAccessVerifier,
    GitHubAuthorizationClient,
    GitHubPublicationFactsProvider,
)
from continuum_api.repositories.preflight import RepositoryPreflightError
from continuum_api.runtime.approvals import ApprovalError

USER_ID = 12345
REPOSITORY_ID = 67890
INSTALLATION_ID = 54321
HEAD = "a" * 40
_DEFAULT_TOKEN = SecretStr("synthetic-user-token")


class GitHubMock:
    """Reusable HTTPX transport fixture for authorization and publication tests."""

    def __init__(
        self,
        *,
        identity_id: int = USER_ID,
        user_repository_id: int = REPOSITORY_ID,
        actor_push: bool = True,
        installation_status: int = 200,
        installation_repositories: list[dict[str, Any]] | None = None,
        full_installation_pages: bool = False,
    ) -> None:
        self.requests: list[httpx.Request] = []
        self.identity_id = identity_id
        self.user_repository_id = user_repository_id
        self.actor_push = actor_push
        self.installation_status = installation_status
        self.installation_repositories = installation_repositories
        self.full_installation_pages = full_installation_pages

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        auth = request.headers.get("authorization")
        if path == "/user":
            return _json({"id": self.identity_id})
        if path == f"/repositories/{REPOSITORY_ID}" and auth == "Bearer synthetic-user-token":
            return _json(
                {
                    "id": self.user_repository_id,
                    "private": False,
                    "name": "widget",
                    "full_name": "acme/widget",
                    "owner": {"login": "acme"},
                    "default_branch": "main",
                    "permissions": {"push": self.actor_push},
                }
            )
        if path == "/repos/acme/widget/installation":
            return _json({"id": INSTALLATION_ID}, self.installation_status)
        if path == f"/user/installations/{INSTALLATION_ID}/repositories":
            if self.full_installation_pages:
                page = int(request.url.params.get("page", "1"))
                return _json(
                    {"repositories": [{"id": page * 1000 + index} for index in range(100)]}
                )
            repos = self.installation_repositories
            if repos is None:
                repos = [{"id": REPOSITORY_ID}]
            return _json({"repositories": repos})
        if path == f"/app/installations/{INSTALLATION_ID}/access_tokens":
            if request.method != "POST":
                return _json({"message": "unexpected method"}, 405)
            return _json({"token": "synthetic-installation-token"})
        if (
            path == f"/repositories/{REPOSITORY_ID}"
            and auth == "Bearer synthetic-installation-token"
        ):
            return _json(
                {
                    "id": REPOSITORY_ID,
                    "private": False,
                    "name": "widget",
                    "full_name": "acme/widget",
                    "owner": {"login": "acme"},
                }
            )
        if path == "/repos/acme/widget/git/ref/heads/main":
            return _json({"object": {"sha": HEAD}})
        return _json({"message": "not found"}, 404)


class _TokenStore:
    def __init__(self, token: SecretStr | None = _DEFAULT_TOKEN) -> None:
        self.token = token
        self.lookups: list[int] = []

    def get_for_user(self, github_user_id: int) -> SecretStr | None:
        self.lookups.append(github_user_id)
        return self.token


@pytest.fixture
def private_key() -> SecretStr:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return SecretStr(pem.decode("ascii"))


def _client(
    mock: GitHubMock, private_key: SecretStr, store: _TokenStore | None = None
) -> tuple[GitHubAuthorizationClient, _TokenStore]:
    token_store = store if store is not None else _TokenStore()
    client = GitHubAuthorizationClient(
        app_id=77,
        app_private_key=private_key,
        user_tokens=token_store,
        transport=httpx.MockTransport(mock),
    )
    return client, token_store


def _json(payload: object, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload)


def test_current_facts_require_user_and_app_access_and_only_read_scope(
    private_key: SecretStr,
) -> None:
    mock = GitHubMock()
    client, store = _client(mock, private_key)
    facts = asyncio.run(GitHubPublicationFactsProvider(client).current(REPOSITORY_ID, USER_ID))

    assert facts.repository_id == REPOSITORY_ID
    assert facts.github_user_id == USER_ID
    assert (facts.owner, facts.name, facts.default_branch, facts.base_commit_sha) == (
        "acme",
        "widget",
        "main",
        HEAD,
    )
    assert facts.access_proof.actor_can_write and facts.access_proof.app_can_access
    assert store.lookups == [USER_ID]
    token_request = next(
        request for request in mock.requests if request.url.path.endswith("/access_tokens")
    )
    assert json.loads(token_request.content) == {
        "repository_ids": [REPOSITORY_ID],
        "permissions": {"contents": "read", "metadata": "read"},
    }
    assert all(
        request.method == "GET" or request.url.path.endswith("/access_tokens")
        for request in mock.requests
    )
    assert not any(
        request.method in {"PATCH", "PUT", "DELETE"}
        or (request.method == "POST" and "/access_tokens" not in request.url.path)
        for request in mock.requests
    )


def test_wrong_numeric_user_identity_fails_closed(private_key: SecretStr) -> None:
    mock = GitHubMock(identity_id=USER_ID + 1)
    client, _ = _client(mock, private_key)
    with pytest.raises(ApprovalError, match="github_identity_mismatch"):
        asyncio.run(GitHubPublicationFactsProvider(client).current(REPOSITORY_ID, USER_ID))
    assert [request.url.path for request in mock.requests] == ["/user"]


def test_wrong_numeric_repository_identity_fails_closed(private_key: SecretStr) -> None:
    mock = GitHubMock(user_repository_id=REPOSITORY_ID + 1)
    client, _ = _client(mock, private_key)
    with pytest.raises(ApprovalError, match="repository_id_mismatch"):
        asyncio.run(GitHubPublicationFactsProvider(client).current(REPOSITORY_ID, USER_ID))
    assert len(mock.requests) == 2


def test_removed_actor_write_permission_fails_closed(private_key: SecretStr) -> None:
    mock = GitHubMock(actor_push=False)
    client, _ = _client(mock, private_key)
    with pytest.raises(ApprovalError, match="repository_actor_write_access_required"):
        asyncio.run(GitHubPublicationFactsProvider(client).current(REPOSITORY_ID, USER_ID))
    assert len(mock.requests) == 2


def test_uninstalled_app_fails_closed(private_key: SecretStr) -> None:
    mock = GitHubMock(installation_status=404)
    client, _ = _client(mock, private_key)
    with pytest.raises(ApprovalError, match="github_api_request_failed"):
        asyncio.run(GitHubPublicationFactsProvider(client).current(REPOSITORY_ID, USER_ID))
    assert not any(request.url.path.endswith("/access_tokens") for request in mock.requests)


def test_installation_user_repository_intersection_is_required(private_key: SecretStr) -> None:
    mock = GitHubMock(installation_repositories=[{"id": REPOSITORY_ID + 1}])
    client, _ = _client(mock, private_key)
    with pytest.raises(ApprovalError, match="github_installation_user_access_required"):
        asyncio.run(GitHubPublicationFactsProvider(client).current(REPOSITORY_ID, USER_ID))
    assert not any(request.url.path.endswith("/access_tokens") for request in mock.requests)


def test_bounded_installation_pagination_fails_closed(private_key: SecretStr) -> None:
    mock = GitHubMock(full_installation_pages=True)
    client, _ = _client(mock, private_key)
    with pytest.raises(ApprovalError, match="github_installation_page_limit"):
        asyncio.run(GitHubPublicationFactsProvider(client).current(REPOSITORY_ID, USER_ID))
    pages = [request for request in mock.requests if request.url.path.endswith("/repositories")]
    assert [request.url.params.get("page") for request in pages] == ["1", "2", "3", "4", "5"]


def test_access_verifier_maps_failures_to_preflight_error(private_key: SecretStr) -> None:
    mock = GitHubMock(actor_push=False)
    client, _ = _client(mock, private_key)
    with pytest.raises(RepositoryPreflightError, match="repository_actor_write_access_required"):
        asyncio.run(
            GitHubAccessVerifier(client).verify(repository_id=REPOSITORY_ID, github_user_id=USER_ID)
        )


def test_missing_session_token_fails_before_network(private_key: SecretStr) -> None:
    mock = GitHubMock()
    client, _ = _client(mock, private_key, _TokenStore(None))
    with pytest.raises(ApprovalError, match="github_user_token_unavailable"):
        asyncio.run(GitHubPublicationFactsProvider(client).current(REPOSITORY_ID, USER_ID))
    assert mock.requests == []
