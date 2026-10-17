"""HTTP-level publication transport fixtures; never contacts GitHub."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import SecretStr

from continuum_api.auth.github import GitHubAuthorizationClient
from continuum_api.publication.github import (
    GitHubPublicationError,
    GitHubPublicationTransport,
    GitHubTreeEntry,
)

USER_ID = 12345
REPOSITORY_ID = 67890
HEAD = "a" * 40


class _Tokens:
    def get_for_user(self, github_user_id: int) -> SecretStr:
        assert github_user_id == USER_ID
        return SecretStr("fixture-user-token")


class _GitHub:
    def __init__(
        self,
        *,
        mutation_status: int = 201,
        mutation_sha: str | None = None,
        base_sha: str = HEAD,
        scoped_repository_id: int = REPOSITORY_ID,
    ) -> None:
        self.requests: list[httpx.Request] = []
        self.mutation_status = mutation_status
        self.mutation_sha = mutation_sha
        self.base_sha = base_sha
        self.scoped_repository_id = scoped_repository_id

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
            return self._json(
                {
                    "token": "fixture-installation-token",
                    "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    "permissions": requested,
                    "repository_selection": "selected",
                    "repositories": [{"id": self.scoped_repository_id}],
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
                {
                    "ref": "refs/heads/main",
                    "object": {"sha": self.base_sha, "type": "commit"},
                }
            )
        if path == "/repos/acme/widget/git/ref/heads/continumm/run/123":
            return self._json(
                {"ref": "refs/heads/continumm/run/123", "object": {"sha": HEAD, "type": "commit"}}
            )
        if path == "/repos/acme/widget/git/blobs":
            body = json.loads(request.content)
            content = base64.b64decode(body["content"])
            digest = hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest()
            if self.mutation_status == 302:
                return httpx.Response(302, headers={"location": "https://attacker.invalid/"})
            return self._json({"sha": self.mutation_sha or digest}, self.mutation_status)
        if path == "/repos/acme/widget/git/trees":
            tree = json.loads(request.content)["tree"]
            body = b"".join(
                (
                    ("40000" if entry["mode"] == "040000" else entry["mode"])
                    + " "
                    + entry["path"]
                    + "\0"
                ).encode()
                + bytes.fromhex(entry["sha"])
                for entry in tree
            )
            digest = hashlib.sha1(f"tree {len(body)}\0".encode() + body).hexdigest()
            return self._json({"sha": self.mutation_sha or digest}, self.mutation_status)
        if path == "/repos/acme/widget/git/commits":
            body = json.loads(request.content)
            author = body["author"]
            committer = body["committer"]
            author_ts = int(
                datetime.fromisoformat(author["date"].replace("Z", "+00:00")).timestamp()
            )
            committer_ts = int(
                datetime.fromisoformat(committer["date"].replace("Z", "+00:00")).timestamp()
            )
            author_line = f"{author['name']} <{author['email']}> {author_ts} +0000"
            committer_line = f"{committer['name']} <{committer['email']}> {committer_ts} +0000"
            content = (
                f"tree {body['tree']}\nparent {body['parents'][0]}\nauthor {author_line}\n"
                f"committer {committer_line}\n\n{body['message']}"
            ).encode()
            digest = hashlib.sha1(f"commit {len(content)}\0".encode() + content).hexdigest()
            return self._json({"sha": self.mutation_sha or digest}, self.mutation_status)
        if path.startswith("/repos/acme/widget/git/blobs/"):
            return self._json({"sha": path.rsplit("/", 1)[-1]})
        if path == "/repos/acme/widget/git/refs":
            body = json.loads(request.content)
            return self._json(
                {
                    "ref": body["ref"],
                    "object": {"sha": body["sha"], "type": "commit"},
                },
                self.mutation_status,
            )
        return self._json({"message": "not found"}, 404)

    @staticmethod
    def _json(body: object, status: int = 200) -> httpx.Response:
        return httpx.Response(status, json=body)


def _transport(mock: _GitHub) -> GitHubPublicationTransport:
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
    return GitHubPublicationTransport(authorization, transport=httpx.MockTransport(mock))


def test_read_branch_uses_fresh_numeric_repository_authorization() -> None:
    mock = _GitHub()
    transport = _transport(mock)
    result = asyncio.run(
        transport.get_ref(
            REPOSITORY_ID,
            USER_ID,
            "continumm/run/123",
            expected_owner="acme",
            expected_name="widget",
        )
    )
    assert result == HEAD
    api_requests = [request for request in mock.requests if request.url.host == "api.github.com"]
    assert all(request.url.scheme == "https" for request in api_requests)
    assert any(
        request.url.path.endswith("/git/ref/heads/continumm/run/123") for request in api_requests
    )
    assert all(
        request.headers.get("authorization") != "Bearer fixture-user-token"
        for request in api_requests
        if "/git/ref/heads/continumm" in request.url.path
    )
    assert (
        asyncio.run(
            transport.get_object(
                REPOSITORY_ID,
                USER_ID,
                "blob",
                HEAD,
                expected_owner="acme",
                expected_name="widget",
            )
        )
        == HEAD
    )


def test_wrong_token_repository_scope_fails_before_git_read_or_write() -> None:
    mock = _GitHub(scoped_repository_id=REPOSITORY_ID + 1)
    transport = _transport(mock)
    content = b"candidate\n"
    sha = hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest()
    with pytest.raises(GitHubPublicationError, match="repository_scope_invalid"):
        asyncio.run(
            transport.upload_blob(
                REPOSITORY_ID,
                USER_ID,
                content=content,
                expected_sha=sha,
                expected_base_branch="main",
                expected_base_sha=HEAD,
                expected_owner="acme",
                expected_name="widget",
            )
        )
    assert not any("/git/blobs" in request.url.path for request in mock.requests)


def test_upload_blob_is_content_addressed_and_branch_publication_only_creates_ref() -> None:
    mock = _GitHub()
    transport = _transport(mock)
    content = b"approved candidate\n"
    sha = hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest()
    assert (
        asyncio.run(
            transport.upload_blob(
                REPOSITORY_ID,
                USER_ID,
                content=content,
                expected_sha=sha,
                expected_base_branch="main",
                expected_base_sha=HEAD,
                expected_owner="acme",
                expected_name="widget",
            )
        )
        == sha
    )
    assert (
        asyncio.run(
            transport.create_ref(
                REPOSITORY_ID,
                USER_ID,
                branch="continumm/run/123",
                sha=HEAD,
                expected_base_branch="main",
                expected_base_sha=HEAD,
                expected_owner="acme",
                expected_name="widget",
            )
        )
        == HEAD
    )

    writes = [
        request
        for request in mock.requests
        if request.url.path.startswith("/repos/acme/widget/git/") and request.method != "GET"
    ]
    assert [request.method for request in writes] == ["POST", "POST"]
    assert writes[0].url.path.endswith("/git/blobs")
    assert writes[1].url.path.endswith("/git/refs")
    assert json.loads(writes[1].content) == {"ref": "refs/heads/continumm/run/123", "sha": HEAD}
    token_request = next(
        request for request in mock.requests if request.url.path.endswith("/access_tokens")
    )
    assert json.loads(token_request.content) == {
        "repository_ids": [REPOSITORY_ID],
        "permissions": {"contents": "write", "metadata": "read"},
    }
    assert not any(request.method == "PATCH" for request in mock.requests)
    assert all(request.url.host == "api.github.com" for request in mock.requests)


def test_write_rejects_local_or_remote_sha_mismatch() -> None:
    mock = _GitHub()
    transport = _transport(mock)
    with pytest.raises(GitHubPublicationError, match="object_sha_mismatch"):
        asyncio.run(
            transport.upload_blob(
                REPOSITORY_ID,
                USER_ID,
                content=b"approved candidate\n",
                expected_sha="b" * 40,
                expected_base_branch="main",
                expected_base_sha=HEAD,
                expected_owner="acme",
                expected_name="widget",
            )
        )
    assert not any(request.url.path.endswith("/git/blobs") for request in mock.requests)

    wrong_response = _GitHub(mutation_sha="b" * 40)
    transport = _transport(wrong_response)
    content = b"approved candidate\n"
    sha = hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest()
    with pytest.raises(GitHubPublicationError, match="object_sha_mismatch"):
        asyncio.run(
            transport.upload_blob(
                REPOSITORY_ID,
                USER_ID,
                content=content,
                expected_sha=sha,
                expected_base_branch="main",
                expected_base_sha=HEAD,
                expected_owner="acme",
                expected_name="widget",
            )
        )


def test_base_moved_during_write_authorization_prevents_post() -> None:
    mock = _GitHub(base_sha="b" * 40)
    transport = _transport(mock)
    content = b"approved candidate\n"
    sha = hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest()
    with pytest.raises(GitHubPublicationError, match="base_changed"):
        asyncio.run(
            transport.upload_blob(
                REPOSITORY_ID,
                USER_ID,
                content=content,
                expected_sha=sha,
                expected_base_branch="main",
                expected_base_sha=HEAD,
                expected_owner="acme",
                expected_name="widget",
            )
        )
    assert not any(request.url.path.endswith("/git/blobs") for request in mock.requests)


def test_tree_and_commit_uploads_preserve_approved_object_hashes() -> None:
    mock = _GitHub()
    transport = _transport(mock)
    blob_content = b"candidate\n"
    blob_sha = hashlib.sha1(f"blob {len(blob_content)}\0".encode() + blob_content).hexdigest()
    tree_body = b"100644 file.txt\0" + bytes.fromhex(blob_sha)
    tree_sha = hashlib.sha1(f"tree {len(tree_body)}\0".encode() + tree_body).hexdigest()
    timestamp = 1_700_000_000
    identity = {
        "name": "Continumm",
        "email": "noreply@continumm.invalid",
        "timestamp": timestamp,
        "timezone": "+0000",
    }
    message = "Apply reviewed change\n"
    identity_line = f"Continumm <noreply@continumm.invalid> {timestamp} +0000"
    commit_body = (
        f"tree {tree_sha}\nparent {HEAD}\nauthor {identity_line}\n"
        f"committer {identity_line}\n\n{message}"
    ).encode()
    commit_sha = hashlib.sha1(f"commit {len(commit_body)}\0".encode() + commit_body).hexdigest()
    approved = {
        "expected_base_branch": "main",
        "expected_base_sha": HEAD,
        "expected_owner": "acme",
        "expected_name": "widget",
    }
    assert (
        asyncio.run(
            transport.upload_tree(
                REPOSITORY_ID,
                USER_ID,
                entries=(GitHubTreeEntry("file.txt", "100644", "blob", blob_sha),),
                expected_sha=tree_sha,
                **approved,
            )
        )
        == tree_sha
    )
    assert (
        asyncio.run(
            transport.upload_commit(
                REPOSITORY_ID,
                USER_ID,
                message=message,
                tree_sha=tree_sha,
                parent_sha=HEAD,
                author=identity,
                committer=identity,
                expected_sha=commit_sha,
                **approved,
            )
        )
        == commit_sha
    )


def test_redirect_is_rejected_without_following_or_retrying_mutation() -> None:
    mock = _GitHub(mutation_status=302)
    transport = _transport(mock)
    content = b"approved candidate\n"
    sha = hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest()
    with pytest.raises(GitHubPublicationError, match="redirect_rejected"):
        asyncio.run(
            transport.upload_blob(
                REPOSITORY_ID,
                USER_ID,
                content=content,
                expected_sha=sha,
                expected_base_branch="main",
                expected_base_sha=HEAD,
                expected_owner="acme",
                expected_name="widget",
            )
        )
    assert sum(request.url.path.endswith("/git/blobs") for request in mock.requests) == 1
    assert not any(request.url.host == "attacker.invalid" for request in mock.requests)
