"""Bounded GitHub Git Database transport for approved publications.

The adapter only sends requests to the fixed GitHub API origin, obtains a fresh
repository-scoped installation credential for each call, and never retries writes.
Repository code and execution workers never receive that credential.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import httpx
from pydantic import SecretStr

from continuum_api.auth.github import GitHubAuthorizationClient, GitHubAuthorizationError
from continuum_api.repositories.preflight import GITHUB_API_ORIGIN, GITHUB_API_VERSION
from continuum_api.runtime.approvals import ApprovalError, RepositoryPublicationFacts

MAX_REQUEST_BYTES = 70 * 1024 * 1024
MAX_RESPONSE_BYTES = 68 * 1024 * 1024
MAX_TOTAL_RESPONSE_BYTES = 70 * 1024 * 1024
DEADLINE_SECONDS = 20.0
HTTP_TIMEOUT_SECONDS = 18.0
_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_BRANCH_PATTERN = re.compile(r"^[A-Za-z0-9._/-]{1,255}$")


class GitHubPublicationError(RuntimeError):
    """Sanitized transport failure that excludes remote bodies and credentials."""

    def __init__(self, code: str = "github_publication_failed") -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class GitHubTreeEntry:
    path: str
    mode: str
    type: str
    sha: str


class _Budget:
    def __init__(self) -> None:
        self.response_bytes = 0

    def start(self, request_bytes: int) -> None:
        if request_bytes > MAX_REQUEST_BYTES:
            raise GitHubPublicationError("github_publication_request_limit")

    def consume(self, count: int) -> None:
        self.response_bytes += count
        if self.response_bytes > MAX_TOTAL_RESPONSE_BYTES:
            raise GitHubPublicationError("github_publication_response_limit")


class GitHubPublicationTransport:
    """Small HTTP-level transport for the publication service.

    Reads use an installation token scoped to exactly one numeric repository. Each
    write rechecks actor and installation access and mints a contents:write token for
    that repository. Mutation requests are single-send operations.
    """

    def __init__(
        self,
        client: GitHubAuthorizationClient,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._authorization = client
        self._transport = transport

    async def current(self, repository_id: int, github_user_id: int) -> RepositoryPublicationFacts:
        self._validate_ids(repository_id, github_user_id)
        try:
            return await self._authorization.current(repository_id, github_user_id)
        except (ApprovalError, GitHubAuthorizationError) as error:
            raise GitHubPublicationError(
                getattr(error, "code", "github_authorization_failed")
            ) from None

    async def get_default_base(self, repository_id: int, github_user_id: int) -> str:
        """Read the currently authorized default-branch head through fresh facts."""
        return (await self.current(repository_id, github_user_id)).base_commit_sha

    async def get_ref(
        self,
        repository_id: int,
        github_user_id: int,
        branch: str,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> str | None:
        """Read one branch after fresh authorization; a missing branch is ``None``."""
        facts, token = await self._read_credentials(repository_id, github_user_id)
        self._assert_repository_identity(facts, repository_id, expected_owner, expected_name)
        self._validate_branch(branch)
        path = self._repository_path(facts) + "/git/ref/heads/" + quote(branch, safe="/")
        budget = _Budget()
        try:
            async with asyncio.timeout(DEADLINE_SECONDS):
                async with self._http_client() as client:
                    payload = await self._request_json(
                        client,
                        budget,
                        "GET",
                        path,
                        token=token,
                        allow_not_found=True,
                    )
        except TimeoutError:
            raise GitHubPublicationError("github_publication_timeout") from None
        if payload is None:
            return None
        object_value = payload.get("object")
        result = object_value.get("sha") if isinstance(object_value, dict) else None
        if (
            payload.get("ref") != f"refs/heads/{branch}"
            or not isinstance(object_value, dict)
            or object_value.get("type") != "commit"
            or not isinstance(result, str)
            or _SHA_PATTERN.fullmatch(result) is None
        ):
            raise GitHubPublicationError("github_publication_response_invalid")
        return result

    async def get_object(
        self,
        repository_id: int,
        github_user_id: int,
        kind: str,
        sha: str,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> str | None:
        """Observe an existing Git object for reconciliation without writing."""
        if kind not in {"blob", "tree", "commit"}:
            raise GitHubPublicationError("github_publication_object_invalid")
        self._require_sha(sha)
        facts, token = await self._read_credentials(repository_id, github_user_id)
        self._assert_repository_identity(facts, repository_id, expected_owner, expected_name)
        budget = _Budget()
        path = self._repository_path(facts) + f"/git/{kind}s/{quote(sha, safe='')}"
        try:
            async with asyncio.timeout(DEADLINE_SECONDS):
                async with self._http_client() as client:
                    result = await self._request_json(
                        client,
                        budget,
                        "GET",
                        path,
                        token=token,
                        allow_not_found=True,
                    )
        except TimeoutError:
            raise GitHubPublicationError("github_publication_timeout") from None
        if result is None:
            return None
        returned = result.get("sha")
        if not isinstance(returned, str) or returned != sha:
            raise GitHubPublicationError("github_publication_object_sha_mismatch")
        return returned

    async def upload_blob(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        content: bytes,
        expected_sha: str,
        expected_base_branch: str,
        expected_base_sha: str,
        expected_owner: str,
        expected_name: str,
    ) -> str:
        if not isinstance(content, bytes):
            raise GitHubPublicationError("github_publication_object_invalid")
        local_sha = hashlib.sha1(f"blob {len(content)}\0".encode() + content).hexdigest()
        self._require_sha(expected_sha)
        if local_sha != expected_sha:
            raise GitHubPublicationError("github_publication_object_sha_mismatch")
        payload = await self._write_json(
            repository_id,
            github_user_id,
            "/git/blobs",
            {"content": base64.b64encode(content).decode("ascii"), "encoding": "base64"},
            expected_sha,
            expected_base_branch,
            expected_base_sha,
            expected_owner,
            expected_name,
        )
        return payload

    async def upload_tree(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        entries: Sequence[GitHubTreeEntry | Mapping[str, str]],
        expected_sha: str,
        expected_base_branch: str,
        expected_base_sha: str,
        expected_owner: str,
        expected_name: str,
    ) -> str:
        self._require_sha(expected_sha)
        normalized: list[dict[str, str]] = []
        for entry in entries:
            item = (
                {"path": entry.path, "mode": entry.mode, "type": entry.type, "sha": entry.sha}
                if isinstance(entry, GitHubTreeEntry)
                else dict(entry)
                if isinstance(entry, Mapping)
                else None
            )
            if (
                item is None
                or set(item) != {"path", "mode", "type", "sha"}
                or not isinstance(item["path"], str)
                or not item["path"]
                or "/" in item["path"]
                or item["path"] in {".", ".."}
                or "\0" in item["path"]
                or not isinstance(item["mode"], str)
                or item["mode"] not in {"100644", "100755", "40000", "040000"}
                or not isinstance(item["type"], str)
                or item["type"] not in {"blob", "tree"}
                or not isinstance(item["sha"], str)
                or _SHA_PATTERN.fullmatch(item["sha"]) is None
                or (item["mode"] in {"40000", "040000"}) != (item["type"] == "tree")
            ):
                raise GitHubPublicationError("github_publication_tree_invalid")
            normalized.append(item)
        normalized.sort(
            key=lambda item: (item["path"] + ("/" if item["type"] == "tree" else "")).encode()
        )
        serialized = b"".join(
            (
                ("40000" if entry["mode"] == "040000" else entry["mode"])
                + " "
                + entry["path"]
                + "\0"
            ).encode("utf-8")
            + bytes.fromhex(entry["sha"])
            for entry in normalized
        )
        local_sha = hashlib.sha1(f"tree {len(serialized)}\0".encode() + serialized).hexdigest()
        if local_sha != expected_sha:
            raise GitHubPublicationError("github_publication_object_sha_mismatch")
        return await self._write_json(
            repository_id,
            github_user_id,
            "/git/trees",
            {"tree": normalized},
            expected_sha,
            expected_base_branch,
            expected_base_sha,
            expected_owner,
            expected_name,
        )

    async def upload_commit(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        message: str,
        tree_sha: str,
        parent_sha: str,
        author: Mapping[str, Any],
        committer: Mapping[str, Any],
        expected_sha: str,
        expected_base_branch: str,
        expected_base_sha: str,
        expected_owner: str,
        expected_name: str,
    ) -> str:
        self._require_sha(tree_sha)
        self._require_sha(parent_sha)
        self._require_sha(expected_sha)
        if not isinstance(message, str) or not message or len(message.encode("utf-8")) > 16_384:
            raise GitHubPublicationError("github_publication_commit_invalid")
        author_payload, author_timestamp = self._normalize_identity(author)
        committer_payload, committer_timestamp = self._normalize_identity(committer)
        author_line = (
            f"{author_payload['name']} <{author_payload['email']}> {author_timestamp} +0000"
        )
        committer_line = (
            f"{committer_payload['name']} <{committer_payload['email']}> "
            f"{committer_timestamp} +0000"
        )
        commit_bytes = (
            f"tree {tree_sha}\nparent {parent_sha}\nauthor {author_line}\n"
            f"committer {committer_line}\n\n{message}"
        ).encode()
        local_sha = hashlib.sha1(
            f"commit {len(commit_bytes)}\0".encode() + commit_bytes
        ).hexdigest()
        if local_sha != expected_sha:
            raise GitHubPublicationError("github_publication_object_sha_mismatch")
        return await self._write_json(
            repository_id,
            github_user_id,
            "/git/commits",
            {
                "message": message,
                "tree": tree_sha,
                "parents": [parent_sha],
                "author": author_payload,
                "committer": committer_payload,
            },
            expected_sha,
            expected_base_branch,
            expected_base_sha,
            expected_owner,
            expected_name,
        )

    async def create_ref(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        branch: str,
        sha: str,
        expected_base_branch: str,
        expected_base_sha: str,
        expected_owner: str,
        expected_name: str,
    ) -> str:
        self._validate_branch(branch)
        self._require_sha(sha)
        facts, token = await self._publication_credentials(
            repository_id,
            github_user_id,
            expected_base_branch=expected_base_branch,
            expected_base_sha=expected_base_sha,
            expected_owner=expected_owner,
            expected_name=expected_name,
        )
        budget = _Budget()
        path = self._repository_path(facts) + "/git/refs"
        try:
            async with asyncio.timeout(DEADLINE_SECONDS):
                async with self._http_client() as client:
                    result = await self._request_json(
                        client,
                        budget,
                        "POST",
                        path,
                        token=token,
                        json_body={"ref": f"refs/heads/{branch}", "sha": sha},
                    )
        except TimeoutError:
            raise GitHubPublicationError("github_publication_timeout") from None
        if result is None:
            raise GitHubPublicationError("github_publication_response_invalid")
        object_value = result.get("object")
        returned = object_value.get("sha") if isinstance(object_value, dict) else None
        if (
            result.get("ref") != f"refs/heads/{branch}"
            or not isinstance(object_value, dict)
            or object_value.get("type") != "commit"
            or returned != sha
        ):
            raise GitHubPublicationError("github_publication_object_sha_mismatch")
        return returned

    async def _write_json(
        self,
        repository_id: int,
        github_user_id: int,
        suffix: str,
        body: Mapping[str, Any],
        expected_sha: str,
        expected_base_branch: str,
        expected_base_sha: str,
        expected_owner: str,
        expected_name: str,
    ) -> str:
        encoded_size = len(json.dumps(body, separators=(",", ":")).encode("utf-8"))
        budget = _Budget()
        budget.start(encoded_size)
        facts, token = await self._publication_credentials(
            repository_id,
            github_user_id,
            expected_base_branch=expected_base_branch,
            expected_base_sha=expected_base_sha,
            expected_owner=expected_owner,
            expected_name=expected_name,
        )
        try:
            async with asyncio.timeout(DEADLINE_SECONDS):
                async with self._http_client() as client:
                    result = await self._request_json(
                        client,
                        budget,
                        "POST",
                        self._repository_path(facts) + suffix,
                        token=token,
                        json_body=body,
                        request_already_counted=True,
                    )
        except TimeoutError:
            raise GitHubPublicationError("github_publication_timeout") from None
        if result is None:
            raise GitHubPublicationError("github_publication_response_invalid")
        returned = result.get("sha")
        if not isinstance(returned, str) or returned != expected_sha:
            raise GitHubPublicationError("github_publication_object_sha_mismatch")
        return returned

    async def _read_credentials(
        self, repository_id: int, github_user_id: int
    ) -> tuple[RepositoryPublicationFacts, SecretStr]:
        self._validate_ids(repository_id, github_user_id)
        try:
            facts, token = await self._authorization.repository_read_authorization(
                repository_id, github_user_id
            )
        except (ApprovalError, GitHubAuthorizationError) as error:
            raise GitHubPublicationError(
                getattr(error, "code", "github_authorization_failed")
            ) from None
        return facts, token

    async def _publication_credentials(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        expected_base_branch: str,
        expected_base_sha: str,
        expected_owner: str,
        expected_name: str,
    ) -> tuple[RepositoryPublicationFacts, SecretStr]:
        self._validate_ids(repository_id, github_user_id)
        try:
            facts, token = await self._authorization.publication_authorization(
                repository_id, github_user_id
            )
        except (ApprovalError, GitHubAuthorizationError) as error:
            raise GitHubPublicationError(
                getattr(error, "code", "github_authorization_failed")
            ) from None
        if (
            facts.owner != expected_owner
            or facts.name != expected_name
            or facts.default_branch != expected_base_branch
            or facts.base_commit_sha != expected_base_sha
        ):
            raise GitHubPublicationError("github_publication_base_changed")
        return facts, token

    @staticmethod
    def _assert_repository_identity(
        facts: RepositoryPublicationFacts,
        repository_id: int,
        expected_owner: str,
        expected_name: str,
    ) -> None:
        if (
            facts.repository_id != repository_id
            or facts.owner != expected_owner
            or facts.name != expected_name
        ):
            raise GitHubPublicationError("github_publication_repository_mismatch")

    def _http_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=GITHUB_API_ORIGIN,
            headers={
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": GITHUB_API_VERSION,
                "User-Agent": "Continuum",
            },
            timeout=httpx.Timeout(HTTP_TIMEOUT_SECONDS),
            follow_redirects=False,
            trust_env=False,
            transport=self._transport,
        )

    async def _request_json(
        self,
        client: httpx.AsyncClient,
        budget: _Budget,
        method: str,
        path: str,
        *,
        token: SecretStr,
        json_body: Mapping[str, Any] | None = None,
        allow_not_found: bool = False,
        request_already_counted: bool = False,
    ) -> dict[str, Any] | None:
        if not path.startswith("/") or path.startswith("//"):
            raise GitHubPublicationError("github_publication_path_invalid")
        request_size = (
            0
            if json_body is None
            else len(json.dumps(json_body, separators=(",", ":")).encode("utf-8"))
        )
        if not request_already_counted:
            budget.start(request_size)
        try:
            async with client.stream(
                method,
                path,
                headers={"Authorization": f"Bearer {token.get_secret_value()}"},
                json=json_body,
            ) as response:
                if response.status_code == 404 and allow_not_found:
                    return None
                if 300 <= response.status_code < 400:
                    raise GitHubPublicationError("github_publication_redirect_rejected")
                if response.status_code < 200 or response.status_code >= 300:
                    raise GitHubPublicationError("github_publication_request_failed")
                content_length = response.headers.get("content-length")
                if content_length:
                    try:
                        if int(content_length) > MAX_RESPONSE_BYTES:
                            raise GitHubPublicationError("github_publication_response_limit")
                    except ValueError:
                        raise GitHubPublicationError(
                            "github_publication_response_invalid"
                        ) from None
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > MAX_RESPONSE_BYTES:
                        raise GitHubPublicationError("github_publication_response_limit")
                    budget.consume(len(chunk))
                    body.extend(chunk)
        except GitHubPublicationError:
            raise
        except (httpx.HTTPError, OSError, TimeoutError):
            raise GitHubPublicationError("github_publication_unavailable") from None
        try:
            result = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise GitHubPublicationError("github_publication_response_invalid") from None
        if not isinstance(result, dict):
            raise GitHubPublicationError("github_publication_response_invalid")
        return result

    @staticmethod
    def _repository_path(facts: RepositoryPublicationFacts) -> str:
        if type(facts.repository_id) is not int or facts.repository_id <= 0:
            raise GitHubPublicationError("repository_id_invalid")
        for component in (facts.owner, facts.name):
            if not isinstance(component, str) or not re.fullmatch(
                r"[A-Za-z0-9_.-]{1,100}", component
            ):
                raise GitHubPublicationError("repository_metadata_invalid")
        return f"/repos/{quote(facts.owner, safe='')}/{quote(facts.name, safe='')}"

    @staticmethod
    def _validate_ids(repository_id: int, github_user_id: int) -> None:
        if type(repository_id) is not int or repository_id <= 0:
            raise GitHubPublicationError("repository_id_invalid")
        if type(github_user_id) is not int or github_user_id <= 0:
            raise GitHubPublicationError("github_user_id_invalid")

    @staticmethod
    def _validate_branch(branch: str) -> None:
        if (
            not isinstance(branch, str)
            or _BRANCH_PATTERN.fullmatch(branch) is None
            or branch.startswith("/")
            or branch.endswith("/")
            or ".." in branch
            or "//" in branch
            or branch.endswith(".")
        ):
            raise GitHubPublicationError("github_publication_branch_invalid")

    @staticmethod
    def _require_sha(value: str) -> None:
        if not isinstance(value, str) or _SHA_PATTERN.fullmatch(value) is None:
            raise GitHubPublicationError("github_publication_sha_invalid")

    @staticmethod
    def _normalize_identity(value: Mapping[str, Any]) -> tuple[dict[str, str], int]:
        if not isinstance(value, Mapping):
            raise GitHubPublicationError("github_publication_commit_invalid")
        name = value.get("name")
        email = value.get("email")
        timestamp = value.get("timestamp")
        timezone = value.get("timezone", "+0000")
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(email, str)
            or not email
            or type(timestamp) is not int
            or timestamp < 0
            or timestamp > 253_402_300_799
            or set(value)
            not in (
                {"name", "email", "timestamp"},
                {"name", "email", "timestamp", "timezone"},
            )
            or timezone != "+0000"
            or "\n" in name + email
            or "\0" in name + email
        ):
            raise GitHubPublicationError("github_publication_commit_invalid")
        date = datetime.fromtimestamp(timestamp, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return {"name": name, "email": email, "date": date}, timestamp
