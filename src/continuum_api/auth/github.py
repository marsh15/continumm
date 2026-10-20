"""Live, fail-closed GitHub App authorization and publication facts."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import quote

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from pydantic import SecretStr

from continuum_api.repositories.preflight import (
    GITHUB_API_ORIGIN,
    GITHUB_API_VERSION,
    RepositoryAccessProof,
    RepositoryAccessVerifier,
    RepositoryPreflightError,
)
from continuum_api.runtime.approvals import (
    ApprovalError,
    PublicationFactsProvider,
    RepositoryPublicationFacts,
)

MAX_RESPONSE_BYTES = 512 * 1024
MAX_TOTAL_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_REQUESTS = 24
MAX_PAGES = 5
PAGE_SIZE = 100
MAX_ITEMS = MAX_PAGES * PAGE_SIZE
AUTHORIZATION_DEADLINE_SECONDS = 20.0
HTTP_TIMEOUT_SECONDS = 6.0
_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_COMPONENT_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")


class GitHubAuthorizationError(RuntimeError):
    """Sanitized authorization failure; never contains remote response details."""

    def __init__(self, code: str = "github_authorization_failed") -> None:
        super().__init__(code)
        self.code = code


class GitHubUserTokenStore(Protocol):
    """Encrypted, immutable-account-bound GitHub user-token retrieval boundary."""

    def get_for_user(self, github_user_id: int) -> SecretStr | None: ...


@dataclass(slots=True)
class _RequestBudget:
    requests: int = 0
    response_bytes: int = 0

    def start(self) -> None:
        self.requests += 1
        if self.requests > MAX_REQUESTS:
            raise GitHubAuthorizationError("github_authorization_request_limit")

    def consume(self, count: int) -> None:
        self.response_bytes += count
        if self.response_bytes > MAX_TOTAL_RESPONSE_BYTES:
            raise GitHubAuthorizationError("github_authorization_response_limit")


@dataclass(frozen=True, slots=True)
class GitHubIssueSnapshot:
    repository_id: int
    github_user_id: int
    issue_id: int
    issue_number: int
    title: str = field(repr=False)
    body: str | None = field(repr=False)
    state: str
    updated_at: str
    sha256: str


@dataclass(frozen=True, slots=True)
class GitHubIssueCommentSnapshot:
    """Current immutable identity and mutable text for one ordinary issue comment."""

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


class GitHubAuthorizationClient:
    """Recheck user write rights and App installation access for public repositories.

    The user token is obtained from the encrypted session-token store on each call.
    The App private key is used only to mint a short-lived JWT and a repository-scoped,
    read-only installation token. This adapter intentionally exposes no Git write API.
    """

    def __init__(
        self,
        *,
        app_id: int,
        app_private_key: SecretStr,
        user_tokens: GitHubUserTokenStore,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if type(app_id) is not int or app_id <= 0:
            raise ValueError("GitHub App ID must be a positive integer")
        if not app_private_key.get_secret_value().strip():
            raise ValueError("GitHub App private key is required")
        self._app_id = app_id
        self._app_private_key = app_private_key
        self._user_tokens = user_tokens
        self._transport = transport

    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
        try:
            async with asyncio.timeout(AUTHORIZATION_DEADLINE_SECONDS):
                facts = await self._current(repository_id, github_user_id)
                assert isinstance(facts, RepositoryPublicationFacts)
                return facts.access_proof
        except TimeoutError:
            raise RepositoryPreflightError("github_authorization_timeout") from None
        except GitHubAuthorizationError as error:
            raise RepositoryPreflightError(error.code) from None

    async def current(self, repository_id: int, github_user_id: int) -> RepositoryPublicationFacts:
        try:
            async with asyncio.timeout(AUTHORIZATION_DEADLINE_SECONDS):
                result = await self._current(repository_id, github_user_id)
                assert isinstance(result, RepositoryPublicationFacts)
                return result
        except TimeoutError:
            raise ApprovalError("github_authorization_timeout") from None
        except GitHubAuthorizationError as error:
            raise ApprovalError(error.code) from None

    async def publication_authorization(
        self, repository_id: int, github_user_id: int
    ) -> tuple[RepositoryPublicationFacts, SecretStr]:
        """Recheck both principals and mint a repository-scoped contents-write token.

        This narrow capability is reserved for the trusted publication adapter. The
        ordinary ``current`` and ``verify`` paths continue to mint read-only tokens.
        """
        try:
            async with asyncio.timeout(AUTHORIZATION_DEADLINE_SECONDS):
                result = await self._current(
                    repository_id,
                    github_user_id,
                    contents_permission="write",
                    return_installation_token=True,
                )
                assert isinstance(result, tuple)
                return result
        except TimeoutError:
            raise GitHubAuthorizationError("github_authorization_timeout") from None
        except GitHubAuthorizationError:
            raise

    async def repository_read_authorization(
        self, repository_id: int, github_user_id: int
    ) -> tuple[RepositoryPublicationFacts, SecretStr]:
        """Return fresh repository facts and a read-only, repository-scoped token."""
        try:
            async with asyncio.timeout(AUTHORIZATION_DEADLINE_SECONDS):
                result = await self._current(
                    repository_id, github_user_id, return_installation_token=True
                )
                assert isinstance(result, tuple)
                return result
        except TimeoutError:
            raise GitHubAuthorizationError("github_authorization_timeout") from None
        except GitHubAuthorizationError:
            raise

    async def pull_request_authorization(
        self, repository_id: int, github_user_id: int, *, permission: str
    ) -> tuple[RepositoryPublicationFacts, SecretStr]:
        """Mint a repository-scoped token for pull-request reads or creation."""
        if permission not in {"read", "write"}:
            raise GitHubAuthorizationError("github_permission_invalid")
        try:
            async with asyncio.timeout(AUTHORIZATION_DEADLINE_SECONDS):
                result = await self._current(
                    repository_id,
                    github_user_id,
                    return_installation_token=True,
                    pull_requests_permission=permission,
                )
                assert isinstance(result, tuple)
                return result
        except TimeoutError:
            raise GitHubAuthorizationError("github_authorization_timeout") from None
        except GitHubAuthorizationError:
            raise

    async def current_issue(
        self, repository_id: int, github_user_id: int, issue_number: int
    ) -> tuple[RepositoryPublicationFacts, GitHubIssueSnapshot]:
        """Return a fresh public-repository proof and a verified ordinary issue snapshot."""
        if type(issue_number) is not int or not 1 <= issue_number <= 2_147_483_647:
            raise GitHubAuthorizationError("github_issue_number_invalid")
        try:
            async with asyncio.timeout(AUTHORIZATION_DEADLINE_SECONDS):
                authorized = await self._current(
                    repository_id,
                    github_user_id,
                    return_installation_token=True,
                    issues_permission="read",
                )
                if not isinstance(authorized, tuple):
                    raise GitHubAuthorizationError("github_installation_token_unavailable")
                facts, token = authorized
                assert isinstance(facts, RepositoryPublicationFacts)
                assert isinstance(token, SecretStr)
                budget = _RequestBudget()
                async with httpx.AsyncClient(
                    base_url=GITHUB_API_ORIGIN,
                    headers={
                        "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": GITHUB_API_VERSION,
                        "User-Agent": "Continuumm",
                    },
                    timeout=httpx.Timeout(HTTP_TIMEOUT_SECONDS),
                    follow_redirects=False,
                    trust_env=False,
                    transport=self._transport,
                ) as client:
                    path = (
                        f"/repos/{quote(facts.owner, safe='')}/{quote(facts.name, safe='')}"
                        f"/issues/{issue_number}"
                    )
                    payload = await _request_json(
                        client,
                        budget,
                        "GET",
                        path,
                        headers={"Authorization": f"Bearer {token.get_secret_value()}"},
                    )
                if "pull_request" in payload:
                    raise GitHubAuthorizationError("github_issue_is_pull_request")
                issue_id = _positive_id(payload.get("id"), "github_issue_identity_invalid")
                if (
                    _positive_id(payload.get("number"), "github_issue_identity_invalid")
                    != issue_number
                ):
                    raise GitHubAuthorizationError("github_issue_identity_invalid")
                title = payload.get("title")
                body = payload.get("body")
                state = payload.get("state")
                updated_at = payload.get("updated_at")
                if (
                    not isinstance(title, str)
                    or not title.strip()
                    or len(title.encode("utf-8")) > 1024
                    or (body is not None and not isinstance(body, str))
                    or (isinstance(body, str) and len(body.encode("utf-8")) > 16_384)
                    or state not in {"open", "closed"}
                    or not isinstance(updated_at, str)
                    or len(updated_at) > 40
                ):
                    raise GitHubAuthorizationError("github_issue_snapshot_invalid")
                try:
                    updated = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                    if updated.tzinfo is None:
                        raise ValueError
                except ValueError:
                    raise GitHubAuthorizationError("github_issue_snapshot_invalid") from None
                snapshot_value = {
                    "id": issue_id,
                    "number": issue_number,
                    "title": title,
                    "body": body,
                    "state": state,
                    "updated_at": updated.isoformat(),
                }
                digest = hashlib.sha256(
                    json.dumps(
                        snapshot_value, sort_keys=True, ensure_ascii=False, separators=(",", ":")
                    ).encode("utf-8")
                ).hexdigest()
                return facts, GitHubIssueSnapshot(
                    repository_id=repository_id,
                    github_user_id=github_user_id,
                    issue_id=issue_id,
                    issue_number=issue_number,
                    title=title,
                    body=body,
                    state=state,
                    updated_at=updated.isoformat(),
                    sha256=digest,
                )
        except TimeoutError:
            raise GitHubAuthorizationError("github_authorization_timeout") from None

    async def current_issue_comment(
        self,
        *,
        repository_id: int,
        github_user_id: int,
        comment_id: int,
        issue_number: int,
    ) -> GitHubIssueCommentSnapshot:
        """Recheck owner write rights and App issue-read access, then fetch live comment."""
        if type(comment_id) is not int or comment_id <= 0:
            raise GitHubAuthorizationError("github_comment_id_invalid")
        if type(issue_number) is not int or not 1 <= issue_number <= 2_147_483_647:
            raise GitHubAuthorizationError("github_issue_number_invalid")
        try:
            async with asyncio.timeout(AUTHORIZATION_DEADLINE_SECONDS):
                authorized = await self._current(
                    repository_id,
                    github_user_id,
                    return_installation_token=True,
                    issues_permission="read",
                )
                if not isinstance(authorized, tuple):
                    raise GitHubAuthorizationError("github_installation_token_unavailable")
                facts, token = authorized
                assert isinstance(facts, RepositoryPublicationFacts)
                assert isinstance(token, SecretStr)
                budget = _RequestBudget()
                async with httpx.AsyncClient(
                    base_url=GITHUB_API_ORIGIN,
                    headers={
                        "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": GITHUB_API_VERSION,
                        "User-Agent": "Continuumm",
                    },
                    timeout=httpx.Timeout(HTTP_TIMEOUT_SECONDS),
                    follow_redirects=False,
                    trust_env=False,
                    transport=self._transport,
                ) as client:
                    headers = {"Authorization": f"Bearer {token.get_secret_value()}"}
                    prefix = f"/repos/{quote(facts.owner, safe='')}/{quote(facts.name, safe='')}"
                    comment = await _request_json(
                        client,
                        budget,
                        "GET",
                        f"{prefix}/issues/comments/{comment_id}",
                        headers=headers,
                    )
                    issue_url = comment.get("issue_url")
                    expected_issue_path = f"{GITHUB_API_ORIGIN}{prefix}/issues/{issue_number}"
                    if issue_url != expected_issue_path:
                        raise GitHubAuthorizationError("github_comment_identity_invalid")
                    issue = await _request_json(
                        client,
                        budget,
                        "GET",
                        f"{prefix}/issues/{issue_number}",
                        headers=headers,
                    )
                if "pull_request" in issue:
                    raise GitHubAuthorizationError("github_comment_pull_request_rejected")
                comment_user = comment.get("user")
                body = comment.get("body")
                created_value = comment.get("created_at")
                updated_value = comment.get("updated_at")
                if (
                    _positive_id(comment.get("id"), "github_comment_identity_invalid") != comment_id
                    or _positive_id(issue.get("number"), "github_issue_identity_invalid")
                    != issue_number
                    or not isinstance(comment_user, dict)
                    or _positive_id(comment_user.get("id"), "github_comment_author_invalid")
                    != github_user_id
                    or not isinstance(comment_user.get("type"), str)
                    or comment_user.get("type") != "User"
                    or not isinstance(body, str)
                    or len(body.encode("utf-8")) > 16_384
                ):
                    raise GitHubAuthorizationError("github_comment_identity_invalid")
                try:
                    if not isinstance(created_value, str) or not isinstance(updated_value, str):
                        raise ValueError
                    created_at = datetime.fromisoformat(created_value.replace("Z", "+00:00"))
                    updated_at = datetime.fromisoformat(updated_value.replace("Z", "+00:00"))
                    if created_at.tzinfo is None or updated_at.tzinfo is None:
                        raise ValueError
                except ValueError:
                    raise GitHubAuthorizationError("github_comment_timestamp_invalid") from None
                return GitHubIssueCommentSnapshot(
                    repository_id=repository_id,
                    issue_id=_positive_id(issue.get("id"), "github_issue_identity_invalid"),
                    issue_number=issue_number,
                    comment_id=comment_id,
                    author_github_id=github_user_id,
                    author_type=comment_user["type"],
                    created_at=created_at,
                    updated_at=updated_at,
                    body=body,
                )
        except TimeoutError:
            raise GitHubAuthorizationError("github_authorization_timeout") from None

    async def _current(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        contents_permission: str = "read",
        return_installation_token: bool = False,
        pull_requests_permission: str | None = None,
        issues_permission: str | None = None,
    ) -> RepositoryPublicationFacts | tuple[RepositoryPublicationFacts, SecretStr]:
        if type(repository_id) is not int or repository_id <= 0:
            raise GitHubAuthorizationError("repository_id_invalid")
        if type(github_user_id) is not int or github_user_id <= 0:
            raise GitHubAuthorizationError("github_user_id_invalid")
        try:
            user_token = await asyncio.to_thread(self._user_tokens.get_for_user, github_user_id)
        except Exception:
            raise GitHubAuthorizationError("github_user_token_unavailable") from None
        if user_token is None or not user_token.get_secret_value().strip():
            raise GitHubAuthorizationError("github_user_token_unavailable")

        budget = _RequestBudget()
        async with httpx.AsyncClient(
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
        ) as client:
            user_headers = {"Authorization": f"Bearer {user_token.get_secret_value()}"}
            identity = await _request_json(client, budget, "GET", "/user", headers=user_headers)
            if _positive_id(identity.get("id"), "github_identity_invalid") != github_user_id:
                raise GitHubAuthorizationError("github_identity_mismatch")

            user_repo = await _request_json(
                client,
                budget,
                "GET",
                f"/repositories/{repository_id}",
                headers=user_headers,
            )
            self._assert_repository_id(user_repo, repository_id)
            permissions = user_repo.get("permissions")
            actor_can_write = isinstance(permissions, dict) and permissions.get("push") is True
            if not actor_can_write:
                raise GitHubAuthorizationError("repository_actor_write_access_required")
            if user_repo.get("private") is not False:
                raise GitHubAuthorizationError("repository_must_be_public")
            owner, name = _repository_coordinates(user_repo)
            default_branch = user_repo.get("default_branch")
            if (
                not isinstance(default_branch, str)
                or not default_branch
                or len(default_branch) > 255
            ):
                raise GitHubAuthorizationError("repository_metadata_invalid")

            app_jwt = _create_app_jwt(self._app_id, self._app_private_key)
            app_headers = {"Authorization": f"Bearer {app_jwt}"}
            installation = await _request_json(
                client,
                budget,
                "GET",
                f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}/installation",
                headers=app_headers,
            )
            installation_id = _positive_id(
                installation.get("id"), "github_installation_unavailable"
            )

            # This endpoint proves the authenticated human can use this installation;
            # the installation ID alone does not establish the user/repository intersection.
            user_installation_ids = await self._list_user_installation_repositories(
                client, budget, user_headers, installation_id, repository_id
            )
            if repository_id not in user_installation_ids:
                raise GitHubAuthorizationError("github_installation_user_access_required")

            requested_permissions = {"contents": contents_permission, "metadata": "read"}
            if pull_requests_permission is not None:
                requested_permissions["pull_requests"] = pull_requests_permission
            if issues_permission is not None:
                requested_permissions["issues"] = issues_permission
            token_payload = await _request_json(
                client,
                budget,
                "POST",
                f"/app/installations/{installation_id}/access_tokens",
                headers=app_headers,
                json_body={
                    "repository_ids": [repository_id],
                    "permissions": requested_permissions,
                },
            )
            installation_token = token_payload.get("token")
            if not isinstance(installation_token, str) or not installation_token:
                raise GitHubAuthorizationError("github_installation_token_unavailable")
            scoped_repositories = token_payload.get("repositories")
            if (
                token_payload.get("repository_selection") != "selected"
                or not isinstance(scoped_repositories, list)
                or len(scoped_repositories) != 1
                or not isinstance(scoped_repositories[0], dict)
                or type(scoped_repositories[0].get("id")) is not int
                or scoped_repositories[0].get("id") != repository_id
            ):
                raise GitHubAuthorizationError("github_installation_token_repository_scope_invalid")
            token_permissions = token_payload.get("permissions")
            if token_permissions != requested_permissions:
                raise GitHubAuthorizationError("github_installation_token_scope_invalid")
            expires_at = token_payload.get("expires_at")
            try:
                if not isinstance(expires_at, str):
                    raise ValueError
                expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                now = datetime.now(UTC)
                if (
                    expiry.tzinfo is None
                    or expiry <= now
                    or expiry > now + timedelta(hours=1, minutes=5)
                ):
                    raise ValueError
            except (AttributeError, TypeError, ValueError):
                raise GitHubAuthorizationError("github_installation_token_expiry_invalid") from None
            scoped_headers = {"Authorization": f"Bearer {installation_token}"}
            app_repo = await _request_json(
                client,
                budget,
                "GET",
                f"/repositories/{repository_id}",
                headers=scoped_headers,
            )
            self._assert_repository_id(app_repo, repository_id)
            app_full_name = _repository_coordinates(app_repo)
            if app_full_name != (owner, name) or app_repo.get("private") is not False:
                raise GitHubAuthorizationError("github_app_repository_mismatch")

            ref = await _request_json(
                client,
                budget,
                "GET",
                f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}/git/ref/heads/"
                f"{quote(default_branch, safe='')}",
                headers=user_headers,
            )
            object_value = ref.get("object")
            head = object_value.get("sha") if isinstance(object_value, dict) else None
            if not isinstance(head, str) or _SHA_PATTERN.fullmatch(head) is None:
                raise GitHubAuthorizationError("repository_head_invalid")

            proof = RepositoryAccessProof(
                repository_id=repository_id,
                github_user_id=github_user_id,
                actor_can_write=True,
                app_can_access=True,
            )
            facts = RepositoryPublicationFacts(
                repository_id=repository_id,
                github_user_id=github_user_id,
                owner=owner,
                name=name,
                default_branch=default_branch,
                base_commit_sha=head,
                access_proof=proof,
            )
            if return_installation_token:
                return facts, SecretStr(installation_token)
            return facts

    async def _list_user_installation_repositories(
        self,
        client: httpx.AsyncClient,
        budget: _RequestBudget,
        headers: Mapping[str, str],
        installation_id: int,
        expected_repository_id: int,
    ) -> set[int]:
        found: set[int] = set()
        for page in range(1, MAX_PAGES + 1):
            payload = await _request_json(
                client,
                budget,
                "GET",
                f"/user/installations/{installation_id}/repositories",
                headers=headers,
                params={"per_page": PAGE_SIZE, "page": page},
            )
            repositories = payload.get("repositories")
            if not isinstance(repositories, list) or len(repositories) > PAGE_SIZE:
                raise GitHubAuthorizationError("github_installation_response_invalid")
            if len(found) + len(repositories) > MAX_ITEMS:
                raise GitHubAuthorizationError("github_installation_item_limit")
            for repository in repositories:
                if not isinstance(repository, dict):
                    raise GitHubAuthorizationError("github_installation_response_invalid")
                repo_id = repository.get("id")
                if type(repo_id) is int and repo_id > 0:
                    found.add(repo_id)
            if len(repositories) < PAGE_SIZE:
                return found
        raise GitHubAuthorizationError("github_installation_page_limit")

    @staticmethod
    def _assert_repository_id(payload: Mapping[str, Any], expected: int) -> None:
        if _positive_id(payload.get("id"), "repository_metadata_invalid") != expected:
            raise GitHubAuthorizationError("repository_id_mismatch")


class GitHubAccessVerifier(RepositoryAccessVerifier):
    """T13 adapter that returns only the current dual-access proof."""

    def __init__(self, client: GitHubAuthorizationClient) -> None:
        self._client = client

    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
        return await self._client.verify(repository_id=repository_id, github_user_id=github_user_id)


class GitHubPublicationFactsProvider(PublicationFactsProvider):
    """T27 adapter that returns current repository facts after the same access checks."""

    def __init__(self, client: GitHubAuthorizationClient) -> None:
        self._client = client

    async def current(self, repository_id: int, github_user_id: int) -> RepositoryPublicationFacts:
        return await self._client.current(repository_id, github_user_id)

    async def current_issue(
        self, repository_id: int, github_user_id: int, issue_number: int
    ) -> tuple[RepositoryPublicationFacts, GitHubIssueSnapshot]:
        return await self._client.current_issue(repository_id, github_user_id, issue_number)


async def _request_json(
    client: httpx.AsyncClient,
    budget: _RequestBudget,
    method: str,
    path: str,
    *,
    headers: Mapping[str, str],
    params: Mapping[str, str | int] | None = None,
    json_body: Mapping[str, object] | None = None,
) -> dict[str, Any]:
    if not path.startswith("/") or path.startswith("//"):
        raise GitHubAuthorizationError("github_api_path_invalid")
    budget.start()
    try:
        async with client.stream(
            method,
            path,
            headers=dict(headers),
            params=params,
            json=json_body,
        ) as response:
            if 300 <= response.status_code < 400:
                raise GitHubAuthorizationError("github_api_redirect_rejected")
            if response.status_code < 200 or response.status_code >= 300:
                raise GitHubAuthorizationError("github_api_request_failed")
            content_length = response.headers.get("content-length")
            if content_length:
                try:
                    if int(content_length) > MAX_RESPONSE_BYTES:
                        raise GitHubAuthorizationError("github_authorization_response_limit")
                except ValueError:
                    raise GitHubAuthorizationError("github_api_response_invalid") from None
            body = bytearray()
            async for chunk in response.aiter_bytes():
                if len(body) + len(chunk) > MAX_RESPONSE_BYTES:
                    raise GitHubAuthorizationError("github_authorization_response_limit")
                budget.consume(len(chunk))
                body.extend(chunk)
    except GitHubAuthorizationError:
        raise
    except (httpx.HTTPError, OSError, TimeoutError):
        raise GitHubAuthorizationError("github_api_unavailable") from None
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise GitHubAuthorizationError("github_api_response_invalid") from None
    if not isinstance(payload, dict):
        raise GitHubAuthorizationError("github_api_response_invalid")
    return payload


def _create_app_jwt(app_id: int, private_key: SecretStr) -> str:
    """Sign a short-lived GitHub App JWT with the configured RSA private key."""
    try:
        key = serialization.load_pem_private_key(
            private_key.get_secret_value().encode("utf-8"), password=None
        )
        if not isinstance(key, rsa.RSAPrivateKey):
            raise ValueError
        now = int(time.time())
        header = _base64url(json.dumps({"alg": "RS256", "typ": "JWT"}, separators=(",", ":")))
        claims = _base64url(
            json.dumps(
                {"iat": now - 60, "exp": now + 8 * 60, "iss": app_id},
                separators=(",", ":"),
            )
        )
        message = f"{header}.{claims}"
        signature = key.sign(message.encode("ascii"), padding.PKCS1v15(), hashes.SHA256())
        return f"{message}.{_base64url(signature)}"
    except Exception:
        raise GitHubAuthorizationError("github_app_credentials_invalid") from None


def _base64url(value: str | bytes) -> str:
    raw = value.encode("utf-8") if isinstance(value, str) else value
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _positive_id(value: object, code: str) -> int:
    if type(value) is not int or value <= 0:
        raise GitHubAuthorizationError(code)
    return value


def _repository_coordinates(payload: Mapping[str, Any]) -> tuple[str, str]:
    owner_value = payload.get("owner")
    owner = owner_value.get("login") if isinstance(owner_value, dict) else None
    name = payload.get("name")
    if (
        not isinstance(owner, str)
        or _COMPONENT_PATTERN.fullmatch(owner) is None
        or not isinstance(name, str)
        or _COMPONENT_PATTERN.fullmatch(name) is None
    ):
        raise GitHubAuthorizationError("repository_metadata_invalid")
    return owner, name
