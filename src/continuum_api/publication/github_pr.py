"""Bounded, non-retrying GitHub pull-request transport."""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import httpx
from pydantic import SecretStr

from continuum_api.auth.github import GitHubAuthorizationClient, GitHubAuthorizationError
from continuum_api.repositories.preflight import GITHUB_API_ORIGIN, GITHUB_API_VERSION
from continuum_api.runtime.approvals import ApprovalError

MAX_REQUEST_BYTES = 64 * 1024
MAX_PAGE_BYTES = 1024 * 1024
MAX_TOTAL_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_PAGES = 10
PAGE_SIZE = 100
DEADLINE_SECONDS = 20.0
HTTP_TIMEOUT_SECONDS = 6.0
_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_COMPONENT_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
_BRANCH_PATTERN = re.compile(r"^[A-Za-z0-9._/-]{1,255}$")
_NEXT_LINK_PATTERN = re.compile(r'<([^>]+)>\s*;\s*rel="?next"?', re.IGNORECASE)


class GitHubPRTransportError(RuntimeError):
    """Sanitized transport failure; remote bodies and credentials are never exposed."""

    def __init__(self, code: str = "github_pr_transport_failed") -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class GitHubPullRequest:
    """Validated GitHub PR facts used for exact reconciliation."""

    number: int
    id: int
    state: str
    merged: bool
    merged_at: str | None
    head_repository_id: int | None
    head_owner: str | None
    head_ref: str
    head_sha: str
    base_repository_id: int
    base_ref: str
    base_sha: str
    title: str
    body: str | None
    html_url: str


class _Budget:
    def __init__(self) -> None:
        self.requests = 0
        self.response_bytes = 0

    def start(self, request_bytes: int = 0) -> None:
        self.requests += 1
        if self.requests > MAX_PAGES + 3:
            raise GitHubPRTransportError("github_pr_request_limit")
        if request_bytes > MAX_REQUEST_BYTES:
            raise GitHubPRTransportError("github_pr_request_limit")

    def consume(self, count: int) -> None:
        self.response_bytes += count
        if self.response_bytes > MAX_TOTAL_RESPONSE_BYTES:
            raise GitHubPRTransportError("github_pr_response_limit")


@asynccontextmanager
async def _deadline() -> AsyncIterator[None]:
    try:
        async with asyncio.timeout(DEADLINE_SECONDS):
            yield
    except TimeoutError:
        raise GitHubPRTransportError("github_pr_timeout") from None


class GitHubPRTransport:
    """HTTP transport for listing, reading, and creating repository pull requests."""

    def __init__(
        self,
        client: GitHubAuthorizationClient,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._authorization = client
        self._transport = transport

    async def list_pull_requests(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> tuple[GitHubPullRequest, ...]:
        """Read all repository PR states/pages or fail closed on an incomplete scan."""
        self._validate_ids(repository_id, github_user_id)
        self._validate_repository(expected_owner, expected_name)
        async with _deadline():
            facts, token = await self._read_credentials(repository_id, github_user_id)
            self._assert_repository(facts, repository_id, expected_owner, expected_name)
            path = self._repository_path(expected_owner, expected_name) + "/pulls"
            params = {
                "state": "all",
                "per_page": str(PAGE_SIZE),
                "page": "1",
            }
            budget = _Budget()
            result: list[GitHubPullRequest] = []
            async with self._http_client() as client:
                for page in range(1, MAX_PAGES + 1):
                    params["page"] = str(page)
                    payload, headers = await self._request_json(
                        client, budget, "GET", path, token=token, params=params, expect_list=True
                    )
                    assert isinstance(payload, list)
                    if len(payload) > PAGE_SIZE:
                        raise GitHubPRTransportError("github_pr_response_invalid")
                    records = [self._parse_record(item) for item in payload]
                    for record in records:
                        self._assert_record_repository(
                            record, repository_id, expected_owner, expected_name
                        )
                    result.extend(records)
                    has_next = self._has_next_page(
                        headers,
                        owner=expected_owner,
                        name=expected_name,
                        next_page=page + 1,
                    )
                    if not has_next:
                        return tuple(result)
                    if page == MAX_PAGES:
                        raise GitHubPRTransportError("github_pr_list_incomplete")
            raise GitHubPRTransportError("github_pr_list_incomplete")

    async def get_pull_request(
        self,
        repository_id: int,
        github_user_id: int,
        number: int,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> GitHubPullRequest:
        """Fetch the latest authoritative state for a ledger-stored PR number."""
        self._validate_ids(repository_id, github_user_id)
        self._validate_repository(expected_owner, expected_name)
        if type(number) is not int or number <= 0:
            raise GitHubPRTransportError("github_pr_number_invalid")
        async with _deadline():
            facts, token = await self._read_credentials(repository_id, github_user_id)
            self._assert_repository(facts, repository_id, expected_owner, expected_name)
            budget = _Budget()
            async with self._http_client() as client:
                payload, _ = await self._request_json(
                    client,
                    budget,
                    "GET",
                    self._repository_path(expected_owner, expected_name) + f"/pulls/{number}",
                    token=token,
                )
            assert isinstance(payload, dict)
            record = self._parse_record(payload)
            self._assert_record_repository(record, repository_id, expected_owner, expected_name)
            return record

    async def create_pull_request(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        expected_owner: str,
        expected_name: str,
        expected_base_branch: str,
        expected_base_sha: str,
        expected_head_branch: str,
        expected_head_sha: str,
        title: str,
        body: str,
        marker: str,
    ) -> GitHubPullRequest:
        """Create one approved PR after fresh rights, base, and head checks."""
        self._validate_ids(repository_id, github_user_id)
        self._validate_repository(expected_owner, expected_name)
        self._validate_branch(expected_base_branch)
        self._validate_branch(expected_head_branch)
        self._require_sha(expected_base_sha)
        self._require_sha(expected_head_sha)
        if (
            not isinstance(title, str)
            or not title.strip()
            or len(title.encode("utf-8")) > 1024
            or not isinstance(body, str)
            or len(body.encode("utf-8")) > 32 * 1024
            or not isinstance(marker, str)
            or not marker
            or len(marker.encode("utf-8")) > 512
            or marker not in body
        ):
            raise GitHubPRTransportError("github_pr_payload_invalid")
        payload = {
            "title": title,
            "body": body,
            "head": expected_head_branch,
            "base": expected_base_branch,
        }
        request_size = len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        budget = _Budget()
        budget.start(request_size)
        async with _deadline():
            facts, token = await self._write_credentials(repository_id, github_user_id)
            self._assert_repository(facts, repository_id, expected_owner, expected_name)
            if (
                facts.default_branch != expected_base_branch
                or facts.base_commit_sha != expected_base_sha
            ):
                raise GitHubPRTransportError("github_pr_base_changed")
            path = self._repository_path(expected_owner, expected_name)
            async with self._http_client() as client:
                base, _ = await self._request_json(
                    client,
                    budget,
                    "GET",
                    path + "/git/ref/heads/" + quote(expected_base_branch, safe="/"),
                    token=token,
                )
                assert isinstance(base, dict)
                base_object = base.get("object")
                observed_base = base_object.get("sha") if isinstance(base_object, dict) else None
                if (
                    base.get("ref") != f"refs/heads/{expected_base_branch}"
                    or not isinstance(base_object, dict)
                    or base_object.get("type") != "commit"
                    or observed_base != expected_base_sha
                ):
                    raise GitHubPRTransportError("github_pr_base_changed")
                head, _ = await self._request_json(
                    client,
                    budget,
                    "GET",
                    path + "/git/ref/heads/" + quote(expected_head_branch, safe="/"),
                    token=token,
                )
                assert isinstance(head, dict)
                object_value = head.get("object")
                observed_head = object_value.get("sha") if isinstance(object_value, dict) else None
                if (
                    head.get("ref") != f"refs/heads/{expected_head_branch}"
                    or not isinstance(object_value, dict)
                    or object_value.get("type") != "commit"
                    or observed_head != expected_head_sha
                ):
                    raise GitHubPRTransportError("github_pr_head_changed")
                created, _ = await self._request_json(
                    client,
                    budget,
                    "POST",
                    path + "/pulls",
                    token=token,
                    json_body=payload,
                    expected_status=201,
                    request_already_counted=True,
                )
            assert isinstance(created, dict)
            record = self._parse_record(created)
            if (
                record.state != "open"
                or record.merged
                or record.head_repository_id != repository_id
                or not isinstance(record.head_owner, str)
                or record.head_owner.casefold() != expected_owner.casefold()
                or record.head_ref != expected_head_branch
                or record.head_sha != expected_head_sha
                or record.base_ref != expected_base_branch
                or record.base_sha != expected_base_sha
                or record.title != title
                or record.body != body
            ):
                raise GitHubPRTransportError("github_pr_create_response_mismatch")
            return record

    async def _read_credentials(
        self, repository_id: int, github_user_id: int
    ) -> tuple[Any, SecretStr]:
        try:
            return await self._authorization.pull_request_authorization(
                repository_id, github_user_id, permission="read"
            )
        except (ApprovalError, GitHubAuthorizationError) as error:
            raise GitHubPRTransportError(
                getattr(error, "code", "github_authorization_failed")
            ) from None

    async def _write_credentials(
        self, repository_id: int, github_user_id: int
    ) -> tuple[Any, SecretStr]:
        try:
            return await self._authorization.pull_request_authorization(
                repository_id, github_user_id, permission="write"
            )
        except (ApprovalError, GitHubAuthorizationError) as error:
            raise GitHubPRTransportError(
                getattr(error, "code", "github_authorization_failed")
            ) from None

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
        params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        expect_list: bool = False,
        expected_status: int = 200,
        request_already_counted: bool = False,
    ) -> tuple[dict[str, Any] | list[Any], httpx.Headers]:
        if not path.startswith("/") or path.startswith("//"):
            raise GitHubPRTransportError("github_pr_path_invalid")
        if not request_already_counted:
            size = (
                0
                if json_body is None
                else len(json.dumps(json_body, separators=(",", ":")).encode())
            )
            budget.start(size)
        else:
            budget.start()
        try:
            async with client.stream(
                method,
                path,
                params=params,
                headers={"Authorization": f"Bearer {token.get_secret_value()}"},
                json=json_body,
            ) as response:
                if 300 <= response.status_code < 400:
                    raise GitHubPRTransportError("github_pr_redirect_rejected")
                if response.status_code != expected_status:
                    raise GitHubPRTransportError("github_pr_request_failed")
                content_length = response.headers.get("content-length")
                if content_length:
                    try:
                        if int(content_length) > MAX_PAGE_BYTES:
                            raise GitHubPRTransportError("github_pr_response_limit")
                    except ValueError:
                        raise GitHubPRTransportError("github_pr_response_invalid") from None
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > MAX_PAGE_BYTES:
                        raise GitHubPRTransportError("github_pr_response_limit")
                    budget.consume(len(chunk))
                    body.extend(chunk)
                headers = response.headers
        except GitHubPRTransportError:
            raise
        except (httpx.HTTPError, OSError, TimeoutError):
            raise GitHubPRTransportError("github_pr_unavailable") from None
        try:
            result = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise GitHubPRTransportError("github_pr_response_invalid") from None
        if expect_list:
            if not isinstance(result, list):
                raise GitHubPRTransportError("github_pr_response_invalid")
        elif not isinstance(result, dict):
            raise GitHubPRTransportError("github_pr_response_invalid")
        return result, headers

    @classmethod
    def _parse_record(cls, value: Any) -> GitHubPullRequest:
        if not isinstance(value, dict):
            raise GitHubPRTransportError("github_pr_response_invalid")
        head = value.get("head")
        base = value.get("base")
        head_repo = head.get("repo") if isinstance(head, dict) else None
        head_user = head_repo.get("owner") if isinstance(head_repo, dict) else None
        head_owner = head_user.get("login") if isinstance(head_user, dict) else None
        head_repository_id = head_repo.get("id") if isinstance(head_repo, dict) else None
        head_sha = head.get("sha") if isinstance(head, dict) else None
        head_ref = head.get("ref") if isinstance(head, dict) else None
        base_ref = base.get("ref") if isinstance(base, dict) else None
        base_sha = base.get("sha") if isinstance(base, dict) else None
        base_repo = base.get("repo") if isinstance(base, dict) else None
        base_repository_id = base_repo.get("id") if isinstance(base_repo, dict) else None
        state = value.get("state")
        merged_at = value.get("merged_at")
        merged_field = value.get("merged")
        body = value.get("body")
        title = value.get("title")
        html_url = value.get("html_url")
        if (
            type(value.get("number")) is not int
            or value["number"] <= 0
            or type(value.get("id")) is not int
            or value["id"] <= 0
            or state not in {"open", "closed"}
            or (merged_at is not None and not isinstance(merged_at, str))
            or (isinstance(merged_at, str) and not merged_at)
            or (merged_field is not None and type(merged_field) is not bool)
            or (
                head_repository_id is not None
                and (type(head_repository_id) is not int or head_repository_id <= 0)
            )
            or (
                head_repository_id is not None
                and (
                    not isinstance(head_owner, str)
                    or _COMPONENT_PATTERN.fullmatch(head_owner) is None
                )
            )
            or (head_repository_id is None and head_owner is not None)
            or not isinstance(head_ref, str)
            or not cls._valid_remote_ref(head_ref)
            or not isinstance(head_sha, str)
            or _SHA_PATTERN.fullmatch(head_sha) is None
            or not isinstance(base_ref, str)
            or not cls._valid_remote_ref(base_ref)
            or not isinstance(base_sha, str)
            or _SHA_PATTERN.fullmatch(base_sha) is None
            or type(base_repository_id) is not int
            or base_repository_id <= 0
            or not isinstance(title, str)
            or (body is not None and not isinstance(body, str))
            or not isinstance(html_url, str)
            or not html_url.startswith("https://github.com/")
        ):
            raise GitHubPRTransportError("github_pr_response_invalid")
        if merged_at is not None and state != "closed":
            raise GitHubPRTransportError("github_pr_response_invalid")
        if merged_at is not None:
            try:
                parsed_merged_at = datetime.fromisoformat(merged_at.replace("Z", "+00:00"))
                if parsed_merged_at.tzinfo is None:
                    raise ValueError
            except ValueError:
                raise GitHubPRTransportError("github_pr_response_invalid") from None
        if merged_field is not None and merged_field != (merged_at is not None):
            raise GitHubPRTransportError("github_pr_response_invalid")
        return GitHubPullRequest(
            number=value["number"],
            id=value["id"],
            state=state,
            merged=merged_at is not None,
            merged_at=merged_at,
            head_repository_id=head_repository_id,
            head_owner=head_owner,
            head_ref=head_ref,
            head_sha=head_sha,
            base_repository_id=base_repository_id,
            base_ref=base_ref,
            base_sha=base_sha,
            title=title,
            body=body,
            html_url=html_url,
        )

    @staticmethod
    def _has_next_page(
        headers: httpx.Headers,
        *,
        owner: str,
        name: str,
        next_page: int,
    ) -> bool:
        link = headers.get("link", "")
        match = _NEXT_LINK_PATTERN.search(link)
        if match is None:
            return False
        candidate = urlsplit(match.group(1))
        expected_path = f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}/pulls"
        query = parse_qs(candidate.query, keep_blank_values=True)
        expected = {
            "state": ["all"],
            "per_page": [str(PAGE_SIZE)],
            "page": [str(next_page)],
        }
        if (
            candidate.scheme != "https"
            or candidate.netloc != "api.github.com"
            or candidate.path != expected_path
            or query != expected
        ):
            raise GitHubPRTransportError("github_pr_pagination_invalid")
        return True

    @staticmethod
    def _repository_path(owner: str, name: str) -> str:
        return f"/repos/{quote(owner, safe='')}/{quote(name, safe='')}"

    @staticmethod
    def _assert_record_repository(
        record: GitHubPullRequest, repository_id: int, owner: str, name: str
    ) -> None:
        expected_url = f"https://github.com/{owner}/{name}/pull/{record.number}"
        if (
            record.base_repository_id != repository_id
            or record.html_url.casefold() != expected_url.casefold()
        ):
            raise GitHubPRTransportError("github_pr_repository_mismatch")

    @staticmethod
    def _assert_repository(facts: Any, repository_id: int, owner: str, name: str) -> None:
        if (
            facts.repository_id != repository_id
            or facts.owner.casefold() != owner.casefold()
            or facts.name.casefold() != name.casefold()
        ):
            raise GitHubPRTransportError("github_pr_repository_mismatch")

    @staticmethod
    def _validate_ids(repository_id: int, github_user_id: int) -> None:
        if type(repository_id) is not int or repository_id <= 0:
            raise GitHubPRTransportError("repository_id_invalid")
        if type(github_user_id) is not int or github_user_id <= 0:
            raise GitHubPRTransportError("github_user_id_invalid")

    @staticmethod
    def _validate_component(value: str) -> None:
        if not isinstance(value, str) or _COMPONENT_PATTERN.fullmatch(value) is None:
            raise GitHubPRTransportError("github_pr_repository_invalid")

    @classmethod
    def _validate_repository(cls, owner: str, name: str) -> None:
        cls._validate_component(owner)
        cls._validate_component(name)

    @staticmethod
    def _validate_branch(value: str) -> None:
        if (
            not isinstance(value, str)
            or _BRANCH_PATTERN.fullmatch(value) is None
            or value.startswith("/")
            or value.endswith("/")
            or ".." in value
            or "//" in value
            or value.endswith(".")
        ):
            raise GitHubPRTransportError("github_pr_branch_invalid")

    @staticmethod
    def _valid_remote_ref(value: Any) -> bool:
        """Accept valid historical Git refs, including non-ASCII branch names."""
        if not isinstance(value, str) or not value or len(value) > 255:
            return False
        forbidden = set(" ~^:?*[\\")
        return not (
            value.startswith("/")
            or value.endswith(("/", ".", ".lock"))
            or "//" in value
            or ".." in value
            or "@{" in value
            or any(char in forbidden or ord(char) < 32 or ord(char) == 127 for char in value)
        )

    @staticmethod
    def _require_sha(value: str) -> None:
        if not isinstance(value, str) or _SHA_PATTERN.fullmatch(value) is None:
            raise GitHubPRTransportError("github_pr_sha_invalid")
