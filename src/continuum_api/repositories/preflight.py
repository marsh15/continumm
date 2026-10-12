"""Fail-closed, metadata-only repository eligibility checks."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import re
import time
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Literal, Protocol
from urllib.parse import quote, urlsplit
from uuid import UUID

import httpx
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import Version
from pydantic import SecretStr

from continuum_api.runtime.persistence import Principal

MAX_REPOSITORY_BYTES = 50 * 1024 * 1024
MAX_REPOSITORY_FILES = 5_000
MAX_MANIFEST_BYTES = 256 * 1024
MAX_PACKAGE_METADATA_REQUESTS = 24
MAX_HTTP_REQUESTS = 32
MAX_TOTAL_RESPONSE_BYTES = 2 * 1024 * 1024
REQUEST_TIMEOUT_SECONDS = 8.0
CHECK_DEADLINE_SECONDS = 30.0
RELEASE_DEADLINE_SECONDS = 5.0
MAX_TREE_ENTRIES = MAX_REPOSITORY_FILES + 1_000
PROFILE_VERSION = "1"
# Python profile v1 is checked against a fixed target; T17 must use this interpreter
# patch and Linux x86_64 environment or introduce a new profile version.
PYTHON_TARGET_VERSION = "3.13.12"
GITHUB_API_ORIGIN = "https://api.github.com"
PYPI_API_ORIGIN = "https://pypi.org"
NPM_REGISTRY_ORIGIN = "https://registry.npmjs.org"
GITHUB_API_VERSION = "2026-03-10"

_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_PYTHON_REQUIREMENT_PATTERN = re.compile(
    r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)=="
    r"(?P<version>[A-Za-z0-9][A-Za-z0-9.!+_-]{0,127})"
    r"(?P<hashes>(?:\s+--hash=sha256:[0-9a-f]{64})+)$"
)
_NPM_PACKAGE_PATTERN = re.compile(r"^(?:@[a-z0-9._~-]+/)?[a-z0-9._~-]+$")
_PACKAGE_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_-]{0,127}$")
_UNSUPPORTED_NPM_SPEC_PREFIXES = (
    "file:",
    "workspace:",
    "link:",
    "git+",
    "git://",
    "git@",
    "github:",
    "http://",
    "https://",
    "npm:",
)
_LFS_POINTER = b"version https://git-lfs.github.com/spec/v1"
_INSTALL_LIFECYCLE_SCRIPTS = frozenset({"preinstall", "install", "postinstall", "prepare"})
_GITHUB_COMPONENT_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
_NATIVE_SOURCE_SUFFIXES = (
    ".c",
    ".h",
    ".cpp",
    ".cc",
    ".cxx",
    ".hpp",
    ".hh",
    ".pyx",
    ".so",
    ".pyd",
    ".dll",
    ".dylib",
    ".a",
    ".o",
    ".obj",
    ".node",
)
_NATIVE_BUILD_FILES = frozenset({"binding.gyp", "cmakelists.txt"})


class _ReadBudget:
    """One serial request and response-byte budget shared across fixed hosts."""

    def __init__(self, *, deadline: float) -> None:
        self.deadline = deadline
        self.requests = 0
        self.response_bytes = 0

    def start_request(self) -> None:
        if time.monotonic() >= self.deadline:
            raise RepositoryPreflightError("repository_preflight_timeout")
        if self.requests >= MAX_HTTP_REQUESTS:
            raise RepositoryPreflightError("repository_request_limit")
        self.requests += 1

    def remaining_bytes(self) -> int:
        return MAX_TOTAL_RESPONSE_BYTES - self.response_bytes

    def consume(self, chunk_size: int) -> None:
        self.response_bytes += chunk_size
        if self.response_bytes > MAX_TOTAL_RESPONSE_BYTES:
            raise RepositoryPreflightError("repository_metadata_byte_limit")

    def require_capacity(self, request_count: int) -> None:
        if request_count < 0 or self.requests + request_count > MAX_HTTP_REQUESTS:
            raise RepositoryPreflightError("repository_request_limit")


@dataclass(frozen=True, slots=True)
class RepositoryMetadata:
    repository_id: int
    owner: str
    name: str
    size_kib: int


class _BoundedMetadataReader:
    """Read small JSON documents from fixed public metadata origins only."""

    def __init__(
        self,
        *,
        budget: _ReadBudget,
        github_transport: httpx.AsyncBaseTransport | None = None,
        pypi_transport: httpx.AsyncBaseTransport | None = None,
        npm_transport: httpx.AsyncBaseTransport | None = None,
        github_token: SecretStr | None = None,
    ) -> None:
        self._budget = budget
        self._github_token = github_token
        transports = {
            "github": github_transport,
            "pypi": pypi_transport,
            "npm": npm_transport,
        }
        self._clients = {
            origin: httpx.AsyncClient(
                transport=transport,
                timeout=httpx.Timeout(connect=3, read=5, write=2, pool=2),
                follow_redirects=False,
                trust_env=False,
                headers={},
                cookies={},
                auth=None,
            )
            for origin, transport in transports.items()
        }

    async def aclose(self) -> None:
        for client in self._clients.values():
            await client.aclose()

    async def github_json(self, path: str) -> dict[str, Any]:
        headers = {
            "accept": "application/vnd.github+json",
            "accept-encoding": "identity",
            "x-github-api-version": GITHUB_API_VERSION,
            "user-agent": "Continuum-Repository-Preflight",
        }
        if self._github_token is not None:
            headers["authorization"] = f"Bearer {self._github_token.get_secret_value()}"
        return await self._get_json("github", GITHUB_API_ORIGIN + path, headers)

    async def pypi_json(self, path: str) -> dict[str, Any]:
        return await self._get_json(
            "pypi",
            PYPI_API_ORIGIN + path,
            {
                "accept": "application/json",
                "accept-encoding": "identity",
                "user-agent": "Continuum-Repository-Preflight",
            },
        )

    async def npm_json(self, path: str) -> dict[str, Any]:
        return await self._get_json(
            "npm",
            NPM_REGISTRY_ORIGIN + path,
            {
                "accept": "application/json",
                "accept-encoding": "identity",
                "user-agent": "Continuum-Repository-Preflight",
            },
        )

    async def _get_json(
        self,
        origin: Literal["github", "pypi", "npm"],
        url: str,
        headers: Mapping[str, str],
    ) -> dict[str, Any]:
        self._budget.start_request()
        parsed_url = urlsplit(url)
        expected_host = {
            "github": "api.github.com",
            "pypi": "pypi.org",
            "npm": "registry.npmjs.org",
        }[origin]
        if (
            parsed_url.scheme != "https"
            or parsed_url.hostname != expected_host
            or parsed_url.port not in {None, 443}
            or parsed_url.username is not None
            or parsed_url.password is not None
            or parsed_url.fragment
        ):
            raise RepositoryPreflightError("repository_api_origin_invalid")

        remaining = self._budget.deadline - time.monotonic()
        if remaining <= 0:
            raise RepositoryPreflightError("repository_preflight_timeout")
        body = bytearray()
        try:
            # HTTPX read timeouts bound inactivity only; this absolute deadline bounds
            # the whole response, including a server that drips bytes indefinitely.
            async with asyncio.timeout(remaining):
                async with self._clients[origin].stream(
                    "GET",
                    url,
                    headers=dict(headers),
                    follow_redirects=False,
                    timeout=httpx.Timeout(connect=3, read=5, write=2, pool=2),
                ) as response:
                    if response.status_code in {403, 429}:
                        raise RepositoryPreflightError("repository_read_rate_limited")
                    if 300 <= response.status_code < 400:
                        raise RepositoryPreflightError("repository_redirect_unsupported")
                    if response.status_code == 404:
                        raise RepositoryPreflightError("repository_not_found_or_inaccessible")
                    if response.status_code != 200:
                        raise RepositoryPreflightError("repository_read_failed")
                    response_host = response.request.url.host
                    if response_host != expected_host:
                        raise RepositoryPreflightError("repository_api_origin_invalid")
                    content_encoding = response.headers.get("content-encoding")
                    if (
                        content_encoding is not None
                        and content_encoding.strip().lower() != "identity"
                    ):
                        # HTTPX's aiter_bytes() automatically decodes content encodings;
                        # require identity before streaming to keep the byte cap effective.
                        # Source: https://www.python-httpx.org/async/#streaming-responses
                        raise RepositoryPreflightError("repository_response_encoding_unsupported")
                    content_length = response.headers.get("content-length")
                    if content_length is not None:
                        try:
                            advertised_bytes = int(content_length)
                        except ValueError:
                            raise RepositoryPreflightError("repository_response_invalid") from None
                        if (
                            advertised_bytes < 0
                            or advertised_bytes > self._budget.remaining_bytes()
                        ):
                            raise RepositoryPreflightError("repository_metadata_byte_limit")
                    async for chunk in response.aiter_bytes():
                        self._budget.consume(len(chunk))
                        body.extend(chunk)
        except RepositoryPreflightError:
            raise
        except (TimeoutError, httpx.TimeoutException):
            raise RepositoryPreflightError("repository_preflight_timeout") from None
        except httpx.RequestError:
            raise RepositoryPreflightError("repository_read_transport") from None

        try:
            document = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
            raise RepositoryPreflightError("repository_response_invalid") from None
        if not isinstance(document, dict):
            raise RepositoryPreflightError("repository_response_invalid")
        return document


class GitHubRepositoryAdapter:
    """Read a public repository at one full commit SHA without cloning or executing it."""

    def __init__(
        self,
        *,
        github_transport: httpx.AsyncBaseTransport | None = None,
        pypi_transport: httpx.AsyncBaseTransport | None = None,
        npm_transport: httpx.AsyncBaseTransport | None = None,
        github_token: SecretStr | None = None,
    ) -> None:
        self._budget = _ReadBudget(deadline=time.monotonic() + CHECK_DEADLINE_SECONDS)
        self._reader = _BoundedMetadataReader(
            budget=self._budget,
            github_transport=github_transport,
            pypi_transport=pypi_transport,
            npm_transport=npm_transport,
            github_token=github_token,
        )

    async def aclose(self) -> None:
        await self._reader.aclose()

    async def current_public_repository(self, repository_id: int) -> RepositoryMetadata:
        if not isinstance(repository_id, int) or repository_id <= 0:
            raise RepositoryPreflightError("repository_id_invalid")
        response = await self._reader.github_json(f"/repositories/{repository_id}")
        owner = response.get("owner")
        repository_name = response.get("name")
        repository_size = response.get("size")
        if (
            type(response.get("id")) is not int
            or response.get("id") != repository_id
            or response.get("private") is not False
            or response.get("visibility") != "public"
            or response.get("archived") is not False
            or not isinstance(owner, dict)
            or not isinstance(owner.get("login"), str)
            or _GITHUB_COMPONENT_PATTERN.fullmatch(owner["login"]) is None
            or not isinstance(repository_name, str)
            or _GITHUB_COMPONENT_PATTERN.fullmatch(repository_name) is None
            or type(repository_size) is not int
            or repository_size < 0
        ):
            if response.get("private") is True or response.get("visibility") != "public":
                raise RepositoryPreflightError("repository_not_public")
            if response.get("archived") is True:
                raise RepositoryPreflightError("repository_archived")
            raise RepositoryPreflightError("repository_metadata_invalid")
        if response.get("fork") is True:
            raise RepositoryPreflightError("repository_fork_unsupported")
        return RepositoryMetadata(
            repository_id=repository_id,
            owner=owner["login"],
            name=repository_name,
            size_kib=repository_size,
        )

    async def inspect_commit(
        self,
        repository: RepositoryMetadata,
        commit_sha: str,
        profile: BuildProfile,
    ) -> EligibilityReceipt:
        validate_commit_sha(commit_sha)
        owner = quote(repository.owner, safe="")
        name = quote(repository.name, safe="")
        commit = await self._reader.github_json(
            f"/repos/{owner}/{name}/commits/{quote(commit_sha, safe='')}"
        )
        if commit.get("sha") != commit_sha:
            raise RepositoryPreflightError("repository_commit_mismatch")
        commit_data = commit.get("commit")
        tree_data = commit_data.get("tree") if isinstance(commit_data, dict) else None
        tree_sha = tree_data.get("sha") if isinstance(tree_data, dict) else None
        if not isinstance(tree_sha, str) or _SHA_PATTERN.fullmatch(tree_sha) is None:
            raise RepositoryPreflightError("repository_commit_invalid")

        # GitHub documents modes 120000 (symlink) and 160000 (submodule) and
        # marks incomplete recursive trees with `truncated=true`; reject both.
        # Source: https://docs.github.com/en/rest/git/trees#get-a-tree
        tree = await self._reader.github_json(
            f"/repos/{owner}/{name}/git/trees/{quote(tree_sha, safe='')}?recursive=1"
        )
        if tree.get("sha") != tree_sha:
            raise RepositoryPreflightError("repository_tree_mismatch")
        if tree.get("truncated") is not False:
            raise RepositoryPreflightError("repository_tree_truncated")
        raw_entries = tree.get("tree")
        if not isinstance(raw_entries, list) or len(raw_entries) > MAX_TREE_ENTRIES:
            raise RepositoryPreflightError("repository_tree_invalid")
        entries, file_count, source_bytes = _parse_tree_entries(raw_entries)
        if file_count > MAX_REPOSITORY_FILES:
            raise RepositoryPreflightError("repository_file_limit")
        if source_bytes > MAX_REPOSITORY_BYTES:
            raise RepositoryPreflightError("repository_source_size_limit")

        manifest_paths = _selected_manifest_paths(profile, entries)
        self._budget.require_capacity(len(manifest_paths) + 1)
        manifests: dict[str, bytes] = {}
        for path in sorted(manifest_paths):
            entry = entries[path]
            manifests[path] = await self._read_blob(repository, entry)
        _reject_lfs_config_and_attributes(manifests)

        if profile.kind == "python":
            python_packages = _parse_python_lock(manifests["requirements.lock"])
            _validate_python_layout(entries, manifests)
            self._budget.require_capacity(len(python_packages))
            active_dependencies: list[RequiredPythonPackage] = []
            for python_package in python_packages:
                active_dependencies.extend(
                    await _verify_python_wheel_metadata(self._reader, python_package)
                )
            _validate_python_dependency_closure(python_packages, active_dependencies)
            package_count = len(python_packages)
        else:
            node_packages = _parse_node_manifest(
                manifests["package.json"], manifests["package-lock.json"]
            )
            self._budget.require_capacity(len(node_packages))
            for node_package in node_packages:
                await _verify_npm_metadata(self._reader, node_package)
            package_count = len(node_packages)

        return EligibilityReceipt(
            repository_id=repository.repository_id,
            commit_sha=commit_sha,
            tree_sha=tree_sha,
            profile_id=profile.profile_id,
            profile_version=profile.version,
            file_count=file_count,
            source_bytes=source_bytes,
            manifest_blobs=tuple((path, entries[path]["sha"]) for path in sorted(manifest_paths)),
            package_count=package_count,
        )

    async def _read_blob(self, repository: RepositoryMetadata, entry: dict[str, Any]) -> bytes:
        if entry["size"] > MAX_MANIFEST_BYTES:
            raise RepositoryPreflightError("repository_manifest_too_large")
        owner = quote(repository.owner, safe="")
        name = quote(repository.name, safe="")
        document = await self._reader.github_json(
            f"/repos/{owner}/{name}/git/blobs/{quote(entry['sha'], safe='')}"
        )
        if document.get("sha") != entry["sha"] or document.get("encoding") != "base64":
            raise RepositoryPreflightError("repository_manifest_invalid")
        content = document.get("content")
        if not isinstance(content, str):
            raise RepositoryPreflightError("repository_manifest_invalid")
        try:
            decoded = base64.b64decode("".join(content.split()), validate=True)
        except (ValueError, binascii.Error):
            raise RepositoryPreflightError("repository_manifest_invalid") from None
        if len(decoded) != entry["size"] or document.get("size") != entry["size"]:
            raise RepositoryPreflightError("repository_manifest_mismatch")
        if decoded.startswith(_LFS_POINTER):
            raise RepositoryPreflightError("repository_lfs_unsupported")
        return decoded


def _parse_tree_entries(raw_entries: list[object]) -> tuple[dict[str, dict[str, Any]], int, int]:
    entries: dict[str, dict[str, Any]] = {}
    file_count = 0
    source_bytes = 0
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict):
            raise RepositoryPreflightError("repository_tree_invalid")
        path = raw_entry.get("path")
        mode = raw_entry.get("mode")
        object_type = raw_entry.get("type")
        if not isinstance(path, str) or not _valid_tree_path(path):
            raise RepositoryPreflightError("repository_tree_invalid")
        if path in entries:
            raise RepositoryPreflightError("repository_tree_duplicate_path")
        if mode == "120000" or object_type == "symlink":
            raise RepositoryPreflightError("repository_symlink_unsupported")
        if mode == "160000" or object_type == "commit":
            raise RepositoryPreflightError("repository_gitlink_unsupported")
        if object_type == "tree" and mode == "040000":
            entries[path] = {"path": path, "mode": mode, "type": object_type}
            continue
        if object_type != "blob" or mode not in {"100644", "100755"}:
            raise RepositoryPreflightError("repository_tree_mode_unsupported")
        size = raw_entry.get("size")
        sha = raw_entry.get("sha")
        if (
            type(size) is not int
            or size < 0
            or not isinstance(sha, str)
            or _SHA_PATTERN.fullmatch(sha) is None
        ):
            raise RepositoryPreflightError("repository_tree_invalid")
        file_count += 1
        source_bytes += size
        entries[path] = {
            "path": path,
            "mode": mode,
            "type": object_type,
            "size": size,
            "sha": sha,
        }
    return entries, file_count, source_bytes


def _valid_tree_path(path: str) -> bool:
    if not path or path.startswith("/") or "\\" in path or "\x00" in path:
        return False
    return all(component not in {"", ".", ".."} for component in path.split("/"))


def _selected_manifest_paths(
    profile: BuildProfile,
    entries: dict[str, dict[str, Any]],
) -> set[str]:
    blob_paths = {path for path, entry in entries.items() if entry["type"] == "blob"}
    _reject_native_tree_inputs(blob_paths, profile)
    lfs_config_paths = {path for path in blob_paths if path.rsplit("/", 1)[-1] == ".lfsconfig"}
    if lfs_config_paths:
        raise RepositoryPreflightError("repository_lfs_unsupported")
    attributes_paths = {path for path in blob_paths if path.rsplit("/", 1)[-1] == ".gitattributes"}
    if profile.kind == "python":
        if "requirements.lock" not in blob_paths:
            raise RepositoryPreflightError("repository_python_lock_missing")
        selected = {"requirements.lock"}
        if "pyproject.toml" in blob_paths:
            selected.add("pyproject.toml")
    else:
        # npm reads project/user config that can redirect package resolution or
        # attach credentials; lockfile precedence also permits shrinkwrap input.
        # Source: https://docs.npmjs.com/files/npmrc/
        if any(path.rsplit("/", 1)[-1] == ".npmrc" for path in blob_paths):
            raise RepositoryPreflightError("repository_package_source_unsupported")
        if "npm-shrinkwrap.json" in blob_paths:
            raise RepositoryPreflightError("repository_node_lock_unsupported")
        selected = {"package.json", "package-lock.json"}
        if not selected <= blob_paths:
            raise RepositoryPreflightError("repository_node_manifest_missing")
    return selected | attributes_paths


def _reject_native_tree_inputs(blob_paths: set[str], profile: BuildProfile) -> None:
    if any(
        path.rsplit("/", 1)[-1].lower() in _NATIVE_BUILD_FILES
        or path.lower().endswith(_NATIVE_SOURCE_SUFFIXES)
        for path in blob_paths
    ):
        failure_code = (
            "repository_python_native_source_unsupported"
            if profile.kind == "python"
            else "repository_node_native_unsupported"
        )
        raise RepositoryPreflightError(failure_code)


def _reject_lfs_config_and_attributes(manifests: dict[str, bytes]) -> None:
    for path, content in manifests.items():
        if path.rsplit("/", 1)[-1] == ".gitattributes":
            try:
                attributes = content.decode("utf-8")
            except UnicodeDecodeError:
                raise RepositoryPreflightError("repository_manifest_invalid") from None
            if re.search(r"(?:^|\s)(?:filter|diff|merge)\s*=\s*lfs(?:\s|$)", attributes, re.I):
                raise RepositoryPreflightError("repository_lfs_unsupported")


class RepositoryPreflightError(RuntimeError):
    """A stable, redacted eligibility or preflight failure code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class BuildProfile:
    """A fixed, versioned repository setup contract."""

    profile_id: str
    version: str
    kind: Literal["python", "node"]
    manifest_paths: tuple[str, ...]
    package_host: str


PYTHON_PYTEST_PROFILE = BuildProfile(
    profile_id="python-pytest",
    version=PROFILE_VERSION,
    kind="python",
    manifest_paths=("requirements.lock", "pyproject.toml"),
    package_host="pypi.org",
)
NODE_NPM_PROFILE = BuildProfile(
    profile_id="node-npm",
    version=PROFILE_VERSION,
    kind="node",
    manifest_paths=("package.json", "package-lock.json"),
    package_host="registry.npmjs.org",
)
BUILD_PROFILES = {
    PYTHON_PYTEST_PROFILE.profile_id: PYTHON_PYTEST_PROFILE,
    NODE_NPM_PROFILE.profile_id: NODE_NPM_PROFILE,
}


@dataclass(frozen=True, slots=True)
class LockedPythonPackage:
    name: str
    version: str
    sha256_hashes: frozenset[str]


@dataclass(frozen=True, slots=True)
class RequiredPythonPackage:
    name: str
    specifier: str


@dataclass(frozen=True, slots=True)
class LockedNodePackage:
    name: str
    version: str
    resolved_url: str
    integrity: str


@dataclass(frozen=True, slots=True)
class EligibilityReceipt:
    """Bounded metadata safe to cache for one immutable repository/profile key."""

    repository_id: int
    commit_sha: str
    tree_sha: str
    profile_id: str
    profile_version: str
    file_count: int
    source_bytes: int
    manifest_blobs: tuple[tuple[str, str], ...]
    package_count: int
    cache_hit: bool = False

    def to_cache_payload(self) -> dict[str, Any]:
        """Serialize only small, non-secret decision metadata."""
        return {
            "repository_id": self.repository_id,
            "commit_sha": self.commit_sha,
            "tree_sha": self.tree_sha,
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "file_count": self.file_count,
            "source_bytes": self.source_bytes,
            "manifest_blobs": [list(item) for item in self.manifest_blobs],
            "package_count": self.package_count,
        }

    @classmethod
    def from_cache_payload(cls, payload: object) -> EligibilityReceipt:
        """Validate the database cache shape before trusting its decision."""
        if not isinstance(payload, dict):
            raise RepositoryPreflightError("repository_preflight_cache_corrupt")
        manifest_blobs = payload.get("manifest_blobs")
        if not isinstance(manifest_blobs, list) or any(
            not isinstance(item, list)
            or len(item) != 2
            or not isinstance(item[0], str)
            or len(item[0]) > 256
            or not isinstance(item[1], str)
            or _SHA_PATTERN.fullmatch(item[1]) is None
            for item in manifest_blobs
        ):
            raise RepositoryPreflightError("repository_preflight_cache_corrupt")
        repository_id = payload.get("repository_id")
        commit_sha = payload.get("commit_sha")
        tree_sha = payload.get("tree_sha")
        profile_id = payload.get("profile_id")
        profile_version = payload.get("profile_version")
        file_count = payload.get("file_count")
        source_bytes = payload.get("source_bytes")
        package_count = payload.get("package_count")
        if (
            type(repository_id) is not int
            or repository_id <= 0
            or not isinstance(commit_sha, str)
            or _SHA_PATTERN.fullmatch(commit_sha) is None
            or not isinstance(tree_sha, str)
            or _SHA_PATTERN.fullmatch(tree_sha) is None
            or not isinstance(profile_id, str)
            or not isinstance(profile_version, str)
            or type(file_count) is not int
            or not 0 <= file_count <= MAX_REPOSITORY_FILES
            or type(source_bytes) is not int
            or not 0 <= source_bytes <= MAX_REPOSITORY_BYTES
            or type(package_count) is not int
            or not 0 <= package_count <= MAX_PACKAGE_METADATA_REQUESTS
        ):
            raise RepositoryPreflightError("repository_preflight_cache_corrupt")
        return cls(
            repository_id=repository_id,
            commit_sha=commit_sha,
            tree_sha=tree_sha,
            profile_id=profile_id,
            profile_version=profile_version,
            file_count=file_count,
            source_bytes=source_bytes,
            manifest_blobs=tuple((item[0], item[1]) for item in manifest_blobs),
            package_count=package_count,
            cache_hit=True,
        )


@dataclass(frozen=True, slots=True)
class PreflightReservation:
    check_id: UUID
    github_user_id: int
    deadline_at: datetime


class RepositoryPreflightState(Protocol):
    """Principal-scoped durable state provided by the runtime persistence layer."""

    def reserve(
        self,
        principal: Principal,
        repository_id: int,
        commit_sha: str,
        profile_id: str,
        profile_version: str,
    ) -> PreflightReservation: ...

    def get_cached(
        self,
        principal: Principal,
        check_id: UUID,
        repository_id: int,
        commit_sha: str,
        profile_id: str,
        profile_version: str,
    ) -> EligibilityReceipt | None: ...

    def put_cached(
        self,
        principal: Principal,
        check_id: UUID,
        receipt: EligibilityReceipt,
    ) -> None: ...

    def release(
        self,
        principal: Principal,
        check_id: UUID,
        outcome: Literal["completed", "failed"],
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class RepositoryAccessProof:
    """Server-derived proof of both actor write access and app installation access."""

    repository_id: int
    github_user_id: int
    actor_can_write: bool
    app_can_access: bool


class RepositoryAccessVerifier(Protocol):
    """Trusted server boundary; it must verify current actor and App access."""

    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof: ...


class PendingRepositoryAccessVerifier:
    """Fail closed until the server-side GitHub App verifier is implemented."""

    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
        del repository_id, github_user_id
        raise RepositoryPreflightError("repository_authorization_pending")


class RepositoryPreflightService:
    """Reserve account quota, revalidate access, then inspect or use a cache hit."""

    def __init__(
        self,
        state: RepositoryPreflightState,
        *,
        access_verifier: RepositoryAccessVerifier | None = None,
        adapter_factory: Callable[[], GitHubRepositoryAdapter] = GitHubRepositoryAdapter,
    ) -> None:
        self._state = state
        self._access_verifier = access_verifier or PendingRepositoryAccessVerifier()
        self._adapter_factory = adapter_factory

    async def check(
        self,
        principal: Principal,
        *,
        repository_id: int,
        commit_sha: str,
        profile_id: str,
        profile_version: str,
    ) -> EligibilityReceipt:
        if (
            not isinstance(repository_id, int)
            or isinstance(repository_id, bool)
            or repository_id <= 0
        ):
            raise RepositoryPreflightError("repository_id_invalid")
        validate_commit_sha(commit_sha)
        profile = resolve_build_profile(profile_id, profile_version)
        reservation: PreflightReservation | None = None
        outcome: Literal["completed", "failed"] = "failed"
        primary_error: BaseException | None = None
        try:
            async with asyncio.timeout(CHECK_DEADLINE_SECONDS):
                reservation = await asyncio.to_thread(
                    self._state.reserve,
                    principal,
                    repository_id,
                    commit_sha,
                    profile.profile_id,
                    profile.version,
                )
                result = await self._run_check(
                    principal,
                    reservation,
                    repository_id=repository_id,
                    commit_sha=commit_sha,
                    profile=profile,
                )
        except TimeoutError:
            timeout_error = RepositoryPreflightError("repository_preflight_timeout")
            primary_error = timeout_error
            raise timeout_error from None
        except BaseException as error:
            primary_error = error
            raise
        else:
            outcome = "completed"
            return result
        finally:
            if reservation is not None:
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(
                            self._state.release,
                            principal,
                            reservation.check_id,
                            outcome,
                        ),
                        timeout=RELEASE_DEADLINE_SECONDS,
                    )
                except asyncio.CancelledError:
                    if primary_error is None:
                        raise
                except Exception:
                    if primary_error is None:
                        raise RepositoryPreflightError(
                            "repository_preflight_state_unavailable"
                        ) from None

    async def _run_check(
        self,
        principal: Principal,
        reservation: PreflightReservation,
        *,
        repository_id: int,
        commit_sha: str,
        profile: BuildProfile,
    ) -> EligibilityReceipt:
        adapter = self._adapter_factory()
        try:
            try:
                proof = await self._access_verifier.verify(
                    repository_id=repository_id,
                    github_user_id=reservation.github_user_id,
                )
            except RepositoryPreflightError:
                raise
            except Exception:
                raise RepositoryPreflightError("repository_authorization_unavailable") from None
            if (
                not isinstance(proof, RepositoryAccessProof)
                or proof.repository_id != repository_id
                or proof.github_user_id != reservation.github_user_id
                or proof.actor_can_write is not True
                or proof.app_can_access is not True
            ):
                raise RepositoryPreflightError("repository_access_denied")

            # Visibility and access are checked before every cache hit. Cache keys
            # reuse only technical metadata; they do not cache authorization.
            repository = await adapter.current_public_repository(repository_id)
            cached = await asyncio.to_thread(
                self._state.get_cached,
                principal,
                reservation.check_id,
                repository_id,
                commit_sha,
                profile.profile_id,
                profile.version,
            )
            if cached is not None:
                if (
                    cached.repository_id != repository_id
                    or cached.commit_sha != commit_sha
                    or cached.profile_id != profile.profile_id
                    or cached.profile_version != profile.version
                ):
                    raise RepositoryPreflightError("repository_preflight_cache_corrupt")
                return replace(cached, cache_hit=True)

            result = await adapter.inspect_commit(repository, commit_sha, profile)
            await asyncio.to_thread(self._state.put_cached, principal, reservation.check_id, result)
            return result
        finally:
            await adapter.aclose()


def resolve_build_profile(profile_id: str, version: str) -> BuildProfile:
    profile = BUILD_PROFILES.get(profile_id)
    if profile is None or profile.version != version:
        raise RepositoryPreflightError("repository_profile_unsupported")
    return profile


def validate_commit_sha(commit_sha: str) -> str:
    if _SHA_PATTERN.fullmatch(commit_sha) is None:
        raise RepositoryPreflightError("repository_commit_invalid")
    return commit_sha


def _normalized_python_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _parse_python_lock(content: bytes) -> tuple[LockedPythonPackage, ...]:
    if len(content) > MAX_MANIFEST_BYTES:
        raise RepositoryPreflightError("repository_manifest_too_large")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        raise RepositoryPreflightError("repository_manifest_invalid") from None

    packages: list[LockedPythonPackage] = []
    seen_names: set[str] = set()
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _PYTHON_REQUIREMENT_PATTERN.fullmatch(line)
        if match is None:
            raise RepositoryPreflightError("repository_python_lock_unsupported")
        name = _normalized_python_name(match.group("name"))
        try:
            Version(match.group("version"))
        except Exception:
            raise RepositoryPreflightError("repository_python_lock_unsupported") from None
        if name in seen_names:
            raise RepositoryPreflightError("repository_python_lock_duplicate")
        seen_names.add(name)
        hashes = frozenset(re.findall(r"--hash=sha256:([0-9a-f]{64})", match.group("hashes")))
        if not hashes:
            raise RepositoryPreflightError("repository_python_lock_unhashed")
        packages.append(LockedPythonPackage(name, match.group("version"), hashes))

    if not packages or "pytest" not in seen_names:
        raise RepositoryPreflightError("repository_python_lock_missing_pytest")
    if len(packages) > MAX_PACKAGE_METADATA_REQUESTS:
        raise RepositoryPreflightError("repository_package_request_limit")
    return tuple(packages)


def _parse_node_manifest(
    package_json: bytes,
    package_lock_json: bytes,
) -> tuple[LockedNodePackage, ...]:
    if max(len(package_json), len(package_lock_json)) > MAX_MANIFEST_BYTES:
        raise RepositoryPreflightError("repository_manifest_too_large")
    try:
        package = json.loads(package_json)
        lock = json.loads(package_lock_json)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        raise RepositoryPreflightError("repository_manifest_invalid") from None
    if not isinstance(package, dict) or not isinstance(lock, dict):
        raise RepositoryPreflightError("repository_manifest_invalid")

    scripts = package.get("scripts", {})
    if not isinstance(scripts, dict) or not isinstance(scripts.get("test"), str):
        raise RepositoryPreflightError("repository_node_test_missing")
    if any(key in package for key in ("workspaces",)):
        raise RepositoryPreflightError("repository_node_workspace_unsupported")
    if _has_install_lifecycle(scripts):
        raise RepositoryPreflightError("repository_node_install_lifecycle_unsupported")

    lockfile_version = lock.get("lockfileVersion")
    packages_map = lock.get("packages")
    if lockfile_version not in {2, 3} or not isinstance(packages_map, dict):
        raise RepositoryPreflightError("repository_node_lock_unsupported")
    root_package = packages_map.get("")
    if not isinstance(root_package, dict):
        raise RepositoryPreflightError("repository_node_lock_invalid")
    _validate_node_package_identity(package, lock, root_package)
    if any(
        key in root_package for key in ("workspaces", "bundledDependencies", "bundleDependencies")
    ):
        raise RepositoryPreflightError("repository_node_workspace_unsupported")
    _validate_node_root_dependencies(package, root_package, packages_map)

    locked: list[LockedNodePackage] = []
    packages_by_identity: dict[tuple[str, str], LockedNodePackage] = {}
    for path, metadata in packages_map.items():
        if path == "":
            continue
        if not isinstance(path, str) or not isinstance(metadata, dict):
            raise RepositoryPreflightError("repository_node_lock_invalid")
        if (
            metadata.get("link") is True
            or metadata.get("inBundle") is True
            or metadata.get("hasShrinkwrap") is True
        ):
            raise RepositoryPreflightError("repository_node_link_unsupported")
        if metadata.get("hasInstallScript") is True:
            raise RepositoryPreflightError("repository_node_install_lifecycle_unsupported")
        if "os" in metadata or "cpu" in metadata:
            raise RepositoryPreflightError("repository_node_platform_package_unsupported")
        if not path.startswith("node_modules/"):
            raise RepositoryPreflightError("repository_node_workspace_unsupported")

        name = _node_package_name_from_lock_path(path)
        if "name" in metadata and metadata["name"] != name:
            raise RepositoryPreflightError("repository_node_lock_mismatch")
        version = metadata.get("version")
        resolved_url = metadata.get("resolved")
        integrity = metadata.get("integrity")
        if (
            not isinstance(version, str)
            or _PACKAGE_VERSION_PATTERN.fullmatch(version) is None
            or not isinstance(resolved_url, str)
            or not isinstance(integrity, str)
        ):
            raise RepositoryPreflightError("repository_node_lock_invalid")
        _require_public_npm_url(resolved_url)
        _require_sha512_integrity(integrity)
        package_record = LockedNodePackage(name, version, resolved_url, integrity)
        identity = (name, version)
        previous = packages_by_identity.get(identity)
        if previous is not None:
            if previous != package_record:
                raise RepositoryPreflightError("repository_node_lock_mismatch")
            continue
        packages_by_identity[identity] = package_record
        locked.append(package_record)

    if not locked:
        raise RepositoryPreflightError("repository_node_lock_empty")
    if len(locked) > MAX_PACKAGE_METADATA_REQUESTS:
        raise RepositoryPreflightError("repository_package_request_limit")
    return tuple(locked)


def _validate_node_root_dependencies(
    package: dict[str, Any],
    lock_root: dict[str, Any],
    packages_map: dict[str, Any],
) -> None:
    for field in ("dependencies", "devDependencies", "optionalDependencies", "peerDependencies"):
        source_dependencies = package.get(field, {})
        lock_dependencies = lock_root.get(field, {})
        if not isinstance(source_dependencies, dict) or not isinstance(lock_dependencies, dict):
            raise RepositoryPreflightError("repository_node_lock_invalid")
        if source_dependencies != lock_dependencies:
            raise RepositoryPreflightError("repository_node_lock_mismatch")
        for name, specifier in source_dependencies.items():
            if (
                not isinstance(name, str)
                or _NPM_PACKAGE_PATTERN.fullmatch(name) is None
                or not isinstance(specifier, str)
                or not specifier.strip()
            ):
                raise RepositoryPreflightError("repository_node_lock_invalid")
            if specifier.strip().lower().startswith(_UNSUPPORTED_NPM_SPEC_PREFIXES):
                raise RepositoryPreflightError("repository_package_source_unsupported")
            if f"node_modules/{name}" not in packages_map:
                raise RepositoryPreflightError("repository_node_lock_mismatch")


def _validate_node_package_identity(
    package: dict[str, Any], lock: dict[str, Any], lock_root: dict[str, Any]
) -> None:
    for field in ("name", "version"):
        source_value = package.get(field)
        lock_value = lock.get(field)
        root_value = lock_root.get(field)
        present_values = [
            value for value in (source_value, lock_value, root_value) if value is not None
        ]
        if not present_values:
            continue
        if (
            len(present_values) != 3
            or not isinstance(source_value, str)
            or source_value != lock_value
            or source_value != root_value
        ):
            raise RepositoryPreflightError("repository_node_lock_mismatch")
    source_name = package.get("name")
    source_version = package.get("version")
    if source_name is not None and (
        not isinstance(source_name, str) or _NPM_PACKAGE_PATTERN.fullmatch(source_name) is None
    ):
        raise RepositoryPreflightError("repository_node_lock_invalid")
    if source_version is not None and (
        not isinstance(source_version, str)
        or _PACKAGE_VERSION_PATTERN.fullmatch(source_version) is None
    ):
        raise RepositoryPreflightError("repository_node_lock_invalid")


def _has_install_lifecycle(scripts: dict[str, Any]) -> bool:
    return any(name in scripts for name in _INSTALL_LIFECYCLE_SCRIPTS)


def _node_package_name_from_lock_path(path: str) -> str:
    components = path.split("/")
    if any(component in {"", ".", ".."} for component in components):
        raise RepositoryPreflightError("repository_node_lock_invalid")
    index = 0
    while index < len(components):
        if components[index] != "node_modules" or index + 1 >= len(components):
            raise RepositoryPreflightError("repository_node_lock_invalid")
        index += 1
        if components[index].startswith("@"):
            if index + 1 >= len(components):
                raise RepositoryPreflightError("repository_node_lock_invalid")
            package_name = f"{components[index]}/{components[index + 1]}"
            index += 2
        else:
            package_name = components[index]
            index += 1
        if _NPM_PACKAGE_PATTERN.fullmatch(package_name) is None:
            raise RepositoryPreflightError("repository_node_lock_invalid")
        if index == len(components):
            return package_name
        if components[index] != "node_modules":
            raise RepositoryPreflightError("repository_node_lock_invalid")
    raise RepositoryPreflightError("repository_node_lock_invalid")


def _require_public_npm_url(value: str) -> None:
    parts = urlsplit(value)
    try:
        port = parts.port
    except ValueError:
        port = -1
    if (
        parts.scheme != "https"
        or parts.hostname != "registry.npmjs.org"
        or port not in {None, 443}
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
    ):
        raise RepositoryPreflightError("repository_package_source_unsupported")


def _require_sha512_integrity(value: str) -> None:
    if not value.startswith("sha512-"):
        raise RepositoryPreflightError("repository_node_integrity_unsupported")
    try:
        digest = base64.b64decode(value.removeprefix("sha512-"), validate=True)
    except (ValueError, binascii.Error):
        raise RepositoryPreflightError("repository_node_integrity_invalid") from None
    if len(digest) != 64:
        raise RepositoryPreflightError("repository_node_integrity_invalid")


def _validate_python_layout(
    entries: dict[str, dict[str, Any]], manifests: dict[str, bytes]
) -> None:
    blob_paths = {path for path, entry in entries.items() if entry["type"] == "blob"}
    if any(path in blob_paths for path in ("setup.py", "setup.cfg")):
        raise RepositoryPreflightError("repository_python_build_hook_unsupported")
    if not any(
        path.endswith(".py") and ("/" not in path or path.startswith("src/")) for path in blob_paths
    ):
        raise RepositoryPreflightError("repository_python_layout_unsupported")
    pyproject = manifests.get("pyproject.toml")
    if pyproject is not None:
        try:
            document = tomllib.loads(pyproject.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError):
            raise RepositoryPreflightError("repository_manifest_invalid") from None
        if "build-system" in document:
            raise RepositoryPreflightError("repository_python_build_hook_unsupported")


async def _verify_python_wheel_metadata(
    reader: _BoundedMetadataReader,
    package: LockedPythonPackage,
) -> tuple[RequiredPythonPackage, ...]:
    # PyPI exposes per-release `requires_dist`/`requires_python`; T17 must verify
    # the downloaded wheel because uploaded metadata can differ from the artifact.
    # Source: https://docs.pypi.org/api/json/
    path = f"/pypi/{quote(package.name, safe='')}/{quote(package.version, safe='')}/json"
    document = await reader.pypi_json(path)
    info = document.get("info")
    files = document.get("urls")
    if (
        not isinstance(info, dict)
        or not isinstance(info.get("name"), str)
        or _normalized_python_name(info["name"]) != package.name
        or info.get("version") != package.version
        or not isinstance(files, list)
        or "requires_python" not in info
        or "requires_dist" not in info
    ):
        raise RepositoryPreflightError("repository_python_package_metadata_invalid")

    _require_python_target_compatibility(info["requires_python"])

    pure_wheel_hashes: set[str] = set()
    for item in files:
        if not isinstance(item, dict) or item.get("packagetype") != "bdist_wheel":
            continue
        filename = item.get("filename")
        if not isinstance(filename, str) or not _is_pure_python_wheel(
            filename, package.name, package.version
        ):
            continue
        if "requires_python" not in item:
            raise RepositoryPreflightError("repository_python_package_metadata_invalid")
        _require_python_target_compatibility(item["requires_python"])
        digests = item.get("digests")
        if not isinstance(digests, dict):
            raise RepositoryPreflightError("repository_python_package_metadata_invalid")
        sha256 = digests.get("sha256")
        if not isinstance(sha256, str) or _SHA256_PATTERN.fullmatch(sha256) is None:
            raise RepositoryPreflightError("repository_python_package_metadata_invalid")
        _require_pypi_file_url(item.get("url"))
        pure_wheel_hashes.add(sha256)

    if not package.sha256_hashes <= pure_wheel_hashes:
        # PyPI upload metadata can be inaccurate; T17 must verify downloaded wheel bytes.
        raise RepositoryPreflightError("repository_python_wheel_unsupported")
    return _active_python_requirements(info["requires_dist"])


def _require_python_target_compatibility(requires_python: object) -> None:
    if requires_python is None:
        return
    if not isinstance(requires_python, str):
        raise RepositoryPreflightError("repository_python_package_metadata_invalid")
    try:
        supported = SpecifierSet(requires_python).contains(
            Version(PYTHON_TARGET_VERSION), prereleases=True
        )
    except Exception:
        raise RepositoryPreflightError("repository_python_package_metadata_invalid") from None
    if not supported:
        raise RepositoryPreflightError("repository_python_version_unsupported")


def _active_python_requirements(requires_dist: object) -> tuple[RequiredPythonPackage, ...]:
    if requires_dist is None:
        return ()
    if not isinstance(requires_dist, list) or any(
        not isinstance(item, str) for item in requires_dist
    ):
        raise RepositoryPreflightError("repository_python_package_metadata_invalid")
    # Packaging parses PEP 508 requirements and evaluates markers against the
    # fixed target, with `extra` empty. Source:
    # https://packaging.pypa.io/en/stable/requirements.html
    # https://packaging.pypa.io/en/stable/markers.html
    environment = {
        "implementation_name": "cpython",
        "implementation_version": PYTHON_TARGET_VERSION,
        "os_name": "posix",
        "platform_machine": "x86_64",
        "platform_python_implementation": "CPython",
        "platform_release": "",
        "platform_system": "Linux",
        "platform_version": "",
        "python_full_version": PYTHON_TARGET_VERSION,
        "python_version": ".".join(PYTHON_TARGET_VERSION.split(".")[:2]),
        "sys_platform": "linux",
        "extra": "",
    }
    active: list[RequiredPythonPackage] = []
    for requirement_text in requires_dist:
        if re.search(r"\bplatform_(?:release|version)\b", requirement_text):
            raise RepositoryPreflightError("repository_python_marker_unsupported")
        try:
            requirement = Requirement(requirement_text)
            applies = requirement.marker is None or requirement.marker.evaluate(
                environment=environment, context="metadata"
            )
        except Exception:
            raise RepositoryPreflightError("repository_python_package_metadata_invalid") from None
        if not applies:
            continue
        if requirement.url is not None:
            raise RepositoryPreflightError("repository_package_source_unsupported")
        if requirement.extras:
            raise RepositoryPreflightError("repository_python_extras_unsupported")
        active.append(
            RequiredPythonPackage(
                name=canonicalize_name(requirement.name),
                specifier=str(requirement.specifier),
            )
        )
    return tuple(active)


def _validate_python_dependency_closure(
    packages: tuple[LockedPythonPackage, ...],
    required_dependencies: list[RequiredPythonPackage],
) -> None:
    locked_versions = {package.name: package.version for package in packages}
    for dependency in required_dependencies:
        locked_version = locked_versions.get(dependency.name)
        if locked_version is None:
            raise RepositoryPreflightError("repository_python_dependency_missing")
        try:
            matches = SpecifierSet(dependency.specifier).contains(
                Version(locked_version), prereleases=True
            )
        except Exception:
            raise RepositoryPreflightError(
                "repository_python_dependency_metadata_invalid"
            ) from None
        if not matches:
            raise RepositoryPreflightError("repository_python_dependency_version_mismatch")


def _is_pure_python_wheel(filename: str, package_name: str, version: str) -> bool:
    if not filename.endswith(".whl"):
        return False
    parts = filename[:-4].rsplit("-", 3)
    if len(parts) != 4 or parts[1:] != ["py3", "none", "any"]:
        return False
    distribution_and_version = parts[0].rsplit("-", 1)
    if len(distribution_and_version) != 2:
        return False
    distribution, file_version = distribution_and_version
    return _normalized_python_name(distribution) == package_name and file_version == version


def _require_pypi_file_url(value: object) -> None:
    if not isinstance(value, str):
        raise RepositoryPreflightError("repository_python_package_metadata_invalid")
    parts = urlsplit(value)
    try:
        port = parts.port
    except ValueError:
        port = -1
    if (
        parts.scheme != "https"
        or parts.hostname != "files.pythonhosted.org"
        or port not in {None, 443}
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
    ):
        raise RepositoryPreflightError("repository_python_package_source_unsupported")


async def _verify_npm_metadata(
    reader: _BoundedMetadataReader,
    package: LockedNodePackage,
) -> None:
    path = f"/{quote(package.name, safe='')}/{quote(package.version, safe='')}"
    document = await reader.npm_json(path)
    distribution = document.get("dist")
    scripts = document.get("scripts", {})
    if (
        document.get("name") != package.name
        or document.get("version") != package.version
        or not isinstance(distribution, dict)
        or not isinstance(scripts, dict)
    ):
        raise RepositoryPreflightError("repository_node_package_metadata_invalid")
    if (
        _has_install_lifecycle(scripts)
        or document.get("hasInstallScript") is True
        or "os" in document
        or "cpu" in document
        or document.get("gypfile") is True
        or "binary" in document
    ):
        raise RepositoryPreflightError("repository_node_native_install_unsupported")
    tarball = distribution.get("tarball")
    integrity = distribution.get("integrity")
    _require_public_npm_url(tarball if isinstance(tarball, str) else "")
    _require_sha512_integrity(integrity if isinstance(integrity, str) else "")
    if tarball != package.resolved_url or integrity != package.integrity:
        raise RepositoryPreflightError("repository_node_lock_mismatch")
