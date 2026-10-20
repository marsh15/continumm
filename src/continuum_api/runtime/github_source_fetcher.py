"""Bounded, read-only acquisition of an admitted GitHub source snapshot."""

from __future__ import annotations

import asyncio
import io
import re
import tarfile
import time
import zlib
from urllib.parse import quote, urlsplit

import httpx
from pydantic import SecretStr
from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.artifacts.archives import (
    MAX_COMPRESSED_BYTES,
    MAX_FILES,
    MAX_SOURCE_BYTES,
    MAX_TAR_BYTES,
    ArchiveFile,
)
from continuum_api.auth.github import GitHubAuthorizationClient
from continuum_api.repositories.preflight import EligibilityReceipt, RepositoryAccessProof
from continuum_api.runtime.approvals import RepositoryPublicationFacts
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, lock_current_execution
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.runtime.source_preparation import SourcePreparationError

GITHUB_API_ORIGIN = "https://api.github.com"
GITHUB_CODELOAD_ORIGIN = "https://codeload.github.com"
SOURCE_FETCH_DEADLINE_SECONDS = 45.0
MAX_ARCHIVE_ENTRIES = MAX_FILES + 1_000
_GITHUB_COMPONENT = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
_FULL_SHA = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_ROOT_COMPONENT = re.compile(r"^[A-Za-z0-9_.-]{1,200}$")


class GitHubSourceFetcher:
    """Fetch a fixed commit through fresh GitHub App read authorization.

    The API bearer token is sent only to api.github.com. GitHub's redirect is
    validated before an unauthenticated request is sent to codeload.github.com.
    Archive members are read into memory and are never extracted on disk.
    """

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        authorization: GitHubAuthorizationClient,
        *,
        api_transport: httpx.AsyncBaseTransport | None = None,
        archive_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._authorization = authorization
        self._api_transport = api_transport
        self._archive_transport = archive_transport

    async def __call__(
        self, lease: ExecutionLease, repository_id: int, receipt: EligibilityReceipt
    ) -> tuple[ArchiveFile, ...]:
        if lease.owner_id != self._principal.account_id:
            raise SourcePreparationError("source_preparation_owner_mismatch")
        if (
            type(repository_id) is not int
            or repository_id <= 0
            or not isinstance(receipt, EligibilityReceipt)
            or receipt.repository_id != repository_id
            or not isinstance(receipt.commit_sha, str)
            or _FULL_SHA.fullmatch(receipt.commit_sha) is None
        ):
            raise SourcePreparationError("source_fetch_admission_invalid")

        try:
            github_user_id = await asyncio.to_thread(
                self._current_github_user_id, lease, repository_id, receipt
            )
        except LeaseLost:
            raise
        except SQLAlchemyError:
            raise SourcePreparationError("source_fetch_persistence_unavailable") from None
        try:
            async with asyncio.timeout(SOURCE_FETCH_DEADLINE_SECONDS):
                try:
                    facts, token = await self._authorization.repository_read_authorization(
                        repository_id, github_user_id
                    )
                except TimeoutError:
                    raise SourcePreparationError("source_fetch_timeout") from None
                except Exception:
                    raise SourcePreparationError("source_fetch_authorization_unavailable") from None
                if not _valid_facts(facts, repository_id, github_user_id):
                    raise SourcePreparationError("source_fetch_authorization_invalid")
                return await self._fetch_archive(facts, token, receipt.commit_sha)
        except TimeoutError:
            raise SourcePreparationError("source_fetch_timeout") from None
        except SourcePreparationError:
            raise
        except Exception:
            raise SourcePreparationError("source_fetch_unavailable") from None

    def _current_github_user_id(
        self, lease: ExecutionLease, repository_id: int, receipt: EligibilityReceipt
    ) -> int:
        if lease.owner_id != self._principal.account_id:
            raise SourcePreparationError("source_preparation_owner_mismatch")
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            admission = connection.execute(
                text(
                    "SELECT users.github_user_id, runs.github_repository_id, "
                    "runs.revision, steps.activity, steps.revision AS step_revision, "
                    "pins.profile_id, pins.profile_version, pins.source_sha, "
                    "pins.source_tree_sha, pins.source_receipt "
                    "FROM runtime_private.users AS users JOIN runtime_private.runs AS runs "
                    "ON runs.owner_id=users.user_id JOIN runtime_private.steps AS steps "
                    "ON steps.owner_id=runs.owner_id AND steps.run_id=runs.run_id "
                    "JOIN runtime_private.run_admission_pins AS pins "
                    "ON pins.owner_id=runs.owner_id AND pins.run_id=runs.run_id "
                    "WHERE users.user_id=:owner AND users.status='active' "
                    "AND runs.run_id=:run AND steps.step_id=:step"
                ),
                {"owner": lease.owner_id, "run": lease.run_id, "step": lease.step_id},
            ).one_or_none()
        if admission is None:
            raise SourcePreparationError("source_preparation_admission_pin_invalid")
        github_user_id = admission.github_user_id
        if (
            admission.github_repository_id != repository_id
            or admission.revision != 0
            or admission.step_revision != 0
            or admission.activity != "source_preparation"
            or admission.profile_id != receipt.profile_id
            or admission.profile_version != receipt.profile_version
            or admission.source_sha != receipt.commit_sha
            or admission.source_tree_sha != receipt.tree_sha
            or admission.source_receipt != receipt.to_cache_payload()
        ):
            raise SourcePreparationError("source_preparation_admission_pin_invalid")
        if type(github_user_id) is not int or github_user_id <= 0:
            raise SourcePreparationError("source_preparation_step_stale")
        return github_user_id

    async def _fetch_archive(
        self,
        facts: RepositoryPublicationFacts,
        token: SecretStr,
        commit_sha: str,
    ) -> tuple[ArchiveFile, ...]:
        owner = quote(facts.owner, safe="")
        name = quote(facts.name, safe="")
        api_url = f"{GITHUB_API_ORIGIN}/repos/{owner}/{name}/tarball/{commit_sha}"
        deadline = time.monotonic() + SOURCE_FETCH_DEADLINE_SECONDS
        timeout = httpx.Timeout(connect=5, read=10, write=5, pool=5)
        async with asyncio.timeout(SOURCE_FETCH_DEADLINE_SECONDS):
            async with httpx.AsyncClient(
                transport=self._api_transport,
                timeout=timeout,
                follow_redirects=False,
                trust_env=False,
                headers={},
                cookies={},
                auth=None,
            ) as api_client:
                async with api_client.stream(
                    "GET",
                    api_url,
                    headers={
                        "accept": "application/vnd.github+json",
                        "accept-encoding": "identity",
                        "x-github-api-version": "2026-03-10",
                        "user-agent": "Continuum-Source-Fetch",
                        "authorization": f"Bearer {token.get_secret_value()}",
                    },
                    follow_redirects=False,
                    timeout=_remaining_timeout(deadline, timeout),
                ) as response:
                    if (
                        response.request.url.scheme != "https"
                        or response.request.url.host != "api.github.com"
                        or response.request.url.port not in {None, 443}
                    ):
                        raise SourcePreparationError("source_fetch_origin_invalid")
                    if response.status_code in {403, 429}:
                        raise SourcePreparationError("source_fetch_rate_limited")
                    if response.status_code != 302:
                        raise SourcePreparationError("source_fetch_redirect_invalid")
                    location = response.headers.get("location")
            archive_url = _validated_codeload_location(
                location, facts.owner, facts.name, commit_sha
            )
            async with httpx.AsyncClient(
                transport=self._archive_transport,
                timeout=timeout,
                follow_redirects=False,
                trust_env=False,
                headers={},
                cookies={},
                auth=None,
            ) as archive_client:
                async with archive_client.stream(
                    "GET",
                    archive_url,
                    headers={
                        "accept": "application/vnd.github+json",
                        "accept-encoding": "identity",
                        "user-agent": "Continuum-Source-Fetch",
                    },
                    follow_redirects=False,
                    timeout=_remaining_timeout(deadline, timeout),
                ) as archive_response:
                    if 300 <= archive_response.status_code < 400:
                        raise SourcePreparationError("source_fetch_redirect_invalid")
                    if archive_response.status_code != 200:
                        raise SourcePreparationError("source_fetch_download_failed")
                    if archive_response.request.url.host != "codeload.github.com":
                        raise SourcePreparationError("source_fetch_origin_invalid")
                    _check_response_headers(archive_response.headers)
                    compressed = bytearray()
                    async for chunk in archive_response.aiter_raw():
                        if time.monotonic() >= deadline:
                            raise SourcePreparationError("source_fetch_timeout")
                        if len(compressed) + len(chunk) > MAX_COMPRESSED_BYTES:
                            raise SourcePreparationError("source_fetch_compressed_size_limit")
                        compressed.extend(chunk)
        return await asyncio.to_thread(_parse_github_archive, bytes(compressed))


def _valid_facts(
    facts: RepositoryPublicationFacts, repository_id: int, github_user_id: int
) -> bool:
    proof = getattr(facts, "access_proof", None)
    return (
        isinstance(facts, RepositoryPublicationFacts)
        and proof is not None
        and isinstance(proof, RepositoryAccessProof)
        and facts.repository_id == repository_id
        and facts.github_user_id == github_user_id
        and proof.repository_id == repository_id
        and proof.github_user_id == github_user_id
        and proof.actor_can_write is True
        and proof.app_can_access is True
        and isinstance(facts.owner, str)
        and _GITHUB_COMPONENT.fullmatch(facts.owner) is not None
        and facts.owner not in {".", ".."}
        and isinstance(facts.name, str)
        and _GITHUB_COMPONENT.fullmatch(facts.name) is not None
        and facts.name not in {".", ".."}
    )


def _validated_codeload_location(
    location: str | None, owner: str, name: str, commit_sha: str
) -> str:
    expected_path = f"/{quote(owner, safe='')}/{quote(name, safe='')}/legacy.tar.gz/{commit_sha}"
    if location is None:
        raise SourcePreparationError("source_fetch_redirect_invalid")
    try:
        parsed = urlsplit(location)
        valid = (
            parsed.scheme == "https"
            and parsed.hostname == "codeload.github.com"
            and parsed.port in {None, 443}
            and parsed.username is None
            and parsed.password is None
            and parsed.path == expected_path
            and not parsed.query
            and not parsed.fragment
        )
    except ValueError:
        valid = False
    if not valid:
        raise SourcePreparationError("source_fetch_redirect_invalid")
    return f"{GITHUB_CODELOAD_ORIGIN}{expected_path}"


def _check_response_headers(headers: httpx.Headers) -> None:
    encoding = headers.get("content-encoding")
    if encoding is not None and encoding.strip().lower() != "identity":
        raise SourcePreparationError("source_fetch_encoding_unsupported")
    length = headers.get("content-length")
    if length is None:
        return
    try:
        advertised = int(length)
    except ValueError:
        raise SourcePreparationError("source_fetch_response_invalid") from None
    if advertised < 0 or advertised > MAX_COMPRESSED_BYTES:
        raise SourcePreparationError("source_fetch_compressed_size_limit")


def _parse_github_archive(compressed: bytes) -> tuple[ArchiveFile, ...]:
    tar_bytes = _inflate_gzip_bounded(compressed)
    try:
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as archive:
            entry_count = 0
            root: str | None = None
            files: list[ArchiveFile] = []
            source_bytes = 0
            for member in archive:
                entry_count += 1
                if entry_count > MAX_ARCHIVE_ENTRIES:
                    raise SourcePreparationError("source_fetch_entry_limit")
                path = member.name.rstrip("/")
                if not path or path.startswith("/") or "\\" in path or "\x00" in path:
                    raise SourcePreparationError("source_fetch_path_invalid")
                components = path.split("/")
                if any(part in {"", ".", ".."} for part in components):
                    raise SourcePreparationError("source_fetch_path_invalid")
                if root is None:
                    root = components[0]
                    if _ROOT_COMPONENT.fullmatch(root) is None:
                        raise SourcePreparationError("source_fetch_root_invalid")
                if components[0] != root:
                    raise SourcePreparationError("source_fetch_root_invalid")
                if len(components) == 1:
                    if not member.isdir():
                        raise SourcePreparationError("source_fetch_root_invalid")
                    continue
                if member.isdir():
                    continue
                if not member.isfile() or member.issparse() or member.linkname:
                    raise SourcePreparationError("source_fetch_member_unsupported")
                if member.mode & 0o7000:
                    raise SourcePreparationError("source_fetch_mode_unsupported")
                if len(files) >= MAX_FILES:
                    raise SourcePreparationError("source_fetch_file_limit")
                if member.size < 0 or source_bytes + member.size > MAX_SOURCE_BYTES:
                    raise SourcePreparationError("source_fetch_source_size_limit")
                stream = archive.extractfile(member)
                if stream is None:
                    raise SourcePreparationError("source_fetch_member_invalid")
                content = stream.read(member.size + 1)
                if len(content) != member.size:
                    raise SourcePreparationError("source_fetch_tar_truncated")
                mode = 0o755 if member.mode & 0o111 else 0o644
                files.append(ArchiveFile("/".join(components[1:]), content, mode))
                source_bytes += len(content)
            if root is None or entry_count == 0:
                raise SourcePreparationError("source_fetch_archive_invalid")
            end_marker = archive.offset
            if (
                end_marker < 0
                or end_marker + 2 * tarfile.BLOCKSIZE > len(tar_bytes)
                or any(tar_bytes[end_marker:])
            ):
                raise SourcePreparationError("source_fetch_archive_invalid")
            return tuple(files)
    except SourcePreparationError:
        raise
    except (tarfile.TarError, OSError, ValueError, EOFError, zlib.error):
        raise SourcePreparationError("source_fetch_archive_invalid") from None


def _inflate_gzip_bounded(compressed: bytes) -> bytes:
    if not compressed:
        raise SourcePreparationError("source_fetch_archive_invalid")
    inflater = zlib.decompressobj(16 + zlib.MAX_WBITS)
    output = bytearray()
    pending = b""
    try:
        for offset in range(0, len(compressed), 64 * 1024):
            pending = compressed[offset : offset + 64 * 1024]
            while pending:
                room = MAX_TAR_BYTES - len(output)
                piece = inflater.decompress(pending, room + 1)
                output.extend(piece)
                if len(output) > MAX_TAR_BYTES:
                    raise SourcePreparationError("source_fetch_tar_size_limit")
                next_pending = inflater.unconsumed_tail
                if not next_pending:
                    break
                if len(next_pending) == len(pending) and not piece:
                    raise SourcePreparationError("source_fetch_archive_invalid")
                pending = next_pending
        while True:
            room = MAX_TAR_BYTES - len(output)
            piece = inflater.decompress(b"", room + 1)
            output.extend(piece)
            if len(output) > MAX_TAR_BYTES:
                raise SourcePreparationError("source_fetch_tar_size_limit")
            if not piece:
                break
    except SourcePreparationError:
        raise
    except (zlib.error, ValueError):
        raise SourcePreparationError("source_fetch_archive_invalid") from None
    if len(output) > MAX_TAR_BYTES:
        raise SourcePreparationError("source_fetch_tar_size_limit")
    if not inflater.eof or inflater.unused_data:
        raise SourcePreparationError("source_fetch_archive_invalid")
    return bytes(output)


def _remaining_timeout(deadline: float, configured: httpx.Timeout) -> httpx.Timeout:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SourcePreparationError("source_fetch_timeout")
    return httpx.Timeout(
        connect=min(configured.connect or remaining, remaining),
        read=min(configured.read or remaining, remaining),
        write=min(configured.write or remaining, remaining),
        pool=min(configured.pool or remaining, remaining),
    )
