"""Mocked HTTP checks for the bounded GitHub source archive adapter."""

from __future__ import annotations

import asyncio
import gzip
import io
import tarfile
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import create_engine

from continuum_api.artifacts.archives import ArchiveFile
from continuum_api.repositories.preflight import EligibilityReceipt, RepositoryAccessProof
from continuum_api.runtime.approvals import RepositoryPublicationFacts
from continuum_api.runtime.github_source_fetcher import (
    GitHubSourceFetcher,
    _parse_github_archive,
)
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.source_preparation import SourcePreparationError

_REPOSITORY_ID = 412
_USER_ID = 77
_SHA = "a" * 40
_OWNER = "sample-owner"
_NAME = "sample-repository"
_LOCATION = f"https://codeload.github.com/{_OWNER}/{_NAME}/legacy.tar.gz/{_SHA}"


def _facts() -> RepositoryPublicationFacts:
    proof = RepositoryAccessProof(_REPOSITORY_ID, _USER_ID, True, True)
    return RepositoryPublicationFacts(
        repository_id=_REPOSITORY_ID,
        github_user_id=_USER_ID,
        owner=_OWNER,
        name=_NAME,
        default_branch="main",
        base_commit_sha=_SHA,
        access_proof=proof,
    )


def _github_tar(
    *, include_link: bool = False, truncated_tar: bool = False, truncated_end: bool = False
) -> bytes:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as archive:
        archive.pax_headers = {"comment": "fixture"}
        root = tarfile.TarInfo("sample-root/")
        root.type = tarfile.DIRTYPE
        root.mode = 0o775
        archive.addfile(root)
        file = tarfile.TarInfo("sample-root/src/app.py")
        file.mode = 0o664
        file.size = 12
        archive.addfile(file, io.BytesIO(b"print('ok')\n"))
        executable = tarfile.TarInfo("sample-root/bin/run")
        executable.mode = 0o775
        executable.size = 2
        archive.addfile(executable, io.BytesIO(b"ok"))
        if include_link:
            link = tarfile.TarInfo("sample-root/src/link")
            link.type = tarfile.SYMTYPE
            link.linkname = "../app.py"
            archive.addfile(link)
    tar_bytes = stream.getvalue()
    if truncated_tar:
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as archive:
            member = next(item for item in archive if item.isfile())
            tar_bytes = tar_bytes[: member.offset_data + 1]
    if truncated_end:
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as archive:
            list(archive)
            tar_bytes = tar_bytes[: archive.offset]
    return gzip.compress(tar_bytes)


def _archive_with_members(entries: list[tarfile.TarInfo]) -> bytes:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as archive:
        root = tarfile.TarInfo("sample-root/")
        root.type = tarfile.DIRTYPE
        archive.addfile(root)
        for member in entries:
            content = b"x" * member.size if member.isfile() else None
            archive.addfile(member, io.BytesIO(content) if content is not None else None)
    return gzip.compress(stream.getvalue())


class _BytesStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes) -> None:
        self._content = content

    async def __aiter__(self):
        yield self._content


def _fetcher(
    api_handler,
    archive_handler,
) -> tuple[GitHubSourceFetcher, list[httpx.Request], list[httpx.Request]]:
    api_requests: list[httpx.Request] = []
    archive_requests: list[httpx.Request] = []

    def api(request: httpx.Request) -> httpx.Response:
        api_requests.append(request)
        return api_handler(request)

    def archive(request: httpx.Request) -> httpx.Response:
        archive_requests.append(request)
        return archive_handler(request)

    principal = Principal(uuid4())
    fetcher = GitHubSourceFetcher(
        create_engine("sqlite://"),
        principal,
        authorization=None,  # Direct archive path test; auth is covered by the caller boundary.
        api_transport=httpx.MockTransport(api),
        archive_transport=httpx.MockTransport(archive),
    )  # type: ignore[arg-type]
    return fetcher, api_requests, archive_requests


def test_fetcher_uses_validated_api_redirect_and_strips_archive_root() -> None:
    payload = _github_tar()

    def api_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.github.com"
        assert request.url.path == f"/repos/{_OWNER}/{_NAME}/tarball/{_SHA}"
        assert request.headers["authorization"] == "Bearer secret-token"
        return httpx.Response(302, headers={"location": _LOCATION}, request=request)

    def archive_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "codeload.github.com"
        assert "authorization" not in request.headers
        assert "cookie" not in request.headers
        return httpx.Response(200, stream=_BytesStream(payload), request=request)

    fetcher, api_requests, archive_requests = _fetcher(api_handler, archive_handler)
    files = asyncio.run(fetcher._fetch_archive(_facts(), SecretStr("secret-token"), _SHA))
    assert files == (
        ArchiveFile("src/app.py", b"print('ok')\n", 0o644),
        ArchiveFile("bin/run", b"ok", 0o755),
    )
    assert len(api_requests) == len(archive_requests) == 1


@pytest.mark.parametrize(
    "location",
    [
        "https://attacker.example/steal",
        f"http://codeload.github.com/{_OWNER}/{_NAME}/legacy.tar.gz/{_SHA}",
        f"https://codeload.github.com/{_OWNER}/{_NAME}/legacy.tar.gz/{'b' * 40}",
        f"https://user@codeload.github.com/{_OWNER}/{_NAME}/legacy.tar.gz/{_SHA}",
        _LOCATION + "?download=1",
    ],
)
def test_untrusted_redirect_is_rejected_before_archive_request(location: str) -> None:
    fetcher, _, archive_requests = _fetcher(
        lambda request: httpx.Response(302, headers={"location": location}, request=request),
        lambda request: httpx.Response(200, stream=_BytesStream(_github_tar()), request=request),
    )
    with pytest.raises(SourcePreparationError, match="source_fetch_redirect_invalid"):
        asyncio.run(fetcher._fetch_archive(_facts(), SecretStr("secret-token"), _SHA))
    assert archive_requests == []


def test_archive_symlink_is_rejected_without_extraction() -> None:
    with pytest.raises(SourcePreparationError, match="source_fetch_member_unsupported"):
        _parse_github_archive(_github_tar(include_link=True))


@pytest.mark.parametrize(
    ("name", "member_type", "linkname", "expected"),
    [
        ("sample-root/../escape", tarfile.REGTYPE, "", "source_fetch_path_invalid"),
        ("other-root/file", tarfile.REGTYPE, "", "source_fetch_root_invalid"),
        (
            "sample-root/hard",
            tarfile.LNKTYPE,
            "sample-root/src/app.py",
            "source_fetch_member_unsupported",
        ),
        ("sample-root/pipe", tarfile.FIFOTYPE, "", "source_fetch_member_unsupported"),
    ],
)
def test_archive_rejects_unsafe_paths_roots_and_member_types(
    name: str, member_type: bytes, linkname: str, expected: str
) -> None:
    member = tarfile.TarInfo(name)
    member.type = member_type
    member.linkname = linkname
    member.size = 1 if member_type == tarfile.REGTYPE else 0
    with pytest.raises(SourcePreparationError, match=expected):
        _parse_github_archive(_archive_with_members([member]))


def test_truncated_gzip_archive_is_rejected_with_sanitized_error() -> None:
    with pytest.raises(SourcePreparationError, match="source_fetch_archive_invalid") as error:
        _parse_github_archive(_github_tar(truncated_tar=True)[:-3])
    assert str(error.value) == "source_fetch_archive_invalid"


def test_truncated_tar_archive_is_rejected() -> None:
    with pytest.raises(SourcePreparationError, match="source_fetch_archive_invalid"):
        _parse_github_archive(_github_tar(truncated_tar=True))


def test_missing_tar_end_marker_is_rejected() -> None:
    with pytest.raises(SourcePreparationError, match="source_fetch_archive_invalid"):
        _parse_github_archive(_github_tar(truncated_end=True))


def test_compressed_response_limit_is_checked_before_body_read(monkeypatch) -> None:
    import continuum_api.runtime.github_source_fetcher as source_fetcher

    monkeypatch.setattr(source_fetcher, "MAX_COMPRESSED_BYTES", 8)
    fetcher, _, _ = _fetcher(
        lambda request: httpx.Response(302, headers={"location": _LOCATION}, request=request),
        lambda request: httpx.Response(200, content=b"x" * 9, request=request),
    )
    with pytest.raises(SourcePreparationError, match="source_fetch_compressed_size_limit"):
        asyncio.run(fetcher._fetch_archive(_facts(), SecretStr("secret-token"), _SHA))


def test_gzip_expansion_limit_is_enforced(monkeypatch) -> None:
    import continuum_api.runtime.github_source_fetcher as source_fetcher

    monkeypatch.setattr(source_fetcher, "MAX_TAR_BYTES", 16)
    with pytest.raises(SourcePreparationError, match="source_fetch_tar_size_limit"):
        _parse_github_archive(gzip.compress(b"x" * 17))


@pytest.mark.parametrize(
    ("limit_name", "limit", "expected"),
    [
        ("MAX_ARCHIVE_ENTRIES", 1, "source_fetch_entry_limit"),
        ("MAX_FILES", 0, "source_fetch_file_limit"),
        ("MAX_SOURCE_BYTES", 1, "source_fetch_source_size_limit"),
    ],
)
def test_tar_entry_file_and_source_limits(
    monkeypatch, limit_name: str, limit: int, expected: str
) -> None:
    import continuum_api.runtime.github_source_fetcher as source_fetcher

    monkeypatch.setattr(source_fetcher, limit_name, limit)
    with pytest.raises(SourcePreparationError, match=expected):
        _parse_github_archive(_github_tar())


def test_response_content_length_over_limit_is_rejected() -> None:
    fetcher, _, _ = _fetcher(
        lambda request: httpx.Response(302, headers={"location": _LOCATION}, request=request),
        lambda request: httpx.Response(
            200,
            headers={"content-length": "999999999"},
            stream=_BytesStream(b""),
            request=request,
        ),
    )
    with pytest.raises(SourcePreparationError, match="source_fetch_compressed_size_limit"):
        asyncio.run(fetcher._fetch_archive(_facts(), SecretStr("secret-token"), _SHA))


def test_api_redirect_body_is_never_read() -> None:
    class CountingStream(httpx.AsyncByteStream):
        reads = 0

        async def __aiter__(self):
            self.reads += 1
            yield b"untrusted body"

    stream = CountingStream()
    fetcher, _, archive_requests = _fetcher(
        lambda request: httpx.Response(
            302, headers={"location": "https://attacker.example/"}, stream=stream, request=request
        ),
        lambda request: httpx.Response(200, stream=_BytesStream(_github_tar()), request=request),
    )
    with pytest.raises(SourcePreparationError, match="source_fetch_redirect_invalid"):
        asyncio.run(fetcher._fetch_archive(_facts(), SecretStr("secret-token"), _SHA))
    assert stream.reads == 0
    assert archive_requests == []


def test_transport_failure_is_sanitized_at_source_fetch_boundary(monkeypatch) -> None:
    principal = Principal(uuid4())

    class StubAuthorization:
        async def repository_read_authorization(
            self, repository_id: int, user_id: int
        ) -> tuple[RepositoryPublicationFacts, SecretStr]:
            return _facts(), SecretStr("secret-token")

    def api_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": _LOCATION}, request=request)

    def archive_handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("private-transport-detail")

    fetcher = GitHubSourceFetcher(
        create_engine("sqlite://"),
        principal,
        StubAuthorization(),  # type: ignore[arg-type]
        api_transport=httpx.MockTransport(api_handler),
        archive_transport=httpx.MockTransport(archive_handler),
    )
    monkeypatch.setattr(fetcher, "_current_github_user_id", lambda *_args: _USER_ID)
    lease = ExecutionLease(
        owner_id=principal.account_id,
        run_id=uuid4(),
        step_id=uuid4(),
        job_id=uuid4(),
        attempt_id=uuid4(),
        generation=1,
        lease_owner="fixture-worker",
        lease_expires_at=datetime.now(UTC),
    )
    receipt = EligibilityReceipt(
        repository_id=_REPOSITORY_ID,
        commit_sha=_SHA,
        tree_sha="b" * 40,
        profile_id="python-pytest",
        profile_version="1",
        file_count=0,
        source_bytes=0,
        manifest_blobs=(),
        package_count=0,
    )
    with pytest.raises(SourcePreparationError, match="source_fetch_unavailable") as error:
        asyncio.run(fetcher(lease, _REPOSITORY_ID, receipt))
    assert "private-transport-detail" not in str(error.value)


def test_stalled_authorization_hits_total_fetch_deadline(monkeypatch) -> None:
    import continuum_api.runtime.github_source_fetcher as source_fetcher

    principal = Principal(uuid4())

    class StalledAuthorization:
        async def repository_read_authorization(
            self, _repository_id: int, _user_id: int
        ) -> tuple[RepositoryPublicationFacts, SecretStr]:
            await asyncio.sleep(0.1)
            return _facts(), SecretStr("secret-token")

    fetcher = GitHubSourceFetcher(
        create_engine("sqlite://"),
        principal,
        StalledAuthorization(),  # type: ignore[arg-type]
        api_transport=httpx.MockTransport(lambda request: httpx.Response(500, request=request)),
        archive_transport=httpx.MockTransport(lambda request: httpx.Response(500, request=request)),
    )
    monkeypatch.setattr(fetcher, "_current_github_user_id", lambda *_args: _USER_ID)
    monkeypatch.setattr(source_fetcher, "SOURCE_FETCH_DEADLINE_SECONDS", 0.01)
    lease = ExecutionLease(
        owner_id=principal.account_id,
        run_id=uuid4(),
        step_id=uuid4(),
        job_id=uuid4(),
        attempt_id=uuid4(),
        generation=1,
        lease_owner="fixture-worker",
        lease_expires_at=datetime.now(UTC),
    )
    receipt = EligibilityReceipt(
        repository_id=_REPOSITORY_ID,
        commit_sha=_SHA,
        tree_sha="b" * 40,
        profile_id="python-pytest",
        profile_version="1",
        file_count=0,
        source_bytes=0,
        manifest_blobs=(),
        package_count=0,
    )
    with pytest.raises(SourcePreparationError, match="source_fetch_timeout"):
        asyncio.run(fetcher(lease, _REPOSITORY_ID, receipt))


def test_codeload_redirect_is_rejected_without_following_it() -> None:
    archive_requests: list[httpx.Request] = []

    def archive_handler(request: httpx.Request) -> httpx.Response:
        archive_requests.append(request)
        return httpx.Response(
            302, headers={"location": "https://attacker.example/"}, request=request
        )

    fetcher, _, _ = _fetcher(
        lambda request: httpx.Response(302, headers={"location": _LOCATION}, request=request),
        archive_handler,
    )
    with pytest.raises(SourcePreparationError, match="source_fetch_redirect_invalid"):
        asyncio.run(fetcher._fetch_archive(_facts(), SecretStr("secret-token"), _SHA))
    assert len(archive_requests) == 1
