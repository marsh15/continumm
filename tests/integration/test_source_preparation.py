"""PostgreSQL proof for the admitted baseline source-preparation activity."""

from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import io
import json
import tarfile
import threading
from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import text
from test_agent_loop import (
    T22Database,
    _checkpoint_service,
    _storage_fixture,
)
from test_agent_loop import t22_database as t22_database

from continuum_api.artifacts.archives import ArchiveFile
from continuum_api.artifacts.checkpoints import CheckpointStorageError
from continuum_api.repositories.preflight import EligibilityReceipt, RepositoryAccessProof
from continuum_api.runtime.approvals import RepositoryPublicationFacts
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.github_source_fetcher import GitHubSourceFetcher
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, QueueService
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RuntimeRepository
from continuum_api.runtime.source_preparation import (
    SourceFetcher,
    SourcePreparationActivityHandler,
    SourcePreparationError,
)
from continuum_api.sandbox.node_profile import _git_blob_sha as node_blob_sha
from continuum_api.sandbox.node_profile import _git_tree_sha as node_tree_sha
from continuum_api.sandbox.python_profile import _git_blob_sha as python_blob_sha
from continuum_api.sandbox.python_profile import _git_tree_sha as python_tree_sha


def _github_tarball(files: tuple[ArchiveFile, ...]) -> bytes:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as archive:
        root = tarfile.TarInfo("fixture-root/")
        root.type = tarfile.DIRTYPE
        root.mode = 0o775
        archive.addfile(root)
        for source_file in files:
            member = tarfile.TarInfo(f"fixture-root/{source_file.path}")
            member.mode = 0o664 if source_file.mode == 0o644 else 0o775
            member.size = len(source_file.content)
            archive.addfile(member, io.BytesIO(source_file.content))
    return gzip.compress(stream.getvalue())


_REPOSITORY_ID = 714_022
_NODE_INTEGRITY = "sha512-" + base64.b64encode(bytes(64)).decode("ascii")
_PYTHON_FILES = (
    ArchiveFile("pyproject.toml", b"[tool.pytest.ini_options]\ntestpaths = ['tests']\n"),
    ArchiveFile("requirements.lock", b"pytest==8.3.5 --hash=sha256:" + b"a" * 64 + b"\n"),
    ArchiveFile("src/app.py", b"def answer():\n    return 42\n"),
    ArchiveFile("tests/test_app.py", b"from src.app import answer\nassert answer() == 42\n"),
)
_NODE_FILES = (
    ArchiveFile(
        "package.json",
        json.dumps(
            {
                "name": "source-prep-fixture",
                "version": "1.0.0",
                "packageManager": "npm@11.19.0",
                "scripts": {"test": "node --test"},
                "dependencies": {"is-number": "7.0.0"},
            },
            sort_keys=True,
        ).encode(),
    ),
    ArchiveFile(
        "package-lock.json",
        json.dumps(
            {
                "name": "source-prep-fixture",
                "version": "1.0.0",
                "lockfileVersion": 3,
                "requires": True,
                "packages": {
                    "": {
                        "name": "source-prep-fixture",
                        "version": "1.0.0",
                        "dependencies": {"is-number": "7.0.0"},
                    },
                    "node_modules/is-number": {
                        "version": "7.0.0",
                        "resolved": "https://registry.npmjs.org/is-number/-/is-number-7.0.0.tgz",
                        "integrity": _NODE_INTEGRITY,
                    },
                },
            },
            sort_keys=True,
        ).encode(),
    ),
    ArchiveFile("src/index.js", b"export const answer = 42;\n"),
)


def _receipt(files: tuple[ArchiveFile, ...], profile: str) -> EligibilityReceipt:
    if profile == "python-pytest":
        tree_sha = python_tree_sha(files, "sha1")
        blob_sha = python_blob_sha
        manifest_paths = ("pyproject.toml", "requirements.lock")
    else:
        tree_sha = node_tree_sha(files, "sha1")
        blob_sha = node_blob_sha
        manifest_paths = ("package.json", "package-lock.json")
    return EligibilityReceipt(
        repository_id=_REPOSITORY_ID,
        commit_sha="c" * 40,
        tree_sha=tree_sha,
        profile_id=profile,
        profile_version="1",
        file_count=len(files),
        source_bytes=sum(len(item.content) for item in files),
        manifest_blobs=tuple(
            (path, blob_sha(next(item.content for item in files if item.path == path), "sha1"))
            for path in manifest_paths
        ),
        package_count=1,
    )


def _start_source_run(
    store: T22Database,
    files: tuple[ArchiveFile, ...],
    profile_id: str,
    receipt: EligibilityReceipt,
) -> tuple[Principal, ExecutionLease]:
    owner_id = uuid4()
    principal = Principal(owner_id)
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) VALUES (:id, :github)"
            ),
            {"id": owner_id, "github": 990_000_000 + owner_id.int % 1_000_000},
        )
        registration_id: int = connection.execute(
            text(
                "INSERT INTO runtime_private.repository_registrations "
                "(owner_id, github_repository_id, repository_owner, repository_name, "
                "profile_id, profile_version, provider, eligibility_status, checked_sha, "
                "eligibility_checked_at, eligibility_receipt) VALUES "
                "(:owner, :repo, 'fixture-owner', 'fixture-repository', :profile, '1', "
                "'openai', 'eligible', :sha, clock_timestamp(), CAST(:receipt AS jsonb)) "
                "RETURNING registration_id"
            ),
            {
                "owner": owner_id,
                "repo": _REPOSITORY_ID,
                "profile": profile_id,
                "sha": receipt.commit_sha,
                "receipt": json.dumps(receipt.to_cache_payload()),
            },
        ).scalar_one()
    with store.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run(github_repository_id=_REPOSITORY_ID, issue_number=1)
        step_id = repository.create_step(run_id, "admission.source", "source_preparation")
        repository.create_job(run_id, step_id, replay_policy=ReplayPolicy.ATTENTION_ON_UNKNOWN)
    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, principal)
        connection.execute(
            text(
                "INSERT INTO runtime_private.run_admission_pins "
                "(owner_id, run_id, registration_id, defaults_version, profile_id, "
                "profile_version, base_branch, source_sha, source_tree_sha, source_receipt, "
                "provider, model_id, task_context, submission_digest) VALUES "
                "(:owner, :run, :registration, 1, :profile, '1', 'main', :sha, :tree, "
                "CAST(:receipt AS jsonb), 'openai', 'gpt-5.4-mini-2026-03-17', "
                "CAST(:context AS jsonb), :digest)"
            ),
            {
                "owner": owner_id,
                "run": run_id,
                "registration": registration_id,
                "profile": profile_id,
                "sha": receipt.commit_sha,
                "tree": receipt.tree_sha,
                "receipt": json.dumps(receipt.to_cache_payload()),
                "context": json.dumps({"task": "Implement the requested change."}),
                "digest": hashlib.sha256(str(run_id).encode()).digest(),
            },
        )
    lease = QueueService(store.runtime_engine, principal).claim_one("source-preparation-test")
    assert lease is not None and lease.run_id == run_id
    return principal, lease


@pytest.mark.parametrize(
    ("files", "profile"),
    [(_PYTHON_FILES, "python-pytest"), (_NODE_FILES, "node-npm")],
)
def test_source_preparation_accepts_verified_root_and_queues_one_agent_turn(
    t22_database: T22Database,
    tmp_path: Path,
    files: tuple[ArchiveFile, ...],
    profile: str,
) -> None:
    receipt = _receipt(files, profile)
    principal, lease = _start_source_run(t22_database, files, profile, receipt)
    storage, requests = _storage_fixture(tmp_path / str(lease.run_id))
    persistence = ArtifactPersistence(t22_database.runtime_engine)
    checkpoints = _checkpoint_service(t22_database.runtime_engine, principal, persistence, storage)
    fetches: list[tuple[int, str]] = []
    fetch_threads: list[int] = []
    calling_thread = threading.get_ident()

    def sync_fetch(
        _lease: ExecutionLease, repository_id: int, requested: EligibilityReceipt
    ) -> Iterable[ArchiveFile]:
        fetch_threads.append(threading.get_ident())
        fetches.append((repository_id, requested.commit_sha))
        return files

    async def async_fetch(
        _lease: ExecutionLease, repository_id: int, requested: EligibilityReceipt
    ) -> Iterable[ArchiveFile]:
        await asyncio.sleep(0)
        fetches.append((repository_id, requested.commit_sha))
        return files

    fetch: SourceFetcher = async_fetch if profile == "node-npm" else sync_fetch

    handler = SourcePreparationActivityHandler(
        t22_database.runtime_engine, principal, checkpoints, fetch
    )
    try:
        result = asyncio.run(handler(lease))
        assert result.artifact_id is not None
        assert fetches == [(_REPOSITORY_ID, receipt.commit_sha)]
        if profile == "python-pytest":
            assert fetch_threads != [calling_thread]
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            accepted: list[UUID | None] = (
                connection.execute(
                    text(
                        "SELECT artifact_id FROM runtime_private.accepted_results "
                        "WHERE owner_id=:owner AND run_id=:run ORDER BY event_sequence"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .scalars()
                .all()
            )
            queued = (
                connection.execute(
                    text(
                        "SELECT step.activity, job.replay_policy "
                        "FROM runtime_private.steps AS step "
                        "JOIN runtime_private.jobs AS job USING (owner_id, run_id, step_id) "
                        "WHERE step.owner_id=:owner AND step.run_id=:run "
                        "AND step.activity='agent_turn'"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .all()
            )
        assert accepted == [result.artifact_id]
        assert len(queued) == 1
        assert queued[0]["replay_policy"] == ReplayPolicy.REPLAY_SAFE.value
        assert len([request for request in requests if request.method == "POST"]) == 1
    finally:
        asyncio.run(storage.aclose())


def test_source_pin_mismatch_rejects_before_fetch_or_storage(
    t22_database: T22Database, tmp_path: Path
) -> None:
    receipt = _receipt(_PYTHON_FILES, "python-pytest")
    principal, lease = _start_source_run(t22_database, _PYTHON_FILES, "python-pytest", receipt)
    with t22_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.run_admission_pins SET source_tree_sha=:tree "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"tree": "d" * 40, "owner": lease.owner_id, "run": lease.run_id},
        )
    storage, requests = _storage_fixture(tmp_path / str(lease.run_id))
    checkpoints = _checkpoint_service(
        t22_database.runtime_engine,
        principal,
        ArtifactPersistence(t22_database.runtime_engine),
        storage,
    )
    fetched = False

    def fetch(*_args: object) -> Iterable[ArchiveFile]:
        nonlocal fetched
        fetched = True
        return _PYTHON_FILES

    handler = SourcePreparationActivityHandler(
        t22_database.runtime_engine, principal, checkpoints, fetch
    )
    try:
        with pytest.raises(
            SourcePreparationError, match="source_preparation_admission_pin_invalid"
        ):
            asyncio.run(handler(lease))
        assert fetched is False
        assert requests == []
    finally:
        asyncio.run(storage.aclose())


@pytest.mark.parametrize("expire_during_fetch", [False, True])
def test_source_preparation_stale_lease_never_uploads(
    t22_database: T22Database, tmp_path: Path, expire_during_fetch: bool
) -> None:
    receipt = _receipt(_PYTHON_FILES, "python-pytest")
    principal, lease = _start_source_run(t22_database, _PYTHON_FILES, "python-pytest", receipt)
    storage, requests = _storage_fixture(tmp_path / str(lease.run_id))
    checkpoints = _checkpoint_service(
        t22_database.runtime_engine,
        principal,
        ArtifactPersistence(t22_database.runtime_engine),
        storage,
    )

    fetched = False

    def expire_lease() -> None:
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            connection.execute(
                text(
                    "UPDATE runtime_private.jobs SET lease_expires_at=clock_timestamp() "
                    "WHERE owner_id=:owner AND run_id=:run AND job_id=:job"
                ),
                {"owner": lease.owner_id, "run": lease.run_id, "job": lease.job_id},
            )

    if not expire_during_fetch:
        expire_lease()

    def fetch(*_args: object) -> Iterable[ArchiveFile]:
        nonlocal fetched
        fetched = True
        if expire_during_fetch:
            expire_lease()
        return _PYTHON_FILES

    handler = SourcePreparationActivityHandler(
        t22_database.runtime_engine, principal, checkpoints, fetch
    )
    try:
        expected_error = CheckpointStorageError if expire_during_fetch else LeaseLost
        with pytest.raises(expected_error) as error:
            asyncio.run(handler(lease))
        if expire_during_fetch:
            assert isinstance(error.value, CheckpointStorageError)
            assert error.value.code == "artifact_lease_lost"
        assert fetched is expire_during_fetch
        assert requests == []
    finally:
        asyncio.run(storage.aclose())


def test_source_byte_mismatch_rejects_before_checkpoint_upload(
    t22_database: T22Database, tmp_path: Path
) -> None:
    receipt = _receipt(_PYTHON_FILES, "python-pytest")
    principal, lease = _start_source_run(t22_database, _PYTHON_FILES, "python-pytest", receipt)
    storage, requests = _storage_fixture(tmp_path / str(lease.run_id))
    checkpoints = _checkpoint_service(
        t22_database.runtime_engine,
        principal,
        ArtifactPersistence(t22_database.runtime_engine),
        storage,
    )
    changed_files = (
        *_PYTHON_FILES[:-1],
        ArchiveFile(
            "tests/test_app.py",
            b"from src.app import answer\nassert answer() == 43\n",
        ),
    )

    handler = SourcePreparationActivityHandler(
        t22_database.runtime_engine,
        principal,
        checkpoints,
        lambda *_args: changed_files,
    )
    try:
        with pytest.raises(SourcePreparationError, match="python_profile_source_tree_mismatch"):
            asyncio.run(handler(lease))
        assert requests == []
    finally:
        asyncio.run(storage.aclose())


def test_other_owner_lease_rejects_before_fetch(t22_database: T22Database, tmp_path: Path) -> None:
    receipt = _receipt(_PYTHON_FILES, "python-pytest")
    principal, lease = _start_source_run(t22_database, _PYTHON_FILES, "python-pytest", receipt)
    storage, requests = _storage_fixture(tmp_path / str(lease.run_id))
    checkpoints = _checkpoint_service(
        t22_database.runtime_engine,
        principal,
        ArtifactPersistence(t22_database.runtime_engine),
        storage,
    )
    fetched = False

    def fetch(*_args: object) -> Iterable[ArchiveFile]:
        nonlocal fetched
        fetched = True
        return _PYTHON_FILES

    other_principal = Principal(uuid4())
    handler = SourcePreparationActivityHandler(
        t22_database.runtime_engine, other_principal, checkpoints, fetch
    )
    try:
        with pytest.raises(SourcePreparationError, match="source_preparation_owner_mismatch"):
            asyncio.run(handler(lease))
        assert fetched is False
        assert requests == []
    finally:
        asyncio.run(storage.aclose())


def test_fetch_error_is_sanitized_and_never_uploads(
    t22_database: T22Database, tmp_path: Path
) -> None:
    receipt = _receipt(_PYTHON_FILES, "python-pytest")
    principal, lease = _start_source_run(t22_database, _PYTHON_FILES, "python-pytest", receipt)
    storage, requests = _storage_fixture(tmp_path / str(lease.run_id))
    checkpoints = _checkpoint_service(
        t22_database.runtime_engine,
        principal,
        ArtifactPersistence(t22_database.runtime_engine),
        storage,
    )

    def fetch(*_args: object) -> Iterable[ArchiveFile]:
        raise RuntimeError("private-token-value")

    handler = SourcePreparationActivityHandler(
        t22_database.runtime_engine, principal, checkpoints, fetch
    )
    try:
        with pytest.raises(SourcePreparationError, match="source_fetch_unavailable") as error:
            asyncio.run(handler(lease))
        assert "private-token-value" not in str(error.value)
        assert requests == []
    finally:
        asyncio.run(storage.aclose())


def test_github_source_fetcher_reads_only_the_exact_live_admission(
    t22_database: T22Database,
) -> None:
    receipt = _receipt(_PYTHON_FILES, "python-pytest")
    principal, lease = _start_source_run(t22_database, _PYTHON_FILES, "python-pytest", receipt)
    github_user_id = 990_000_000 + principal.account_id.int % 1_000_000
    authorizations: list[tuple[int, int]] = []

    class StubAuthorization:
        async def repository_read_authorization(
            self, repository_id: int, user_id: int
        ) -> tuple[RepositoryPublicationFacts, SecretStr]:
            authorizations.append((repository_id, user_id))
            proof = RepositoryAccessProof(repository_id, user_id, True, True)
            return (
                RepositoryPublicationFacts(
                    repository_id=repository_id,
                    github_user_id=user_id,
                    owner="fixture-owner",
                    name="fixture-repository",
                    default_branch="main",
                    base_commit_sha=receipt.commit_sha,
                    access_proof=proof,
                ),
                SecretStr("fixture-installation-token"),
            )

    requests: list[httpx.Request] = []

    def api_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            302,
            headers={
                "location": (
                    "https://codeload.github.com/fixture-owner/fixture-repository/"
                    f"legacy.tar.gz/{receipt.commit_sha}"
                )
            },
            request=request,
        )

    def archive_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(503, request=request)

    fetcher = GitHubSourceFetcher(
        t22_database.runtime_engine,
        principal,
        StubAuthorization(),  # type: ignore[arg-type]
        api_transport=httpx.MockTransport(api_handler),
        archive_transport=httpx.MockTransport(archive_handler),
    )
    changed_receipt = EligibilityReceipt(
        repository_id=receipt.repository_id,
        commit_sha="d" * 40,
        tree_sha=receipt.tree_sha,
        profile_id=receipt.profile_id,
        profile_version=receipt.profile_version,
        file_count=receipt.file_count,
        source_bytes=receipt.source_bytes,
        manifest_blobs=receipt.manifest_blobs,
        package_count=receipt.package_count,
    )

    async def exercise() -> None:
        stale_lease = replace(lease, generation=lease.generation + 1)
        with pytest.raises(LeaseLost):
            await fetcher(stale_lease, _REPOSITORY_ID, receipt)
        assert authorizations == []
        with pytest.raises(
            SourcePreparationError,
            match="source_preparation_admission_pin_invalid",
        ):
            await fetcher(lease, _REPOSITORY_ID, changed_receipt)
        assert authorizations == []
        with pytest.raises(
            SourcePreparationError,
            match="source_fetch_download_failed",
        ):
            await fetcher(lease, _REPOSITORY_ID, receipt)
        assert authorizations == [(_REPOSITORY_ID, github_user_id)]

    asyncio.run(exercise())
    assert len(requests) == 2
    assert requests[0].url.host == "api.github.com"
    assert requests[0].headers["authorization"] == "Bearer fixture-installation-token"
    assert requests[1].url.host == "codeload.github.com"
    assert "authorization" not in requests[1].headers


def test_github_source_fetcher_completes_admitted_source_preparation(
    t22_database: T22Database, tmp_path: Path
) -> None:
    receipt = _receipt(_PYTHON_FILES, "python-pytest")
    principal, lease = _start_source_run(t22_database, _PYTHON_FILES, "python-pytest", receipt)

    class StubAuthorization:
        async def repository_read_authorization(
            self, repository_id: int, user_id: int
        ) -> tuple[RepositoryPublicationFacts, SecretStr]:
            proof = RepositoryAccessProof(repository_id, user_id, True, True)
            return (
                RepositoryPublicationFacts(
                    repository_id=repository_id,
                    github_user_id=user_id,
                    owner="fixture-owner",
                    name="fixture-repository",
                    default_branch="main",
                    base_commit_sha=receipt.commit_sha,
                    access_proof=proof,
                ),
                SecretStr("fixture-installation-token"),
            )

    def api_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith(f"/tarball/{receipt.commit_sha}")
        assert request.headers["authorization"] == "Bearer fixture-installation-token"
        return httpx.Response(
            302,
            headers={
                "location": (
                    "https://codeload.github.com/fixture-owner/fixture-repository/"
                    f"legacy.tar.gz/{receipt.commit_sha}"
                )
            },
            request=request,
        )

    def archive_handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers
        return httpx.Response(
            200, stream=_IntegrationBytesStream(_github_tarball(_PYTHON_FILES)), request=request
        )

    fetcher = GitHubSourceFetcher(
        t22_database.runtime_engine,
        principal,
        StubAuthorization(),  # type: ignore[arg-type]
        api_transport=httpx.MockTransport(api_handler),
        archive_transport=httpx.MockTransport(archive_handler),
    )
    storage, requests = _storage_fixture(tmp_path / str(lease.run_id))
    checkpoints = _checkpoint_service(
        t22_database.runtime_engine,
        principal,
        ArtifactPersistence(t22_database.runtime_engine),
        storage,
    )

    try:
        result = asyncio.run(
            SourcePreparationActivityHandler(
                t22_database.runtime_engine, principal, checkpoints, fetcher
            )(lease)
        )
        assert result.artifact_id is not None
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            accepted = connection.execute(
                text(
                    "SELECT artifact_id FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one()
            agent_turns = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.steps "
                    "WHERE owner_id=:owner AND run_id=:run AND activity='agent_turn'"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one()
        assert accepted == result.artifact_id
        assert agent_turns == 1
        assert len([request for request in requests if request.method == "POST"]) == 1
    finally:
        asyncio.run(storage.aclose())


class _IntegrationBytesStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes) -> None:
        self._content = content

    async def __aiter__(self):
        yield self._content
