"""PG17 proof for checkpoint quota, upload reconciliation, and fenced commit."""

from __future__ import annotations

import asyncio
import hashlib
import os
import secrets
import subprocess
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, Connection, make_url
from sqlalchemy.exc import IntegrityError

from continuum_api.artifacts.archives import (
    MAX_COMPRESSED_BYTES,
    ArchiveBundle,
    ArchiveFile,
    create_archive,
)
from continuum_api.artifacts.checkpoints import CheckpointStorageError, CheckpointStorageService
from continuum_api.connectivity.storage import SupabaseStorage
from continuum_api.repositories.preflight import (
    GitHubRepositoryAdapter,
    RepositoryAccessProof,
    RepositoryMetadata,
)
from continuum_api.runtime.artifact_persistence import (
    ArtifactCapacityExceeded,
    ArtifactPersistence,
    ArtifactUploadReservation,
)
from continuum_api.runtime.leases import ExecutionLease, QueueService
from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository
from continuum_api.runtime.run_queries import event_page_response, read_event_page, read_run
from continuum_api.runtime.scheduler import RuntimeRecoveryService
from continuum_api.runtime.transitions import NextWork, TransitionService

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t15_test"
LATEST_REVISION = "20261012_09"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)
STORAGE_URL = "https://fixture-project.supabase.co"
STORAGE_SECRET = "sb_secret_t15_local_fixture_only"
STORAGE_BUCKET = "continuum-checkpoints"
REPOSITORY_ID = 714_001


def test_checkpoint_uploads_are_budgeted_fenced_and_recoverable() -> None:
    database_url = os.environ.get("CONTINUUM_T15_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T15_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T15 integration requires loopback PostgreSQL named continuum_t15_test")

    admin_engine = create_engine(
        url.set(drivername="postgresql+psycopg"), hide_parameters=True, pool_size=8, max_overflow=0
    )
    runtime_password = secrets.token_urlsafe(32)
    runtime_engine: Engine | None = None
    migration_applied = False
    try:
        _reset_database(admin_engine, runtime_password)
        _assert_postgres_17(admin_engine)
        _run_migration("upgrade", LATEST_REVISION, database_url)
        migration_applied = True
        runtime_engine = create_engine(
            _role_url(url, runtime_password), hide_parameters=True, pool_size=8, max_overflow=0
        )
        owners = _seed_users(admin_engine, 8)
        asyncio.run(_exercise_checkpoint_scenarios(admin_engine, runtime_engine, owners))
        _exercise_concurrent_capacity_reservations(
            admin_engine, runtime_engine, ArtifactPersistence(runtime_engine), owners[6], owners[7]
        )
        _assert_artifact_rls(runtime_engine, owners[0], owners[1])
        _assert_capacity_ledger_matches_uploads(admin_engine)

        runtime_engine.dispose()
        runtime_engine = None
        _assert_legacy_transition_after_artifact_migration_down(
            admin_engine, url, runtime_password, database_url
        )
        _run_migration("downgrade", "base", database_url)
        migration_applied = False
        _assert_schema_absent(admin_engine, "runtime_private")
    finally:
        if runtime_engine is not None:
            runtime_engine.dispose()
        try:
            if migration_applied:
                _run_migration("downgrade", "base", database_url)
        finally:
            _cleanup_database(admin_engine)
            admin_engine.dispose()


async def _exercise_checkpoint_scenarios(
    admin_engine: Engine, runtime_engine: Engine, owners: tuple[UUID, ...]
) -> None:
    persistence = ArtifactPersistence(runtime_engine)
    main_owner = owners[0]
    principal = Principal(main_owner)
    main_run, _, _ = _seed_work(runtime_engine, principal, "prepare:source")
    queue = QueueService(runtime_engine, principal)
    first_lease = queue.claim_one("checkpoint-worker-1")
    assert first_lease is not None
    files_one = (ArchiveFile("src/main.py", b"print('one')\n"),)
    bundle_one = create_archive(files_one)
    success_storage, success_requests, objects = _storage_fixture()
    access_verifier = _AccessVerifier()
    service = _checkpoint_service(
        runtime_engine, persistence, principal, success_storage, access_verifier
    )
    receipt_one = await service.commit_checkpoint(
        principal,
        first_lease,
        "tool:edit-one",
        files_one,
        result={"updated": ["src/main.py"]},
        target_run_status=RunStatus.RUNNING,
        next_work=NextWork("validate:one", "validate"),
    )
    assert receipt_one.uploaded and not receipt_one.adopted
    assert receipt_one.archive_sha256 == bundle_one.archive_sha256
    assert receipt_one.tree_sha256 == bundle_one.manifest.tree_sha256
    assert receipt_one.transition.artifact_id == receipt_one.artifact_id
    _assert_committed_artifact(admin_engine, receipt_one.artifact_id, bundle_one)

    second_lease = queue.claim_one("checkpoint-worker-2")
    assert second_lease is not None
    restored = await service.restore_archive(principal, second_lease, receipt_one.artifact_id)
    assert restored.manifest == bundle_one.manifest
    assert restored.archive_sha256 == bundle_one.archive_sha256
    assert restored.files == files_one

    files_two = (ArchiveFile("src/main.py", b"print('two')\n", mode=0o755),)
    bundle_two = create_archive(files_two)
    receipt_two = await service.commit_checkpoint(
        principal,
        second_lease,
        "tool:edit-two",
        files_two,
        result={"updated": ["src/main.py"], "revision": 2},
        target_run_status=RunStatus.RUNNING,
        next_work=NextWork("review:two", "review"),
    )
    assert receipt_two.uploaded and not receipt_two.adopted
    assert receipt_two.transition.artifact_id == receipt_two.artifact_id
    _assert_committed_artifact(
        admin_engine,
        receipt_two.artifact_id,
        bundle_two,
        parent_artifact_id=receipt_one.artifact_id,
    )
    with admin_engine.connect() as connection:
        parent_id: UUID | None = connection.execute(
            text(
                "SELECT parent_upload_id FROM runtime_private.artifact_uploads "
                "WHERE upload_id=:upload"
            ),
            {"upload": receipt_two.artifact_id},
        ).scalar_one()
    assert parent_id == receipt_one.artifact_id

    # A lost acknowledgment replay adopts the accepted result without another
    # POST or a new capacity charge, even though the attempt has finished.
    post_count = sum(request.method == "POST" for request in success_requests)
    charged_before_replay = _charged_bytes(admin_engine)
    replay = await service.commit_checkpoint(
        principal,
        second_lease,
        "tool:edit-two",
        files_two,
        result={"updated": ["src/main.py"], "revision": 2},
        target_run_status=RunStatus.RUNNING,
        next_work=NextWork("review:two", "review"),
    )
    assert replay.adopted and not replay.uploaded
    assert replay.artifact_id == receipt_two.artifact_id
    assert sum(request.method == "POST" for request in success_requests) == post_count
    assert _charged_bytes(admin_engine) == charged_before_replay

    third_lease = queue.claim_one("checkpoint-worker-3")
    assert third_lease is not None
    restored_two = await service.restore_archive(principal, third_lease, receipt_two.artifact_id)
    assert restored_two.files == files_two
    assert restored_two.manifest == bundle_two.manifest
    objects[receipt_one.object_key] = b"corrupt archive bytes"
    with pytest.raises(CheckpointStorageError, match="corrupt_artifact"):
        await service.restore_archive(principal, third_lease, receipt_one.artifact_id)
    _assert_corrupt_artifact_attention(admin_engine, main_owner, main_run, third_lease)
    visible_run = read_run(runtime_engine, principal, main_run)
    assert visible_run is not None and visible_run.status is RunStatus.NEEDS_ATTENTION
    visible_page = event_page_response(
        main_run,
        0,
        read_event_page(runtime_engine, principal, main_run, after_sequence=0, limit=100),
    )
    assert visible_page.events[-1].type == "job.needs_attention"
    assert visible_page.events[-1].data.failure_class == "corrupt_artifact"

    await _exercise_failed_transition_rollback(admin_engine, runtime_engine, persistence, owners[5])
    await _exercise_unknown_upload_replay(admin_engine, runtime_engine, persistence, owners[1])
    await _exercise_stale_uploader_orphan(admin_engine, runtime_engine, persistence, owners[2])
    await _exercise_known_absence_refund(admin_engine, runtime_engine, persistence, owners[4])
    await _exercise_capacity_rejection(admin_engine, runtime_engine, persistence, owners[3])

    await success_storage.aclose()


async def _exercise_failed_transition_rollback(
    admin_engine: Engine,
    runtime_engine: Engine,
    persistence: ArtifactPersistence,
    owner_id: UUID,
) -> None:
    principal = Principal(owner_id)
    run_id, _, _ = _seed_work(runtime_engine, principal, "source:rollback")
    lease = QueueService(runtime_engine, principal).claim_one("rollback-worker")
    assert lease is not None
    storage, requests, _ = _storage_fixture()
    service = _checkpoint_service(
        runtime_engine, persistence, principal, storage, _AccessVerifier()
    )
    files = (ArchiveFile("src/rollback.py", b"rollback proof\n"),)
    before_events = _run_event_count(admin_engine, principal, run_id)
    charge_before_upload = _charged_bytes(runtime_engine)
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "ALTER TABLE runtime_private.accepted_results "
                "ADD CONSTRAINT ck_t15_force_accepted_result_rollback "
                "CHECK (artifact_id IS NULL) NOT VALID"
            )
        )
    try:
        with pytest.raises(IntegrityError):
            await service.commit_checkpoint(
                principal,
                lease,
                "tool:rollback",
                files,
                result={"changed": True},
                target_run_status=RunStatus.RUNNING,
                next_work=NextWork("after-rollback", "validate"),
            )
    finally:
        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "ALTER TABLE runtime_private.accepted_results "
                    "DROP CONSTRAINT ck_t15_force_accepted_result_rollback"
                )
            )
    assert _run_event_count(admin_engine, principal, run_id) == before_events
    assert _accepted_result_count(admin_engine, principal, run_id) == 0
    assert _charged_bytes(runtime_engine) > charge_before_upload
    with admin_engine.connect() as connection:
        status: str = connection.execute(
            text(
                "SELECT status FROM runtime_private.artifact_uploads "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
    assert status == "orphan"
    assert requests
    await storage.aclose()


async def _exercise_unknown_upload_replay(
    admin_engine: Engine,
    runtime_engine: Engine,
    persistence: ArtifactPersistence,
    owner_id: UUID,
) -> None:
    principal = Principal(owner_id)
    _, _, _ = _seed_work(runtime_engine, principal, "source:unknown")
    lease = QueueService(runtime_engine, principal).claim_one("unknown-worker")
    assert lease is not None
    storage, requests, _ = _storage_fixture(upload_status=500)
    service = _checkpoint_service(
        runtime_engine, persistence, principal, storage, _AccessVerifier()
    )
    files = (ArchiveFile("src/unknown.py", b"unknown upload\n"),)
    before = _charged_bytes(runtime_engine)
    for _ in range(2):
        with pytest.raises(CheckpointStorageError, match="artifact_upload_outcome_unknown"):
            await service.commit_checkpoint(
                principal,
                lease,
                "tool:unknown",
                files,
                result={"changed": True},
                target_run_status=RunStatus.WAITING_APPROVAL,
            )
    assert sum(request.method == "POST" for request in requests) == 1
    assert (
        sum(
            request.method == "GET" and "/object/authenticated/" in request.url.path
            for request in requests
        )
        == 2
    )
    assert _charged_bytes(runtime_engine) > before
    with admin_engine.begin() as connection:
        status: str = connection.execute(
            text(
                "SELECT status FROM runtime_private.artifact_uploads "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": lease.run_id},
        ).scalar_one()
    assert status == "unknown"
    assert _accepted_result_count(admin_engine, principal, lease.run_id) == 0
    await storage.aclose()


async def _exercise_stale_uploader_orphan(
    admin_engine: Engine,
    runtime_engine: Engine,
    persistence: ArtifactPersistence,
    owner_id: UUID,
) -> None:
    principal = Principal(owner_id)
    _, _, _ = _seed_work(runtime_engine, principal, "source:stale")
    lease = QueueService(runtime_engine, principal).claim_one("stale-worker")
    assert lease is not None
    files = (ArchiveFile("src/stale.py", b"stale uploader\n"),)
    expected_bundle = create_archive(files)

    def age_reservation_and_expire_lease(_upload_id: UUID) -> None:
        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.artifact_uploads "
                    "SET created_at=clock_timestamp() - interval '2 hours', "
                    "orphan_grace_until=clock_timestamp() - interval '1 hour', "
                    "content_expires_at=clock_timestamp() + interval '12 days' "
                    "WHERE owner_id=:owner AND run_id=:run AND status='uploading'"
                ),
                {"owner": owner_id, "run": lease.run_id},
            )
            connection.execute(
                text(
                    "UPDATE runtime_private.jobs SET lease_expires_at=clock_timestamp() "
                    "- interval '1 second' WHERE owner_id=:owner AND run_id=:run "
                    "AND job_id=:job"
                ),
                {"owner": owner_id, "run": lease.run_id, "job": lease.job_id},
            )

    storage, requests, _ = _storage_fixture(on_download=age_reservation_and_expire_lease)
    service = _checkpoint_service(
        runtime_engine, persistence, principal, storage, _AccessVerifier()
    )
    with pytest.raises(CheckpointStorageError, match="artifact_lease_lost"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:stale",
            files,
            result={"changed": True},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )
    assert sum(request.method == "POST" for request in requests) == 1
    with admin_engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT status, created_at, orphaned_at, orphan_grace_until "
                "FROM runtime_private.artifact_uploads WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": lease.run_id},
        ).one()
        db_now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
        artifact_id: UUID = connection.execute(
            text(
                "SELECT upload_id FROM runtime_private.artifact_uploads "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": lease.run_id},
        ).scalar_one()
        capacity_before_replay: int = connection.execute(
            text("SELECT charged_bytes FROM runtime_private.artifact_capacity WHERE capacity_id=1")
        ).scalar_one()
    assert row.status == "orphan"
    assert row.orphaned_at > row.created_at + timedelta(hours=1)
    assert row.orphan_grace_until > db_now
    await asyncio.to_thread(persistence.mark_orphan, principal, lease, artifact_id)
    with admin_engine.connect() as connection:
        grace_after_replay: datetime = connection.execute(
            text(
                "SELECT orphan_grace_until FROM runtime_private.artifact_uploads "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": lease.run_id},
        ).scalar_one()
        capacity_after_replay: int = connection.execute(
            text("SELECT charged_bytes FROM runtime_private.artifact_capacity WHERE capacity_id=1")
        ).scalar_one()
    assert grace_after_replay == row.orphan_grace_until
    assert capacity_after_replay == capacity_before_replay
    assert _accepted_result_count(admin_engine, principal, lease.run_id) == 0
    assert expected_bundle.archive
    await storage.aclose()


async def _exercise_known_absence_refund(
    admin_engine: Engine,
    runtime_engine: Engine,
    persistence: ArtifactPersistence,
    owner_id: UUID,
) -> None:
    principal = Principal(owner_id)
    _, _, _ = _seed_work(runtime_engine, principal, "source:known-failure")
    lease = QueueService(runtime_engine, principal).claim_one("known-failure-worker")
    assert lease is not None
    storage, requests, _ = _storage_fixture(upload_status=400)
    service = _checkpoint_service(
        runtime_engine, persistence, principal, storage, _AccessVerifier()
    )
    before = _charged_bytes(runtime_engine)
    with pytest.raises(CheckpointStorageError, match="artifact_upload_rejected"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:known-failure",
            (ArchiveFile("src/fail.py", b"known failed upload\n"),),
            result={"changed": True},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )
    assert _charged_bytes(runtime_engine) == before
    assert sum(request.method == "POST" for request in requests) == 1
    with admin_engine.begin() as connection:
        status: str = connection.execute(
            text(
                "SELECT status FROM runtime_private.artifact_uploads "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner_id, "run": lease.run_id},
        ).scalar_one()
    assert status == "released"
    await storage.aclose()


async def _exercise_capacity_rejection(
    admin_engine: Engine,
    runtime_engine: Engine,
    persistence: ArtifactPersistence,
    owner_id: UUID,
) -> None:
    principal = Principal(owner_id)
    _, _, _ = _seed_work(runtime_engine, principal, "source:capacity")
    lease = QueueService(runtime_engine, principal).claim_one("capacity-worker")
    assert lease is not None
    files = (ArchiveFile("src/capacity.py", b"capacity check\n"),)
    size = len(create_archive(files).archive)
    storage, requests, _ = _storage_fixture()
    service = _checkpoint_service(
        runtime_engine, persistence, principal, storage, _AccessVerifier()
    )
    with admin_engine.begin() as connection:
        prior_charge: int = connection.execute(
            text("SELECT charged_bytes FROM runtime_private.artifact_capacity WHERE capacity_id=1")
        ).scalar_one()
        limit: int = connection.execute(
            text(
                "SELECT capacity_limit_bytes FROM runtime_private.artifact_capacity "
                "WHERE capacity_id=1"
            )
        ).scalar_one()
        connection.execute(
            text(
                "UPDATE runtime_private.artifact_capacity SET charged_bytes=:bytes "
                "WHERE capacity_id=1"
            ),
            {"bytes": limit - size + 1},
        )
    try:
        with pytest.raises(CheckpointStorageError, match="artifact_capacity_exceeded"):
            await service.commit_checkpoint(
                principal,
                lease,
                "tool:capacity",
                files,
                result={"changed": True},
                target_run_status=RunStatus.WAITING_APPROVAL,
            )
        assert requests == []
    finally:
        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.artifact_capacity SET charged_bytes=:bytes "
                    "WHERE capacity_id=1"
                ),
                {"bytes": prior_charge},
            )
        await storage.aclose()


def _checkpoint_service(
    engine: Engine,
    persistence: ArtifactPersistence,
    principal: Principal,
    storage: SupabaseStorage,
    verifier: _AccessVerifier,
) -> CheckpointStorageService:
    return CheckpointStorageService(
        persistence,
        storage,
        TransitionService(engine, principal, artifact_persistence=persistence),
        RuntimeRecoveryService(engine, principal),
        access_verifier=verifier,
        github_adapter_factory=_PublicRepositoryAdapter,
    )


def _storage_fixture(
    *,
    upload_status: int = 200,
    on_download: Callable[[UUID], None] | None = None,
) -> tuple[SupabaseStorage, list[httpx.Request], dict[str, bytes]]:
    requests: list[httpx.Request] = []
    objects: dict[str, bytes] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET" and request.url.path.endswith(f"/bucket/{STORAGE_BUCKET}"):
            return httpx.Response(200, json={"id": STORAGE_BUCKET, "public": False})
        upload_prefix = f"/storage/v1/object/{STORAGE_BUCKET}/"
        if request.method == "POST" and request.url.path.startswith(upload_prefix):
            key = request.url.path.removeprefix(upload_prefix)
            if upload_status >= 400:
                return httpx.Response(upload_status)
            objects[key] = request.read()
            return httpx.Response(200, json={"Key": key})
        download_prefix = f"/storage/v1/object/authenticated/{STORAGE_BUCKET}/"
        if request.method == "GET" and request.url.path.startswith(download_prefix):
            key = request.url.path.removeprefix(download_prefix)
            upload_id = UUID(key.removeprefix("checkpoints/").removesuffix(".tar.gz"))
            if on_download is not None:
                on_download(upload_id)
            content = objects.get(key)
            return httpx.Response(404) if content is None else httpx.Response(200, content=content)
        raise AssertionError(f"unexpected storage request {request.method} {request.url.path}")

    storage = SupabaseStorage(
        STORAGE_URL,
        STORAGE_SECRET,
        STORAGE_BUCKET,
        transport=httpx.MockTransport(handler),
        fixture_byte_limit=MAX_COMPRESSED_BYTES,
        request_deadline_seconds=2.0,
    )
    return storage, requests, objects


class _AccessVerifier:
    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
        return RepositoryAccessProof(repository_id, github_user_id, True, True)


class _PublicRepositoryAdapter(GitHubRepositoryAdapter):
    async def current_public_repository(self, repository_id: int) -> RepositoryMetadata:
        return RepositoryMetadata(repository_id, "fixture-owner", "fixture-project", 1)

    async def aclose(self) -> None:
        return


def _seed_users(engine: Engine, count: int) -> tuple[UUID, ...]:
    owners = tuple(UUID(int=1_000_000 + index) for index in range(count))
    with engine.begin() as connection:
        for index, owner_id in enumerate(owners):
            connection.execute(
                text(
                    "INSERT INTO runtime_private.users (user_id, github_user_id) "
                    "VALUES (:id, :github_id)"
                ),
                {"id": owner_id, "github_id": 810_001 + index},
            )
    return owners


def _seed_work(
    engine: Engine,
    principal: Principal,
    logical_key: str,
    *,
    duplicate: bool = False,
) -> tuple[UUID, UUID, UUID]:
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run(github_repository_id=REPOSITORY_ID, issue_number=1)
        step_id = repository.create_step(run_id, logical_key, "edit")
        job_id = repository.create_job(run_id, step_id)
        if duplicate:
            repository.create_step(run_id, "duplicate-next", "validate")
    return run_id, step_id, job_id


def _assert_committed_artifact(
    engine: Engine,
    artifact_id: UUID,
    bundle: ArchiveBundle,
    *,
    parent_artifact_id: UUID | None = None,
) -> None:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT status, archive_sha256, tree_sha256, compressed_bytes, source_bytes, "
                "file_count, content_expires_at FROM runtime_private.artifact_uploads "
                "WHERE upload_id=:upload"
            ),
            {"upload": artifact_id},
        ).one()
        accepted: dict[str, object] = dict(
            connection.execute(
                text(
                    "SELECT artifact_id, event_sequence FROM runtime_private.accepted_results "
                    "WHERE artifact_id=:artifact"
                ),
                {"artifact": artifact_id},
            )
            .mappings()
            .one()
        )
        event_payload: dict[str, object] = connection.execute(
            text(
                "SELECT payload FROM runtime_private.events "
                "WHERE sequence=:sequence AND run_id=(SELECT run_id "
                "FROM runtime_private.artifact_uploads WHERE upload_id=:artifact)"
            ),
            {"sequence": accepted["event_sequence"], "artifact": artifact_id},
        ).scalar_one()
        stored_parent: UUID | None = connection.execute(
            text(
                "SELECT parent_upload_id FROM runtime_private.artifact_uploads "
                "WHERE upload_id=:artifact"
            ),
            {"artifact": artifact_id},
        ).scalar_one()
    assert row.status == "committed"
    assert row.archive_sha256.hex() == bundle.archive_sha256
    assert row.tree_sha256.hex() == bundle.manifest.tree_sha256
    assert row.compressed_bytes == len(bundle.archive)
    assert row.source_bytes == sum(entry.size for entry in bundle.manifest.entries)
    assert row.file_count == len(bundle.manifest.entries)
    assert accepted["artifact_id"] == artifact_id
    assert event_payload["artifact_id"] == str(artifact_id)
    assert event_payload["archive_sha256"] == bundle.archive_sha256
    assert event_payload["tree_sha256"] == bundle.manifest.tree_sha256
    assert event_payload.get("parent_artifact_id") == (
        None if parent_artifact_id is None else str(parent_artifact_id)
    )
    assert stored_parent == parent_artifact_id
    assert row.content_expires_at is not None


def _assert_corrupt_artifact_attention(
    engine: Engine, owner_id: UUID, run_id: UUID, lease: ExecutionLease
) -> None:
    with engine.connect() as connection:
        state: str = connection.execute(
            text("SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
        failure: str | None = connection.execute(
            text(
                "SELECT failure_class FROM runtime_private.attempts "
                "WHERE owner_id=:owner AND run_id=:run AND attempt_id=:attempt"
            ),
            {"owner": owner_id, "run": run_id, "attempt": lease.attempt_id},
        ).scalar_one()
        event: str = connection.execute(
            text(
                "SELECT event_type FROM runtime_private.events WHERE owner_id=:owner "
                "AND run_id=:run ORDER BY sequence DESC LIMIT 1"
            ),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
    assert state == "needs_attention"
    assert failure == "corrupt_artifact"
    assert event == "job.needs_attention"


def _assert_artifact_rls(engine: Engine, owner_id: UUID, other_owner_id: UUID) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("SELECT set_config('continuum.principal_id', :owner, true)"),
            {"owner": str(owner_id)},
        )
        own_count: int = connection.execute(
            text("SELECT count(*) FROM runtime_private.artifact_uploads")
        ).scalar_one()
        connection.execute(
            text("SELECT set_config('continuum.principal_id', :owner, true)"),
            {"owner": str(other_owner_id)},
        )
        other_count: int = connection.execute(
            text("SELECT count(*) FROM runtime_private.artifact_uploads WHERE owner_id=:owner"),
            {"owner": owner_id},
        ).scalar_one()
    assert own_count >= 2
    assert other_count == 0


def _assert_capacity_ledger_matches_uploads(engine: Engine) -> None:
    with engine.connect() as connection:
        charged: int = connection.execute(
            text("SELECT charged_bytes FROM runtime_private.artifact_capacity WHERE capacity_id=1")
        ).scalar_one()
        expected: int = connection.execute(
            text(
                "SELECT coalesce(sum(compressed_bytes), 0) "
                "FROM runtime_private.artifact_uploads WHERE status <> 'released'"
            )
        ).scalar_one()
    assert charged == expected


def _charged_bytes(engine: Engine) -> int:
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT charged_bytes FROM runtime_private.artifact_capacity WHERE capacity_id=1")
        ).scalar_one()


def _run_event_count(engine: Engine, principal: Principal, run_id: UUID) -> int:
    with engine.connect() as connection:
        return connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.events WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": principal.account_id, "run": run_id},
        ).scalar_one()


def _accepted_result_count(engine: Engine, principal: Principal, run_id: UUID) -> int:
    with engine.connect() as connection:
        return connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": principal.account_id, "run": run_id},
        ).scalar_one()


def _role_url(url: URL, password: str) -> URL:
    return url.set(drivername="postgresql+psycopg", username=RUNTIME_ROLE, password=password)


def _reset_database(engine: Engine, runtime_password: str) -> None:
    with engine.begin() as connection:
        _drop_schemas(connection)
        for role_name in ROLE_NAMES:
            exists: bool = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role_name)"),
                {"role_name": role_name},
            ).scalar_one()
            if exists:
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE {role_name}"))
        role_definitions = (
            ("continuum_ingress", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_issuer", "NOLOGIN", None),
            ("continuum_wake", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_receiver", "LOGIN", secrets.token_urlsafe(32)),
            (RUNTIME_ROLE, "LOGIN", runtime_password),
        )
        for role_name, login_mode, password in role_definitions:
            password_sql = "" if password is None else f"PASSWORD '{password}'"
            connection.exec_driver_sql(
                f"CREATE ROLE {role_name} {login_mode} NOSUPERUSER NOCREATEDB "
                f"NOCREATEROLE NOINHERIT NOBYPASSRLS {password_sql}"
            )


def _drop_schemas(connection: Connection) -> None:
    for schema_name in (
        "runtime_private",
        "wake_private",
        "webhook_private",
        "continuum_migrations",
    ):
        connection.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
    connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))


def _assert_postgres_17(engine: Engine) -> None:
    with engine.connect() as connection:
        version: int = connection.execute(
            text("SELECT current_setting('server_version_num')::integer")
        ).scalar_one()
    if not 170000 <= version < 180000:
        pytest.fail("T15 integration requires PostgreSQL 17")


def _run_migration(action: str, revision: str, database_url: str) -> None:
    environment = os.environ.copy()
    environment["CONTINUUM_MIGRATION_DATABASE_URL"] = database_url
    result = subprocess.run(
        [sys.executable, "-m", "continuum_api.migrations", action, revision],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(f"T15 migration {action} {revision} failed: {result.stdout}\n{result.stderr}")


def _assert_schema_absent(engine: Engine, schema_name: str) -> None:
    with engine.connect() as connection:
        assert not connection.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=:schema)"),
            {"schema": schema_name},
        ).scalar_one()


def _cleanup_database(engine: Engine) -> None:
    with engine.begin() as connection:
        _drop_schemas(connection)
        for role_name in ROLE_NAMES:
            exists: bool = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role_name)"),
                {"role_name": role_name},
            ).scalar_one()
            if exists:
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE {role_name}"))


def _exercise_concurrent_capacity_reservations(
    admin_engine: Engine,
    runtime_engine: Engine,
    persistence: ArtifactPersistence,
    first_owner: UUID,
    second_owner: UUID,
) -> None:
    owners = (first_owner, second_owner)
    principals = tuple(Principal(owner_id) for owner_id in owners)
    leases: list[ExecutionLease] = []
    for index, principal in enumerate(principals):
        _seed_work(runtime_engine, principal, f"source:capacity-race-{index}")
        lease = QueueService(runtime_engine, principal).claim_one(f"capacity-race-{index}")
        assert lease is not None
        leases.append(lease)

    bundle = create_archive((ArchiveFile("src/race.py", b"capacity race\n"),))
    byte_count = len(bundle.archive)
    with admin_engine.begin() as connection:
        previous_charge: int = connection.execute(
            text("SELECT charged_bytes FROM runtime_private.artifact_capacity WHERE capacity_id=1")
        ).scalar_one()
        limit: int = connection.execute(
            text(
                "SELECT capacity_limit_bytes FROM runtime_private.artifact_capacity "
                "WHERE capacity_id=1"
            )
        ).scalar_one()
        connection.execute(
            text(
                "UPDATE runtime_private.artifact_capacity SET charged_bytes=:charged "
                "WHERE capacity_id=1"
            ),
            {"charged": limit - byte_count},
        )

    def reserve(index: int) -> tuple[ArtifactUploadReservation | None, str | None]:
        principal = principals[index]
        lease = leases[index]
        intent_digest = hashlib.sha256(
            principal.account_id.bytes + bundle.archive_sha256.encode("ascii")
        ).digest()
        try:
            reservation = persistence.reserve_upload(
                principal,
                lease,
                "capacity-race",
                bundle.archive_sha256,
                bundle.manifest.tree_sha256,
                bundle.manifest.format_version,
                byte_count,
                sum(entry.size for entry in bundle.manifest.entries),
                len(bundle.manifest.entries),
                intent_digest,
            )
            return reservation, None
        except ArtifactCapacityExceeded as error:
            return None, error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, (0, 1)))
    winners = [reservation for reservation, error in results if reservation is not None]
    assert len(winners) == 1 and winners[0].is_new
    assert [error for _, error in results if error is not None] == ["artifact_capacity_exceeded"]
    with admin_engine.begin() as connection:
        exact_charge: int = connection.execute(
            text("SELECT charged_bytes FROM runtime_private.artifact_capacity WHERE capacity_id=1")
        ).scalar_one()
        connection.execute(
            text(
                "UPDATE runtime_private.artifact_uploads SET status='released', "
                "released_at=clock_timestamp(), updated_at=clock_timestamp() "
                "WHERE upload_id=:upload AND status='reserved'"
            ),
            {"upload": winners[0].upload_id},
        )
        connection.execute(
            text(
                "UPDATE runtime_private.artifact_capacity SET charged_bytes=:charged "
                "WHERE capacity_id=1"
            ),
            {"charged": previous_charge},
        )
    assert exact_charge == limit


def _assert_legacy_transition_after_artifact_migration_down(
    admin_engine: Engine, url: URL, runtime_password: str, database_url: str
) -> None:
    _run_migration("downgrade", "20261012_08", database_url)
    _run_migration("downgrade", "20261011_07", database_url)
    runtime_engine = create_engine(
        _role_url(url, runtime_password), hide_parameters=True, pool_size=2, max_overflow=0
    )
    owner_id = uuid4()
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:owner, 819999)"
            ),
            {"owner": owner_id},
        )
    principal = Principal(owner_id)
    run_id, _, _ = _seed_work(runtime_engine, principal, "legacy:transition")
    lease = QueueService(runtime_engine, principal).claim_one("legacy-transition-worker")
    assert lease is not None
    transition = TransitionService(runtime_engine, principal)
    receipt = transition.commit_result(
        lease=lease,
        result={"legacy": "artifact-less"},
        target_run_status=RunStatus.WAITING_APPROVAL,
    )
    adopted = transition.commit_result(
        lease=lease,
        result={"legacy": "artifact-less"},
        target_run_status=RunStatus.WAITING_APPROVAL,
    )
    assert not receipt.adopted and adopted.adopted
    assert receipt.artifact_id is None and adopted.artifact_id is None
    assert _accepted_result_count(admin_engine, principal, run_id) == 1
    with admin_engine.connect() as connection:
        artifact_column_present: bool = connection.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                "WHERE table_schema='runtime_private' AND table_name='accepted_results' "
                "AND column_name='artifact_id')"
            )
        ).scalar_one()
    assert not artifact_column_present
    runtime_engine.dispose()
