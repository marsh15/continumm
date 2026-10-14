"""T21 worker death and late-result fencing proofs.

The portable process fixture exercises canonical archive restoration across an
OS process death. The PostgreSQL 17 cases below exercise the real T15/T21
services and are the only cases that claim runtime recovery or fencing.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import secrets
import select
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, replace
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.engine import URL, make_url

from continuum_api.artifacts.archives import ArchiveFile, create_archive, verify_archive
from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.connectivity.storage import SupabaseStorage
from continuum_api.repositories.preflight import (
    GitHubRepositoryAdapter,
    RepositoryAccessProof,
    RepositoryMetadata,
)
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus, RuntimeRepository
from continuum_api.runtime.sandbox_allocations import SandboxAllocationStore
from continuum_api.runtime.scheduler import RuntimeRecoveryService
from continuum_api.runtime.transitions import NextWork, TransitionService
from continuum_api.runtime.workspace_recovery import (
    WorkspaceRecoveryError,
    WorkspaceRecoveryService,
)
from continuum_api.tools.contracts import TextEdit
from continuum_api.tools.source_tools import apply_text_edits, snapshot_from_verified_archive

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_WORKER_LINE_BUFFERS: dict[int, bytearray] = {}
DATABASE_NAME = "continuum_t21_test"
LATEST_REVISION = "20261012_10"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)
STORAGE_URL = "https://fixture-project.supabase.co"
STORAGE_SECRET = "sb_secret_t21_process_fixture_only"
STORAGE_BUCKET = "continuum-checkpoints"
REPOSITORY_ID = 714_021


@dataclass(frozen=True, slots=True)
class RunFixture:
    owner_id: UUID
    run_id: UUID
    step_id: UUID
    job_id: UUID

    @property
    def principal(self) -> Principal:
        return Principal(self.owner_id)


@dataclass(slots=True)
class T21Database:
    admin_engine: Engine
    runtime_engine: Engine
    runtime_url: URL


@pytest.fixture(scope="module")
def t21_database() -> Any:
    database_url = os.environ.get("CONTINUUM_T21_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T21_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T21 integration requires loopback PostgreSQL named continuum_t21_test")

    admin_engine = create_engine(
        url.set(drivername="postgresql+psycopg"),
        hide_parameters=True,
        pool_size=8,
        max_overflow=0,
    )
    runtime_password = secrets.token_urlsafe(32)
    runtime_url = _role_url(url, runtime_password)
    migration_applied = False
    runtime_engine: Engine | None = None
    try:
        _reset_database(admin_engine, runtime_password)
        _assert_postgres_17(admin_engine)
        _run_migration("upgrade", LATEST_REVISION, database_url)
        migration_applied = True
        runtime_engine = create_engine(
            runtime_url,
            hide_parameters=True,
            pool_size=4,
            max_overflow=0,
            connect_args={
                "application_name": "t21-workspace-recovery-controller",
                "connect_timeout": 5,
                "options": "-c statement_timeout=5000 -c lock_timeout=2000",
            },
        )
        yield T21Database(admin_engine, runtime_engine, runtime_url)
    finally:
        if runtime_engine is not None:
            runtime_engine.dispose()
        try:
            if migration_applied:
                _run_migration("downgrade", "base", database_url)
        finally:
            _cleanup_database(admin_engine)
            admin_engine.dispose()


@pytest.fixture
def child_workers() -> Any:
    workers: list[subprocess.Popen[str]] = []
    yield workers
    for worker in workers:
        if worker.poll() is None:
            worker.kill()
            worker.wait(timeout=5)
        if worker.stdin is not None:
            worker.stdin.close()
        if worker.stdout is not None:
            worker.stdout.close()
        if worker.stderr is not None:
            worker.stderr.close()


def test_local_process_restore_follows_only_the_committed_archive(tmp_path: Path) -> None:
    """Always-run archive/process smoke; this does not exercise DB authority."""
    committed_files = (
        ArchiveFile("src/app.py", b"def answer():\n    return 'committed'\n"),
        ArchiveFile("bin/check", b"#!/bin/sh\nexit 0\n", mode=0o755),
    )
    unattached_files = (ArchiveFile("src/app.py", b"def answer():\n    return 'unattached'\n"),)
    committed = create_archive(committed_files)
    unattached = create_archive(unattached_files)
    object_path = tmp_path / "private-objects"
    object_path.mkdir()
    committed_path = object_path / "committed.tar.gz"
    unattached_path = object_path / "unattached.tar.gz"
    committed_path.write_bytes(committed.archive)
    unattached_path.write_bytes(unattached.archive)

    worker = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _LOCAL_ARCHIVE_WORKER,
            str(committed_path),
            str(unattached_path),
            committed.archive_sha256,
            committed.manifest.tree_sha256,
        ],
        cwd=PROJECT_ROOT,
        env={"PATH": os.environ.get("PATH", ""), "PYTHONUNBUFFERED": "1"},
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    try:
        ready = _read_worker_message(worker)
        assert ready == {"kind": "unattached_object_written"}
        worker.send_signal(signal.SIGKILL)
        assert worker.wait(timeout=5) == -signal.SIGKILL
    finally:
        _close_process_pipes(worker)

    restored = subprocess.run(
        [
            sys.executable,
            "-c",
            _LOCAL_ARCHIVE_RESTORER,
            str(committed_path),
            committed.archive_sha256,
            committed.manifest.tree_sha256,
        ],
        cwd=PROJECT_ROOT,
        env={"PATH": os.environ.get("PATH", ""), "PYTHONUNBUFFERED": "1"},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert restored.returncode == 0
    payload = json.loads(restored.stdout)
    assert payload["archive_sha256"] == committed.archive_sha256
    assert payload["tree_sha256"] == committed.manifest.tree_sha256
    assert payload["files"] == _files_payload(committed_files)
    assert unattached.archive_sha256 != committed.archive_sha256


def test_sigkill_before_private_upload_restores_last_committed_bytes(
    t21_database: T21Database,
    child_workers: list[subprocess.Popen[str]],
    tmp_path: Path,
) -> None:
    _run_kill_before_or_after_upload(
        t21_database,
        child_workers,
        tmp_path,
        fault_point="before_upload",
        expected_upload_status="uploading",
    )


def test_sigkill_after_private_upload_before_result_commit_restores_last_committed_bytes(
    t21_database: T21Database,
    child_workers: list[subprocess.Popen[str]],
    tmp_path: Path,
) -> None:
    _run_kill_before_or_after_upload(
        t21_database,
        child_workers,
        tmp_path,
        fault_point="before_result_commit",
        expected_upload_status="verified",
    )


def test_sigkill_after_result_commit_is_adopted_and_restores_new_exact_bytes(
    t21_database: T21Database,
    child_workers: list[subprocess.Popen[str]],
    tmp_path: Path,
) -> None:
    store = t21_database
    objects = tmp_path / "objects"
    objects.mkdir()
    run = _seed_committed_workspace(store, objects)

    child = _start_workspace_worker(
        store.runtime_url,
        run,
        objects,
        mode="accept",
        fault_point="after_result_commit",
        replacement_text="committed-after-crash",
    )
    child_workers.append(child)
    restored = _read_worker_message(child)
    _assert_snapshot(restored, _base_files())
    lease_message = _read_worker_message(child)
    assert lease_message["kind"] == "lease_claimed"
    commit_barrier = _read_worker_message(child)
    assert commit_barrier["kind"] == "result_committed"
    result_committed = commit_barrier
    commit_barrier = _read_worker_message(child)
    assert commit_barrier["kind"] == "barrier"
    assert commit_barrier["point"] == "after_result_commit"
    _assert_one_candidate_accepted(store.admin_engine, run, lease_message, result_committed)

    child.send_signal(signal.SIGKILL)
    assert child.wait(timeout=5) == -signal.SIGKILL

    next_worker = _start_workspace_worker(
        store.runtime_url,
        run,
        objects,
        mode="restore",
        fault_point=None,
        replacement_text="unused",
        prior_lease=lease_message,
    )
    child_workers.append(next_worker)
    adoption = _read_worker_message(next_worker)
    assert adoption == {
        "kind": "accepted_result_adopted",
        "artifact_id": result_committed["artifact_id"],
        "result_id": result_committed["result_id"],
        "candidate": "committed-after-crash",
    }
    recovered = _read_worker_message(next_worker)
    expected_files = _candidate_files(b"committed-after-crash")
    _assert_snapshot(recovered, expected_files)
    next_lease = _read_worker_message(next_worker)
    assert next_lease["kind"] == "lease_claimed"
    done = _read_worker_message(next_worker)
    assert done["kind"] == "worker_done"
    assert next_worker.wait(timeout=5) == 0


def test_late_sandbox_result_cannot_replace_newer_generation(
    t21_database: T21Database,
    child_workers: list[subprocess.Popen[str]],
    tmp_path: Path,
) -> None:
    store = t21_database
    objects = tmp_path / "objects"
    objects.mkdir()
    run = _seed_committed_workspace(store, objects)

    stale_worker = _start_workspace_worker(
        store.runtime_url,
        run,
        objects,
        mode="accept",
        fault_point="after_sandbox_result",
        replacement_text="stale-sandbox-result",
    )
    child_workers.append(stale_worker)
    stale_snapshot = _read_worker_message(stale_worker)
    _assert_snapshot(stale_snapshot, _base_files())
    stale_lease = _read_worker_message(stale_worker)
    assert stale_lease["kind"] == "lease_claimed"
    sandbox_barrier = _read_worker_message(stale_worker)
    assert sandbox_barrier == {"kind": "barrier", "point": "after_sandbox_result"}

    _expire_lease(store.admin_engine, run, stale_lease)
    assert RuntimeRecoveryService(store.runtime_engine, run.principal).recover_due(limit=1) == 1
    assert RuntimeRecoveryService(store.runtime_engine, run.principal).recover_due(limit=1) == 0

    winner = _start_workspace_worker(
        store.runtime_url,
        run,
        objects,
        mode="accept",
        fault_point=None,
        replacement_text="new-generation-result",
    )
    child_workers.append(winner)
    winner_snapshot = _read_worker_message(winner)
    _assert_snapshot(winner_snapshot, _base_files())
    winner_lease = _read_worker_message(winner)
    assert winner_lease["kind"] == "lease_claimed"
    assert winner_lease["generation"] == stale_lease["generation"] + 1
    winner_receipt = _read_worker_message(winner)
    assert winner_receipt["kind"] == "accepted"
    assert winner_receipt["generation"] == winner_lease["generation"]
    assert winner.wait(timeout=5) == 0
    _assert_only_generation_result(store.admin_engine, run, winner_lease, winner_receipt)

    stale_worker.stdin.write("resume\n")
    stale_worker.stdin.flush()
    stale_outcome = _read_worker_message(stale_worker)
    assert stale_outcome == {
        "kind": "late_result_rejected",
        "code": "workspace_activity_already_accepted",
    }
    assert stale_worker.wait(timeout=5) == 0
    _assert_only_generation_result(store.admin_engine, run, winner_lease, winner_receipt)


def test_forged_same_identity_workspace_is_rejected_before_upload(
    t21_database: T21Database,
    tmp_path: Path,
) -> None:
    store = t21_database
    objects = tmp_path / "objects"
    objects.mkdir()
    run = _seed_committed_workspace(store, objects)
    lease = QueueService(store.runtime_engine, run.principal).claim_one("t21-forged-base")
    assert lease is not None
    storage, requests, _memory_objects = _storage_fixture(objects)
    persistence = ArtifactPersistence(store.runtime_engine)
    checkpoints = _checkpoint_service(store.runtime_engine, run.principal, persistence, storage)
    recovery = WorkspaceRecoveryService(
        store.runtime_engine,
        run.principal,
        checkpoints,
        TransitionService(store.runtime_engine, run.principal),
    )

    async def attempt_forged_candidate() -> None:
        trusted = await recovery.restore_latest(run.principal, lease)
        forged_bundle = create_archive(
            (
                ArchiveFile("src/app.py", b"def answer():\n    return 'forged-base'\n"),
                ArchiveFile("bin/check", b"#!/bin/sh\nexit 0\n", mode=0o755),
            )
        )
        forged_verified = verify_archive(
            forged_bundle.archive,
            expected_archive_sha256=forged_bundle.archive_sha256,
            expected_tree_sha256=forged_bundle.manifest.tree_sha256,
            expected_manifest=forged_bundle.manifest,
        )
        forged_source = snapshot_from_verified_archive(
            forged_verified,
            expected_archive_sha256=forged_bundle.archive_sha256,
            expected_tree_sha256=forged_bundle.manifest.tree_sha256,
        )
        forged_snapshot = replace(trusted, verified=forged_verified, source=forged_source)
        candidate = apply_text_edits(
            forged_source,
            [
                TextEdit(
                    path="src/app.py",
                    expected_text="return 'forged-base'",
                    replacement_text="return 'must-not-upload'",
                )
            ],
            expected_tree_sha256=forged_source.tree_sha256,
            authorized_write_paths={"src/app.py"},
        )
        with pytest.raises(WorkspaceRecoveryError, match="workspace_checkpoint_identity_mismatch"):
            await recovery.accept_candidate(
                run.principal,
                lease,
                "tool:forged-base",
                forged_snapshot,
                candidate,
                result={"candidate": "must-not-upload"},
                target_run_status=RunStatus.RUNNING,
                next_work=None,
            )

    asyncio.run(attempt_forged_candidate())
    assert sum(request.method == "POST" for request in requests) == 0
    assert _accepted_artifact_ids(store.admin_engine, run) == [
        str(_initial_artifact_id(store.admin_engine, run))
    ]
    _assert_upload_count(store.admin_engine, run, expected=1)
    asyncio.run(storage.aclose())


def test_other_account_cannot_restore_an_owned_workspace(
    t21_database: T21Database,
    tmp_path: Path,
) -> None:
    store = t21_database
    objects = tmp_path / "objects"
    objects.mkdir()
    run = _seed_committed_workspace(store, objects)
    lease = QueueService(store.runtime_engine, run.principal).claim_one("t21-owner-boundary")
    assert lease is not None
    other_principal = Principal(uuid4())
    storage, requests, _memory_objects = _storage_fixture(objects)
    persistence = ArtifactPersistence(store.runtime_engine)
    checkpoints = _checkpoint_service(store.runtime_engine, other_principal, persistence, storage)
    recovery = WorkspaceRecoveryService(
        store.runtime_engine,
        other_principal,
        checkpoints,
        TransitionService(store.runtime_engine, other_principal),
    )

    with pytest.raises(WorkspaceRecoveryError, match="workspace_owner_mismatch"):
        asyncio.run(recovery.recover(other_principal, lease))

    assert requests == []
    _assert_upload_count(store.admin_engine, run, expected=1)
    asyncio.run(storage.aclose())


def test_unresolved_prior_allocation_blocks_restore_before_dispatch(
    t21_database: T21Database,
    tmp_path: Path,
) -> None:
    store = t21_database
    objects = tmp_path / "objects"
    objects.mkdir()
    run = _seed_committed_workspace(store, objects)
    lease = QueueService(store.runtime_engine, run.principal).claim_one("t21-unresolved-allocation")
    assert lease is not None
    period_id = "t21-process-recovery-period"
    _seed_t21_provider_period(store.admin_engine, period_id)
    allocation_store = SandboxAllocationStore(store.runtime_engine)
    reservation = allocation_store.reserve(
        run.principal,
        lease,
        "python:unknown-recovery-allocation",
        hashlib.sha256(b"t21-unknown-recovery-allocation").digest(),
        period_id,
        "public",
        120,
    )
    assert reservation.is_new
    assert allocation_store.authorize_create(run.principal, lease, reservation.allocation_id)
    allocation_store.mark_create_unknown(run.principal, reservation.allocation_id)

    storage, requests, _memory_objects = _storage_fixture(objects)
    persistence = ArtifactPersistence(store.runtime_engine)
    checkpoints = _checkpoint_service(store.runtime_engine, run.principal, persistence, storage)
    recovery = WorkspaceRecoveryService(
        store.runtime_engine,
        run.principal,
        checkpoints,
        TransitionService(store.runtime_engine, run.principal),
        allocation_store=allocation_store,
    )

    with pytest.raises(WorkspaceRecoveryError, match="workspace_prior_allocation_unresolved"):
        asyncio.run(recovery.recover(run.principal, lease))

    assert requests == []
    _assert_upload_count(store.admin_engine, run, expected=1)
    assert _accepted_artifact_ids(store.admin_engine, run) == [
        str(_initial_artifact_id(store.admin_engine, run))
    ]
    with store.admin_engine.connect() as connection:
        state = connection.execute(
            text(
                "SELECT status FROM runtime_private.sandbox_allocations "
                "WHERE allocation_id=:allocation"
            ),
            {"allocation": reservation.allocation_id},
        ).scalar_one()
        active_slot = connection.execute(
            text(
                "SELECT active_allocation_id, active_cpu_seconds "
                "FROM runtime_private.sandbox_global_capacity WHERE capacity_id=1"
            )
        ).one()
        charged_cpu_seconds = connection.execute(
            text(
                "SELECT charged_cpu_seconds FROM runtime_private.sandbox_period_usage "
                "WHERE provider_period_id=:period AND allocation_class='public'"
            ),
            {"period": period_id},
        ).scalar_one()
    assert state == "unknown"
    assert active_slot.active_allocation_id == reservation.allocation_id
    assert active_slot.active_cpu_seconds == 240
    assert charged_cpu_seconds == 240
    asyncio.run(storage.aclose())


@pytest.mark.parametrize("latest_state", ["corrupt", "expired"])
def test_unrecoverable_latest_checkpoint_requires_attention_without_fallback(
    t21_database: T21Database,
    child_workers: list[subprocess.Popen[str]],
    tmp_path: Path,
    latest_state: str,
) -> None:
    store = t21_database
    objects = tmp_path / latest_state
    objects.mkdir()
    run = _seed_committed_workspace(store, objects)
    commit_worker = _start_workspace_worker(
        store.runtime_url,
        run,
        objects,
        mode="accept",
        fault_point=None,
        replacement_text="latest-accepted-source",
    )
    child_workers.append(commit_worker)
    restored = _read_worker_message(commit_worker)
    _assert_snapshot(restored, _base_files())
    lease_message = _read_worker_message(commit_worker)
    assert lease_message["kind"] == "lease_claimed"
    accepted = _read_worker_message(commit_worker)
    assert accepted["kind"] == "accepted"
    assert commit_worker.wait(timeout=5) == 0

    latest_artifact_id, latest_object_key = _latest_artifact_metadata(store.admin_engine, run)
    assert latest_artifact_id != _initial_artifact_id(store.admin_engine, run)
    if latest_state == "corrupt":
        (objects / latest_object_key).write_bytes(b"corrupted latest archive")
        expected_error = "corrupt_artifact"
    else:
        with store.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.artifact_uploads "
                    "SET created_at=clock_timestamp() - interval '2 hours', "
                    "orphan_grace_until=clock_timestamp() - interval '61 minutes', "
                    "content_expires_at=clock_timestamp() - interval '1 second' "
                    "WHERE owner_id=:owner AND run_id=:run AND upload_id=:artifact"
                ),
                {
                    "owner": run.owner_id,
                    "run": run.run_id,
                    "artifact": latest_artifact_id,
                },
            )
        expected_error = "artifact_upload_reservation_lost"

    recovery_worker = _start_workspace_worker(
        store.runtime_url,
        run,
        objects,
        mode="restore",
        fault_point=None,
        replacement_text="unused",
    )
    child_workers.append(recovery_worker)
    failure = _read_worker_message(recovery_worker, allow_error=True)
    assert failure == {"kind": "worker_error", "code": expected_error}
    assert recovery_worker.wait(timeout=5) == 2
    _assert_run_needs_attention(store.admin_engine, run)
    accepted_ids = _accepted_artifact_ids(store.admin_engine, run)
    assert len(accepted_ids) == 2
    assert accepted_ids[-1] == str(latest_artifact_id)
    _assert_upload_count(store.admin_engine, run, expected=2)


def _run_kill_before_or_after_upload(
    store: T21Database,
    workers: list[subprocess.Popen[str]],
    tmp_path: Path,
    *,
    fault_point: str,
    expected_upload_status: str,
) -> None:
    objects = tmp_path / fault_point
    objects.mkdir()
    run = _seed_committed_workspace(store, objects)

    child = _start_workspace_worker(
        store.runtime_url,
        run,
        objects,
        mode="accept",
        fault_point=fault_point,
        replacement_text=f"uncommitted-{fault_point}",
    )
    workers.append(child)
    restored = _read_worker_message(child)
    _assert_snapshot(restored, _base_files())
    lease_message = _read_worker_message(child)
    assert lease_message["kind"] == "lease_claimed"
    barrier = _read_worker_message(child)
    assert barrier == {"kind": "barrier", "point": fault_point}

    child.send_signal(signal.SIGKILL)
    assert child.wait(timeout=5) == -signal.SIGKILL
    _assert_uncommitted_upload_only(store.admin_engine, run, expected_upload_status)
    assert _accepted_artifact_ids(store.admin_engine, run) == [
        str(_initial_artifact_id(store.admin_engine, run))
    ]

    _expire_lease(store.admin_engine, run, lease_message)
    recovery = RuntimeRecoveryService(store.runtime_engine, run.principal)
    assert recovery.recover_due(limit=1) == 1
    assert recovery.recover_due(limit=1) == 0

    replacement = _start_workspace_worker(
        store.runtime_url,
        run,
        objects,
        mode="restore",
        fault_point=None,
        replacement_text="unused",
    )
    workers.append(replacement)
    snapshot = _read_worker_message(replacement)
    _assert_snapshot(snapshot, _base_files())
    replacement_lease = _read_worker_message(replacement)
    assert replacement_lease["kind"] == "lease_claimed"
    done = _read_worker_message(replacement)
    assert done["kind"] == "worker_done"
    assert replacement.wait(timeout=5) == 0


def _seed_committed_workspace(store: T21Database, objects: Path) -> RunFixture:
    owner_id = uuid4()
    principal = Principal(owner_id)
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:owner_id, :github_user_id)"
            ),
            {"owner_id": owner_id, "github_user_id": 980_000_000 + owner_id.int % 1_000_000},
        )
    with store.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run(github_repository_id=REPOSITORY_ID, issue_number=1)
        step_id = repository.create_step(run_id, "source:initial", "source")
        job_id = repository.create_job(run_id, step_id)
    run = RunFixture(owner_id, run_id, step_id, job_id)
    lease = QueueService(store.runtime_engine, principal).claim_one("t21-base-checkpoint")
    assert lease is not None
    storage, _requests, _objects = _storage_fixture(objects)
    persistence = ArtifactPersistence(store.runtime_engine)
    service = _checkpoint_service(store.runtime_engine, principal, persistence, storage)
    receipt = asyncio.run(
        service.commit_checkpoint(
            principal,
            lease,
            "source:initial",
            _base_files(),
            result={"workspace": "initial", "exact_source": True},
            target_run_status=RunStatus.RUNNING,
            next_work=NextWork(
                "workspace:edit", "agent_work", replay_policy=ReplayPolicy.REPLAY_SAFE
            ),
        )
    )
    assert receipt.transition.artifact_id == receipt.artifact_id
    asyncio.run(storage.aclose())
    return run


def _seed_t21_provider_period(engine: Engine, period_id: str) -> None:
    with engine.begin() as connection:
        database_now = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
        connection.execute(
            text(
                "INSERT INTO runtime_private.sandbox_provider_periods "
                "(provider_period_id, starts_at, ends_at, public_cpu_seconds_limit, "
                "operator_cpu_seconds_limit) "
                "VALUES (:period, :starts_at, :ends_at, 14400, 3600)"
            ),
            {
                "period": period_id,
                "starts_at": database_now - timedelta(days=1),
                "ends_at": database_now + timedelta(days=1),
            },
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.sandbox_period_usage "
                "(provider_period_id, allocation_class) VALUES (:period, 'public'), "
                "(:period, 'operator')"
            ),
            {"period": period_id},
        )


def _base_files() -> tuple[ArchiveFile, ...]:
    return (
        ArchiveFile("src/app.py", b"def answer():\n    return 'base'\n"),
        ArchiveFile("bin/check", b"#!/bin/sh\nexit 0\n", mode=0o755),
    )


def _candidate_files(replacement: bytes) -> tuple[ArchiveFile, ...]:
    return (
        ArchiveFile("src/app.py", b"def answer():\n    return '" + replacement + b"'\n"),
        ArchiveFile("bin/check", b"#!/bin/sh\nexit 0\n", mode=0o755),
    )


def _files_payload(files: tuple[ArchiveFile, ...]) -> list[dict[str, Any]]:
    return sorted(
        [
            {
                "content_b64": base64.b64encode(file.content).decode("ascii"),
                "mode": file.mode,
                "path": file.path,
            }
            for file in files
        ],
        key=lambda item: item["path"],
    )


def _assert_snapshot(message: dict[str, Any], files: tuple[ArchiveFile, ...]) -> None:
    assert message["kind"] == "restored"
    assert message["files"] == _files_payload(files)


def _start_workspace_worker(
    runtime_url: URL,
    run: RunFixture,
    objects: Path,
    *,
    mode: str,
    fault_point: str | None,
    replacement_text: str,
    prior_lease: dict[str, Any] | None = None,
) -> subprocess.Popen[str]:
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONUNBUFFERED": "1",
        "CONTINUUM_T21_RUNTIME_DATABASE_URL": runtime_url.render_as_string(hide_password=False),
    }
    arguments = [
        sys.executable,
        "-c",
        _WORKER_CODE,
        str(run.owner_id),
        str(run.run_id),
        str(objects),
        mode,
        fault_point or "-",
        replacement_text,
        json.dumps(prior_lease, sort_keys=True) if prior_lease is not None else "-",
    ]
    return subprocess.Popen(
        arguments,
        cwd=PROJECT_ROOT,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


def _storage_fixture(
    objects: Path,
) -> tuple[SupabaseStorage, list[httpx.Request], dict[str, bytes]]:
    objects.mkdir(mode=0o700, parents=True, exist_ok=True)
    requests: list[httpx.Request] = []
    memory_objects: dict[str, bytes] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET" and request.url.path.endswith(f"/bucket/{STORAGE_BUCKET}"):
            return httpx.Response(200, json={"id": STORAGE_BUCKET, "public": False})
        upload_prefix = f"/storage/v1/object/{STORAGE_BUCKET}/"
        if request.method == "POST" and request.url.path.startswith(upload_prefix):
            key = request.url.path.removeprefix(upload_prefix)
            content = request.read()
            memory_objects[key] = content
            _write_private_object(objects, key, content)
            return httpx.Response(200, json={"Key": key})
        download_prefix = f"/storage/v1/object/authenticated/{STORAGE_BUCKET}/"
        if request.method == "GET" and request.url.path.startswith(download_prefix):
            key = request.url.path.removeprefix(download_prefix)
            content = memory_objects.get(key)
            if content is None:
                content = _read_private_object(objects, key)
            return httpx.Response(404) if content is None else httpx.Response(200, content=content)
        raise AssertionError(f"unexpected storage request {request.method} {request.url.path}")

    storage = SupabaseStorage(
        STORAGE_URL,
        STORAGE_SECRET,
        STORAGE_BUCKET,
        transport=httpx.MockTransport(handler),
        fixture_byte_limit=10 * 1024 * 1024,
        request_deadline_seconds=5.0,
    )
    return storage, requests, memory_objects


def _write_private_object(directory: Path, key: str, content: bytes) -> None:
    target = directory / key
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(
        target,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    with os.fdopen(descriptor, "wb") as object_file:
        object_file.write(content)
        object_file.flush()
        os.fsync(object_file.fileno())


def _read_private_object(directory: Path, key: str) -> bytes | None:
    target = directory / key
    try:
        return target.read_bytes()
    except FileNotFoundError:
        return None


class _AccessVerifier:
    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
        return RepositoryAccessProof(repository_id, github_user_id, True, True)


class _PublicRepositoryAdapter(GitHubRepositoryAdapter):
    async def current_public_repository(self, repository_id: int) -> RepositoryMetadata:
        return RepositoryMetadata(repository_id, "fixture-owner", "fixture-project", 1)

    async def aclose(self) -> None:
        return


def _checkpoint_service(
    engine: Engine,
    principal: Principal,
    persistence: ArtifactPersistence,
    storage: SupabaseStorage,
) -> CheckpointStorageService:
    return CheckpointStorageService(
        persistence,
        storage,
        TransitionService(engine, principal, artifact_persistence=persistence),
        RuntimeRecoveryService(engine, principal),
        access_verifier=_AccessVerifier(),
        github_adapter_factory=_PublicRepositoryAdapter,
    )


def _expire_lease(engine: Engine, run: RunFixture, lease: dict[str, Any]) -> None:
    with engine.begin() as connection:
        expired = connection.execute(
            text(
                "UPDATE runtime_private.jobs SET lease_expires_at=clock_timestamp() "
                "- interval '1 second' WHERE owner_id=:owner_id AND run_id=:run_id "
                "AND job_id=:job_id AND generation=:generation AND status='leased' "
                "RETURNING job_id"
            ),
            {
                "owner_id": run.owner_id,
                "run_id": run.run_id,
                "job_id": UUID(str(lease["job_id"])),
                "generation": lease["generation"],
            },
        ).one_or_none()
    assert expired is not None


def _assert_uncommitted_upload_only(engine: Engine, run: RunFixture, expected_status: str) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        rows = (
            connection.execute(
                text(
                    "SELECT status FROM runtime_private.artifact_uploads "
                    "WHERE owner_id=:owner AND run_id=:run AND checkpoint_key <> 'source:initial'"
                ),
                {"owner": run.owner_id, "run": run.run_id},
            )
            .scalars()
            .all()
        )
        assert rows == [expected_status]
        accepted_candidate_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL "
                "AND job_id <> :initial_job"
            ),
            {"owner": run.owner_id, "run": run.run_id, "initial_job": run.job_id},
        ).scalar_one()
    assert accepted_candidate_count == 0


def _accepted_artifact_ids(engine: Engine, run: RunFixture) -> list[str]:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        return [
            str(value)
            for value in connection.execute(
                text(
                    "SELECT artifact_id FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL "
                    "ORDER BY event_sequence"
                ),
                {"owner": run.owner_id, "run": run.run_id},
            ).scalars()
        ]


def _latest_artifact_metadata(engine: Engine, run: RunFixture) -> tuple[UUID, str]:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        row = connection.execute(
            text(
                "SELECT accepted.artifact_id, artifact.object_key "
                "FROM runtime_private.accepted_results AS accepted "
                "JOIN runtime_private.artifact_uploads AS artifact "
                "ON artifact.owner_id=accepted.owner_id AND artifact.run_id=accepted.run_id "
                "AND artifact.upload_id=accepted.artifact_id "
                "WHERE accepted.owner_id=:owner AND accepted.run_id=:run "
                "AND accepted.artifact_id IS NOT NULL "
                "ORDER BY accepted.event_sequence DESC LIMIT 1"
            ),
            {"owner": run.owner_id, "run": run.run_id},
        ).one()
    return row.artifact_id, row.object_key


def _assert_run_needs_attention(engine: Engine, run: RunFixture) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        run_status = connection.execute(
            text("SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
            {"owner": run.owner_id, "run": run.run_id},
        ).scalar_one()
        event = connection.execute(
            text(
                "SELECT event_type, payload FROM runtime_private.events "
                "WHERE owner_id=:owner AND run_id=:run "
                "ORDER BY sequence DESC LIMIT 1"
            ),
            {"owner": run.owner_id, "run": run.run_id},
        ).one()
    assert run_status == RunStatus.NEEDS_ATTENTION.value
    assert event.event_type == "job.needs_attention"
    assert event.payload["failure_class"] == "corrupt_artifact"


def _assert_upload_count(engine: Engine, run: RunFixture, *, expected: int) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.artifact_uploads "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": run.owner_id, "run": run.run_id},
        ).scalar_one()
    assert count == expected


def _initial_artifact_id(engine: Engine, run: RunFixture) -> UUID:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        return connection.execute(
            text(
                "SELECT artifact_id FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner AND run_id=:run AND job_id=:job"
            ),
            {"owner": run.owner_id, "run": run.run_id, "job": run.job_id},
        ).scalar_one()


def _assert_one_candidate_accepted(
    engine: Engine,
    run: RunFixture,
    lease: dict[str, Any],
    committed: dict[str, Any],
) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        rows = (
            connection.execute(
                text(
                    "SELECT generation, attempt_id, artifact_id, payload FROM "
                    "runtime_private.accepted_results WHERE owner_id=:owner AND run_id=:run "
                    "AND job_id=:job"
                ),
                {
                    "owner": run.owner_id,
                    "run": run.run_id,
                    "job": UUID(str(lease["job_id"])),
                },
            )
            .mappings()
            .all()
        )
    assert len(rows) == 1
    assert rows[0]["generation"] == lease["generation"]
    assert str(rows[0]["attempt_id"]) == lease["attempt_id"]
    assert str(rows[0]["artifact_id"]) == committed["artifact_id"]
    assert rows[0]["payload"]["candidate"] == "committed-after-crash"


def _assert_only_generation_result(
    engine: Engine,
    run: RunFixture,
    lease: dict[str, Any],
    receipt: dict[str, Any],
) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        rows = (
            connection.execute(
                text(
                    "SELECT generation, attempt_id, artifact_id, payload FROM "
                    "runtime_private.accepted_results WHERE owner_id=:owner AND run_id=:run "
                    "AND job_id=:job"
                ),
                {
                    "owner": run.owner_id,
                    "run": run.run_id,
                    "job": UUID(str(lease["job_id"])),
                },
            )
            .mappings()
            .all()
        )
        candidate_uploads = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.artifact_uploads "
                "WHERE owner_id=:owner AND run_id=:run "
                "AND checkpoint_key='tool:replace-answer'"
            ),
            {"owner": run.owner_id, "run": run.run_id},
        ).scalar_one()
    assert len(rows) == 1
    assert rows[0]["generation"] == lease["generation"]
    assert str(rows[0]["attempt_id"]) == lease["attempt_id"]
    assert str(rows[0]["artifact_id"]) == receipt["artifact_id"]
    assert rows[0]["payload"]["candidate"] == "new-generation-result"
    assert candidate_uploads == 1


def _read_worker_message(
    worker: subprocess.Popen[str], *, timeout: float = 10.0, allow_error: bool = False
) -> dict[str, Any]:
    if worker.stdout is None:
        pytest.fail("worker_stdout_unavailable")
    descriptor = worker.stdout.fileno()
    buffer = _WORKER_LINE_BUFFERS.setdefault(descriptor, bytearray())
    deadline = time.monotonic() + timeout
    while b"\n" not in buffer:
        remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
        poller = select.poll()
        poller.register(descriptor, select.POLLIN | select.POLLHUP | select.POLLERR)
        if not poller.poll(remaining_ms):
            pytest.fail("worker_message_timed_out")
        chunk = os.read(descriptor, 4096)
        if not chunk:
            pytest.fail("worker_exited_before_message")
        buffer.extend(chunk)
    line, _, remaining = buffer.partition(b"\n")
    _WORKER_LINE_BUFFERS[descriptor] = bytearray(remaining)
    try:
        message = json.loads(line.decode("utf-8"))
    except json.JSONDecodeError:
        pytest.fail("worker_message_invalid")
    if not isinstance(message, dict) or (message.get("kind") == "worker_error" and not allow_error):
        safe_code = message.get("code") if isinstance(message, dict) else None
        pytest.fail(f"worker_operation_failed:{safe_code or 'invalid_message'}")
    return message


def _close_process_pipes(worker: subprocess.Popen[str]) -> None:
    if worker.stdout is not None:
        _WORKER_LINE_BUFFERS.pop(worker.stdout.fileno(), None)
    for stream in (worker.stdin, worker.stdout, worker.stderr):
        if stream is not None and not stream.closed:
            stream.close()


def _role_url(url: URL, password: str) -> URL:
    return url.set(
        drivername="postgresql+psycopg",
        username=RUNTIME_ROLE,
        password=password,
        query={"sslmode": "disable"},
    )


def _reset_database(engine: Engine, runtime_password: str) -> None:
    with engine.begin() as connection:
        _drop_schemas(connection)
        for role_name in ROLE_NAMES:
            exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)"),
                {"name": role_name},
            ).scalar_one()
            if exists:
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE {role_name}"))
        roles = (
            ("continuum_ingress", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_issuer", "NOLOGIN", None),
            ("continuum_wake", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_receiver", "LOGIN", secrets.token_urlsafe(32)),
            (RUNTIME_ROLE, "LOGIN", runtime_password),
        )
        for role_name, login_mode, password in roles:
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
        version = connection.execute(
            text("SELECT current_setting('server_version_num')::integer")
        ).scalar_one()
    if not 170000 <= version < 180000:
        pytest.fail("T21 integration requires PostgreSQL 17")


def _run_migration(action: str, revision: str, database_url: str) -> None:
    environment = os.environ.copy()
    environment["CONTINUUM_MIGRATION_DATABASE_URL"] = database_url
    result = subprocess.run(
        [sys.executable, "-m", "continuum_api.migrations", action, revision],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(f"T21 migration {action} {revision} failed with exit code {result.returncode}")


def _cleanup_database(engine: Engine) -> None:
    with engine.begin() as connection:
        _drop_schemas(connection)
        for role_name in ROLE_NAMES:
            exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)"),
                {"name": role_name},
            ).scalar_one()
            if exists:
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE {role_name}"))


_LOCAL_ARCHIVE_WORKER = r"""
import json
import os
import sys
from pathlib import Path
from continuum_api.artifacts.archives import verify_archive

committed_path, unattached_path, committed_archive_digest, committed_tree_digest = sys.argv[1:]
unattached = Path(unattached_path).read_bytes()
verify_archive(unattached)
print(json.dumps({"kind": "unattached_object_written"}), flush=True)
sys.stdin.readline()
"""

_LOCAL_ARCHIVE_RESTORER = r"""
import base64
import json
import sys
from pathlib import Path
from continuum_api.artifacts.archives import verify_archive

archive_path, archive_digest, tree_digest = sys.argv[1:]
verified = verify_archive(
    Path(archive_path).read_bytes(),
    expected_archive_sha256=archive_digest,
    expected_tree_sha256=tree_digest,
)
print(json.dumps({
    "archive_sha256": verified.archive_sha256,
    "tree_sha256": verified.manifest.tree_sha256,
    "files": [
        {
            "path": item.path,
            "mode": item.mode,
            "content_b64": base64.b64encode(item.content).decode("ascii"),
        }
        for item in verified.files
    ],
}, sort_keys=True))
"""

_WORKER_CODE = r"""
import asyncio
import base64
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from uuid import UUID

import httpx
from sqlalchemy import create_engine

from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.connectivity.storage import SupabaseStorage
from continuum_api.repositories.preflight import (
    GitHubRepositoryAdapter,
    RepositoryAccessProof,
    RepositoryMetadata,
)
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import ExecutionLease, QueueService
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus
from continuum_api.runtime.scheduler import RuntimeRecoveryService
from continuum_api.runtime.transitions import NextWork, TransitionService
from continuum_api.runtime.workspace_recovery import WorkspaceRecoveryService
from continuum_api.tools.contracts import TextEdit
from continuum_api.tools.source_tools import apply_text_edits

STORAGE_URL = "https://fixture-project.supabase.co"
STORAGE_SECRET = "sb_secret_t21_process_fixture_only"
STORAGE_BUCKET = "continuum-checkpoints"
owner_id, run_id, objects_arg, mode, fault_point, replacement_text, prior_lease_json = sys.argv[1:]
fault_point = None if fault_point == "-" else fault_point
objects = Path(objects_arg)
engine = create_engine(
    os.environ["CONTINUUM_T21_RUNTIME_DATABASE_URL"],
    hide_parameters=True,
    pool_size=1,
    max_overflow=0,
    connect_args={
        "application_name": "t21-workspace-recovery-worker",
        "connect_timeout": 5,
        "options": "-c statement_timeout=5000 -c lock_timeout=2000",
    },
)
principal = Principal(UUID(owner_id))

def emit(payload):
    print(json.dumps(payload, sort_keys=True), flush=True)

def barrier(point):
    emit({"kind": "barrier", "point": point})
    sys.stdin.readline()

def key_path(key):
    target = (objects / key).resolve()
    if not target.is_relative_to(objects.resolve()):
        raise RuntimeError("storage_fixture_path_invalid")
    return target

def storage_handler(request):
    if request.method == "GET" and request.url.path.endswith(f"/bucket/{STORAGE_BUCKET}"):
        return httpx.Response(200, json={"id": STORAGE_BUCKET, "public": False})
    upload_prefix = f"/storage/v1/object/{STORAGE_BUCKET}/"
    if request.method == "POST" and request.url.path.startswith(upload_prefix):
        key = request.url.path.removeprefix(upload_prefix)
        content = request.read()
        if fault_point == "before_upload":
            barrier("before_upload")
        target = key_path(key)
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as object_file:
            object_file.write(content)
            object_file.flush()
            os.fsync(object_file.fileno())
        return httpx.Response(200, json={"Key": key})
    download_prefix = f"/storage/v1/object/authenticated/{STORAGE_BUCKET}/"
    if request.method == "GET" and request.url.path.startswith(download_prefix):
        key = request.url.path.removeprefix(download_prefix)
        try:
            content = key_path(key).read_bytes()
        except FileNotFoundError:
            return httpx.Response(404)
        return httpx.Response(200, content=content)
    raise RuntimeError("storage_fixture_unexpected_request")

class AccessVerifier:
    async def verify(self, *, repository_id, github_user_id):
        return RepositoryAccessProof(repository_id, github_user_id, True, True)

class PublicRepositoryAdapter(GitHubRepositoryAdapter):
    async def current_public_repository(self, repository_id):
        return RepositoryMetadata(repository_id, "fixture-owner", "fixture-project", 1)
    async def aclose(self):
        return

class BarrierTransitions:
    def __init__(self, wrapped):
        self._wrapped = wrapped
    def commit_result(self, **kwargs):
        if fault_point == "before_result_commit":
            barrier("before_result_commit")
        receipt = self._wrapped.commit_result(**kwargs)
        if fault_point == "after_result_commit":
            emit({
                "kind": "result_committed",
                "result_id": str(receipt.result_id),
                "artifact_id": None if receipt.artifact_id is None else str(receipt.artifact_id),
                "event_sequence": receipt.event_sequence,
            })
            barrier("after_result_commit")
        return receipt

try:
    storage = SupabaseStorage(
        STORAGE_URL,
        STORAGE_SECRET,
        STORAGE_BUCKET,
        transport=httpx.MockTransport(storage_handler),
        fixture_byte_limit=10 * 1024 * 1024,
        request_deadline_seconds=5.0,
    )
    persistence = ArtifactPersistence(engine)
    base_transition = TransitionService(engine, principal, artifact_persistence=persistence)
    checkpoints = CheckpointStorageService(
        persistence,
        storage,
        BarrierTransitions(base_transition),
        RuntimeRecoveryService(engine, principal),
        access_verifier=AccessVerifier(),
        github_adapter_factory=PublicRepositoryAdapter,
    )
    recovery = WorkspaceRecoveryService(engine, principal, checkpoints, base_transition)
    if prior_lease_json != "-":
        previous = json.loads(prior_lease_json)
        old_lease = ExecutionLease(
            owner_id=UUID(previous["owner_id"]),
            run_id=UUID(previous["run_id"]),
            step_id=UUID(previous["step_id"]),
            job_id=UUID(previous["job_id"]),
            attempt_id=UUID(previous["attempt_id"]),
            generation=previous["generation"],
            lease_owner=previous["lease_owner"],
            lease_expires_at=datetime.fromisoformat(previous["lease_expires_at"]),
        )
        recovered_operation = asyncio.run(recovery.recover(principal, old_lease))
        accepted = recovered_operation.accepted_result
        if accepted is None or recovered_operation.workspace is not None:
            emit({"kind": "worker_error", "code": "accepted_result_missing"})
            raise SystemExit(2)
        emit({
            "kind": "accepted_result_adopted",
            "artifact_id": None if accepted.artifact_id is None else str(accepted.artifact_id),
            "result_id": str(accepted.result_id),
            "candidate": accepted.result.get("candidate"),
        })
    queue = QueueService(engine, principal)
    lease = queue.claim_one("t21-subprocess-" + mode)
    if lease is None:
        emit({"kind": "worker_error", "code": "claim_missing"})
        raise SystemExit(2)
    snapshot = asyncio.run(recovery.restore_latest(principal, lease))
    emit({
        "kind": "restored",
        "artifact_id": str(snapshot.artifact_id),
        "archive_sha256": snapshot.verified.archive_sha256,
        "tree_sha256": snapshot.verified.manifest.tree_sha256,
        "files": [
            {
                "path": item.path,
                "mode": item.mode,
                "content_b64": base64.b64encode(item.content).decode("ascii"),
            }
            for item in snapshot.verified.files
        ],
    })
    lease_message = {
        "kind": "lease_claimed",
        "owner_id": str(lease.owner_id),
        "run_id": str(lease.run_id),
        "step_id": str(lease.step_id),
        "job_id": str(lease.job_id),
        "attempt_id": str(lease.attempt_id),
        "generation": lease.generation,
        "lease_owner": lease.lease_owner,
        "lease_expires_at": lease.lease_expires_at.isoformat(),
    }
    emit(lease_message)
    if mode == "restore":
        emit({"kind": "worker_done"})
    elif mode == "accept":
        candidate = apply_text_edits(
            snapshot.source,
            [
                TextEdit(
                    path="src/app.py",
                    expected_text="return 'base'",
                    replacement_text="return '" + replacement_text + "'",
                )
            ],
            expected_tree_sha256=snapshot.source.tree_sha256,
            authorized_write_paths={"src/app.py"},
        )
        if fault_point == "after_sandbox_result":
            barrier("after_sandbox_result")
        next_work = NextWork(
            "workspace:after", "agent_work", replay_policy=ReplayPolicy.REPLAY_SAFE
        )
        receipt = asyncio.run(
            recovery.accept_candidate(
                principal,
                lease,
                "tool:replace-answer",
                snapshot,
                candidate,
                result={"candidate": replacement_text, "sandbox_result": "late-or-current"},
                target_run_status=RunStatus.RUNNING,
                next_work=next_work,
            )
        )
        emit({
            "kind": "accepted",
            "generation": lease.generation,
            "artifact_id": str(receipt.artifact_id),
            "adopted": receipt.adopted,
            "result_id": str(receipt.transition.result_id),
        })
    else:
        emit({"kind": "worker_error", "code": "mode_invalid"})
        raise SystemExit(2)
    asyncio.run(storage.aclose())
except BaseException as error:
    code = getattr(error, "code", None)
    if code in {
        "artifact_lease_lost",
        "workspace_lease_lost",
        "workspace_activity_already_accepted",
    }:
        emit({"kind": "late_result_rejected", "code": code})
        asyncio.run(storage.aclose())
    else:
        emit({"kind": "worker_error", "code": code or type(error).__name__})
        raise SystemExit(2) from None
finally:
    engine.dispose()
"""
