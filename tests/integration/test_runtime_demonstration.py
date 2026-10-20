"""Controlled process-death and paused-stale-worker proof on PostgreSQL 17."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import select
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url

from continuum_api.app import create_app
from continuum_api.runtime.leases import ExecutionLease, QueueService
from continuum_api.runtime.persistence import (
    EventActor,
    Principal,
    ReplayPolicy,
    RunStatus,
    RuntimeRepository,
)
from continuum_api.runtime.scheduler import RuntimeRecoveryService
from continuum_api.runtime.transitions import TransitionService

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_checkpoint_d_test"
LATEST_REVISION = "20261011_07"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)

_WORKER_CODE = r"""
import json
import os
import sys
from uuid import UUID

from sqlalchemy import create_engine, text

from continuum_api.runtime.leases import LeaseLost, QueueService
from continuum_api.runtime.persistence import Principal, RunStatus
from continuum_api.runtime.transitions import TransitionService

engine = create_engine(
    os.environ["CONTINUUM_CHECKPOINT_D_RUNTIME_DATABASE_URL"],
    hide_parameters=True,
    pool_size=1,
    max_overflow=0,
    connect_args={
        "application_name": "checkpoint-d-worker",
        "connect_timeout": 5,
        "options": "-c statement_timeout=5000 -c lock_timeout=2000",
    },
)
principal = Principal(UUID(sys.argv[1]))
run_id = UUID(sys.argv[2])
mode = sys.argv[3]
try:
    queue = QueueService(engine, principal)
    lease = queue.claim_one(f"checkpoint-d-{mode}-worker")
    if lease is None or lease.run_id != run_id:
        print(json.dumps({"kind": "worker_error", "code": "claim_missing"}), flush=True)
        sys.exit(2)
    receipt = queue.persist_checkpoint(
        lease,
        "after_prepare",
        {"durable_marker": mode, "committed": True},
    )
    with engine.connect() as connection:
        backend_pid = connection.execute(text("SELECT pg_backend_pid()")).scalar_one()
    print(
        json.dumps(
            {
                "kind": "ready",
                "run_id": str(lease.run_id),
                "step_id": str(lease.step_id),
                "job_id": str(lease.job_id),
                "attempt_id": str(lease.attempt_id),
                "generation": lease.generation,
                "lease_owner": lease.lease_owner,
                "backend_pid": backend_pid,
                "checkpoint_id": str(receipt.checkpoint_id),
                "checkpoint_adopted": receipt.adopted,
            }
        ),
        flush=True,
    )
    if sys.stdin.readline().strip() == "attempt_stale_writes":
        outcomes = {}
        try:
            queue.persist_checkpoint(
                lease,
                "stale_after_resume",
                {"must_not_commit": True},
            )
        except LeaseLost:
            outcomes["checkpoint"] = "lease_lost"
        else:
            outcomes["checkpoint"] = "unexpectedly_committed"
        try:
            TransitionService(engine, principal).commit_result(
                lease=lease,
                result={"stale_generation": lease.generation},
                target_run_status=RunStatus.WAITING_CREDENTIALS,
            )
        except LeaseLost:
            outcomes["transition"] = "lease_lost"
        else:
            outcomes["transition"] = "unexpectedly_committed"
        print(json.dumps({"kind": "stale_write_results", **outcomes}), flush=True)
except BaseException:
    print(json.dumps({"kind": "worker_error", "code": "worker_operation_failed"}), flush=True)
    sys.exit(2)
finally:
    engine.dispose()
"""


@dataclass(frozen=True, slots=True)
class RunFixture:
    owner_id: UUID
    run_id: UUID
    step_id: UUID
    job_id: UUID
    scenario: str

    @property
    def principal(self) -> Principal:
        return Principal(self.owner_id)


@dataclass(frozen=True, slots=True)
class VerifiedCheckpoint:
    checkpoint_id: UUID
    attempt_id: UUID
    generation: int
    digest_hex: str
    payload: dict[str, Any]


@dataclass(slots=True)
class CheckpointDDatabase:
    admin_engine: Engine
    runtime_engine: Engine
    runtime_url: URL
    database_url: str
    runs: list[RunFixture] = field(default_factory=list)


@pytest.fixture(scope="module")
def checkpoint_d_database() -> Any:
    database_url = os.environ.get("CONTINUUM_CHECKPOINT_D_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip(
            "set CONTINUUM_CHECKPOINT_D_TEST_DATABASE_URL to disposable loopback PostgreSQL 17"
        )
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("Checkpoint D requires loopback PostgreSQL named continuum_checkpoint_d_test")

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
    retained_dir = _retention_directory()
    store: CheckpointDDatabase | None = None

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
                "application_name": "checkpoint-d-test-controller",
                "connect_timeout": 5,
                "options": "-c statement_timeout=5000 -c lock_timeout=2000",
            },
        )
        store = CheckpointDDatabase(admin_engine, runtime_engine, runtime_url, database_url)
        yield store
    finally:
        if runtime_engine is not None:
            runtime_engine.dispose()
        try:
            if retained_dir is not None and store is not None:
                _write_retained_visual_fixture(retained_dir, store, runtime_url)
            else:
                if migration_applied:
                    _run_migration("downgrade", "base", database_url)
                _cleanup_database(admin_engine)
        finally:
            admin_engine.dispose()


@pytest.fixture
def checkpoint_d_workers() -> Any:
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


def test_sigkill_keeps_checkpoint_and_only_recovered_generation_commits(
    checkpoint_d_database: CheckpointDDatabase,
    checkpoint_d_workers: list[subprocess.Popen[str]],
    record_property: Any,
) -> None:
    store = checkpoint_d_database
    admin_engine, runtime_engine, runtime_url = (
        store.admin_engine,
        store.runtime_engine,
        store.runtime_url,
    )
    run = _seed_run(admin_engine, runtime_engine, "sigkill")
    store.runs.append(run)

    killed_worker = _start_worker(runtime_url, run, mode="sigkill")
    checkpoint_d_workers.append(killed_worker)
    killed_ready = _read_worker_message(killed_worker)
    _assert_ready_message(killed_ready, run, generation=1)

    record_property(
        "intermediate_authority",
        json.dumps(
            {
                "generation": killed_ready["generation"],
                "checkpoint_id": killed_ready["checkpoint_id"],
            }
        ),
    )
    killed_worker.send_signal(signal.SIGKILL)
    assert killed_worker.wait(timeout=5) == -signal.SIGKILL
    _assert_checkpoint(runtime_engine, run, killed_ready, marker="sigkill")
    _append_observed_checkpoint_event(runtime_engine, run, killed_ready)

    _expire_database_lease(admin_engine, run, killed_ready)
    recovery = RuntimeRecoveryService(runtime_engine, run.principal)
    assert recovery.recover_due(limit=1) == 1
    assert recovery.recover_due(limit=1) == 0
    replacement = _claim_replacement(runtime_engine, run, "after-sigkill")
    assert replacement.generation == 2
    recovered_checkpoint = _verify_previous_checkpoint(runtime_engine, run, killed_ready)
    _commit_generation_two(runtime_engine, run, replacement, recovered_checkpoint)
    _assert_only_generation_two_result(runtime_engine, run, replacement, recovered_checkpoint)
    _assert_timeline_api(runtime_engine, [run], stale_write_runs=set())
    record_property(
        "final_authority",
        json.dumps(
            {
                "generation": replacement.generation,
                "status": "waiting_credentials",
                "accepted_results": 1,
                "recovered_checkpoint_id": str(recovered_checkpoint.checkpoint_id),
            }
        ),
    )


def test_sigstop_rejects_stale_intermediate_and_final_writes(
    checkpoint_d_database: CheckpointDDatabase,
    checkpoint_d_workers: list[subprocess.Popen[str]],
    record_property: Any,
) -> None:
    store = checkpoint_d_database
    admin_engine, runtime_engine, runtime_url = (
        store.admin_engine,
        store.runtime_engine,
        store.runtime_url,
    )
    run = _seed_run(admin_engine, runtime_engine, "sigstop")
    store.runs.append(run)

    paused_worker = _start_worker(runtime_url, run, mode="sigstop")
    checkpoint_d_workers.append(paused_worker)
    paused_ready = _read_worker_message(paused_worker)
    _assert_ready_message(paused_ready, run, generation=1)
    _assert_checkpoint(runtime_engine, run, paused_ready, marker="sigstop")
    _append_observed_checkpoint_event(runtime_engine, run, paused_ready)

    record_property(
        "intermediate_authority",
        json.dumps(
            {
                "generation": paused_ready["generation"],
                "checkpoint_id": paused_ready["checkpoint_id"],
            }
        ),
    )
    paused_worker.send_signal(signal.SIGSTOP)
    _wait_for_stopped_process(paused_worker.pid)
    _assert_backend_has_no_transaction_or_locks(admin_engine, paused_ready["backend_pid"])

    _expire_database_lease(admin_engine, run, paused_ready)
    recovery = RuntimeRecoveryService(runtime_engine, run.principal)
    assert recovery.recover_due(limit=1) == 1
    assert recovery.recover_due(limit=1) == 0
    replacement = _claim_replacement(runtime_engine, run, "after-sigstop")
    assert replacement.generation == 2

    checkpoint_two = QueueService(runtime_engine, run.principal).persist_checkpoint(
        replacement,
        "after_recovery",
        {"durable_marker": "generation-2", "committed": True},
    )
    assert not checkpoint_two.adopted
    generation_two = {
        "attempt_id": str(replacement.attempt_id),
        "generation": replacement.generation,
        "job_id": str(replacement.job_id),
        "step_id": str(replacement.step_id),
        "checkpoint_id": str(checkpoint_two.checkpoint_id),
    }
    _append_observed_checkpoint_event(runtime_engine, run, generation_two)
    _assert_checkpoint(
        runtime_engine,
        run,
        generation_two,
        marker="generation-2",
        checkpoint_key="after_recovery",
    )

    paused_worker.stdin.write("attempt_stale_writes\n")
    paused_worker.stdin.flush()
    paused_worker.send_signal(signal.SIGCONT)
    stale_results = _read_worker_message(paused_worker)
    assert stale_results == {
        "kind": "stale_write_results",
        "checkpoint": "lease_lost",
        "transition": "lease_lost",
    }
    assert paused_worker.wait(timeout=5) == 0
    _assert_checkpoint_missing(
        runtime_engine,
        run,
        paused_ready,
        checkpoint_key="stale_after_resume",
    )
    _append_stale_write_rejected_event(runtime_engine, run, paused_ready)
    recovered_checkpoint = _verify_previous_checkpoint(runtime_engine, run, paused_ready)
    _commit_generation_two(runtime_engine, run, replacement, recovered_checkpoint)
    _assert_only_generation_two_result(runtime_engine, run, replacement, recovered_checkpoint)
    _assert_timeline_api(runtime_engine, [run], stale_write_runs={run.run_id})
    record_property(
        "final_authority",
        json.dumps(
            {
                "generation": replacement.generation,
                "stale_writes": stale_results,
                "accepted_results": 1,
                "status": "waiting_credentials",
            }
        ),
    )


def _role_url(url: URL, password: str) -> URL:
    return url.set(
        drivername="postgresql+psycopg",
        username=RUNTIME_ROLE,
        password=password,
        query={"sslmode": "disable"},
    )


def _retention_directory() -> Path | None:
    configured = os.environ.get("CONTINUUM_CHECKPOINT_D_RETAIN_PATH")
    if not configured:
        return None
    path = Path(configured).expanduser()
    resolved = path.resolve()
    tmp_root = Path("/tmp").resolve()
    if not resolved.is_relative_to(tmp_root) or resolved == tmp_root or path.is_symlink():
        pytest.fail("Checkpoint D retention path must be a real directory under /tmp")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def _write_retained_visual_fixture(
    directory: Path,
    store: CheckpointDDatabase,
    runtime_url: URL,
) -> None:
    run_state = {
        "database_name": DATABASE_NAME,
        "runs": [
            {
                "scenario": run.scenario,
                "owner_id": str(run.owner_id),
                "run_id": str(run.run_id),
                "url_path": f"/runs/{run.run_id}",
            }
            for run in store.runs
        ],
    }
    private_values = {
        "runtime_database_url": runtime_url.render_as_string(hide_password=False),
        "visual_state.json": json.dumps(run_state, sort_keys=True) + "\n",
    }
    for filename, value in private_values.items():
        target = directory / filename
        descriptor = os.open(
            target,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as private_file:
            private_file.write(value)
            private_file.flush()
            os.fsync(private_file.fileno())
        os.chmod(target, 0o600)


def _seed_run(admin_engine: Engine, runtime_engine: Engine, scenario: str) -> RunFixture:
    owner_id = uuid4()
    github_user_id = 930_000_000 + owner_id.int % 1_000_000
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:owner_id, :github_user_id)"
            ),
            {"owner_id": owner_id, "github_user_id": github_user_id},
        )
    return _seed_queued_work(runtime_engine, owner_id, scenario)


def _seed_queued_work(engine: Engine, owner_id: UUID, scenario: str) -> RunFixture:
    principal = Principal(owner_id)
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run(deadline_at=datetime.now(UTC) + timedelta(hours=1))
        step_id = repository.create_step(run_id, "demonstrate:recovery", "demonstrate")
        job_id = repository.create_job(
            run_id,
            step_id,
            replay_policy=ReplayPolicy.REPLAY_SAFE,
        )
    return RunFixture(owner_id, run_id, step_id, job_id, scenario)


def _start_worker(runtime_url: URL, run: RunFixture, *, mode: str) -> subprocess.Popen[str]:
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONUNBUFFERED": "1",
        "CONTINUUM_CHECKPOINT_D_RUNTIME_DATABASE_URL": runtime_url.render_as_string(
            hide_password=False
        ),
    }
    return subprocess.Popen(
        [sys.executable, "-c", _WORKER_CODE, str(run.owner_id), str(run.run_id), mode],
        cwd=PROJECT_ROOT,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


def _read_worker_message(worker: subprocess.Popen[str], *, timeout: float = 10.0) -> dict[str, Any]:
    if worker.stdout is None:
        pytest.fail("worker_stdout_unavailable")
    ready, _, _ = select.select([worker.stdout], [], [], timeout)
    if not ready:
        pytest.fail("worker_message_timed_out")
    line = worker.stdout.readline()
    try:
        message = json.loads(line)
    except json.JSONDecodeError:
        pytest.fail("worker_message_invalid")
    if not isinstance(message, dict) or message.get("kind") == "worker_error":
        pytest.fail("worker_operation_failed")
    return message


def _assert_ready_message(message: dict[str, Any], run: RunFixture, *, generation: int) -> None:
    assert message["kind"] == "ready"
    assert message["run_id"] == str(run.run_id)
    assert message["step_id"] == str(run.step_id)
    assert message["job_id"] == str(run.job_id)
    assert message["generation"] == generation
    assert message["checkpoint_adopted"] is False


def _assert_checkpoint(
    engine: Engine,
    run: RunFixture,
    message: dict[str, Any],
    *,
    marker: str,
    checkpoint_key: str = "after_prepare",
) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        checkpoint = (
            connection.execute(
                text(
                    "SELECT attempt_id, generation, payload "
                    "FROM runtime_private.attempt_checkpoints "
                    "WHERE owner_id=:owner_id AND run_id=:run_id "
                    "AND checkpoint_key=:checkpoint_key"
                ),
                {
                    "owner_id": run.owner_id,
                    "run_id": run.run_id,
                    "checkpoint_key": checkpoint_key,
                },
            )
            .mappings()
            .one_or_none()
        )
    assert checkpoint is not None
    assert str(checkpoint["attempt_id"]) == str(message["attempt_id"])
    assert checkpoint["generation"] == message["generation"]
    assert checkpoint["payload"]["durable_marker"] == marker
    assert checkpoint["payload"]["committed"] is True


def _assert_checkpoint_missing(
    engine: Engine,
    run: RunFixture,
    message: dict[str, Any],
    *,
    checkpoint_key: str,
) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.attempt_checkpoints "
                "WHERE owner_id=:owner_id AND run_id=:run_id "
                "AND attempt_id=:attempt_id AND checkpoint_key=:checkpoint_key"
            ),
            {
                "owner_id": run.owner_id,
                "run_id": run.run_id,
                "attempt_id": UUID(str(message["attempt_id"])),
                "checkpoint_key": checkpoint_key,
            },
        ).scalar_one()
    assert count == 0


def _verify_previous_checkpoint(
    engine: Engine,
    run: RunFixture,
    original_worker: dict[str, Any],
) -> VerifiedCheckpoint:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        row = (
            connection.execute(
                text(
                    "SELECT checkpoint_id, attempt_id, generation, payload_sha256, payload "
                    "FROM runtime_private.attempt_checkpoints "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND attempt_id=:attempt_id "
                    "AND checkpoint_key='after_prepare'"
                ),
                {
                    "owner_id": run.owner_id,
                    "run_id": run.run_id,
                    "attempt_id": UUID(str(original_worker["attempt_id"])),
                },
            )
            .mappings()
            .one()
        )
    encoded_payload = json.dumps(
        row["payload"], ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    calculated_digest = hashlib.sha256(encoded_payload).digest()
    assert calculated_digest == row["payload_sha256"]
    assert row["generation"] == 1
    return VerifiedCheckpoint(
        checkpoint_id=row["checkpoint_id"],
        attempt_id=row["attempt_id"],
        generation=row["generation"],
        digest_hex=calculated_digest.hex(),
        payload=row["payload"],
    )


def _append_observed_checkpoint_event(
    engine: Engine, run: RunFixture, message: dict[str, Any]
) -> None:
    _append_system_event(
        engine,
        run,
        "runtime.checkpoint_committed",
        {
            "attempt_id": str(message["attempt_id"]),
            "generation": message["generation"],
            "job_id": str(message["job_id"]),
            "step_id": str(message["step_id"]),
        },
    )


def _append_stale_write_rejected_event(
    engine: Engine, run: RunFixture, message: dict[str, Any]
) -> None:
    _append_system_event(
        engine,
        run,
        "runtime.stale_write_rejected",
        {
            "attempt_id": str(message["attempt_id"]),
            "generation": message["generation"],
            "job_id": str(message["job_id"]),
            "step_id": str(message["step_id"]),
        },
    )


def _append_system_event(
    engine: Engine,
    run: RunFixture,
    event_type: str,
    payload: dict[str, object],
) -> None:
    with engine.begin() as connection:
        repository = RuntimeRepository(connection, run.principal)
        connection.execute(
            text(
                "SELECT run_id FROM runtime_private.runs "
                "WHERE owner_id=:owner_id AND run_id=:run_id FOR UPDATE"
            ),
            {"owner_id": run.owner_id, "run_id": run.run_id},
        ).one()
        next_sequence = connection.execute(
            text(
                "SELECT coalesce(max(sequence), 0) + 1 FROM runtime_private.events "
                "WHERE owner_id=:owner_id AND run_id=:run_id"
            ),
            {"owner_id": run.owner_id, "run_id": run.run_id},
        ).scalar_one()
        repository.append_event(
            run.run_id,
            next_sequence,
            event_type,
            EventActor.SYSTEM,
            payload=payload,
        )


def _expire_database_lease(
    admin_engine: Engine,
    run: RunFixture,
    message: dict[str, Any],
) -> None:
    """Shorten only this disposable fixture's lease using PostgreSQL time."""
    with admin_engine.begin() as connection:
        expired = connection.execute(
            text(
                "UPDATE runtime_private.jobs "
                "SET lease_expires_at=clock_timestamp() - interval '1 second' "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id "
                "AND generation=:generation AND status='leased' "
                "RETURNING job_id"
            ),
            {
                "owner_id": run.owner_id,
                "run_id": run.run_id,
                "job_id": run.job_id,
                "generation": message["generation"],
            },
        ).one_or_none()
    assert expired is not None


def _claim_replacement(engine: Engine, run: RunFixture, name: str) -> ExecutionLease:
    lease = QueueService(engine, run.principal).claim_one(f"checkpoint-d-{name}")
    assert lease is not None
    assert lease.job_id == run.job_id
    return lease


def _commit_generation_two(
    engine: Engine,
    run: RunFixture,
    lease: ExecutionLease,
    checkpoint: VerifiedCheckpoint,
) -> None:
    receipt = TransitionService(engine, run.principal).commit_result(
        lease=lease,
        result={
            "generation": lease.generation,
            "recovered_checkpoint_id": str(checkpoint.checkpoint_id),
            "recovered_checkpoint_sha256": checkpoint.digest_hex,
            "recovered_checkpoint_generation": checkpoint.generation,
            "recovered_checkpoint_attempt_id": str(checkpoint.attempt_id),
            "durable_marker": checkpoint.payload["durable_marker"],
            "checkpoint_committed": checkpoint.payload["committed"],
        },
        target_run_status=RunStatus.WAITING_CREDENTIALS,
    )
    assert receipt.run_status is RunStatus.WAITING_CREDENTIALS
    assert not receipt.adopted


def _assert_only_generation_two_result(
    engine: Engine,
    run: RunFixture,
    lease: ExecutionLease,
    checkpoint: VerifiedCheckpoint,
) -> None:
    with engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        rows = (
            connection.execute(
                text(
                    "SELECT generation, attempt_id, payload FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id"
                ),
                {"owner_id": run.owner_id, "run_id": run.run_id, "job_id": run.job_id},
            )
            .mappings()
            .all()
        )
    assert len(rows) == 1
    assert rows[0]["generation"] == 2
    assert rows[0]["attempt_id"] == lease.attempt_id
    assert rows[0]["payload"]["generation"] == 2
    assert rows[0]["payload"]["recovered_checkpoint_id"] == str(checkpoint.checkpoint_id)
    assert rows[0]["payload"]["recovered_checkpoint_sha256"] == checkpoint.digest_hex
    assert rows[0]["payload"]["recovered_checkpoint_generation"] == 1
    assert rows[0]["payload"]["recovered_checkpoint_attempt_id"] == str(checkpoint.attempt_id)
    assert rows[0]["payload"]["durable_marker"] == checkpoint.payload["durable_marker"]
    assert rows[0]["payload"]["checkpoint_committed"] is True
    assert lease.generation == 2


def _wait_for_stopped_process(pid: int, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["ps", "-o", "state=", "-p", str(pid)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip().startswith("T"):
            return
        time.sleep(0.05)
    pytest.fail("worker_stop_not_observed")


def _assert_backend_has_no_transaction_or_locks(admin_engine: Engine, backend_pid: int) -> None:
    with admin_engine.connect() as connection:
        backend = connection.execute(
            text("SELECT state, xact_start FROM pg_stat_activity WHERE pid=:backend_pid"),
            {"backend_pid": backend_pid},
        ).one_or_none()
        lock_count = connection.execute(
            text("SELECT count(*) FROM pg_locks WHERE pid=:backend_pid AND granted"),
            {"backend_pid": backend_pid},
        ).scalar_one()
    assert backend is not None
    assert backend.state == "idle"
    assert backend.xact_start is None
    assert lock_count == 0


def _assert_timeline_api(
    engine: Engine,
    runs: list[RunFixture],
    *,
    stale_write_runs: set[UUID],
) -> None:
    principals = {run.run_id: run.principal for run in runs}

    def resolve_test_principal(request: Any) -> Principal:
        return principals[UUID(request.path_params["run_id"])]

    application = create_app(
        static_dir=PROJECT_ROOT / "frontend" / "dist",
        principal_resolver=resolve_test_principal,
        runtime_engine=engine,
    )

    async def fetch() -> list[tuple[dict[str, Any], dict[str, Any]]]:
        transport = httpx.ASGITransport(app=application)
        results = []
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            for run in runs:
                snapshot_response = await client.get(f"/api/v1/runs/{run.run_id}")
                events_response = await client.get(
                    f"/api/v1/runs/{run.run_id}/events?after=0&limit=100"
                )
                assert snapshot_response.status_code == 200
                assert events_response.status_code == 200
                results.append((snapshot_response.json(), events_response.json()))
        return results

    results = asyncio.run(fetch())
    for run, (snapshot, page) in zip(runs, results, strict=True):
        assert snapshot["status"] == "waiting_credentials"
        assert page["history_gap"] is False
        event_types = [event["type"] for event in page["events"]]
        assert "job.claimed" in event_types
        assert "runtime.checkpoint_committed" in event_types
        assert "job.lease_expired_requeued" in event_types
        assert "activity.result_accepted" in event_types
        for event in page["events"]:
            if event["type"] == "activity.result_accepted":
                assert event["data"]["target_run_status"] == "waiting_credentials"
        if run.run_id in stale_write_runs:
            assert "runtime.stale_write_rejected" in event_types
        assert [int(event["sequence"]) for event in page["events"]] == list(
            range(1, len(page["events"]) + 1)
        )


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
        pytest.fail(f"Checkpoint D migration {action} failed; output redacted")


def _reset_database(engine: Engine, runtime_password: str) -> None:
    with engine.begin() as connection:
        for schema_name in (
            "runtime_private",
            "wake_private",
            "webhook_private",
            "continuum_migrations",
        ):
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        for role_name in ROLE_NAMES:
            exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role_name)"),
                {"role_name": role_name},
            ).scalar_one()
            if exists:
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE {role_name}"))

        role_passwords = {
            "continuum_ingress": secrets.token_urlsafe(32),
            "continuum_wake": secrets.token_urlsafe(32),
            "continuum_wake_receiver": secrets.token_urlsafe(32),
            RUNTIME_ROLE: runtime_password,
        }
        for role_name in ROLE_NAMES:
            password = role_passwords.get(role_name)
            login_mode = "NOLOGIN" if password is None else "LOGIN"
            if password is None:
                password_sql = ""
            else:
                quoted_password = connection.execute(
                    text("SELECT quote_literal(:password)"), {"password": password}
                ).scalar_one()
                password_sql = f"PASSWORD {quoted_password}"
            connection.exec_driver_sql(
                f"CREATE ROLE {role_name} {login_mode} NOSUPERUSER NOCREATEDB "
                f"NOCREATEROLE NOINHERIT NOBYPASSRLS {password_sql}"
            )


def _assert_postgres_17(engine: Engine) -> None:
    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT current_setting('server_version_num')::integer")
        ).scalar_one()
    if not 170000 <= version < 180000:
        pytest.fail("Checkpoint D integration requires PostgreSQL 17")


def _cleanup_database(engine: Engine) -> None:
    with engine.begin() as connection:
        for schema_name in (
            "runtime_private",
            "wake_private",
            "webhook_private",
            "continuum_migrations",
        ):
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        for role_name in ROLE_NAMES:
            exists = connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role_name)"),
                {"role_name": role_name},
            ).scalar_one()
            if exists:
                connection.execute(
                    text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role_name}")
                )
                connection.execute(text(f"DROP ROLE {role_name}"))
