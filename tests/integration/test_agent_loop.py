"""Deterministic T22 durable-turn and tool-invocation recovery proof."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, IntegrityError

from continuum_api.artifacts.archives import ArchiveFile, create_archive, verify_archive
from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.connectivity.storage import SupabaseStorage
from continuum_api.repositories.preflight import (
    GitHubRepositoryAdapter,
    RepositoryAccessProof,
    RepositoryMetadata,
)
from continuum_api.runtime.agent_loop import (
    AgentLoopError,
    DurableAgentLoopService,
    ScriptedModelRequest,
    ScriptedModelResponse,
    ScriptedResponseQueue,
    ScriptedToolCall,
    _validate_complete_response,
)
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus, RuntimeRepository
from continuum_api.runtime.scheduler import RuntimeRecoveryService
from continuum_api.runtime.transitions import NextWork, TransitionService
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.source_tools import (
    discover_baseline_test_paths,
    snapshot_from_verified_archive,
    source_paths,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t22_test"
LATEST_REVISION = "head"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_admission",
    "continuum_ingress",
    "continuum_recovery",
    "continuum_operator",
)
REPOSITORY_ID = 714_022
STORAGE_URL = "https://fixture-project.supabase.co"
STORAGE_SECRET = "sb_secret_t22_scripted_fixture_only"
STORAGE_BUCKET = "continuum-checkpoints"
_WORKER_LINE_BUFFERS: dict[int, bytearray] = {}


@dataclass(frozen=True, slots=True)
class T22Run:
    owner_id: UUID
    run_id: UUID
    base_step_id: UUID
    base_job_id: UUID
    turn_step_id: UUID
    turn_job_id: UUID
    base_artifact_id: UUID
    baseline_tree_sha256: str
    baseline_source_paths: tuple[str, ...]
    baseline_test_paths: tuple[str, ...]

    @property
    def principal(self) -> Principal:
        return Principal(self.owner_id)


@dataclass(slots=True)
class T22Database:
    admin_engine: Engine
    runtime_engine: Engine
    runtime_url: URL


@pytest.fixture(scope="module")
def t22_database() -> Any:
    database_url = os.environ.get("CONTINUUM_T22_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T22_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T22 integration requires loopback PostgreSQL named continuum_t22_test")

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
            pool_size=6,
            max_overflow=0,
            connect_args={
                "application_name": "t22-agent-loop-controller",
                "connect_timeout": 5,
                "options": "-c statement_timeout=5000 -c lock_timeout=2000",
            },
        )
        yield T22Database(admin_engine, runtime_engine, runtime_url)
        _run_migration("downgrade", "20261012_10", database_url)
        with admin_engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT to_regclass('runtime_private.agent_turns')")
                ).scalar_one()
                is None
            )
        legacy_objects = Path(tempfile.mkdtemp(prefix="continuum-t22-downgrade."))
        try:
            legacy = _seed_agent_run(
                T22Database(admin_engine, runtime_engine, runtime_url), legacy_objects
            )
            lease = QueueService(runtime_engine, legacy.principal).claim_one("t22-after-downgrade")
            assert lease is not None
            receipt = TransitionService(runtime_engine, legacy.principal).commit_result(
                lease=lease,
                result={"legacy_transition": True},
                target_run_status=RunStatus.WAITING_CREDENTIALS,
            )
            assert receipt.result == {"legacy_transition": True}
        finally:
            shutil.rmtree(legacy_objects)
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
        _close_process_pipes(worker)


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
            ("continuum_admission", "NOLOGIN", None),
            ("continuum_wake", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_receiver", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_recovery", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_operator", "LOGIN", secrets.token_urlsafe(32)),
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
        "recovery_private",
        "operator_private",
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
        pytest.fail("T22 integration requires PostgreSQL 17")


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
        pytest.fail(f"T22 migration {action} {revision} failed with exit code {result.returncode}")


def _cleanup_database(engine: Engine) -> None:
    with engine.begin() as connection:
        _drop_schemas(connection)
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


def _seed_agent_run(
    store: T22Database,
    objects: Path,
    *,
    baseline_files: tuple[ArchiveFile, ...] | None = None,
) -> T22Run:
    owner_id = uuid4()
    principal = Principal(owner_id)
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:owner_id, :github_user_id)"
            ),
            {"owner_id": owner_id, "github_user_id": 990_000_000 + owner_id.int % 1_000_000},
        )
    with store.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run(github_repository_id=REPOSITORY_ID, issue_number=1)
        base_step_id = repository.create_step(run_id, "source:initial", "source")
        base_job_id = repository.create_job(run_id, base_step_id)

    lease = QueueService(store.runtime_engine, principal).claim_one("t22-base-checkpoint")
    assert lease is not None
    assert lease.job_id == base_job_id
    storage, _requests = _storage_fixture(objects)
    persistence = ArtifactPersistence(store.runtime_engine)
    checkpoints = _checkpoint_service(store.runtime_engine, principal, persistence, storage)
    source_files = _base_source_files() if baseline_files is None else baseline_files
    baseline_bundle = create_archive(source_files)
    baseline_verified = verify_archive(
        baseline_bundle.archive,
        expected_archive_sha256=baseline_bundle.archive_sha256,
        expected_tree_sha256=baseline_bundle.manifest.tree_sha256,
        expected_manifest=baseline_bundle.manifest,
    )
    baseline_source = snapshot_from_verified_archive(
        baseline_verified,
        expected_archive_sha256=baseline_bundle.archive_sha256,
        expected_tree_sha256=baseline_bundle.manifest.tree_sha256,
    )
    baseline_test_paths = discover_baseline_test_paths(
        baseline_source,
        "python-pytest",
        expected_tree_sha256=baseline_source.tree_sha256,
    )
    receipt = asyncio.run(
        checkpoints.commit_checkpoint(
            principal,
            lease,
            "source:initial",
            source_files,
            result={"workspace": "initial", "source_tree_committed": True},
            target_run_status=RunStatus.RUNNING,
            next_work=NextWork(
                "agent-turn:primary",
                "agent_turn",
                replay_policy=ReplayPolicy.REPLAY_SAFE,
            ),
        )
    )
    assert receipt.transition.artifact_id == receipt.artifact_id
    assert receipt.transition.next_step_id is not None
    assert receipt.transition.next_job_id is not None
    asyncio.run(storage.aclose())
    return T22Run(
        owner_id,
        run_id,
        base_step_id,
        base_job_id,
        receipt.transition.next_step_id,
        receipt.transition.next_job_id,
        receipt.artifact_id,
        baseline_source.tree_sha256,
        source_paths(baseline_source),
        tuple(sorted(baseline_test_paths)),
    )


def _base_source_files() -> tuple[ArchiveFile, ...]:
    return (
        ArchiveFile(
            "pyproject.toml",
            b"[tool.pytest.ini_options]\ntestpaths = ['tests']\naddopts = '-ra'\n",
        ),
        ArchiveFile("src/app.py", b"def answer():\n    return 'base'\n"),
        ArchiveFile(
            "tests/test_app.py",
            b"from src.app import answer\n\ndef test_answer():\n    assert answer() == 'base'\n",
        ),
    )


def _storage_fixture(
    objects: Path,
) -> tuple[SupabaseStorage, list[httpx.Request]]:
    objects.mkdir(mode=0o700, parents=True, exist_ok=True)
    requests: list[httpx.Request] = []

    def object_path(key: str) -> Path:
        target = (objects / key).resolve()
        if not target.is_relative_to(objects.resolve()):
            raise AssertionError("fixture_object_path_invalid")
        return target

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET" and request.url.path.endswith(f"/bucket/{STORAGE_BUCKET}"):
            return httpx.Response(200, json={"id": STORAGE_BUCKET, "public": False})
        upload_prefix = f"/storage/v1/object/{STORAGE_BUCKET}/"
        if request.method == "POST" and request.url.path.startswith(upload_prefix):
            key = request.url.path.removeprefix(upload_prefix)
            target = object_path(key)
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            descriptor = os.open(
                target,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            with os.fdopen(descriptor, "wb") as object_file:
                object_file.write(request.read())
                object_file.flush()
                os.fsync(object_file.fileno())
            return httpx.Response(200, json={"Key": key})
        download_prefix = f"/storage/v1/object/authenticated/{STORAGE_BUCKET}/"
        if request.method == "GET" and request.url.path.startswith(download_prefix):
            key = request.url.path.removeprefix(download_prefix)
            try:
                content = object_path(key).read_bytes()
            except FileNotFoundError:
                return httpx.Response(404)
            return httpx.Response(200, content=content)
        raise AssertionError(f"unexpected_storage_request:{request.method}")

    storage = SupabaseStorage(
        STORAGE_URL,
        STORAGE_SECRET,
        STORAGE_BUCKET,
        transport=httpx.MockTransport(handler),
        fixture_byte_limit=10 * 1024 * 1024,
        request_deadline_seconds=5.0,
    )
    return storage, requests


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


def _policy_context_payload(run: T22Run) -> dict[str, object]:
    return {
        "capabilities": [item.value for item in ToolCapability],
        "profile_id": "python-pytest",
        "profile_version": "1",
        "actor_can_write_repository": True,
        "app_can_access_repository": True,
        "stage": ToolStage.AGENT_WORK.value,
        "run_budget_available": True,
        "baseline_tree_sha256": run.baseline_tree_sha256,
        "baseline_source_paths": list(run.baseline_source_paths),
        "baseline_test_paths": list(run.baseline_test_paths),
        "baseline_paths_verified": True,
        "protected_paths": ["pyproject.toml", "tests/test_app.py"],
    }


def _start_agent_worker(
    runtime_url: URL,
    run: T22Run,
    objects: Path,
    *,
    mode: str,
    response: ScriptedModelResponse,
    model_log: Path,
    tool_log: Path,
    turn_id: UUID | None = None,
    fault: str | None = None,
    tool_limit: int = 0,
) -> subprocess.Popen[str]:
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONUNBUFFERED": "1",
        "CONTINUUM_T22_RUNTIME_DATABASE_URL": runtime_url.render_as_string(hide_password=False),
    }
    python_path_entries = [str(PROJECT_ROOT / "src"), str(PROJECT_ROOT)]
    if os.environ.get("PYTHONPATH"):
        python_path_entries.append(os.environ["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(python_path_entries)
    arguments = [
        sys.executable,
        str(PROJECT_ROOT / "tests" / "integration" / "agent_loop_worker.py"),
        str(run.owner_id),
        str(objects),
        mode,
        "agent-turn:primary",
        str(turn_id) if turn_id is not None else "-",
        str(run.base_artifact_id),
        json.dumps(response.model_dump(mode="json", warnings=False), sort_keys=True),
        json.dumps(_policy_context_payload(run), sort_keys=True),
        str(model_log),
        str(tool_log),
        str(tool_limit),
        fault or "-",
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
            return_code = worker.poll()
            if return_code is None:
                try:
                    return_code = worker.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pytest.fail("worker_stdout_closed_while_running")
            diagnostic = ""
            if worker.stderr is not None:
                diagnostic = worker.stderr.read(4096)
                diagnostic = re.sub(
                    r"postgresql(?:\+[^:/ ]+)?://[^\s'\"]+", "<database-url>", diagnostic
                )
            pytest.fail(f"worker_exited_before_message:{return_code}:{diagnostic[-1200:]}")
        buffer.extend(chunk)
    line, _, remaining = buffer.partition(b"\n")
    _WORKER_LINE_BUFFERS[descriptor] = bytearray(remaining)
    try:
        message = json.loads(line.decode("utf-8"))
    except json.JSONDecodeError:
        pytest.fail("worker_message_invalid")
    if not isinstance(message, dict) or (message.get("kind") == "worker_error" and not allow_error):
        code = message.get("code") if isinstance(message, dict) else None
        pytest.fail(f"worker_operation_failed:{code or 'invalid_message'}")
    return message


def _close_process_pipes(worker: subprocess.Popen[str]) -> None:
    if worker.stdout is not None:
        _WORKER_LINE_BUFFERS.pop(worker.stdout.fileno(), None)
    for stream in (worker.stdin, worker.stdout, worker.stderr):
        if stream is not None and not stream.closed:
            stream.close()


def _expire_worker_lease(engine: Engine, run: T22Run, lease: dict[str, Any]) -> None:
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


def _read_json_lines(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_scripted_response_contract_is_deterministic_without_postgres() -> None:
    response = _response_with_calls(
        (
            ("call-read", "read_file", {"path": "src/app.py"}),
            (
                "call-patch",
                "apply_patch",
                {
                    "edits": [
                        {
                            "path": "src/app.py",
                            "expected_text": "base",
                            "replacement_text": "patched",
                        }
                    ]
                },
            ),
        )
    )
    request = ScriptedModelRequest(
        turn_id=uuid4(),
        model_id="scripted-v1",
        context={"task": "change answer"},
        tool_schemas=(),
        request_sha256="a" * 64,
    )
    scripted = ScriptedResponseQueue((response,))
    returned = asyncio.run(scripted.complete(request))
    assert scripted.call_count == 1
    assert scripted.requests == [request]
    materialized = _validate_complete_response(
        returned, "scripted-v1", request.turn_id, output_tokens_reserved=4096
    )
    assert [item["provider_call_id"] for item in materialized] == ["call-read", "call-patch"]
    assert [item["ordinal"] for item in materialized] == [0, 1]

    incomplete = ScriptedModelResponse(
        complete=False,
        stop_reason="max_output_tokens",
        model_id="scripted-v1",
        response_id="partial-response",
        output_text="partial",
        calls=(
            ScriptedToolCall(
                provider_call_id="partial-call",
                name="read_file",
                arguments_json='{"path":"src/app.py"}',
            ),
        ),
        input_tokens=3,
        output_tokens=4,
    )
    with pytest.raises(AgentLoopError, match="agent_response_incomplete"):
        _validate_complete_response(
            incomplete, "scripted-v1", request.turn_id, output_tokens_reserved=4096
        )

    malformed_set = _response_with_calls(
        (
            ("valid-first", "read_file", {"path": "src/app.py"}),
            ("invalid-second", "read_file", {"path": "../../outside"}),
        )
    )
    with pytest.raises(AgentLoopError):
        _validate_complete_response(
            malformed_set, "scripted-v1", request.turn_id, output_tokens_reserved=4096
        )


def _response_with_calls(
    calls: tuple[tuple[str, str, dict[str, Any]], ...],
) -> ScriptedModelResponse:
    return ScriptedModelResponse(
        complete=True,
        stop_reason="tool_calls",
        model_id="scripted-v1",
        response_id="scripted-response-1",
        output_text="",
        calls=tuple(
            ScriptedToolCall(
                provider_call_id=provider_call_id,
                name=name,
                arguments_json=json.dumps(arguments, sort_keys=True, separators=(",", ":")),
            )
            for provider_call_id, name, arguments in calls
        ),
        input_tokens=12,
        output_tokens=8,
    )


def _read_patch_read_response() -> ScriptedModelResponse:
    return _response_with_calls(
        (
            ("provider-read-before", "read_file", {"path": "src/app.py"}),
            (
                "provider-patch-answer",
                "apply_patch",
                {
                    "edits": [
                        {
                            "path": "src/app.py",
                            "expected_text": "return 'base'",
                            "replacement_text": "return 'patched'",
                        }
                    ]
                },
            ),
            ("provider-read-after", "read_file", {"path": "src/app.py"}),
        )
    )


def _invalid_response(case: str) -> tuple[ScriptedModelResponse, str]:
    if case == "incomplete":
        response = _response_with_calls(
            (("partial-call", "read_file", {"path": "src/app.py"}),)
        ).model_copy(update={"complete": False, "stop_reason": "max_output_tokens"})
        return response, "agent_response_incomplete"
    if case == "malformed_later_arguments":
        return (
            _response_with_calls(
                (
                    ("valid-first", "read_file", {"path": "src/app.py"}),
                    ("invalid-second", "read_file", {"path": "../outside.py"}),
                )
            ),
            "tool_arguments_invalid",
        )
    if case == "truncated_later_arguments":
        response = _response_with_calls(
            (
                ("valid-first", "read_file", {"path": "src/app.py"}),
                ("truncated-second", "read_file", {"path": "src/app.py"}),
            )
        )
        calls = list(response.calls)
        calls[1] = ScriptedToolCall(
            provider_call_id="truncated-second",
            name="read_file",
            arguments_json='{"path":"src/app.py"',
        )
        return response.model_copy(update={"calls": tuple(calls)}), "agent_tool_arguments_invalid"
    if case == "stop_reason_mismatch":
        response = _response_with_calls((("mismatched-call", "read_file", {"path": "src/app.py"}),))
        return response.model_copy(
            update={"stop_reason": "stop"}
        ), "agent_response_stop_reason_mismatch"
    raise AssertionError(f"invalid_response_case_unknown:{case}")


@pytest.mark.parametrize(
    "case",
    [
        "incomplete",
        "malformed_later_arguments",
        "truncated_later_arguments",
        "stop_reason_mismatch",
    ],
)
def test_invalid_full_turn_never_materializes_or_executes_any_call(
    t22_database: T22Database,
    child_workers: list[subprocess.Popen[str]],
    tmp_path: Path,
    case: str,
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    objects.mkdir()
    run = _seed_agent_run(store, objects)
    model_log = tmp_path / "model-calls.jsonl"
    tool_log = tmp_path / "tool-executions.jsonl"
    response, expected_code = _invalid_response(case)
    worker = _start_agent_worker(
        store.runtime_url,
        run,
        objects,
        mode="turn",
        response=response,
        model_log=model_log,
        tool_log=tool_log,
    )
    child_workers.append(worker)
    lease = _read_worker_message(worker)
    assert lease["kind"] == "lease_claimed"
    failure = _read_worker_message(worker, allow_error=True)
    assert failure == {"kind": "worker_error", "code": expected_code}
    assert worker.wait(timeout=5) == 2
    assert len(_read_json_lines(model_log)) == 1
    assert _read_json_lines(tool_log) == []

    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        turn = connection.execute(
            text(
                "SELECT turn_id, status, response_error_code FROM runtime_private.agent_turns "
                "WHERE owner_id=:owner AND run_id=:run AND job_id=:job"
            ),
            {
                "owner": run.owner_id,
                "run": run.run_id,
                "job": UUID(lease["job_id"]),
            },
        ).one()
        invocation_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.agent_invocations "
                "WHERE owner_id=:owner AND run_id=:run AND turn_id=:turn"
            ),
            {"owner": run.owner_id, "run": run.run_id, "turn": turn.turn_id},
        ).scalar_one()
        call_steps = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.steps WHERE owner_id=:owner "
                "AND run_id=:run AND logical_key LIKE 'agent-tool:%'"
            ),
            {"owner": run.owner_id, "run": run.run_id},
        ).scalar_one()
        attempt_status = connection.execute(
            text(
                "SELECT status FROM runtime_private.attempts WHERE owner_id=:owner "
                "AND run_id=:run AND job_id=:job AND generation=:generation"
            ),
            {
                "owner": run.owner_id,
                "run": run.run_id,
                "job": UUID(lease["job_id"]),
                "generation": lease["generation"],
            },
        ).scalar_one()
    assert turn.status == "invalid_response"
    assert turn.response_error_code == expected_code
    assert invocation_count == 0
    assert call_steps == 0
    assert _accepted_tool_results(store, run, {"turn_id": str(turn.turn_id)}) == []
    assert attempt_status == "definitive_failure"


def test_lost_model_response_is_fenced_and_not_regenerated(
    t22_database: T22Database,
    child_workers: list[subprocess.Popen[str]],
    tmp_path: Path,
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    objects.mkdir()
    run = _seed_agent_run(store, objects)
    model_log = tmp_path / "model-calls.jsonl"
    tool_log = tmp_path / "tool-executions.jsonl"
    response = _read_patch_read_response()

    stale = _start_agent_worker(
        store.runtime_url,
        run,
        objects,
        mode="turn",
        response=response,
        model_log=model_log,
        tool_log=tool_log,
        fault="provider_response_ready",
    )
    child_workers.append(stale)
    stale_lease = _read_worker_message(stale)
    assert stale_lease["kind"] == "lease_claimed"
    assert _read_worker_message(stale) == {
        "kind": "barrier",
        "point": "provider_response_ready",
    }
    assert len(_read_json_lines(model_log)) == 1
    _expire_worker_lease(store.admin_engine, run, stale_lease)
    assert RuntimeRecoveryService(store.runtime_engine, run.principal).recover_due(limit=1) == 1
    assert RuntimeRecoveryService(store.runtime_engine, run.principal).recover_due(limit=1) == 0

    replacement = _start_agent_worker(
        store.runtime_url,
        run,
        objects,
        mode="turn",
        response=response,
        model_log=model_log,
        tool_log=tool_log,
    )
    child_workers.append(replacement)
    replacement_lease = _read_worker_message(replacement)
    assert replacement_lease["kind"] == "lease_claimed"
    assert replacement_lease["generation"] == stale_lease["generation"] + 1
    unresolved = _read_worker_message(replacement, allow_error=True)
    assert unresolved == {
        "kind": "worker_error",
        "code": "agent_response_outcome_unknown",
    }
    assert replacement.wait(timeout=5) == 2

    stale.stdin.write("resume\n")
    stale.stdin.flush()
    stale_result = _read_worker_message(stale, allow_error=True)
    assert stale_result == {"kind": "worker_error", "code": "agent_lease_lost"}
    assert stale.wait(timeout=5) == 2
    assert len(_read_json_lines(model_log)) == 1
    assert _read_json_lines(tool_log) == []

    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        turn = connection.execute(
            text(
                "SELECT turn_id, status, input_bytes_reserved, output_tokens_reserved, "
                "dispatch_generation FROM runtime_private.agent_turns "
                "WHERE owner_id=:owner AND run_id=:run AND job_id=:job"
            ),
            {
                "owner": run.owner_id,
                "run": run.run_id,
                "job": UUID(stale_lease["job_id"]),
            },
        ).one()
        invocation_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.agent_invocations "
                "WHERE owner_id=:owner AND run_id=:run AND turn_id=:turn"
            ),
            {"owner": run.owner_id, "run": run.run_id, "turn": turn.turn_id},
        ).scalar_one()
        accepted_turns = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner AND run_id=:run AND job_id=:job"
            ),
            {
                "owner": run.owner_id,
                "run": run.run_id,
                "job": UUID(stale_lease["job_id"]),
            },
        ).scalar_one()
        run_status = connection.execute(
            text("SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
            {"owner": run.owner_id, "run": run.run_id},
        ).scalar_one()
    assert turn.status == "unknown"
    assert turn.dispatch_generation == stale_lease["generation"]
    assert turn.input_bytes_reserved > 0
    assert turn.output_tokens_reserved > 0
    assert invocation_count == 0
    assert accepted_turns == 0
    assert run_status == RunStatus.NEEDS_ATTENTION.value


def test_sigkill_reuses_persisted_response_and_runs_read_patch_read_sequentially(
    t22_database: T22Database,
    child_workers: list[subprocess.Popen[str]],
    tmp_path: Path,
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    objects.mkdir()
    run = _seed_agent_run(store, objects)
    model_log = tmp_path / "model-calls.jsonl"
    tool_log = tmp_path / "tool-executions.jsonl"
    response = _read_patch_read_response()

    first = _start_agent_worker(
        store.runtime_url,
        run,
        objects,
        mode="turn",
        response=response,
        model_log=model_log,
        tool_log=tool_log,
        fault="after_response_saved",
    )
    child_workers.append(first)
    first_lease = _read_worker_message(first)
    assert first_lease["kind"] == "lease_claimed"
    first_saved = _read_worker_message(first)
    assert first_saved["kind"] == "response_saved"
    assert first_saved["adopted"] is False
    barrier = _read_worker_message(first)
    assert barrier == {"kind": "barrier", "point": "after_response_saved"}
    _assert_persisted_turn(store, run, first_saved)
    _assert_agent_turn_rls(store, run, first_saved)
    with pytest.raises(IntegrityError):
        with store.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.agent_turns SET status='dispatching' "
                    "WHERE owner_id=:owner AND run_id=:run AND turn_id=:turn"
                ),
                {
                    "owner": run.owner_id,
                    "run": run.run_id,
                    "turn": UUID(first_saved["turn_id"]),
                },
            )
    _assert_persisted_turn(store, run, first_saved)

    first.send_signal(signal.SIGKILL)
    assert first.wait(timeout=5) == -signal.SIGKILL
    _expire_worker_lease(store.admin_engine, run, first_lease)
    assert RuntimeRecoveryService(store.runtime_engine, run.principal).recover_due(limit=1) == 1
    assert RuntimeRecoveryService(store.runtime_engine, run.principal).recover_due(limit=1) == 0

    recovered_turn = _start_agent_worker(
        store.runtime_url,
        run,
        objects,
        mode="turn",
        response=response,
        model_log=model_log,
        tool_log=tool_log,
    )
    child_workers.append(recovered_turn)
    recovered_lease = _read_worker_message(recovered_turn)
    assert recovered_lease["kind"] == "lease_claimed"
    assert recovered_lease["generation"] == first_lease["generation"] + 1
    reused = _read_worker_message(recovered_turn)
    assert reused["kind"] == "response_saved"
    assert reused["turn_id"] == first_saved["turn_id"]
    assert reused["adopted"] is True
    assert reused["response_sha256"] == first_saved["response_sha256"]
    assert reused["calls"] == first_saved["calls"]
    completed = _read_worker_message(recovered_turn)
    assert completed["kind"] == "turn_completed"
    assert completed["turn_id"] == first_saved["turn_id"]
    assert completed["next_job_id"] is not None
    assert recovered_turn.wait(timeout=5) == 0
    assert len(_read_json_lines(model_log)) == 1

    read_before = _start_agent_worker(
        store.runtime_url,
        run,
        objects,
        mode="tools",
        response=response,
        model_log=model_log,
        tool_log=tool_log,
        turn_id=UUID(first_saved["turn_id"]),
        tool_limit=1,
        fault="after_tool_accepted",
    )
    child_workers.append(read_before)
    read_lease = _read_worker_message(read_before)
    assert read_lease["kind"] == "lease_claimed"
    read_receipt = _read_worker_message(read_before)
    assert read_receipt["kind"] == "tool_accepted"
    assert read_receipt["ordinal"] == 0
    assert read_receipt["artifact_id"] is None
    assert "base" in read_receipt["result"]["tool_result"]["content"]
    assert _read_worker_message(read_before) == {
        "kind": "barrier",
        "point": "after_tool_accepted",
    }
    _assert_invocation_result(store, run, first_saved, read_receipt)
    read_before.send_signal(signal.SIGKILL)
    assert read_before.wait(timeout=5) == -signal.SIGKILL

    patch_and_read = _start_agent_worker(
        store.runtime_url,
        run,
        objects,
        mode="tools",
        response=response,
        model_log=model_log,
        tool_log=tool_log,
        turn_id=UUID(first_saved["turn_id"]),
        tool_limit=2,
    )
    child_workers.append(patch_and_read)
    patch_lease = _read_worker_message(patch_and_read)
    assert patch_lease["kind"] == "lease_claimed"
    patch_receipt = _read_worker_message(patch_and_read)
    assert patch_receipt["kind"] == "tool_accepted"
    assert patch_receipt["ordinal"] == 1
    assert patch_receipt["artifact_id"] is not None
    assert patch_receipt["result"]["tool_result"]["changed_paths"] == ["src/app.py"]
    _assert_invocation_result(store, run, first_saved, patch_receipt)

    final_lease = _read_worker_message(patch_and_read)
    assert final_lease["kind"] == "lease_claimed"
    final_receipt = _read_worker_message(patch_and_read)
    assert final_receipt["kind"] == "tool_accepted"
    assert final_receipt["ordinal"] == 2
    assert final_receipt["artifact_id"] is None
    assert "patched" in final_receipt["result"]["tool_result"]["content"]
    _assert_invocation_result(store, run, first_saved, final_receipt)
    assert _read_worker_message(patch_and_read)["kind"] == "worker_done"
    assert patch_and_read.wait(timeout=5) == 0

    assert len(_read_json_lines(model_log)) == 1
    executions = _read_json_lines(tool_log)
    assert [item["ordinal"] for item in executions] == [0, 1, 2]
    assert [item["provider_call_id"] for item in executions] == [
        "provider-read-before",
        "provider-patch-answer",
        "provider-read-after",
    ]
    assert len({item["invocation_id"] for item in executions}) == 3
    accepted_tool_results = _accepted_tool_results(store, run, first_saved)
    assert [item["ordinal"] for item in accepted_tool_results] == [0, 1, 2]
    assert [item["agent_invocation_id"] for item in accepted_tool_results] == [
        item["invocation_id"] for item in first_saved["calls"]
    ]
    assert [item["artifact_id"] for item in accepted_tool_results] == [
        None,
        patch_receipt["artifact_id"],
        None,
    ]

    second_response = _response_with_calls(
        (
            (
                "second-patch",
                "apply_patch",
                {
                    "edits": [
                        {
                            "path": "src/app.py",
                            "expected_text": "patched",
                            "replacement_text": "final",
                        }
                    ]
                },
            ),
            ("second-read", "read_file", {"path": "src/app.py"}),
        )
    )
    second_turn = _start_agent_worker(
        store.runtime_url,
        run,
        objects,
        mode="turn",
        response=second_response,
        model_log=model_log,
        tool_log=tool_log,
    )
    child_workers.append(second_turn)
    assert _read_worker_message(second_turn)["kind"] == "lease_claimed"
    second_saved = _read_worker_message(second_turn)
    assert second_saved["turn_id"] != first_saved["turn_id"]
    assert _read_worker_message(second_turn)["kind"] == "turn_completed"
    assert second_turn.wait(timeout=5) == 0
    second_tools = _start_agent_worker(
        store.runtime_url,
        run,
        objects,
        mode="tools",
        response=second_response,
        model_log=model_log,
        tool_log=tool_log,
        turn_id=UUID(second_saved["turn_id"]),
        tool_limit=2,
    )
    child_workers.append(second_tools)
    assert _read_worker_message(second_tools)["kind"] == "lease_claimed"
    second_patch = _read_worker_message(second_tools)
    assert second_patch["kind"] == "tool_accepted" and second_patch["artifact_id"] is not None
    assert _read_worker_message(second_tools)["kind"] == "lease_claimed"
    second_read = _read_worker_message(second_tools)
    assert "final" in second_read["result"]["tool_result"]["content"]
    assert _read_worker_message(second_tools)["kind"] == "worker_done"
    assert second_tools.wait(timeout=5) == 0
    assert len(_read_json_lines(model_log)) == 2
    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        assert (
            connection.execute(
                text(
                    "SELECT count(DISTINCT baseline_artifact_id) FROM runtime_private.agent_turns "
                    "WHERE run_id=:run"
                ),
                {"run": run.run_id},
            ).scalar_one()
            == 1
        )
        assert connection.execute(
            text(
                "SELECT parent_upload_id FROM runtime_private.artifact_uploads "
                "WHERE upload_id=:artifact"
            ),
            {"artifact": UUID(second_patch["artifact_id"])},
        ).scalar_one() == UUID(patch_receipt["artifact_id"])


def test_request_reservations_survive_reclaim_without_reset(
    t22_database: T22Database, tmp_path: Path
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    queue = QueueService(store.runtime_engine, run.principal)
    lease = queue.claim_one("t22-reservations")
    assert lease is not None
    storage, _requests = _storage_fixture(objects)
    checkpoints = _checkpoint_service(
        store.runtime_engine, run.principal, ArtifactPersistence(store.runtime_engine), storage
    )
    service = DurableAgentLoopService(
        store.runtime_engine,
        run.principal,
        TransitionService(store.runtime_engine, run.principal),
        checkpoints=checkpoints,
    )

    async def prepare_all() -> UUID:
        first_id: UUID | None = None
        for index in range(30):
            intent = await service.prepare_turn(
                lease,
                f"reserved:{index}",
                context={"task": "bounded scripted reservation"},
                tool_schemas=(),
                baseline_artifact_id=run.base_artifact_id,
                output_tokens_reserved=1,
            )
            first_id = first_id or intent.turn_id
        assert first_id is not None
        return first_id

    try:
        first_id = asyncio.run(prepare_all())
        _expire_worker_lease(
            store.admin_engine, run, {"job_id": str(lease.job_id), "generation": lease.generation}
        )
        assert RuntimeRecoveryService(store.runtime_engine, run.principal).recover_due(limit=1) == 1
        replacement = queue.claim_one("t22-reservation-replacement")
        assert replacement is not None and replacement.generation == lease.generation + 1
        adopted = asyncio.run(
            service.prepare_turn(
                replacement,
                "reserved:0",
                context={"task": "bounded scripted reservation"},
                tool_schemas=(),
                baseline_artifact_id=run.base_artifact_id,
                output_tokens_reserved=1,
            )
        )
        assert adopted.turn_id == first_id
        with pytest.raises(AgentLoopError, match="agent_request_limit"):
            asyncio.run(
                service.prepare_turn(
                    replacement,
                    "reserved:30",
                    context={"task": "bounded scripted reservation"},
                    tool_schemas=(),
                    baseline_artifact_id=run.base_artifact_id,
                    output_tokens_reserved=1,
                )
            )
        with store.runtime_engine.begin() as connection:
            RuntimeRepository(connection, run.principal)
            assert (
                connection.execute(
                    text("SELECT count(*) FROM runtime_private.agent_turns WHERE run_id=:run"),
                    {"run": run.run_id},
                ).scalar_one()
                == 30
            )
    finally:
        asyncio.run(storage.aclose())


def test_adapter_receives_reserved_output_limit_and_overage_cannot_dispatch(
    t22_database: T22Database, tmp_path: Path
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    lease = QueueService(store.runtime_engine, run.principal).claim_one("t22-output-reservation")
    assert lease is not None
    storage, _requests = _storage_fixture(objects)
    checkpoints = _checkpoint_service(
        store.runtime_engine, run.principal, ArtifactPersistence(store.runtime_engine), storage
    )
    service = DurableAgentLoopService(
        store.runtime_engine,
        run.principal,
        TransitionService(store.runtime_engine, run.principal),
        checkpoints=checkpoints,
    )

    class ExceedingAdapter:
        async def complete(self, request: ScriptedModelRequest) -> ScriptedModelResponse:
            assert request.max_output_tokens == 4
            return _read_patch_read_response()

    async def exercise() -> UUID:
        intent = await service.prepare_turn(
            lease,
            "output-reserved",
            context={"task": "bounded output"},
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
            output_tokens_reserved=4,
        )
        with pytest.raises(AgentLoopError, match="agent_output_reservation_exceeded"):
            await service.infer_or_reuse(lease, intent.turn_id, ExceedingAdapter())
        return intent.turn_id

    try:
        turn_id = asyncio.run(exercise())
        with store.runtime_engine.begin() as connection:
            RuntimeRepository(connection, run.principal)
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.agent_invocations WHERE turn_id=:turn"
                    ),
                    {"turn": turn_id},
                ).scalar_one()
                == 0
            )
            assert (
                connection.execute(
                    text("SELECT status FROM runtime_private.agent_turns WHERE turn_id=:turn"),
                    {"turn": turn_id},
                ).scalar_one()
                == "invalid_response"
            )
    finally:
        asyncio.run(storage.aclose())


def test_protected_original_test_edit_is_a_durable_failure(
    t22_database: T22Database,
    child_workers: list[subprocess.Popen[str]],
    tmp_path: Path,
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    model_log, tool_log = tmp_path / "model.jsonl", tmp_path / "tools.jsonl"
    response = _response_with_calls(
        (
            (
                "protected-test",
                "apply_patch",
                {
                    "edits": [
                        {
                            "path": "tests/test_app.py",
                            "expected_text": "base",
                            "replacement_text": "changed",
                        }
                    ]
                },
            ),
        )
    )
    model_worker = _start_agent_worker(
        store.runtime_url,
        run,
        objects,
        mode="turn",
        response=response,
        model_log=model_log,
        tool_log=tool_log,
    )
    child_workers.append(model_worker)
    assert _read_worker_message(model_worker)["kind"] == "lease_claimed"
    saved = _read_worker_message(model_worker)
    assert saved["kind"] == "response_saved"
    assert _read_worker_message(model_worker)["kind"] == "turn_completed"
    assert model_worker.wait(timeout=5) == 0
    tool_worker = _start_agent_worker(
        store.runtime_url,
        run,
        objects,
        mode="tools",
        response=response,
        model_log=model_log,
        tool_log=tool_log,
        turn_id=UUID(saved["turn_id"]),
        tool_limit=1,
    )
    child_workers.append(tool_worker)
    assert _read_worker_message(tool_worker)["kind"] == "lease_claimed"
    assert _read_worker_message(tool_worker, allow_error=True) == {
        "kind": "worker_error",
        "code": "tool_protected_path_denied",
    }
    assert tool_worker.wait(timeout=5) == 2
    assert _accepted_tool_results(store, run, saved) == []
    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        assert (
            connection.execute(
                text("SELECT count(*) FROM runtime_private.artifact_uploads WHERE run_id=:run"),
                {"run": run.run_id},
            ).scalar_one()
            == 1
        )
        assert (
            connection.execute(
                text("SELECT status FROM runtime_private.runs WHERE run_id=:run"),
                {"run": run.run_id},
            ).scalar_one()
            == "failed"
        )


def _assert_persisted_turn(store: T22Database, run: T22Run, saved: dict[str, Any]) -> None:
    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        turn = connection.execute(
            text(
                "SELECT status, response_sha256, response, generation, baseline_artifact_id "
                "FROM runtime_private.agent_turns WHERE owner_id=:owner AND run_id=:run "
                "AND turn_id=:turn"
            ),
            {"owner": run.owner_id, "run": run.run_id, "turn": UUID(saved["turn_id"])},
        ).one()
        invocations = connection.execute(
            text(
                "SELECT invocation_id, ordinal, provider_call_id, logical_key "
                "FROM runtime_private.agent_invocations WHERE owner_id=:owner "
                "AND run_id=:run AND turn_id=:turn ORDER BY ordinal"
            ),
            {"owner": run.owner_id, "run": run.run_id, "turn": UUID(saved["turn_id"])},
        ).all()
    assert turn.status == "response_accepted"
    assert turn.generation == 1
    assert turn.baseline_artifact_id == run.base_artifact_id
    assert turn.response_sha256.hex() == saved["response_sha256"]
    assert len(invocations) == 3
    assert [str(row.invocation_id) for row in invocations] == [
        call["invocation_id"] for call in saved["calls"]
    ]
    assert [row.ordinal for row in invocations] == [0, 1, 2]
    assert [row.provider_call_id for row in invocations] == [
        call["provider_call_id"] for call in saved["calls"]
    ]
    assert [row.logical_key for row in invocations] == [
        call["logical_key"] for call in saved["calls"]
    ]


def _assert_agent_turn_rls(store: T22Database, run: T22Run, saved: dict[str, Any]) -> None:
    other_principal = Principal(uuid4())
    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, other_principal)
        visible_turns = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.agent_turns WHERE owner_id=:owner "
                "AND run_id=:run AND turn_id=:turn"
            ),
            {
                "owner": run.owner_id,
                "run": run.run_id,
                "turn": UUID(saved["turn_id"]),
            },
        ).scalar_one()
        visible_invocations = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.agent_invocations WHERE owner_id=:owner "
                "AND run_id=:run AND turn_id=:turn"
            ),
            {
                "owner": run.owner_id,
                "run": run.run_id,
                "turn": UUID(saved["turn_id"]),
            },
        ).scalar_one()
        update_result = connection.execute(
            text(
                "UPDATE runtime_private.agent_turns SET status='invalid_response' "
                "WHERE owner_id=:owner AND run_id=:run AND turn_id=:turn"
            ),
            {
                "owner": run.owner_id,
                "run": run.run_id,
                "turn": UUID(saved["turn_id"]),
            },
        )
    assert visible_turns == 0
    assert visible_invocations == 0
    assert update_result.rowcount == 0

    invocation_id = uuid4()
    arguments = {"name": "read_file", "path": "src/app.py"}
    arguments_digest = hashlib.sha256(
        json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode()
    ).digest()
    with pytest.raises(DBAPIError):
        with store.runtime_engine.begin() as connection:
            RuntimeRepository(connection, other_principal)
            connection.execute(
                text(
                    "INSERT INTO runtime_private.agent_invocations ("
                    "invocation_id, owner_id, run_id, turn_id, ordinal, provider_call_id, "
                    "tool_name, logical_key, arguments, arguments_sha256) VALUES ("
                    ":id, :owner, :run, :turn, 9, 'foreign-call', 'read_file', "
                    "'foreign-invocation', CAST(:arguments AS jsonb), :digest)"
                ),
                {
                    "id": invocation_id,
                    "owner": run.owner_id,
                    "run": run.run_id,
                    "turn": UUID(saved["turn_id"]),
                    "arguments": json.dumps(arguments),
                    "digest": arguments_digest,
                },
            )


def _assert_invocation_result(
    store: T22Database,
    run: T22Run,
    saved: dict[str, Any],
    receipt: dict[str, Any],
) -> None:
    accepted = _accepted_tool_results(store, run, saved)
    matching = next(
        row for row in accepted if row["agent_invocation_id"] == receipt["invocation_id"]
    )
    assert matching["ordinal"] == receipt["ordinal"]
    assert matching["artifact_id"] == receipt["artifact_id"]
    assert matching["tool_result"] == receipt["result"]["tool_result"]


def _accepted_tool_results(
    store: T22Database, run: T22Run, saved: dict[str, Any]
) -> list[dict[str, Any]]:
    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, run.principal)
        rows = (
            connection.execute(
                text(
                    "SELECT payload, artifact_id FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner AND run_id=:run "
                    "AND payload->>'agent_turn_id'=:turn "
                    "AND payload ? 'agent_invocation_id' "
                    "ORDER BY (payload->>'ordinal')::integer"
                ),
                {"owner": run.owner_id, "run": run.run_id, "turn": saved["turn_id"]},
            )
            .mappings()
            .all()
        )
    return [
        {
            **dict(row["payload"]),
            "artifact_id": None if row["artifact_id"] is None else str(row["artifact_id"]),
        }
        for row in rows
    ]
