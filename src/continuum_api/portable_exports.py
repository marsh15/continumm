"""Encrypted, allowlisted owner exports and offline quarantine checks."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import io
import json
import os
import re
import tarfile
import tempfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.artifacts.archives import verify_archive
from continuum_api.runtime.persistence import Principal, RuntimeRepository

EXPORT_MAGIC = b"CONTINUUM-PORTABLE-EXPORT-V1\n"
MAX_PACKAGE_BYTES = 256 * 1024 * 1024
MAX_RUNS = 100
MAX_ARTIFACTS = 200
MAX_TABLE_ROWS = 20_000
_SECRET_FIELD = re.compile(
    r"(?:ciphertext|nonce|session[_-]?hash|csrf[_-]?hash|private[_-]?key|"
    r"access[_-]?token|refresh[_-]?token|api[_-]?key|provider[_-]?credential)",
    re.IGNORECASE,
)
_FILE_NAME = re.compile(r"^[0-9a-f-]{36}\.cpx$")


class PortableExportError(RuntimeError):
    """Sanitized export or restore-check failure."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ObjectReader(Protocol):
    async def download(self, object_key: str) -> bytes: ...


TABLE_FIELDS: dict[str, tuple[str, ...]] = {
    "runs": (
        "run_id",
        "owner_id",
        "github_repository_id",
        "issue_number",
        "status",
        "revision",
        "created_at",
        "deadline_at",
        "content_expires_at",
        "model_id",
        "model_task_context",
        "model_tool_schema_sha256",
        "model_requests_reserved",
        "model_requests_authorized",
        "model_requests_received",
        "model_requests_ambiguous",
        "model_usage_unknown_requests",
        "model_input_tokens_used",
        "model_input_tokens_reserved",
        "model_output_tokens_used",
        "model_output_tokens_reserved",
        "model_budget_exceeded",
    ),
    "run_admission_pins": (
        "run_id",
        "registration_id",
        "defaults_version",
        "profile_id",
        "profile_version",
        "base_branch",
        "source_sha",
        "source_tree_sha",
        "source_receipt",
        "issue_snapshot",
        "provider",
        "model_id",
        "task_context",
        "submission_digest",
        "created_at",
    ),
    "steps": (
        "run_id",
        "step_id",
        "logical_key",
        "activity",
        "revision",
        "status",
        "created_at",
    ),
    "jobs": (
        "run_id",
        "step_id",
        "job_id",
        "status",
        "generation",
        "available_at",
        "lease_expires_at",
        "failure_count",
        "replay_policy",
        "created_at",
    ),
    "attempts": (
        "run_id",
        "job_id",
        "generation",
        "attempt_id",
        "status",
        "started_at",
        "finished_at",
        "failure_class",
    ),
    "events": (
        "run_id",
        "sequence",
        "event_type",
        "actor_type",
        "actor_id",
        "payload_version",
        "payload",
        "created_at",
    ),
    "accepted_results": (
        "run_id",
        "step_id",
        "job_id",
        "result_id",
        "result_sha256",
        "intent_sha256",
        "run_status",
        "event_sequence",
        "next_job_id",
        "artifact_id",
        "payload",
        "accepted_at",
    ),
    "attempt_checkpoints": (
        "run_id",
        "attempt_id",
        "checkpoint_key",
        "payload_sha256",
        "payload",
        "created_at",
    ),
    "artifact_uploads": (
        "run_id",
        "upload_id",
        "parent_upload_id",
        "github_repository_id",
        "checkpoint_key",
        "object_key",
        "archive_sha256",
        "tree_sha256",
        "format_version",
        "compressed_bytes",
        "source_bytes",
        "file_count",
        "content_expires_at",
        "committed_at",
    ),
    "agent_turns": (
        "run_id",
        "turn_id",
        "step_id",
        "job_id",
        "logical_key",
        "generation",
        "attempt_id",
        "baseline_artifact_id",
        "model_id",
        "request_sha256",
        "context",
        "tool_schemas",
        "response",
        "response_sha256",
        "response_received_at",
        "response_error_code",
        "input_tokens",
        "output_tokens",
        "created_at",
    ),
    "agent_invocations": (
        "run_id",
        "invocation_id",
        "turn_id",
        "ordinal",
        "provider_call_id",
        "tool_name",
        "logical_key",
        "arguments",
        "arguments_sha256",
        "created_at",
    ),
    "approvals": (
        "run_id",
        "approval_id",
        "revision",
        "candidate_artifact_id",
        "evidence_result_id",
        "manifest",
        "manifest_sha256",
        "status",
        "decision_actor_id",
        "decided_at",
        "created_at",
        "expires_at",
    ),
    "revision_feedback": (
        "run_id",
        "feedback_id",
        "revision",
        "approval_id",
        "feedback",
        "feedback_sha256",
        "actor_id",
        "created_at",
    ),
    "external_operations": (
        "run_id",
        "operation_id",
        "step_id",
        "job_id",
        "operation_key",
        "request_sha256",
        "request_metadata",
        "approval_id",
        "approval_sha256",
        "status",
        "dispatch_authorized_at",
        "created_at",
        "updated_at",
    ),
    "external_operation_observations": (
        "run_id",
        "observation_id",
        "operation_id",
        "observation_key",
        "outcome",
        "details",
        "actor_type",
        "actor_id",
        "observed_at",
    ),
    "validation_source_pins": (
        "run_id",
        "baseline_artifact_id",
        "eligibility_receipt",
        "eligibility_sha256",
        "revision",
        "step_id",
        "job_id",
        "generation",
        "attempt_id",
        "lease_owner",
        "created_at",
    ),
    "sandbox_allocations": (
        "run_id",
        "allocation_id",
        "provider_name",
        "provider_period_id",
        "allocation_class",
        "requested_seconds",
        "vcpus",
        "memory_mb",
        "reserved_cpu_seconds",
        "status",
        "last_sandbox_status",
        "last_session_status",
        "created_at",
        "updated_at",
    ),
}


def parse_export_key(value: str) -> bytes:
    """Read a 32-byte operator key encoded as 64 hex digits or base64."""
    try:
        key = (
            bytes.fromhex(value)
            if re.fullmatch(r"[0-9a-fA-F]{64}", value)
            else base64.urlsafe_b64decode(value)
        )
    except Exception:
        raise PortableExportError("export_key_invalid") from None
    if len(key) != 32:
        raise PortableExportError("export_key_invalid")
    return key


def export_owner(
    engine: Engine,
    principal: Principal,
    object_reader: ObjectReader,
    output_dir: Path,
    key: bytes,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Export one account's current run state and verified source objects."""
    if len(key) != 32:
        raise PortableExportError("export_key_invalid")
    created_at = (now or datetime.now(UTC)).astimezone(UTC)
    owner = principal.account_id
    try:
        with engine.connect().execution_options(isolation_level="REPEATABLE READ") as connection:
            with connection.begin():
                RuntimeRepository(connection, principal)
                account = (
                    connection.execute(
                        text(
                            "SELECT user_id, github_user_id, github_login, status, created_at "
                            "FROM runtime_private.users WHERE user_id=:owner"
                        ),
                        {"owner": owner},
                    )
                    .mappings()
                    .one_or_none()
                )
                if account is None or account["status"] != "active":
                    raise PortableExportError("export_owner_unavailable")
                run_rows = (
                    connection.execute(
                        text(
                            "SELECT run_id, content_expires_at FROM runtime_private.runs "
                            "WHERE owner_id=:owner AND content_deleted_at IS NULL "
                            "AND content_expires_at>:now "
                            "ORDER BY created_at,run_id LIMIT :limit"
                        ),
                        {"owner": owner, "now": created_at, "limit": MAX_RUNS + 1},
                    )
                    .mappings()
                    .all()
                )
                if len(run_rows) > MAX_RUNS:
                    raise PortableExportError("export_run_limit")
                run_ids = [row["run_id"] for row in run_rows]
                state: dict[str, Any] = {
                    "users": [_portable_row(dict(account))],
                    "tables": {},
                }
                if run_ids:
                    for table_name, fields in TABLE_FIELDS.items():
                        selected = ", ".join(fields)
                        if "owner_id" not in fields:
                            selected = f"owner_id, {selected}"
                        extra = (
                            " AND status='committed' AND EXISTS (SELECT 1 "
                            "FROM runtime_private.accepted_results AS accepted "
                            "WHERE accepted.owner_id=artifact_uploads.owner_id "
                            "AND accepted.run_id=artifact_uploads.run_id "
                            "AND accepted.artifact_id=artifact_uploads.upload_id)"
                            if table_name == "artifact_uploads"
                            else ""
                        )
                        table_alias = table_name
                        rows = (
                            connection.execute(
                                text(
                                    f"SELECT {selected} FROM runtime_private.{table_name} "
                                    f"AS {table_alias} "
                                    f"WHERE {table_alias}.owner_id=:owner "
                                    f"AND {table_alias}.run_id=ANY(:runs)"
                                    f"{extra} LIMIT :limit"
                                ),
                                {"owner": owner, "runs": run_ids, "limit": MAX_TABLE_ROWS + 1},
                            )
                            .mappings()
                            .all()
                        )
                        if len(rows) > MAX_TABLE_ROWS:
                            raise PortableExportError("export_table_row_limit")
                        state["tables"][table_name] = [_portable_row(dict(row)) for row in rows]
                selected_runs = state["tables"].get("runs", [])
                run_expiries = cast(
                    list[datetime],
                    [
                        expiry
                        for row in selected_runs
                        if (expiry := _parse_datetime(row.get("content_expires_at"))) is not None
                    ],
                )
                expires_at: datetime = min([created_at + timedelta(days=7), *run_expiries])
                if expires_at <= created_at:
                    raise PortableExportError("export_content_expired")
                artifact_rows = state["tables"].get("artifact_uploads", [])
    except PortableExportError:
        raise
    except SQLAlchemyError:
        raise PortableExportError("export_database_unavailable") from None

    objects: dict[str, bytes] = {}
    total_object_bytes = 0
    if len(artifact_rows) > MAX_ARTIFACTS:
        raise PortableExportError("export_artifact_limit")
    for artifact in artifact_rows:
        object_key = artifact.get("object_key")
        archive_sha = _digest_hex(artifact.get("archive_sha256"))
        tree_sha = _digest_hex(artifact.get("tree_sha256"))
        if not isinstance(object_key, str) or not archive_sha or not tree_sha:
            raise PortableExportError("export_artifact_reference_invalid")
        try:
            content = _download_sync(object_reader, object_key)
            verify_archive(
                content,
                expected_archive_sha256=archive_sha,
                expected_tree_sha256=tree_sha,
            )
        except PortableExportError:
            raise
        except Exception:
            raise PortableExportError("export_artifact_verification_failed") from None
        total_object_bytes += len(content)
        if total_object_bytes > MAX_PACKAGE_BYTES // 2:
            raise PortableExportError("export_artifact_byte_limit")
        objects[f"objects/{archive_sha}.tar.gz"] = content

    export_id = uuid4()
    manifest = {
        "format": "continumm-portable-export-v1",
        "export_id": str(export_id),
        "owner_id": str(owner),
        "run_ids": [str(run_id) for run_id in run_ids],
        "created_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "secrets_included": False,
        "database_sha256": _sha256(_canonical_json(state)),
        "objects": {
            name: {"sha256": _sha256(content), "bytes": len(content)}
            for name, content in sorted(objects.items())
        },
    }
    package = _build_plaintext_package(manifest, state, objects)
    encrypted = _seal(package, key)
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    archive_name = f"{export_id}.cpx"
    archive_path = output_dir / archive_name
    _write_private_file(archive_path, encrypted)
    try:
        with engine.begin() as connection:
            RuntimeRepository(connection, principal)
            connection.execute(
                text(
                    "SELECT operator_private.register_portable_export("
                    ":export,:owner,CAST(:runs AS jsonb),:name,:manifest,:archive,:expires)"
                ),
                {
                    "export": export_id,
                    "owner": owner,
                    "runs": json.dumps([str(item) for item in run_ids]),
                    "name": archive_name,
                    "manifest": bytes.fromhex(_sha256(_canonical_json(manifest))),
                    "archive": hashlib.sha256(encrypted).digest(),
                    "expires": expires_at,
                },
            )
    except SQLAlchemyError:
        archive_path.unlink(missing_ok=True)
        raise PortableExportError("export_journal_unavailable") from None
    return {
        "export_id": str(export_id),
        "path": str(archive_path),
        "created_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "runs": len(run_ids),
        "artifacts": len(objects),
        "bytes": len(encrypted),
        "sha256": hashlib.sha256(encrypted).hexdigest(),
    }


def restore_check(
    archive_path: Path,
    engine: Engine,
    quarantine_dir: Path,
    key: bytes,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Verify into a private temporary quarantine, applying current deletion and expiry data."""
    if len(key) != 32:
        raise PortableExportError("export_key_invalid")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    try:
        encrypted = archive_path.read_bytes()
    except OSError:
        raise PortableExportError("export_file_unavailable") from None
    package = _open_sealed(encrypted, key)
    manifest = _read_package_manifest(package)
    try:
        export_id = UUID(manifest["export_id"])
        owner_id = UUID(manifest["owner_id"])
        expires_at = _parse_datetime(manifest["expires_at"])
        if expires_at is None or expires_at <= current:
            archive_path.unlink(missing_ok=True)
            raise PortableExportError("export_expired")
        with engine.begin() as connection:
            principal = Principal(owner_id)
            RuntimeRepository(connection, principal)
            journal = (
                connection.execute(
                    text(
                        "SELECT * FROM operator_private.portable_export_status(:export,:manifest)"
                    ),
                    {
                        "export": export_id,
                        "manifest": bytes.fromhex(_sha256(_canonical_json(manifest))),
                    },
                )
                .mappings()
                .one_or_none()
            )
            if journal is None:
                raise PortableExportError("export_journal_unavailable")
            if journal["invalidated"]:
                archive_path.unlink(missing_ok=True)
                raise PortableExportError("export_invalidated_by_deletion")
            current_runs = _current_export_runs(connection, owner_id, current)
            unresolved = _unresolved_effects(connection, owner_id, current_runs)
    except PortableExportError:
        raise
    except SQLAlchemyError:
        # No current journal means source bytes must not leave metadata-only mode.
        return _metadata_only_result(manifest)

    manifest, state, objects = _read_plaintext_package(package)

    invalidated_run_ids = set(journal["invalidated_run_ids"] or [])
    current_run_ids = {str(run_id) for run_id in current_runs}
    included_ids = set(manifest.get("run_ids", []))
    allowed_run_ids = included_ids & current_run_ids - invalidated_run_ids
    quarantined_state = _filter_state_for_runs(state, allowed_run_ids)
    if _sha256(_canonical_json(state)) != manifest["database_sha256"]:
        raise PortableExportError("export_database_checksum_mismatch")
    _verify_object_references(quarantined_state, objects)
    _prepare_quarantined_state(quarantined_state, current)
    for row in quarantined_state.get("tables", {}).get("approvals", []):
        row["status"] = "expired"
        row["decision_actor_id"] = None
        row["decided_at"] = current.isoformat()
    quarantine_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    # Recheck invalidation immediately before any plaintext staging. User
    # deletion can race the initial package verification.
    try:
        with engine.begin() as connection:
            RuntimeRepository(connection, Principal(owner_id))
            latest = (
                connection.execute(
                    text(
                        "SELECT * FROM operator_private.portable_export_status(:export,:manifest)"
                    ),
                    {
                        "export": export_id,
                        "manifest": bytes.fromhex(_sha256(_canonical_json(manifest))),
                    },
                )
                .mappings()
                .one_or_none()
            )
            if latest is None or latest["invalidated"]:
                archive_path.unlink(missing_ok=True)
                raise PortableExportError("export_invalidated_by_deletion")
            latest_runs = _current_export_runs(connection, owner_id, current)
            latest_allowed = included_ids & {str(run_id) for run_id in latest_runs} - set(
                latest["invalidated_run_ids"] or []
            )
            if latest_allowed != allowed_run_ids:
                allowed_run_ids = latest_allowed
                quarantined_state = _filter_state_for_runs(state, allowed_run_ids)
                _prepare_quarantined_state(quarantined_state, current)
                for approval in quarantined_state.get("tables", {}).get("approvals", []):
                    approval["status"] = "expired"
                    approval["decision_actor_id"] = None
                    approval["decided_at"] = current.isoformat()
            unresolved = _unresolved_effects(connection, owner_id, latest_runs)
    except PortableExportError:
        raise
    except SQLAlchemyError:
        return _metadata_only_result(manifest)

    with tempfile.TemporaryDirectory(
        prefix="restore-quarantine-", dir=str(quarantine_dir)
    ) as temp_dir:
        # Materialize only in a private temporary directory while checks run.
        stage = Path(temp_dir)
        _write_private_file(stage / "state.json", _canonical_json(quarantined_state))
        for name, contents in objects.items():
            object_sha = name.split("/", 1)[-1].removesuffix(".tar.gz")
            if object_sha in _artifact_digests(quarantined_state):
                destination = stage / name
                destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                _write_private_file(destination, contents)
    return {
        "export_id": str(export_id),
        "state": "quarantined_validated",
        "quarantine": "private_temporary_stage",
        "mutations_enabled": False,
        "metadata_only": False,
        "runs_restored": len(allowed_run_ids),
        "runs_dropped": len(included_ids - allowed_run_ids),
        "artifacts_verified": len(_artifact_digests(quarantined_state)),
        "unresolved_effects": [str(row["operation_id"]) for row in unresolved],
        "writes_blocked_for_reconciliation": bool(unresolved),
        "source_import_performed": False,
    }


def cleanup_invalidated_exports(
    engine: Engine,
    output_dir: Path,
    key: bytes,
    *,
    now: datetime | None = None,
) -> int:
    """Destroy local export files invalidated by the current deletion journal."""
    current = (now or datetime.now(UTC)).astimezone(UTC)
    removed = 0
    if not output_dir.exists():
        return 0
    for archive_path in output_dir.iterdir():
        if not archive_path.is_file() or not _FILE_NAME.fullmatch(archive_path.name):
            continue
        try:
            encrypted = archive_path.read_bytes()
            manifest = _read_package_manifest(_open_sealed(encrypted, key))
            export_id = UUID(manifest["export_id"])
            owner_id = UUID(manifest["owner_id"])
            expires_at = _parse_datetime(manifest.get("expires_at"))
            if expires_at is None:
                raise PortableExportError("export_manifest_invalid")
            with engine.begin() as connection:
                RuntimeRepository(connection, Principal(owner_id))
                journal = (
                    connection.execute(
                        text(
                            "SELECT * FROM operator_private.portable_export_status("
                            ":export,:manifest)"
                        ),
                        {
                            "export": export_id,
                            "manifest": bytes.fromhex(_sha256(_canonical_json(manifest))),
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
                current_runs = _current_export_runs(connection, owner_id, current)
            if journal is None:
                continue
            valid_runs = set(manifest.get("run_ids", [])) & {str(run_id) for run_id in current_runs}
            valid_runs -= set(journal["invalidated_run_ids"] or [])
            if journal["invalidated"] or expires_at <= current or not valid_runs:
                archive_path.unlink(missing_ok=True)
                removed += 1
        except PortableExportError:
            continue
        except SQLAlchemyError:
            raise PortableExportError("export_journal_unavailable") from None
        except OSError:
            continue
    return removed


def _prepare_quarantined_state(state: dict[str, Any], now: datetime) -> None:
    """Strip reusable identity and invalidate every old execution generation."""
    for user in state.get("users", []):
        user["status"] = "reauth_required"
        user["github_user_id"] = None
        user["github_login"] = None
    tables = state.get("tables", {})
    for run in tables.get("runs", []):
        if run.get("status") in {
            "queued",
            "running",
            "waiting_approval",
            "waiting_credentials",
            "waiting_capacity",
            "needs_attention",
        }:
            run["status"] = "needs_attention"
    for job in tables.get("jobs", []):
        job["generation"] = int(job.get("generation", 0)) + 1
        job["status"] = "cancelled"
        if "lease_expires_at" in job:
            job["lease_expires_at"] = None
    for attempt in tables.get("attempts", []):
        if attempt.get("status") == "running":
            attempt["status"] = "unknown"
            attempt["finished_at"] = now.isoformat()


def _current_export_runs(connection: Connection, owner_id: UUID, now: datetime) -> list[UUID]:
    return cast(
        list[UUID],
        connection.execute(
            text(
                "SELECT run_id FROM runtime_private.runs WHERE owner_id=:owner "
                "AND content_deleted_at IS NULL AND content_expires_at>:now "
            ),
            {"owner": owner_id, "now": now},
        )
        .scalars()
        .all(),
    )


def _unresolved_effects(
    connection: Connection, owner_id: UUID, run_ids: Sequence[UUID]
) -> list[Mapping[str, Any]]:
    if not run_ids:
        return []
    return cast(
        list[Mapping[str, Any]],
        connection.execute(
            text(
                "SELECT operation_id,status FROM runtime_private.external_operations "
                "WHERE owner_id=:owner AND run_id=ANY(:runs) "
                "AND status IN ('dispatching','unknown','conflict')"
            ),
            {"owner": owner_id, "runs": run_ids},
        )
        .mappings()
        .all(),
    )


def _download_sync(reader: ObjectReader, object_key: str) -> bytes:
    result = reader.download(object_key)
    if inspect.isawaitable(result):
        return cast(bytes, asyncio.run(cast(Any, result)))
    if not isinstance(result, bytes):
        raise PortableExportError("export_object_invalid")
    return result


def _portable_row(row: dict[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for key, value in row.items():
        if _SECRET_FIELD.search(key):
            raise PortableExportError("export_secret_field_rejected")
        clean[key] = _json_value(value)
    return clean


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise PortableExportError("export_value_type_unsupported")


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError, OverflowError):
        raise PortableExportError("export_json_invalid") from None


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest_hex(value: Any) -> str | None:
    if isinstance(value, bytes) and len(value) == 32:
        return value.hex()
    if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
        return value
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).astimezone(UTC)
        except ValueError:
            return None
    return None


def _build_plaintext_package(
    manifest: dict[str, Any], state: dict[str, Any], objects: dict[str, bytes]
) -> bytes:
    files: dict[str, bytes] = {
        "manifest.json": _canonical_json(manifest),
        "database.json": _canonical_json(state),
        **objects,
    }
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz", compresslevel=6) as archive:
        for name, contents in sorted(files.items()):
            if not _safe_package_name(name):
                raise PortableExportError("export_package_path_invalid")
            info = tarfile.TarInfo(name)
            info.size = len(contents)
            info.mode = 0o600
            info.uid = 0
            info.gid = 0
            info.mtime = 0
            archive.addfile(info, io.BytesIO(contents))
    value = stream.getvalue()
    if len(value) > MAX_PACKAGE_BYTES:
        raise PortableExportError("export_package_too_large")
    return value


def _seal(package: bytes, key: bytes) -> bytes:
    if len(package) > MAX_PACKAGE_BYTES:
        raise PortableExportError("export_package_too_large")
    nonce = os.urandom(12)
    return EXPORT_MAGIC + nonce + AESGCM(key).encrypt(nonce, package, EXPORT_MAGIC)


def _open_sealed(encrypted: bytes, key: bytes) -> bytes:
    if len(encrypted) > MAX_PACKAGE_BYTES or not encrypted.startswith(EXPORT_MAGIC):
        raise PortableExportError("export_envelope_invalid")
    offset = len(EXPORT_MAGIC)
    if len(encrypted) <= offset + 12 + 16:
        raise PortableExportError("export_envelope_invalid")
    nonce = encrypted[offset : offset + 12]
    try:
        plaintext = AESGCM(key).decrypt(nonce, encrypted[offset + 12 :], EXPORT_MAGIC)
    except InvalidTag:
        raise PortableExportError("export_authentication_failed") from None
    if len(plaintext) > MAX_PACKAGE_BYTES:
        raise PortableExportError("export_package_too_large")
    return plaintext


def _read_plaintext_package(
    package: bytes,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, bytes]]:
    files: dict[str, bytes] = {}
    try:
        with tarfile.open(fileobj=io.BytesIO(package), mode="r:gz") as archive:
            members = archive.getmembers()
            if len(members) > MAX_ARTIFACTS + 2:
                raise PortableExportError("export_file_count_limit")
            for member in members:
                if (
                    not member.isfile()
                    or not _safe_package_name(member.name)
                    or member.name in files
                ):
                    raise PortableExportError("export_package_path_invalid")
                if member.size < 0 or member.size > MAX_PACKAGE_BYTES:
                    raise PortableExportError("export_package_too_large")
                stream = archive.extractfile(member)
                if stream is None:
                    raise PortableExportError("export_package_invalid")
                contents = stream.read(member.size + 1)
                if len(contents) != member.size:
                    raise PortableExportError("export_package_invalid")
                files[member.name] = contents
    except PortableExportError:
        raise
    except Exception:
        raise PortableExportError("export_package_invalid") from None
    try:
        manifest = json.loads(files.pop("manifest.json"))
        state = json.loads(files.pop("database.json"))
    except (KeyError, UnicodeDecodeError, json.JSONDecodeError):
        raise PortableExportError("export_manifest_invalid") from None
    if not isinstance(manifest, dict) or not isinstance(state, dict):
        raise PortableExportError("export_manifest_invalid")
    if (
        manifest.get("format") != "continumm-portable-export-v1"
        or manifest.get("secrets_included") is not False
    ):
        raise PortableExportError("export_manifest_invalid")
    if _sha256(_canonical_json(state)) != manifest.get("database_sha256"):
        raise PortableExportError("export_database_checksum_mismatch")
    expected = manifest.get("objects")
    if not isinstance(expected, dict) or set(files) != set(expected):
        raise PortableExportError("export_object_manifest_mismatch")
    for name, contents in files.items():
        item = expected.get(name)
        if (
            not isinstance(item, dict)
            or item.get("sha256") != _sha256(contents)
            or item.get("bytes") != len(contents)
        ):
            raise PortableExportError("export_object_checksum_mismatch")
    _validate_export_allowlist(state)
    return manifest, state, files


def _read_package_manifest(package: bytes) -> dict[str, Any]:
    """Read bounded metadata only before checking current deletion state."""
    try:
        with tarfile.open(fileobj=io.BytesIO(package), mode="r:gz") as archive:
            members = [member for member in archive.getmembers() if member.name == "manifest.json"]
            if len(members) != 1 or not members[0].isfile() or members[0].size > 65_536:
                raise PortableExportError("export_manifest_invalid")
            stream = archive.extractfile(members[0])
            if stream is None:
                raise PortableExportError("export_manifest_invalid")
            manifest = json.loads(stream.read(65_537))
    except PortableExportError:
        raise
    except Exception:
        raise PortableExportError("export_manifest_invalid") from None
    if (
        not isinstance(manifest, dict)
        or manifest.get("format") != "continumm-portable-export-v1"
        or manifest.get("secrets_included") is not False
    ):
        raise PortableExportError("export_manifest_invalid")
    return manifest


def _validate_export_allowlist(state: dict[str, Any]) -> None:
    if set(state) != {"users", "tables"} or not isinstance(state.get("tables"), dict):
        raise PortableExportError("export_allowlist_violation")
    tables = state["tables"]
    if not set(tables).issubset(TABLE_FIELDS):
        raise PortableExportError("export_allowlist_violation")
    user_fields = {"user_id", "github_user_id", "github_login", "status", "created_at"}
    if not isinstance(state["users"], list) or any(
        not isinstance(row, dict) or set(row) != user_fields for row in state["users"]
    ):
        raise PortableExportError("export_allowlist_violation")
    for table, rows in tables.items():
        if not isinstance(rows, list):
            raise PortableExportError("export_allowlist_violation")
        allowed = set(TABLE_FIELDS[table]) | {"owner_id"}
        if any(not isinstance(row, dict) or not set(row).issubset(allowed) for row in rows):
            raise PortableExportError("export_allowlist_violation")


def _filter_state_for_runs(state: dict[str, Any], allowed_run_ids: set[str]) -> dict[str, Any]:
    _validate_export_allowlist(state)
    result = {"users": state["users"], "tables": {}}
    for table, rows in state["tables"].items():
        retained = [row for row in rows if str(row.get("run_id")) in allowed_run_ids]
        result["tables"][table] = retained
    return result


def _verify_object_references(state: dict[str, Any], objects: dict[str, bytes]) -> None:
    digests = _artifact_digests(state)
    if not digests.issubset({name.split("/", 1)[-1].removesuffix(".tar.gz") for name in objects}):
        raise PortableExportError("export_artifact_missing")
    for row in state.get("tables", {}).get("artifact_uploads", []):
        digest = _digest_hex(row.get("archive_sha256"))
        if digest is None or digest not in digests:
            raise PortableExportError("export_artifact_reference_invalid")
        verify_archive(
            objects[f"objects/{digest}.tar.gz"],
            expected_archive_sha256=digest,
            expected_tree_sha256=_digest_hex(row.get("tree_sha256")),
        )


def _artifact_digests(state: dict[str, Any]) -> set[str]:
    result = set()
    for row in state.get("tables", {}).get("artifact_uploads", []):
        digest = _digest_hex(row.get("archive_sha256"))
        if digest is not None:
            result.add(digest)
    return result


def _safe_package_name(name: str) -> bool:
    return name in {"manifest.json", "database.json"} or bool(
        re.fullmatch(r"objects/[0-9a-f]{64}\.tar\.gz", name)
    )


def _write_private_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def _metadata_only_result(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "export_id": manifest.get("export_id"),
        "state": "metadata_only_journal_unavailable",
        "mutations_enabled": False,
        "metadata_only": True,
        "runs_restored": 0,
        "runs_dropped": len(manifest.get("run_ids", [])),
        "artifacts_verified": 0,
        "unresolved_effects": [],
        "writes_blocked_for_reconciliation": True,
        "source_import_performed": False,
    }
