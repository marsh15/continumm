"""Controlled runtime for evaluating two already-admitted live runs."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
import re
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy import Engine, text

from continuum_api.credentials import CredentialProvider
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.runtime.workflow_resources import (
    WorkflowConfigurationError,
    WorkflowResources,
    _read_admission_pins,
    build_workflow_resources,
)
from continuum_api.validation.managed_hidden_checks import ManagedHiddenCheckEvidence

_SAFE_CLASS = re.compile(r"^[a-z0-9_]{1,64}$")
_PROTOCOL_PASS = frozenset({"passed", "passed_with_baseline_failure"})
_STOP_REASONS = frozenset({"idle", "waiting", "job_limit", "time_limit"})
MAX_EVALUATION_JOBS = 100
MAX_EVALUATION_SECONDS = 300.0
_HIDDEN_STATUSES = frozenset({"passed", "failed", "invalid", "unknown", "not_run"})


class LiveEvaluationError(RuntimeError):
    """Short sanitized error for an invalid evaluation request or failed read."""

    def __init__(self, code: str) -> None:
        self.code = code if _SAFE_CLASS.fullmatch(code) else "live_evaluation_failed"
        super().__init__(self.code)


class _SnapshotModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class _RunMetadata(_SnapshotModel):
    run_id: UUID
    status: str = Field(max_length=32)
    revision: int = Field(ge=0)
    provider: str = Field(max_length=16)
    model_id: str = Field(max_length=64)
    profile_id: str = Field(max_length=32)
    profile_version: str = Field(max_length=16)
    model_requests_authorized: int = Field(ge=0)
    model_requests_received: int = Field(ge=0)
    model_requests_ambiguous: int = Field(ge=0)
    model_usage_unknown_requests: int = Field(ge=0)
    model_requests_reserved: int = Field(ge=0)
    input_tokens: int = Field(ge=0)
    input_tokens_reserved: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    output_tokens_reserved: int = Field(ge=0)
    input_tokens_unknown: bool
    output_tokens_unknown: bool


def _provider(value: CredentialProvider | str) -> str:
    try:
        return CredentialProvider(value).value
    except (TypeError, ValueError):
        raise LiveEvaluationError("live_provider_invalid") from None


def _check_limits(max_jobs: int, max_seconds: float) -> None:
    if type(max_jobs) is not int or not 1 <= max_jobs <= MAX_EVALUATION_JOBS:
        raise LiveEvaluationError("live_job_bound_invalid")
    if (
        isinstance(max_seconds, bool)
        or not isinstance(max_seconds, (int, float))
        or not math.isfinite(max_seconds)
        or max_seconds <= 0
        or max_seconds > MAX_EVALUATION_SECONDS
    ):
        raise LiveEvaluationError("live_time_bound_invalid")


async def run_live_evaluation(
    settings: Any,
    account_id: UUID,
    provider: CredentialProvider | str,
    python_run_id: UUID,
    node_run_id: UUID,
    *,
    execute: bool,
    max_jobs: int = 1,
    max_seconds: float = 30.0,
    resource_factory: Callable[..., Any] | None = None,
    snapshot_reader: Callable[[Engine, UUID, UUID], Mapping[str, Any]] | None = None,
    admission_reader: Callable[[Engine, UUID, UUID], Any] | None = None,
    hidden_check_selector: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """Observe or boundedly drain two selected-provider admitted run UUIDs.

    Admission preflight for both runs completes before a resource factory or
    runner can be reached. Injected readers/factories keep tests independent of
    provider keys and external services.
    """
    if (
        not isinstance(account_id, UUID)
        or not isinstance(python_run_id, UUID)
        or not isinstance(node_run_id, UUID)
    ):
        raise LiveEvaluationError("live_identity_invalid")
    if python_run_id == node_run_id:
        raise LiveEvaluationError("live_runs_must_differ")
    if type(execute) is not bool:
        raise LiveEvaluationError("live_execute_flag_invalid")
    if hidden_check_selector is not None and (not execute or not callable(hidden_check_selector)):
        raise LiveEvaluationError("live_hidden_selector_invalid")
    _check_limits(max_jobs, max_seconds)
    selected_provider = _provider(provider)
    factory = resource_factory or build_workflow_resources
    reader = snapshot_reader or read_live_evaluation_snapshot
    admission = admission_reader or (
        _strict_admission_pins if execute else _observer_admission_pins
    )

    engine = _preflight_engine(settings)
    try:
        pins: dict[str, Any] = {}
        for profile, run_id in (("python-pytest", python_run_id), ("node-npm", node_run_id)):
            try:
                pin = admission(engine, account_id, run_id)
            except WorkflowConfigurationError as error:
                raise LiveEvaluationError(error.code) from None
            if (
                getattr(pin, "profile_id", None) != profile
                or getattr(pin, "profile_version", None) != "1"
                or _pin_provider(pin) != selected_provider
            ):
                raise LiveEvaluationError("live_admission_pin_mismatch")
            if execute:
                _reject_public_allocations(engine, account_id, run_id)
            pins[profile] = pin

        if not execute:
            cases = {
                "python": _safe_snapshot(reader(engine, account_id, python_run_id)),
                "node": _safe_snapshot(reader(engine, account_id, node_run_id)),
            }
            return {
                "mode": "observer",
                "provider": selected_provider,
                "outcome": "observed",
                "protocol_smoke_ready": False,
                "cases": cases,
                "independent_hidden_checks": _aggregate_hidden_checks(cases),
                "task_correctness": "not_assessed",
            }

        results: dict[str, Any] = {}
        for label, _profile, run_id in (
            ("python", "python-pytest", python_run_id),
            ("node", "node-npm", node_run_id),
        ):
            resources: WorkflowResources | Any | None = None
            drain_started: float | None = None
            try:
                resources = await _call_factory(
                    factory, settings, account_id, run_id, hidden_check_selector
                )
                drain_started = time.monotonic()
                receipt = await resources.runner.run_admitted(
                    run_id, max_jobs=max_jobs, max_seconds=max_seconds
                )
                jobs_started = getattr(receipt, "jobs_started", None)
                stop_reason = str(getattr(receipt, "stop_reason", ""))
                if (
                    getattr(receipt, "run_id", None) != run_id
                    or type(jobs_started) is not int
                    or not 0 <= jobs_started <= max_jobs
                    or stop_reason not in _STOP_REASONS
                ):
                    raise LiveEvaluationError("live_drain_receipt_invalid")
                elapsed = max(0.0, time.monotonic() - drain_started)
                results[label] = {
                    "drain": {
                        "jobs_started": jobs_started,
                        "stop_reason": stop_reason,
                    },
                    "elapsed_active_drain_seconds": elapsed,
                    "snapshot": _safe_snapshot(reader(resources.engine, account_id, run_id)),
                }
            except asyncio.CancelledError:
                raise
            except Exception as error:
                results[label] = {
                    "failure": _safe_error(error),
                    "elapsed_active_drain_seconds": max(0.0, time.monotonic() - drain_started)
                    if drain_started is not None
                    else 0.0,
                    "snapshot": _snapshot_after_failure(
                        reader, resources, engine, account_id, run_id
                    ),
                }
            finally:
                if resources is not None:
                    try:
                        await _close_resources(resources)
                    except asyncio.CancelledError:
                        raise
                    except Exception as close_error:
                        item = results.setdefault(label, {})
                        item["cleanup_error"] = _safe_error(close_error)
                        item.setdefault(
                            "snapshot",
                            _snapshot_after_failure(reader, resources, engine, account_id, run_id),
                        )
        ready = len(results) == 2 and all(
            isinstance(item.get("snapshot"), dict)
            and item["snapshot"].get("protocol_smoke_ready") is True
            and "failure" not in item
            for item in results.values()
        )
        failed = any("failure" in item or "cleanup_error" in item for item in results.values())
        return {
            "mode": "execute",
            "provider": selected_provider,
            "outcome": "protocol_smoke_ready" if ready else "failed" if failed else "incomplete",
            "protocol_smoke_ready": ready,
            "cases": results,
            "independent_hidden_checks": _aggregate_hidden_checks(results),
            "task_correctness": "not_assessed",
        }
    finally:
        engine.dispose()


def _preflight_engine(settings: Any) -> Engine:
    try:
        url = settings.runtime_database_url.get_secret_value()
        from continuum_api.runtime.database import create_runtime_engine

        return create_runtime_engine(url)
    except Exception:
        raise LiveEvaluationError("live_database_unavailable") from None


def _strict_admission_pins(engine: Engine, account_id: UUID, run_id: UUID) -> Any:
    return _read_admission_pins(engine, account_id, run_id, require_binding=True)


def _observer_admission_pins(engine: Engine, account_id: UUID, run_id: UUID) -> Any:
    return _read_admission_pins(engine, account_id, run_id, allow_terminal=True)


def _pin_provider(pin: Any) -> Any:
    provider = getattr(pin, "provider", None)
    return getattr(provider, "value", provider)


def _reject_public_allocations(engine: Engine, account_id: UUID, run_id: UUID) -> None:
    try:
        with engine.begin() as connection:
            RuntimeRepository(connection, Principal(account_id))
            count: int = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.sandbox_allocations "
                    "WHERE owner_id=:owner AND run_id=:run AND allocation_class='public'"
                ),
                {"owner": account_id, "run": run_id},
            ).scalar_one()
    except Exception:
        raise LiveEvaluationError("live_allocation_preflight_failed") from None
    if count:
        raise LiveEvaluationError("live_public_allocation_exists")


async def _call_factory(
    factory: Callable[..., Any],
    settings: Any,
    account_id: UUID,
    run_id: UUID,
    hidden_check_selector: Callable[..., Any] | None = None,
) -> Any:
    kwargs: dict[str, Any] = {"evaluation": True}
    if hidden_check_selector is not None:
        kwargs["hidden_check_selector"] = hidden_check_selector
    value = factory(settings, account_id, run_id, **kwargs)
    return await value if inspect.isawaitable(value) else value


async def _close_resources(resources: Any) -> None:
    close = getattr(resources, "aclose", None)
    if callable(close):
        result = close()
        if inspect.isawaitable(result):
            await result


def _snapshot_after_failure(
    reader: Callable[[Engine, UUID, UUID], Mapping[str, Any]],
    resources: Any,
    engine: Engine,
    account_id: UUID,
    run_id: UUID,
) -> dict[str, Any]:
    try:
        selected_engine = getattr(resources, "engine", engine)
        return _safe_snapshot(reader(selected_engine, account_id, run_id))
    except Exception as error:
        return {"snapshot_error": _safe_error(error)}


def _safe_error(error: Exception) -> str:
    code = getattr(error, "code", None)
    if isinstance(code, str) and _SAFE_CLASS.fullmatch(code):
        return code
    if isinstance(error, WorkflowConfigurationError):
        return error.code
    return "live_run_failed"


def _safe_snapshot(value: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {
        "run_id",
        "observed_at",
        "status",
        "revision",
        "provider",
        "model_id",
        "profile_id",
        "profile_version",
        "model_requests_authorized",
        "model_requests_received",
        "model_requests_ambiguous",
        "model_usage_unknown_requests",
        "model_requests_reserved",
        "input_tokens",
        "input_tokens_reserved",
        "output_tokens",
        "output_tokens_reserved",
        "input_tokens_unknown",
        "output_tokens_unknown",
        "tool_errors",
        "tool_unauthorized",
        "tool_results_accepted",
        "unclassified_invalid_attempts",
        "validation_classification",
        "validation_commands",
        "candidate_patch_bytes",
        "candidate_patch_count",
        "human_review_status",
        "pending_effects",
        "allocations_active",
        "operator_allocations",
        "public_allocations",
        "protocol_smoke_ready",
        "independent_hidden_checks",
        "hidden_check_suite_id",
        "hidden_check_suite_sha256",
        "hidden_check_count",
        "hidden_check_passed_count",
        "task_correctness",
        "dispatches",
    }
    return {key: value[key] for key in allowed if key in value}


def read_live_evaluation_snapshot(engine: Engine, account_id: UUID, run_id: UUID) -> dict[str, Any]:
    """Return a bounded, owner-scoped snapshot containing no source or prompt data."""
    try:
        with engine.begin() as connection:
            connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            RuntimeRepository(connection, Principal(account_id))
            run = (
                connection.execute(
                    text(
                        "SELECT run.run_id,run.status,run.revision,run.github_repository_id,"
                        "run.content_expires_at,"
                        "run.content_deleted_at,account.status AS account_status,"
                        "pins.provider,pins.model_id,"
                        "pins.profile_id,pins.profile_version,run.model_requests_authorized,"
                        "run.model_requests_reserved,"
                        "run.model_requests_received,run.model_requests_ambiguous,"
                        "run.model_usage_unknown_requests,run.model_input_tokens_used,"
                        "run.model_input_tokens_reserved,run.model_output_tokens_used,"
                        "run.model_output_tokens_reserved FROM runtime_private.runs AS run "
                        "JOIN runtime_private.run_admission_pins AS pins "
                        "ON pins.owner_id=run.owner_id AND pins.run_id=run.run_id "
                        "JOIN runtime_private.users AS account ON account.user_id=run.owner_id "
                        "WHERE run.owner_id=:owner AND run.run_id=:run"
                    ),
                    {"owner": account_id, "run": run_id},
                )
                .mappings()
                .one_or_none()
            )
            if run is None:
                raise LiveEvaluationError("live_run_not_found")
            database_now: datetime = connection.execute(
                text("SELECT clock_timestamp()")
            ).scalar_one()
            if (
                run["account_status"] != "active"
                or run["content_deleted_at"] is not None
                or run["content_expires_at"] <= database_now
            ):
                raise LiveEvaluationError("live_run_not_found")
            turns = (
                connection.execute(
                    text(
                        "SELECT count(*) FILTER "
                        "(WHERE dispatch_authorized_at IS NOT NULL)::integer AS dispatches,"
                        "count(*) FILTER "
                        "(WHERE response_received_at IS NOT NULL)::integer AS received,"
                        "count(*) FILTER (WHERE status='unknown')::integer AS unknown,"
                        "bool_or(input_tokens IS NULL OR output_tokens IS NULL) FILTER "
                        "(WHERE status='response_accepted') AS usage_unknown "
                        "FROM runtime_private.agent_turns WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": account_id, "run": run_id},
                )
                .mappings()
                .one()
            )
            invalid_attempts = connection.execute(
                text(
                    "SELECT count(*)::integer FROM runtime_private.attempts "
                    "WHERE owner_id=:owner AND run_id=:run "
                    "AND failure_class='invalid_input'"
                ),
                {"owner": account_id, "run": run_id},
            ).scalar_one_or_none()
            results = (
                connection.execute(
                    text(
                        "SELECT result_id,result_sha256,payload,owner_id,run_id,step_id,job_id,"
                        "generation,attempt_id,lease_owner,run_status,event_sequence,artifact_id "
                        "FROM runtime_private.accepted_results "
                        "WHERE owner_id=:owner AND run_id=:run "
                        "ORDER BY event_sequence DESC LIMIT 512"
                    ),
                    {"owner": account_id, "run": run_id},
                )
                .mappings()
                .all()
            )
            approvals = (
                connection.execute(
                    text(
                        "SELECT approval.status,approval.revision,approval.manifest,"
                        "approval.manifest_sha256,approval.candidate_artifact_id,"
                        "approval.evidence_result_id,approval.expires_at "
                        "FROM runtime_private.approvals AS approval "
                        "WHERE owner_id=:owner AND run_id=:run ORDER BY revision DESC LIMIT 1"
                    ),
                    {"owner": account_id, "run": run_id},
                )
                .mappings()
                .one_or_none()
            )
            pending = connection.execute(
                text(
                    "SELECT count(*)::integer FROM runtime_private.external_operations "
                    "WHERE owner_id=:owner AND run_id=:run "
                    "AND status IN ('prepared','dispatching','unknown','conflict')"
                ),
                {"owner": account_id, "run": run_id},
            ).scalar_one_or_none()
            pending_commands = connection.execute(
                text(
                    "SELECT count(*)::integer FROM runtime_private.github_status_comment_effects "
                    "WHERE owner_id=:owner AND run_id=:run "
                    "AND effect_status IN ('prepared','dispatching','unknown','conflict')"
                ),
                {"owner": account_id, "run": run_id},
            ).scalar_one_or_none()
            active_allocations = connection.execute(
                text(
                    "SELECT count(*)::integer FROM runtime_private.sandbox_allocations "
                    "WHERE owner_id=:owner AND run_id=:run "
                    "AND status NOT IN ('stopped','destroyed','not_applied','released')"
                ),
                {"owner": account_id, "run": run_id},
            ).scalar_one_or_none()
            operator_allocations = connection.execute(
                text(
                    "SELECT count(*)::integer FROM runtime_private.sandbox_allocations "
                    "WHERE owner_id=:owner AND run_id=:run AND allocation_class='operator'"
                ),
                {"owner": account_id, "run": run_id},
            ).scalar_one_or_none()
            public_allocations = connection.execute(
                text(
                    "SELECT count(*)::integer FROM runtime_private.sandbox_allocations "
                    "WHERE owner_id=:owner AND run_id=:run AND allocation_class='public'"
                ),
                {"owner": account_id, "run": run_id},
            ).scalar_one_or_none()
            unknown_attempts = connection.execute(
                text(
                    "SELECT count(*)::integer FROM runtime_private.attempts "
                    "WHERE owner_id=:owner AND run_id=:run AND status='unknown'"
                ),
                {"owner": account_id, "run": run_id},
            ).scalar_one_or_none()
            result_rows = [dict(row) for row in results]
            approval_data = dict(approvals) if approvals is not None else None
            tool_counts = _tool_counts([row["payload"] for row in result_rows])
            validation, commands = _validation_summary(result_rows, approval_data)
            patch_bytes, patch_count = _candidate_patch(approval_data)
            approval_identity = _approval_identity_matches(
                approval_data,
                result_rows,
                metadata_run_id=run_id,
                revision=run["revision"],
                repository_id=run["github_repository_id"],
                profile_id=run["profile_id"],
                profile_version=run["profile_version"],
                validation_classification=validation,
            )
            validation_allocations_match = _operator_validation_allocations_match(
                connection, account_id, run_id, result_rows, approval_data
            )
            hidden_checks, hidden_metadata = _hidden_check_snapshot(
                connection,
                account_id,
                run_id,
                revision=run["revision"],
                repository_id=run["github_repository_id"],
                profile_id=run["profile_id"],
                profile_version=run["profile_version"],
                results=result_rows,
                observed_at=database_now,
            )
            metadata = _RunMetadata(
                run_id=run["run_id"],
                status=run["status"],
                revision=run["revision"],
                provider=run["provider"],
                model_id=run["model_id"],
                profile_id=run["profile_id"],
                profile_version=run["profile_version"],
                model_requests_authorized=run["model_requests_authorized"],
                model_requests_received=run["model_requests_received"],
                model_requests_ambiguous=run["model_requests_ambiguous"],
                model_usage_unknown_requests=run["model_usage_unknown_requests"],
                model_requests_reserved=run["model_requests_reserved"],
                input_tokens=run["model_input_tokens_used"],
                input_tokens_reserved=run["model_input_tokens_reserved"],
                output_tokens=run["model_output_tokens_used"],
                output_tokens_reserved=run["model_output_tokens_reserved"],
                input_tokens_unknown=bool(
                    turns["usage_unknown"] or run["model_usage_unknown_requests"]
                ),
                output_tokens_unknown=bool(
                    turns["usage_unknown"] or run["model_usage_unknown_requests"]
                ),
            )
            is_current_pending = (
                approvals is not None
                and approvals["status"] == "pending"
                and approvals["revision"] == metadata.revision
                and approvals["expires_at"] > database_now
            )
            protocol_ready = (
                metadata.status == "waiting_approval"
                and is_current_pending
                and validation in _PROTOCOL_PASS
                and pending == 0
                and pending_commands == 0
                and active_allocations == 0
                and (operator_allocations or 0) >= 2
                and validation_allocations_match
                and (public_allocations or 0) == 0
                and turns["dispatches"] > 0
                and metadata.model_requests_received > 0
                and turns["received"] > 0
                and metadata.model_requests_ambiguous == 0
                and metadata.model_usage_unknown_requests == 0
                and turns["unknown"] == 0
                and unknown_attempts == 0
                and approval_identity
            )
            return {
                **metadata.model_dump(mode="json"),
                "observed_at": database_now.astimezone(UTC).isoformat(),
                "dispatches": int(turns["dispatches"]),
                "tool_errors": None,
                "tool_unauthorized": None,
                "tool_results_accepted": tool_counts["accepted"],
                "unclassified_invalid_attempts": int(invalid_attempts or 0),
                "validation_classification": validation,
                "validation_commands": commands,
                "candidate_patch_bytes": patch_bytes,
                "candidate_patch_count": patch_count,
                "human_review_status": approvals["status"] if approvals is not None else "none",
                "pending_effects": int(pending or 0) + int(pending_commands or 0),
                "allocations_active": int(active_allocations or 0),
                "operator_allocations": int(operator_allocations or 0),
                "public_allocations": int(public_allocations or 0),
                "protocol_smoke_ready": bool(protocol_ready),
                "independent_hidden_checks": hidden_checks,
                **hidden_metadata,
                "task_correctness": "not_assessed",
            }
    except LiveEvaluationError:
        raise
    except Exception:
        raise LiveEvaluationError("live_snapshot_unavailable") from None


def _tool_counts(payloads: list[Any]) -> dict[str, int]:
    from continuum_api.tools.contracts import parse_tool_result

    valid = 0
    errors = 0
    unauthorized = 0
    for payload in payloads:
        if not isinstance(payload, dict) or "tool_result" not in payload:
            continue
        try:
            parse_tool_result(payload["tool_result"])
            valid += 1
        except Exception:
            errors += 1
            unauthorized += int(
                isinstance(payload.get("error_code"), str)
                and payload["error_code"].startswith("tool_unauthorized")
            )
    return {"accepted": valid, "errors": errors, "unauthorized": unauthorized}


def _validation_summary(
    payloads: list[Any], approval: Mapping[str, Any] | None
) -> tuple[str | None, list[dict[str, Any]]]:
    from continuum_api.validation.contracts import ValidationEvidence

    evidence_id = None if approval is None else approval.get("evidence_result_id")
    if not isinstance(evidence_id, UUID):
        try:
            evidence_id = UUID(str(evidence_id))
        except (ValueError, TypeError):
            return None, []
    manifest = None if approval is None else approval.get("manifest")
    validation = manifest.get("validation") if isinstance(manifest, dict) else None
    if not _evidence_digest_matches(payloads, evidence_id, validation):
        return "unknown", []
    for row in payloads:
        if not isinstance(row, Mapping) or row.get("result_id") != evidence_id:
            continue
        payload = row.get("payload")
        evidence_payload = _validation_evidence_payload(payload)
        if evidence_payload is None:
            continue
        try:
            evidence = ValidationEvidence.model_validate_json(
                json.dumps(evidence_payload, ensure_ascii=False, allow_nan=False)
            )
        except ValidationError:
            return "unknown", []
        commands = []
        for phase in (evidence.baseline, evidence.candidate):
            for command in phase.commands:
                commands.append(
                    {
                        "phase": command.phase,
                        "exit_code": command.exit_code,
                        "stdout_bytes": command.stdout_bytes,
                        "stderr_bytes": command.stderr_bytes,
                        "stdout_sha256": command.stdout_sha256,
                        "stderr_sha256": command.stderr_sha256,
                    }
                )
        return evidence.classification.value, commands[:48]
    return None, []


def _candidate_patch(approval: Mapping[str, Any] | None) -> tuple[None, int | None]:
    if approval is None or not isinstance(approval.get("manifest"), dict):
        return None, None
    candidate = approval["manifest"].get("candidate")
    if not isinstance(candidate, dict):
        return None, None
    changed_paths = candidate.get("changed_paths")
    if not isinstance(changed_paths, list) or len(changed_paths) > 5000:
        return None, None
    digest = approval.get("manifest_sha256")
    if not isinstance(digest, (bytes, bytearray, memoryview)) or len(digest) != 32:
        return None, None
    from continuum_api.approvals.manifest import digest_manifest

    try:
        _encoded, actual_digest = digest_manifest(approval["manifest"])
    except ValueError:
        return None, None
    if bytes(digest) != actual_digest:
        return None, None
    return None, len(changed_paths)


def _approval_identity_matches(
    approval: Mapping[str, Any] | None,
    results: Sequence[Mapping[str, Any]],
    *,
    metadata_run_id: UUID,
    revision: int,
    repository_id: int | None,
    profile_id: str,
    profile_version: str,
    validation_classification: str | None,
) -> bool:
    if approval is None or not isinstance(approval.get("manifest"), dict):
        return False
    manifest = approval["manifest"]
    digest = approval.get("manifest_sha256")
    from continuum_api.approvals.manifest import digest_manifest

    try:
        _manifest_bytes, expected_manifest_digest = digest_manifest(manifest)
    except Exception:
        return False
    try:
        evidence_id = UUID(str(approval["evidence_result_id"]))
        candidate_id = UUID(str(approval["candidate_artifact_id"]))
    except (KeyError, TypeError, ValueError):
        return False
    candidate = manifest.get("candidate")
    validation = manifest.get("validation")
    repository = manifest.get("repository")
    if (
        manifest.get("manifest_version") != 1
        or manifest.get("run_id") != str(metadata_run_id)
        or manifest.get("revision") != revision
        or not isinstance(repository, dict)
        or repository.get("id") != repository_id
        or not isinstance(candidate, dict)
        or candidate.get("checkpoint_id") != str(candidate_id)
        or not isinstance(validation, dict)
        or validation.get("evidence_result_id") != str(evidence_id)
        or validation.get("profile_id") != profile_id
        or validation.get("profile_version") != profile_version
        or validation.get("classification") != validation_classification
        or not isinstance(digest, (bytes, bytearray, memoryview))
        or len(digest) != 32
        or bytes(digest) != expected_manifest_digest
    ):
        return False
    evidence_payload = next(
        (
            row.get("payload")
            for row in results
            if row.get("result_id") == evidence_id and isinstance(row.get("payload"), dict)
        ),
        None,
    )
    if not isinstance(evidence_payload, dict):
        return False
    serialized_evidence = _validation_evidence_payload(evidence_payload)
    if serialized_evidence is None:
        return False
    from continuum_api.validation.contracts import ValidationEvidence

    try:
        evidence = ValidationEvidence.model_validate_json(
            json.dumps(serialized_evidence, ensure_ascii=False, allow_nan=False)
        )
    except (ValidationError, TypeError, ValueError):
        return False
    return (
        isinstance(evidence_payload, dict)
        and evidence_payload.get("candidate_artifact_id") == str(candidate_id)
        and candidate.get("archive_sha256") == evidence.candidate_archive_sha256
        and candidate.get("source_tree_sha256") == evidence.candidate_source_tree_sha256
        and _evidence_digest_matches(results, evidence_id, validation)
    )


def _operator_validation_allocations_match(
    connection: Any,
    account_id: UUID,
    run_id: UUID,
    results: Sequence[Mapping[str, Any]],
    approval: Mapping[str, Any] | None,
) -> bool:
    if approval is None:
        return False
    from continuum_api.validation.contracts import ValidationEvidence

    try:
        evidence_id = UUID(str(approval["evidence_result_id"]))
    except (KeyError, TypeError, ValueError):
        return False
    accepted = next((row for row in results if row.get("result_id") == evidence_id), None)
    payload = None if accepted is None else accepted.get("payload")
    serialized_evidence = _validation_evidence_payload(payload)
    if serialized_evidence is None:
        return False
    try:
        evidence = ValidationEvidence.model_validate_json(
            json.dumps(serialized_evidence, ensure_ascii=False, allow_nan=False)
        )
    except ValidationError:
        return False
    allocation_ids = (evidence.baseline.allocation_id, evidence.candidate.allocation_id)
    if (
        any(not isinstance(value, UUID) for value in allocation_ids)
        or allocation_ids[0] == allocation_ids[1]
    ):
        return False
    rows = (
        connection.execute(
            text(
                "SELECT allocation_id FROM runtime_private.sandbox_allocations "
                "WHERE owner_id=:owner AND run_id=:run "
                "AND allocation_id IN (:baseline,:candidate) "
                "AND allocation_class='operator' AND status IN ('stopped','destroyed')"
            ),
            {
                "owner": account_id,
                "run": run_id,
                "baseline": allocation_ids[0],
                "candidate": allocation_ids[1],
            },
        )
        .scalars()
        .all()
    )
    return set(rows) == set(allocation_ids)


def _hidden_check_snapshot(
    connection: Any,
    account_id: UUID,
    run_id: UUID,
    *,
    revision: int,
    repository_id: int | None,
    profile_id: str,
    profile_version: str,
    results: Sequence[Mapping[str, Any]],
    observed_at: datetime,
) -> tuple[str, dict[str, Any]]:
    """Verify only the latest current-revision accepted candidate-validation record."""
    candidates = []
    for row in results:
        payload = row.get("payload")
        if not isinstance(payload, dict) or payload.get("run_revision") != revision:
            continue
        if not any(key in payload for key in ("approval_validation", "validation_evidence")):
            continue
        candidates.append(row)
    if not candidates:
        return "not_run", {}
    row = max(candidates, key=lambda item: int(item.get("event_sequence") or 0))
    payload = row.get("payload")
    if not _accepted_payload_hash_matches(row):
        return "unknown", {}
    if not isinstance(payload, dict) or "hidden_check_evidence" not in payload:
        # Attention records can describe rejected observations without retaining
        # malformed evidence. They never establish a passing check.
        if isinstance(payload, dict):
            if payload.get("hidden_check_status") in {"invalid", "unknown"}:
                return payload["hidden_check_status"], {}
            if payload.get("hidden_check_error_code"):
                return "unknown", {}
        return "not_run", {}
    try:
        candidate_id = UUID(str(payload["candidate_artifact_id"]))
    except (KeyError, TypeError, ValueError):
        return "invalid", {}
    normal = _normal_validation_evidence(payload)
    if normal is None:
        return "unknown", {}
    hidden_value = payload.get("hidden_check_evidence")
    try:
        hidden = ManagedHiddenCheckEvidence.model_validate_json(
            _canonical_json(hidden_value), strict=True
        )
    except Exception:
        return "invalid", {}
    if (
        row.get("owner_id") != account_id
        or row.get("run_id") != run_id
        or hidden.run_id != run_id
        or hidden.step_id != row.get("step_id")
        or hidden.attempt_id != row.get("attempt_id")
        or hidden.generation != row.get("generation")
        or payload.get("run_revision") != revision
        or payload.get("candidate_artifact_id") != str(candidate_id)
        or normal.classification.value not in _PROTOCOL_PASS
        or normal.candidate.outcome.value != "passed"
        or normal.candidate.profile_id != profile_id
        or normal.candidate.profile_version != profile_version
        or normal.candidate.repository_id != repository_id
        or hidden.execution.profile_id != profile_id
        or hidden.execution.profile_version != profile_version
        or normal.candidate.image != hidden.execution.image
        or normal.candidate.runtime_version != hidden.execution.runtime_version
        or normal.candidate.npm_version != hidden.execution.npm_version
        or normal.candidate.base_commit_sha != hidden.execution.base_commit_sha
        or normal.candidate.baseline_git_tree_sha256 != hidden.execution.baseline_git_tree_sha256
        or normal.candidate.baseline_archive_sha256 != hidden.execution.baseline_archive_sha256
        or normal.candidate.baseline_archive_tree_sha256
        != hidden.execution.baseline_archive_tree_sha256
        or normal.candidate.artifact_id != hidden.execution.artifact_id
        or normal.candidate.package_count != hidden.execution.package_count
        or normal.candidate.source_archive_sha256 != hidden.execution.source_archive_sha256
        or normal.candidate.source_tree_sha256 != hidden.execution.source_tree_sha256
        or normal.candidate.allocation_id == hidden.execution.allocation_id
        or normal.baseline.allocation_id == hidden.execution.allocation_id
    ):
        return "invalid", {}
    step = (
        connection.execute(
            text(
                "SELECT activity,revision FROM runtime_private.steps "
                "WHERE owner_id=:owner AND run_id=:run AND step_id=:step"
            ),
            {"owner": account_id, "run": run_id, "step": hidden.step_id},
        )
        .mappings()
        .one_or_none()
    )
    if step is None or step["activity"] != "candidate_validation" or step["revision"] != revision:
        return "invalid", {}
    latest_artifact_id = connection.execute(
        text(
            "SELECT artifact_id FROM runtime_private.accepted_results "
            "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL "
            "ORDER BY event_sequence DESC LIMIT 1"
        ),
        {"owner": account_id, "run": run_id},
    ).scalar_one_or_none()
    if latest_artifact_id != candidate_id:
        return "invalid", {}
    artifact = (
        connection.execute(
            text(
                "SELECT status,archive_sha256,tree_sha256,github_repository_id,content_expires_at "
                "FROM runtime_private.artifact_uploads WHERE owner_id=:owner AND run_id=:run "
                "AND upload_id=:artifact"
            ),
            {"owner": account_id, "run": run_id, "artifact": candidate_id},
        )
        .mappings()
        .one_or_none()
    )
    if (
        artifact is None
        or artifact["status"] != "committed"
        or artifact["github_repository_id"] != repository_id
        or not _stored_digest_matches(
            artifact["archive_sha256"], normal.candidate.source_archive_sha256
        )
        or not _stored_digest_matches(artifact["tree_sha256"], normal.candidate.source_tree_sha256)
        or artifact["content_expires_at"] is None
        or artifact["content_expires_at"] <= observed_at
    ):
        return "invalid", {}
    baseline_id = normal.baseline.artifact_id
    baseline_artifact = (
        connection.execute(
            text(
                "SELECT status,archive_sha256,tree_sha256,github_repository_id,content_expires_at "
                "FROM runtime_private.artifact_uploads WHERE owner_id=:owner AND run_id=:run "
                "AND upload_id=:artifact"
            ),
            {"owner": account_id, "run": run_id, "artifact": baseline_id},
        )
        .mappings()
        .one_or_none()
    )
    if (
        baseline_artifact is None
        or baseline_artifact["status"] != "committed"
        or baseline_artifact["github_repository_id"] != repository_id
        or not _stored_digest_matches(
            baseline_artifact["archive_sha256"], normal.baseline.source_archive_sha256
        )
        or not _stored_digest_matches(
            baseline_artifact["tree_sha256"], normal.baseline.source_tree_sha256
        )
        or baseline_artifact["content_expires_at"] is None
        or baseline_artifact["content_expires_at"] <= observed_at
    ):
        return "invalid", {}
    allocation_key = f"t48:hidden:v1:{hidden.invocation_sha256}"
    allocation = (
        connection.execute(
            text(
                "SELECT allocation_id,owner_id,run_id,step_id,job_id,generation,attempt_id,"
                "lease_owner,allocation_class,status FROM runtime_private.sandbox_allocations "
                "WHERE owner_id=:owner AND run_id=:run AND logical_key=:logical_key "
                "AND allocation_id=:allocation"
            ),
            {
                "owner": account_id,
                "run": run_id,
                "logical_key": allocation_key,
                "allocation": hidden.execution.allocation_id,
            },
        )
        .mappings()
        .one_or_none()
    )
    if (
        allocation is None
        or allocation["allocation_id"] != hidden.execution.allocation_id
        or allocation["owner_id"] != row.get("owner_id")
        or allocation["run_id"] != row.get("run_id")
        or allocation["step_id"] != row.get("step_id")
        or allocation["job_id"] != row.get("job_id")
        or allocation["generation"] != row.get("generation")
        or allocation["attempt_id"] != row.get("attempt_id")
        or allocation["lease_owner"] != row.get("lease_owner")
        or allocation["allocation_class"] != "operator"
        or allocation["status"] not in {"stopped", "destroyed"}
        or allocation["status"] != hidden.execution.allocation_status
    ):
        return "invalid", {}
    outcome = hidden.comparison.outcome
    status = "passed" if outcome == "passed" else outcome
    if status not in _HIDDEN_STATUSES - {"not_run"}:
        return "invalid", {}
    expected_classification = {
        "failed": "hidden_check_failed",
        "invalid": "hidden_check_invalid",
        "unknown": "hidden_check_unknown",
    }.get(status)
    if "classification" in payload and (
        expected_classification is None or payload.get("classification") != expected_classification
    ):
        return "invalid", {}
    return status, {
        "hidden_check_suite_id": hidden.suite_id,
        "hidden_check_suite_sha256": hidden.suite_sha256,
        "hidden_check_count": hidden.comparison.total_count,
        "hidden_check_passed_count": hidden.comparison.passed_count,
    }


def _stored_digest_matches(value: Any, expected_hex: str) -> bool:
    return (
        isinstance(value, (bytes, bytearray, memoryview))
        and len(value) == 32
        and bytes(value).hex() == expected_hex
    )


def _accepted_payload_hash_matches(row: Mapping[str, Any]) -> bool:
    payload = row.get("payload")
    digest = row.get("result_sha256")
    if not isinstance(payload, dict) or not isinstance(digest, (bytes, bytearray, memoryview)):
        return False
    try:
        return bytes(digest) == hashlib.sha256(_canonical_json(payload).encode("utf-8")).digest()
    except (TypeError, ValueError, UnicodeError):
        return False


def _normal_validation_evidence(payload: Mapping[str, Any]) -> Any | None:
    value = payload.get("approval_validation", payload.get("validation_evidence"))
    if not isinstance(value, dict):
        return None
    from continuum_api.validation.contracts import ValidationEvidence

    try:
        return ValidationEvidence.model_validate_json(_canonical_json(value), strict=True)
    except (ValidationError, TypeError, ValueError):
        return None


def _aggregate_hidden_checks(cases: Mapping[str, Any]) -> str:
    values = []
    for case in cases.values():
        if not isinstance(case, Mapping):
            continue
        snapshot = case.get("snapshot")
        source = snapshot if isinstance(snapshot, Mapping) else case
        values.append(source.get("independent_hidden_checks"))
    if len(values) != 2 or any(value not in _HIDDEN_STATUSES for value in values):
        return "unknown"
    if values == ["not_run", "not_run"]:
        return "not_run"
    if values == ["passed", "passed"]:
        return "passed"
    if "unknown" in values:
        return "unknown"
    if "invalid" in values:
        return "invalid"
    if "failed" in values:
        return "failed"
    return "partial"


def _evidence_digest_matches(
    results: Sequence[Mapping[str, Any]], evidence_id: UUID, validation: Any
) -> bool:
    if not isinstance(validation, dict):
        return False
    expected_digest = validation.get("evidence_result_sha256")
    if not isinstance(expected_digest, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_digest):
        return False
    result = next((row for row in results if row.get("result_id") == evidence_id), None)
    digest = None if result is None else result.get("result_sha256")
    payload = None if result is None else result.get("payload")
    if not isinstance(payload, dict):
        return False
    try:
        payload_digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).digest()
    except (TypeError, ValueError, UnicodeError):
        return False
    return (
        isinstance(digest, (bytes, bytearray, memoryview))
        and bytes(digest).hex() == expected_digest
        and bytes(digest) == payload_digest
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )


def _validation_evidence_payload(payload: Any) -> Mapping[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("approval_validation", payload.get("validation_evidence"))
    return value if isinstance(value, dict) else None
