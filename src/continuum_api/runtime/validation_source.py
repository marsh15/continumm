"""Persist and resolve the trusted inputs for candidate validation."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from continuum_api.artifacts.archives import VerifiedArchive
from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.repositories.preflight import EligibilityReceipt, RepositoryPreflightError
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, lock_current_execution
from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository
from continuum_api.runtime.validation_activity import (
    CandidateValidationInput,
    ValidationActivityError,
)
from continuum_api.sandbox.validation.preparation import (
    CandidateValidationError,
    ValidationSourceAuthorization,
    prepare_validation_source,
)
from continuum_api.tools.policy import ToolPolicyContext
from continuum_api.tools.source_tools import (
    PatchCandidate,
    SourceToolError,
    candidate_from_baseline,
    snapshot_from_verified_archive,
)

MAX_VALIDATION_RECEIPT_BYTES = 4096

PolicyContextResolver = Callable[
    [ExecutionLease, EligibilityReceipt, VerifiedArchive],
    ToolPolicyContext | Awaitable[ToolPolicyContext],
]


@dataclass(frozen=True, slots=True)
class ValidationSourcePin:
    baseline_artifact_id: UUID
    eligibility: EligibilityReceipt
    eligibility_sha256: bytes
    revision: int


class ValidationSourcePinService:
    """Pin an actual preflight receipt to the accepted run baseline once."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        checkpoints: CheckpointStorageService,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._checkpoints = checkpoints

    async def pin(
        self,
        lease: ExecutionLease,
        eligibility: EligibilityReceipt,
        baseline_artifact_id: UUID,
    ) -> ValidationSourcePin:
        if lease.owner_id != self._principal.account_id:
            raise ValidationActivityError("validation_source_pin_owner_mismatch")
        if not isinstance(eligibility, EligibilityReceipt) or not isinstance(
            baseline_artifact_id, UUID
        ):
            raise ValidationActivityError("validation_source_pin_invalid")
        try:
            canonical_payload = eligibility.to_cache_payload()
            canonical_receipt = EligibilityReceipt.from_cache_payload(canonical_payload)
            payload_json = _canonical_json(canonical_payload)
            payload_bytes = payload_json.encode("utf-8", errors="strict")
            if len(payload_bytes) > MAX_VALIDATION_RECEIPT_BYTES:
                raise ValidationActivityError("validation_source_pin_receipt_too_large")
        except (RepositoryPreflightError, TypeError, ValueError, UnicodeError):
            raise ValidationActivityError("validation_source_pin_receipt_invalid") from None
        digest = hashlib.sha256(payload_bytes).digest()

        binding = await asyncio.to_thread(
            self._assert_pin_baseline, lease, canonical_receipt, baseline_artifact_id
        )
        try:
            verified = await self._checkpoints.restore_archive(
                self._principal, lease, baseline_artifact_id
            )
            await asyncio.to_thread(_verify_eligibility_baseline, canonical_receipt, verified)
        except ValidationActivityError:
            raise
        except Exception as error:
            code = getattr(error, "code", "validation_source_baseline_invalid")
            raise ValidationActivityError(_safe_code(code)) from None

        return await asyncio.to_thread(
            self._insert_pin,
            lease,
            canonical_receipt,
            payload_json,
            digest,
            baseline_artifact_id,
            binding["revision"],
        )

    def _assert_pin_baseline(
        self,
        lease: ExecutionLease,
        eligibility: EligibilityReceipt,
        baseline_artifact_id: UUID,
    ) -> dict[str, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            run = (
                connection.execute(
                    text(
                        "SELECT run.github_repository_id, run.revision, run.status, "
                        "step.revision AS step_revision FROM runtime_private.runs AS run "
                        "JOIN runtime_private.steps AS step ON step.owner_id=run.owner_id "
                        "AND step.run_id=run.run_id WHERE run.owner_id=:owner AND run.run_id=:run "
                        "AND step.step_id=:step"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id, "step": lease.step_id},
                )
                .mappings()
                .one_or_none()
            )
            root = connection.execute(
                text(
                    "SELECT artifact_id FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL "
                    "ORDER BY event_sequence ASC LIMIT 1"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one_or_none()
            artifact = (
                connection.execute(
                    text(
                        "SELECT github_repository_id, archive_sha256, tree_sha256, status, "
                        "content_expires_at > clock_timestamp() AS content_active, file_count, "
                        "source_bytes FROM runtime_private.artifact_uploads "
                        "WHERE owner_id=:owner AND run_id=:run AND upload_id=:artifact"
                    ),
                    {
                        "owner": lease.owner_id,
                        "run": lease.run_id,
                        "artifact": baseline_artifact_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            existing = (
                connection.execute(
                    text(
                        "SELECT baseline_artifact_id, eligibility_receipt, eligibility_sha256, "
                        "revision "
                        "FROM runtime_private.validation_source_pins "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one_or_none()
            )

        if (
            run is None
            or run["status"] != RunStatus.RUNNING.value
            or run["revision"] != run["step_revision"]
            or root != baseline_artifact_id
            or artifact is None
            or artifact["status"] != "committed"
            or not artifact["content_active"]
            or artifact["github_repository_id"] != run["github_repository_id"]
            or eligibility.repository_id != run["github_repository_id"]
        ):
            raise ValidationActivityError("validation_source_pin_baseline_invalid")
        if existing is None and run["revision"] != 0:
            raise ValidationActivityError("validation_source_pin_revision_invalid")
        if existing is not None and (
            existing["baseline_artifact_id"] != baseline_artifact_id
            or existing["eligibility_sha256"]
            != hashlib.sha256(
                _canonical_json(eligibility.to_cache_payload()).encode("utf-8")
            ).digest()
            or existing["eligibility_receipt"] != eligibility.to_cache_payload()
        ):
            raise ValidationActivityError("validation_source_pin_conflict")
        return {"revision": int(run["revision"]), "artifact": artifact}

    def _insert_pin(
        self,
        lease: ExecutionLease,
        eligibility: EligibilityReceipt,
        payload_json: str,
        digest: bytes,
        baseline_artifact_id: UUID,
        revision: int,
    ) -> ValidationSourcePin:
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                lock_current_execution(connection, self._principal, lease)
                row = (
                    connection.execute(
                        text(
                            "SELECT run.revision, run.status, step.revision AS step_revision "
                            "FROM runtime_private.runs AS run JOIN runtime_private.steps AS step "
                            "ON step.owner_id=run.owner_id AND step.run_id=run.run_id "
                            "WHERE run.owner_id=:owner AND run.run_id=:run AND step.step_id=:step"
                        ),
                        {"owner": lease.owner_id, "run": lease.run_id, "step": lease.step_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    row is None
                    or row["status"] != RunStatus.RUNNING.value
                    or row["revision"] != row["step_revision"]
                    or row["revision"] != revision
                ):
                    raise ValidationActivityError("validation_source_pin_stale")
                existing = (
                    connection.execute(
                        text(
                            "SELECT baseline_artifact_id, eligibility_receipt, eligibility_sha256, "
                            "revision FROM runtime_private.validation_source_pins "
                            "WHERE owner_id=:owner AND run_id=:run"
                        ),
                        {"owner": lease.owner_id, "run": lease.run_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if existing is None and row["revision"] != 0:
                    raise ValidationActivityError("validation_source_pin_revision_invalid")
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.validation_source_pins "
                        "(owner_id, run_id, baseline_artifact_id, eligibility_receipt, "
                        "eligibility_sha256, revision, step_id, job_id, generation, attempt_id, "
                        "lease_owner) VALUES (:owner, :run, :baseline, CAST(:receipt AS jsonb), "
                        ":digest, :revision, :step, :job, :generation, :attempt, :lease_owner) "
                        "ON CONFLICT (owner_id, run_id) DO NOTHING"
                    ),
                    {
                        "owner": lease.owner_id,
                        "run": lease.run_id,
                        "baseline": baseline_artifact_id,
                        "receipt": payload_json,
                        "digest": digest,
                        "revision": revision,
                        "step": lease.step_id,
                        "job": lease.job_id,
                        "generation": lease.generation,
                        "attempt": lease.attempt_id,
                        "lease_owner": lease.lease_owner,
                    },
                )
                stored = (
                    connection.execute(
                        text(
                            "SELECT baseline_artifact_id, eligibility_receipt, eligibility_sha256, "
                            "revision FROM runtime_private.validation_source_pins "
                            "WHERE owner_id=:owner AND run_id=:run"
                        ),
                        {"owner": lease.owner_id, "run": lease.run_id},
                    )
                    .mappings()
                    .one()
                )
                if (
                    stored["baseline_artifact_id"] != baseline_artifact_id
                    or stored["eligibility_receipt"] != eligibility.to_cache_payload()
                    or stored["eligibility_sha256"] != digest
                    or stored["revision"] > row["revision"]
                ):
                    raise ValidationActivityError("validation_source_pin_conflict")
                return ValidationSourcePin(
                    baseline_artifact_id=baseline_artifact_id,
                    eligibility=eligibility,
                    eligibility_sha256=digest,
                    revision=int(stored["revision"]),
                )
        except (ValidationActivityError, LeaseLost):
            raise
        except Exception:
            raise ValidationActivityError("validation_source_pin_unavailable") from None


class PersistedCandidateValidationInputResolver:
    """Rebuild candidate authorization from immutable pins and accepted bytes."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        checkpoints: CheckpointStorageService,
        resolve_policy_context: PolicyContextResolver,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._checkpoints = checkpoints
        self._resolve_policy_context = resolve_policy_context

    async def __call__(self, lease: ExecutionLease) -> CandidateValidationInput:
        if lease.owner_id != self._principal.account_id:
            raise ValidationActivityError("candidate_validation_owner_mismatch")
        binding = await asyncio.to_thread(self._read_binding, lease)
        try:
            eligibility = EligibilityReceipt.from_cache_payload(binding["eligibility_receipt"])
            if binding["eligibility_receipt"] != eligibility.to_cache_payload():
                raise ValidationActivityError("candidate_validation_pin_corrupt")
            payload_json = _canonical_json(eligibility.to_cache_payload())
            if (
                len(payload_json.encode("utf-8")) > MAX_VALIDATION_RECEIPT_BYTES
                or hashlib.sha256(payload_json.encode("utf-8")).digest()
                != binding["eligibility_sha256"]
            ):
                raise ValidationActivityError("candidate_validation_pin_corrupt")
            baseline_artifact_id = binding["baseline_artifact_id"]
            candidate_artifact_id = binding["candidate_artifact_id"]
            baseline = await self._checkpoints.restore_archive(
                self._principal, lease, baseline_artifact_id
            )
            await asyncio.to_thread(_verify_eligibility_baseline, eligibility, baseline)
            candidate_verified = await self._checkpoints.restore_archive(
                self._principal, lease, candidate_artifact_id
            )
            candidate = await asyncio.to_thread(
                _candidate_from_archives, baseline, candidate_verified
            )
            context = await asyncio.to_thread(
                _call_policy_context,
                self._resolve_policy_context,
                lease,
                eligibility,
                baseline,
            )
            if inspect.isawaitable(context):
                context = await context
            if not isinstance(context, ToolPolicyContext):
                raise ValidationActivityError("candidate_validation_policy_context_invalid")
            source = await asyncio.to_thread(
                prepare_validation_source,
                eligibility,
                baseline_artifact_id,
                baseline,
                candidate,
                context,
            )
            if not isinstance(source, ValidationSourceAuthorization):
                raise ValidationActivityError("candidate_validation_source_invalid")
            current = await asyncio.to_thread(self._read_binding, lease)
            if (
                current["revision"] != binding["revision"]
                or current["baseline_artifact_id"] != baseline_artifact_id
                or current["candidate_artifact_id"] != candidate_artifact_id
                or current["eligibility_sha256"] != binding["eligibility_sha256"]
            ):
                raise ValidationActivityError("candidate_validation_source_stale")
            return CandidateValidationInput(
                baseline_artifact_id=baseline_artifact_id,
                candidate_artifact_id=candidate_artifact_id,
                eligibility=eligibility,
                source=source,
            )
        except ValidationActivityError:
            raise
        except (RepositoryPreflightError, CandidateValidationError, SourceToolError) as error:
            raise ValidationActivityError(
                _safe_code(getattr(error, "code", "source_invalid"))
            ) from None
        except Exception as error:
            code = getattr(error, "code", "candidate_validation_source_unavailable")
            raise ValidationActivityError(_safe_code(code)) from None

    def _read_binding(self, lease: ExecutionLease) -> dict[str, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            run = (
                connection.execute(
                    text(
                        "SELECT run.github_repository_id, run.revision, run.status, "
                        "step.activity, step.revision AS step_revision "
                        "FROM runtime_private.runs AS run "
                        "JOIN runtime_private.steps AS step ON step.owner_id=run.owner_id "
                        "AND step.run_id=run.run_id WHERE run.owner_id=:owner AND run.run_id=:run "
                        "AND step.step_id=:step"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id, "step": lease.step_id},
                )
                .mappings()
                .one_or_none()
            )
            pin = (
                connection.execute(
                    text(
                        "SELECT baseline_artifact_id, eligibility_receipt, eligibility_sha256, "
                        "revision "
                        "FROM runtime_private.validation_source_pins "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one_or_none()
            )
            root = connection.execute(
                text(
                    "SELECT artifact_id FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL "
                    "ORDER BY event_sequence ASC LIMIT 1"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one_or_none()
            latest = connection.execute(
                text(
                    "SELECT accepted.artifact_id FROM runtime_private.accepted_results AS accepted "
                    "JOIN runtime_private.steps AS step ON step.owner_id=accepted.owner_id "
                    "AND step.run_id=accepted.run_id AND step.step_id=accepted.step_id "
                    "WHERE accepted.owner_id=:owner AND accepted.run_id=:run "
                    "AND accepted.artifact_id IS NOT NULL AND step.revision=:revision "
                    "AND accepted.event_sequence=(SELECT MAX(event_sequence) "
                    "FROM runtime_private.accepted_results WHERE owner_id=:owner AND run_id=:run "
                    "AND artifact_id IS NOT NULL) LIMIT 1"
                ),
                {
                    "owner": lease.owner_id,
                    "run": lease.run_id,
                    "revision": run["revision"] if run is not None else -1,
                },
            ).scalar_one_or_none()
            if run is None or pin is None:
                raise ValidationActivityError("candidate_validation_inputs_unpinned")
            artifacts = (
                connection.execute(
                    text(
                        "SELECT upload_id, github_repository_id, archive_sha256, tree_sha256, "
                        "status, "
                        "content_expires_at > clock_timestamp() AS content_active "
                        "FROM runtime_private.artifact_uploads "
                        "WHERE owner_id=:owner AND run_id=:run "
                        "AND upload_id IN (:baseline, :candidate)"
                    ),
                    {
                        "owner": lease.owner_id,
                        "run": lease.run_id,
                        "baseline": pin["baseline_artifact_id"],
                        "candidate": latest,
                    },
                )
                .mappings()
                .all()
            )
        by_id = {row["upload_id"]: row for row in artifacts}
        baseline = by_id.get(pin["baseline_artifact_id"])
        candidate = by_id.get(latest)
        try:
            receipt = EligibilityReceipt.from_cache_payload(pin["eligibility_receipt"])
        except RepositoryPreflightError:
            raise ValidationActivityError("candidate_validation_pin_corrupt") from None
        if pin["eligibility_receipt"] != receipt.to_cache_payload():
            raise ValidationActivityError("candidate_validation_pin_corrupt")
        if (
            run["status"] != RunStatus.RUNNING.value
            or run["activity"] != "candidate_validation"
            or run["revision"] != run["step_revision"]
            or pin["revision"] > run["revision"]
            or root != pin["baseline_artifact_id"]
            or latest is None
            or latest == pin["baseline_artifact_id"]
            or baseline is None
            or candidate is None
            or baseline["status"] != "committed"
            or candidate["status"] != "committed"
            or not baseline["content_active"]
            or not candidate["content_active"]
            or baseline["github_repository_id"] != run["github_repository_id"]
            or candidate["github_repository_id"] != run["github_repository_id"]
            or receipt.repository_id != run["github_repository_id"]
        ):
            raise ValidationActivityError("candidate_validation_artifact_binding_invalid")
        receipt_json = _canonical_json(receipt.to_cache_payload())
        if hashlib.sha256(receipt_json.encode("utf-8")).digest() != pin["eligibility_sha256"]:
            raise ValidationActivityError("candidate_validation_pin_corrupt")
        return {
            "baseline_artifact_id": pin["baseline_artifact_id"],
            "candidate_artifact_id": latest,
            "revision": int(run["revision"]),
            "eligibility_receipt": receipt.to_cache_payload(),
            "eligibility_sha256": bytes(pin["eligibility_sha256"]),
        }


def _verify_eligibility_baseline(
    eligibility: EligibilityReceipt, baseline: VerifiedArchive
) -> None:
    if (
        len(baseline.manifest.entries) != eligibility.file_count
        or sum(entry.size for entry in baseline.manifest.entries) != eligibility.source_bytes
    ):
        raise ValidationActivityError("validation_source_baseline_receipt_mismatch")
    try:
        if eligibility.profile_id == "python-pytest":
            from continuum_api.sandbox.python_profile import build_python_profile_plan

            build_python_profile_plan(eligibility, baseline)
        elif eligibility.profile_id == "node-npm":
            from continuum_api.sandbox.node_profile import build_node_profile_plan

            build_node_profile_plan(eligibility, baseline)
        else:
            raise ValidationActivityError("validation_source_profile_unsupported")
    except ValidationActivityError:
        raise
    except Exception:
        raise ValidationActivityError("validation_source_baseline_receipt_mismatch") from None


def _candidate_from_archives(
    baseline: VerifiedArchive, candidate: VerifiedArchive
) -> PatchCandidate:
    baseline_source = snapshot_from_verified_archive(
        baseline,
        expected_archive_sha256=baseline.archive_sha256,
        expected_tree_sha256=baseline.manifest.tree_sha256,
    )
    candidate_source = snapshot_from_verified_archive(
        candidate,
        expected_archive_sha256=candidate.archive_sha256,
        expected_tree_sha256=candidate.manifest.tree_sha256,
    )
    return candidate_from_baseline(baseline_source, candidate_source)


def _call_policy_context(
    resolver: PolicyContextResolver,
    lease: ExecutionLease,
    eligibility: EligibilityReceipt,
    baseline: VerifiedArchive,
) -> ToolPolicyContext | Awaitable[ToolPolicyContext]:
    return resolver(lease, eligibility, baseline)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )


def _safe_code(value: object) -> str:
    code = str(value)
    return (
        code
        if code.isascii() and code.replace("_", "").isalnum() and len(code) <= 96
        else "validation_source_invalid"
    )
