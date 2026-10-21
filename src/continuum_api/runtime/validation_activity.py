"""Lease-bound candidate validation and approval-freeze activity handlers."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Protocol, cast
from uuid import UUID

from sqlalchemy import Engine, text

from continuum_api.repositories.preflight import EligibilityReceipt
from continuum_api.runtime.approvals import ApprovalService
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, lock_current_execution
from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository
from continuum_api.runtime.transitions import TransitionService
from continuum_api.sandbox.hidden_probe_recipe import HiddenProbe
from continuum_api.sandbox.validation.preparation import ValidationSourceAuthorization
from continuum_api.validation.contracts import ValidationClassification
from continuum_api.validation.hidden_checks import HiddenCheckSuite
from continuum_api.validation.managed_hidden_checks import (
    ManagedHiddenCheckError,
    ManagedHiddenCheckEvidence,
    ManagedHiddenCheckService,
)
from continuum_api.validation.service import ValidationService


@dataclass(frozen=True, slots=True)
class CandidateValidationInput:
    """Trusted, prepared T13/T14 inputs resolved for one validation lease."""

    baseline_artifact_id: UUID
    candidate_artifact_id: UUID
    eligibility: EligibilityReceipt
    source: ValidationSourceAuthorization


class CandidateValidationInputResolver(Protocol):
    def __call__(
        self, lease: ExecutionLease
    ) -> CandidateValidationInput | Awaitable[CandidateValidationInput]: ...


@dataclass(frozen=True, slots=True)
class HiddenCheckSelection:
    """Trusted operator-selected hidden suite and candidate entry point."""

    suite: HiddenCheckSuite
    entry_path: str


class HiddenCheckSelector(Protocol):
    def __call__(
        self, lease: ExecutionLease, inputs: CandidateValidationInput
    ) -> HiddenCheckSelection | Awaitable[HiddenCheckSelection]: ...


class CandidateValidationActivityHandler:
    """Run fixed candidate validation and persist its exact result under T08."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        resolver: CandidateValidationInputResolver,
        validation: ValidationService,
        approvals: ApprovalService,
        transitions: TransitionService,
        *,
        hidden_checks: ManagedHiddenCheckService | None = None,
        hidden_check_selector: HiddenCheckSelector | None = None,
    ) -> None:
        if (hidden_checks is None) != (hidden_check_selector is None):
            raise ValueError("candidate_validation_hidden_dependencies_unpaired")
        self._engine = engine
        self._principal = principal
        self._resolver = resolver
        self._validation = validation
        self._approvals = approvals
        self._transitions = transitions
        self._hidden_checks = hidden_checks
        self._hidden_check_selector = hidden_check_selector

    async def __call__(self, lease: ExecutionLease) -> Any:
        revision = await asyncio.to_thread(self._assert_current_candidate_step, lease)
        if inspect.iscoroutinefunction(self._resolver):
            inputs = await self._resolver(lease)
        else:
            inputs = await asyncio.to_thread(self._resolver, lease)
            if inspect.isawaitable(inputs):
                inputs = await inputs
        if not isinstance(inputs, CandidateValidationInput):
            raise ValidationActivityError("candidate_validation_inputs_invalid")
        await asyncio.to_thread(self._assert_persisted_artifact_binding, lease, inputs)

        selection: HiddenCheckSelection | None = None
        if self._hidden_check_selector is not None:
            selection = await self._resolve_hidden_selection(lease, inputs)
            await asyncio.to_thread(self._assert_current_candidate_step, lease, revision)
            await asyncio.to_thread(self._assert_persisted_artifact_binding, lease, inputs)

        evidence = await self._validation.run(
            self._principal,
            lease,
            inputs.baseline_artifact_id,
            inputs.eligibility,
            inputs.source,
        )
        await asyncio.to_thread(self._assert_current_candidate_step, lease, revision)
        if evidence.classification in {
            ValidationClassification.PASSED,
            ValidationClassification.PASSED_WITH_BASELINE_FAILURE,
        }:
            hidden_evidence: ManagedHiddenCheckEvidence | None = None
            if selection is not None:
                # The first fence protects the allocation boundary; the second
                # prevents an expired attempt from committing its result.
                await asyncio.to_thread(self._assert_persisted_artifact_binding, lease, inputs)
                await asyncio.to_thread(self._assert_current_candidate_step, lease, revision)
                assert self._hidden_checks is not None
                try:
                    raw_hidden_evidence = await self._hidden_checks.run(
                        self._principal,
                        lease,
                        inputs.source,
                        selection.suite,
                        entry_path=selection.entry_path,
                        allow_operator_sandbox=True,
                    )
                except (LeaseLost, asyncio.CancelledError):
                    raise
                except Exception as error:
                    code = _safe_hidden_error_code(error)
                    await asyncio.to_thread(self._assert_current_candidate_step, lease, revision)
                    await asyncio.to_thread(self._assert_persisted_artifact_binding, lease, inputs)
                    return await self._attention_result(
                        lease,
                        inputs,
                        evidence,
                        run_revision=revision,
                        hidden_status="unknown",
                        classification="hidden_check_unknown",
                        detail={"code": code},
                    )
                await asyncio.to_thread(self._assert_current_candidate_step, lease, revision)
                await asyncio.to_thread(self._assert_persisted_artifact_binding, lease, inputs)
                try:
                    hidden_evidence = _validate_hidden_evidence(
                        raw_hidden_evidence, lease, inputs.source, selection, evidence
                    )
                except (ManagedHiddenCheckError, TypeError, ValueError):
                    return await self._attention_result(
                        lease,
                        inputs,
                        evidence,
                        run_revision=revision,
                        hidden_status="invalid",
                        classification="hidden_check_invalid",
                    )
                if hidden_evidence.comparison.outcome != "passed":
                    outcome = hidden_evidence.comparison.outcome
                    classification = {
                        "failed": "hidden_check_failed",
                        "invalid": "hidden_check_invalid",
                        "unknown": "hidden_check_unknown",
                    }.get(outcome, "hidden_check_invalid")
                    return await self._attention_result(
                        lease,
                        inputs,
                        evidence,
                        run_revision=revision,
                        hidden_status=(
                            outcome if outcome in {"failed", "invalid", "unknown"} else "invalid"
                        ),
                        classification=classification,
                        hidden_evidence=hidden_evidence,
                    )
            approval_kwargs = (
                {"hidden_check_evidence": hidden_evidence} if hidden_evidence is not None else {}
            )
            return await asyncio.to_thread(
                self._approvals.record_validation,
                lease,
                inputs.candidate_artifact_id,
                evidence,
                **approval_kwargs,
            )

        return await asyncio.to_thread(
            self._transitions.commit_result,
            lease=lease,
            result={
                "activity": "candidate_validation",
                "candidate_artifact_id": str(inputs.candidate_artifact_id),
                "run_revision": revision,
                "classification": evidence.classification.value,
                "validation_evidence": evidence.model_dump(mode="json"),
            },
            target_run_status=RunStatus.NEEDS_ATTENTION,
        )

    async def _resolve_hidden_selection(
        self, lease: ExecutionLease, inputs: CandidateValidationInput
    ) -> HiddenCheckSelection:
        selector = self._hidden_check_selector
        assert selector is not None
        try:
            if inspect.iscoroutinefunction(selector):
                selection = await selector(lease, inputs)
            else:
                selection = await asyncio.to_thread(selector, lease, inputs)
                if inspect.isawaitable(selection):
                    selection = await selection
            if type(selection) is not HiddenCheckSelection:
                raise ValueError
            suite = selection.suite
            source = inputs.source
            if (
                type(suite) is not HiddenCheckSuite
                or (suite.profile_id, suite.profile_version)
                != (source.profile_id, source.profile_version)
                or suite.candidate_archive_sha256 != source.candidate_archive_sha256
                or suite.candidate_tree_sha256 != source.candidate_source_tree_sha256
                or selection.entry_path
                not in {entry.path for entry in source.candidate_manifest.entries}
            ):
                raise ValueError
            HiddenProbe(
                suite.profile_id,
                suite.profile_version,
                selection.entry_path,
                suite.public_inputs_json(),
            )
            return selection
        except (LeaseLost, asyncio.CancelledError):
            raise
        except Exception:
            raise ValidationActivityError("candidate_validation_hidden_selection_invalid") from None

    async def _attention_result(
        self,
        lease: ExecutionLease,
        inputs: CandidateValidationInput,
        evidence: Any,
        *,
        run_revision: int,
        hidden_status: str,
        classification: str,
        detail: dict[str, str] | None = None,
        hidden_evidence: ManagedHiddenCheckEvidence | None = None,
    ) -> Any:
        result: dict[str, Any] = {
            "activity": "candidate_validation",
            "run_revision": run_revision,
            "candidate_artifact_id": str(inputs.candidate_artifact_id),
            "classification": classification,
            "validation_evidence": evidence.model_dump(mode="json"),
            "hidden_check_status": hidden_status,
        }
        if detail and "code" in detail:
            result["hidden_check_error_code"] = detail["code"]
        if hidden_evidence is not None:
            result["hidden_check_evidence"] = hidden_evidence.model_dump(mode="json")
        return await asyncio.to_thread(
            self._transitions.commit_result,
            lease=lease,
            result=result,
            target_run_status=RunStatus.NEEDS_ATTENTION,
        )

    def _assert_current_candidate_step(
        self, lease: ExecutionLease, expected_revision: int | None = None
    ) -> int:
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                lock_current_execution(connection, self._principal, lease)
                row = (
                    connection.execute(
                        text(
                            "SELECT run.status AS run_status, run.revision, "
                            "step.activity, step.revision AS step_revision "
                            "FROM runtime_private.runs AS run JOIN runtime_private.steps AS step "
                            "ON step.owner_id=run.owner_id AND step.run_id=run.run_id "
                            "WHERE run.owner_id=:owner AND run.run_id=:run AND step.step_id=:step"
                        ),
                        {"owner": lease.owner_id, "run": lease.run_id, "step": lease.step_id},
                    )
                    .mappings()
                    .one_or_none()
                )
        except LeaseLost:
            raise
        if (
            row is None
            or row["activity"] != "candidate_validation"
            or row["run_status"] != RunStatus.RUNNING.value
            or row["revision"] != row["step_revision"]
            or (expected_revision is not None and row["revision"] != expected_revision)
        ):
            raise ValidationActivityError("candidate_validation_step_stale")
        return cast(int, row["revision"])

    def _assert_persisted_artifact_binding(
        self, lease: ExecutionLease, inputs: CandidateValidationInput
    ) -> None:
        if (
            not isinstance(inputs.baseline_artifact_id, UUID)
            or not isinstance(inputs.candidate_artifact_id, UUID)
            or not isinstance(inputs.eligibility, EligibilityReceipt)
            or not isinstance(inputs.source, ValidationSourceAuthorization)
            or inputs.baseline_artifact_id == inputs.candidate_artifact_id
        ):
            raise ValidationActivityError("candidate_validation_inputs_invalid")
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
            roots = connection.execute(
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
                    "AND accepted.artifact_id IS NOT NULL AND step.revision=(SELECT revision "
                    "FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run) "
                    "AND accepted.event_sequence=(SELECT MAX(event_sequence) "
                    "FROM runtime_private.accepted_results WHERE owner_id=:owner AND run_id=:run "
                    "AND artifact_id IS NOT NULL) "
                    "ORDER BY accepted.event_sequence DESC LIMIT 1"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one_or_none()
            artifacts = (
                connection.execute(
                    text(
                        "SELECT upload_id, github_repository_id, archive_sha256, "
                        "tree_sha256, status, "
                        "content_expires_at > clock_timestamp() AS content_active "
                        "FROM runtime_private.artifact_uploads "
                        "WHERE owner_id=:owner AND run_id=:run "
                        "AND upload_id IN (:baseline, :candidate)"
                    ),
                    {
                        "owner": lease.owner_id,
                        "run": lease.run_id,
                        "baseline": inputs.baseline_artifact_id,
                        "candidate": inputs.candidate_artifact_id,
                    },
                )
                .mappings()
                .all()
            )
        by_id = {row["upload_id"]: row for row in artifacts}
        baseline = by_id.get(inputs.baseline_artifact_id)
        candidate = by_id.get(inputs.candidate_artifact_id)
        source = inputs.source
        if (
            run is None
            or run["status"] != RunStatus.RUNNING.value
            or run["revision"] != run["step_revision"]
            or roots != inputs.baseline_artifact_id
            or latest != inputs.candidate_artifact_id
            or baseline is None
            or candidate is None
            or baseline["status"] != "committed"
            or candidate["status"] != "committed"
            or not baseline["content_active"]
            or not candidate["content_active"]
            or baseline["github_repository_id"] != run["github_repository_id"]
            or candidate["github_repository_id"] != run["github_repository_id"]
            or inputs.eligibility.repository_id != run["github_repository_id"]
            or source.baseline_artifact_id != inputs.baseline_artifact_id
            or source.eligibility != inputs.eligibility
            or source.baseline_archive_sha256 != bytes(baseline["archive_sha256"]).hex()
            or source.baseline_source_tree_sha256 != bytes(baseline["tree_sha256"]).hex()
            or source.candidate_archive_sha256 != bytes(candidate["archive_sha256"]).hex()
            or source.candidate_source_tree_sha256 != bytes(candidate["tree_sha256"]).hex()
        ):
            raise ValidationActivityError("candidate_validation_artifact_binding_invalid")


class ApprovalFreezeActivityHandler:
    """Delegate one separately leased approval-freeze job to ApprovalService."""

    def __init__(self, engine: Engine, principal: Principal, approvals: ApprovalService) -> None:
        self._engine = engine
        self._principal = principal
        self._approvals = approvals

    async def __call__(self, lease: ExecutionLease) -> Any:
        await asyncio.to_thread(self._assert_current_freeze_step, lease)
        return await self._approvals.freeze(lease)

    def _assert_current_freeze_step(self, lease: ExecutionLease) -> None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            row = (
                connection.execute(
                    text(
                        "SELECT run.status AS run_status, run.revision, "
                        "step.activity, step.revision AS step_revision "
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
            or row["activity"] != "approval_freeze"
            or row["run_status"] != RunStatus.RUNNING.value
            or row["revision"] != row["step_revision"]
        ):
            raise ValidationActivityError("approval_freeze_step_stale")


class ValidationActivityError(RuntimeError):
    """Sanitized candidate-validation activity boundary rejection."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _safe_hidden_error_code(error: Exception) -> str:
    code = getattr(error, "code", None)
    if (
        isinstance(code, str)
        and 1 <= len(code) <= 96
        and all(
            char.isascii() and (char.islower() or char.isdigit() or char == "_") for char in code
        )
    ):
        return code
    return "hidden_check_service_failed"


def _validate_hidden_evidence(
    raw: Any,
    lease: ExecutionLease,
    source: ValidationSourceAuthorization,
    selection: HiddenCheckSelection,
    normal_evidence: Any,
) -> ManagedHiddenCheckEvidence:
    if type(raw) is not ManagedHiddenCheckEvidence:
        raise ManagedHiddenCheckError("hidden_evidence_type_invalid")
    evidence = ManagedHiddenCheckEvidence.model_validate_json(raw.model_dump_json(), strict=True)
    execution = evidence.execution
    normal_candidate = normal_evidence.candidate
    if (
        (evidence.run_id, evidence.step_id, evidence.attempt_id, evidence.generation)
        != (lease.run_id, lease.step_id, lease.attempt_id, lease.generation)
        or evidence.suite_id != selection.suite.suite_id
        or evidence.suite_sha256 != selection.suite.digest
        or evidence.entry_path != selection.entry_path
        or (execution.profile_id, execution.profile_version)
        != (source.profile_id, source.profile_version)
        or execution.source_archive_sha256 != source.candidate_archive_sha256
        or execution.source_tree_sha256 != source.candidate_source_tree_sha256
        or evidence.comparison.candidate_archive_sha256 != source.candidate_archive_sha256
        or evidence.comparison.candidate_tree_sha256 != source.candidate_source_tree_sha256
        or execution.image != normal_candidate.image
        or execution.runtime_version != normal_candidate.runtime_version
        or execution.npm_version != normal_candidate.npm_version
        or execution.package_count != normal_candidate.package_count
    ):
        raise ManagedHiddenCheckError("hidden_evidence_identity_mismatch")
    return evidence
