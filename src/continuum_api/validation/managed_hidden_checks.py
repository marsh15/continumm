"""Prepare independent hidden-check evidence in a fresh operator allocation.

This is an internal, explicitly enabled service, not a public API or workflow
handler. Its caller selects the trusted suite and commits returned evidence via
a fenced transition. No expected answers enter the sandbox.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from typing import Literal, Protocol, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from continuum_api.repositories.preflight import EligibilityReceipt
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.sandbox_allocations import AllocationClass, AllocationReservation
from continuum_api.sandbox.config import (
    ALLOCATION_MEMORY_MB,
    ALLOCATION_VCPUS,
    SandboxAllocationLimits,
)
from continuum_api.sandbox.hidden_probe_recipe import HiddenProbe
from continuum_api.sandbox.validation.preparation import ValidationSourceAuthorization
from continuum_api.validation.contracts import PhaseEvidence, PhaseOutcome, ValidationPhase
from continuum_api.validation.hidden_checks import (
    HiddenCheckResult,
    HiddenCheckSuite,
    compare_hidden_output,
)
from continuum_api.validation.service import ProfileResult, ValidationService

_LOGGER = logging.getLogger(__name__)


class ManagedHiddenCheckError(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class HiddenProfile(Protocol):
    @property
    def configured_image(self) -> str | None: ...

    async def run_hidden_probe(
        self,
        principal: Principal,
        lease: ExecutionLease,
        artifact_id: UUID,
        eligibility: EligibilityReceipt,
        allocation_id: UUID,
        *,
        validation_source: ValidationSourceAuthorization,
        probe: HiddenProbe,
    ) -> ProfileResult: ...


class FreshAllocationCreator(Protocol):
    async def create(
        self,
        principal: Principal,
        lease: ExecutionLease,
        logical_key: str,
        requested_seconds: int,
        *,
        allocation_class: AllocationClass = "public",
        require_new: bool = False,
    ) -> AllocationReservation: ...


class AllocationCleanup(Protocol):
    async def stop(self, principal: Principal, allocation_id: UUID) -> AllocationReservation: ...


class ManagedHiddenCheckEvidence(BaseModel):
    """Prepared digest-only evidence; process success alone is not correctness."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    evidence_version: Literal[1] = 1
    allocation_class: Literal["operator"] = "operator"
    run_id: UUID
    step_id: UUID
    attempt_id: UUID
    generation: int = Field(ge=1)
    suite_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,63}$")
    suite_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    entry_path: str = Field(min_length=1, max_length=4096)
    invocation_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    execution: PhaseEvidence
    comparison: HiddenCheckResult

    @model_validator(mode="after")
    def coherent(self) -> Self:
        expected_command = {
            "python-pytest": "python-hidden-probe-v1",
            "node-npm": "node-hidden-probe-v1",
        }.get(self.execution.profile_id)
        if (
            expected_command is None
            or self.execution.profile_version != "1"
            or self.execution.command_id != expected_command
            or self.execution.phase is not ValidationPhase.CANDIDATE
            or self.execution.allocation_id is None
            or self.execution.allocation_status not in {"stopped", "destroyed"}
            or self.comparison.suite_sha256 != self.suite_sha256
            or self.comparison.candidate_archive_sha256 != self.execution.source_archive_sha256
            or self.comparison.candidate_tree_sha256 != self.execution.source_tree_sha256
        ):
            raise ManagedHiddenCheckError("hidden_evidence_identity_mismatch")
        HiddenProbe(
            self.execution.profile_id, self.execution.profile_version, self.entry_path, "[null]"
        )
        if self.invocation_sha256 != _invocation_digest(
            self.suite_sha256, self.entry_path, expected_command
        ):
            raise ManagedHiddenCheckError("hidden_evidence_invocation_mismatch")
        commands = [
            command for command in self.execution.commands if command.phase == "hidden_probe"
        ]
        if (
            len(commands) != 1
            or commands[0].exit_code != self.execution.test_exit_code
            or commands[0].stdout_sha256 != self.comparison.stdout_sha256
            or commands[0].stderr_sha256 != self.comparison.stderr_sha256
            or not 1 <= self.comparison.total_count <= 32
            or not 0 <= self.comparison.passed_count <= self.comparison.total_count
            or re.fullmatch(r"[a-z0-9_]{1,96}", self.comparison.code) is None
        ):
            raise ManagedHiddenCheckError("hidden_evidence_observation_mismatch")
        if self.comparison.outcome == "passed" and (
            self.execution.outcome is not PhaseOutcome.PASSED
            or self.execution.test_exit_code != 0
            or not 1 <= self.comparison.total_count <= 32
            or self.comparison.passed_count != self.comparison.total_count
        ):
            raise ManagedHiddenCheckError("hidden_evidence_pass_invalid")
        return self


def _invocation_digest(suite_digest: str, entry_path: str, command_id: str) -> str:
    payload = json.dumps(
        {"suite_sha256": suite_digest, "entry_path": entry_path, "command_id": command_id},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ManagedHiddenCheckService:
    def __init__(
        self,
        python_profile: HiddenProfile,
        node_profile: HiddenProfile,
        allocation_creator: FreshAllocationCreator,
        cleanup: AllocationCleanup,
        *,
        allocation_seconds: int = 300,
    ) -> None:
        if type(allocation_seconds) is not int or not 1 <= allocation_seconds <= 300:
            raise ValueError("hidden_allocation_duration_invalid")
        self._profiles = {"python-pytest": python_profile, "node-npm": node_profile}
        self._creator = allocation_creator
        self._cleanup = cleanup
        self._seconds = allocation_seconds

    async def run(
        self,
        principal: Principal,
        lease: ExecutionLease,
        source: ValidationSourceAuthorization,
        suite: HiddenCheckSuite,
        *,
        entry_path: str,
        allow_operator_sandbox: bool = False,
    ) -> ManagedHiddenCheckEvidence:
        if allow_operator_sandbox is not True:
            raise ManagedHiddenCheckError("hidden_operator_opt_in_required")
        if principal.account_id != lease.owner_id:
            raise ManagedHiddenCheckError("hidden_owner_mismatch")
        if not isinstance(source, ValidationSourceAuthorization) or not isinstance(
            suite, HiddenCheckSuite
        ):
            raise ManagedHiddenCheckError("hidden_request_invalid")
        eligibility = source.eligibility
        ValidationService._validate_source(
            source, baseline_artifact_id=source.baseline_artifact_id, eligibility=eligibility
        )
        if (
            (suite.profile_id, suite.profile_version) != (source.profile_id, source.profile_version)
            or suite.candidate_archive_sha256 != source.candidate_archive_sha256
            or suite.candidate_tree_sha256 != source.candidate_source_tree_sha256
        ):
            raise ManagedHiddenCheckError("hidden_suite_source_mismatch")
        probe = HiddenProbe(
            suite.profile_id, suite.profile_version, entry_path, suite.public_inputs_json()
        )
        if entry_path not in {entry.path for entry in source.candidate_manifest.entries}:
            raise ManagedHiddenCheckError("hidden_entry_unavailable")
        argv = probe.argv()
        if len(argv) > 32 or any(len(arg.encode("utf-8")) > 4096 for arg in argv):
            raise ManagedHiddenCheckError("hidden_arguments_invalid")
        profile = self._profiles[suite.profile_id]
        image = profile.configured_image
        if image is None:
            raise ManagedHiddenCheckError("hidden_image_unconfigured")
        SandboxAllocationLimits(
            image=image,
            session_seconds=self._seconds,
            vcpus=ALLOCATION_VCPUS,
            memory_mb=ALLOCATION_MEMORY_MB,
        )
        invocation_digest = _invocation_digest(suite.digest, entry_path, probe.command_id)
        reservation = await self._creator.create(
            principal,
            lease,
            f"t48:hidden:v1:{invocation_digest}",
            self._seconds,
            allocation_class="operator",
            require_new=True,
        )
        # Never stop a returned allocation belonging to another execution.
        if (
            reservation.owner_id,
            reservation.run_id,
            reservation.step_id,
            reservation.job_id,
            reservation.attempt_id,
            reservation.generation,
            reservation.lease_owner,
        ) != (
            lease.owner_id,
            lease.run_id,
            lease.step_id,
            lease.job_id,
            lease.attempt_id,
            lease.generation,
            lease.lease_owner,
        ):
            raise ManagedHiddenCheckError("hidden_allocation_identity_mismatch")
        complete = False
        try:
            if (
                reservation.status != "running"
                or reservation.allocation_class != "operator"
                or reservation.provider_session_id is None
                or reservation.max_requested_seconds != self._seconds
            ):
                raise ManagedHiddenCheckError("hidden_allocation_unavailable")
            result = await profile.run_hidden_probe(
                principal,
                lease,
                source.baseline_artifact_id,
                eligibility,
                reservation.allocation_id,
                validation_source=source,
                probe=probe,
            )
            execution = ValidationService._phase_from_result(
                result,
                phase=ValidationPhase.CANDIDATE,
                expected_allocation_id=reservation.allocation_id,
                baseline_artifact_id=source.baseline_artifact_id,
                eligibility=eligibility,
                source_archive_sha256=source.candidate_archive_sha256,
                source_tree_sha256=source.candidate_source_tree_sha256,
                baseline_source_tree_sha256=source.baseline_source_tree_sha256,
                baseline_archive_sha256=source.baseline_archive_sha256,
                candidate_archive_sha256=source.candidate_archive_sha256,
                candidate_source_tree_sha256=source.candidate_source_tree_sha256,
                expected_command_id=probe.command_id,
                expected_image=image,
            )
            complete = True
            observations = [
                item for item in result.outputs if getattr(item, "phase", None) == "hidden_probe"
            ]
            if len(observations) != 1:
                raise ManagedHiddenCheckError("hidden_observation_count_invalid")
            observation = observations[0]
            if getattr(observation, "exit_code", None) != result.test_exit_code:
                raise ManagedHiddenCheckError("hidden_observation_exit_mismatch")
            stdout = getattr(observation, "stdout", None)
            stderr = getattr(observation, "stderr", None)
            if not isinstance(stdout, str) or not isinstance(stderr, str):
                raise ManagedHiddenCheckError("hidden_observation_output_invalid")
            comparison = compare_hidden_output(
                suite,
                candidate_archive_sha256=source.candidate_archive_sha256,
                candidate_tree_sha256=source.candidate_source_tree_sha256,
                exit_code=result.test_exit_code,
                stdout=stdout,
                stderr=stderr,
            )
            return ManagedHiddenCheckEvidence(
                run_id=lease.run_id,
                step_id=lease.step_id,
                attempt_id=lease.attempt_id,
                generation=lease.generation,
                suite_id=suite.suite_id,
                suite_sha256=suite.digest,
                entry_path=entry_path,
                invocation_sha256=invocation_digest,
                execution=execution,
                comparison=comparison,
            )
        finally:
            if not complete:
                # stop() journals dispatch, observes exact identity and does not
                # blindly redispatch an earlier unknown cleanup effect.
                task = asyncio.create_task(self._cleanup.stop(principal, reservation.allocation_id))
                try:
                    async with asyncio.timeout(95):
                        await asyncio.shield(task)
                except (Exception, asyncio.CancelledError) as error:
                    _LOGGER.warning("hidden_cleanup_unresolved error_type=%s", type(error).__name__)
