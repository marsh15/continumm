"""Compose persisted run pins with current authorization for fixed source tools."""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Protocol

from sqlalchemy import Engine, text

from continuum_api.artifacts.archives import VerifiedArchive
from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.repositories.preflight import (
    EligibilityReceipt,
    RepositoryAccessProof,
    RepositoryAccessVerifier,
    RepositoryPreflightError,
)
from continuum_api.runtime.agent_activity import AgentActivityStep
from continuum_api.runtime.agent_loop import (
    MAX_AGENT_TOOL_CALLS,
    MAX_AGENT_TURNS_PER_RUN,
    AgentLoopError,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, lock_current_execution
from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository
from continuum_api.runtime.validation_activity import ValidationActivityError
from continuum_api.runtime.validation_source import (
    PersistedCandidateValidationInputResolver,
    _verify_eligibility_baseline,
)
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import ToolPolicyContext
from continuum_api.tools.source_tools import (
    SourceToolError,
    discover_baseline_test_paths,
    snapshot_from_verified_archive,
    source_paths,
)

_CAPABILITIES = frozenset(
    {
        ToolCapability.LIST_FILES,
        ToolCapability.READ_FILE,
        ToolCapability.SEARCH_TEXT,
        ToolCapability.APPLY_PATCH,
        ToolCapability.DIFF,
    }
)
_MAX_RUN_TOOL_CALLS = MAX_AGENT_TURNS_PER_RUN * MAX_AGENT_TOOL_CALLS


class _RepositoryAccessResolver(Protocol):
    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof: ...


class PersistedToolPolicyContextResolver:
    """Build the five fixed file-tool capabilities from the active persisted run."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        checkpoints: CheckpointStorageService,
        access_verifier: _RepositoryAccessResolver | RepositoryAccessVerifier,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._checkpoints = checkpoints
        self._access_verifier = access_verifier
        # Reuse its complete current-revision/artifact/validation-pin checks for the
        # validation callback without duplicating that binding logic here.
        self._candidate_inputs = PersistedCandidateValidationInputResolver(
            engine, principal, checkpoints, self.for_validation
        )

    async def __call__(self, lease: ExecutionLease, step: AgentActivityStep) -> ToolPolicyContext:
        if lease.owner_id != self._principal.account_id:
            raise AgentLoopError("agent_tool_policy_owner_mismatch")
        try:
            binding = await asyncio.to_thread(self._read_agent_binding, lease, step)
            baseline = await self._checkpoints.restore_archive(
                self._principal, lease, binding["baseline_artifact_id"]
            )
            await asyncio.to_thread(_verify_eligibility_baseline, binding["eligibility"], baseline)
            return await self._build_context(
                lease,
                binding["repository_id"],
                binding["eligibility"],
                baseline,
                stage=ToolStage.AGENT_WORK,
                expected_revision=step.revision,
            )
        except AgentLoopError:
            raise
        except LeaseLost:
            raise AgentLoopError("agent_lease_lost") from None
        except (RepositoryPreflightError, SourceToolError, ValueError, TypeError):
            raise AgentLoopError("agent_tool_policy_pins_invalid") from None
        except Exception as error:
            code = getattr(error, "code", "agent_tool_policy_unavailable")
            raise AgentLoopError(_safe_code(code, "agent_tool_policy_unavailable")) from None

    async def for_validation(
        self,
        lease: ExecutionLease,
        eligibility: EligibilityReceipt,
        baseline: VerifiedArchive,
    ) -> ToolPolicyContext:
        """ValidationSource callback compatible with PersistedCandidateValidationInputResolver."""
        if lease.owner_id != self._principal.account_id:
            raise ValidationActivityError("candidate_validation_owner_mismatch")
        try:
            binding = await asyncio.to_thread(self._candidate_inputs._read_binding, lease)
            admission = await asyncio.to_thread(self._read_admission_identity, lease)
            serialized = json.dumps(
                eligibility.to_cache_payload(),
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            if (
                binding["eligibility_receipt"] != eligibility.to_cache_payload()
                or hashlib.sha256(serialized).digest() != binding["eligibility_sha256"]
                or admission["root_artifact_id"] != binding["baseline_artifact_id"]
                or admission["repository_id"] != eligibility.repository_id
                or admission["profile_id"] != eligibility.profile_id
                or admission["profile_version"] != eligibility.profile_version
                or admission["source_sha"] != eligibility.commit_sha
                or admission["source_tree_sha"] != eligibility.tree_sha
                or admission["source_receipt"] != eligibility.to_cache_payload()
            ):
                raise ValidationActivityError("candidate_validation_pin_mismatch")
            await asyncio.to_thread(_verify_eligibility_baseline, eligibility, baseline)
            return await self._build_context(
                lease,
                eligibility.repository_id,
                eligibility,
                baseline,
                stage=ToolStage.AGENT_WORK,
                expected_revision=binding["revision"],
            )
        except ValidationActivityError:
            raise
        except LeaseLost:
            raise ValidationActivityError("candidate_validation_lease_lost") from None
        except (RepositoryPreflightError, SourceToolError, ValueError, TypeError):
            raise ValidationActivityError("candidate_validation_policy_pins_invalid") from None
        except Exception:
            raise ValidationActivityError("candidate_validation_policy_unavailable") from None

    def _read_agent_binding(self, lease: ExecutionLease, step: AgentActivityStep) -> dict[str, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            row = (
                connection.execute(
                    text(
                        "SELECT run.github_repository_id, run.revision, run.status, "
                        "step.activity, step.logical_key, step.revision AS step_revision, "
                        "pins.profile_id, pins.profile_version, pins.source_sha, "
                        "pins.source_tree_sha, pins.source_receipt, "
                        "source_pin.baseline_artifact_id, source_pin.eligibility_receipt, "
                        "source_pin.eligibility_sha256, source_pin.revision AS pin_revision, "
                        "root.artifact_id AS root_artifact_id "
                        "FROM runtime_private.runs AS run "
                        "JOIN runtime_private.steps AS step ON step.owner_id=run.owner_id "
                        "AND step.run_id=run.run_id "
                        "JOIN runtime_private.run_admission_pins AS pins "
                        "ON pins.owner_id=run.owner_id AND pins.run_id=run.run_id "
                        "LEFT JOIN runtime_private.validation_source_pins AS source_pin "
                        "ON source_pin.owner_id=run.owner_id AND source_pin.run_id=run.run_id "
                        "LEFT JOIN LATERAL (SELECT artifact_id FROM "
                        "runtime_private.accepted_results AS accepted "
                        "WHERE accepted.owner_id=run.owner_id AND accepted.run_id=run.run_id "
                        "AND accepted.artifact_id IS NOT NULL ORDER BY event_sequence ASC LIMIT 1) "
                        "AS root ON TRUE WHERE run.owner_id=:owner AND run.run_id=:run "
                        "AND step.step_id=:step"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id, "step": lease.step_id},
                )
                .mappings()
                .one_or_none()
            )
        if (
            row is None
            or step.activity != "agent_tool"
            or row["activity"] != step.activity
            or row["logical_key"] != step.logical_key
            or row["step_revision"] != step.revision
            or row["revision"] != step.revision
            or row["status"] != RunStatus.RUNNING.value
            or row["root_artifact_id"] is None
            or row["baseline_artifact_id"] != row["root_artifact_id"]
            or row["pin_revision"] is None
            or row["pin_revision"] > row["revision"]
        ):
            raise AgentLoopError("agent_tool_policy_step_stale")
        try:
            admission_receipt = EligibilityReceipt.from_cache_payload(row["source_receipt"])
            eligibility = EligibilityReceipt.from_cache_payload(row["eligibility_receipt"])
            raw = json.dumps(
                eligibility.to_cache_payload(),
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            if (
                row["source_receipt"] != admission_receipt.to_cache_payload()
                or row["eligibility_receipt"] != admission_receipt.to_cache_payload()
                or hashlib.sha256(raw).digest() != bytes(row["eligibility_sha256"])
                or row["github_repository_id"] != eligibility.repository_id
                or row["profile_id"] != eligibility.profile_id
                or row["profile_version"] != eligibility.profile_version
                or row["source_sha"] != eligibility.commit_sha
                or row["source_tree_sha"] != eligibility.tree_sha
            ):
                raise ValueError
        except (RepositoryPreflightError, TypeError, ValueError):
            raise AgentLoopError("agent_tool_policy_pins_invalid") from None
        return {
            "repository_id": row["github_repository_id"],
            "eligibility": eligibility,
            "baseline_artifact_id": row["baseline_artifact_id"],
            "revision": int(row["revision"]),
        }

    async def _build_context(
        self,
        lease: ExecutionLease,
        repository_id: int,
        eligibility: EligibilityReceipt,
        baseline: VerifiedArchive,
        *,
        stage: ToolStage,
        expected_revision: int,
    ) -> ToolPolicyContext:
        # Re-read and fence current persisted identity immediately before authorizing.
        if lease.owner_id != self._principal.account_id:
            raise AgentLoopError("agent_tool_policy_owner_mismatch")
        binding = await asyncio.to_thread(self._read_current_run_and_budget, lease)
        if binding["repository_id"] != repository_id or binding["revision"] != expected_revision:
            raise AgentLoopError("agent_tool_policy_repository_mismatch")
        if binding["user"] is None or binding["user"]["status"] != "active":
            raise AgentLoopError("agent_tool_policy_actor_unavailable")
        github_user_id = binding["user"]["github_user_id"]
        proof = await self._access_verifier.verify(
            repository_id=repository_id,
            github_user_id=github_user_id,
        )
        if (
            not isinstance(proof, RepositoryAccessProof)
            or proof.repository_id != repository_id
            or proof.github_user_id != github_user_id
            or type(proof.actor_can_write) is not bool
            or type(proof.app_can_access) is not bool
        ):
            raise AgentLoopError("agent_tool_policy_authorization_invalid")

        # Authorization may take a network round trip. Re-fence the lease and
        # reread the budget after it, so a stale worker cannot carry an old allow.
        current = await asyncio.to_thread(self._read_current_run_and_budget, lease)
        if (
            current["repository_id"] != repository_id
            or current["revision"] != expected_revision
            or current["user"] is None
            or current["user"]["status"] != "active"
            or current["user"]["github_user_id"] != github_user_id
        ):
            raise AgentLoopError("agent_tool_policy_step_stale")

        source = await asyncio.to_thread(
            snapshot_from_verified_archive,
            baseline,
            expected_archive_sha256=baseline.archive_sha256,
            expected_tree_sha256=baseline.manifest.tree_sha256,
        )
        paths = frozenset(source_paths(source))
        tests = discover_baseline_test_paths(
            source, eligibility.profile_id, expected_tree_sha256=source.tree_sha256
        )
        return ToolPolicyContext(
            capabilities=_CAPABILITIES,
            profile_id=eligibility.profile_id,
            profile_version=eligibility.profile_version,
            actor_can_write_repository=proof.actor_can_write,
            app_can_access_repository=proof.app_can_access,
            stage=stage,
            run_budget_available=current["run_budget_available"],
            baseline_tree_sha256=source.tree_sha256,
            baseline_source_paths=paths,
            baseline_test_paths=tests,
            baseline_paths_verified=True,
            protected_paths=frozenset(path for path, _digest in eligibility.manifest_blobs),
        )

    def _read_current_run_and_budget(self, lease: ExecutionLease) -> dict[str, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            run = (
                connection.execute(
                    text(
                        "SELECT github_repository_id, revision, status FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one_or_none()
            )
            user = (
                connection.execute(
                    text(
                        "SELECT github_user_id, status FROM runtime_private.users "
                        "WHERE user_id=:owner"
                    ),
                    {"owner": lease.owner_id},
                )
                .mappings()
                .one_or_none()
            )
            tool_calls_used: int = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.agent_invocations "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one()
            tool_turns_used: int = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.agent_turns "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one()
            model_budget_exceeded = connection.execute(
                text(
                    "SELECT model_budget_exceeded FROM runtime_private.runs "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one_or_none()
        if run is None or run["status"] != RunStatus.RUNNING.value:
            raise AgentLoopError("agent_tool_policy_run_stale")
        if (
            type(tool_calls_used) is not int
            or tool_calls_used < 0
            or type(tool_turns_used) is not int
            or tool_turns_used < 0
        ):
            raise AgentLoopError("agent_tool_policy_budget_invalid")
        return {
            "repository_id": run["github_repository_id"],
            "revision": int(run["revision"]),
            "run_budget_available": (
                model_budget_exceeded is False
                and tool_turns_used <= MAX_AGENT_TURNS_PER_RUN
                and tool_calls_used <= _MAX_RUN_TOOL_CALLS
            ),
            "user": user,
        }

    def _read_admission_identity(self, lease: ExecutionLease) -> dict[str, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            row = (
                connection.execute(
                    text(
                        "SELECT run.github_repository_id, pins.profile_id, "
                        "pins.profile_version, pins.source_sha, pins.source_tree_sha, "
                        "pins.source_receipt, root.artifact_id AS root_artifact_id "
                        "FROM runtime_private.runs AS run "
                        "JOIN runtime_private.run_admission_pins AS pins "
                        "ON pins.owner_id=run.owner_id AND pins.run_id=run.run_id "
                        "LEFT JOIN LATERAL (SELECT artifact_id FROM "
                        "runtime_private.accepted_results AS accepted "
                        "WHERE accepted.owner_id=run.owner_id AND accepted.run_id=run.run_id "
                        "AND accepted.artifact_id IS NOT NULL "
                        "ORDER BY event_sequence ASC LIMIT 1) AS root ON TRUE "
                        "WHERE run.owner_id=:owner AND run.run_id=:run"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise ValidationActivityError("candidate_validation_admission_missing")
        try:
            receipt = EligibilityReceipt.from_cache_payload(row["source_receipt"])
            if row["source_receipt"] != receipt.to_cache_payload():
                raise ValueError
        except (RepositoryPreflightError, TypeError, ValueError):
            raise ValidationActivityError("candidate_validation_admission_pin_invalid") from None
        return {
            "repository_id": row["github_repository_id"],
            "profile_id": row["profile_id"],
            "profile_version": row["profile_version"],
            "source_sha": row["source_sha"],
            "source_tree_sha": row["source_tree_sha"],
            "source_receipt": receipt.to_cache_payload(),
            "root_artifact_id": row["root_artifact_id"],
        }


def _safe_code(value: object, fallback: str) -> str:
    code = str(value)
    return (
        code if code.isascii() and code.replace("_", "").isalnum() and len(code) <= 96 else fallback
    )
