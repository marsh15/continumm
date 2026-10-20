"""Resolve admitted inputs for the first persisted agent turn."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Iterable
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.repositories.preflight import EligibilityReceipt, RepositoryPreflightError
from continuum_api.runtime.agent_activity import (
    AgentActivityStep,
    AgentTurnInputs,
    PersistedAgentTurnInputResolver,
)
from continuum_api.runtime.agent_context import AgentContextError, canonicalize_initial_context
from continuum_api.runtime.agent_loop import ANTHROPIC_MODEL_ID, OPENAI_MODEL_ID, AgentLoopError
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, lock_current_execution
from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository
from continuum_api.runtime.validation_activity import ValidationActivityError
from continuum_api.runtime.validation_source import ValidationSourcePinService

_FIRST_AGENT_TURN_KEY = "agent-turn:0"
_SOURCE_PREPARATION_KEY = "admission.source"


class AdmittedAgentTurnInputResolver:
    """Initialize revision-zero model inputs only from accepted admission data.

    On the initial ``agent-turn:0`` lease this verifies the immutable admission
    and accepted source-preparation result, pins the eligibility receipt, and
    initializes the persisted model/context pins. Every other turn only reads
    the persisted resolver, so rework cannot reset the initial context.
    """

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        checkpoints: CheckpointStorageService,
        *,
        tool_schemas: Iterable[dict[str, Any]],
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._source_pins = ValidationSourcePinService(engine, principal, checkpoints)
        self._resolver = PersistedAgentTurnInputResolver(
            engine, principal, tool_schemas=tool_schemas
        )

    async def __call__(self, lease: ExecutionLease, step: AgentActivityStep) -> AgentTurnInputs:
        is_first_turn = (
            step.activity == "agent_turn"
            and step.revision == 0
            and step.logical_key == _FIRST_AGENT_TURN_KEY
        )
        if is_first_turn:
            try:
                admission, baseline_artifact_id = await asyncio.to_thread(
                    self._read_and_verify_admission, lease, step
                )
            except LeaseLost:
                raise AgentLoopError("agent_lease_lost") from None
            try:
                await self._source_pins.pin(lease, admission["receipt"], baseline_artifact_id)
            except ValidationActivityError as error:
                raise AgentLoopError(error.code) from None
            except LeaseLost:
                raise AgentLoopError("agent_lease_lost") from None
            await self._resolver.initialize(
                lease,
                step,
                context=admission["task_context"],
                model_id=admission["model_id"],
                baseline_artifact_id=baseline_artifact_id,
            )
        return await self._resolver(lease, step)

    def _read_and_verify_admission(
        self, lease: ExecutionLease, step: AgentActivityStep
    ) -> tuple[dict[str, Any], UUID]:
        if lease.owner_id != self._principal.account_id:
            raise AgentLoopError("agent_lease_owner_mismatch")
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                lock_current_execution(connection, self._principal, lease)
                rows = (
                    connection.execute(
                        text(
                            "SELECT run.github_repository_id, run.revision, run.status, "
                            "run.model_id AS run_model_id, run.model_task_context, "
                            "run.model_tool_schema_sha256, step.activity, step.logical_key, "
                            "step.revision AS step_revision, job.replay_policy, pins.profile_id, "
                            "pins.profile_version, pins.source_sha, pins.source_tree_sha, "
                            "pins.source_receipt, pins.provider, pins.model_id, pins.task_context, "
                            "accepted.step_id AS source_step_id, accepted.job_id AS source_job_id, "
                            "accepted.artifact_id AS baseline_artifact_id, accepted.next_job_id, "
                            "accepted.payload AS source_payload, accepted.result_sha256, "
                            "source_step.activity AS source_activity, "
                            "source_step.logical_key AS source_logical_key, "
                            "source_step.revision AS source_revision, "
                            "next_job.step_id AS next_step_id, "
                            "next_step.activity AS next_activity, "
                            "next_step.logical_key AS next_logical_key, "
                            "next_step.revision AS next_revision "
                            "FROM runtime_private.runs AS run "
                            "JOIN runtime_private.steps AS step ON step.owner_id=run.owner_id "
                            "AND step.run_id=run.run_id "
                            "JOIN runtime_private.jobs AS job ON job.owner_id=step.owner_id "
                            "AND job.run_id=step.run_id AND job.step_id=step.step_id "
                            "JOIN runtime_private.run_admission_pins AS pins "
                            "ON pins.owner_id=run.owner_id AND pins.run_id=run.run_id "
                            "LEFT JOIN LATERAL (SELECT result.* FROM "
                            "runtime_private.accepted_results AS result WHERE "
                            "result.owner_id=run.owner_id AND result.run_id=run.run_id "
                            "ORDER BY result.event_sequence ASC LIMIT 1) AS accepted ON TRUE "
                            "LEFT JOIN runtime_private.steps AS source_step "
                            "ON source_step.owner_id=accepted.owner_id "
                            "AND source_step.run_id=accepted.run_id "
                            "AND source_step.step_id=accepted.step_id "
                            "LEFT JOIN runtime_private.jobs AS next_job "
                            "ON next_job.owner_id=accepted.owner_id "
                            "AND next_job.run_id=accepted.run_id "
                            "AND next_job.job_id=accepted.next_job_id "
                            "LEFT JOIN runtime_private.steps AS next_step "
                            "ON next_step.owner_id=next_job.owner_id "
                            "AND next_step.run_id=next_job.run_id "
                            "AND next_step.step_id=next_job.step_id "
                            "WHERE run.owner_id=:owner AND run.run_id=:run "
                            "AND step.step_id=:step"
                        ),
                        {"owner": lease.owner_id, "run": lease.run_id, "step": lease.step_id},
                    )
                    .mappings()
                    .one_or_none()
                )
        except (LeaseLost, AgentLoopError):
            raise
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

        if (
            rows is None
            or step.activity != "agent_turn"
            or step.logical_key != _FIRST_AGENT_TURN_KEY
            or step.revision != 0
            or rows["activity"] != step.activity
            or rows["logical_key"] != step.logical_key
            or rows["step_revision"] != 0
            or rows["revision"] != 0
            or rows["status"] != RunStatus.RUNNING.value
            or rows["replay_policy"] != "replay_safe"
        ):
            raise AgentLoopError("agent_admission_step_invalid")

        try:
            receipt = EligibilityReceipt.from_cache_payload(rows["source_receipt"])
            if not isinstance(rows["task_context"], dict):
                raise ValueError
            task_context = canonicalize_initial_context(rows["task_context"])
            task_context = json.loads(_canonical_json(task_context))
            payload = rows["source_payload"]
            if not isinstance(payload, dict):
                raise ValueError
            result_hash = hashlib.sha256(_canonical_json(payload).encode("utf-8")).digest()
            expected_provider_model = {
                "openai": OPENAI_MODEL_ID,
                "anthropic": ANTHROPIC_MODEL_ID,
            }.get(rows["provider"])
        except (
            AgentContextError,
            RepositoryPreflightError,
            TypeError,
            ValueError,
            UnicodeError,
            RecursionError,
        ):
            raise AgentLoopError("agent_admission_binding_invalid") from None

        if (
            type(rows["github_repository_id"]) is not int
            or rows["github_repository_id"] <= 0
            or rows["github_repository_id"] != receipt.repository_id
            or rows["profile_id"] != receipt.profile_id
            or rows["profile_version"] != receipt.profile_version
            or rows["source_sha"] != receipt.commit_sha
            or rows["source_tree_sha"] != receipt.tree_sha
            or rows["model_id"] != expected_provider_model
            or receipt.profile_id not in {"python-pytest", "node-npm"}
            or receipt.profile_version != "1"
            or rows["source_activity"] != "source_preparation"
            or rows["source_logical_key"] != _SOURCE_PREPARATION_KEY
            or rows["source_revision"] != 0
            or rows["baseline_artifact_id"] is None
            or not isinstance(rows["baseline_artifact_id"], UUID)
            or rows["next_job_id"] != lease.job_id
            or rows["next_step_id"] != lease.step_id
            or rows["next_activity"] != "agent_turn"
            or rows["next_logical_key"] != _FIRST_AGENT_TURN_KEY
            or rows["next_revision"] != 0
            or rows["source_payload"]
            != {
                "activity": "source_preparation",
                "repository_id": rows["github_repository_id"],
                "commit_sha": rows["source_sha"],
                "git_tree_sha": rows["source_tree_sha"],
                "profile_id": rows["profile_id"],
                "profile_version": rows["profile_version"],
            }
            or bytes(rows["result_sha256"]) != result_hash
        ):
            raise AgentLoopError("agent_admission_binding_invalid")

        # Existing run pins are either wholly absent or exactly match admission.
        run_values = (
            rows["run_model_id"],
            rows["model_task_context"],
            rows["model_tool_schema_sha256"],
        )
        if any(value is not None for value in run_values) and (
            any(value is None for value in run_values)
            or rows["run_model_id"] != rows["model_id"]
            or rows["model_task_context"] != task_context
        ):
            raise AgentLoopError("agent_admission_model_pin_conflict")

        return (
            {"receipt": receipt, "task_context": task_context, "model_id": rows["model_id"]},
            rows["baseline_artifact_id"],
        )


def _canonical_json(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        encoded.encode("utf-8", errors="strict")
        return encoded
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError):
        raise ValueError("agent_admission_binding_invalid") from None
