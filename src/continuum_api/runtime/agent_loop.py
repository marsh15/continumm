"""Durable scripted model turns and sequential T19 tool activities."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from sqlalchemy import Engine, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from continuum_api.artifacts.checkpoints import CheckpointStorageError, CheckpointStorageService
from continuum_api.credentials import CredentialError, CredentialProvider
from continuum_api.runtime.agent_context import (
    AgentContextError,
    build_agent_context,
    canonicalize_initial_context,
    context_source_sha256,
)
from continuum_api.runtime.leases import (
    ExecutionLease,
    LeaseLost,
    LockedExecution,
    lock_current_execution,
)
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus, RuntimeRepository
from continuum_api.runtime.provider_credentials import CredentialDispatchGrant, CredentialService
from continuum_api.runtime.scheduler import FailureClass, RuntimeRecoveryService
from continuum_api.runtime.transitions import (
    InvalidTransition,
    NextWork,
    TransitionReceipt,
    TransitionService,
)
from continuum_api.runtime.workspace_recovery import (
    WorkspaceRecoveryError,
    WorkspaceRecoveryService,
    WorkspaceSnapshot,
)
from continuum_api.tools.contracts import (
    MAX_TOOL_ARGUMENT_BYTES,
    MAX_TOOL_RESULT_BYTES,
    ApplyPatchResult,
    ToolContractError,
    parse_tool_call,
    parse_tool_result,
)
from continuum_api.tools.policy import ToolPolicyContext
from continuum_api.tools.source_tools import (
    PatchCandidate,
    SourceToolError,
    candidate_from_baseline,
    execute_tool_call,
    rebase_patch_candidate,
    snapshot_from_verified_archive,
)

MAX_AGENT_TURNS_PER_RUN = 30
MAX_AGENT_CONTEXT_BYTES = 65_536
MAX_AGENT_RESPONSE_BYTES = 65_536
MAX_AGENT_TOOL_SCHEMAS = 16
MAX_AGENT_TOOL_CALLS = 10
MAX_AGENT_OUTPUT_TOKENS = 4096
MAX_AGENT_INPUT_TOKENS_PER_RUN = 120_000
MAX_AGENT_OUTPUT_TOKENS_PER_RUN = 30_000
MODEL_INPUT_ENVELOPE_RESERVATION = 1024
_SAFE_PROVIDER_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_SCRIPTED_MODEL_ID = "scripted-v1"
OPENAI_MODEL_ID = "gpt-5.4-mini-2026-03-17"
ANTHROPIC_MODEL_ID = "claude-sonnet-5-5"
_TOOL_NAMES = frozenset({"list_files", "read_file", "search_text", "apply_patch", "diff"})


class AgentLoopError(RuntimeError):
    """Sanitized scripted agent loop failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, revalidate_instances="always"
    )


class ScriptedToolCall(_StrictModel):
    provider_call_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=32)
    arguments_json: str = Field(max_length=MAX_TOOL_ARGUMENT_BYTES)

    @model_validator(mode="after")
    def validate_call_identity(self) -> ScriptedToolCall:
        if _SAFE_PROVIDER_ID.fullmatch(self.provider_call_id) is None:
            raise ValueError("agent_provider_call_id_invalid")
        if self.name not in _TOOL_NAMES:
            raise ValueError("agent_tool_name_invalid")
        return self


class NormalizedModelResponse(_StrictModel):
    complete: bool
    stop_reason: str = Field(min_length=1, max_length=32)
    model_id: str = Field(min_length=1, max_length=64)
    response_id: str | None = Field(default=None, max_length=128)
    provider_request_id: str | None = Field(default=None, max_length=128)
    output_text: str = Field(max_length=MAX_AGENT_RESPONSE_BYTES)
    calls: tuple[ScriptedToolCall, ...] = Field(max_length=MAX_AGENT_TOOL_CALLS)
    input_tokens: int | None = Field(default=None, ge=0, le=120_000)
    output_tokens: int | None = Field(default=None, ge=0, le=MAX_AGENT_OUTPUT_TOKENS)
    cached_input_tokens: int | None = Field(default=None, ge=0, le=120_000)
    cache_creation_input_tokens: int | None = Field(default=None, ge=0, le=120_000)
    reasoning_output_tokens: int | None = Field(default=None, ge=0, le=MAX_AGENT_OUTPUT_TOKENS)
    latency_ms: int | None = Field(default=None, ge=0, le=600_000)
    continuation: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_response_metadata(self) -> NormalizedModelResponse:
        if self.response_id is not None and _SAFE_PROVIDER_ID.fullmatch(self.response_id) is None:
            raise ValueError("agent_response_id_invalid")
        if (
            self.provider_request_id is not None
            and _SAFE_PROVIDER_ID.fullmatch(self.provider_request_id) is None
        ):
            raise ValueError("agent_provider_request_id_invalid")
        if (self.input_tokens is None) != (self.output_tokens is None):
            raise ValueError("agent_usage_incomplete")
        if (
            self.cached_input_tokens is not None
            and self.input_tokens is not None
            and self.cached_input_tokens > self.input_tokens
        ):
            raise ValueError("agent_cached_usage_invalid")
        if (
            self.cache_creation_input_tokens is not None
            and self.input_tokens is not None
            and self.cached_input_tokens is not None
            and self.cached_input_tokens + self.cache_creation_input_tokens > self.input_tokens
        ):
            raise ValueError("agent_cache_creation_usage_invalid")
        if (
            self.reasoning_output_tokens is not None
            and self.output_tokens is not None
            and self.reasoning_output_tokens > self.output_tokens
        ):
            raise ValueError("agent_reasoning_usage_invalid")
        if len({call.provider_call_id for call in self.calls}) != len(self.calls):
            raise ValueError("agent_provider_call_id_duplicate")
        return self


# Kept as a compatibility name for the deterministic T22 adapter and fixtures.
ScriptedModelResponse = NormalizedModelResponse


class ScriptedModelRequest(_StrictModel):
    turn_id: UUID
    model_id: str = Field(min_length=1, max_length=64)
    context: dict[str, Any]
    tool_schemas: tuple[dict[str, Any], ...] = Field(max_length=MAX_AGENT_TOOL_SCHEMAS)
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    max_output_tokens: int = Field(
        default=MAX_AGENT_OUTPUT_TOKENS, ge=1, le=MAX_AGENT_OUTPUT_TOKENS
    )


class ScriptedModelAdapter(Protocol):
    async def complete(self, request: ScriptedModelRequest) -> ScriptedModelResponse: ...


class CredentialedModelAdapter(Protocol):
    async def complete_with_grant(
        self, request: ScriptedModelRequest, grant: CredentialDispatchGrant
    ) -> ScriptedModelResponse: ...


@dataclass(frozen=True, slots=True)
class ModelBudgetSnapshot:
    run_id: UUID
    model_id: str | None
    requests_reserved: int
    requests_authorized: int
    requests_received: int
    requests_ambiguous: int
    requests_in_flight: int
    requests_remaining: int
    input_tokens_used: int
    input_tokens_reserved: int
    input_tokens_remaining: int
    output_tokens_used: int
    output_tokens_reserved: int
    output_tokens_remaining: int
    usage_unknown_requests: int
    exceeded: bool

    @property
    def usage_uncertain(self) -> bool:
        return self.usage_unknown_requests > 0


class ScriptedResponseQueue:
    """A deterministic in-process adapter used by local protocol tests."""

    def __init__(self, responses: Iterable[ScriptedModelResponse]) -> None:
        self._responses = tuple(responses)
        self.requests: list[ScriptedModelRequest] = []

    @property
    def call_count(self) -> int:
        return len(self.requests)

    async def complete(self, request: ScriptedModelRequest) -> ScriptedModelResponse:
        index = len(self.requests)
        self.requests.append(request)
        if index >= len(self._responses):
            raise AgentLoopError("agent_script_exhausted")
        return self._responses[index]


@dataclass(frozen=True, slots=True)
class AgentTurnIntent:
    turn_id: UUID
    owner_id: UUID
    run_id: UUID
    job_id: UUID
    logical_key: str
    model_id: str
    request_sha256: str
    baseline_artifact_id: UUID
    status: str


@dataclass(frozen=True, slots=True)
class AgentInvocation:
    invocation_id: UUID
    turn_id: UUID
    ordinal: int
    provider_call_id: str
    name: str
    logical_key: str
    arguments: dict[str, Any]
    accepted: bool
    result: dict[str, Any] | None
    artifact_id: UUID | None


@dataclass(frozen=True, slots=True)
class AgentTurnReceipt:
    turn_id: UUID
    request_sha256: str
    response_sha256: str
    response: ScriptedModelResponse
    invocations: tuple[AgentInvocation, ...]
    adopted: bool


@dataclass(frozen=True, slots=True)
class AgentToolReceipt:
    invocation_id: UUID
    turn_id: UUID
    ordinal: int
    transition: TransitionReceipt
    artifact_id: UUID | None
    adopted: bool


class DurableAgentLoopService:
    """Persist intent before inference; advance one accepted tool call per job."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        transitions: TransitionService,
        *,
        workspace_recovery: WorkspaceRecoveryService | None = None,
        checkpoints: CheckpointStorageService | None = None,
        credential_service: CredentialService | None = None,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._transitions = transitions
        self._workspace_recovery = workspace_recovery
        self._checkpoints = checkpoints
        self._credential_service = credential_service
        self._recovery = RuntimeRecoveryService(engine, principal)

    def read_model_budget(self, run_id: UUID) -> ModelBudgetSnapshot:
        """Read the owner's current model reservation, usage and uncertainty."""
        if not isinstance(run_id, UUID):
            raise AgentLoopError("agent_run_id_invalid")
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                row = (
                    connection.execute(
                        text(
                            "SELECT model_id, model_requests_reserved, model_requests_authorized, "
                            "model_requests_received, model_requests_ambiguous, "
                            "model_input_tokens_used, "
                            "model_input_tokens_reserved, model_output_tokens_used, "
                            "model_output_tokens_reserved, model_usage_unknown_requests, "
                            "model_budget_exceeded FROM runtime_private.runs "
                            "WHERE owner_id=:owner AND run_id=:run"
                        ),
                        {"owner": self._principal.account_id, "run": run_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise AgentLoopError("agent_run_not_found")
                return _model_budget_snapshot(run_id, dict(row))
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    async def prepare_turn(
        self,
        lease: ExecutionLease,
        logical_key: str,
        *,
        context: dict[str, Any],
        tool_schemas: Iterable[dict[str, Any]],
        baseline_artifact_id: UUID,
        model_id: str = _SCRIPTED_MODEL_ID,
        output_tokens_reserved: int = MAX_AGENT_OUTPUT_TOKENS,
    ) -> AgentTurnIntent:
        self._check_owner(lease)
        schemas = tuple(tool_schemas)
        if model_id not in {_SCRIPTED_MODEL_ID, OPENAI_MODEL_ID, ANTHROPIC_MODEL_ID}:
            raise AgentLoopError("agent_model_unsupported")
        if (
            not logical_key
            or len(logical_key.encode("utf-8")) > 128
            or logical_key.strip() != logical_key
        ):
            raise AgentLoopError("agent_turn_key_invalid")
        if len(schemas) > MAX_AGENT_TOOL_SCHEMAS:
            raise AgentLoopError("agent_tool_schema_limit")
        if self._checkpoints is None:
            raise AgentLoopError("agent_checkpoint_storage_unavailable")
        root_artifact_id = await self._root_artifact_id(lease)
        if root_artifact_id is None:
            await self._fail_invalid_attempt(lease)
            raise AgentLoopError("agent_baseline_artifact_missing")
        if root_artifact_id != baseline_artifact_id:
            await self._fail_invalid_attempt(lease)
            raise AgentLoopError("agent_baseline_artifact_mismatch")
        try:
            await self._checkpoints.restore_archive(self._principal, lease, root_artifact_id)
        except CheckpointStorageError as error:
            raise AgentLoopError(error.code) from None
        if not isinstance(context, dict) or any(not isinstance(schema, dict) for schema in schemas):
            raise AgentLoopError("agent_context_invalid")
        if (
            type(output_tokens_reserved) is not int
            or not 1 <= output_tokens_reserved <= MAX_AGENT_OUTPUT_TOKENS
        ):
            raise AgentLoopError("agent_output_reservation_invalid")
        try:
            initial_context = canonicalize_initial_context(context)
        except AgentContextError as error:
            raise AgentLoopError(error.code) from None
        tool_schemas = list(schemas)
        schema_serialized = _canonical_json(tool_schemas, "agent_tool_schema_invalid")
        tool_schema_sha256 = hashlib.sha256(schema_serialized.encode("utf-8")).digest()
        source_request_payload = {
            "baseline_artifact_id": str(baseline_artifact_id),
            "initial_context": initial_context,
            "logical_key": logical_key,
            "model_id": model_id,
            "output_tokens_reserved": output_tokens_reserved,
            "tool_schema_sha256": tool_schema_sha256.hex(),
        }
        source_request_digest = hashlib.sha256(
            _canonical_json(source_request_payload, "agent_request_invalid").encode("utf-8")
        ).digest()
        try:
            row = await asyncio.to_thread(
                self._prepare_turn_once,
                lease,
                logical_key,
                baseline_artifact_id,
                model_id,
                source_request_digest,
                initial_context,
                tool_schemas,
                tool_schema_sha256,
                output_tokens_reserved,
            )
        except LeaseLost:
            raise AgentLoopError("agent_lease_lost") from None
        except IntegrityError:
            raise AgentLoopError("agent_turn_identity_conflict") from None
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None
        return _intent_from_row(row)

    def _prepare_turn_once(
        self,
        lease: ExecutionLease,
        logical_key: str,
        baseline_artifact_id: UUID,
        model_id: str,
        source_request_digest: bytes,
        initial_context: dict[str, Any],
        tool_schemas: list[dict[str, Any]],
        tool_schema_sha256: bytes,
        output_tokens_reserved: int,
    ) -> dict[str, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            locked = lock_current_execution(connection, self._principal, lease)
            replay_policy: str = connection.execute(
                text(
                    "SELECT replay_policy FROM runtime_private.jobs "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id"
                ),
                {"owner_id": lease.owner_id, "run_id": lease.run_id, "job_id": lease.job_id},
            ).scalar_one()
            if replay_policy != ReplayPolicy.REPLAY_SAFE.value:
                raise AgentLoopError("agent_turn_replay_policy_required")
            step_row = (
                connection.execute(
                    text(
                        "SELECT activity, revision FROM runtime_private.steps "
                        "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
                    ),
                    {
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                        "step_id": lease.step_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if step_row is None or step_row["activity"] != "agent_turn":
                raise AgentLoopError("agent_turn_activity_required")
            revision_feedback = _load_revision_feedback(
                connection, lease.owner_id, lease.run_id, step_row["revision"]
            )
            root_artifact_id = connection.execute(
                text(
                    "SELECT artifact_id FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND artifact_id IS NOT NULL "
                    "ORDER BY event_sequence ASC LIMIT 1"
                ),
                {"owner_id": lease.owner_id, "run_id": lease.run_id},
            ).scalar_one_or_none()
            if root_artifact_id is None or root_artifact_id != baseline_artifact_id:
                raise AgentLoopError("agent_baseline_artifact_mismatch")
            run_row = (
                connection.execute(
                    text(
                        "SELECT model_id, model_task_context, model_tool_schema_sha256, "
                        "model_requests_reserved, model_input_tokens_used, "
                        "model_input_tokens_reserved, model_output_tokens_used, "
                        "model_output_tokens_reserved, model_requests_ambiguous, "
                        "model_usage_unknown_requests, model_budget_exceeded "
                        "FROM runtime_private.runs "
                        "WHERE owner_id=:owner_id AND run_id=:run_id FOR UPDATE"
                    ),
                    {"owner_id": lease.owner_id, "run_id": lease.run_id},
                )
                .mappings()
                .one_or_none()
            )
            if run_row is None:
                raise AgentLoopError("agent_run_not_found")
            existing = (
                connection.execute(
                    text(
                        "SELECT * FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id "
                        "AND logical_key=:logical_key FOR UPDATE"
                    ),
                    {
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                        "job_id": lease.job_id,
                        "logical_key": logical_key,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                row = dict(existing)
                if row["source_sha256"] is None or row["source_request_sha256"] is None:
                    raise AgentLoopError("agent_context_provenance_missing")
                if row["source_request_sha256"] != source_request_digest:
                    raise AgentLoopError("agent_turn_identity_conflict")
                stored_initial = _initial_context_from_persisted(row["context"])
                stored_schema_digest = hashlib.sha256(
                    _canonical_json(row["tool_schemas"], "agent_tool_schema_invalid").encode(
                        "utf-8"
                    )
                ).digest()
                if (
                    stored_initial != initial_context
                    or stored_schema_digest != tool_schema_sha256
                    or row["baseline_artifact_id"] != baseline_artifact_id
                    or row["model_id"] != model_id
                    or row["output_tokens_reserved"] != output_tokens_reserved
                ):
                    raise AgentLoopError("agent_turn_identity_conflict")
                pinned_model = run_row["model_id"]
                if pinned_model is not None and pinned_model != row["model_id"]:
                    raise AgentLoopError("agent_model_switch_forbidden")
                if run_row["model_task_context"] is not None and (
                    canonicalize_initial_context(run_row["model_task_context"]) != initial_context
                ):
                    raise AgentLoopError("agent_task_context_pinned")
                if run_row["model_tool_schema_sha256"] is not None and (
                    run_row["model_tool_schema_sha256"] != tool_schema_sha256
                ):
                    raise AgentLoopError("agent_tool_schema_pinned")
                prior_turns, prior_turn_ids, prior_invocation_ids = _load_accepted_context_history(
                    connection,
                    owner_id=lease.owner_id,
                    run_id=lease.run_id,
                    model_id=row["model_id"],
                    initial_context=initial_context,
                    before=row["created_at"],
                )
                expected_source = context_source_sha256(
                    initial_context=initial_context,
                    accepted_turn_ids=prior_turn_ids,
                    accepted_invocation_ids=prior_invocation_ids,
                    revision_feedback=revision_feedback,
                )
                if row["source_sha256"].hex() != expected_source:
                    raise AgentLoopError("agent_context_provenance_missing")
                if _persisted_request_digest(row) != row["request_sha256"]:
                    raise AgentLoopError("agent_persisted_request_corrupt")
                if row["status"] == "prepared":
                    try:
                        expected_context = build_agent_context(
                            initial_context=initial_context,
                            accepted_turns=prior_turns,
                            model_id=row["model_id"],
                            budget=row["context"]["budget"],
                            revision_feedback=revision_feedback,
                        )
                    except (AgentContextError, KeyError, TypeError):
                        raise AgentLoopError("agent_context_provenance_missing") from None
                    if expected_context != row["context"]:
                        raise AgentLoopError("agent_context_provenance_missing")
                connection.execute(
                    text(
                        "UPDATE runtime_private.runs SET model_id=COALESCE(model_id,:model), "
                        "model_task_context=COALESCE(model_task_context,CAST(:context AS jsonb)), "
                        "model_tool_schema_sha256=COALESCE(model_tool_schema_sha256,:schema_hash) "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {
                        "model": row["model_id"],
                        "context": _canonical_json(initial_context, "agent_context_invalid"),
                        "schema_hash": tool_schema_sha256,
                        "owner": lease.owner_id,
                        "run": lease.run_id,
                    },
                )
                return row

            if bool(run_row.get("model_budget_exceeded", False)):
                raise AgentLoopError("agent_model_budget_exceeded")
            if run_row["model_task_context"] is not None and (
                canonicalize_initial_context(run_row["model_task_context"]) != initial_context
            ):
                raise AgentLoopError("agent_task_context_pinned")
            if run_row["model_tool_schema_sha256"] is not None and (
                run_row["model_tool_schema_sha256"] != tool_schema_sha256
            ):
                raise AgentLoopError("agent_tool_schema_pinned")
            if run_row["model_id"] is not None and run_row["model_id"] != model_id:
                raise AgentLoopError("agent_model_switch_forbidden")

            run_baseline = connection.execute(
                text(
                    "SELECT baseline_artifact_id FROM runtime_private.agent_turns "
                    "WHERE owner_id=:owner_id AND run_id=:run_id "
                    "ORDER BY created_at LIMIT 1"
                ),
                {"owner_id": lease.owner_id, "run_id": lease.run_id},
            ).scalar_one_or_none()
            if run_baseline is not None and run_baseline != baseline_artifact_id:
                raise AgentLoopError("agent_baseline_artifact_mismatch")

            reserved_count: int = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.agent_turns "
                    "WHERE owner_id=:owner_id AND run_id=:run_id"
                ),
                {"owner_id": lease.owner_id, "run_id": lease.run_id},
            ).scalar_one()
            if reserved_count >= MAX_AGENT_TURNS_PER_RUN:
                raise AgentLoopError("agent_request_limit")
            pinned_context = run_row["model_task_context"]
            pinned_schema_sha = run_row["model_tool_schema_sha256"]
            if pinned_context is None or pinned_schema_sha is None:
                first_turn = (
                    connection.execute(
                        text(
                            "SELECT context, tool_schemas FROM runtime_private.agent_turns "
                            "WHERE owner_id=:owner AND run_id=:run "
                            "ORDER BY created_at, turn_id LIMIT 1"
                        ),
                        {"owner": lease.owner_id, "run": lease.run_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if first_turn is None:
                    pinned_context = initial_context
                    pinned_schema_sha = tool_schema_sha256
                else:
                    try:
                        legacy_context = _initial_context_from_persisted(first_turn["context"])
                    except AgentLoopError:
                        raise AgentLoopError("agent_context_provenance_missing") from None
                    legacy_schema_sha = hashlib.sha256(
                        _canonical_json(
                            first_turn["tool_schemas"], "agent_tool_schema_invalid"
                        ).encode("utf-8")
                    ).digest()
                    if legacy_context != initial_context:
                        raise AgentLoopError("agent_task_context_pinned")
                    if legacy_schema_sha != tool_schema_sha256:
                        raise AgentLoopError("agent_tool_schema_pinned")
                    pinned_context = legacy_context
                    pinned_schema_sha = legacy_schema_sha
            if canonicalize_initial_context(pinned_context) != initial_context:
                raise AgentLoopError("agent_task_context_pinned")
            if pinned_schema_sha != tool_schema_sha256:
                raise AgentLoopError("agent_tool_schema_pinned")

            accepted_turns, accepted_turn_ids, accepted_invocation_ids = (
                _load_accepted_context_history(
                    connection,
                    owner_id=lease.owner_id,
                    run_id=lease.run_id,
                    model_id=model_id,
                    initial_context=initial_context,
                )
            )

            usage_uncertain = (
                run_row["model_usage_unknown_requests"] > 0
                or run_row["model_requests_ambiguous"] > 0
            )
            budget_context = {
                "requests_remaining": max(
                    0, MAX_AGENT_TURNS_PER_RUN - run_row["model_requests_reserved"]
                ),
                "input_tokens_remaining": max(
                    0,
                    MAX_AGENT_INPUT_TOKENS_PER_RUN
                    - run_row["model_input_tokens_used"]
                    - run_row["model_input_tokens_reserved"],
                ),
                "output_tokens_remaining": max(
                    0,
                    MAX_AGENT_OUTPUT_TOKENS_PER_RUN
                    - run_row["model_output_tokens_used"]
                    - run_row["model_output_tokens_reserved"],
                ),
                "usage_uncertain": usage_uncertain,
            }
            try:
                context = build_agent_context(
                    initial_context=initial_context,
                    accepted_turns=accepted_turns,
                    model_id=model_id,
                    budget=budget_context,
                    revision_feedback=revision_feedback,
                )
            except AgentContextError as error:
                raise AgentLoopError(error.code) from None
            source_digest = bytes.fromhex(
                context_source_sha256(
                    initial_context=initial_context,
                    accepted_turn_ids=accepted_turn_ids,
                    accepted_invocation_ids=accepted_invocation_ids,
                    revision_feedback=revision_feedback,
                )
            )
            request_payload = {
                "baseline_artifact_id": str(baseline_artifact_id),
                "context": context,
                "logical_key": logical_key,
                "model_id": model_id,
                "output_tokens_reserved": output_tokens_reserved,
                "tool_schemas": tool_schemas,
            }
            serialized = _canonical_json(request_payload, "agent_request_invalid")
            request_bytes = serialized.encode("utf-8")
            if len(request_bytes) > MAX_AGENT_CONTEXT_BYTES:
                raise AgentLoopError("agent_context_size_limit")
            input_bytes_reserved = len(request_bytes)
            input_tokens_reserved = _estimate_input_tokens(request_bytes)
            request_digest = hashlib.sha256(request_bytes).digest()
            requests_reserved: int = run_row["model_requests_reserved"]
            input_used: int = run_row["model_input_tokens_used"]
            input_reserved: int = run_row["model_input_tokens_reserved"]
            output_used: int = run_row["model_output_tokens_used"]
            output_reserved: int = run_row["model_output_tokens_reserved"]
            if requests_reserved >= MAX_AGENT_TURNS_PER_RUN:
                raise AgentLoopError("agent_request_limit")
            if input_used + input_reserved + input_tokens_reserved > MAX_AGENT_INPUT_TOKENS_PER_RUN:
                raise AgentLoopError("agent_input_budget_exceeded")
            if (
                output_used + output_reserved + output_tokens_reserved
                > MAX_AGENT_OUTPUT_TOKENS_PER_RUN
            ):
                raise AgentLoopError("agent_output_budget_exceeded")
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET model_id=COALESCE(model_id,:model), "
                    "model_task_context=COALESCE(model_task_context,CAST(:task_context AS jsonb)), "
                    "model_tool_schema_sha256=COALESCE(model_tool_schema_sha256,:schema_hash), "
                    "model_requests_reserved=model_requests_reserved+1, "
                    "model_input_tokens_reserved=model_input_tokens_reserved+:input_tokens, "
                    "model_output_tokens_reserved=model_output_tokens_reserved+:output_tokens "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {
                    "model": model_id,
                    "task_context": _canonical_json(
                        pinned_context, "agent_context_initial_invalid"
                    ),
                    "schema_hash": pinned_schema_sha,
                    "input_tokens": input_tokens_reserved,
                    "output_tokens": output_tokens_reserved,
                    "owner": lease.owner_id,
                    "run": lease.run_id,
                },
            )
            turn_id = uuid4()
            inserted = (
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.agent_turns ("
                        "turn_id, owner_id, run_id, step_id, job_id, logical_key, generation, "
                        "attempt_id, lease_owner, baseline_artifact_id, model_id, request_sha256, "
                        "source_sha256, source_request_sha256, context, tool_schemas, "
                        "input_bytes_reserved, "
                        "input_tokens_reserved, output_tokens_reserved) "
                        "VALUES (:turn_id, :owner_id, :run_id, :step_id, :job_id, :logical_key, "
                        ":generation, :attempt_id, :lease_owner, :baseline_artifact_id, :model_id, "
                        ":request_sha256, :source_sha256, :source_request_sha256, "
                        "CAST(:context AS jsonb), "
                        "CAST(:tool_schemas AS jsonb), :input_bytes_reserved, "
                        ":input_tokens_reserved, :output_tokens_reserved) RETURNING *"
                    ),
                    {
                        "turn_id": turn_id,
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                        "step_id": lease.step_id,
                        "job_id": lease.job_id,
                        "logical_key": logical_key,
                        "generation": lease.generation,
                        "attempt_id": lease.attempt_id,
                        "lease_owner": lease.lease_owner,
                        "baseline_artifact_id": baseline_artifact_id,
                        "model_id": model_id,
                        "request_sha256": request_digest,
                        "source_sha256": source_digest,
                        "source_request_sha256": source_request_digest,
                        "context": _canonical_json(context, "agent_context_invalid"),
                        "tool_schemas": _canonical_json(tool_schemas, "agent_tool_schema_invalid"),
                        "input_bytes_reserved": input_bytes_reserved,
                        "input_tokens_reserved": input_tokens_reserved,
                        "output_tokens_reserved": output_tokens_reserved,
                    },
                )
                .mappings()
                .one()
            )
            _assert_execution_live(connection, locked)
            return dict(inserted)

    async def infer_or_reuse(
        self,
        lease: ExecutionLease,
        turn_id: UUID,
        adapter: ScriptedModelAdapter,
    ) -> AgentTurnReceipt:
        self._check_owner(lease)
        row = await self._get_turn(lease, turn_id, require_turn_job=True)
        if row["status"] == "response_accepted":
            return await self._turn_receipt(lease, row, adopted=True)
        if row["status"] == "invalid_response":
            await self._fail_invalid_attempt(lease)
            raise AgentLoopError(row["response_error_code"] or "agent_response_invalid")
        if row["status"] == "unknown":
            await self._record_unknown_failure(lease)
            raise AgentLoopError("agent_response_outcome_unknown")
        if row["status"] == "dispatching":
            await self._mark_response_unknown(lease, turn_id)
            raise AgentLoopError("agent_response_outcome_unknown")
        if row["status"] != "prepared":
            raise AgentLoopError("agent_turn_state_invalid")
        if self._checkpoints is None:
            raise AgentLoopError("agent_checkpoint_storage_unavailable")
        try:
            await asyncio.to_thread(self._verify_prepared_context_once, lease, row)
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None
        try:
            await self._checkpoints.restore_archive(
                self._principal, lease, row["baseline_artifact_id"]
            )
        except CheckpointStorageError as error:
            if error.code in {"repository_access_denied", "repository_metadata_unavailable"}:
                await self._fail_invalid_attempt(lease)
            raise AgentLoopError(error.code) from None

        request = ScriptedModelRequest(
            turn_id=turn_id,
            model_id=row["model_id"],
            context=row["context"],
            tool_schemas=tuple(row["tool_schemas"]),
            request_sha256=row["request_sha256"].hex(),
            max_output_tokens=row["output_tokens_reserved"],
        )
        started_at = time.monotonic()
        credential_grant: CredentialDispatchGrant | None = None
        complete_with_grant = None
        try:
            if row["model_id"] != _SCRIPTED_MODEL_ID:
                if self._credential_service is None:
                    raise AgentLoopError("credential_service_unavailable")
                complete_with_grant = getattr(adapter, "complete_with_grant", None)
                if not callable(complete_with_grant):
                    raise AgentLoopError("agent_credentialed_adapter_required")
                validate_request = getattr(adapter, "validate_request", None)
                if callable(validate_request):
                    validate_request(request)
                provider = (
                    CredentialProvider.OPENAI
                    if row["model_id"] == OPENAI_MODEL_ID
                    else CredentialProvider.ANTHROPIC
                )
                credential_grant = await asyncio.to_thread(
                    self._credential_service.authorize_dispatch,
                    lease,
                    provider,
                    turn_id,
                )
            row = await self._authorize_model_dispatch(lease, turn_id)
            if row["model_id"] == _SCRIPTED_MODEL_ID:
                response_value = await adapter.complete(request)
            else:
                assert complete_with_grant is not None and credential_grant is not None
                response_value = await complete_with_grant(request, credential_grant)
        except asyncio.CancelledError:
            await self._mark_response_unknown(lease, turn_id)
            raise
        except CredentialError as error:
            if error.code == "credential_dispatch_already_authorized":
                await self._authorize_model_dispatch(lease, turn_id)
                await self._mark_response_unknown(lease, turn_id)
            raise AgentLoopError(error.code) from None
        except AgentLoopError:
            await self._mark_response_unknown(lease, turn_id)
            raise
        except Exception as error:
            safe_code = getattr(error, "code", "agent_response_outcome_unknown")
            if (
                not isinstance(safe_code, str)
                or re.fullmatch(r"[a-z0-9_]{1,96}", safe_code) is None
            ):
                safe_code = "agent_response_outcome_unknown"
            if getattr(error, "outcome_unknown", True) is False:
                await self._persist_invalid_response(
                    lease, turn_id, {"protocol_error": safe_code}, safe_code
                )
                await self._fail_invalid_attempt(lease)
                raise AgentLoopError(safe_code) from None
            await self._mark_response_unknown(lease, turn_id)
            raise AgentLoopError(safe_code) from None

        response: ScriptedModelResponse | None = None
        try:
            response = ScriptedModelResponse.model_validate(response_value, strict=True)
            if response.latency_ms is None:
                response = response.model_copy(
                    update={
                        "latency_ms": min(
                            600_000, max(0, round((time.monotonic() - started_at) * 1000))
                        )
                    }
                )
            if (
                response.input_tokens is not None
                and response.input_tokens > row["input_tokens_reserved"]
            ):
                raise AgentLoopError("agent_input_reservation_exceeded")
            invocation_values = _validate_complete_response(
                response,
                row["model_id"],
                turn_id,
                row["output_tokens_reserved"],
            )
            serialized_response = _canonical_json(
                response.model_dump(mode="json", warnings=False), "agent_response_invalid"
            )
            if len(serialized_response.encode("utf-8")) > MAX_AGENT_RESPONSE_BYTES:
                raise AgentLoopError("agent_response_size_limit")
        except (ValidationError, AgentLoopError) as error:
            error_code = (
                error.code if isinstance(error, AgentLoopError) else "agent_response_invalid"
            )
            safe_response = {"protocol_error": error_code}
            if response is not None and response.stop_reason == "refusal":
                safe_response["output_text"] = response.output_text[:4096]
            await self._persist_invalid_response(
                lease, turn_id, safe_response, error_code, response=response
            )
            await self._fail_invalid_attempt(lease)
            raise AgentLoopError(error_code) from None
        receipt_row = await self._accept_response(lease, turn_id, response, invocation_values)
        return await self._turn_receipt(lease, receipt_row, adopted=False)

    def _verify_prepared_context_once(self, lease: ExecutionLease, turn: dict[str, Any]) -> None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            row = (
                connection.execute(
                    text(
                        "SELECT * FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner AND run_id=:run AND turn_id=:turn FOR UPDATE"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id, "turn": turn["turn_id"]},
                )
                .mappings()
                .one_or_none()
            )
            if row is None or row["status"] != "prepared":
                raise AgentLoopError("agent_turn_state_invalid")
            persisted = dict(row)
            if persisted["source_sha256"] is None or persisted["source_request_sha256"] is None:
                raise AgentLoopError("agent_context_provenance_missing")
            initial_context = _initial_context_from_persisted(persisted["context"])
            schemas = persisted["tool_schemas"]
            schema_digest = hashlib.sha256(
                _canonical_json(schemas, "agent_tool_schema_invalid").encode("utf-8")
            ).digest()
            run = (
                connection.execute(
                    text(
                        "SELECT model_id, model_task_context, model_tool_schema_sha256 "
                        "FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one_or_none()
            )
            if run is None:
                raise AgentLoopError("agent_run_not_found")
            if (
                run["model_id"] != persisted["model_id"]
                or canonicalize_initial_context(run["model_task_context"]) != initial_context
                or run["model_tool_schema_sha256"] != schema_digest
            ):
                raise AgentLoopError("agent_context_provenance_missing")
            step_revision = connection.execute(
                text(
                    "SELECT revision FROM runtime_private.steps WHERE owner_id=:owner "
                    "AND run_id=:run AND step_id=:step"
                ),
                {"owner": lease.owner_id, "run": lease.run_id, "step": persisted["step_id"]},
            ).scalar_one_or_none()
            if step_revision is None:
                raise AgentLoopError("agent_context_provenance_missing")
            revision_feedback = _load_revision_feedback(
                connection, lease.owner_id, lease.run_id, step_revision
            )
            expected_source_request = _source_request_digest(
                baseline_artifact_id=persisted["baseline_artifact_id"],
                logical_key=persisted["logical_key"],
                model_id=persisted["model_id"],
                initial_context=initial_context,
                tool_schema_sha256=schema_digest,
                output_tokens_reserved=persisted["output_tokens_reserved"],
            )
            if persisted["source_request_sha256"] != expected_source_request:
                raise AgentLoopError("agent_persisted_request_corrupt")
            if _persisted_request_digest(persisted) != persisted["request_sha256"]:
                raise AgentLoopError("agent_persisted_request_corrupt")
            prior_turns, prior_turn_ids, prior_invocation_ids = _load_accepted_context_history(
                connection,
                owner_id=lease.owner_id,
                run_id=lease.run_id,
                model_id=persisted["model_id"],
                initial_context=initial_context,
                before=persisted["created_at"],
            )
            expected_source = context_source_sha256(
                initial_context=initial_context,
                accepted_turn_ids=prior_turn_ids,
                accepted_invocation_ids=prior_invocation_ids,
                revision_feedback=revision_feedback,
            )
            if persisted["source_sha256"].hex() != expected_source:
                raise AgentLoopError("agent_context_provenance_missing")
            budget = persisted["context"].get("budget")
            try:
                rebuilt = build_agent_context(
                    initial_context=initial_context,
                    accepted_turns=prior_turns,
                    model_id=persisted["model_id"],
                    budget=budget,
                    revision_feedback=revision_feedback,
                )
            except (AgentContextError, KeyError, TypeError):
                raise AgentLoopError("agent_context_provenance_missing") from None
            if rebuilt != persisted["context"]:
                raise AgentLoopError("agent_context_provenance_missing")
            serialized = _canonical_json(
                {
                    "baseline_artifact_id": str(persisted["baseline_artifact_id"]),
                    "context": persisted["context"],
                    "logical_key": persisted["logical_key"],
                    "model_id": persisted["model_id"],
                    "output_tokens_reserved": persisted["output_tokens_reserved"],
                    "tool_schemas": schemas,
                },
                "agent_persisted_request_corrupt",
            ).encode("utf-8")
            if persisted["input_bytes_reserved"] != len(serialized) or persisted[
                "input_tokens_reserved"
            ] != _estimate_input_tokens(serialized):
                raise AgentLoopError("agent_persisted_request_corrupt")

    async def pending_invocations(
        self, lease: ExecutionLease, turn_id: UUID
    ) -> tuple[AgentInvocation, ...]:
        self._check_owner(lease)
        _turn, rows = await self._verified_invocation_rows(lease, turn_id)
        return tuple(_invocation_from_row(row) for row in rows)

    async def complete_turn(
        self,
        lease: ExecutionLease,
        turn_id: UUID,
        *,
        next_work: NextWork | None = None,
        target_run_status: RunStatus = RunStatus.RUNNING,
    ) -> TransitionReceipt:
        """Accept the persisted response and enqueue its first sequential tool job."""
        self._check_owner(lease)
        existing = await self._accepted_transition(lease)
        if existing is not None:
            row = await self._read_turn(lease, turn_id)
            invocation_records = await self._read_invocations(lease, turn_id)
            await self._verify_turn_invocation_rows(lease, row, invocation_records)
            expected_result = {
                "agent_turn_id": str(turn_id),
                "response_sha256": row["response_sha256"].hex(),
                "invocation_ids": [str(item["invocation_id"]) for item in invocation_records],
            }
            expected_next = next_work
            current_revision = await self._read_step_revision(lease, row["step_id"])
            if invocation_records:
                generated_next = NextWork(
                    logical_key=invocation_records[0]["logical_key"],
                    activity="agent_tool",
                    revision=current_revision,
                    replay_policy=ReplayPolicy.REPLAY_SAFE,
                )
                if next_work is not None and next_work != generated_next:
                    raise AgentLoopError("agent_turn_next_work_mismatch")
                expected_next = generated_next
            if (
                existing.result != expected_result
                or existing.run_status is not target_run_status
                or not await self._accepted_next_work_matches(lease, existing, expected_next)
            ):
                raise AgentLoopError("agent_turn_transition_conflict")
            return existing
        row = await self._get_turn(lease, turn_id, require_turn_job=True)
        if row["status"] != "response_accepted":
            raise AgentLoopError("agent_turn_response_not_accepted")
        invocation_receipts = await self.pending_invocations(lease, turn_id)
        current_revision = await self._read_step_revision(lease, lease.step_id)
        if invocation_receipts:
            first = invocation_receipts[0]
            generated_next = NextWork(
                logical_key=first.logical_key,
                activity="agent_tool",
                revision=current_revision,
                replay_policy=ReplayPolicy.REPLAY_SAFE,
            )
            if next_work is not None and next_work != generated_next:
                raise AgentLoopError("agent_turn_next_work_mismatch")
            next_work = generated_next
            if target_run_status is not RunStatus.RUNNING:
                raise AgentLoopError("agent_turn_next_work_status_invalid")
        elif next_work is None:
            raise AgentLoopError("agent_turn_next_work_required")
        result = {
            "agent_turn_id": str(turn_id),
            "response_sha256": row["response_sha256"].hex(),
            "invocation_ids": [str(item.invocation_id) for item in invocation_receipts],
        }
        try:
            return await asyncio.to_thread(
                self._transitions.commit_result,
                lease=lease,
                result=result,
                target_run_status=target_run_status,
                next_work=next_work,
            )
        except (LeaseLost, InvalidTransition):
            raise AgentLoopError("agent_turn_transition_rejected") from None

    async def execute_tool_invocation(
        self,
        lease: ExecutionLease,
        turn_id: UUID,
        invocation_id: UUID,
        *,
        policy_context: ToolPolicyContext,
    ) -> AgentToolReceipt:
        """Execute stored arguments through T19, then accept through T08/T21."""
        self._check_owner(lease)
        if not isinstance(policy_context, ToolPolicyContext):
            await self._fail_invalid_attempt(lease)
            raise AgentLoopError("agent_tool_policy_context_invalid")
        existing = await self._accepted_transition(lease)
        if existing is not None:
            return await self._adopt_tool_receipt(lease, turn_id, invocation_id, existing)

        _turn, invocation_rows = await self._verified_invocation_rows(lease, turn_id)
        selected = next(
            (row for row in invocation_rows if row["invocation_id"] == invocation_id), None
        )
        if selected is None:
            raise AgentLoopError("agent_invocation_not_found")
        current_key, current_activity = await self._current_step_info(lease)
        if current_activity != "agent_tool" or current_key != selected["logical_key"]:
            raise AgentLoopError("agent_invocation_not_current")
        for earlier in invocation_rows:
            if earlier["ordinal"] >= selected["ordinal"]:
                break
            if earlier["accepted_payload"] is None:
                raise AgentLoopError("agent_invocation_out_of_order")
        if self._workspace_recovery is None or self._checkpoints is None:
            raise AgentLoopError("agent_workspace_recovery_unavailable")

        turn = await self._get_turn(lease, turn_id, require_turn_job=False)
        try:
            verified_baseline = await self._checkpoints.restore_archive(
                self._principal, lease, turn["baseline_artifact_id"]
            )
            baseline_source = await asyncio.to_thread(
                snapshot_from_verified_archive,
                verified_baseline,
                expected_archive_sha256=verified_baseline.archive_sha256,
                expected_tree_sha256=verified_baseline.manifest.tree_sha256,
            )
            workspace = await self._workspace_recovery.restore_latest(self._principal, lease)
            current_candidate = None
            if workspace.source.tree_sha256 != baseline_source.tree_sha256:
                current_candidate = await asyncio.to_thread(
                    candidate_from_baseline, baseline_source, workspace.source
                )
            execution = await asyncio.to_thread(
                execute_tool_call,
                selected["arguments"],
                policy_context,
                baseline_source,
                current_candidate=current_candidate,
            )
        except WorkspaceRecoveryError as error:
            raise AgentLoopError(error.code) from None
        except CheckpointStorageError as error:
            raise AgentLoopError(error.code) from None
        except SourceToolError as error:
            await self._fail_invalid_attempt(lease)
            raise AgentLoopError(error.code) from None
        except AgentLoopError:
            raise
        except Exception:
            await self._fail_invalid_attempt(lease)
            raise AgentLoopError("agent_tool_execution_failed") from None

        candidate = execution.candidate if selected["tool_name"] == "apply_patch" else None
        return await self._accept_tool_result(
            lease,
            turn_id,
            invocation_id,
            execution.result,
            workspace=workspace,
            candidate=candidate,
            checkpoint_key=f"agent-tool-{invocation_id.hex}" if candidate is not None else None,
        )

    async def _accept_tool_result(
        self,
        lease: ExecutionLease,
        turn_id: UUID,
        invocation_id: UUID,
        result: object,
        *,
        workspace: WorkspaceSnapshot | None = None,
        candidate: PatchCandidate | None = None,
        checkpoint_key: str | None = None,
        next_work: NextWork | None = None,
    ) -> AgentToolReceipt:
        """Accept one ordered tool result; candidates also commit their T15 artifact."""
        self._check_owner(lease)
        existing = await self._accepted_transition(lease)
        if existing is not None:
            return await self._adopt_tool_receipt(lease, turn_id, invocation_id, existing)

        turn, invocation_rows = await self._verified_invocation_rows(lease, turn_id)
        selected = next(
            (row for row in invocation_rows if row["invocation_id"] == invocation_id), None
        )
        if selected is None:
            raise AgentLoopError("agent_invocation_not_found")
        current_key, current_activity = await self._current_step_info(lease)
        if current_activity != "agent_tool" or current_key != selected["logical_key"]:
            raise AgentLoopError("agent_invocation_not_current")
        for earlier in invocation_rows:
            if earlier["ordinal"] >= selected["ordinal"]:
                break
            if earlier["accepted_payload"] is None:
                raise AgentLoopError("agent_invocation_out_of_order")

        try:
            parsed_result = parse_tool_result(result)
        except ToolContractError as error:
            raise AgentLoopError(error.code) from None
        if parsed_result.name != selected["tool_name"]:
            raise AgentLoopError("agent_tool_result_name_mismatch")
        serialized_result = parsed_result.model_dump(mode="json", warnings=False)
        if selected["tool_name"] == "apply_patch" and candidate is None:
            raise AgentLoopError("agent_candidate_checkpoint_required")
        if candidate is not None and selected["tool_name"] != "apply_patch":
            raise AgentLoopError("agent_candidate_tool_mismatch")
        accepted_payload = {
            "agent_turn_id": str(turn_id),
            "agent_invocation_id": str(invocation_id),
            "provider_call_id": selected["provider_call_id"],
            "ordinal": selected["ordinal"],
            "tool_name": selected["tool_name"],
            "tool_result": serialized_result,
        }
        if len(_canonical_json(accepted_payload, "agent_tool_result_invalid").encode("utf-8")) > (
            MAX_TOOL_RESULT_BYTES
        ):
            raise AgentLoopError("agent_tool_result_size_limit")
        current_revision = await self._read_step_revision(lease, lease.step_id)
        generated_next = self._next_tool_work(
            turn_id, invocation_rows, selected["ordinal"], current_revision
        )
        if next_work is not None and next_work != generated_next:
            raise AgentLoopError("agent_invocation_next_work_mismatch")
        next_work = generated_next

        transition: TransitionReceipt
        if candidate is not None:
            if (
                workspace is None
                or checkpoint_key is None
                or self._workspace_recovery is None
                or self._checkpoints is None
            ):
                raise AgentLoopError("agent_candidate_checkpoint_context_required")
            if selected["tool_name"] != "apply_patch":
                raise AgentLoopError("agent_candidate_tool_mismatch")
            if not isinstance(parsed_result, ApplyPatchResult):
                raise AgentLoopError("agent_candidate_result_invalid")
            if (
                candidate.source.archive_sha256 != parsed_result.candidate_archive_sha256
                or candidate.source.tree_sha256 != parsed_result.candidate_tree_sha256
                or list(candidate.changed_paths) != parsed_result.changed_paths
            ):
                raise AgentLoopError("agent_candidate_result_mismatch")
            try:
                verified_baseline = await self._checkpoints.restore_archive(
                    self._principal, lease, turn["baseline_artifact_id"]
                )
                incremental = await asyncio.to_thread(
                    rebase_patch_candidate, verified_baseline, workspace.verified, candidate
                )
            except (SourceToolError, WorkspaceRecoveryError, CheckpointStorageError) as error:
                raise AgentLoopError(
                    getattr(error, "code", "agent_candidate_lineage_invalid")
                ) from None
            except Exception:
                raise AgentLoopError("agent_baseline_restore_failed") from None
            try:
                checkpoint = await self._workspace_recovery.accept_candidate(
                    self._principal,
                    lease,
                    checkpoint_key,
                    workspace,
                    incremental,
                    result=accepted_payload,
                    target_run_status=RunStatus.RUNNING,
                    next_work=next_work,
                )
            except WorkspaceRecoveryError as error:
                raise AgentLoopError(error.code) from None
            return AgentToolReceipt(
                invocation_id=invocation_id,
                turn_id=turn_id,
                ordinal=selected["ordinal"],
                transition=checkpoint.transition,
                artifact_id=checkpoint.artifact_id,
                adopted=checkpoint.adopted,
            )

        try:
            transition = await asyncio.to_thread(
                self._transitions.commit_result,
                lease=lease,
                result=accepted_payload,
                target_run_status=RunStatus.RUNNING,
                next_work=next_work,
            )
        except (LeaseLost, InvalidTransition):
            raise AgentLoopError("agent_tool_transition_rejected") from None
        return AgentToolReceipt(
            invocation_id=invocation_id,
            turn_id=turn_id,
            ordinal=selected["ordinal"],
            transition=transition,
            artifact_id=transition.artifact_id,
            adopted=transition.adopted,
        )

    def _next_tool_work(
        self,
        turn_id: UUID,
        invocations: list[dict[str, Any]],
        current_ordinal: int,
        revision: int,
    ) -> NextWork:
        following = next(
            (row for row in invocations if row["ordinal"] == current_ordinal + 1), None
        )
        if following is not None:
            return NextWork(
                logical_key=following["logical_key"],
                activity="agent_tool",
                revision=revision,
                replay_policy=ReplayPolicy.REPLAY_SAFE,
            )
        return NextWork(
            logical_key=f"agent-turn:{turn_id.hex}:next",
            activity="agent_turn",
            revision=revision,
            replay_policy=ReplayPolicy.REPLAY_SAFE,
        )

    async def _adopt_tool_receipt(
        self,
        lease: ExecutionLease,
        turn_id: UUID,
        invocation_id: UUID,
        receipt: TransitionReceipt,
    ) -> AgentToolReceipt:
        turn = await self._read_turn(lease, turn_id)
        invocations = await self._read_invocations(lease, turn_id)
        await self._verify_turn_invocation_rows(lease, turn, invocations)
        selected = next((row for row in invocations if row["invocation_id"] == invocation_id), None)
        if (
            selected is None
            or receipt.result.get("agent_turn_id") != str(turn_id)
            or receipt.result.get("agent_invocation_id") != str(invocation_id)
            or receipt.result.get("ordinal") != selected["ordinal"]
            or receipt.result.get("provider_call_id") != selected["provider_call_id"]
            or receipt.result.get("tool_name") != selected["tool_name"]
            or receipt.run_status is not RunStatus.RUNNING
        ):
            raise AgentLoopError("agent_tool_transition_conflict")
        revision = await self._read_step_revision(lease, turn["step_id"])
        expected_next = self._next_tool_work(turn_id, invocations, selected["ordinal"], revision)
        if not await self._accepted_next_work_matches(lease, receipt, expected_next) or (
            selected["tool_name"] == "apply_patch"
        ) != (receipt.artifact_id is not None):
            raise AgentLoopError("agent_tool_transition_conflict")
        return AgentToolReceipt(
            invocation_id=invocation_id,
            turn_id=turn_id,
            ordinal=selected["ordinal"],
            transition=receipt,
            artifact_id=receipt.artifact_id,
            adopted=True,
        )

    async def _get_turn(
        self, lease: ExecutionLease, turn_id: UUID, *, require_turn_job: bool
    ) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(
                self._get_turn_once, lease, turn_id, require_turn_job=require_turn_job
            )
        except LeaseLost:
            raise AgentLoopError("agent_lease_lost") from None
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    async def _root_artifact_id(self, lease: ExecutionLease) -> UUID | None:
        try:
            return await asyncio.to_thread(self._root_artifact_id_once, lease)
        except LeaseLost:
            raise AgentLoopError("agent_lease_lost") from None
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    def _root_artifact_id_once(self, lease: ExecutionLease) -> UUID | None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            return connection.execute(
                text(
                    "SELECT artifact_id FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND artifact_id IS NOT NULL "
                    "ORDER BY event_sequence ASC LIMIT 1"
                ),
                {"owner_id": lease.owner_id, "run_id": lease.run_id},
            ).scalar_one_or_none()

    async def _read_turn(self, lease: ExecutionLease, turn_id: UUID) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(self._read_turn_once, lease, turn_id)
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    def _read_turn_once(self, lease: ExecutionLease, turn_id: UUID) -> dict[str, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = (
                connection.execute(
                    text(
                        "SELECT * FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner_id AND run_id=:run_id AND turn_id=:turn_id"
                    ),
                    {"owner_id": lease.owner_id, "run_id": lease.run_id, "turn_id": turn_id},
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise AgentLoopError("agent_turn_not_found")
        return dict(row)

    async def _read_invocations(self, lease: ExecutionLease, turn_id: UUID) -> list[dict[str, Any]]:
        try:
            return await asyncio.to_thread(self._read_invocations_once, lease, turn_id)
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    def _read_invocations_once(self, lease: ExecutionLease, turn_id: UUID) -> list[dict[str, Any]]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            rows = (
                connection.execute(
                    text(
                        "SELECT * FROM runtime_private.agent_invocations "
                        "WHERE owner_id=:owner_id AND run_id=:run_id AND turn_id=:turn_id "
                        "ORDER BY ordinal"
                    ),
                    {"owner_id": lease.owner_id, "run_id": lease.run_id, "turn_id": turn_id},
                )
                .mappings()
                .all()
            )
        return [dict(row) for row in rows]

    async def _read_step_revision(self, lease: ExecutionLease, step_id: UUID) -> int:
        try:
            return await asyncio.to_thread(self._read_step_revision_once, lease, step_id)
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    def _read_step_revision_once(self, lease: ExecutionLease, step_id: UUID) -> int:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            revision = connection.execute(
                text(
                    "SELECT revision FROM runtime_private.steps "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
                ),
                {"owner_id": lease.owner_id, "run_id": lease.run_id, "step_id": step_id},
            ).scalar_one_or_none()
        if revision is None:
            raise AgentLoopError("agent_step_not_found")
        return revision

    async def _accepted_next_work_matches(
        self,
        lease: ExecutionLease,
        receipt: TransitionReceipt,
        expected: NextWork | None,
    ) -> bool:
        if receipt.next_job_id is None:
            return expected is None
        if expected is None:
            return False
        try:
            actual = await asyncio.to_thread(self._read_next_work_once, lease, receipt.next_job_id)
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None
        return actual == (
            expected.logical_key,
            expected.activity,
            expected.revision,
            expected.replay_policy.value,
        )

    def _read_next_work_once(
        self, lease: ExecutionLease, job_id: UUID
    ) -> tuple[str, str, int, str] | None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = connection.execute(
                text(
                    "SELECT step.logical_key, step.activity, step.revision, job.replay_policy "
                    "FROM runtime_private.jobs AS job JOIN runtime_private.steps AS step "
                    "ON step.owner_id=job.owner_id AND step.run_id=job.run_id "
                    "AND step.step_id=job.step_id WHERE job.owner_id=:owner_id "
                    "AND job.run_id=:run_id AND job.job_id=:job_id"
                ),
                {"owner_id": lease.owner_id, "run_id": lease.run_id, "job_id": job_id},
            ).one_or_none()
        return (
            None
            if row is None
            else (row.logical_key, row.activity, row.revision, row.replay_policy)
        )

    def _get_turn_once(
        self, lease: ExecutionLease, turn_id: UUID, *, require_turn_job: bool
    ) -> dict[str, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            row = (
                connection.execute(
                    text(
                        "SELECT * FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner_id AND run_id=:run_id AND turn_id=:turn_id "
                        "FOR UPDATE"
                    ),
                    {"owner_id": lease.owner_id, "run_id": lease.run_id, "turn_id": turn_id},
                )
                .mappings()
                .one_or_none()
            )
            if row is None or (require_turn_job and row["job_id"] != lease.job_id):
                raise AgentLoopError("agent_turn_not_found")
            return dict(row)

    async def _authorize_model_dispatch(
        self, lease: ExecutionLease, turn_id: UUID
    ) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(self._authorize_model_dispatch_once, lease, turn_id)
        except LeaseLost:
            raise AgentLoopError("agent_lease_lost") from None
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    def _authorize_model_dispatch_once(
        self, lease: ExecutionLease, turn_id: UUID
    ) -> dict[str, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            locked = lock_current_execution(connection, self._principal, lease)
            run_row = connection.execute(
                text(
                    "SELECT model_budget_exceeded FROM runtime_private.runs "
                    "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).one_or_none()
            if run_row is None:
                raise AgentLoopError("agent_run_not_found")
            if run_row.model_budget_exceeded:
                raise AgentLoopError("agent_model_budget_exceeded")
            row = (
                connection.execute(
                    text(
                        "SELECT * FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id "
                        "AND turn_id=:turn_id FOR UPDATE"
                    ),
                    {
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                        "job_id": lease.job_id,
                        "turn_id": turn_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if (
                row is None
                or row["status"] != "prepared"
                or row["dispatch_authorized_at"] is not None
            ):
                raise AgentLoopError("agent_model_dispatch_not_authorized")
            updated = connection.execute(
                text(
                    "UPDATE runtime_private.agent_turns SET status='dispatching', "
                    "dispatch_authorized_at=clock_timestamp(), dispatch_generation=:generation, "
                    "dispatch_attempt_id=:attempt_id, dispatch_lease_owner=:lease_owner, "
                    "updated_at=clock_timestamp() WHERE owner_id=:owner_id AND run_id=:run_id "
                    "AND turn_id=:turn_id AND status='prepared'"
                ),
                {
                    "generation": lease.generation,
                    "attempt_id": lease.attempt_id,
                    "lease_owner": lease.lease_owner,
                    "owner_id": lease.owner_id,
                    "run_id": lease.run_id,
                    "turn_id": turn_id,
                },
            )
            if updated.rowcount != 1:
                raise LeaseLost("agent_model_dispatch_not_authorized")
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET model_requests_authorized="
                    "model_requests_authorized+1 WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            )
            _assert_execution_live(connection, locked)
            return dict(row)

    async def _accept_response(
        self,
        lease: ExecutionLease,
        turn_id: UUID,
        response: ScriptedModelResponse,
        invocations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(
                self._accept_response_once, lease, turn_id, response, invocations
            )
        except LeaseLost:
            raise AgentLoopError("agent_lease_lost") from None
        except IntegrityError:
            raise AgentLoopError("agent_response_identity_conflict") from None
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    def _accept_response_once(
        self,
        lease: ExecutionLease,
        turn_id: UUID,
        response: ScriptedModelResponse,
        invocations: list[dict[str, Any]],
    ) -> dict[str, Any]:
        serialized = _canonical_json(
            response.model_dump(mode="json", warnings=False), "agent_response_invalid"
        )
        response_bytes = serialized.encode("utf-8")
        if len(response_bytes) > MAX_AGENT_RESPONSE_BYTES:
            raise AgentLoopError("agent_response_size_limit")
        response_digest = hashlib.sha256(response_bytes).digest()
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            locked = lock_current_execution(connection, self._principal, lease)
            row = (
                connection.execute(
                    text(
                        "SELECT * FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id "
                        "AND turn_id=:turn_id FOR UPDATE"
                    ),
                    {
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                        "job_id": lease.job_id,
                        "turn_id": turn_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if (
                row is None
                or row["status"] != "dispatching"
                or row["dispatch_generation"] != lease.generation
                or row["dispatch_attempt_id"] != lease.attempt_id
                or row["dispatch_lease_owner"] != lease.lease_owner
            ):
                raise LeaseLost("agent_response_lease_lost")
            budget_row = (
                connection.execute(
                    text(
                        "SELECT model_input_tokens_used, model_input_tokens_reserved, "
                        "model_output_tokens_used, model_output_tokens_reserved, "
                        "model_usage_unknown_requests FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one_or_none()
            )
            if budget_row is None:
                raise AgentLoopError("agent_run_not_found")
            input_used = int(budget_row["model_input_tokens_used"])
            input_reserved = int(budget_row["model_input_tokens_reserved"])
            output_used = int(budget_row["model_output_tokens_used"])
            output_reserved = int(budget_row["model_output_tokens_reserved"])
            usage_unknown_requests = int(budget_row["model_usage_unknown_requests"])
            if response.input_tokens is not None and response.output_tokens is not None:
                if (
                    input_reserved < row["input_tokens_reserved"]
                    or output_reserved < row["output_tokens_reserved"]
                ):
                    raise AgentLoopError("agent_model_budget_corrupt")
                input_reserved -= row["input_tokens_reserved"]
                output_reserved -= row["output_tokens_reserved"]
                input_used += response.input_tokens
                output_used += response.output_tokens
            else:
                usage_unknown_requests += 1
            for invocation in invocations:
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.agent_invocations ("
                        "invocation_id, owner_id, run_id, turn_id, ordinal, provider_call_id, "
                        "tool_name, logical_key, arguments, arguments_sha256) VALUES ("
                        ":invocation_id, :owner_id, :run_id, :turn_id, :ordinal, "
                        ":provider_call_id, :tool_name, :logical_key, CAST(:arguments AS jsonb), "
                        ":arguments_sha256)"
                    ),
                    {
                        **invocation,
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                        "turn_id": turn_id,
                        "arguments": _canonical_json(
                            invocation["arguments"], "agent_tool_arguments_invalid"
                        ),
                    },
                )
            connection.execute(
                text(
                    "UPDATE runtime_private.agent_turns SET status='response_accepted', "
                    "response=CAST(:response AS jsonb), response_sha256=:response_sha256, "
                    "response_received_at=clock_timestamp(), input_tokens=:input_tokens, "
                    "output_tokens=:output_tokens, cached_input_tokens=:cached_input_tokens, "
                    "cache_creation_input_tokens=:cache_creation_input_tokens, "
                    "reasoning_output_tokens=:reasoning_output_tokens, "
                    "provider_request_id=:provider_request_id, latency_ms=:latency_ms, "
                    "updated_at=clock_timestamp() "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND turn_id=:turn_id"
                ),
                {
                    "response": serialized,
                    "response_sha256": response_digest,
                    "input_tokens": response.input_tokens,
                    "output_tokens": response.output_tokens,
                    "cached_input_tokens": response.cached_input_tokens,
                    "cache_creation_input_tokens": response.cache_creation_input_tokens,
                    "reasoning_output_tokens": response.reasoning_output_tokens,
                    "provider_request_id": response.provider_request_id,
                    "latency_ms": response.latency_ms,
                    "owner_id": lease.owner_id,
                    "run_id": lease.run_id,
                    "turn_id": turn_id,
                },
            )
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET model_input_tokens_used=:input_used, "
                    "model_input_tokens_reserved=:input_reserved, "
                    "model_output_tokens_used=:output_used, "
                    "model_output_tokens_reserved=:output_reserved, "
                    "model_requests_received=model_requests_received+1, "
                    "model_usage_unknown_requests=:usage_unknown_requests, "
                    "model_budget_exceeded=(model_budget_exceeded OR "
                    ":input_used+:input_reserved>:input_limit OR "
                    ":output_used+:output_reserved>:output_limit) "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {
                    "input_used": input_used,
                    "input_reserved": input_reserved,
                    "output_used": output_used,
                    "output_reserved": output_reserved,
                    "usage_unknown_requests": usage_unknown_requests,
                    "input_limit": MAX_AGENT_INPUT_TOKENS_PER_RUN,
                    "output_limit": MAX_AGENT_OUTPUT_TOKENS_PER_RUN,
                    "owner": lease.owner_id,
                    "run": lease.run_id,
                },
            )
            _assert_execution_live(connection, locked)
            result = dict(row)
            result.update(
                {
                    "status": "response_accepted",
                    "response": response.model_dump(mode="json", warnings=False),
                    "response_sha256": response_digest,
                    "input_tokens": response.input_tokens,
                    "output_tokens": response.output_tokens,
                    "cached_input_tokens": response.cached_input_tokens,
                    "cache_creation_input_tokens": response.cache_creation_input_tokens,
                    "reasoning_output_tokens": response.reasoning_output_tokens,
                    "provider_request_id": response.provider_request_id,
                    "latency_ms": response.latency_ms,
                }
            )
            return result

    async def _persist_invalid_response(
        self,
        lease: ExecutionLease,
        turn_id: UUID,
        safe_response: dict[str, Any],
        code: str,
        *,
        response: ScriptedModelResponse | None = None,
    ) -> None:
        try:
            await asyncio.to_thread(
                self._persist_invalid_response_once,
                lease,
                turn_id,
                safe_response,
                code,
                response,
            )
        except Exception:
            # The original invalid-response error remains primary; an uncommitted
            # dispatch authorization still prevents a second inference.
            return

    def _persist_invalid_response_once(
        self,
        lease: ExecutionLease,
        turn_id: UUID,
        safe_response: dict[str, Any],
        code: str,
        response: ScriptedModelResponse | None,
    ) -> None:
        serialized = _canonical_json(safe_response, "agent_response_invalid")
        if len(serialized.encode("utf-8")) > MAX_AGENT_RESPONSE_BYTES:
            serialized = _canonical_json({"protocol_error": code}, "agent_response_invalid")
        digest = hashlib.sha256(serialized.encode("utf-8")).digest()
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            locked = lock_current_execution(connection, self._principal, lease)
            budget_row = (
                connection.execute(
                    text(
                        "SELECT model_input_tokens_used, model_input_tokens_reserved, "
                        "model_output_tokens_used, model_output_tokens_reserved, "
                        "model_usage_unknown_requests FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one_or_none()
            )
            turn_row = (
                connection.execute(
                    text(
                        "SELECT input_tokens_reserved, output_tokens_reserved, "
                        "dispatch_generation, dispatch_attempt_id, dispatch_lease_owner "
                        "FROM runtime_private.agent_turns WHERE owner_id=:owner AND run_id=:run "
                        "AND job_id=:job AND turn_id=:turn AND status='dispatching' FOR UPDATE"
                    ),
                    {
                        "owner": lease.owner_id,
                        "run": lease.run_id,
                        "job": lease.job_id,
                        "turn": turn_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if (
                budget_row is None
                or turn_row is None
                or (
                    turn_row["dispatch_generation"] != lease.generation
                    or turn_row["dispatch_attempt_id"] != lease.attempt_id
                    or turn_row["dispatch_lease_owner"] != lease.lease_owner
                )
            ):
                raise LeaseLost("agent_response_attempt_not_current")
            input_used = int(budget_row["model_input_tokens_used"])
            input_reserved = int(budget_row["model_input_tokens_reserved"])
            output_used = int(budget_row["model_output_tokens_used"])
            output_reserved = int(budget_row["model_output_tokens_reserved"])
            usage_unknown = int(budget_row["model_usage_unknown_requests"])
            definitive_auth_rejection = response is None and code.endswith(
                ("_authentication_failed", "_request_rejected")
            )
            known_usage = (
                response is not None
                and response.input_tokens is not None
                and response.output_tokens is not None
            )
            if known_usage or definitive_auth_rejection:
                if (
                    input_reserved < turn_row["input_tokens_reserved"]
                    or output_reserved < turn_row["output_tokens_reserved"]
                ):
                    raise AgentLoopError("agent_model_budget_corrupt")
                input_reserved -= turn_row["input_tokens_reserved"]
                output_reserved -= turn_row["output_tokens_reserved"]
            if known_usage and response is not None:
                input_used += response.input_tokens or 0
                output_used += response.output_tokens or 0
            elif not definitive_auth_rejection:
                usage_unknown += 1
            reservation_overrun = bool(
                known_usage
                and response is not None
                and (
                    response.input_tokens > turn_row["input_tokens_reserved"]
                    or response.output_tokens > turn_row["output_tokens_reserved"]
                )
            )
            updated = connection.execute(
                text(
                    "UPDATE runtime_private.agent_turns SET status='invalid_response', "
                    "response=CAST(:response AS jsonb), response_sha256=:response_sha256, "
                    "response_received_at=clock_timestamp(), response_error_code=:code, "
                    "input_tokens=:input_tokens, output_tokens=:output_tokens, "
                    "cached_input_tokens=:cached_input_tokens, "
                    "cache_creation_input_tokens=:cache_creation_input_tokens, "
                    "reasoning_output_tokens=:reasoning_output_tokens, "
                    "provider_request_id=:provider_request_id, latency_ms=:latency_ms, "
                    "updated_at=clock_timestamp() WHERE owner_id=:owner_id AND run_id=:run_id "
                    "AND job_id=:job_id AND turn_id=:turn_id AND status='dispatching' "
                    "AND dispatch_generation=:generation AND dispatch_attempt_id=:attempt_id "
                    "AND dispatch_lease_owner=:lease_owner"
                ),
                {
                    "response": serialized,
                    "response_sha256": digest,
                    "code": code,
                    "input_tokens": None if response is None else response.input_tokens,
                    "output_tokens": None if response is None else response.output_tokens,
                    "cached_input_tokens": None
                    if response is None
                    else response.cached_input_tokens,
                    "cache_creation_input_tokens": (
                        None if response is None else response.cache_creation_input_tokens
                    ),
                    "reasoning_output_tokens": (
                        None if response is None else response.reasoning_output_tokens
                    ),
                    "provider_request_id": (
                        None if response is None else response.provider_request_id
                    ),
                    "latency_ms": None if response is None else response.latency_ms,
                    "owner_id": lease.owner_id,
                    "run_id": lease.run_id,
                    "job_id": lease.job_id,
                    "turn_id": turn_id,
                    "generation": lease.generation,
                    "attempt_id": lease.attempt_id,
                    "lease_owner": lease.lease_owner,
                },
            )
            if updated.rowcount != 1:
                raise LeaseLost("agent_response_attempt_not_current")
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET model_input_tokens_used=:input_used, "
                    "model_input_tokens_reserved=:input_reserved, "
                    "model_output_tokens_used=:output_used, "
                    "model_output_tokens_reserved=:output_reserved, "
                    "model_requests_received=model_requests_received+1, "
                    "model_usage_unknown_requests=:usage_unknown, "
                    "model_budget_exceeded=(model_budget_exceeded OR :overrun OR "
                    ":input_used+:input_reserved>:input_limit OR "
                    ":output_used+:output_reserved>:output_limit) "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {
                    "input_used": input_used,
                    "input_reserved": input_reserved,
                    "output_used": output_used,
                    "output_reserved": output_reserved,
                    "usage_unknown": usage_unknown,
                    "overrun": reservation_overrun,
                    "input_limit": MAX_AGENT_INPUT_TOKENS_PER_RUN,
                    "output_limit": MAX_AGENT_OUTPUT_TOKENS_PER_RUN,
                    "owner": lease.owner_id,
                    "run": lease.run_id,
                },
            )
            _assert_execution_live(connection, locked)

    async def _mark_response_unknown(self, lease: ExecutionLease, turn_id: UUID) -> None:
        try:
            should_fail = await asyncio.to_thread(self._mark_response_unknown_once, lease, turn_id)
        except Exception:
            return
        if should_fail:
            await self._record_unknown_failure(lease)

    def _mark_response_unknown_once(self, lease: ExecutionLease, turn_id: UUID) -> bool:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            locked = lock_current_execution(connection, self._principal, lease)
            connection.execute(
                text(
                    "SELECT run_id FROM runtime_private.runs "
                    "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).one_or_none()
            row = connection.execute(
                text(
                    "SELECT status FROM runtime_private.agent_turns "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id "
                    "AND turn_id=:turn_id FOR UPDATE"
                ),
                {
                    "owner_id": lease.owner_id,
                    "run_id": lease.run_id,
                    "job_id": lease.job_id,
                    "turn_id": turn_id,
                },
            ).one_or_none()
            if row is None or row.status not in {"dispatching", "unknown"}:
                return False
            if row.status == "dispatching":
                connection.execute(
                    text(
                        "UPDATE runtime_private.agent_turns SET status='unknown', "
                        "updated_at=clock_timestamp() WHERE owner_id=:owner_id AND run_id=:run_id "
                        "AND job_id=:job_id AND turn_id=:turn_id AND status='dispatching'"
                    ),
                    {
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                        "job_id": lease.job_id,
                        "turn_id": turn_id,
                    },
                )
                connection.execute(
                    text(
                        "UPDATE runtime_private.runs SET model_requests_ambiguous="
                        "model_requests_ambiguous+1, model_usage_unknown_requests="
                        "model_usage_unknown_requests+1 WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                _assert_execution_live(connection, locked)
            return True

    async def _record_unknown_failure(self, lease: ExecutionLease) -> None:
        try:
            await asyncio.to_thread(
                self._recovery.fail_attempt,
                lease=lease,
                failure_class=FailureClass.MODEL_RESPONSE_LOST,
            )
        except Exception:
            return

    async def _fail_invalid_attempt(self, lease: ExecutionLease) -> None:
        try:
            await asyncio.to_thread(
                self._recovery.fail_attempt,
                lease=lease,
                failure_class=FailureClass.INVALID_INPUT,
            )
        except Exception:
            return

    async def _fail_corrupt_attempt(self, lease: ExecutionLease) -> None:
        try:
            await asyncio.to_thread(
                self._recovery.fail_attempt,
                lease=lease,
                failure_class=FailureClass.CORRUPT_ARTIFACT,
            )
        except Exception:
            return

    async def _turn_receipt(
        self, lease: ExecutionLease, row: dict[str, Any], *, adopted: bool
    ) -> AgentTurnReceipt:
        invocations = await self.pending_invocations(lease, row["turn_id"])
        raw_response = row["response"]
        try:
            response = ScriptedModelResponse.model_validate_json(
                _canonical_json(raw_response, "agent_persisted_response_corrupt"), strict=True
            )
        except ValidationError:
            raise AgentLoopError("agent_persisted_response_corrupt") from None
        return AgentTurnReceipt(
            turn_id=row["turn_id"],
            request_sha256=row["request_sha256"].hex(),
            response_sha256=row["response_sha256"].hex(),
            response=response,
            invocations=invocations,
            adopted=adopted,
        )

    async def _load_invocations(self, lease: ExecutionLease, turn_id: UUID) -> list[dict[str, Any]]:
        try:
            return await asyncio.to_thread(self._load_invocations_once, lease, turn_id)
        except LeaseLost:
            raise AgentLoopError("agent_lease_lost") from None
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    async def _verified_invocation_rows(
        self, lease: ExecutionLease, turn_id: UUID
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        turn = await self._get_turn(lease, turn_id, require_turn_job=False)
        if turn["status"] != "response_accepted":
            raise AgentLoopError("agent_turn_response_not_accepted")
        rows = await self._load_invocations(lease, turn_id)
        await self._verify_turn_invocation_rows(lease, turn, rows)
        return turn, rows

    async def _verify_turn_invocation_rows(
        self,
        lease: ExecutionLease,
        turn: dict[str, Any],
        rows: list[dict[str, Any]],
    ) -> None:
        try:
            _verify_persisted_response_and_calls(turn, rows)
        except AgentLoopError as error:
            if error.code in {
                "agent_persisted_response_corrupt",
                "agent_invocation_materialization_corrupt",
                "agent_invocation_result_corrupt",
            }:
                await self._fail_corrupt_attempt(lease)
            raise

    def _load_invocations_once(self, lease: ExecutionLease, turn_id: UUID) -> list[dict[str, Any]]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            rows = (
                connection.execute(
                    text(
                        "SELECT invocation.*, accepted.payload AS accepted_payload, "
                        "accepted.artifact_id AS accepted_artifact_id, "
                        "accepted.result_sha256 AS accepted_result_sha256, "
                        "accepted.step_id AS accepted_step_id, "
                        "accepted.job_id AS accepted_job_id, "
                        "accepted_step.logical_key AS accepted_logical_key, "
                        "accepted_step.activity AS accepted_activity "
                        "FROM runtime_private.agent_invocations AS invocation "
                        "LEFT JOIN LATERAL ("
                        "SELECT payload, artifact_id, result_sha256, step_id, job_id "
                        "FROM runtime_private.accepted_results "
                        "WHERE owner_id=invocation.owner_id AND run_id=invocation.run_id "
                        "AND payload->>'agent_invocation_id'=CAST(invocation.invocation_id AS text)"
                        " LIMIT 1) AS accepted ON true "
                        "LEFT JOIN runtime_private.steps AS accepted_step ON "
                        "accepted_step.owner_id=invocation.owner_id "
                        "AND accepted_step.run_id=invocation.run_id "
                        "AND accepted_step.step_id=accepted.step_id "
                        "WHERE invocation.owner_id=:owner_id AND invocation.run_id=:run_id "
                        "AND invocation.turn_id=:turn_id ORDER BY invocation.ordinal"
                    ),
                    {"owner_id": lease.owner_id, "run_id": lease.run_id, "turn_id": turn_id},
                )
                .mappings()
                .all()
            )
        return [dict(row) for row in rows]

    async def _current_step_info(self, lease: ExecutionLease) -> tuple[str, str]:
        try:
            return await asyncio.to_thread(self._current_step_info_once, lease)
        except LeaseLost:
            raise AgentLoopError("agent_lease_lost") from None
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    def _current_step_info_once(self, lease: ExecutionLease) -> tuple[str, str]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            row = connection.execute(
                text(
                    "SELECT logical_key, activity FROM runtime_private.steps "
                    "WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id"
                ),
                {"owner_id": lease.owner_id, "run_id": lease.run_id, "step_id": lease.step_id},
            ).one_or_none()
            if row is None:
                raise AgentLoopError("agent_step_not_found")
            return row.logical_key, row.activity

    async def _accepted_transition(self, lease: ExecutionLease) -> TransitionReceipt | None:
        try:
            return await asyncio.to_thread(self._transitions.get_accepted_result, lease)
        except LeaseLost:
            raise AgentLoopError("agent_owner_mismatch") from None
        except SQLAlchemyError:
            raise AgentLoopError("agent_persistence_unavailable") from None

    def _check_owner(self, lease: ExecutionLease) -> None:
        if lease.owner_id != self._principal.account_id:
            raise AgentLoopError("agent_owner_mismatch")


def _validate_complete_response(
    response: ScriptedModelResponse,
    requested_model_id: str,
    turn_id: UUID,
    output_tokens_reserved: int,
) -> list[dict[str, Any]]:
    if response.model_id != requested_model_id:
        raise AgentLoopError("agent_model_identity_mismatch")
    if response.stop_reason == "refusal":
        raise AgentLoopError("agent_response_refusal")
    if not response.complete or response.stop_reason not in {"stop", "tool_calls"}:
        raise AgentLoopError("agent_response_incomplete")
    if (response.stop_reason == "tool_calls") != bool(response.calls):
        raise AgentLoopError("agent_response_stop_reason_mismatch")
    if response.output_tokens is not None and response.output_tokens > output_tokens_reserved:
        raise AgentLoopError("agent_output_reservation_exceeded")
    if len(response.calls) > MAX_AGENT_TOOL_CALLS:
        raise AgentLoopError("agent_tool_call_limit")
    materialized: list[dict[str, Any]] = []
    for ordinal, call in enumerate(response.calls):
        try:
            raw_arguments = json.loads(
                call.arguments_json,
                object_pairs_hook=_unique_json_object,
                parse_constant=_reject_json_constant,
            )
        except (TypeError, ValueError, RecursionError):
            raise AgentLoopError("agent_tool_arguments_invalid") from None
        if not isinstance(raw_arguments, dict) or "name" in raw_arguments:
            raise AgentLoopError("agent_tool_arguments_invalid")
        try:
            parsed_call = parse_tool_call({"name": call.name, **raw_arguments})
        except ToolContractError as error:
            raise AgentLoopError(error.code) from None
        arguments = parsed_call.model_dump(mode="json", warnings=False)
        argument_bytes = _canonical_json(arguments, "agent_tool_arguments_invalid").encode("utf-8")
        if len(argument_bytes) > MAX_TOOL_ARGUMENT_BYTES:
            raise AgentLoopError("agent_tool_arguments_size_limit")
        materialized.append(
            {
                "invocation_id": uuid5(turn_id, f"{ordinal}:{call.provider_call_id}"),
                "ordinal": ordinal,
                "provider_call_id": call.provider_call_id,
                "tool_name": call.name,
                "logical_key": f"agent-tool:{turn_id.hex}:{ordinal}",
                "arguments": arguments,
                "arguments_sha256": hashlib.sha256(argument_bytes).digest(),
            }
        )
    return materialized


def _estimate_input_tokens(serialized_request: bytes) -> int:
    """Conservative local fallback: one token per UTF-8 byte plus provider envelope."""
    if not isinstance(serialized_request, bytes) or not serialized_request:
        raise AgentLoopError("agent_input_estimate_invalid")
    return len(serialized_request) + MODEL_INPUT_ENVELOPE_RESERVATION


def _source_request_digest(
    *,
    baseline_artifact_id: UUID,
    logical_key: str,
    model_id: str,
    initial_context: dict[str, Any],
    tool_schema_sha256: bytes,
    output_tokens_reserved: int,
) -> bytes:
    payload = {
        "baseline_artifact_id": str(baseline_artifact_id),
        "initial_context": initial_context,
        "logical_key": logical_key,
        "model_id": model_id,
        "output_tokens_reserved": output_tokens_reserved,
        "tool_schema_sha256": tool_schema_sha256.hex(),
    }
    serialized = _canonical_json(payload, "agent_request_invalid")
    return hashlib.sha256(serialized.encode("utf-8")).digest()


def _load_revision_feedback(
    connection: Any,
    owner_id: UUID,
    run_id: UUID,
    revision: int,
) -> dict[str, Any] | None:
    if revision == 0:
        return None
    if revision not in {1, 2}:
        raise AgentLoopError("agent_revision_limit_exceeded")
    row = (
        connection.execute(
            text(
                "SELECT feedback_id, revision, feedback, feedback_sha256, actor_id "
                "FROM runtime_private.revision_feedback WHERE owner_id=:owner "
                "AND run_id=:run AND revision=:revision"
            ),
            {"owner": owner_id, "run": run_id, "revision": revision},
        )
        .mappings()
        .one_or_none()
    )
    if row is None or row["actor_id"] != owner_id:
        raise AgentLoopError("agent_revision_feedback_missing")
    feedback = row["feedback"]
    digest = bytes(row["feedback_sha256"]).hex()
    try:
        encoded = feedback.encode("utf-8", errors="strict")
    except (AttributeError, UnicodeEncodeError):
        raise AgentLoopError("agent_revision_feedback_corrupt") from None
    if len(encoded) > 16_384 or hashlib.sha256(encoded).hexdigest() != digest:
        raise AgentLoopError("agent_revision_feedback_corrupt")
    return {
        "feedback_id": str(row["feedback_id"]),
        "revision": row["revision"],
        "feedback": feedback,
        "feedback_sha256": digest,
    }


def _load_accepted_context_history(
    connection: Any,
    *,
    owner_id: UUID,
    run_id: UUID,
    model_id: str,
    initial_context: dict[str, Any],
    before: Any | None = None,
) -> tuple[list[dict[str, Any]], list[UUID], list[UUID]]:
    turns = (
        connection.execute(
            text(
                "SELECT turn.*, step.revision AS step_revision "
                "FROM runtime_private.agent_turns AS turn "
                "JOIN runtime_private.steps AS step ON step.owner_id=turn.owner_id "
                "AND step.run_id=turn.run_id AND step.step_id=turn.step_id "
                "WHERE turn.owner_id=:owner AND turn.run_id=:run "
                "AND turn.status='response_accepted' ORDER BY turn.created_at, turn.turn_id"
            ),
            {"owner": owner_id, "run": run_id},
        )
        .mappings()
        .all()
    )
    accepted_turns: list[dict[str, Any]] = []
    turn_ids: list[UUID] = []
    invocation_ids: list[UUID] = []
    for raw_turn in turns:
        turn = dict(raw_turn)
        if before is not None and (
            turn["response_received_at"] is None or turn["response_received_at"] >= before
        ):
            continue
        if turn["model_id"] != model_id:
            raise AgentLoopError("agent_model_switch_forbidden")
        if turn["source_sha256"] is None or turn["source_request_sha256"] is None:
            raise AgentLoopError("agent_context_provenance_missing")
        if _initial_context_from_persisted(turn["context"]) != initial_context:
            raise AgentLoopError("agent_task_context_pinned")

        schemas = turn["tool_schemas"]
        schema_sha = hashlib.sha256(
            _canonical_json(schemas, "agent_tool_schema_corrupt").encode("utf-8")
        ).digest()
        expected_source_request = _source_request_digest(
            baseline_artifact_id=turn["baseline_artifact_id"],
            logical_key=turn["logical_key"],
            model_id=turn["model_id"],
            initial_context=initial_context,
            tool_schema_sha256=schema_sha,
            output_tokens_reserved=turn["output_tokens_reserved"],
        )
        if turn["source_request_sha256"] != expected_source_request:
            raise AgentLoopError("agent_persisted_request_corrupt")
        if _persisted_request_digest(turn) != turn["request_sha256"]:
            raise AgentLoopError("agent_persisted_request_corrupt")
        expected_source = context_source_sha256(
            initial_context=initial_context,
            accepted_turn_ids=turn_ids,
            accepted_invocation_ids=invocation_ids,
            revision_feedback=_load_revision_feedback(
                connection, owner_id, run_id, turn["step_revision"]
            ),
        )
        if turn["source_sha256"].hex() != expected_source:
            raise AgentLoopError("agent_context_provenance_missing")

        invocations = (
            connection.execute(
                text(
                    "SELECT invocation.*, accepted.payload AS accepted_payload, "
                    "accepted.accepted_at AS accepted_at, "
                    "accepted.result_sha256 AS accepted_result_sha256, "
                    "accepted.step_id AS accepted_step_id, "
                    "accepted.job_id AS accepted_job_id, "
                    "accepted_step.logical_key AS accepted_logical_key, "
                    "accepted_step.activity AS accepted_activity "
                    "FROM runtime_private.agent_invocations AS invocation "
                    "LEFT JOIN LATERAL (SELECT payload, accepted_at, result_sha256, "
                    "step_id, job_id FROM "
                    "runtime_private.accepted_results WHERE owner_id=invocation.owner_id "
                    "AND run_id=invocation.run_id AND "
                    "payload->>'agent_invocation_id'="
                    "CAST(invocation.invocation_id AS text) LIMIT 1) AS accepted ON true "
                    "LEFT JOIN runtime_private.steps AS accepted_step ON "
                    "accepted_step.owner_id=invocation.owner_id "
                    "AND accepted_step.run_id=invocation.run_id "
                    "AND accepted_step.step_id=accepted.step_id "
                    "WHERE invocation.owner_id=:owner AND invocation.run_id=:run "
                    "AND invocation.turn_id=:turn ORDER BY invocation.ordinal"
                ),
                {"owner": owner_id, "run": run_id, "turn": turn["turn_id"]},
            )
            .mappings()
            .all()
        )
        invocation_rows = [dict(item) for item in invocations]
        if any(
            item["accepted_payload"] is None
            or item["accepted_at"] is None
            or (before is not None and item["accepted_at"] >= before)
            for item in invocation_rows
        ):
            raise AgentLoopError("agent_context_tool_result_missing")
        _verify_persisted_response_and_calls(turn, invocation_rows)
        invocation_context = [
            {
                "provider_call_id": item["provider_call_id"],
                "tool_name": item["tool_name"],
                "arguments": item["arguments"],
                "accepted_payload": item["accepted_payload"],
            }
            for item in invocation_rows
        ]
        accepted_turns.append(
            {
                "model_id": turn["model_id"],
                "context": turn["context"],
                "response": turn["response"],
                "invocations": invocation_context,
            }
        )
        turn_ids.append(turn["turn_id"])
        invocation_ids.extend(item["invocation_id"] for item in invocation_rows)
    return accepted_turns, turn_ids, invocation_ids


def _initial_context_from_persisted(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AgentLoopError("agent_context_provenance_missing")
    initial = {
        key: value[key] for key in ("task", "objective", "constraints", "policy") if key in value
    }
    try:
        return canonicalize_initial_context(initial)
    except AgentContextError:
        raise AgentLoopError("agent_context_provenance_missing") from None


def _model_budget_snapshot(run_id: UUID, row: dict[str, Any]) -> ModelBudgetSnapshot:
    requests_reserved = int(row["model_requests_reserved"])
    requests_authorized = int(row["model_requests_authorized"])
    requests_received = int(row["model_requests_received"])
    requests_ambiguous = int(row["model_requests_ambiguous"])
    input_used = int(row["model_input_tokens_used"])
    input_reserved = int(row["model_input_tokens_reserved"])
    output_used = int(row["model_output_tokens_used"])
    output_reserved = int(row["model_output_tokens_reserved"])
    return ModelBudgetSnapshot(
        run_id=run_id,
        model_id=row["model_id"],
        requests_reserved=requests_reserved,
        requests_authorized=requests_authorized,
        requests_received=requests_received,
        requests_ambiguous=requests_ambiguous,
        requests_in_flight=max(0, requests_authorized - requests_received - requests_ambiguous),
        requests_remaining=max(0, MAX_AGENT_TURNS_PER_RUN - requests_reserved),
        input_tokens_used=input_used,
        input_tokens_reserved=input_reserved,
        input_tokens_remaining=max(0, MAX_AGENT_INPUT_TOKENS_PER_RUN - input_used - input_reserved),
        output_tokens_used=output_used,
        output_tokens_reserved=output_reserved,
        output_tokens_remaining=max(
            0, MAX_AGENT_OUTPUT_TOKENS_PER_RUN - output_used - output_reserved
        ),
        usage_unknown_requests=int(row["model_usage_unknown_requests"]),
        exceeded=bool(row["model_budget_exceeded"]),
    )


def _verify_persisted_response_and_calls(
    turn: dict[str, Any], invocations: list[dict[str, Any]]
) -> None:
    try:
        response = ScriptedModelResponse.model_validate_json(
            _canonical_json(turn["response"], "agent_persisted_response_corrupt"), strict=True
        )
    except ValidationError:
        raise AgentLoopError("agent_persisted_response_corrupt") from None
    # Hash the stored representation so additive defaults preserve prior scripted responses.
    response_json = _canonical_json(turn["response"], "agent_persisted_response_corrupt")
    if hashlib.sha256(response_json.encode("utf-8")).digest() != turn["response_sha256"]:
        raise AgentLoopError("agent_persisted_response_corrupt")
    if (
        response.input_tokens != turn["input_tokens"]
        or response.output_tokens != turn["output_tokens"]
        or response.cached_input_tokens != turn.get("cached_input_tokens")
        or response.cache_creation_input_tokens != turn.get("cache_creation_input_tokens")
        or response.reasoning_output_tokens != turn.get("reasoning_output_tokens")
        or response.provider_request_id != turn.get("provider_request_id")
        or response.latency_ms != turn.get("latency_ms")
    ):
        raise AgentLoopError("agent_persisted_response_corrupt")
    expected = _validate_complete_response(
        response,
        turn["model_id"],
        turn["turn_id"],
        turn["output_tokens_reserved"],
    )
    if len(expected) != len(invocations):
        raise AgentLoopError("agent_invocation_materialization_corrupt")
    for expected_call, actual in zip(expected, invocations, strict=True):
        arguments = actual["arguments"]
        arguments_json = _canonical_json(arguments, "agent_tool_arguments_corrupt")
        if (
            actual["invocation_id"] != expected_call["invocation_id"]
            or actual["ordinal"] != expected_call["ordinal"]
            or actual["provider_call_id"] != expected_call["provider_call_id"]
            or actual["tool_name"] != expected_call["tool_name"]
            or actual["logical_key"] != expected_call["logical_key"]
            or arguments != expected_call["arguments"]
            or hashlib.sha256(arguments_json.encode("utf-8")).digest() != actual["arguments_sha256"]
        ):
            raise AgentLoopError("agent_invocation_materialization_corrupt")
        accepted = actual.get("accepted_payload")
        if accepted is not None:
            if (
                accepted.get("agent_turn_id") != str(turn["turn_id"])
                or accepted.get("agent_invocation_id") != str(actual["invocation_id"])
                or accepted.get("ordinal") != actual["ordinal"]
                or accepted.get("tool_name") != actual["tool_name"]
            ):
                raise AgentLoopError("agent_invocation_result_corrupt")
            if "accepted_result_sha256" in actual:
                payload_digest = hashlib.sha256(
                    _canonical_json(accepted, "agent_invocation_result_corrupt").encode("utf-8")
                ).digest()
                if payload_digest != actual["accepted_result_sha256"]:
                    raise AgentLoopError("agent_invocation_result_corrupt")
                if (
                    actual.get("accepted_step_id") is None
                    or actual.get("accepted_job_id") is None
                    or actual.get("accepted_logical_key") != actual["logical_key"]
                    or actual.get("accepted_activity") != "agent_tool"
                ):
                    raise AgentLoopError("agent_invocation_result_corrupt")


def _persisted_request_digest(turn: dict[str, Any]) -> bytes:
    payload = {
        "baseline_artifact_id": str(turn["baseline_artifact_id"]),
        "context": turn["context"],
        "logical_key": turn["logical_key"],
        "model_id": turn["model_id"],
        "output_tokens_reserved": turn["output_tokens_reserved"],
        "tool_schemas": turn["tool_schemas"],
    }
    serialized = _canonical_json(payload, "agent_persisted_request_corrupt")
    return hashlib.sha256(serialized.encode("utf-8")).digest()


def _canonical_json(value: Any, error_code: str) -> str:
    try:
        serialized = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        serialized.encode("utf-8", errors="strict")
        return serialized
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError):
        raise AgentLoopError(error_code) from None


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("agent_tool_arguments_duplicate_key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"agent_json_constant_invalid:{value}")


def _assert_execution_live(connection: Any, locked: LockedExecution) -> None:
    database_now = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
    if (
        locked.lease_expires_at is None
        or locked.lease_expires_at <= database_now
        or (locked.run_deadline_at is not None and locked.run_deadline_at <= database_now)
    ):
        raise LeaseLost("agent_lease_expired_before_commit")


def _intent_from_row(row: dict[str, Any]) -> AgentTurnIntent:
    return AgentTurnIntent(
        turn_id=row["turn_id"],
        owner_id=row["owner_id"],
        run_id=row["run_id"],
        job_id=row["job_id"],
        logical_key=row["logical_key"],
        model_id=row["model_id"],
        request_sha256=row["request_sha256"].hex(),
        baseline_artifact_id=row["baseline_artifact_id"],
        status=row["status"],
    )


def _invocation_from_row(row: dict[str, Any]) -> AgentInvocation:
    return AgentInvocation(
        invocation_id=row["invocation_id"],
        turn_id=row["turn_id"],
        ordinal=row["ordinal"],
        provider_call_id=row["provider_call_id"],
        name=row["tool_name"],
        logical_key=row["logical_key"],
        arguments=row["arguments"],
        accepted=row["accepted_payload"] is not None,
        result=row["accepted_payload"],
        artifact_id=row["accepted_artifact_id"],
    )
