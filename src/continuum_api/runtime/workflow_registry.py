"""Trusted composition of the bounded admitted-workflow activity handlers."""

from __future__ import annotations

from typing import Any

from sqlalchemy import Engine

from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.runtime.activity_runner import ActivityHandler, ActivityRunner
from continuum_api.runtime.admitted_agent_activity import AdmittedAgentTurnInputResolver
from continuum_api.runtime.agent_activity import (
    AgentActivityDispatcher,
    AgentActivityStep,
    PolicyResolver,
)
from continuum_api.runtime.agent_loop import DurableAgentLoopService
from continuum_api.runtime.approvals import ApprovalService
from continuum_api.runtime.github_pr_publication import GitHubPullRequestService
from continuum_api.runtime.github_publication import GitHubBranchPublicationService
from continuum_api.runtime.leases import ExecutionLease, QueueService
from continuum_api.runtime.persistence import Principal, ReplayPolicy
from continuum_api.runtime.publication_activity import ApprovedPublicationActivityHandler
from continuum_api.runtime.source_preparation import SourceFetcher, SourcePreparationActivityHandler
from continuum_api.runtime.transitions import NextWork, TransitionService
from continuum_api.runtime.validation_activity import (
    ApprovalFreezeActivityHandler,
    CandidateValidationActivityHandler,
)
from continuum_api.runtime.validation_source import (
    PersistedCandidateValidationInputResolver,
    PolicyContextResolver,
)
from continuum_api.tools.schemas import fixed_model_tool_schemas
from continuum_api.validation.service import ValidationService


def candidate_validation_next_work(_lease: ExecutionLease, step: AgentActivityStep) -> NextWork:
    """Build the sole allowed continuation after a final agent turn."""
    if (
        not isinstance(step, AgentActivityStep)
        or step.activity != "agent_turn"
        or type(step.revision) is not int
        or step.revision < 0
    ):
        raise ValueError("agent_candidate_validation_work_invalid")
    return NextWork(
        logical_key=f"candidate-validation:r{step.revision}",
        activity="candidate_validation",
        revision=step.revision,
        replay_policy=ReplayPolicy.REPLAY_SAFE,
    )


def build_workflow_activity_runner(
    engine: Engine,
    principal: Principal,
    *,
    checkpoints: CheckpointStorageService,
    fetch_source: SourceFetcher,
    agent_service: DurableAgentLoopService,
    model_adapter: Any,
    resolve_tool_policy_context: PolicyResolver,
    resolve_validation_policy_context: PolicyContextResolver,
    validation: ValidationService,
    approvals: ApprovalService,
    transitions: TransitionService,
    branch_publication: GitHubBranchPublicationService,
    pull_request_publication: GitHubPullRequestService,
    lease_owner: str,
    queue: QueueService | None = None,
    heartbeat_interval_seconds: float = 10.0,
) -> ActivityRunner:
    """Compose the fixed admitted workflow registry around injected services.

    Construction only wires trusted dependencies; it performs no provider,
    repository, or network work. Activity names and handler bindings are fixed
    here so callers cannot add routes or override one handler independently.
    """
    tool_schemas = fixed_model_tool_schemas()
    source_preparation = SourcePreparationActivityHandler(
        engine, principal, checkpoints, fetch_source
    )
    agent_inputs = AdmittedAgentTurnInputResolver(
        engine, principal, checkpoints, tool_schemas=tool_schemas
    )
    agent_dispatcher = AgentActivityDispatcher(
        engine,
        principal,
        agent_service,
        resolve_turn_inputs=agent_inputs,
        resolve_policy_context=resolve_tool_policy_context,
        candidate_validation_work=candidate_validation_next_work,
        adapter=model_adapter,
    )
    validation_inputs = PersistedCandidateValidationInputResolver(
        engine, principal, checkpoints, resolve_validation_policy_context
    )
    candidate_validation = CandidateValidationActivityHandler(
        engine,
        principal,
        validation_inputs,
        validation,
        approvals,
        transitions,
    )
    approval_freeze = ApprovalFreezeActivityHandler(engine, principal, approvals)
    publication = ApprovedPublicationActivityHandler(
        engine, principal, transitions, branch_publication, pull_request_publication
    )
    handlers: dict[str, ActivityHandler] = {
        "source_preparation": source_preparation,
        "agent_turn": agent_dispatcher,
        "agent_tool": agent_dispatcher,
        "candidate_validation": candidate_validation,
        "approval_freeze": approval_freeze,
        "publish_approved": publication,
    }
    return ActivityRunner(
        engine,
        principal,
        handlers,
        lease_owner=lease_owner,
        queue=queue,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )
