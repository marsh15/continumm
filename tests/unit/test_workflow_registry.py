from __future__ import annotations

import asyncio
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy import Engine

from continuum_api.adapters.anthropic import _tools as anthropic_tools
from continuum_api.adapters.openai import _tools as openai_tools
from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.runtime.activity_runner import ActivityRunner, UnknownActivity
from continuum_api.runtime.agent_activity import AgentActivityStep, PolicyResolver
from continuum_api.runtime.agent_loop import DurableAgentLoopService
from continuum_api.runtime.approvals import ApprovalService
from continuum_api.runtime.github_pr_publication import GitHubPullRequestService
from continuum_api.runtime.github_publication import GitHubBranchPublicationService
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.persistence import Principal, ReplayPolicy
from continuum_api.runtime.source_preparation import SourceFetcher
from continuum_api.runtime.transitions import NextWork, TransitionService
from continuum_api.runtime.validation_source import PolicyContextResolver
from continuum_api.runtime.workflow_registry import (
    build_workflow_activity_runner,
    candidate_validation_next_work,
)
from continuum_api.tools.contracts import parse_tool_call
from continuum_api.tools.schemas import fixed_model_tool_schemas
from continuum_api.validation.managed_hidden_checks import ManagedHiddenCheckService
from continuum_api.validation.service import ValidationService


def test_fixed_schemas_are_strict_and_supported_by_both_adapters() -> None:
    schemas = fixed_model_tool_schemas()

    assert tuple(schema["name"] for schema in schemas) == (
        "list_files",
        "read_file",
        "search_text",
        "apply_patch",
        "diff",
    )
    assert len(openai_tools(schemas)) == len(schemas)
    assert len(anthropic_tools(schemas)) == len(schemas)
    parsed = parse_tool_call({"name": "list_files", "path": ".", "max_entries": 20})
    assert parsed.name == "list_files"
    assert "name" not in schemas[0]["parameters"]["properties"]
    assert "default" not in schemas[0]["parameters"]["properties"]["path"]
    assert "default" not in schemas[2]["parameters"]["properties"]["path_prefix"]
    schemas[0]["description"] = "caller mutation"
    assert fixed_model_tool_schemas()[0]["description"] != "caller mutation"


def test_candidate_validation_continuation_is_fixed_and_revision_bound() -> None:
    step = AgentActivityStep("agent_turn", "agent-turn:sample", 7)

    work = candidate_validation_next_work(cast(ExecutionLease, None), step)

    assert work == NextWork(
        logical_key="candidate-validation:r7",
        activity="candidate_validation",
        revision=7,
        replay_policy=ReplayPolicy.REPLAY_SAFE,
    )
    with pytest.raises(ValueError, match="agent_candidate_validation_work_invalid"):
        candidate_validation_next_work(
            cast(ExecutionLease, None), AgentActivityStep("agent_tool", "bad", 7)
        )


def _registry(*, evaluation: bool = False, queue=None) -> ActivityRunner:
    principal = Principal(uuid4())

    return build_workflow_activity_runner(
        cast(Engine, object()),
        principal,
        checkpoints=cast(CheckpointStorageService, object()),
        fetch_source=cast(SourceFetcher, lambda *_: ()),
        agent_service=cast(DurableAgentLoopService, object()),
        model_adapter=object(),
        resolve_tool_policy_context=cast(PolicyResolver, lambda *_: None),
        resolve_validation_policy_context=cast(PolicyContextResolver, lambda *_: None),
        validation=cast(ValidationService, object()),
        approvals=cast(ApprovalService, object()),
        transitions=cast(TransitionService, object()),
        branch_publication=cast(GitHubBranchPublicationService, object()),
        pull_request_publication=cast(GitHubPullRequestService, object()),
        lease_owner="unit-test",
        evaluation=evaluation,
        queue=queue,
    )


def test_registry_composes_only_the_fixed_workflow_handlers() -> None:
    runner = _registry()
    assert isinstance(runner, ActivityRunner)
    assert set(runner._handlers) == {
        "source_preparation",
        "agent_turn",
        "agent_tool",
        "candidate_validation",
        "approval_freeze",
        "publish_approved",
    }
    assert runner._handlers["agent_turn"] is runner._handlers["agent_tool"]


def test_evaluation_registry_does_not_register_publication_dispatch() -> None:
    runner = build_workflow_activity_runner(
        cast(Engine, object()),
        Principal(uuid4()),
        checkpoints=cast(CheckpointStorageService, object()),
        fetch_source=cast(SourceFetcher, lambda *_: ()),
        agent_service=cast(DurableAgentLoopService, object()),
        model_adapter=object(),
        resolve_tool_policy_context=cast(PolicyResolver, lambda *_: None),
        resolve_validation_policy_context=cast(PolicyContextResolver, lambda *_: None),
        validation=cast(ValidationService, object()),
        approvals=cast(ApprovalService, object()),
        transitions=cast(TransitionService, object()),
        branch_publication=cast(GitHubBranchPublicationService, object()),
        pull_request_publication=cast(GitHubPullRequestService, object()),
        lease_owner="evaluation-test",
        evaluation=True,
    )

    assert "publish_approved" not in runner._handlers


def test_evaluation_refuses_publication_before_claim_even_if_approval_arrives(monkeypatch) -> None:
    class Queue:
        def claim_one(self, *_args, **_kwargs):
            pytest.fail("evaluation must not claim publication work")

    runner = _registry(evaluation=True, queue=Queue())
    monkeypatch.setattr(runner, "_next_due_activity", lambda _run: ("publish_approved", False))
    with pytest.raises(UnknownActivity, match="runtime_activity_unregistered:publish_approved"):
        asyncio.run(runner.run_admitted(uuid4(), max_jobs=1, max_seconds=1))


def test_hidden_check_dependencies_are_evaluation_only_and_paired() -> None:
    hidden = cast(ManagedHiddenCheckService, object())
    base = {
        "engine": cast(Engine, object()),
        "principal": Principal(uuid4()),
        "checkpoints": cast(CheckpointStorageService, object()),
        "fetch_source": cast(SourceFetcher, lambda *_: ()),
        "agent_service": cast(DurableAgentLoopService, object()),
        "model_adapter": object(),
        "resolve_tool_policy_context": cast(PolicyResolver, lambda *_: None),
        "resolve_validation_policy_context": cast(PolicyContextResolver, lambda *_: None),
        "validation": cast(ValidationService, object()),
        "approvals": cast(ApprovalService, object()),
        "transitions": cast(TransitionService, object()),
        "branch_publication": cast(GitHubBranchPublicationService, object()),
        "pull_request_publication": cast(GitHubPullRequestService, object()),
        "lease_owner": "hidden-check-test",
    }
    with pytest.raises(ValueError, match="workflow_hidden_dependencies_unpaired"):
        build_workflow_activity_runner(**base, evaluation=True, hidden_checks=hidden)
    with pytest.raises(ValueError, match="workflow_hidden_checks_evaluation_only"):
        build_workflow_activity_runner(
            **base,
            evaluation=False,
            hidden_checks=hidden,
            hidden_check_selector=lambda *_: None,
        )
