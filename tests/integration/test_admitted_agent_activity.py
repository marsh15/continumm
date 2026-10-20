"""PostgreSQL checks for admission-bound first agent-turn initialization."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from test_agent_loop import (
    T22Database,
    _checkpoint_service,
    _storage_fixture,
)
from test_agent_loop import t22_database as t22_database
from test_source_preparation import (
    _PYTHON_FILES,
    _REPOSITORY_ID,
    _receipt,
    _start_source_run,
)

from continuum_api.adapters.openai import OpenAIModelAdapter
from continuum_api.artifacts.archives import ArchiveFile
from continuum_api.credentials import CredentialCipher, CredentialProvider, CredentialScope
from continuum_api.repositories.preflight import EligibilityReceipt, RepositoryAccessProof
from continuum_api.runtime import agent_activity
from continuum_api.runtime.admitted_agent_activity import AdmittedAgentTurnInputResolver
from continuum_api.runtime.agent_loop import (
    OPENAI_MODEL_ID,
    AgentLoopError,
    DurableAgentLoopService,
)
from continuum_api.runtime.approvals import ApprovalService
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.github_pr_publication import GitHubPullRequestService
from continuum_api.runtime.github_publication import GitHubBranchPublicationService
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.persistence import (
    Principal,
    ReplayPolicy,
    RuntimeRepository,
)
from continuum_api.runtime.provider_credentials import CredentialService
from continuum_api.runtime.source_preparation import SourcePreparationActivityHandler
from continuum_api.runtime.transitions import TransitionService
from continuum_api.runtime.trusted_tool_policy import PersistedToolPolicyContextResolver
from continuum_api.runtime.workflow_registry import build_workflow_activity_runner
from continuum_api.runtime.workspace_recovery import WorkspaceRecoveryService
from continuum_api.validation.service import ValidationService


class _WorkflowAccessVerifier:
    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
        return RepositoryAccessProof(repository_id, github_user_id, True, True)


def _create_unclaimed_admitted_run(
    database: T22Database,
    files: tuple[ArchiveFile, ...],
    receipt: EligibilityReceipt,
) -> tuple[Principal, UUID]:
    """Create the same pinned run as _start_source_run without claiming its first job."""
    owner_id = uuid4()
    principal = Principal(owner_id)
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) VALUES (:id, :github)"
            ),
            {"id": owner_id, "github": 990_000_000 + owner_id.int % 1_000_000},
        )
        registration_id: int = connection.execute(
            text(
                "INSERT INTO runtime_private.repository_registrations "
                "(owner_id, github_repository_id, repository_owner, repository_name, "
                "profile_id, profile_version, provider, eligibility_status, checked_sha, "
                "eligibility_checked_at, eligibility_receipt) VALUES "
                "(:owner, :repo, 'fixture-owner', 'fixture-repository', :profile, '1', "
                "'openai', 'eligible', :sha, clock_timestamp(), CAST(:receipt AS jsonb)) "
                "RETURNING registration_id"
            ),
            {
                "owner": owner_id,
                "repo": _REPOSITORY_ID,
                "profile": receipt.profile_id,
                "sha": receipt.commit_sha,
                "receipt": json.dumps(receipt.to_cache_payload()),
            },
        ).scalar_one()
    with database.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run(github_repository_id=_REPOSITORY_ID, issue_number=1)
        step_id = repository.create_step(run_id, "admission.source", "source_preparation")
        repository.create_job(run_id, step_id, replay_policy=ReplayPolicy.ATTENTION_ON_UNKNOWN)
    with database.runtime_engine.begin() as connection:
        RuntimeRepository(connection, principal)
        connection.execute(
            text(
                "INSERT INTO runtime_private.run_admission_pins "
                "(owner_id, run_id, registration_id, defaults_version, profile_id, "
                "profile_version, base_branch, source_sha, source_tree_sha, source_receipt, "
                "provider, model_id, task_context, submission_digest) VALUES "
                "(:owner, :run, :registration, 1, :profile, '1', 'main', :sha, :tree, "
                "CAST(:receipt AS jsonb), 'openai', :model, CAST(:context AS jsonb), :digest)"
            ),
            {
                "owner": owner_id,
                "run": run_id,
                "registration": registration_id,
                "profile": receipt.profile_id,
                "sha": receipt.commit_sha,
                "tree": receipt.tree_sha,
                "receipt": json.dumps(receipt.to_cache_payload()),
                "model": OPENAI_MODEL_ID,
                "context": json.dumps({"task": "Implement the requested change."}),
                "digest": hashlib.sha256(str(run_id).encode()).digest(),
            },
        )
    return principal, run_id


def test_workflow_registry_runs_admission_turn_and_patched_tool_with_fake_provider(
    t22_database: T22Database, tmp_path: Path
) -> None:
    files = _PYTHON_FILES
    receipt = _receipt(files, "python-pytest")
    principal, run_id = _create_unclaimed_admitted_run(t22_database, files, receipt)
    storage, storage_requests = _storage_fixture(tmp_path / str(run_id))
    persistence = ArtifactPersistence(t22_database.runtime_engine)
    checkpoints = _checkpoint_service(t22_database.runtime_engine, principal, persistence, storage)
    transitions = TransitionService(
        t22_database.runtime_engine, principal, artifact_persistence=persistence
    )
    credentials = CredentialService(
        t22_database.runtime_engine,
        principal,
        CredentialCipher("fixture-v1", {"fixture-v1": bytes.fromhex("11" * 32)}),
    )
    credential = credentials.save(
        CredentialProvider.OPENAI,
        SecretStr("synthetic-workflow-provider-key"),
        scope=CredentialScope.RUN,
        run_id=run_id,
    )
    credentials.bind_run(run_id, CredentialProvider.OPENAI, credential.credential_id)
    workspace_recovery = WorkspaceRecoveryService(
        t22_database.runtime_engine, principal, checkpoints, transitions
    )
    agent_service = DurableAgentLoopService(
        t22_database.runtime_engine,
        principal,
        transitions,
        workspace_recovery=workspace_recovery,
        checkpoints=checkpoints,
        credential_service=credentials,
    )
    sent: list[dict[str, Any]] = []

    class FakeProviderClient:
        @property
        def responses(self) -> FakeProviderClient:
            return self

        async def create(self, **kwargs: Any) -> Any:
            sent.append(kwargs)
            return SimpleNamespace(
                id="resp_workflow_patch",
                model=OPENAI_MODEL_ID,
                status="completed",
                output=[
                    SimpleNamespace(
                        type="function_call",
                        call_id="call_workflow_patch",
                        name="apply_patch",
                        arguments=json.dumps(
                            {
                                "edits": [
                                    {
                                        "path": "src/app.py",
                                        "expected_text": "return 42",
                                        "replacement_text": "return 43",
                                    }
                                ]
                            }
                        ),
                    )
                ],
                usage=SimpleNamespace(input_tokens=20, output_tokens=5),
                _request_id="req_workflow_patch",
            )

        async def close(self) -> None:
            return None

    def client_factory(**kwargs: Any) -> FakeProviderClient:
        assert kwargs["api_key"] == "synthetic-workflow-provider-key"
        assert kwargs["max_retries"] == 0
        return FakeProviderClient()

    policy = PersistedToolPolicyContextResolver(
        t22_database.runtime_engine,
        principal,
        checkpoints,
        _WorkflowAccessVerifier(),
    )
    approvals = ApprovalService(t22_database.runtime_engine, principal, transitions, None)
    runner = build_workflow_activity_runner(
        t22_database.runtime_engine,
        principal,
        checkpoints=checkpoints,
        fetch_source=lambda _lease, _repository_id, _receipt: files,
        agent_service=agent_service,
        model_adapter=OpenAIModelAdapter(client_factory=client_factory),
        resolve_tool_policy_context=policy,
        resolve_validation_policy_context=policy.for_validation,
        validation=cast(ValidationService, object()),
        approvals=approvals,
        transitions=transitions,
        branch_publication=cast(GitHubBranchPublicationService, object()),
        pull_request_publication=cast(GitHubPullRequestService, object()),
        lease_owner="workflow-registry-integration",
    )

    try:
        receipt_run = asyncio.run(runner.run_admitted(run_id, max_jobs=3, max_seconds=60))
        assert receipt_run.jobs_started == 3
        assert receipt_run.stop_reason.value == "job_limit"
        assert len(sent) == 1
        assert sent[0]["tools"][0]["name"] == "list_files"
        assert sent[0]["tools"][-2]["name"] == "apply_patch"
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            accepted = (
                connection.execute(
                    text(
                        "SELECT step.activity, accepted.artifact_id, accepted.payload "
                        "FROM runtime_private.accepted_results AS accepted JOIN "
                        "runtime_private.steps AS step ON step.owner_id=accepted.owner_id "
                        "AND step.run_id=accepted.run_id AND step.step_id=accepted.step_id "
                        "WHERE accepted.owner_id=:owner AND accepted.run_id=:run "
                        "ORDER BY accepted.event_sequence"
                    ),
                    {"owner": principal.account_id, "run": run_id},
                )
                .mappings()
                .all()
            )
            budget = (
                connection.execute(
                    text(
                        "SELECT model_id, model_requests_reserved, model_requests_authorized, "
                        "model_requests_received FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": principal.account_id, "run": run_id},
                )
                .mappings()
                .one()
            )
            turn = (
                connection.execute(
                    text(
                        "SELECT status, provider_request_id FROM "
                        "runtime_private.agent_turns WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": principal.account_id, "run": run_id},
                )
                .mappings()
                .one()
            )
        assert [row["activity"] for row in accepted] == [
            "source_preparation",
            "agent_turn",
            "agent_tool",
        ]
        candidate_id = accepted[-1]["artifact_id"]
        assert candidate_id is not None
        assert accepted[-1]["payload"]["tool_result"]["name"] == "apply_patch"
        assert budget == {
            "model_id": OPENAI_MODEL_ID,
            "model_requests_reserved": 1,
            "model_requests_authorized": 1,
            "model_requests_received": 1,
        }
        assert turn["status"] == "response_accepted"
        assert turn["provider_request_id"] == "req_workflow_patch"

        # Claim the queued next turn only to read the candidate under a current lease;
        # the runner has stopped before making a second provider request.
        next_turn = QueueService(t22_database.runtime_engine, principal).claim_one(
            "workflow-registry-read-candidate", run_id=run_id
        )
        assert next_turn is not None
        candidate = asyncio.run(checkpoints.restore_archive(principal, next_turn, candidate_id))
        content_by_path = {item.path: item.content for item in candidate.files}
        assert content_by_path["src/app.py"] == b"def answer():\n    return 43\n"
        assert content_by_path["tests/test_app.py"] == next(
            item.content for item in files if item.path == "tests/test_app.py"
        )
        assert len(sent) == 1
        assert storage_requests
    finally:
        try:
            asyncio.run(storage.aclose())
        finally:
            with t22_database.admin_engine.begin() as connection:
                connection.execute(
                    text(
                        "DELETE FROM runtime_private.agent_invocations "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": principal.account_id, "run": run_id},
                )
                connection.execute(
                    text(
                        "DELETE FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": principal.account_id, "run": run_id},
                )


def _prepared_first_turn(database: T22Database, tmp_path: Path):
    receipt = _receipt(_PYTHON_FILES, "python-pytest")
    principal, source_lease = _start_source_run(database, _PYTHON_FILES, "python-pytest", receipt)
    storage, _requests = _storage_fixture(tmp_path / str(source_lease.run_id))
    checkpoints = _checkpoint_service(
        database.runtime_engine,
        principal,
        ArtifactPersistence(database.runtime_engine),
        storage,
    )
    preparation = SourcePreparationActivityHandler(
        database.runtime_engine,
        principal,
        checkpoints,
        lambda _lease, _repository_id, _receipt: _PYTHON_FILES,
    )
    asyncio.run(preparation(source_lease))
    turn_lease = QueueService(database.runtime_engine, principal).claim_one(
        "t34-admitted-agent-turn",
        run_id=source_lease.run_id,
    )
    assert turn_lease is not None
    step = agent_activity._read_claimed_step(database.runtime_engine, principal, turn_lease)
    resolver = AdmittedAgentTurnInputResolver(
        database.runtime_engine,
        principal,
        checkpoints,
        tool_schemas=(),
    )
    return principal, turn_lease, step, resolver, storage, checkpoints


def _queue_revision_one_turn(database: T22Database, principal: Principal, run_id):
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET revision=1 WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": principal.account_id, "run": run_id},
        )
    with database.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        step_id = repository.create_step(run_id, "agent-turn:1", "agent_turn", revision=1)
        job_id = repository.create_job(run_id, step_id, replay_policy=ReplayPolicy.REPLAY_SAFE)
    lease = QueueService(database.runtime_engine, principal).claim_one(
        "t34-admitted-revision-one", run_id=run_id
    )
    assert lease is not None and lease.job_id == job_id
    return lease, agent_activity._read_claimed_step(database.runtime_engine, principal, lease)


def test_admitted_first_turn_pins_source_and_model_inputs_with_partial_recovery(
    t22_database: T22Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    principal, lease, step, resolver, storage, _checkpoints = _prepared_first_turn(
        t22_database, tmp_path
    )
    assert step.activity == "agent_turn"
    assert step.logical_key == "agent-turn:0"
    assert step.revision == 0

    actual_initialize = resolver._resolver.initialize
    failed_once = False

    async def interrupt_after_source_pin(*args, **kwargs):
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise AgentLoopError("simulated_initialization_interruption")
        await actual_initialize(*args, **kwargs)

    monkeypatch.setattr(resolver._resolver, "initialize", interrupt_after_source_pin)
    try:
        with pytest.raises(AgentLoopError, match="simulated_initialization_interruption"):
            asyncio.run(resolver(lease, step))
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            state = (
                connection.execute(
                    text(
                        "SELECT (SELECT count(*) FROM runtime_private.validation_source_pins "
                        "WHERE owner_id=:owner AND run_id=:run) AS source_pins, "
                        "(SELECT count(*) FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner AND run_id=:run) AS turns, "
                        "(SELECT model_id IS NULL AND model_task_context IS NULL "
                        "AND model_tool_schema_sha256 IS NULL AND model_requests_reserved=0 "
                        "FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run) AS model_uninitialized"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one()
            )
        assert state["source_pins"] == 1
        assert state["turns"] == 0
        assert state["model_uninitialized"] is True

        inputs = asyncio.run(resolver(lease, step))
        assert inputs.initial_context == {"task": "Implement the requested change."}
        assert inputs.model_id == "gpt-5.4-mini-2026-03-17"
        assert inputs.baseline_artifact_id is not None
        replayed = asyncio.run(resolver(lease, step))
        assert replayed == inputs
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            state = (
                connection.execute(
                    text(
                        "SELECT (SELECT count(*) FROM runtime_private.validation_source_pins "
                        "WHERE owner_id=:owner AND run_id=:run) AS source_pins, "
                        "(SELECT count(*) FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner AND run_id=:run) AS turns, "
                        "(SELECT model_id FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run) AS pinned_model, "
                        "(SELECT model_requests_reserved FROM runtime_private.runs "
                        "WHERE owner_id=:owner AND run_id=:run) AS requests_reserved"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one()
            )
        assert state["source_pins"] == 1
        assert state["turns"] == 0
        assert state["pinned_model"] == inputs.model_id
        assert state["requests_reserved"] == 0

        later_lease, later_step = _queue_revision_one_turn(t22_database, principal, lease.run_id)
        assert later_step.logical_key == "agent-turn:1"
        assert asyncio.run(resolver(later_lease, later_step)) == inputs

        wrong_owner = AdmittedAgentTurnInputResolver(
            t22_database.runtime_engine,
            Principal(uuid4()),
            _checkpoints,
            tool_schemas=(),
        )
        with pytest.raises(AgentLoopError, match="agent_lease_owner_mismatch"):
            asyncio.run(wrong_owner(lease, step))
        with pytest.raises(AgentLoopError, match="agent_lease_lost"):
            asyncio.run(resolver(replace(lease, generation=lease.generation + 1), step))
    finally:
        asyncio.run(storage.aclose())


def test_admitted_first_turn_rejects_corrupt_source_result_binding(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    principal, lease, step, resolver, storage, _checkpoints = _prepared_first_turn(
        t22_database, tmp_path
    )
    try:
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE runtime_private.accepted_results DISABLE TRIGGER USER")
            )
            connection.execute(
                text(
                    "UPDATE runtime_private.accepted_results SET payload="
                    "jsonb_set(payload, '{commit_sha}', to_jsonb('wrong'::text)) "
                    "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            )
            connection.execute(
                text("ALTER TABLE runtime_private.accepted_results ENABLE TRIGGER USER")
            )
        with pytest.raises(AgentLoopError, match="agent_admission_binding_invalid"):
            asyncio.run(resolver(lease, step))
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.validation_source_pins "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                ).scalar_one()
                == 0
            )
    finally:
        asyncio.run(storage.aclose())


def test_later_turn_does_not_initialize_missing_admission_model_pins(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    principal, lease, _step, resolver, storage, _checkpoints = _prepared_first_turn(
        t22_database, tmp_path
    )
    try:
        later_lease, later_step = _queue_revision_one_turn(t22_database, principal, lease.run_id)
        with pytest.raises(AgentLoopError, match="agent_turn_inputs_unpinned"):
            asyncio.run(resolver(later_lease, later_step))
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            state = connection.execute(
                text(
                    "SELECT model_id IS NULL AND model_task_context IS NULL "
                    "AND model_tool_schema_sha256 IS NULL FROM runtime_private.runs "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one()
            source_pins = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.validation_source_pins "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one()
        assert state is True
        assert source_pins == 0
    finally:
        asyncio.run(storage.aclose())


def test_admitted_first_turn_rejects_provider_model_mismatch(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    principal, lease, step, resolver, storage, _checkpoints = _prepared_first_turn(
        t22_database, tmp_path
    )
    try:
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE runtime_private.run_admission_pins DISABLE TRIGGER USER")
            )
            connection.execute(
                text(
                    "UPDATE runtime_private.run_admission_pins SET provider='anthropic' "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            )
            connection.execute(
                text("ALTER TABLE runtime_private.run_admission_pins ENABLE TRIGGER USER")
            )
        with pytest.raises(AgentLoopError, match="agent_admission_binding_invalid"):
            asyncio.run(resolver(lease, step))
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.validation_source_pins "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                ).scalar_one()
                == 0
            )
    finally:
        asyncio.run(storage.aclose())
