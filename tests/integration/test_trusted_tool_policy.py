"""PostgreSQL integration for persisted source-tool authorization composition."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_agent_loop import T22Database, _checkpoint_service, _storage_fixture
from test_agent_loop import t22_database as t22_database
from test_source_preparation import _PYTHON_FILES, _receipt, _start_source_run

from continuum_api.artifacts.archives import ArchiveFile
from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.repositories.preflight import RepositoryAccessProof
from continuum_api.runtime import agent_activity
from continuum_api.runtime.admitted_agent_activity import AdmittedAgentTurnInputResolver
from continuum_api.runtime.agent_activity import AgentActivityStep
from continuum_api.runtime.agent_loop import AgentLoopError
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import ExecutionLease, QueueService
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus
from continuum_api.runtime.source_preparation import SourcePreparationActivityHandler
from continuum_api.runtime.transitions import NextWork
from continuum_api.runtime.trusted_tool_policy import PersistedToolPolicyContextResolver
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import authorize_tool_call, check_tool_context_gate


class _AccessVerifier:
    def __init__(
        self,
        *,
        actor: bool = True,
        app: bool = True,
        repository_id: int | None = None,
        github_user_id: int | None = None,
        on_verify=None,
    ) -> None:
        self.actor = actor
        self.app = app
        self.repository_id = repository_id
        self.github_user_id = github_user_id
        self.on_verify = on_verify
        self.calls: list[tuple[int, int]] = []

    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
        self.calls.append((repository_id, github_user_id))
        if self.on_verify is not None:
            self.on_verify()
        return RepositoryAccessProof(
            self.repository_id or repository_id,
            self.github_user_id or github_user_id,
            self.actor,
            self.app,
        )


def _prepare_tool_step(
    database: T22Database, tmp_path: Path
) -> tuple[Principal, ExecutionLease, AgentActivityStep, CheckpointStorageService]:
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
    lease = QueueService(database.runtime_engine, principal).claim_one(
        "t34-tool-policy-test", run_id=source_lease.run_id
    )
    assert lease is not None
    step = agent_activity._read_claimed_step(database.runtime_engine, principal, lease)
    admitted = AdmittedAgentTurnInputResolver(
        database.runtime_engine, principal, checkpoints, tool_schemas=()
    )
    asyncio.run(admitted(lease, step))
    logical_key = f"agent-tool:{uuid4().hex}:0"
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.steps SET activity='agent_tool', logical_key=:key "
                "WHERE owner_id=:owner AND run_id=:run AND step_id=:step"
            ),
            {
                "key": logical_key,
                "owner": principal.account_id,
                "run": lease.run_id,
                "step": lease.step_id,
            },
        )
    return principal, lease, AgentActivityStep("agent_tool", logical_key, 0), checkpoints


def test_persisted_policy_composes_admitted_baseline_and_current_authorization(
    t22_database: T22Database, tmp_path: Path
) -> None:
    principal, lease, step, checkpoints = _prepare_tool_step(t22_database, tmp_path)
    verifier = _AccessVerifier()
    resolver = PersistedToolPolicyContextResolver(
        t22_database.runtime_engine, principal, checkpoints, verifier
    )

    context = asyncio.run(resolver(lease, step))

    assert context.capabilities == frozenset(
        {
            ToolCapability.LIST_FILES,
            ToolCapability.READ_FILE,
            ToolCapability.SEARCH_TEXT,
            ToolCapability.APPLY_PATCH,
            ToolCapability.DIFF,
        }
    )
    assert context.profile_id == "python-pytest"
    assert context.stage is ToolStage.AGENT_WORK
    assert context.baseline_paths_verified
    assert "tests/test_app.py" in context.baseline_test_paths
    assert "pyproject.toml" in context.protected_paths
    assert context.run_budget_available
    assert verifier.calls and verifier.calls[0][0] > 0
    assert check_tool_context_gate(
        {
            "name": "apply_patch",
            "edits": [
                {
                    "path": "src/app.py",
                    "expected_text": "42",
                    "replacement_text": "43",
                }
            ],
        },
        context,
    ).allowed
    protected_test = authorize_tool_call(
        {
            "name": "apply_patch",
            "edits": [
                {
                    "path": "tests/test_app.py",
                    "expected_text": "42",
                    "replacement_text": "43",
                }
            ],
        },
        context,
        context.baseline_source_paths,
        {path: 20 for path in context.baseline_source_paths},
    )
    assert not protected_test.allowed and protected_test.code == "tool_protected_path_denied"


def test_validation_callback_resolves_actual_persisted_candidate_step(
    t22_database: T22Database, tmp_path: Path
) -> None:
    principal, lease, _tool_step, checkpoints = _prepare_tool_step(t22_database, tmp_path)
    receipt = _receipt(_PYTHON_FILES, "python-pytest")
    candidate = tuple(
        ArchiveFile(
            item.path,
            item.content.replace(b"return 42", b"return 43")
            if item.path == "src/app.py"
            else item.content,
        )
        for item in _PYTHON_FILES
    )
    asyncio.run(
        checkpoints.commit_checkpoint(
            principal,
            lease,
            "candidate:policy-callback",
            candidate,
            result={"activity": "agent_tool"},
            target_run_status=RunStatus.RUNNING,
            next_work=NextWork(
                logical_key="candidate-validation:0",
                activity="candidate_validation",
                revision=0,
                replay_policy=ReplayPolicy.REPLAY_SAFE,
            ),
        ),
    )
    validation_lease = QueueService(t22_database.runtime_engine, principal).claim_one(
        "t34-tool-policy-validation", run_id=lease.run_id
    )
    assert validation_lease is not None
    resolver = PersistedToolPolicyContextResolver(
        t22_database.runtime_engine, principal, checkpoints, _AccessVerifier()
    )
    binding = resolver._candidate_inputs._read_binding(validation_lease)
    baseline = asyncio.run(
        checkpoints.restore_archive(principal, validation_lease, binding["baseline_artifact_id"])
    )

    context = asyncio.run(resolver.for_validation(validation_lease, receipt, baseline))

    assert context.stage is ToolStage.AGENT_WORK
    assert context.profile_id == receipt.profile_id
    assert context.baseline_paths_verified
    assert context.baseline_test_paths == frozenset({"tests/test_app.py", "pyproject.toml"})


def test_persisted_policy_fails_closed_for_owner_mismatch_and_revoked_access(
    t22_database: T22Database, tmp_path: Path
) -> None:
    principal, lease, step, checkpoints = _prepare_tool_step(t22_database, tmp_path)
    resolver = PersistedToolPolicyContextResolver(
        t22_database.runtime_engine, principal, checkpoints, _AccessVerifier(actor=False)
    )
    context = asyncio.run(resolver(lease, step))
    denied = check_tool_context_gate({"name": "read_file", "path": "module.py"}, context)
    assert not denied.allowed and denied.code == "tool_repository_access_denied"

    wrong_owner_resolver = PersistedToolPolicyContextResolver(
        t22_database.runtime_engine,
        Principal(uuid4()),
        checkpoints,
        _AccessVerifier(),
    )
    with pytest.raises(AgentLoopError, match="agent_tool_policy_owner_mismatch"):
        asyncio.run(wrong_owner_resolver(lease, step))


def test_persisted_policy_rejects_pin_mismatch_and_model_budget_overrun(
    t22_database: T22Database, tmp_path: Path
) -> None:
    principal, lease, step, checkpoints = _prepare_tool_step(t22_database, tmp_path)
    resolver = PersistedToolPolicyContextResolver(
        t22_database.runtime_engine, principal, checkpoints, _AccessVerifier()
    )

    with t22_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.validation_source_pins SET eligibility_sha256=:digest "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"digest": b"x" * 32, "owner": principal.account_id, "run": lease.run_id},
        )
    with pytest.raises(AgentLoopError, match="agent_tool_policy_pins_invalid"):
        asyncio.run(resolver(lease, step))

    # A separate admitted run exercises the persisted budget gate.
    principal, lease, step, checkpoints = _prepare_tool_step(t22_database, tmp_path)
    resolver = PersistedToolPolicyContextResolver(
        t22_database.runtime_engine, principal, checkpoints, _AccessVerifier()
    )
    with t22_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET model_budget_exceeded=true "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": principal.account_id, "run": lease.run_id},
        )
    context = asyncio.run(resolver(lease, step))
    assert not context.run_budget_available
    denied = check_tool_context_gate({"name": "read_file", "path": "module.py"}, context)
    assert not denied.allowed and denied.code == "tool_run_budget_exhausted"


def test_persisted_policy_rejects_a_stale_run_revision(
    t22_database: T22Database, tmp_path: Path
) -> None:
    principal, lease, step, checkpoints = _prepare_tool_step(t22_database, tmp_path)
    resolver = PersistedToolPolicyContextResolver(
        t22_database.runtime_engine, principal, checkpoints, _AccessVerifier()
    )
    with t22_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET revision=revision+1 "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": principal.account_id, "run": lease.run_id},
        )
    with pytest.raises(AgentLoopError, match="agent_tool_policy_step_stale"):
        asyncio.run(resolver(lease, step))


def test_persisted_policy_rejects_mismatched_fresh_proof_identity(
    t22_database: T22Database, tmp_path: Path
) -> None:
    principal, lease, step, checkpoints = _prepare_tool_step(t22_database, tmp_path)
    resolver = PersistedToolPolicyContextResolver(
        t22_database.runtime_engine,
        principal,
        checkpoints,
        _AccessVerifier(repository_id=99_999),
    )
    with pytest.raises(AgentLoopError, match="agent_tool_policy_authorization_invalid"):
        asyncio.run(resolver(lease, step))


def test_persisted_policy_refences_lease_after_authorization_returns(
    t22_database: T22Database, tmp_path: Path
) -> None:
    principal, lease, step, checkpoints = _prepare_tool_step(t22_database, tmp_path)

    def expire_lease() -> None:
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.jobs SET lease_expires_at=clock_timestamp() "
                    "- interval '1 second' WHERE owner_id=:owner AND run_id=:run "
                    "AND job_id=:job"
                ),
                {
                    "owner": lease.owner_id,
                    "run": lease.run_id,
                    "job": lease.job_id,
                },
            )

    resolver = PersistedToolPolicyContextResolver(
        t22_database.runtime_engine,
        principal,
        checkpoints,
        _AccessVerifier(on_verify=expire_lease),
    )
    with pytest.raises(AgentLoopError, match="agent_lease_lost"):
        asyncio.run(resolver(lease, step))
