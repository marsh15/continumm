"""PostgreSQL proof for exact publication approval and deadline-bound dispatch."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from test_agent_loop import (
    T22Database,
    T22Run,
    _checkpoint_service,
    _seed_agent_run,
    _storage_fixture,
)
from test_agent_loop import t22_database as t22_database

from continuum_api.approvals.manifest import digest_manifest
from continuum_api.artifacts.archives import ArchiveFile, create_archive, verify_archive
from continuum_api.connectivity.storage import SupabaseStorage
from continuum_api.repositories.preflight import PYTHON_TARGET_VERSION, RepositoryAccessProof
from continuum_api.runtime.approvals import (
    ApprovalError,
    ApprovalReceipt,
    ApprovalService,
    RepositoryPublicationFacts,
)
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import ExecutionLease, QueueService
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus, RuntimeRepository
from continuum_api.runtime.transitions import NextWork, TransitionReceipt, TransitionService
from continuum_api.runtime.workspace_recovery import WorkspaceRecoveryService
from continuum_api.tools.source_tools import (
    candidate_from_baseline,
    snapshot_from_verified_archive,
)
from continuum_api.validation.contracts import (
    CommandEvidence,
    PhaseEvidence,
    PhaseOutcome,
    ValidationClassification,
    ValidationEvidence,
    ValidationPhase,
)

_REPOSITORY_ID = 714_022
_BASE_SHA = "a" * 40
_FACTS_BASE_SHA = _BASE_SHA
_FIXTURE_LINEAGE_SHA256 = hashlib.sha256(b"t27-synthetic-validation-lineage").hexdigest()
_T27_PERIOD = "t27-approval-fixture"


@dataclass(slots=True)
class _FactsProvider:
    facts: RepositoryPublicationFacts
    calls: int = 0

    async def current(self, repository_id: int, github_user_id: int) -> RepositoryPublicationFacts:
        self.calls += 1
        if self.facts.repository_id != repository_id or self.facts.github_user_id != github_user_id:
            raise ApprovalError("repository_access_denied")
        return self.facts


@dataclass(slots=True)
class ApprovalScenario:
    store: T22Database
    run: T22Run
    principal: Principal
    service: ApprovalService
    queue: QueueService
    transitions: TransitionService
    storage: SupabaseStorage
    objects: Path
    facts_provider: _FactsProvider
    baseline_artifact_id: UUID
    candidate_artifact_id: UUID
    evidence: ValidationEvidence
    validation_receipt: TransitionReceipt
    validation_lease: ExecutionLease
    freeze_lease: ExecutionLease
    allocation_ids: tuple[UUID, UUID]
    frozen_receipt: ApprovalReceipt | None = None


def _publication_facts(
    repository_id: int,
    github_user_id: int,
    *,
    base_sha: str = _FACTS_BASE_SHA,
    can_write: bool = True,
) -> _FactsProvider:
    proof = RepositoryAccessProof(repository_id, github_user_id, can_write, True)
    return _FactsProvider(
        RepositoryPublicationFacts(
            repository_id,
            github_user_id,
            "fixture-owner",
            "fixture-project",
            "main",
            base_sha,
            proof,
        )
    )


def _validation_evidence(
    run: T22Run,
    baseline_artifact_id: UUID,
    baseline_archive_sha256: str,
    baseline_tree_sha256: str,
    candidate_archive_sha256: str,
    candidate_tree_sha256: str,
    allocation_ids: tuple[UUID, UUID],
    *,
    candidate_exit_code: int = 0,
    candidate_allocation_status: str = "stopped",
) -> ValidationEvidence:
    baseline_allocation, candidate_allocation = allocation_ids
    baseline_command = CommandEvidence(
        phase="pytest",
        exit_code=0,
        stdout_bytes=0,
        stderr_bytes=0,
        stdout_sha256="0" * 64,
        stderr_sha256="0" * 64,
    )
    candidate_command = CommandEvidence(
        phase="pytest",
        exit_code=candidate_exit_code,
        stdout_bytes=0,
        stderr_bytes=0,
        stdout_sha256="1" * 64,
        stderr_sha256="1" * 64,
    )
    shared = {
        "repository_id": _REPOSITORY_ID,
        "base_commit_sha": _BASE_SHA,
        "baseline_git_tree_sha256": "b" * 40,
        "profile_id": "python-pytest",
        "profile_version": "1",
        "image": "continuum-python@sha256:" + "f" * 64,
        "runtime_version": PYTHON_TARGET_VERSION,
        "command_id": "python-pytest-v1",
        "baseline_archive_sha256": baseline_archive_sha256,
        "baseline_archive_tree_sha256": baseline_tree_sha256,
        "package_count": 0,
        "allocation_status": "stopped",
    }
    baseline = PhaseEvidence(
        phase=ValidationPhase.BASELINE,
        outcome=PhaseOutcome.PASSED,
        allocation_id=baseline_allocation,
        artifact_id=baseline_artifact_id,
        source_archive_sha256=baseline_archive_sha256,
        source_tree_sha256=baseline_tree_sha256,
        test_exit_code=0,
        commands=(baseline_command,),
        **shared,
    )
    candidate = PhaseEvidence(
        phase=ValidationPhase.CANDIDATE,
        outcome=PhaseOutcome.PASSED,
        allocation_id=candidate_allocation,
        artifact_id=baseline_artifact_id,
        source_archive_sha256=candidate_archive_sha256,
        source_tree_sha256=candidate_tree_sha256,
        test_exit_code=candidate_exit_code,
        commands=(candidate_command,),
        allocation_status=candidate_allocation_status,
        **{key: value for key, value in shared.items() if key != "allocation_status"},
    )
    return ValidationEvidence(
        classification=ValidationClassification.PASSED,
        baseline_artifact_id=baseline_artifact_id,
        candidate_archive_sha256=candidate_archive_sha256,
        candidate_source_tree_sha256=candidate_tree_sha256,
        candidate_lineage_sha256=_FIXTURE_LINEAGE_SHA256,
        changed_path_count=1,
        baseline=baseline,
        candidate=candidate,
    )


def _seed_allocations(
    store: T22Database,
    run: T22Run,
    lease: ExecutionLease,
    allocation_ids: tuple[UUID, UUID],
    evidence: ValidationEvidence,
) -> None:
    now = datetime.now(UTC)
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.sandbox_provider_periods "
                "(provider_period_id, starts_at, ends_at, public_cpu_seconds_limit, "
                "operator_cpu_seconds_limit) VALUES (:period, :starts, :ends, 14400, 3600) "
                "ON CONFLICT (provider_period_id) DO NOTHING"
            ),
            {
                "period": _T27_PERIOD,
                "starts": now - timedelta(days=1),
                "ends": now + timedelta(days=3),
            },
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.sandbox_period_usage "
                "(provider_period_id, allocation_class, charged_cpu_seconds) "
                "VALUES (:period, 'public', 0) ON CONFLICT DO NOTHING"
            ),
            {"period": _T27_PERIOD},
        )
        for allocation_id, phase in zip(
            allocation_ids,
            (evidence.baseline.phase.value, evidence.candidate.phase.value),
            strict=True,
        ):
            connection.execute(
                text(
                    "INSERT INTO runtime_private.sandbox_allocations "
                    "(allocation_id, owner_id, run_id, step_id, job_id, generation, attempt_id, "
                    "lease_owner, logical_key, request_sha256, provider_name, provider_period_id, "
                    "allocation_class, requested_seconds, vcpus, memory_mb, reserved_cpu_seconds, "
                    "status, create_authorized_at, last_observed_at, last_sandbox_status) VALUES "
                    "(:id, :owner, :run, :step, :job, :generation, :attempt, :lease_owner, "
                    ":logical_key, :request_sha, :provider, :period, 'public', 120, 2, 4096, 240, "
                    "'stopped', :now, :now, 'stopped')"
                ),
                {
                    "id": allocation_id,
                    "owner": run.owner_id,
                    "run": run.run_id,
                    "step": lease.step_id,
                    "job": lease.job_id,
                    "generation": lease.generation,
                    "attempt": lease.attempt_id,
                    "lease_owner": lease.lease_owner,
                    "logical_key": f"t20:v1:{_FIXTURE_LINEAGE_SHA256}:{phase}",
                    "request_sha": bytes.fromhex(
                        hashlib.sha256(str(allocation_id).encode()).hexdigest()
                    ),
                    "provider": f"continuum-{allocation_id.hex}",
                    "period": _T27_PERIOD,
                    "now": now,
                },
            )


def create_approval_scenario(
    store: T22Database,
    tmp_path: Path,
    *,
    approval_ttl: timedelta | None = None,
    second_candidate: bool = False,
) -> ApprovalScenario:
    """Seed a real candidate checkpoint and durable T20 evidence, ready to freeze."""
    objects = tmp_path / f"approval-objects-{uuid4().hex}"
    run = _seed_agent_run(store, objects)
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET deadline_at=clock_timestamp()+interval '6 days' "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": run.owner_id, "run": run.run_id},
        )
    principal = run.principal
    queue = QueueService(store.runtime_engine, principal)
    persistence = ArtifactPersistence(store.runtime_engine)
    storage, _storage_requests = _storage_fixture(objects)
    checkpoints = _checkpoint_service(store.runtime_engine, principal, persistence, storage)
    transitions = TransitionService(
        store.runtime_engine, principal, artifact_persistence=persistence
    )
    workspace_recovery = WorkspaceRecoveryService(
        store.runtime_engine, principal, checkpoints, transitions
    )
    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, principal)
        github_user_id = int(
            connection.execute(
                text("SELECT github_user_id FROM runtime_private.users WHERE user_id=:owner"),
                {"owner": run.owner_id},
            ).scalar_one()
        )
    facts_provider = _publication_facts(_REPOSITORY_ID, github_user_id)
    service = ApprovalService(
        store.runtime_engine,
        principal,
        transitions,
        storage,
        publication_facts=facts_provider,
    )

    async def prepare() -> ApprovalScenario:
        with store.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.steps SET activity='agent_tool' WHERE step_id="
                    "(SELECT step_id FROM runtime_private.jobs WHERE job_id=:job)"
                ),
                {"job": run.turn_job_id},
            )
        candidate_lease = queue.claim_one("t27-candidate-checkpoint")
        assert candidate_lease is not None and candidate_lease.job_id == run.turn_job_id
        workspace = await workspace_recovery.restore_latest(principal, candidate_lease)
        candidate_files = tuple(
            ArchiveFile(
                item.path,
                item.content.replace(b"return 'base'", b"return 'reviewed'"),
                item.mode,
            )
            if item.path == "src/app.py"
            else item
            for item in workspace.verified.files
        )
        bundle = create_archive(candidate_files)
        verified = verify_archive(
            bundle.archive,
            expected_archive_sha256=bundle.archive_sha256,
            expected_tree_sha256=bundle.manifest.tree_sha256,
            expected_manifest=bundle.manifest,
        )
        candidate_source = snapshot_from_verified_archive(
            verified,
            expected_archive_sha256=bundle.archive_sha256,
            expected_tree_sha256=bundle.manifest.tree_sha256,
        )
        candidate = candidate_from_baseline(workspace.source, candidate_source)
        with store.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            candidate_step_revision = int(
                connection.execute(
                    text(
                        "SELECT revision FROM runtime_private.steps "
                        "WHERE owner_id=:owner AND run_id=:run AND step_id=:step"
                    ),
                    {"owner": run.owner_id, "run": run.run_id, "step": candidate_lease.step_id},
                ).scalar_one()
            )
        accepted = await workspace_recovery.accept_candidate(
            principal,
            candidate_lease,
            f"t27-candidate-{candidate_lease.generation}",
            workspace,
            candidate,
            result={
                "tool_name": "apply_patch",
                "tool_result": {
                    "name": "apply_patch",
                    "candidate_archive_sha256": candidate.source.archive_sha256,
                    "candidate_tree_sha256": candidate.source.tree_sha256,
                    "changed_paths": list(candidate.changed_paths),
                },
            },
            target_run_status=RunStatus.RUNNING,
            next_work=NextWork(
                "t27:candidate-validation",
                "agent_tool" if second_candidate else "candidate_validation",
                revision=candidate_step_revision,
                replay_policy=ReplayPolicy.REPLAY_SAFE,
            ),
        )
        if second_candidate:
            second_lease = queue.claim_one("t27-second-edit")
            assert second_lease is not None
            latest = await workspace_recovery.restore_latest(principal, second_lease)
            second_bundle = create_archive(
                (
                    *latest.verified.files,
                    ArchiveFile(
                        "src/second.py",
                        b"ANSWER = 42\n",
                        0o644,
                    ),
                )
            )
            second_verified = verify_archive(
                second_bundle.archive,
                expected_archive_sha256=second_bundle.archive_sha256,
                expected_tree_sha256=second_bundle.manifest.tree_sha256,
            )
            candidate = candidate_from_baseline(
                latest.source,
                snapshot_from_verified_archive(
                    second_verified,
                    expected_archive_sha256=second_bundle.archive_sha256,
                    expected_tree_sha256=second_bundle.manifest.tree_sha256,
                ),
            )
            accepted = await workspace_recovery.accept_candidate(
                principal,
                second_lease,
                "t27-second-candidate",
                latest,
                candidate,
                result={
                    "tool_name": "apply_patch",
                    "tool_result": {
                        "name": "apply_patch",
                        "candidate_archive_sha256": candidate.source.archive_sha256,
                        "candidate_tree_sha256": candidate.source.tree_sha256,
                        "changed_paths": ["src/app.py", "src/second.py"],
                    },
                },
                target_run_status=RunStatus.RUNNING,
                next_work=NextWork(
                    "t27:final-validation",
                    "candidate_validation",
                    revision=candidate_step_revision,
                    replay_policy=ReplayPolicy.REPLAY_SAFE,
                ),
            )
        validation_lease = queue.claim_one("t27-validation")
        assert validation_lease is not None
        allocation_ids = (uuid4(), uuid4())
        baseline_archive_sha256 = workspace.source.archive_sha256
        baseline_tree_sha256 = workspace.source.tree_sha256
        evidence = _validation_evidence(
            run,
            run.base_artifact_id,
            baseline_archive_sha256,
            baseline_tree_sha256,
            candidate.source.archive_sha256,
            candidate.source.tree_sha256,
            allocation_ids,
        )
        if second_candidate:
            evidence = evidence.model_copy(update={"changed_path_count": 2})
        _seed_allocations(store, run, candidate_lease, allocation_ids, evidence)
        validation_receipt = service.record_validation(
            validation_lease, accepted.artifact_id, evidence
        )
        freeze_lease = queue.claim_one("t27-freeze")
        assert freeze_lease is not None and freeze_lease.job_id == validation_receipt.next_job_id
        if approval_ttl is not None:
            with store.admin_engine.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE runtime_private.runs SET deadline_at=clock_timestamp()+:ttl "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"ttl": approval_ttl, "owner": run.owner_id, "run": run.run_id},
                )
        return ApprovalScenario(
            store=store,
            run=run,
            principal=principal,
            service=service,
            queue=queue,
            transitions=transitions,
            storage=storage,
            objects=objects,
            facts_provider=facts_provider,
            baseline_artifact_id=run.base_artifact_id,
            candidate_artifact_id=accepted.artifact_id,
            evidence=evidence,
            validation_receipt=validation_receipt,
            validation_lease=validation_lease,
            freeze_lease=freeze_lease,
            allocation_ids=allocation_ids,
        )

    try:
        return asyncio.run(prepare())
    except BaseException:
        asyncio.run(storage.aclose())
        raise


def create_frozen_approval(
    store: T22Database,
    tmp_path: Path,
    *,
    approval_ttl: timedelta | None = None,
) -> ApprovalScenario:
    scenario = create_approval_scenario(store, tmp_path, approval_ttl=approval_ttl)
    scenario.frozen_receipt = asyncio.run(scenario.service.freeze(scenario.freeze_lease))
    return scenario


def _read_approval(store: T22Database, scenario: ApprovalScenario) -> dict[str, Any]:
    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, scenario.principal)
        return dict(
            connection.execute(
                text(
                    "SELECT * FROM runtime_private.approvals WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
            )
            .mappings()
            .one()
        )


def _close_scenario(scenario: ApprovalScenario) -> None:
    asyncio.run(scenario.storage.aclose())


def test_candidate_evidence_freezes_exact_manifest_and_replays_stably(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        receipt = scenario.frozen_receipt
        assert receipt is not None and receipt.status == "pending"
        row = _read_approval(t22_database, scenario)
        _encoded, digest = digest_manifest(row["manifest"])
        assert digest == row["manifest_sha256"] == receipt.manifest_sha256
        assert row["manifest"]["candidate"]["checkpoint_id"] == str(scenario.candidate_artifact_id)
        assert row["manifest"]["candidate"]["source_tree_sha256"] == (
            scenario.evidence.candidate_source_tree_sha256
        )
        assert row["manifest"]["validation"]["evidence_result_sha256"] == (
            scenario.validation_receipt.result_sha256.hex()
        )
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, scenario.principal)
            run_deadline = connection.execute(
                text("SELECT deadline_at FROM runtime_private.runs WHERE run_id=:run"),
                {"run": scenario.run.run_id},
            ).scalar_one()
        assert row["expires_at"] <= run_deadline and row["revision"] == receipt.revision
        assert row["manifest"]["base"]["commit_sha"] == _BASE_SHA

        adopted = asyncio.run(scenario.service.freeze(scenario.freeze_lease))
        assert adopted.adopted is True
        assert adopted.approval_id == receipt.approval_id
        assert adopted.expires_at == receipt.expires_at
        assert _read_approval(t22_database, scenario)["approval_id"] == receipt.approval_id
    finally:
        _close_scenario(scenario)


def _decide(scenario: ApprovalScenario, decision: str = "approve") -> ApprovalReceipt:
    receipt = scenario.frozen_receipt
    assert receipt is not None
    return asyncio.run(
        scenario.service.decide(
            approval_id=receipt.approval_id,
            manifest_sha256=receipt.manifest_sha256.hex(),
            decision=decision,
        )
    )


def test_approval_schedules_once_and_requires_live_publication_lease(
    t22_database: T22Database, tmp_path: Path
) -> None:
    import pytest

    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        approved = _decide(scenario)
        assert approved.status == "approved"
        assert _decide(scenario).adopted
        with pytest.raises(ApprovalError):
            _decide(scenario, "reject")
        with pytest.raises(ApprovalError):
            asyncio.run(
                scenario.service.authorize_dispatch(
                    approved.approval_id,
                    approved.manifest_sha256.hex(),
                    operation="write_git_objects",
                    lease=scenario.freeze_lease,
                )
            )
        lease = scenario.queue.claim_one("t27-publication")
        assert lease is not None
        grant = asyncio.run(
            scenario.service.authorize_dispatch(
                approved.approval_id,
                approved.manifest_sha256.hex(),
                operation="write_git_objects",
                lease=lease,
            )
        )
        assert grant.approval_id == approved.approval_id
        assert grant.attempt_id == lease.attempt_id
        with t22_database.admin_engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.steps WHERE run_id=:run "
                        "AND activity='publish_approved'"
                    ),
                    {"run": scenario.run.run_id},
                ).scalar_one()
                == 1
            )
        from continuum_api.runtime.cancellation import CancellationService

        CancellationService(t22_database.runtime_engine, scenario.principal).cancel(
            scenario.run.run_id
        )
        with pytest.raises(ApprovalError):
            asyncio.run(
                scenario.service.authorize_dispatch(
                    approved.approval_id,
                    approved.manifest_sha256.hex(),
                    operation="create_pull_request",
                    lease=lease,
                )
            )
    finally:
        _close_scenario(scenario)


def test_rejection_terminalizes_and_changes_does_not_publish(
    t22_database: T22Database, tmp_path: Path
) -> None:
    for decision, expected_status in (("reject", "failed"), ("changes", "waiting_approval")):
        scenario = create_frozen_approval(t22_database, tmp_path)
        try:
            _decide(scenario, decision)
            assert _decide(scenario, decision).adopted
            with t22_database.admin_engine.connect() as connection:
                assert (
                    connection.execute(
                        text("SELECT status FROM runtime_private.runs WHERE run_id=:run"),
                        {"run": scenario.run.run_id},
                    ).scalar_one()
                    == expected_status
                )
                assert (
                    connection.execute(
                        text(
                            "SELECT count(*) FROM runtime_private.steps WHERE run_id=:run "
                            "AND activity='publish_approved'"
                        ),
                        {"run": scenario.run.run_id},
                    ).scalar_one()
                    == 0
                )
        finally:
            _close_scenario(scenario)


def test_stale_digest_access_and_base_cannot_authorize(
    t22_database: T22Database, tmp_path: Path
) -> None:
    import pytest

    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        receipt = scenario.frozen_receipt
        assert receipt is not None
        with pytest.raises(ApprovalError):
            asyncio.run(
                scenario.service.decide(
                    approval_id=receipt.approval_id,
                    manifest_sha256="0" * 64,
                    decision="approve",
                )
            )
        facts = scenario.facts_provider.facts
        scenario.facts_provider.facts = _publication_facts(
            facts.repository_id,
            facts.github_user_id,
            can_write=False,
        ).facts
        with pytest.raises(ApprovalError):
            _decide(scenario)
        scenario.facts_provider.facts = _publication_facts(
            facts.repository_id,
            facts.github_user_id,
            base_sha="c" * 40,
        ).facts
        with pytest.raises(ApprovalError):
            _decide(scenario)
        assert _read_approval(t22_database, scenario)["status"] == "pending"
        scenario.facts_provider.facts = facts
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text("UPDATE runtime_private.runs SET revision=revision+1 WHERE run_id=:run"),
                {"run": scenario.run.run_id},
            )
        with pytest.raises(ApprovalError):
            _decide(scenario)
    finally:
        _close_scenario(scenario)


def test_concurrent_owner_decisions_accept_exactly_one(
    t22_database: T22Database, tmp_path: Path
) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    scenario = create_frozen_approval(t22_database, tmp_path)
    barrier = Barrier(2)

    def decide(decision: str) -> str:
        barrier.wait(timeout=5)
        try:
            return _decide(scenario, decision).status
        except ApprovalError:
            return "conflict"

    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            futures = [workers.submit(decide, item) for item in ("approve", "reject")]
            outcomes = [future.result(timeout=10) for future in futures]
        assert outcomes.count("conflict") == 1
        assert _read_approval(t22_database, scenario)["status"] in {"approved", "rejected"}
    finally:
        _close_scenario(scenario)


def test_cancelled_run_rejects_pending_decision(t22_database: T22Database, tmp_path: Path) -> None:
    import pytest

    from continuum_api.runtime.cancellation import CancellationService

    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        CancellationService(t22_database.runtime_engine, scenario.principal).cancel(
            scenario.run.run_id
        )
        with pytest.raises(ApprovalError):
            _decide(scenario)
        assert _read_approval(t22_database, scenario)["status"] == "pending"
    finally:
        _close_scenario(scenario)


def test_freeze_recovers_prepared_manifest_after_transition_loss(
    t22_database: T22Database, tmp_path: Path
) -> None:
    from unittest.mock import patch

    import pytest

    scenario = create_approval_scenario(t22_database, tmp_path)
    try:
        with patch.object(scenario.transitions, "commit_result", side_effect=RuntimeError("crash")):
            with pytest.raises(ApprovalError, match="approval_manifest_transition_failed"):
                asyncio.run(scenario.service.freeze(scenario.freeze_lease))
        prepared = _read_approval(t22_database, scenario)
        recovered = asyncio.run(scenario.service.freeze(scenario.freeze_lease))
        assert recovered.adopted
        assert recovered.approval_id == prepared["approval_id"]
        assert recovered.manifest_sha256 == prepared["manifest_sha256"]
        assert recovered.expires_at == prepared["expires_at"]
        scenario.frozen_receipt = recovered
        assert _decide(scenario).status == "approved"
    finally:
        _close_scenario(scenario)


def test_cross_owner_denial_and_manifest_column_immutability(
    t22_database: T22Database, tmp_path: Path
) -> None:
    import pytest
    from sqlalchemy.exc import DBAPIError

    scenario = create_frozen_approval(t22_database, tmp_path)
    other = create_frozen_approval(t22_database, tmp_path)
    try:
        receipt = scenario.frozen_receipt
        assert receipt is not None
        with pytest.raises(ApprovalError, match="approval_not_found"):
            asyncio.run(
                other.service.decide(
                    approval_id=receipt.approval_id,
                    manifest_sha256=receipt.manifest_sha256.hex(),
                    decision="approve",
                )
            )
        with pytest.raises(DBAPIError):
            with t22_database.runtime_engine.begin() as connection:
                RuntimeRepository(connection, scenario.principal)
                connection.execute(
                    text(
                        "UPDATE runtime_private.approvals SET manifest='{}'::jsonb "
                        "WHERE approval_id=:approval"
                    ),
                    {"approval": receipt.approval_id},
                )
        assert _read_approval(t22_database, scenario)["status"] == "pending"
    finally:
        _close_scenario(scenario)
        _close_scenario(other)


@pytest.mark.parametrize("stale_revision", [False, True])
def test_due_expiry_is_durable_and_idempotent(
    t22_database: T22Database,
    tmp_path: Path,
    stale_revision: bool,
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path, approval_ttl=timedelta(seconds=2))
    try:
        receipt = scenario.frozen_receipt
        assert receipt is not None
        if stale_revision:
            with t22_database.admin_engine.begin() as connection:
                connection.execute(
                    text("UPDATE runtime_private.runs SET revision=revision+1 WHERE run_id=:run"),
                    {"run": scenario.run.run_id},
                )
        with t22_database.admin_engine.connect() as connection:
            connection.execute(
                text(
                    "SELECT pg_sleep(GREATEST(0.0, EXTRACT(EPOCH FROM "
                    "CAST(:expiry AS timestamptz)-clock_timestamp()))+0.02)"
                ),
                {"expiry": receipt.expires_at},
            )
        assert scenario.service.expire_due() == 1
        assert scenario.service.expire_due() == 0
        assert _read_approval(t22_database, scenario)["status"] == "expired"
        with t22_database.admin_engine.connect() as connection:
            assert connection.execute(
                text("SELECT status FROM runtime_private.runs WHERE run_id=:run"),
                {"run": scenario.run.run_id},
            ).scalar_one() == ("waiting_approval" if stale_revision else "expired")
    finally:
        _close_scenario(scenario)


def test_corrupt_candidate_lineage_cannot_freeze(t22_database: T22Database, tmp_path: Path) -> None:
    import pytest

    scenario = create_approval_scenario(t22_database, tmp_path)
    try:
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.accepted_results SET payload=payload || "
                    "CAST(:corruption AS jsonb) WHERE run_id=:run AND artifact_id=:artifact"
                ),
                {
                    "run": scenario.run.run_id,
                    "artifact": scenario.candidate_artifact_id,
                    "corruption": '{"corrupt":true}',
                },
            )
        with pytest.raises(ApprovalError, match="approval_candidate_result_hash_mismatch"):
            asyncio.run(scenario.service.freeze(scenario.freeze_lease))
    finally:
        _close_scenario(scenario)


def test_multiple_candidate_checkpoints_freeze_cumulative_patch(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_approval_scenario(t22_database, tmp_path, second_candidate=True)
    try:
        receipt = asyncio.run(scenario.service.freeze(scenario.freeze_lease))
        assert receipt.status == "pending"
        row = _read_approval(t22_database, scenario)
        assert row["manifest"]["candidate"]["checkpoint_id"] == str(scenario.candidate_artifact_id)
    finally:
        _close_scenario(scenario)
