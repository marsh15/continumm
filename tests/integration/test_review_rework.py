"""PostgreSQL proof for owner review, atomic decisions and cumulative rework."""

from __future__ import annotations

import asyncio
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_agent_loop import T22Database, _checkpoint_service
from test_agent_loop import t22_database as t22_database
from test_approvals import ApprovalScenario, _close_scenario, create_frozen_approval

from continuum_api.runtime.agent_loop import DurableAgentLoopService
from continuum_api.runtime.approvals import ApprovalError
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.review import ReviewService


def _review(scenario: ApprovalScenario) -> ReviewService:
    return ReviewService(
        scenario.store.runtime_engine,
        scenario.principal,
        scenario.storage,
        publication_facts=scenario.facts_provider,
    )


def _decision(
    scenario: ApprovalScenario,
    *,
    decision: str = "changes",
    key: str = "review-one",
    feedback: str | None = "Keep the change and add a boundary test",
):
    approval = scenario.frozen_receipt
    assert approval is not None
    return asyncio.run(
        _review(scenario).decide(
            approval.approval_id,
            approval.manifest_sha256.hex(),
            decision,
            feedback=feedback,
            idempotency_key=key,
        )
    )


def _run_state(scenario: ApprovalScenario) -> dict:
    with scenario.store.admin_engine.connect() as connection:
        return dict(
            connection.execute(
                text(
                    "SELECT status, revision, deadline_at, model_requests_reserved, "
                    "model_input_tokens_used, model_input_tokens_reserved, "
                    "model_output_tokens_used, "
                    "model_output_tokens_reserved, model_task_context FROM runtime_private.runs "
                    "WHERE run_id=:run"
                ),
                {"run": scenario.run.run_id},
            )
            .mappings()
            .one()
        )


def test_review_returns_exact_candidate_evidence_and_limits(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        result = asyncio.run(_review(scenario).get_review(scenario.run.run_id))
        assert result.approval.approval_id == scenario.frozen_receipt.approval_id
        assert result.manifest_sha256 == scenario.frozen_receipt.manifest_sha256.hex()
        assert "reviewed" in result.diff.text and "base" in result.diff.text
        assert result.diff.changed_paths == ("src/app.py",)
        assert result.evidence == scenario.evidence
        assert result.revisions.remaining == 2
        assert result.decision_eligible
        assert result.limits.requests.limit == 30
        assert result.limits.sandbox_seconds.limit == 900
    finally:
        _close_scenario(scenario)


def test_rework_is_atomic_idempotent_and_keeps_resource_allowances(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    original_task = {"task": "Apply a bounded reviewed change"}
    try:
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET model_id='scripted-v1', "
                    "model_task_context=CAST(:task AS jsonb), model_tool_schema_sha256=:schemas, "
                    "model_requests_reserved=5, model_input_tokens_used=1234, "
                    "model_output_tokens_used=321 WHERE run_id=:run"
                ),
                {
                    "run": scenario.run.run_id,
                    "task": json.dumps(original_task),
                    "schemas": hashlib.sha256(b"[]").digest(),
                },
            )
        before = _run_state(scenario)
        first = _decision(scenario)
        replay = _decision(scenario)
        assert replay == first
        assert _decision(scenario, key="same-decision-new-key") == first
        with pytest.raises(ApprovalError):
            _decision(
                scenario,
                key="different-feedback-new-key",
                feedback="Ignored feedback must conflict",
            )
        after = _run_state(scenario)
        assert after["revision"] == 1 and after["status"] == "running"
        for field in before.keys() - {"status", "revision"}:
            assert after[field] == before[field], field
        with pytest.raises(ApprovalError):
            _decision(scenario, feedback="Different feedback")
        with pytest.raises(ApprovalError):
            _decision(scenario, decision="approve", key="other-decision", feedback=None)
        lease = scenario.queue.claim_one("t28-revision-worker")
        assert lease is not None
        persistence = ArtifactPersistence(t22_database.runtime_engine)
        checkpoints = _checkpoint_service(
            t22_database.runtime_engine, scenario.principal, persistence, scenario.storage
        )
        loop = DurableAgentLoopService(
            t22_database.runtime_engine,
            scenario.principal,
            scenario.transitions,
            checkpoints=checkpoints,
        )
        intent = asyncio.run(
            loop.prepare_turn(
                lease,
                "t28:first-revision",
                context=original_task,
                tool_schemas=(),
                baseline_artifact_id=scenario.baseline_artifact_id,
            )
        )
        with t22_database.admin_engine.connect() as connection:
            context = connection.execute(
                text("SELECT context FROM runtime_private.agent_turns WHERE turn_id=:turn"),
                {"turn": intent.turn_id},
            ).scalar_one()
        assert "Keep the change and add a boundary test" in json.dumps(context)
        assert context["task"] == original_task["task"]
        assert _run_state(scenario)["model_requests_reserved"] == 6
    finally:
        _close_scenario(scenario)


def test_cross_owner_and_access_loss_do_not_expose_review(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    from continuum_api.repositories.preflight import RepositoryAccessProof

    scenario = create_frozen_approval(t22_database, tmp_path)
    other = create_frozen_approval(t22_database, tmp_path)
    try:
        with pytest.raises(ApprovalError):
            asyncio.run(_review(other).get_review(scenario.run.run_id))
        facts = scenario.facts_provider.facts
        scenario.facts_provider.facts = replace(
            facts,
            access_proof=RepositoryAccessProof(
                facts.repository_id,
                facts.github_user_id,
                False,
                True,
            ),
        )
        with pytest.raises(ApprovalError):
            asyncio.run(_review(scenario).get_review(scenario.run.run_id))
        with pytest.raises(ApprovalError):
            _decision(scenario)
        assert _run_state(scenario)["revision"] == 0
    finally:
        _close_scenario(scenario)
        _close_scenario(other)


def test_competing_rework_and_approval_commit_one_outcome(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    barrier = Barrier(2)

    def decide(decision: str) -> str:
        barrier.wait(timeout=5)
        try:
            _decision(
                scenario,
                decision=decision,
                key=str(uuid4()),
                feedback="Add a test" if decision == "changes" else None,
            )
        except ApprovalError:
            return "conflict"
        return decision

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(decide, decision) for decision in ("changes", "approve")]
            outcomes = [future.result(timeout=10) for future in futures]
        assert outcomes.count("conflict") == 1
        assert _run_state(scenario)["revision"] == (1 if "changes" in outcomes else 0)
    finally:
        _close_scenario(scenario)


def _validate_reworked_candidate(scenario: ApprovalScenario) -> None:
    """Create actual T21 checkpoints and new T20 evidence without a model/provider call."""
    from unittest.mock import patch
    from uuid import uuid4

    import test_approvals as fixtures

    from continuum_api.artifacts.archives import ArchiveFile, create_archive, verify_archive
    from continuum_api.runtime.persistence import ReplayPolicy, RunStatus
    from continuum_api.runtime.transitions import NextWork
    from continuum_api.runtime.workspace_recovery import WorkspaceRecoveryService
    from continuum_api.tools.source_tools import (
        candidate_from_baseline,
        snapshot_from_verified_archive,
    )

    async def prepare() -> None:
        revision = _run_state(scenario)["revision"]
        lease = scenario.queue.claim_one(f"t28-edit-revision-{revision}")
        assert lease is not None
        with scenario.store.admin_engine.begin() as connection:
            connection.execute(
                text("UPDATE runtime_private.steps SET activity='agent_tool' WHERE step_id=:step"),
                {"step": lease.step_id},
            )
        checkpoints = _checkpoint_service(
            scenario.store.runtime_engine,
            scenario.principal,
            ArtifactPersistence(scenario.store.runtime_engine),
            scenario.storage,
        )
        recovery = WorkspaceRecoveryService(
            scenario.store.runtime_engine, scenario.principal, checkpoints, scenario.transitions
        )
        workspace = await recovery.restore_latest(scenario.principal, lease)
        bundle = create_archive(
            tuple(
                ArchiveFile(
                    item.path,
                    f"def answer():\n    return 'revision{revision}'\n".encode(),
                    item.mode,
                )
                if item.path == "src/app.py"
                else item
                for item in workspace.verified.files
            )
        )
        verified = verify_archive(
            bundle.archive,
            expected_archive_sha256=bundle.archive_sha256,
            expected_tree_sha256=bundle.manifest.tree_sha256,
        )
        source = snapshot_from_verified_archive(
            verified,
            expected_archive_sha256=bundle.archive_sha256,
            expected_tree_sha256=bundle.manifest.tree_sha256,
        )
        candidate = candidate_from_baseline(workspace.source, source)
        accepted = await recovery.accept_candidate(
            scenario.principal,
            lease,
            f"t28-candidate-r{revision}",
            workspace,
            candidate,
            result={
                "tool_name": "apply_patch",
                "tool_result": {
                    "name": "apply_patch",
                    "candidate_archive_sha256": source.archive_sha256,
                    "candidate_tree_sha256": source.tree_sha256,
                    "changed_paths": list(candidate.changed_paths),
                },
            },
            target_run_status=RunStatus.RUNNING,
            next_work=NextWork(
                f"t28-validation-r{revision}",
                "candidate_validation",
                revision=revision,
                replay_policy=ReplayPolicy.REPLAY_SAFE,
            ),
        )
        validation_lease = scenario.queue.claim_one(f"t28-validation-r{revision}")
        assert validation_lease is not None
        allocation_ids = (uuid4(), uuid4())
        lineage = hashlib.sha256(
            f"t28-revision:{scenario.run.run_id}:{revision}".encode()
        ).hexdigest()
        with patch.object(fixtures, "_FIXTURE_LINEAGE_SHA256", lineage):
            evidence = fixtures._validation_evidence(
                scenario.run,
                scenario.baseline_artifact_id,
                scenario.evidence.baseline.baseline_archive_sha256,
                scenario.evidence.baseline.baseline_archive_tree_sha256,
                source.archive_sha256,
                source.tree_sha256,
                allocation_ids,
            )
            fixtures._seed_allocations(
                scenario.store, scenario.run, lease, allocation_ids, evidence
            )
        validation_receipt = scenario.service.record_validation(
            validation_lease, accepted.artifact_id, evidence
        )
        freeze_lease = scenario.queue.claim_one(f"t28-freeze-r{revision}")
        assert freeze_lease is not None
        scenario.frozen_receipt = await scenario.service.freeze(freeze_lease)
        scenario.evidence = evidence
        scenario.candidate_artifact_id = accepted.artifact_id
        scenario.validation_receipt = validation_receipt
        scenario.freeze_lease = freeze_lease

    asyncio.run(prepare())


def test_two_rework_rounds_require_new_evidence_and_exact_approval(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        initial = _run_state(scenario)
        ids = {scenario.frozen_receipt.approval_id}
        digests = {scenario.frozen_receipt.manifest_sha256}
        evidence_ids = {scenario.validation_receipt.result_id}
        for revision in (1, 2):
            old = scenario.frozen_receipt
            _decision(scenario, key=f"rework-{revision}", feedback=f"Revise boundary {revision}")
            assert _run_state(scenario)["revision"] == revision
            with pytest.raises(ApprovalError):
                asyncio.run(
                    _review(scenario).decide(
                        old.approval_id,
                        old.manifest_sha256.hex(),
                        "approve",
                        idempotency_key=f"stale-{revision}",
                    )
                )
            _validate_reworked_candidate(scenario)
            current = scenario.frozen_receipt
            assert current.approval_id not in ids
            assert current.manifest_sha256 not in digests
            assert scenario.validation_receipt.result_id not in evidence_ids
            ids.add(current.approval_id)
            digests.add(current.manifest_sha256)
            evidence_ids.add(scenario.validation_receipt.result_id)
        with pytest.raises(ApprovalError):
            _decision(scenario, key="third-round", feedback="One round too many")
        assert _run_state(scenario)["revision"] == 2
        for field in (
            "model_requests_reserved",
            "model_input_tokens_used",
            "model_output_tokens_used",
        ):
            assert _run_state(scenario)[field] == initial[field]
        _decision(scenario, decision="approve", key="final-approval", feedback=None)
        assert _run_state(scenario)["status"] == "running"
    finally:
        _close_scenario(scenario)


def test_review_http_routes_commit_and_replay_real_owner_decision(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    from fastapi.testclient import TestClient

    from continuum_api.app import create_app

    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        app = create_app(
            static_dir=tmp_path / "absent-static",
            principal_resolver=lambda request: scenario.principal,
            runtime_engine=t22_database.runtime_engine,
            review_service_factory=lambda request, principal: ReviewService(
                t22_database.runtime_engine,
                principal,
                scenario.storage,
                publication_facts=scenario.facts_provider,
            ),
            csrf_guard=lambda request: (
                request.headers.get("origin") == "https://console.fixture.invalid"
                and request.headers.get("x-csrf-token") == "fixture-csrf-token"
            ),
        )
        approval = scenario.frozen_receipt
        path = f"/api/v1/approvals/{approval.approval_id}/decision"
        body = {
            "decision": "changes",
            "manifest_sha256": approval.manifest_sha256.hex(),
            "feedback": "Add a regression test",
        }
        headers = {
            "Origin": "https://console.fixture.invalid",
            "X-CSRF-Token": "fixture-csrf-token",
            "Idempotency-Key": "http-review-command",
        }
        with TestClient(app) as client:
            review = client.get(f"/api/v1/runs/{scenario.run.run_id}/review")
            assert review.status_code == 200
            assert review.headers["cache-control"] == "private, no-store"
            denied = client.post(
                path, json=body, headers={**headers, "Origin": "https://foreign.invalid"}
            )
            assert denied.status_code == 403
            assert _run_state(scenario)["revision"] == 0
            accepted = client.post(path, json=body, headers=headers)
            assert accepted.status_code == 202, accepted.text
            replay = client.post(path, json=body, headers=headers)
            assert replay.status_code == 202 and replay.json() == accepted.json()
            assert accepted.json()["revision"] == 1
            conflict = client.post(
                path, json={**body, "feedback": "Different feedback"}, headers=headers
            )
            assert conflict.status_code == 409
            assert conflict.json()["code"] == "approval_idempotency_conflict"
        assert _run_state(scenario)["revision"] == 1
    finally:
        _close_scenario(scenario)
