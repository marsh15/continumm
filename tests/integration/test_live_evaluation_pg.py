"""PostgreSQL proof for owner-scoped evaluation snapshots and strict pins."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text
from test_agent_loop import t22_database as shared_t22_database
from test_approvals import _REPOSITORY_ID, _close_scenario, create_frozen_approval
from test_workflow_resource_pins import _seed_admitted_run

from continuum_api.runtime.agent_loop import OPENAI_MODEL_ID
from continuum_api.runtime.live_evaluation import LiveEvaluationError, read_live_evaluation_snapshot
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.runtime.workflow_resources import (
    WorkflowConfigurationError,
    _read_admission_pins,
)


@pytest.fixture(scope="module")
def live_eval_database():
    yield from shared_t22_database.__wrapped__()


def test_snapshot_is_owner_scoped_and_reports_only_safe_metadata(live_eval_database) -> None:
    owner = uuid4()
    run_id = _seed_admitted_run(live_eval_database, owner)

    snapshot = read_live_evaluation_snapshot(live_eval_database.runtime_engine, owner, run_id)

    assert snapshot["run_id"] == str(run_id)
    assert snapshot["observed_at"].endswith("+00:00")
    assert snapshot["profile_id"] == "python-pytest"
    assert snapshot["model_id"] == "gpt-5.4-mini-2026-03-17"
    assert snapshot["independent_hidden_checks"] == "not_run"
    assert snapshot["task_correctness"] == "not_assessed"
    assert snapshot["protocol_smoke_ready"] is False
    assert "task_context" not in snapshot
    with pytest.raises(LiveEvaluationError, match="live_run_not_found"):
        read_live_evaluation_snapshot(live_eval_database.runtime_engine, uuid4(), run_id)


def test_evaluation_preflight_requires_provider_credential_binding(live_eval_database) -> None:
    owner = uuid4()
    run_id = _seed_admitted_run(live_eval_database, owner)

    with pytest.raises(WorkflowConfigurationError, match="workflow_provider_pin_invalid"):
        _read_admission_pins(live_eval_database.runtime_engine, owner, run_id, require_binding=True)


def test_snapshot_reads_real_approval_evidence_from_jsonb(live_eval_database, tmp_path) -> None:
    scenario = create_frozen_approval(live_eval_database, tmp_path)
    try:
        _seed_admission_pins(live_eval_database, scenario)
        snapshot = read_live_evaluation_snapshot(
            live_eval_database.runtime_engine, scenario.run.owner_id, scenario.run.run_id
        )

        assert snapshot["validation_classification"] == "passed"
        assert snapshot["human_review_status"] == "pending"
        assert snapshot["candidate_patch_count"] is not None
        assert snapshot["candidate_patch_bytes"] is None
        assert snapshot["protocol_smoke_ready"] is False
    finally:
        _close_scenario(scenario)


def _seed_admission_pins(database, scenario) -> None:
    owner_id = scenario.run.owner_id
    run_id = scenario.run.run_id
    with database.admin_engine.begin() as connection:
        registration_id = connection.execute(
            text(
                "INSERT INTO runtime_private.repository_registrations "
                "(owner_id,github_repository_id,repository_owner,repository_name,"
                "profile_id,profile_version,provider) "
                "VALUES (:owner,:repository,'owner','repo','python-pytest','1','openai') "
                "RETURNING registration_id"
            ),
            {"owner": owner_id, "repository": _REPOSITORY_ID},
        ).scalar_one()
    with database.runtime_engine.begin() as connection:
        RuntimeRepository(connection, Principal(owner_id))
        connection.execute(
            text(
                "INSERT INTO runtime_private.run_admission_pins "
                "(owner_id,run_id,registration_id,defaults_version,profile_id,profile_version,"
                "base_branch,source_sha,source_tree_sha,source_receipt,provider,model_id,"
                "task_context,submission_digest) "
                "VALUES (:owner,:run,:registration,1,'python-pytest','1','main',:sha,:tree,"
                "'{}'::jsonb,'openai',:model,'{\"task\":\"fixture\"}'::jsonb,:digest)"
            ),
            {
                "owner": owner_id,
                "run": run_id,
                "registration": registration_id,
                "sha": "a" * 40,
                "tree": scenario.run.baseline_tree_sha256,
                "model": OPENAI_MODEL_ID,
                "digest": b"x" * 32,
            },
        )
