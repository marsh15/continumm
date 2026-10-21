"""PostgreSQL negative proof for absent owner-scoped hidden evidence."""

from __future__ import annotations

import pytest
from test_agent_loop import t22_database as shared_t22_database
from test_approvals import _close_scenario, create_frozen_approval
from test_live_evaluation_pg import _seed_admission_pins

from continuum_api.runtime.live_evaluation import read_live_evaluation_snapshot


@pytest.fixture(scope="module")
def hidden_snapshot_database():
    yield from shared_t22_database.__wrapped__()


def test_current_normal_approval_without_hidden_evidence_stays_not_run(
    hidden_snapshot_database, tmp_path
) -> None:
    scenario = create_frozen_approval(hidden_snapshot_database, tmp_path)
    try:
        _seed_admission_pins(hidden_snapshot_database, scenario)
        snapshot = read_live_evaluation_snapshot(
            hidden_snapshot_database.runtime_engine,
            scenario.run.owner_id,
            scenario.run.run_id,
        )

        assert snapshot["validation_classification"] == "passed"
        assert snapshot["independent_hidden_checks"] == "not_run"
        assert snapshot["task_correctness"] == "not_assessed"
    finally:
        _close_scenario(scenario)
