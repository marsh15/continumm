"""PostgreSQL proofs for bounded approved-publication activity dispatch."""

# ruff: noqa: E402, I001 -- the PR fixture is loaded from the sibling fault-test directory.

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

from test_agent_loop import T22Database, t22_database as t22_database
from test_approvals import _close_scenario, _decide, _read_approval, create_frozen_approval
from test_github_publication import FakeGitHub, _remote_for

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FAULT_TESTS = PROJECT_ROOT / "tests" / "faults"
if str(FAULT_TESTS) not in sys.path:
    sys.path.insert(0, str(FAULT_TESTS))

from test_pr_reconciliation import FakeGitHubPR

import continuum_api.runtime.github_publication as github_publication_module
from continuum_api.runtime.activity_runner import ActivityRunner, RunnerStopReason
from continuum_api.runtime.external_ledger import ExternalOperationService
from continuum_api.runtime.github_pr_publication import GitHubPullRequestService
from continuum_api.runtime.github_publication import GitHubBranchPublicationService
from continuum_api.runtime.persistence import RunStatus, RuntimeRepository
from continuum_api.runtime.publication_activity import ApprovedPublicationActivityHandler


def _publication_runner(scenario: Any, *, lose_create_response: bool = False):
    branch_remote: FakeGitHub = _remote_for(scenario)
    pr_remote = FakeGitHubPR(
        facts=scenario.facts_provider.facts,
        refs=branch_remote.refs,
        lose_create_response=lose_create_response,
    )
    ledger = ExternalOperationService(scenario.store.runtime_engine, scenario.principal)
    branch_service = GitHubBranchPublicationService(
        scenario.service,
        ledger,
        branch_remote,
    )
    pull_request_service = GitHubPullRequestService(
        scenario.service,
        ledger,
        pr_remote,
    )
    handler = ApprovedPublicationActivityHandler(
        scenario.store.runtime_engine,
        scenario.principal,
        scenario.transitions,
        branch_service,
        pull_request_service,
    )
    runner = ActivityRunner(
        scenario.store.runtime_engine,
        scenario.principal,
        {"publish_approved": handler},
        lease_owner="t34-publication-activity",
        heartbeat_interval_seconds=0.1,
    )
    return runner, branch_remote, pr_remote


def _publication_state(scenario: Any) -> tuple[str, list[Any], list[str], dict[str, str]]:
    with scenario.store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, scenario.principal)
        run_status = connection.execute(
            text("SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
            {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
        ).scalar_one()
        results = connection.execute(
            text(
                "SELECT step.logical_key, accepted.payload "
                "FROM runtime_private.accepted_results AS accepted "
                "JOIN runtime_private.steps AS step ON step.owner_id=accepted.owner_id "
                "AND step.run_id=accepted.run_id AND step.step_id=accepted.step_id "
                "WHERE accepted.owner_id=:owner AND accepted.run_id=:run "
                "AND step.activity='publish_approved'"
            ),
            {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
        ).all()
        keys = (
            connection.execute(
                text(
                    "SELECT logical_key FROM runtime_private.steps "
                    "WHERE owner_id=:owner AND run_id=:run AND activity='publish_approved'"
                ),
                {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
            )
            .scalars()
            .all()
        )
        operation_rows = connection.execute(
            text(
                "SELECT operation_key, status FROM runtime_private.external_operations "
                "WHERE owner_id=:owner AND run_id=:run "
                "AND operation_key IN ('github:managed-branch','github:pull-request')"
            ),
            {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
        ).all()
    return run_status, list(results), list(keys), dict(operation_rows)


def test_activity_runner_completes_exact_publication_and_is_owner_scoped(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    other = create_frozen_approval(t22_database, tmp_path)
    try:
        approved = _decide(scenario)
        assert approved.status == "approved"
        runner, branch_remote, pr_remote = _publication_runner(scenario)

        receipt = asyncio.run(runner.run_admitted(scenario.run.run_id, max_jobs=20, max_seconds=60))

        assert receipt.jobs_started == 1
        assert receipt.stop_reason is RunnerStopReason.IDLE
        assert branch_remote.create_ref_calls == 1
        assert pr_remote.create_calls == 1
        status, accepted, keys, operations = _publication_state(scenario)
        assert status == RunStatus.SUCCEEDED.value
        assert keys == [f"publish-approved:{approved.approval_id.hex}"]
        assert accepted[0].payload["approval_id"] == str(approved.approval_id)
        assert accepted[0].payload["manifest_sha256"] == approved.manifest_sha256.hex()
        assert operations == {
            "github:managed-branch": "confirmed",
            "github:pull-request": "confirmed",
        }
        assert _read_approval(t22_database, other)["status"] == "pending"
        assert _run_status(other) == RunStatus.WAITING_APPROVAL.value
    finally:
        _close_scenario(other)
        _close_scenario(scenario)


def test_pending_git_object_budget_reschedules_with_same_binding(
    t22_database: T22Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        approved = _decide(scenario)
        monkeypatch.setattr(github_publication_module, "MAX_MUTATIONS_PER_SLICE", 1)
        runner, branch_remote, pr_remote = _publication_runner(scenario)

        receipt = asyncio.run(
            runner.run_admitted(scenario.run.run_id, max_jobs=100, max_seconds=90)
        )

        assert receipt.jobs_started > 1
        assert receipt.stop_reason is RunnerStopReason.IDLE
        assert branch_remote.create_ref_calls == 1
        assert pr_remote.create_calls == 1
        status, accepted, keys, operations = _publication_state(scenario)
        assert status == RunStatus.SUCCEEDED.value
        expected_keys = [f"publish-approved:{approved.approval_id.hex}"] + [
            f"publish-approved:{approved.approval_id.hex}:part:{index}"
            for index in range(1, receipt.jobs_started)
        ]
        assert sorted(keys, key=_part_number) == expected_keys
        assert all(
            result.payload["approval_id"] == str(approved.approval_id)
            and result.payload["manifest_sha256"] == approved.manifest_sha256.hex()
            for result in accepted
        )
        assert operations == {
            "github:managed-branch": "confirmed",
            "github:pull-request": "confirmed",
        }
    finally:
        _close_scenario(scenario)


def test_unknown_pull_request_moves_to_attention_without_repeated_creation(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        runner, _branch_remote, pr_remote = _publication_runner(
            scenario,
            lose_create_response=True,
        )

        first = asyncio.run(runner.run_admitted(scenario.run.run_id, max_jobs=10, max_seconds=60))
        second = asyncio.run(runner.run_admitted(scenario.run.run_id, max_jobs=10, max_seconds=60))

        assert first.jobs_started == 1
        assert second.jobs_started == 0
        assert pr_remote.create_calls == 1
        status, _accepted, _keys, operations = _publication_state(scenario)
        assert status == RunStatus.NEEDS_ATTENTION.value
        assert operations["github:pull-request"] == "unknown"
    finally:
        _close_scenario(scenario)


def _part_number(key: str) -> int:
    suffix = key.rsplit(":part:", 1)
    return 0 if len(suffix) == 1 else int(suffix[1])


def _run_status(scenario: Any) -> str:
    with scenario.store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, scenario.principal)
        return connection.execute(
            text("SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
            {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
        ).scalar_one()
