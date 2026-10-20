# ruff: noqa: E402
"""A browser approval and GitHub rework contend on one database decision barrier."""

from __future__ import annotations

import asyncio
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from uuid import uuid4

from sqlalchemy import text

INTEGRATION_TESTS = Path(__file__).resolve().parents[1] / "integration"
if str(INTEGRATION_TESTS) not in sys.path:
    sys.path.insert(0, str(INTEGRATION_TESTS))

from test_approvals import _close_scenario, create_frozen_approval
from test_cross_channel_decisions import (
    _CurrentComment,
    _event,
    _register,
    _snapshot,
)
from test_cross_channel_decisions import t38_database as t38_database

from continuum_api.runtime.approvals import ApprovalError
from continuum_api.runtime.cancellation import CancellationService
from continuum_api.runtime.github_command_execution import GitHubCommandProcessor
from continuum_api.runtime.github_commands import GitHubCommandError, GitHubCommandService


def test_browser_approval_and_comment_rework_commit_one_authoritative_decision(
    t38_database, tmp_path, record_property
) -> None:
    scenario = create_frozen_approval(t38_database, tmp_path)
    approval = scenario.frozen_receipt
    assert approval is not None
    repository_id = 714_038
    github_user_id = _register(t38_database, scenario.principal, repository_id)
    event = _event(
        f"/continumm changes {approval.approval_id}\nPlease adjust the test boundary.",
        repository_id=repository_id,
        author_id=github_user_id,
        issue_id=77_038,
        issue_number=1,
    )
    reader = _CurrentComment()
    reader.value = _snapshot(event)
    accepted = asyncio.run(
        GitHubCommandService(
            t38_database.runtime_engine, scenario.principal, reader
        ).accept_created_comment(event)
    )
    assert accepted is not None
    reader.value = None
    processor = GitHubCommandProcessor(
        t38_database.runtime_engine,
        scenario.principal,
        scenario.service,
        CancellationService(t38_database.runtime_engine, scenario.principal),
    )
    barrier = Barrier(2)

    def browser_decision() -> str:
        barrier.wait(timeout=5)
        try:
            asyncio.run(
                scenario.service.decide(
                    approval_id=approval.approval_id,
                    manifest_sha256=approval.manifest_sha256.hex(),
                    decision="approve",
                    feedback=None,
                    idempotency_key=str(uuid4()),
                )
            )
            return "approved"
        except ApprovalError:
            return "conflict"

    def comment_decision() -> str:
        barrier.wait(timeout=5)
        try:
            result = asyncio.run(processor.execute(accepted))
            assert result.kind == "decision"
            return result.approval_status or "missing_status"
        except GitHubCommandError:
            return "conflict"

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = [pool.submit(browser_decision), pool.submit(comment_decision)]
            results = [future.result(timeout=10) for future in outcomes]
        assert results.count("conflict") == 1
        committed = set(results) & {"approved", "changes"}
        assert len(committed) == 1
        with t38_database.admin_engine.connect() as connection:
            state = connection.execute(
                text(
                    "SELECT status FROM runtime_private.approvals WHERE owner_id=:owner "
                    "AND approval_id=:approval"
                ),
                {"owner": scenario.principal.account_id, "approval": approval.approval_id},
            ).scalar_one()
            revision = connection.execute(
                text(
                    "SELECT revision FROM runtime_private.runs "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": scenario.principal.account_id, "run": scenario.run.run_id},
            ).scalar_one()
        record_property(
            "final_authority",
            json.dumps(
                {
                    "approval_status": state,
                    "revision": revision,
                    "competing_results": results,
                }
            ),
        )
        assert state in {"approved", "changes"}
        assert revision == (1 if state == "changes" else 0)
        assert ("approved" if state == "approved" else "changes") in results
    finally:
        _close_scenario(scenario)
