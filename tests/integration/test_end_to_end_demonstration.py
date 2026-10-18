"""Local fixture journeys through rework, approval and GitHub publication.

Remote provider, sandbox and GitHub behavior stays at the adapter boundaries; the
durable runtime, review, validation, approval and publication services are real.
The initial candidate and revision source edit are fixture supplied. These runs
are controlled local evidence, not a live-provider demonstration.
"""

# ruff: noqa: E402, I001 -- the fault-test helpers need their sibling test directory on sys.path.

from __future__ import annotations

import sys
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from test_agent_loop import T22Database, t22_database as t22_database  # noqa: F401
from test_approvals import _close_scenario, _decide, _read_approval, create_frozen_approval
from test_github_publication import (
    _publication_lease,
    _publish as publish_branch,
    _remote_for as branch_remote_for,
    _service as branch_service_for,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FAULT_TESTS = PROJECT_ROOT / "tests" / "faults"
if str(FAULT_TESTS) not in sys.path:
    sys.path.insert(0, str(FAULT_TESTS))

from test_pr_reconciliation import (  # noqa: E402
    FakeGitHubPR,
    _managed_pr,
    _publish as publish_pr,
    _service as pr_service_for,
)
from test_review_rework import _decision, _validate_reworked_candidate

from continuum_api.app import create_app
from continuum_api.runtime.external_attention import ExternalAttentionService


@pytest.mark.parametrize("lose_create_response", [False, True], ids=["normal", "response-lost"])
def test_local_review_to_publication_attention_and_reconciliation(
    t22_database: T22Database, tmp_path: Path, lose_create_response: bool
) -> None:
    """Drive the same fixture candidate through normal create or recovery."""
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        first_approval = scenario.frozen_receipt
        assert first_approval is not None

        # Reviewer feedback advances the run revision. The existing workspace
        # recovery fixture supplies a controlled candidate patch, then records
        # fresh validation and freezes its manifest through production services.
        _decision(scenario, decision="changes", key="demo-revision-1")
        _validate_reworked_candidate(scenario)
        revised_approval = scenario.frozen_receipt
        assert revised_approval is not None
        assert revised_approval.approval_id != first_approval.approval_id
        assert revised_approval.revision == 1
        assert _read_approval(t22_database, scenario)["status"] == "pending"

        approved = _decide(scenario, decision="approve")
        assert approved.status == "approved"

        # Publish the exact approved tree and then its PR with a local GitHub
        # model. Both calls go through the existing publication services.
        lease = _publication_lease(scenario, "t34-demo-publisher")
        branch_remote = branch_remote_for(scenario)
        branch_result = publish_branch(branch_service_for(scenario, branch_remote), lease, scenario)
        manifest = _read_approval(t22_database, scenario)["manifest"]
        assert branch_result.status == "published"
        assert branch_remote.refs == {manifest["branch"]["name"]: manifest["commit"]["sha"]}

        pr_remote = FakeGitHubPR(
            facts=scenario.facts_provider.facts,
            refs=dict(branch_remote.refs),
            lose_create_response=lose_create_response,
        )
        pr_service = pr_service_for(scenario, pr_remote)
        pr_result = publish_pr(pr_service, lease, scenario)
        assert pr_remote.create_calls == 1
        assert pr_remote.pull_requests == [_managed_pr(scenario)]

        if lose_create_response:
            assert pr_result.status == "unknown"
        else:
            assert pr_result.status == "created"

        application = create_app(
            tmp_path,
            principal_resolver=lambda _request: scenario.principal,
            csrf_guard=lambda _request: True,
            runtime_engine=t22_database.runtime_engine,
            external_attention_service_factory=lambda _request, principal: (
                ExternalAttentionService(t22_database.runtime_engine, principal),
                pr_service,
            ),
        )
        with TestClient(application, base_url="https://continuum.test") as client:
            effects_path = f"/api/v1/runs/{scenario.run.run_id}/external-effects"
            listed = client.get(effects_path)
            assert listed.status_code == 200
            effect = next(
                (item for item in listed.json()["effects"] if item["kind"] == "external_operation"),
                None,
            )
            if lose_create_response:
                assert effect is not None
                assert effect["status"] == "unknown"
                assert effect["can_reconcile"] is True

                reconciled = client.post(
                    f"{effects_path}/{effect['effect_id']}/reconcile",
                    headers={
                        "Origin": "https://continuum.test",
                        "X-CSRF-Token": "fixture-token",
                    },
                )
                assert reconciled.status_code == 200
                assert reconciled.json()["status"] == "adopted"
                assert reconciled.json()["pr_number"] == 17
                assert pr_remote.create_calls == 1

            remaining = client.get(effects_path)
            assert remaining.status_code == 200
            assert all(item["kind"] != "external_operation" for item in remaining.json()["effects"])
    finally:
        _close_scenario(scenario)
