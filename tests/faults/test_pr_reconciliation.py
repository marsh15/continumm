"""Fault proofs for exact, recoverable GitHub pull-request publication."""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INTEGRATION_TESTS = PROJECT_ROOT / "tests" / "integration"
if str(INTEGRATION_TESTS) not in sys.path:
    sys.path.insert(0, str(INTEGRATION_TESTS))

from test_agent_loop import (  # noqa: E402
    T22Database,  # noqa: E402
    _expire_worker_lease,
)
from test_agent_loop import (  # noqa: E402
    t22_database as t22_database,
)
from test_approvals import (  # noqa: E402
    _close_scenario,
    _decide,
    _read_approval,
    create_frozen_approval,
)

from continuum_api.app import create_app  # noqa: E402
from continuum_api.publication.github_pr import (  # noqa: E402
    GitHubPRTransportError,
    GitHubPullRequest,
)
from continuum_api.runtime.approvals import RepositoryPublicationFacts  # noqa: E402
from continuum_api.runtime.external_attention import ExternalAttentionService  # noqa: E402
from continuum_api.runtime.external_ledger import ExternalOperationService  # noqa: E402
from continuum_api.runtime.github_pr_publication import (  # noqa: E402
    OPERATION_KEY,
    GitHubPullRequestPublicationError,
    GitHubPullRequestService,
)
from continuum_api.runtime.leases import ExecutionLease  # noqa: E402
from continuum_api.runtime.persistence import RuntimeRepository  # noqa: E402


@dataclass
class FakeGitHubPR:
    """Bounded remote model with response-loss and incomplete-scan faults."""

    facts: RepositoryPublicationFacts
    refs: dict[str, str] = field(default_factory=dict)
    pull_requests: list[GitHubPullRequest] = field(default_factory=list)
    create_calls: int = 0
    list_calls: int = 0
    get_calls: int = 0
    get_overrides: dict[int, GitHubPullRequest] = field(default_factory=dict)
    lose_create_response: bool = False
    hidden_list_reads_after_create: int = 0
    _hidden_reads_remaining: int = 0
    incomplete_list_error: str | None = None
    deny_create: bool = False
    after_create_rows: list[GitHubPullRequest] = field(default_factory=list)

    async def current(self, repository_id: int, github_user_id: int) -> RepositoryPublicationFacts:
        assert repository_id == self.facts.repository_id
        return replace(self.facts, github_user_id=github_user_id)

    async def get_ref(
        self,
        repository_id: int,
        github_user_id: int,
        branch: str,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> str | None:
        del repository_id, github_user_id
        assert (expected_owner, expected_name) == (self.facts.owner, self.facts.name)
        return self.refs.get(branch)

    async def list_pull_requests(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> tuple[GitHubPullRequest, ...]:
        del github_user_id
        assert repository_id == self.facts.repository_id
        assert (expected_owner, expected_name) == (self.facts.owner, self.facts.name)
        self.list_calls += 1
        if self.incomplete_list_error:
            raise GitHubPRTransportError(self.incomplete_list_error)
        if self._hidden_reads_remaining:
            self._hidden_reads_remaining -= 1
            return ()
        return tuple(self.pull_requests)

    async def get_pull_request(
        self,
        repository_id: int,
        github_user_id: int,
        number: int,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> GitHubPullRequest:
        del github_user_id
        assert repository_id == self.facts.repository_id
        assert (expected_owner, expected_name) == (self.facts.owner, self.facts.name)
        self.get_calls += 1
        return self.get_overrides.get(number) or next(
            pr for pr in self.pull_requests if pr.number == number
        )

    async def create_pull_request(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        expected_owner: str,
        expected_name: str,
        expected_base_branch: str,
        expected_base_sha: str,
        expected_head_branch: str,
        expected_head_sha: str,
        title: str,
        body: str,
        marker: str,
    ) -> GitHubPullRequest:
        del github_user_id
        if self.deny_create:
            raise GitHubPRTransportError("github_authorization_failed")
        assert repository_id == self.facts.repository_id
        assert (expected_owner, expected_name) == (self.facts.owner, self.facts.name)
        assert (expected_base_branch, expected_base_sha) == (
            self.facts.default_branch,
            self.facts.base_commit_sha,
        )
        assert self.refs.get(expected_head_branch) == expected_head_sha
        assert marker in body
        self.create_calls += 1
        pr = GitHubPullRequest(
            number=17,
            id=70017,
            head_repository_id=repository_id,
            base_repository_id=repository_id,
            state="open",
            merged=False,
            merged_at=None,
            head_owner=expected_owner,
            head_ref=expected_head_branch,
            head_sha=expected_head_sha,
            base_ref=expected_base_branch,
            base_sha=expected_base_sha,
            title=title,
            body=body,
            html_url=f"https://github.com/{expected_owner}/{expected_name}/pull/17",
        )
        self.pull_requests.append(pr)
        self.pull_requests.extend(self.after_create_rows)
        self._hidden_reads_remaining = self.hidden_list_reads_after_create
        if self.lose_create_response:
            self.lose_create_response = False
            raise TimeoutError("response lost after remote create")
        return pr


def _service(scenario: Any, remote: FakeGitHubPR) -> GitHubPullRequestService:
    ledger = ExternalOperationService(scenario.store.runtime_engine, scenario.principal)
    return GitHubPullRequestService(scenario.service, ledger, remote)


def _lease(scenario: Any, worker: str = "t32-pr-worker") -> ExecutionLease:
    lease = scenario.queue.claim_one(worker)
    assert lease is not None
    return lease


def _remote(scenario: Any) -> FakeGitHubPR:
    facts = scenario.facts_provider.facts
    manifest = _read_approval(scenario.store, scenario)["manifest"]
    return FakeGitHubPR(facts=facts, refs={manifest["branch"]["name"]: manifest["commit"]["sha"]})


def _publish(service: GitHubPullRequestService, lease: ExecutionLease, scenario: Any) -> Any:
    receipt = scenario.frozen_receipt
    assert receipt is not None
    return asyncio.run(service.publish(lease, receipt.approval_id, receipt.manifest_sha256.hex()))


def _reconcile(service: GitHubPullRequestService, scenario: Any) -> Any:
    receipt = scenario.frozen_receipt
    assert receipt is not None
    return asyncio.run(service.reconcile(receipt.approval_id, receipt.manifest_sha256.hex()))


def _managed_pr(scenario: Any, **changes: Any) -> GitHubPullRequest:
    manifest = _read_approval(scenario.store, scenario)["manifest"]
    pr = manifest["pull_request"]
    facts = scenario.facts_provider.facts
    values: dict[str, Any] = {
        "number": 17,
        "id": 70017,
        "head_repository_id": facts.repository_id,
        "base_repository_id": facts.repository_id,
        "state": "open",
        "merged": False,
        "merged_at": None,
        "head_owner": facts.owner,
        "head_ref": pr["head"],
        "head_sha": manifest["commit"]["sha"],
        "base_ref": pr["base"],
        "base_sha": manifest["base"]["commit_sha"],
        "title": pr["title"],
        "body": pr["body"],
        "html_url": f"https://github.com/{facts.owner}/{facts.name}/pull/17",
    }
    values.update(changes)
    return GitHubPullRequest(**values)


def _marker(scenario: Any) -> str:
    return _read_approval(scenario.store, scenario)["manifest"]["pull_request"]["marker"]


def test_creates_exact_approved_pr_then_repeat_adopts_read_only(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        service = _service(scenario, remote)
        lease = _lease(scenario)
        manifest = _read_approval(t22_database, scenario)["manifest"]

        created = _publish(service, lease, scenario)
        assert created.status in {"created", "already_confirmed"}
        assert created.pr_number == 17
        assert remote.create_calls == 1
        assert remote.pull_requests == [_managed_pr(scenario)]

        reads_before_repeat = (remote.list_calls, remote.get_calls)
        replay = _publish(service, lease, scenario)
        assert replay.status in {"adopted", "already_confirmed"}
        assert replay.pr_number == created.pr_number
        assert remote.create_calls == 1
        assert (remote.list_calls, remote.get_calls) > reads_before_repeat
        assert remote.pull_requests[0].title == manifest["pull_request"]["title"]
        assert remote.pull_requests[0].body == manifest["pull_request"]["body"]
    finally:
        _close_scenario(scenario)


@pytest.mark.parametrize(
    ("state", "merged", "expected"),
    [("closed", False, "closed"), ("closed", True, "merged")],
)
def test_matching_terminal_pr_is_reported_without_replacement_or_reopen(
    t22_database: T22Database, tmp_path: Path, state: str, merged: bool, expected: str
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        remote.pull_requests.append(
            _managed_pr(
                scenario,
                state=state,
                merged=merged,
                merged_at="2026-01-01T00:00:00Z",
                base_sha="c" * 40,
            )
        )
        result = _publish(_service(scenario, remote), _lease(scenario), scenario)
        assert result.status == expected
        assert remote.create_calls == 0
        assert len(remote.pull_requests) == 1
    finally:
        _close_scenario(scenario)


@pytest.mark.parametrize(
    "changes",
    [
        {"head_owner": "somebody-else"},
        {"head_repository_id": 99},
        {"base_repository_id": 99},
        {"head_ref": "continumm/run/other"},
        {"head_sha": "d" * 40},
        {"base_ref": "release"},
        {"title": "unapproved title"},
        {"body": "foreign body"},
    ],
)
def test_marker_match_with_foreign_pr_facts_conflicts_without_second_pr(
    t22_database: T22Database, tmp_path: Path, changes: dict[str, Any]
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        # A same-run marker on a foreign record must block publication even if
        # the remaining manifest fields happen to match.
        if "body" in changes:
            changes = {**changes, "body": f"foreign body\n{_marker(scenario)}"}
        remote.pull_requests.append(_managed_pr(scenario, **changes))
        result = _publish(_service(scenario, remote), _lease(scenario), scenario)
        assert result.status == "conflict"
        assert remote.create_calls == 0
        assert len(remote.pull_requests) == 1
    finally:
        _close_scenario(scenario)


def test_lost_create_response_and_repeated_invisible_reads_never_create_twice(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        lease = _lease(scenario, "t32-pr-first-worker")
        remote = _remote(scenario)
        remote.lose_create_response = True
        remote.hidden_list_reads_after_create = 2
        first_service = _service(scenario, remote)

        first = _publish(first_service, lease, scenario)
        assert first.status == "unknown"
        assert remote.create_calls == 1
        assert len(remote.pull_requests) == 1

        # The source archive is needed for a new write but never for recovery.
        with t22_database.runtime_engine.begin() as connection:
            RuntimeRepository(connection, scenario.principal)
            object_key = connection.execute(
                text(
                    "SELECT object_key FROM runtime_private.artifact_uploads "
                    "WHERE owner_id=:owner AND run_id=:run AND upload_id=:artifact"
                ),
                {
                    "owner": scenario.run.owner_id,
                    "run": scenario.run.run_id,
                    "artifact": scenario.candidate_artifact_id,
                },
            ).scalar_one()
        source_path = scenario.objects / object_key
        frozen_source = source_path.read_bytes()
        source_path.unlink()
        try:
            for _ in range(2):
                hidden = _reconcile(_service(scenario, remote), scenario)
                assert hidden.status == "unknown"
                assert remote.create_calls == 1

            _expire_worker_lease(
                t22_database.admin_engine,
                scenario.run,
                {"job_id": lease.job_id, "generation": lease.generation},
            )
            recovered = _reconcile(_service(scenario, remote), scenario)
            assert recovered.status in {"adopted", "already_confirmed"}
            assert recovered.pr_number == 17
            assert remote.create_calls == 1
        finally:
            source_path.write_bytes(frozen_source)
    finally:
        _close_scenario(scenario)


def test_reconciliation_after_cancel_or_expiry_is_read_only(
    t22_database: T22Database, tmp_path: Path
) -> None:
    from continuum_api.runtime.cancellation import CancellationService

    for terminal in ("cancel", "expire"):
        scenario = create_frozen_approval(t22_database, tmp_path)
        try:
            _decide(scenario)
            remote = _remote(scenario)
            remote.lose_create_response = True
            remote.hidden_list_reads_after_create = 3
            assert (
                _publish(_service(scenario, remote), _lease(scenario), scenario).status == "unknown"
            )
            assert remote.create_calls == 1
            with t22_database.runtime_engine.begin() as connection:
                RuntimeRepository(connection, scenario.principal)
                object_key = connection.execute(
                    text(
                        "SELECT object_key FROM runtime_private.artifact_uploads "
                        "WHERE owner_id=:owner AND run_id=:run AND upload_id=:artifact"
                    ),
                    {
                        "owner": scenario.run.owner_id,
                        "run": scenario.run.run_id,
                        "artifact": scenario.candidate_artifact_id,
                    },
                ).scalar_one()
            source_path = scenario.objects / object_key
            frozen_source = source_path.read_bytes()
            source_path.unlink()
            before_terminal = _reconcile(_service(scenario, remote), scenario)
            assert before_terminal.status == "unknown"
            if terminal == "cancel":
                CancellationService(t22_database.runtime_engine, scenario.principal).cancel(
                    scenario.run.run_id
                )
            else:
                with t22_database.admin_engine.begin() as connection:
                    connection.execute(
                        text(
                            "UPDATE runtime_private.runs SET deadline_at="
                            "created_at+interval '1 microsecond' "
                            "WHERE owner_id=:owner AND run_id=:run"
                        ),
                        {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
                    )
            result = _reconcile(_service(scenario, remote), scenario)
            assert result.status == "unknown"
            visible = result
            for _ in range(4):
                if visible.status != "unknown":
                    break
                visible = _reconcile(_service(scenario, remote), scenario)
            assert visible.status in {"adopted", "already_confirmed"}
            assert remote.create_calls == 1
            source_path.write_bytes(frozen_source)
        finally:
            _close_scenario(scenario)


def test_cancelled_unknown_pr_is_visible_and_adopted_through_owner_scoped_api(
    t22_database: T22Database, tmp_path: Path
) -> None:
    from fastapi.testclient import TestClient

    from continuum_api.runtime.cancellation import CancellationService, DispatchIntentService

    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        lease = _lease(scenario)
        dispatcher = DispatchIntentService(t22_database.runtime_engine, scenario.principal)
        unknown_dispatch = dispatcher.authorize(lease, "legacy-unknown", {"intent": "test"})
        completed_dispatch = dispatcher.authorize(lease, "legacy-completed", {"intent": "test"})
        remote = _remote(scenario)
        remote.lose_create_response = True
        first = _publish(_service(scenario, remote), lease, scenario)
        assert first.status == "unknown"
        CancellationService(t22_database.runtime_engine, scenario.principal).cancel(
            scenario.run.run_id
        )
        dispatcher.observe(
            run_id=scenario.run.run_id,
            operation_key=unknown_dispatch.operation_key,
            observation={"state": "unknown", "secret": "must-not-be-returned"},
        )
        dispatcher.observe(
            run_id=scenario.run.run_id,
            operation_key=completed_dispatch.operation_key,
            observation={"state": "completed", "secret": "must-not-be-returned"},
        )
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO runtime_private.resource_reservations "
                    "(reservation_id, owner_id, run_id, logical_key, resource_kind, "
                    "status, metadata) "
                    "VALUES (:id, :owner, :run, :key, 'sandbox', :status, "
                    '\'{"secret":"hidden"}\'::jsonb)'
                ),
                [
                    {
                        "id": uuid4(),
                        "owner": scenario.run.owner_id,
                        "run": scenario.run.run_id,
                        "key": f"reservation-{index}",
                        "status": "reserved" if index % 2 == 0 else "unknown",
                    }
                    for index in range(101)
                ],
            )

        service = _service(scenario, remote)
        application = create_app(
            tmp_path,
            principal_resolver=lambda _request: scenario.principal,
            csrf_guard=lambda _request: True,
            runtime_engine=t22_database.runtime_engine,
            external_attention_service_factory=lambda _request, principal: (
                ExternalAttentionService(t22_database.runtime_engine, principal),
                service,
            ),
        )
        with TestClient(application, base_url="https://continuum.test") as client:
            path = f"/api/v1/runs/{scenario.run.run_id}/external-effects"
            listed = client.get(path)
            assert listed.status_code == 200
            effects = listed.json()["effects"]
            effect = next(item for item in effects if item["kind"] == "external_operation")
            assert effect["kind"] == "external_operation"
            assert effect["status"] == "unknown"
            assert effect["can_reconcile"] is True
            assert listed.json()["truncated"] is True
            assert effect["evidence"] == {
                "pr_number": None,
                "pr_id": None,
                "pr_url": None,
                "pr_state": None,
            }
            assert "request_metadata" not in effect
            assert len([item for item in effects if item["kind"] == "resource_reservation"]) == 100
            assert len([item for item in effects if item["kind"] == "dispatch_intent"]) == 1
            assert len([item for item in effects if item["kind"] == "cleanup_intent"]) == 2
            assert all("secret" not in str(item) for item in effects)
            assert all(item["status"] != "completed" for item in effects)
            assert listed.headers["cache-control"] == "private, no-store"

            effect_id = effect["effect_id"]
            reconcile_path = f"{path}/{effect_id}/reconcile"
            denied = client.post(reconcile_path)
            assert denied.status_code == 403
            malformed = client.post(
                f"{path}/not-a-uuid/reconcile",
                headers={"Origin": "https://continuum.test", "X-CSRF-Token": "fixture-token"},
            )
            assert malformed.status_code == 422
            body_override = client.post(
                reconcile_path,
                json={"approval_id": str(uuid4()), "manifest_sha256": "0" * 64},
                headers={"Origin": "https://continuum.test", "X-CSRF-Token": "fixture-token"},
            )
            assert body_override.status_code == 400
            reconciled = client.post(
                reconcile_path,
                headers={"Origin": "https://continuum.test", "X-CSRF-Token": "fixture-token"},
            )
            assert reconciled.status_code == 200
            assert reconciled.json()["status"] == "adopted"
            assert reconciled.json()["pr_number"] == 17
            assert remote.create_calls == 1
            remaining = client.get(path).json()["effects"]
            assert all(item["kind"] != "external_operation" for item in remaining)
            assert any(item["kind"] == "cleanup_intent" for item in remaining)

        stranger_app = create_app(
            tmp_path,
            principal_resolver=lambda _request: type(scenario.principal)(uuid4()),
            runtime_engine=t22_database.runtime_engine,
        )
        with TestClient(stranger_app, base_url="https://continuum.test") as client:
            cross_owner = client.get(f"/api/v1/runs/{scenario.run.run_id}/external-effects")
            assert cross_owner.status_code == 404

        with t22_database.admin_engine.connect() as connection:
            status = connection.execute(
                text("SELECT status FROM runtime_private.runs WHERE run_id=:run"),
                {"run": scenario.run.run_id},
            ).scalar_one()
        assert status == "cancelled"
    finally:
        _close_scenario(scenario)


@pytest.mark.parametrize("drift", ["auth", "repo", "base", "head"])
def test_changed_authorization_or_ref_facts_block_new_pr_writes(
    t22_database: T22Database, tmp_path: Path, drift: str
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        manifest = _read_approval(t22_database, scenario)["manifest"]
        if drift == "auth":
            remote.deny_create = True
        elif drift == "repo":
            remote.facts = replace(remote.facts, repository_id=remote.facts.repository_id + 1)
        elif drift == "base":
            remote.facts = replace(remote.facts, base_commit_sha="f" * 40)
        else:
            remote.refs[manifest["branch"]["name"]] = "e" * 40

        result = _publish(_service(scenario, remote), _lease(scenario), scenario)
        assert result.status in {"conflict", "unknown", "unavailable"}
        assert remote.create_calls == 0
        assert remote.pull_requests == []
    finally:
        _close_scenario(scenario)


@pytest.mark.parametrize("error", ["github_pr_list_incomplete", "github_pr_pagination_invalid"])
def test_incomplete_bounded_scan_never_proves_absence_or_creates(
    t22_database: T22Database, tmp_path: Path, error: str
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        remote.incomplete_list_error = error
        result = _publish(_service(scenario, remote), _lease(scenario), scenario)
        assert result.status in {"unknown", "unavailable", "not_dispatched"}
        assert remote.create_calls == 0
        assert remote.pull_requests == []
    finally:
        _close_scenario(scenario)


def test_complete_all_state_scan_adopts_exact_match_and_duplicate_markers_conflict(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        lease = _lease(scenario)
        exact = _managed_pr(scenario)
        remote.pull_requests[:] = [
            _managed_pr(
                scenario,
                number=8,
                id=70008,
                state="closed",
                head_ref="continumm/run/another-run",
                body="unrelated pull request",
            ),
            exact,
        ]
        adopted = _publish(_service(scenario, remote), lease, scenario)
        assert adopted.status == "adopted"
        assert adopted.pr_number == exact.number
        assert remote.create_calls == 0

        remote.pull_requests.append(
            replace(
                _managed_pr(scenario, number=19, id=70019),
                html_url="https://github.com/x/y/pull/19",
            )
        )
        duplicate = _publish(_service(scenario, remote), lease, scenario)
        assert duplicate.status == "conflict"
        assert remote.create_calls == 0
    finally:
        _close_scenario(scenario)


def test_successful_create_with_lagging_followup_scan_stays_confirmed(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        remote.hidden_list_reads_after_create = 1
        service = _service(scenario, remote)
        lease = _lease(scenario)

        created = _publish(service, lease, scenario)
        assert created.status == "created"
        assert created.pr_number == 17
        assert remote.create_calls == 1

        replay = _publish(service, lease, scenario)
        assert replay.status in {"adopted", "already_confirmed"}
        assert replay.pr_number == 17
        assert remote.create_calls == 1
    finally:
        _close_scenario(scenario)


def test_successful_create_with_second_matching_postscan_row_records_both_conflicts(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        remote.after_create_rows.append(
            replace(
                _managed_pr(scenario),
                number=18,
                id=70018,
                html_url=f"https://github.com/{scenario.facts_provider.facts.owner}/"
                f"{scenario.facts_provider.facts.name}/pull/18",
            )
        )
        result = _publish(_service(scenario, remote), _lease(scenario), scenario)

        assert result.status == "conflict"
        assert remote.create_calls == 1
        ledger = ExternalOperationService(t22_database.runtime_engine, scenario.principal)
        observations = ledger.read_observations(scenario.run.run_id, OPERATION_KEY, limit=100)
        conflict = next(item for item in observations if item.outcome == "conflict")
        assert conflict.details["count"] == 2
        assert conflict.details["sample_numbers"] == [17, 18]
        assert len(conflict.details["fingerprint"]) == 20
    finally:
        _close_scenario(scenario)


def test_conflict_remains_sticky_after_remote_pr_becomes_exact(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        remote.pull_requests.append(_managed_pr(scenario, title="foreign title"))
        service = _service(scenario, remote)
        lease = _lease(scenario)

        first = _publish(service, lease, scenario)
        assert first.status == "conflict"
        remote.pull_requests[:] = [_managed_pr(scenario)]

        replay = _publish(service, lease, scenario)
        assert replay.status == "conflict"
        assert remote.create_calls == 0
    finally:
        _close_scenario(scenario)


def test_fresh_stored_pr_that_disagrees_with_exact_list_row_conflicts(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        service = _service(scenario, remote)
        lease = _lease(scenario)
        created = _publish(service, lease, scenario)
        assert created.status == "created"
        exact = remote.pull_requests[0]
        remote.get_overrides[exact.number] = replace(exact, title="fresh divergent title")

        result = _reconcile(service, scenario)
        assert result.status == "conflict"
        assert remote.create_calls == 1
    finally:
        _close_scenario(scenario)


def test_corrupt_stored_reference_fails_closed_with_publication_error(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        service = _service(scenario, remote)
        created = _publish(service, _lease(scenario), scenario)
        assert created.status == "created"

        service._ledger.read_observations = lambda *_args, **_kwargs: (  # type: ignore[method-assign]
            SimpleNamespace(details={"number": "17", "id": 70017, "url": created.html_url}),
        )
        with pytest.raises(GitHubPullRequestPublicationError) as error:
            _reconcile(service, scenario)
        assert error.value.code == "github_pr_reference_corrupt"
    finally:
        _close_scenario(scenario)


def test_stored_reference_id_drift_conflicts_with_listed_exact_pr(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        service = _service(scenario, remote)
        created = _publish(service, _lease(scenario), scenario)
        assert created.status == "created"
        exact = remote.pull_requests[0]
        remote.get_overrides[exact.number] = replace(exact, id=exact.id + 1)

        result = _reconcile(service, scenario)
        assert result.status == "conflict"
        assert remote.create_calls == 1
    finally:
        _close_scenario(scenario)


@pytest.mark.parametrize("terminal", ["closed", "merged"])
def test_fresh_terminal_get_state_wins_over_stale_open_list_row(
    t22_database: T22Database, tmp_path: Path, terminal: str
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        service = _service(scenario, remote)
        created = _publish(service, _lease(scenario), scenario)
        assert created.status == "created"
        exact = remote.pull_requests[0]
        remote.get_overrides[exact.number] = replace(
            exact,
            state="closed",
            merged=terminal == "merged",
            merged_at="2026-01-01T00:00:00Z",
        )

        result = _reconcile(service, scenario)
        assert result.status == terminal
        assert result.remote_state == terminal
        assert remote.create_calls == 1
    finally:
        _close_scenario(scenario)


@pytest.mark.parametrize("terminal_action", ["cancel", "expire"])
def test_readonly_reconciliation_seals_prepared_exact_pr_after_approval_ends(
    t22_database: T22Database, tmp_path: Path, terminal_action: str
) -> None:
    from continuum_api.runtime.cancellation import CancellationService

    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        remote.incomplete_list_error = "github_pr_list_incomplete"
        lease = _lease(scenario)
        unavailable = _publish(_service(scenario, remote), lease, scenario)
        assert unavailable.status == "unavailable"
        assert remote.create_calls == 0

        if terminal_action == "cancel":
            CancellationService(t22_database.runtime_engine, scenario.principal).cancel(
                scenario.run.run_id
            )
        else:
            with t22_database.admin_engine.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE runtime_private.runs SET deadline_at="
                        "created_at+interval '1 microsecond' "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
                )

        remote.incomplete_list_error = None
        remote.pull_requests.append(_managed_pr(scenario))
        result = _reconcile(_service(scenario, remote), scenario)
        assert result.status == "adopted"
        assert result.pr_number == 17

        ledger = ExternalOperationService(t22_database.runtime_engine, scenario.principal)
        receipt = ledger.read(scenario.run.run_id, OPERATION_KEY)
        assert receipt is not None
        assert receipt.status != "prepared"
        assert receipt.dispatch_allowed is False
        reference = ledger.read_observations(
            scenario.run.run_id,
            OPERATION_KEY,
            observation_key="pull-request-reference",
            limit=1,
        )
        assert reference
        assert reference[0].details == {
            "number": 17,
            "id": 70017,
            "url": remote.pull_requests[0].html_url,
        }
        assert remote.create_calls == 0
    finally:
        _close_scenario(scenario)


def test_readonly_adoption_of_prepared_pr_prevents_later_create_after_scan_turns_empty(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        remote.incomplete_list_error = "github_pr_list_incomplete"
        lease = _lease(scenario)
        assert _publish(_service(scenario, remote), lease, scenario).status == "unavailable"
        assert remote.create_calls == 0

        remote.incomplete_list_error = None
        remote.pull_requests.append(_managed_pr(scenario))
        service = _service(scenario, remote)
        adopted = _reconcile(service, scenario)
        assert adopted.status == "adopted"
        remote.pull_requests.clear()

        replay = _publish(service, lease, scenario)
        assert replay.status in {"unknown", "not_dispatched"}
        assert remote.create_calls == 0
    finally:
        _close_scenario(scenario)


def test_large_conflict_set_keeps_ledger_observation_bounded(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        remote = _remote(scenario)
        exact = _managed_pr(scenario)
        remote.pull_requests.extend(
            replace(
                exact,
                number=number,
                id=70000 + number,
                html_url=f"https://github.com/{scenario.facts_provider.facts.owner}/"
                f"{scenario.facts_provider.facts.name}/pull/{number}",
            )
            for number in range(1, 1001)
        )

        result = _publish(_service(scenario, remote), _lease(scenario), scenario)
        assert result.status == "conflict"
        ledger = ExternalOperationService(t22_database.runtime_engine, scenario.principal)
        observations = ledger.read_observations(scenario.run.run_id, OPERATION_KEY, limit=100)
        conflict = next(item for item in observations if item.outcome == "conflict")
        assert conflict.details["count"] == 1000
        assert len(conflict.details["sample_numbers"]) == 20
        assert conflict.details["sample_numbers"] == list(range(1, 21))
        assert len(conflict.details["fingerprint"]) == 20
    finally:
        _close_scenario(scenario)


def test_concurrent_exact_confirmation_cannot_overwrite_conflict(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        initial_remote = _remote(scenario)
        initial_service = _service(scenario, initial_remote)
        created = _publish(initial_service, _lease(scenario), scenario)
        assert created.status == "created"
        exact = initial_remote.pull_requests[0]
        duplicate = replace(
            exact,
            number=18,
            id=70018,
            html_url=f"https://github.com/{scenario.facts_provider.facts.owner}/"
            f"{scenario.facts_provider.facts.name}/pull/18",
        )

        async def run_concurrent_reconciliation() -> tuple[Any, Any]:
            exact_list_entered = asyncio.Event()
            conflict_recorded = asyncio.Event()
            exact_remote = _remote(scenario)
            exact_remote.pull_requests[:] = [exact]
            conflicting_remote = _remote(scenario)
            conflicting_remote.pull_requests[:] = [exact, duplicate]

            async def delayed_exact_list(
                repository_id: int,
                github_user_id: int,
                *,
                expected_owner: str,
                expected_name: str,
            ) -> tuple[GitHubPullRequest, ...]:
                del repository_id, github_user_id, expected_owner, expected_name
                exact_list_entered.set()
                await conflict_recorded.wait()
                return (exact,)

            exact_remote.list_pull_requests = delayed_exact_list  # type: ignore[method-assign]
            exact_service = _service(scenario, exact_remote)
            conflict_service = _service(scenario, conflicting_remote)
            original_observe = conflict_service._ledger.observe

            def observe_conflict(**kwargs: Any) -> Any:
                receipt = original_observe(**kwargs)
                if kwargs["outcome"] == "conflict":
                    conflict_recorded.set()
                return receipt

            conflict_service._ledger.observe = observe_conflict  # type: ignore[method-assign]

            exact_task = asyncio.create_task(
                exact_service.reconcile(
                    scenario.frozen_receipt.approval_id,
                    scenario.frozen_receipt.manifest_sha256.hex(),
                )
            )
            await exact_list_entered.wait()
            conflict_task = asyncio.create_task(
                conflict_service.reconcile(
                    scenario.frozen_receipt.approval_id,
                    scenario.frozen_receipt.manifest_sha256.hex(),
                )
            )
            return await asyncio.gather(exact_task, conflict_task)

        exact_result, conflict_result = asyncio.run(run_concurrent_reconciliation())
        assert conflict_result.status == "conflict"
        assert exact_result.status == "conflict"
        ledger = ExternalOperationService(t22_database.runtime_engine, scenario.principal)
        receipt = ledger.read(scenario.run.run_id, OPERATION_KEY)
        assert receipt is not None
        assert receipt.status == "conflict"
    finally:
        _close_scenario(scenario)
