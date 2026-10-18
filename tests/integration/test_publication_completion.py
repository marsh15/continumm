"""PostgreSQL proofs for evidence-gated successful publication completion."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from test_agent_loop import T22Database, _expire_worker_lease
from test_agent_loop import t22_database as t22_database
from test_approvals import _close_scenario, _decide, _read_approval, create_frozen_approval
from test_github_publication import FakeGitHub
from test_github_publication import _service as _branch_service

from continuum_api.publication.github_pr import GitHubPullRequest
from continuum_api.runtime.approvals import RepositoryPublicationFacts
from continuum_api.runtime.cancellation import CancellationService
from continuum_api.runtime.external_ledger import ExternalOperationService
from continuum_api.runtime.github_pr_publication import (
    GitHubPullRequestService,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost
from continuum_api.runtime.persistence import (
    RunStatus,
    RuntimeRepository,
)
from continuum_api.runtime.transitions import InvalidTransition, TransitionReceipt


class _PullRequestRemote:
    def __init__(self, facts: RepositoryPublicationFacts, refs: dict[str, str]) -> None:
        self.facts = facts
        self.refs = refs
        self.pull_requests: list[GitHubPullRequest] = []
        self.lose_create_response = False
        self.create_conflict = False

    async def get_ref(
        self,
        repository_id: int,
        github_user_id: int,
        branch: str,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> str | None:
        assert repository_id == self.facts.repository_id
        assert (expected_owner, expected_name) == (self.facts.owner, self.facts.name)
        del github_user_id
        return self.refs.get(branch)

    async def list_pull_requests(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> tuple[GitHubPullRequest, ...]:
        assert repository_id == self.facts.repository_id
        assert (expected_owner, expected_name) == (self.facts.owner, self.facts.name)
        del github_user_id
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
        del repository_id, github_user_id, expected_owner, expected_name
        return next(item for item in self.pull_requests if item.number == number)

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
        assert repository_id == self.facts.repository_id
        assert (expected_owner, expected_name) == (self.facts.owner, self.facts.name)
        assert self.refs.get(expected_head_branch) == expected_head_sha
        assert marker in body
        created = GitHubPullRequest(
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
        self.pull_requests.append(created)
        if self.create_conflict:
            self.pull_requests.append(replace(created, title=created.title + " changed"))
        if self.lose_create_response:
            raise TimeoutError("fixture lost the create response")
        return created


def _publication_binding(scenario: Any) -> tuple[UUID, str]:
    receipt = scenario.frozen_receipt
    assert receipt is not None
    return receipt.approval_id, receipt.manifest_sha256.hex()


def _approved_lease(scenario: Any) -> tuple[ExecutionLease, UUID, str]:
    _decide(scenario)
    approval_id, digest = _publication_binding(scenario)
    lease = scenario.queue.claim_one("t34-publication-completion")
    assert lease is not None
    return lease, approval_id, digest


def _confirm_branch(
    scenario: Any, lease: ExecutionLease, approval_id: Any, digest: str
) -> FakeGitHub:
    remote = FakeGitHub(scenario.facts_provider.facts)
    service = _branch_service(scenario, remote)
    for _ in range(12):
        receipt = asyncio.run(service.publish(lease, approval_id, digest))
        if receipt.status == "published":
            break
    assert receipt.status == "published"
    return remote


def _publish_pull_request(
    scenario: Any,
    lease: ExecutionLease,
    approval_id: Any,
    digest: str,
    branch_remote: FakeGitHub,
    *,
    outcome: str = "open",
) -> tuple[Any, _PullRequestRemote]:
    row = _read_approval(scenario.store, scenario)
    remote = _PullRequestRemote(scenario.facts_provider.facts, dict(branch_remote.refs))
    if outcome == "unknown":
        remote.lose_create_response = True
    elif outcome == "conflict":
        remote.create_conflict = True
    ledger = ExternalOperationService(scenario.store.runtime_engine, scenario.principal)
    service = GitHubPullRequestService(scenario.service, ledger, remote)
    receipt = asyncio.run(service.publish(lease, approval_id, digest))
    if outcome == "open":
        assert receipt.status in {"created", "adopted"}
        assert receipt.remote_state == "open"
    else:
        assert receipt.status == outcome
    assert row["manifest"]["commit"]["sha"] == next(iter(branch_remote.refs.values()))
    return receipt, remote


def _complete(
    scenario: Any, lease: ExecutionLease, approval_id: Any, digest: str
) -> TransitionReceipt:
    return scenario.transitions.complete_publication(lease, approval_id, digest)


def _run_status(store: T22Database, scenario: Any) -> RunStatus:
    with store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, scenario.principal)
        status = connection.execute(
            text("SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
            {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
        ).scalar_one()
    return RunStatus(status)


def _fully_published(scenario: Any) -> tuple[ExecutionLease, Any, str, FakeGitHub]:
    lease, approval_id, digest = _approved_lease(scenario)
    branch_remote = _confirm_branch(scenario, lease, approval_id, digest)
    _publish_pull_request(scenario, lease, approval_id, digest, branch_remote)
    with scenario.store.runtime_engine.begin() as connection:
        RuntimeRepository(connection, scenario.principal)
        statuses = dict(
            connection.execute(
                text(
                    "SELECT operation_key, status FROM runtime_private.external_operations "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
            ).all()
        )
    assert statuses["github:managed-branch"] == "confirmed"
    assert statuses["github:pull-request"] == "confirmed"
    assert all(status == "confirmed" for status in statuses.values())
    return lease, approval_id, digest, branch_remote


def test_completion_requires_real_branch_and_pull_request_confirmation_and_adopts_replay(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        lease, approval_id, digest, _remote = _fully_published(scenario)
        with pytest.raises(
            InvalidTransition, match="activity_result_cannot_set_terminal_run_status"
        ):
            scenario.transitions.commit_result(
                lease=lease,
                result={"activity": "publish_approved"},
                target_run_status=RunStatus.SUCCEEDED,
            )

        receipt = _complete(scenario, lease, approval_id, digest)
        replay = _complete(scenario, lease, approval_id, digest)

        assert receipt.run_status is RunStatus.SUCCEEDED
        assert receipt.result == {
            "approval_id": str(approval_id),
            "manifest_sha256": digest,
            "publication": "confirmed",
        }
        assert not receipt.adopted
        assert replay.adopted and replay.result_id == receipt.result_id
        assert _run_status(t22_database, scenario) is RunStatus.SUCCEEDED
    finally:
        _close_scenario(scenario)


@pytest.mark.parametrize("binding", ["missing", "wrong_digest"])
def test_completion_rejects_missing_or_wrong_approval_binding(
    t22_database: T22Database, tmp_path: Path, binding: str
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        lease, approval_id, digest, _remote = _fully_published(scenario)
        with pytest.raises(InvalidTransition):
            if binding == "missing":
                _complete(scenario, lease, uuid4(), digest)
            else:
                _complete(scenario, lease, approval_id, "0" * 64)
        assert _run_status(t22_database, scenario) is RunStatus.RUNNING
    finally:
        _close_scenario(scenario)


def test_completion_rejects_expired_publication_lease(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        lease, approval_id, digest, _remote = _fully_published(scenario)
        _expire_worker_lease(
            t22_database.admin_engine,
            scenario.run,
            {"job_id": lease.job_id, "generation": lease.generation},
        )
        with pytest.raises(LeaseLost):
            _complete(scenario, lease, approval_id, digest)
        assert _run_status(t22_database, scenario) is RunStatus.RUNNING
    finally:
        _close_scenario(scenario)


def test_completion_cannot_overwrite_cancellation(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        lease, approval_id, digest, _remote = _fully_published(scenario)
        CancellationService(t22_database.runtime_engine, scenario.principal).cancel(
            scenario.run.run_id
        )
        with pytest.raises((InvalidTransition, LeaseLost)):
            _complete(scenario, lease, approval_id, digest)
        assert _run_status(t22_database, scenario) is RunStatus.CANCELLED
    finally:
        _close_scenario(scenario)


@pytest.mark.parametrize("outcome", ["unknown", "conflict"])
def test_completion_requires_confirmed_pull_request_ledger(
    t22_database: T22Database, tmp_path: Path, outcome: str
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        lease, approval_id, digest = _approved_lease(scenario)
        branch_remote = _confirm_branch(scenario, lease, approval_id, digest)
        _publish_pull_request(scenario, lease, approval_id, digest, branch_remote, outcome=outcome)
        with pytest.raises(InvalidTransition, match="publication_completion_unconfirmed"):
            _complete(scenario, lease, approval_id, digest)
        assert _run_status(t22_database, scenario) is RunStatus.RUNNING
    finally:
        _close_scenario(scenario)


def test_completion_blocks_other_queued_steps_and_jobs(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        lease, approval_id, digest, _remote = _fully_published(scenario)
        with t22_database.runtime_engine.begin() as connection:
            repository = RuntimeRepository(connection, scenario.principal)
            step_id = repository.create_step(
                scenario.run.run_id, "unrelated-validation", "candidate_validation"
            )
            repository.create_job(scenario.run.run_id, step_id)
        with pytest.raises(InvalidTransition, match="publication_completion_unresolved_work"):
            _complete(scenario, lease, approval_id, digest)
        assert _run_status(t22_database, scenario) is RunStatus.RUNNING
    finally:
        _close_scenario(scenario)


def test_completion_blocks_unresolved_external_operations(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        lease, approval_id, digest, _remote = _fully_published(scenario)
        ledger = ExternalOperationService(t22_database.runtime_engine, scenario.principal)
        ledger.prepare(lease, "fixture:unresolved", {"operation": "fixture"})
        with pytest.raises(InvalidTransition, match="publication_completion_unresolved_work"):
            _complete(scenario, lease, approval_id, digest)
        assert _run_status(t22_database, scenario) is RunStatus.RUNNING
    finally:
        _close_scenario(scenario)


@pytest.mark.parametrize("resource", ["resource_reservations", "sandbox_allocations"])
def test_completion_blocks_unresolved_resources(
    t22_database: T22Database, tmp_path: Path, resource: str
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        lease, approval_id, digest, _remote = _fully_published(scenario)
        with t22_database.admin_engine.begin() as connection:
            if resource == "resource_reservations":
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.resource_reservations "
                        "(reservation_id, owner_id, run_id, logical_key, resource_kind, status) "
                        "VALUES (:id, :owner, :run, 'fixture-resource', 'sandbox', 'reserved')"
                    ),
                    {
                        "id": uuid4(),
                        "owner": scenario.run.owner_id,
                        "run": scenario.run.run_id,
                    },
                )
            else:
                connection.execute(
                    text(
                        "UPDATE runtime_private.sandbox_allocations SET status='unknown' "
                        "WHERE allocation_id=:allocation AND owner_id=:owner AND run_id=:run"
                    ),
                    {
                        "allocation": scenario.allocation_ids[0],
                        "owner": scenario.run.owner_id,
                        "run": scenario.run.run_id,
                    },
                )
        with pytest.raises(InvalidTransition, match="publication_completion_unresolved_work"):
            _complete(scenario, lease, approval_id, digest)
        assert _run_status(t22_database, scenario) is RunStatus.RUNNING
    finally:
        _close_scenario(scenario)


def test_completion_racing_cancellation_has_one_immutable_terminal_outcome(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        lease, approval_id, digest, _remote = _fully_published(scenario)
        barrier = Barrier(2)

        def complete() -> TransitionReceipt | BaseException:
            barrier.wait(timeout=10)
            try:
                return _complete(scenario, lease, approval_id, digest)
            except BaseException as error:
                return error

        def cancel() -> Any:
            barrier.wait(timeout=10)
            return CancellationService(t22_database.runtime_engine, scenario.principal).cancel(
                scenario.run.run_id
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            completion_future = executor.submit(complete)
            cancellation_future = executor.submit(cancel)
            completion = completion_future.result(timeout=30)
            cancellation = cancellation_future.result(timeout=30)

        status = _run_status(t22_database, scenario)
        assert status in {RunStatus.SUCCEEDED, RunStatus.CANCELLED}
        if isinstance(completion, TransitionReceipt):
            assert status is RunStatus.SUCCEEDED
            assert cancellation.run_status is RunStatus.SUCCEEDED
        else:
            assert isinstance(completion, (InvalidTransition, LeaseLost))
            assert status is RunStatus.CANCELLED
            assert cancellation.run_status is RunStatus.CANCELLED
    finally:
        _close_scenario(scenario)
