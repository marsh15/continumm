"""PostgreSQL proof for exact and recoverable managed GitHub branch publication."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_agent_loop import T22Database, _expire_worker_lease
from test_agent_loop import t22_database as t22_database
from test_approvals import _close_scenario, _decide, _read_approval, create_frozen_approval

import continuum_api.runtime.github_publication as github_publication_module
from continuum_api.approvals.manifest import git_object_sha
from continuum_api.artifacts.archives import ArchiveFile, verify_archive
from continuum_api.publication.github import GitHubTreeEntry
from continuum_api.runtime.approvals import ApprovalError, RepositoryPublicationFacts
from continuum_api.runtime.external_ledger import ExternalOperationService
from continuum_api.runtime.github_publication import (
    GitHubBranchPublicationError,
    GitHubBranchPublicationService,
)
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.persistence import RuntimeRepository


@dataclass
class FakeGitHub:
    """Small remote model that records immutable Git objects and branch writes."""

    facts: RepositoryPublicationFacts
    refs: dict[str, str] = field(default_factory=dict)
    blobs: dict[str, bytes] = field(default_factory=dict)
    trees: dict[str, tuple[GitHubTreeEntry, ...]] = field(default_factory=dict)
    tree_calls: list[tuple[GitHubTreeEntry, ...]] = field(default_factory=list)
    commits: dict[str, dict[str, Any]] = field(default_factory=dict)
    mutations: list[tuple[str, str]] = field(default_factory=list)
    create_ref_calls: int = 0
    fail_ref_reads: int = 0
    lose_create_response: bool = False
    hidden_ref_reads_after_create: int = 0
    _hidden_reads_remaining: int = 0

    async def current(self, repository_id: int, github_user_id: int) -> RepositoryPublicationFacts:
        assert repository_id == self.facts.repository_id
        return RepositoryPublicationFacts(
            repository_id=self.facts.repository_id,
            github_user_id=github_user_id,
            owner=self.facts.owner,
            name=self.facts.name,
            default_branch=self.facts.default_branch,
            base_commit_sha=self.facts.base_commit_sha,
            access_proof=self.facts.access_proof,
        )

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
        if self.fail_ref_reads:
            self.fail_ref_reads -= 1
            raise TimeoutError("temporary ref read failure")
        if self._hidden_reads_remaining:
            self._hidden_reads_remaining -= 1
            return None
        return self.refs.get(branch)

    async def get_object(
        self,
        repository_id: int,
        github_user_id: int,
        kind: str,
        sha: str,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> str | None:
        del repository_id, github_user_id
        assert (expected_owner, expected_name) == (self.facts.owner, self.facts.name)
        objects = {"blob": self.blobs, "tree": self.trees, "commit": self.commits}[kind]
        return sha if sha in objects else None

    async def upload_blob(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        content: bytes,
        expected_sha: str,
        expected_owner: str,
        expected_name: str,
        expected_base_branch: str,
        expected_base_sha: str,
    ) -> str:
        del repository_id, github_user_id
        self._assert_write_scope(
            expected_owner, expected_name, expected_base_branch, expected_base_sha
        )
        actual = git_object_sha("blob", content)
        self.mutations.append(("blob", expected_sha))
        self.blobs[actual] = content
        return actual

    async def upload_tree(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        entries: Sequence[GitHubTreeEntry],
        expected_sha: str,
        expected_owner: str,
        expected_name: str,
        expected_base_branch: str,
        expected_base_sha: str,
    ) -> str:
        del repository_id, github_user_id
        self._assert_write_scope(
            expected_owner, expected_name, expected_base_branch, expected_base_sha
        )
        self.mutations.append(("tree", expected_sha))
        self.trees[expected_sha] = tuple(entries)
        self.tree_calls.append(tuple(entries))
        serialized = []
        for entry in entries:
            name = entry.path + ("/" if entry.type == "tree" else "")
            mode = "40000" if entry.type == "tree" else entry.mode
            serialized.append(
                (
                    name.encode("utf-8"),
                    mode.encode("ascii")
                    + b" "
                    + entry.path.encode("utf-8")
                    + b"\0"
                    + bytes.fromhex(entry.sha),
                )
            )
        body = b"".join(value for _name, value in sorted(serialized, key=lambda row: row[0]))
        return git_object_sha("tree", body)

    async def upload_commit(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        message: str,
        tree_sha: str,
        parent_sha: str,
        author: dict[str, Any],
        committer: dict[str, Any],
        expected_sha: str,
        expected_owner: str,
        expected_name: str,
        expected_base_branch: str,
        expected_base_sha: str,
    ) -> str:
        del repository_id, github_user_id
        self._assert_write_scope(
            expected_owner, expected_name, expected_base_branch, expected_base_sha
        )
        self.mutations.append(("commit", expected_sha))
        self.commits[expected_sha] = {
            "message": message,
            "tree_sha": tree_sha,
            "parent_sha": parent_sha,
            "author": author,
            "committer": committer,
        }
        return expected_sha

    async def create_ref(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        branch: str,
        sha: str,
        expected_owner: str,
        expected_name: str,
        expected_base_branch: str,
        expected_base_sha: str,
    ) -> str:
        del repository_id, github_user_id
        self._assert_write_scope(
            expected_owner, expected_name, expected_base_branch, expected_base_sha
        )
        self.create_ref_calls += 1
        self.mutations.append(("ref", branch))
        self.refs[branch] = sha
        self._hidden_reads_remaining = self.hidden_ref_reads_after_create
        if self.lose_create_response:
            self.lose_create_response = False
            raise TimeoutError("response lost after remote create")
        return sha

    def _assert_write_scope(self, owner: str, name: str, base_branch: str, base_sha: str) -> None:
        assert (owner, name) == (self.facts.owner, self.facts.name)
        assert (base_branch, base_sha) == (
            self.facts.default_branch,
            self.facts.base_commit_sha,
        )


def _service(scenario: Any, remote: FakeGitHub) -> GitHubBranchPublicationService:
    ledger = ExternalOperationService(scenario.store.runtime_engine, scenario.principal)
    return GitHubBranchPublicationService(scenario.service, ledger, remote)


def _publish(service: GitHubBranchPublicationService, lease: ExecutionLease, scenario: Any) -> Any:
    receipt = scenario.frozen_receipt
    assert receipt is not None
    return asyncio.run(service.publish(lease, receipt.approval_id, receipt.manifest_sha256.hex()))


def _publication_lease(scenario: Any, worker: str = "t31-publication") -> ExecutionLease:
    lease = scenario.queue.claim_one(worker)
    assert lease is not None
    return lease


def _remote_for(scenario: Any) -> FakeGitHub:
    return FakeGitHub(scenario.facts_provider.facts)


def _candidate_contents(scenario: Any) -> dict[str, bytes]:
    row = _read_approval(scenario.store, scenario)
    source_path = _candidate_object_path(scenario)
    archive_bytes = source_path.read_bytes()
    archive = verify_archive(
        archive_bytes,
        expected_archive_sha256=row["manifest"]["candidate"]["archive_sha256"],
        expected_tree_sha256=row["manifest"]["candidate"]["source_tree_sha256"],
    )
    return {item.path: item.content for item in archive.files}


def _candidate_object_path(scenario: Any) -> Path:
    with scenario.store.runtime_engine.begin() as connection:
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
    return scenario.objects / object_key


def test_publishes_frozen_candidate_and_replays_exact_branch_without_writes(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        approved = _decide(scenario)
        remote = _remote_for(scenario)
        service = _service(scenario, remote)
        lease = _publication_lease(scenario)

        published = _publish(service, lease, scenario)
        manifest = _read_approval(t22_database, scenario)["manifest"]
        assert published.status == "published"
        assert published.branch == manifest["branch"]["name"]
        assert published.commit_sha == manifest["commit"]["sha"]
        assert remote.refs == {published.branch: published.commit_sha}
        assert remote.create_ref_calls == 1
        contents = _candidate_contents(scenario)
        uploaded_contents = {
            path: remote.blobs[git_object_sha("blob", content)]
            for path, content in contents.items()
        }
        assert uploaded_contents == contents
        assert (
            remote.commits[published.commit_sha]["tree_sha"]
            == manifest["candidate"]["git_tree_sha"]
        )
        assert remote.commits[published.commit_sha]["parent_sha"] == manifest["base"]["commit_sha"]
        writes_before_replay = list(remote.mutations)

        replay = _publish(service, lease, scenario)
        assert replay.status == "published"
        assert replay.commit_sha == published.commit_sha
        assert remote.mutations == writes_before_replay
        assert remote.create_ref_calls == 1

        remote.fail_ref_reads = 1
        uncertain_read = _publish(service, lease, scenario)
        assert uncertain_read.status == "unknown"
        assert remote.mutations == writes_before_replay
        retry_after_read = _publish(service, lease, scenario)
        assert retry_after_read.status == "published"
        assert remote.mutations == writes_before_replay

        remote.refs[published.branch] = "d" * 40
        conflicted_replay = _publish(service, lease, scenario)
        assert conflicted_replay.status == "conflict"
        assert conflicted_replay.branch_status == "conflict"
        assert remote.mutations == writes_before_replay
        assert approved.status == "approved"
    finally:
        _close_scenario(scenario)


def test_base_move_and_diverged_managed_branch_never_create_a_replacement(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        lease = _publication_lease(scenario)
        remote = _remote_for(scenario)
        remote.facts = RepositoryPublicationFacts(
            repository_id=remote.facts.repository_id,
            github_user_id=remote.facts.github_user_id,
            owner=remote.facts.owner,
            name=remote.facts.name,
            default_branch=remote.facts.default_branch,
            base_commit_sha="c" * 40,
            access_proof=remote.facts.access_proof,
        )
        with pytest.raises(GitHubBranchPublicationError, match="github_publication_base_changed"):
            _publish(_service(scenario, remote), lease, scenario)
        assert remote.mutations == []
        assert remote.refs == {}
    finally:
        _close_scenario(scenario)

    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        lease = _publication_lease(scenario)
        remote = _remote_for(scenario)
        branch = _read_approval(t22_database, scenario)["manifest"]["branch"]["name"]
        remote.refs[branch] = "d" * 40
        result = _publish(_service(scenario, remote), lease, scenario)
        assert result.status == "conflict"
        assert result.branch_status == "conflict"
        assert remote.refs[branch] == "d" * 40
        assert remote.create_ref_calls == 0
    finally:
        _close_scenario(scenario)


def test_forged_approval_pending_action_and_corrupt_source_bytes_are_denied(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        remote = _remote_for(scenario)
        service = _service(scenario, remote)
        lease = scenario.freeze_lease
        with pytest.raises(GitHubBranchPublicationError):
            asyncio.run(service.publish(lease, uuid4(), "0" * 64))
        assert remote.mutations == []

        pending = scenario.frozen_receipt
        assert pending is not None
        with pytest.raises(GitHubBranchPublicationError):
            asyncio.run(service.publish(lease, pending.approval_id, pending.manifest_sha256.hex()))
        assert remote.mutations == []

        _decide(scenario)
        lease = _publication_lease(scenario)
        with pytest.raises(ApprovalError):
            asyncio.run(
                scenario.service.authorize_dispatch(
                    pending.approval_id,
                    pending.manifest_sha256.hex(),
                    operation="delete_repository",
                    lease=lease,
                )
            )
        assert remote.mutations == []
        row = _read_approval(t22_database, scenario)
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
        original = source_path.read_bytes()
        source_path.write_bytes(original + b"forged")
        try:
            with pytest.raises(GitHubBranchPublicationError):
                _publish(service, lease, scenario)
            assert remote.mutations == []
        finally:
            source_path.write_bytes(original)
        assert row["manifest"]["candidate"]["archive_sha256"]
    finally:
        _close_scenario(scenario)


def test_lost_branch_response_and_restart_reconcile_without_second_create(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        lease = _publication_lease(scenario, "t31-first-worker")
        remote = _remote_for(scenario)
        remote.lose_create_response = True
        remote.hidden_ref_reads_after_create = 1
        service = _service(scenario, remote)

        first = _publish(service, lease, scenario)
        assert first.status == "unknown"
        assert remote.create_ref_calls == 1
        assert len(remote.refs) == 1
        writes_before_restart = list(remote.mutations)
        source_path = _candidate_object_path(scenario)
        frozen_source = source_path.read_bytes()
        source_path.unlink()

        _expire_worker_lease(
            t22_database.admin_engine,
            scenario.run,
            {"job_id": lease.job_id, "generation": lease.generation},
        )
        # The replacement service carries the expired worker lease. The existing
        # ledger entry permits only a read to reconcile the already-created ref.
        recovered = _publish(_service(scenario, remote), lease, scenario)
        assert recovered.status == "published"
        assert remote.create_ref_calls == 1
        assert remote.mutations == writes_before_restart
        source_path.write_bytes(frozen_source)
    finally:
        _close_scenario(scenario)


def test_content_addressed_uploads_preserve_tree_shaped_blobs_modes_and_depth(
    t22_database: T22Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import test_agent_loop

    original_source_files = test_agent_loop._base_source_files

    def source_files_with_edge_cases() -> tuple[ArchiveFile, ...]:
        return (
            *original_source_files(),
            ArchiveFile("src/nested/deep/value.py", b"answer = 'nested'\n"),
            ArchiveFile("tree-shaped.bin", b"100644 x\0" + bytes.fromhex("a" * 40)),
            ArchiveFile("tools/run.sh", b"#!/bin/sh\necho reviewed\n", mode=0o755),
        )

    monkeypatch.setattr(test_agent_loop, "_base_source_files", source_files_with_edge_cases)
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        lease = _publication_lease(scenario)
        remote = _remote_for(scenario)
        published = _publish(_service(scenario, remote), lease, scenario)
        assert published.status == "published"

        contents = _candidate_contents(scenario)
        assert {sha: remote.blobs[sha] for sha in remote.blobs} == {
            git_object_sha("blob", content): content for content in contents.values()
        }
        assert any(content.startswith(b"100644 x\0") for content in remote.blobs.values())

        tree_names = [
            tuple((entry.path, entry.type) for entry in entries) for entries in remote.tree_calls
        ]
        leaf = tree_names.index((("value.py", "blob"),))
        nested = tree_names.index((("deep", "tree"),))
        source = tree_names.index(
            (
                ("app.py", "blob"),
                ("nested", "tree"),
            )
        )
        root = tree_names.index(
            (
                ("pyproject.toml", "blob"),
                ("src", "tree"),
                ("tests", "tree"),
                ("tools", "tree"),
                ("tree-shaped.bin", "blob"),
            )
        )
        assert leaf < nested < source < root
        root_entries = remote.tree_calls[root]
        tools_entry = next(entry for entry in root_entries if entry.path == "tools")
        assert tools_entry.type == "tree"
        executable_tree = next(
            entries
            for entries in remote.tree_calls
            if any(entry.path == "run.sh" and entry.mode == "100755" for entry in entries)
        )
        assert any(entry.path == "run.sh" and entry.mode == "100755" for entry in executable_tree)

        # A second, distinct run reuses the same content-addressed objects while
        # creating only its own deterministic commit and managed branch.
        object_count = len(remote.blobs)
        other = create_frozen_approval(t22_database, tmp_path)
        try:
            _decide(other)
            other_lease = _publication_lease(other, "t31-second-run")
            other_remote_calls_before = len(remote.mutations)
            second = _publish(_service(other, remote), other_lease, other)
            assert second.status == "published"
            assert second.branch != published.branch
            assert len(remote.blobs) == object_count
            assert len(remote.mutations) > other_remote_calls_before
        finally:
            _close_scenario(other)
    finally:
        _close_scenario(scenario)


def test_publication_resumes_with_at_most_two_remote_mutations_per_invocation(
    t22_database: T22Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        lease = _publication_lease(scenario)
        remote = _remote_for(scenario)
        service = _service(scenario, remote)
        monkeypatch.setattr(github_publication_module, "MAX_MUTATIONS_PER_SLICE", 2)

        invocation_counts: list[int] = []
        results = []
        for _ in range(12):
            before = len(remote.mutations)
            result = _publish(service, lease, scenario)
            invocation_counts.append(len(remote.mutations) - before)
            results.append(result)
            if result.status == "published":
                break

        assert results[-1].status == "published"
        assert len(results) > 1
        assert all(count <= 2 for count in invocation_counts)
        assert all(result.status in {"pending", "published"} for result in results)
        assert remote.create_ref_calls == 1
        mutation_counts: dict[tuple[str, str], int] = {}
        for mutation in remote.mutations:
            mutation_counts[mutation] = mutation_counts.get(mutation, 0) + 1
        assert all(count == 1 for count in mutation_counts.values())
        assert sum(1 for kind, _value in remote.mutations if kind != "ref") > 2
    finally:
        _close_scenario(scenario)


def test_cancelled_run_and_expired_approval_do_not_start_new_remote_mutations(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        lease = _publication_lease(scenario)
        remote = _remote_for(scenario)
        from continuum_api.runtime.cancellation import CancellationService

        CancellationService(t22_database.runtime_engine, scenario.principal).cancel(
            scenario.run.run_id
        )
        with pytest.raises(GitHubBranchPublicationError):
            _publish(_service(scenario, remote), lease, scenario)
        assert remote.mutations == []
    finally:
        _close_scenario(scenario)

    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        _decide(scenario)
        lease = _publication_lease(scenario)
        remote = _remote_for(scenario)
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET deadline_at="
                    "created_at+interval '1 microsecond' "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
            )
        with pytest.raises(GitHubBranchPublicationError):
            _publish(_service(scenario, remote), lease, scenario)
        assert remote.mutations == []
    finally:
        _close_scenario(scenario)

    scenario = create_frozen_approval(t22_database, tmp_path, approval_ttl=timedelta(seconds=1))
    try:
        _decide(scenario)
        lease = _publication_lease(scenario)
        remote = _remote_for(scenario)
        time.sleep(1.1)
        with pytest.raises(GitHubBranchPublicationError):
            _publish(_service(scenario, remote), lease, scenario)
        assert remote.mutations == []
    finally:
        _close_scenario(scenario)
