"""Trusted GitHub command execution shares browser decision transitions."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from test_agent_loop import (
    DATABASE_NAME,
    T22Database,
    _assert_postgres_17,
    _cleanup_database,
    _reset_database,
    _role_url,
    _run_migration,
)
from test_approvals import _REPOSITORY_ID, _close_scenario, create_frozen_approval

from continuum_api.auth.github import GitHubIssueCommentSnapshot, GitHubIssueSnapshot
from continuum_api.repositories.preflight import RepositoryAccessProof
from continuum_api.runtime.admission import AdmissionService
from continuum_api.runtime.approvals import ApprovalService, RepositoryPublicationFacts
from continuum_api.runtime.cancellation import CancellationService
from continuum_api.runtime.github_command_execution import (
    GitHubCommandProcessor,
    status_comment_body,
)
from continuum_api.runtime.github_commands import (
    GitHubCommandError,
    GitHubCommandService,
    IssueCommentCommandEvent,
)
from continuum_api.runtime.github_status_comments import (
    GitHubStatusCommentError,
    GitHubStatusCommentLedger,
    GitHubStatusCommentPublisher,
)
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.provider_credentials import CredentialService
from continuum_api.runtime.transitions import TransitionService


@pytest.fixture(scope="module")
def t38_database() -> Any:
    """Run against the complete schema including mandatory admission controls."""
    database_url = os.environ.get("CONTINUUM_T22_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T22_TEST_DATABASE_URL to disposable loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail(f"T38 requires loopback PostgreSQL named {DATABASE_NAME}")
    admin = create_engine(
        url.set(drivername="postgresql+psycopg"),
        hide_parameters=True,
        pool_size=8,
        max_overflow=0,
    )
    runtime_password = secrets.token_urlsafe(32)
    runtime_url = _role_url(url, runtime_password)
    runtime = None
    migrated = False
    try:
        _reset_database(admin, runtime_password)
        _assert_postgres_17(admin)
        _run_migration("upgrade", "head", database_url)
        migrated = True
        runtime = create_engine(
            runtime_url,
            hide_parameters=True,
            pool_size=6,
            max_overflow=0,
            connect_args={
                "application_name": "t38-cross-channel-controller",
                "connect_timeout": 5,
                "options": "-c statement_timeout=5000 -c lock_timeout=2000",
            },
        )
        yield T22Database(admin, runtime, runtime_url)
    finally:
        if runtime is not None:
            runtime.dispose()
        try:
            if migrated:
                _run_migration("downgrade", "20261012_10", database_url)
        finally:
            _cleanup_database(admin)
            admin.dispose()


class _CurrentComment:
    def __init__(self) -> None:
        self.value: GitHubIssueCommentSnapshot | None = None
        self.calls = 0

    async def current_issue_comment(
        self, *, repository_id: int, github_user_id: int, comment_id: int, issue_number: int
    ) -> GitHubIssueCommentSnapshot:
        self.calls += 1
        assert self.value is not None
        assert self.value.repository_id == repository_id
        assert self.value.author_github_id == github_user_id
        assert self.value.comment_id == comment_id
        assert self.value.issue_number == issue_number
        return self.value


class _StatusCommentTransport:
    def __init__(self) -> None:
        self.comments: dict[str, tuple[str | None, str | None]] = {}
        self.creates = 0
        self.lookups = 0
        self.lose_create_ack = False

    async def find_status_comment(
        self, repository_id: int, github_user_id: int, issue_number: int, marker: str
    ) -> str | None:
        del repository_id, github_user_id, issue_number
        self.lookups += 1
        return self.comments.get(marker, (None, None))[0]

    async def create_status_comment(
        self, repository_id: int, github_user_id: int, issue_number: int, body: str
    ) -> str:
        del repository_id, github_user_id, issue_number
        self.creates += 1
        comment_id = str(3000 + self.creates)
        marker = body.rsplit("\n\n", 1)[-1]
        self.comments[marker] = (comment_id, body)
        if self.lose_create_ack:
            raise RuntimeError("synthetic lost create response")
        return comment_id


def _event(
    body: str,
    *,
    repository_id: int,
    author_id: int,
    issue_id: int,
    issue_number: int,
) -> IssueCommentCommandEvent:
    now = datetime.now(UTC) + timedelta(seconds=1)
    return IssueCommentCommandEvent(
        action="created",
        repository_id=repository_id,
        issue_id=issue_id,
        issue_number=issue_number,
        comment_id=uuid4().int % 9_000_000_000 + 1,
        author_github_id=author_id,
        author_type="User",
        created_at=now,
        updated_at=now,
        body=body,
    )


def _snapshot(event: IssueCommentCommandEvent) -> GitHubIssueCommentSnapshot:
    return GitHubIssueCommentSnapshot(
        repository_id=event.repository_id,
        issue_id=event.issue_id,
        issue_number=event.issue_number,
        comment_id=event.comment_id,
        author_github_id=event.author_github_id,
        author_type=event.author_type,
        created_at=event.created_at,
        updated_at=event.updated_at,
        body=event.body,
        is_pull_request=False,
    )


def _register(store: T22Database, owner: Principal, repository_id: int) -> int:
    with store.admin_engine.begin() as connection:
        github_user_id = connection.execute(
            text("SELECT github_user_id FROM runtime_private.users WHERE user_id=:owner"),
            {"owner": owner.account_id},
        ).scalar_one()
        connection.execute(
            text(
                "INSERT INTO runtime_private.repository_registrations "
                "(owner_id, github_repository_id, repository_owner, repository_name, profile_id) "
                "VALUES (:owner, :repo, 'continuum-test', 'cross-channel', 'python-pytest')"
            ),
            {"owner": owner.account_id, "repo": repository_id},
        )
    return github_user_id


def _repository_facts(repository_id: int, github_user_id: int) -> RepositoryPublicationFacts:
    return RepositoryPublicationFacts(
        repository_id=repository_id,
        github_user_id=github_user_id,
        owner="owner",
        name=f"repo-{repository_id}",
        default_branch="main",
        base_commit_sha="a" * 40,
        access_proof=RepositoryAccessProof(
            repository_id=repository_id,
            github_user_id=github_user_id,
            actor_can_write=True,
            app_can_access=True,
        ),
    )


def _accept_and_process(
    store: T22Database,
    owner: Principal,
    github_user_id: int,
    repository_id: int,
    issue_id: int,
    issue_number: int,
    body: str,
    approvals: ApprovalService,
) -> tuple[Any, Any, _CurrentComment]:
    event = _event(
        body,
        repository_id=repository_id,
        author_id=github_user_id,
        issue_id=issue_id,
        issue_number=issue_number,
    )
    reader = _CurrentComment()
    reader.value = _snapshot(event)
    commands = GitHubCommandService(store.runtime_engine, owner, reader)
    accepted = asyncio.run(commands.accept_created_comment(event))
    assert accepted is not None
    reader.value = None  # accepted intent survives immediate post-acceptance deletion
    processor = GitHubCommandProcessor(
        store.runtime_engine,
        owner,
        approvals,
        CancellationService(store.runtime_engine, owner),
    )
    result = asyncio.run(processor.execute(accepted))
    return accepted, result, reader


def test_github_approval_uses_shared_transition_and_replays_result(
    t38_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t38_database, tmp_path)
    try:
        approval = scenario.frozen_receipt
        assert approval is not None
        repository_id = _REPOSITORY_ID
        issue_number = 1
        github_user_id = _register(t38_database, scenario.principal, repository_id)
        initial_authorization_calls = scenario.facts_provider.calls
        accepted, result, _reader = _accept_and_process(
            t38_database,
            scenario.principal,
            github_user_id,
            repository_id,
            77_001,
            issue_number,
            f"/continumm approve {approval.approval_id} {approval.manifest_sha256.hex()}",
            scenario.service,
        )
        assert result.kind == "decision"
        assert result.approval_status == "approved"
        assert result.run_id == scenario.run.run_id

        # Once accepted, deleting or editing the public comment does not erase
        # the immutable command intent or its idempotent result.
        _reader.value = None
        processor = GitHubCommandProcessor(
            t38_database.runtime_engine,
            scenario.principal,
            scenario.service,
            CancellationService(t38_database.runtime_engine, scenario.principal),
        )
        replay = asyncio.run(processor.execute(accepted))
        assert replay.duplicate is True
        assert replay.approval_status == "approved"
        assert scenario.facts_provider.calls - initial_authorization_calls == 3
    finally:
        _close_scenario(scenario)


def test_command_target_must_match_repository_issue_and_current_comment(
    t38_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t38_database, tmp_path)
    try:
        approval = scenario.frozen_receipt
        assert approval is not None
        repository_id = _REPOSITORY_ID
        issue_number = 1
        github_user_id = _register(t38_database, scenario.principal, repository_id)
        initial_authorization_calls = scenario.facts_provider.calls
        event = _event(
            f"/continumm approve {approval.approval_id} {approval.manifest_sha256.hex()}",
            repository_id=repository_id,
            author_id=github_user_id,
            issue_id=77_002,
            issue_number=issue_number + 1,
        )
        reader = _CurrentComment()
        reader.value = _snapshot(event)
        accepted = asyncio.run(
            GitHubCommandService(
                t38_database.runtime_engine, scenario.principal, reader
            ).accept_created_comment(event)
        )
        assert accepted is not None
        processor = GitHubCommandProcessor(
            t38_database.runtime_engine,
            scenario.principal,
            scenario.service,
            CancellationService(t38_database.runtime_engine, scenario.principal),
        )
        rejected = asyncio.run(processor.execute(accepted))
        assert rejected.kind == "rejected" and rejected.status == "rejected"
        assert scenario.facts_provider.calls - initial_authorization_calls == 1
    finally:
        _close_scenario(scenario)


@pytest.mark.parametrize(
    ("decision", "expected"),
    [("reject", "rejected"), ("changes", "changes")],
)
def test_github_reject_and_changes_use_shared_approval_transition(
    t38_database: T22Database,
    tmp_path: Path,
    decision: str,
    expected: str,
) -> None:
    scenario = create_frozen_approval(t38_database, tmp_path)
    try:
        approval = scenario.frozen_receipt
        assert approval is not None
        repository_id = _REPOSITORY_ID
        github_user_id = _register(t38_database, scenario.principal, repository_id)
        feedback = "Please adjust the generated test." if decision == "changes" else None
        body = f"/continumm {decision} {approval.approval_id}"
        if feedback is not None:
            body += f"\n{feedback}"
        accepted, result, _reader = _accept_and_process(
            t38_database,
            scenario.principal,
            github_user_id,
            repository_id,
            77_004,
            1,
            body,
            scenario.service,
        )
        assert accepted.command.kind == decision
        assert result.approval_status == expected
        with t38_database.admin_engine.connect() as connection:
            approval_row = connection.execute(
                text(
                    "SELECT approval.status, (SELECT feedback FROM "
                    "runtime_private.revision_feedback AS feedback "
                    "WHERE feedback.owner_id=approval.owner_id "
                    "AND feedback.approval_id=approval.approval_id) AS decided_feedback "
                    "FROM runtime_private.approvals AS approval "
                    "WHERE approval.owner_id=:owner AND approval.approval_id=:approval"
                ),
                {"owner": scenario.principal.account_id, "approval": approval.approval_id},
            ).one()
            assert approval_row.status == expected
            assert approval_row.decided_feedback == feedback
    finally:
        _close_scenario(scenario)


def test_github_cancel_uses_shared_cancellation_transition(
    t38_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t38_database, tmp_path)
    try:
        repository_id = _REPOSITORY_ID
        github_user_id = _register(t38_database, scenario.principal, repository_id)
        _accepted, result, _reader = _accept_and_process(
            t38_database,
            scenario.principal,
            github_user_id,
            repository_id,
            77_005,
            1,
            f"/continumm cancel {scenario.run.run_id}",
            scenario.service,
        )
        assert result.kind == "cancel"
        assert result.status == "cancelled"
        with t38_database.admin_engine.connect() as connection:
            status = connection.execute(
                text("SELECT status FROM runtime_private.runs WHERE run_id=:run"),
                {"run": scenario.run.run_id},
            ).scalar_one()
        assert status == "cancelled"
    finally:
        _close_scenario(scenario)


def test_status_comment_is_stable_and_contains_only_public_status() -> None:
    run_id = uuid4()
    console = "https://console.example.test"
    approval_id = uuid4()
    digest = "a" * 64
    expiry = datetime.now(UTC) + timedelta(hours=2)
    first = status_comment_body(
        run_id,
        "waiting_approval",
        3,
        console,
        event_sequence=12,
        approval_id=approval_id,
        approval_sha256=digest,
        approval_expires_at=expiry,
    )
    replay = status_comment_body(
        run_id,
        "waiting_approval",
        3,
        console,
        event_sequence=12,
        approval_id=approval_id,
        approval_sha256=digest,
        approval_expires_at=expiry,
    )
    assert first == replay
    assert f"<!-- continuum:status:{run_id}:12 -->" in first
    assert str(approval_id) in first and digest in first
    assert f"{console}/runs/{run_id}" in first
    assert "private" not in first.casefold()
    assert "stdout" not in first.casefold()
    assert "secret" not in first.casefold()


def test_status_comment_uses_ledger_identity_and_reconciles_once(
    t38_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t38_database, tmp_path)
    run_id = scenario.run.run_id
    owner = scenario.principal
    repository_id = _REPOSITORY_ID
    _register(t38_database, owner, repository_id)
    with t38_database.admin_engine.connect() as connection:
        status, revision = connection.execute(
            text("SELECT status, revision FROM runtime_private.runs WHERE run_id=:run"),
            {"run": run_id},
        ).one()
    transport = _StatusCommentTransport()
    ledger = GitHubStatusCommentLedger(t38_database.runtime_engine, owner)
    sequence = ledger.current_snapshot(run_id)["event_sequence"]
    publisher = GitHubStatusCommentPublisher(
        owner,
        ledger,
        scenario.service,
        transport,
        "https://console.example.test",
    )
    transport.lose_create_ack = True
    with pytest.raises(GitHubStatusCommentError, match="github_status_comment_effect_unknown"):
        asyncio.run(
            publisher.publish(
                run_id=run_id,
                repository_id=repository_id,
                issue_number=1,
                status=status,
                revision=revision,
            )
        )
    transport.lose_create_ack = False
    # The public snapshot advances before the lost acknowledgement is resolved.
    # Recovery must look up the old marker, never create its replacement.
    CancellationService(t38_database.runtime_engine, owner).cancel(run_id)
    replay_id = asyncio.run(publisher.reconcile(run_id, sequence))
    assert replay_id == "3001"
    assert transport.creates == 1 and transport.lookups == 2
    comment_body = next(iter(transport.comments.values()))[1]
    assert comment_body is not None
    assert "private" not in comment_body.casefold()
    assert "stdout" not in comment_body.casefold()
    assert str(scenario.frozen_receipt.approval_id) in comment_body

    with t38_database.admin_engine.connect() as connection:
        terminal_status, terminal_revision = connection.execute(
            text("SELECT status, revision FROM runtime_private.runs WHERE run_id=:run"),
            {"run": run_id},
        ).one()
    terminal_id = asyncio.run(
        publisher.publish(
            run_id=run_id,
            repository_id=repository_id,
            issue_number=1,
            status=terminal_status,
            revision=terminal_revision,
        )
    )
    assert terminal_id == "3002"
    assert transport.creates == 2 and transport.lookups == 3
    _close_scenario(scenario)


def test_run_command_uses_disabled_admission_service_by_default(
    t38_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t38_database, tmp_path)
    try:
        repository_id = _REPOSITORY_ID
        github_user_id = _register(t38_database, scenario.principal, repository_id)
        event = _event(
            "/continumm run",
            repository_id=repository_id,
            author_id=github_user_id,
            issue_id=77_003,
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
        processor = GitHubCommandProcessor(
            t38_database.runtime_engine,
            scenario.principal,
            scenario.service,
            CancellationService(t38_database.runtime_engine, scenario.principal),
            AdmissionService(t38_database.runtime_engine, scenario.principal, None, None),
        )
        rejected = asyncio.run(processor.execute(accepted))
        assert rejected.kind == "rejected" and rejected.status == "rejected"
        with t38_database.admin_engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT count(*) FROM runtime_private.runs WHERE owner_id=:owner"),
                    {"owner": scenario.principal.account_id},
                ).scalar_one()
                == 1
            )
            result = connection.execute(
                text(
                    "SELECT result FROM runtime_private.github_command_results "
                    "WHERE owner_id=:owner AND repository_id=:repo AND comment_id=:comment"
                ),
                {
                    "owner": scenario.principal.account_id,
                    "repo": repository_id,
                    "comment": event.comment_id,
                },
            ).scalar_one()
        assert result == {"kind": "rejected", "status": "rejected"}
    finally:
        _close_scenario(scenario)


def test_run_admission_replay_uses_persisted_command_identity_before_current_issue(
    t38_database: T22Database,
) -> None:
    owner = Principal(uuid4())
    repository_id = 830_000
    issue_number = 7
    with t38_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:owner, 930001)"
            ),
            {"owner": owner.account_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.repository_registrations "
                "(owner_id, github_repository_id, repository_owner, repository_name, "
                "profile_id, provider, eligibility_status, checked_sha, "
                "eligibility_checked_at, eligibility_receipt) "
                "VALUES (:owner, :repo, 'owner', 'retry-run', 'python-pytest', 'openai', "
                "'eligible', :sha, clock_timestamp(), CAST(:receipt AS jsonb))"
            ),
            {
                "owner": owner.account_id,
                "repo": repository_id,
                "sha": "a" * 40,
                "receipt": (
                    '{"repository_id":830000,"commit_sha":"' + "a" * 40 + '",'
                    '"tree_sha":"' + "b" * 40 + '","profile_id":"python-pytest",'
                    '"profile_version":"1","file_count":4,"source_bytes":256,'
                    '"manifest_blobs":[["pyproject.toml","' + "c" * 40 + '"]],'
                    '"package_count":1}'
                ),
            },
        )

    class _ChangingIssueSource:
        issue_reads = 0
        body = "First accepted issue content."

        async def current(self, requested_repository_id: int, github_user_id: int):
            return _repository_facts(requested_repository_id, github_user_id)

        async def current_issue(
            self, requested_repository_id: int, github_user_id: int, number: int
        ):
            self.issue_reads += 1
            facts = _repository_facts(requested_repository_id, github_user_id)
            issue_id = requested_repository_id * 10 + number
            title = f"Issue {number}"
            snapshot_body = self.body
            digest = hashlib.sha256(
                json.dumps(
                    {
                        "id": issue_id,
                        "number": number,
                        "title": title,
                        "body": snapshot_body,
                        "state": "open",
                        "updated_at": "2026-10-20T00:00:00+00:00",
                    },
                    sort_keys=True,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            return facts, GitHubIssueSnapshot(
                repository_id=requested_repository_id,
                github_user_id=github_user_id,
                issue_id=issue_id,
                issue_number=number,
                title=title,
                body=snapshot_body,
                state="open",
                updated_at="2026-10-20T00:00:00+00:00",
                sha256=digest,
            )

    source = _ChangingIssueSource()
    approvals = ApprovalService(
        t38_database.runtime_engine,
        owner,
        TransitionService(t38_database.runtime_engine, owner),
        None,
        publication_facts=source,
    )
    admissions = AdmissionService(
        t38_database.runtime_engine,
        owner,
        source,
        CredentialService(t38_database.runtime_engine, owner, None),
        enabled=True,
    )
    event = _event(
        "/continumm run",
        repository_id=repository_id,
        author_id=930_001,
        issue_id=repository_id * 10 + issue_number,
        issue_number=issue_number,
    )
    reader = _CurrentComment()
    reader.value = _snapshot(event)
    accepted = asyncio.run(
        GitHubCommandService(t38_database.runtime_engine, owner, reader).accept_created_comment(
            event
        )
    )
    assert accepted is not None
    processor = GitHubCommandProcessor(
        t38_database.runtime_engine,
        owner,
        approvals,
        CancellationService(t38_database.runtime_engine, owner),
        admissions,
    )

    async def lose_result_ack(_receipt: Any, _result: Any) -> None:
        raise GitHubCommandError("synthetic_result_ack_lost")

    processor._store_result = lose_result_ack
    with pytest.raises(GitHubCommandError, match="synthetic_result_ack_lost"):
        asyncio.run(processor.execute(accepted))
    assert source.issue_reads == 1

    source.body = "The issue was edited after command acceptance."
    replay = asyncio.run(
        GitHubCommandProcessor(
            t38_database.runtime_engine,
            owner,
            approvals,
            CancellationService(t38_database.runtime_engine, owner),
            admissions,
        ).execute(accepted)
    )
    assert replay.kind == "run" and replay.duplicate is False
    assert source.issue_reads == 1
    with t38_database.admin_engine.connect() as connection:
        run_count, mapped_run = connection.execute(
            text(
                "SELECT (SELECT count(*) FROM runtime_private.github_command_admissions "
                "WHERE owner_id=:owner AND repository_id=:repo AND comment_id=:comment), "
                "(SELECT run_id FROM runtime_private.github_command_admissions "
                "WHERE owner_id=:owner AND repository_id=:repo AND comment_id=:comment)"
            ),
            {
                "owner": owner.account_id,
                "repo": repository_id,
                "comment": event.comment_id,
            },
        ).one()
    assert run_count == 1 and mapped_run == replay.run_id
