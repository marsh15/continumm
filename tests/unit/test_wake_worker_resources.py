from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import SecretStr

from continuum_api.runtime import wake_worker_resources as resources
from continuum_api.runtime.github_commands import GitHubCommandError, GitHubCommandReceipt
from continuum_api.runtime.inbox_events import (
    parse_public_issue_comment,
    parse_public_issue_comment_identity,
)
from continuum_api.runtime.wake_worker_store import InboxWork, RunWork


def _payload(*, body: str = "/continumm status 123e4567-e89b-42d3-a456-426614174000") -> dict:
    timestamp = "2026-10-22T12:00:00Z"
    return {
        "action": "created",
        "repository": {"id": 17},
        "issue": {"id": 19, "number": 23},
        "comment": {
            "id": 29,
            "user": {"id": 31, "type": "User"},
            "created_at": timestamp,
            "updated_at": timestamp,
            "body": body,
        },
    }


def test_public_issue_comment_parser_extracts_only_bounded_normalized_fields() -> None:
    event = parse_public_issue_comment(_payload())

    assert event is not None
    assert event.repository_id == 17
    assert event.issue_id == 19
    assert event.issue_number == 23
    assert event.comment_id == 29
    assert event.author_github_id == 31
    assert event.created_at == datetime(2026, 10, 22, 12, tzinfo=UTC)
    assert event.updated_at == event.created_at
    assert event.is_pull_request is False


def test_public_issue_comment_identity_is_available_for_replay_lookup_after_edit() -> None:
    payload = _payload()
    payload["action"] = "edited"

    assert parse_public_issue_comment(payload) is None
    assert parse_public_issue_comment_identity(payload) == (17, 29, 31)
    assert parse_public_issue_comment_identity({"repository": {"id": True}}) is None


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {**_payload(), "action": "edited"},
        {**_payload(), "repository": {"id": True}},
        {**_payload(), "issue": {"id": 19, "number": 23, "pull_request": {}}},
        {
            **_payload(),
            "comment": {
                **_payload()["comment"],
                "user": {"id": 31, "type": "Bot"},
            },
        },
        {
            **_payload(),
            "comment": {**_payload()["comment"], "created_at": "no timestamp"},
        },
        _payload(body="x" * 16_385),
    ],
)
def test_public_issue_comment_parser_returns_inert_for_unsupported_or_invalid_payloads(
    payload: object,
) -> None:
    assert parse_public_issue_comment(payload) is None


def test_wake_worker_database_url_requires_dedicated_role_and_tls_for_nonlocal() -> None:
    local = resources._validated_worker_url(
        "postgresql://continuum_wake_worker:pw@localhost/db", production=False
    )
    assert local.query["sslmode"] == "disable"

    external = resources._validated_worker_url(
        "postgresql://continuum_wake_worker:pw@db.example/db", production=False
    )
    assert external.query == {"sslmode": "verify-full", "sslrootcert": "system"}

    with pytest.raises(resources.WakeWorkerConfigurationError):
        resources._validated_worker_url(
            "postgresql://continuum_runtime:pw@localhost/db", production=False
        )
    with pytest.raises(resources.WakeWorkerConfigurationError):
        resources._validated_worker_url(
            "postgresql://continuum_wake_worker:pw@db.example/db?sslmode=require",
            production=False,
        )


def test_disabled_worker_is_refused_before_importing_or_constructing_dependencies() -> None:
    with pytest.raises(resources.WakeWorkerConfigurationError, match="wake_worker_disabled"):
        asyncio.run(
            resources.build_wake_worker_resources(SimpleNamespace(enable_wake_worker=False))
        )


def test_resource_callbacks_check_app_identity_and_close_run_resources(monkeypatch) -> None:
    from continuum_api.auth import github as github_auth
    from continuum_api.auth import sessions
    from continuum_api.connectivity import storage as storage_module
    from continuum_api.credentials import CredentialCipher
    from continuum_api.runtime import (
        admission,
        approvals,
        cancellation,
        database,
        github_command_execution,
        github_commands,
        provider_credentials,
        scheduler,
        transitions,
        workflow_resources,
    )
    from continuum_api.runtime.github_command_execution import GitHubCommandResult

    class Engine:
        disposed = False

        def dispose(self):
            self.disposed = True

    class Storage:
        closed = False

        def __init__(self, *_args, **_kwargs):
            pass

        async def aclose(self):
            self.closed = True

    captured: dict[str, object] = {}

    class Worker:
        def __init__(self, store, process_inbox, run_workflow):
            captured.update(store=store, process_inbox=process_inbox, run_workflow=run_workflow)

    owner_id, run_id = uuid4(), uuid4()
    result = SimpleNamespace(run_id=run_id, jobs_started=1, stop_reason="idle")
    run_resource = SimpleNamespace(
        runner=SimpleNamespace(run_admitted=lambda *_args, **_kwargs: _async_result(result)),
        closed=False,
        aclose=lambda: _close(run_resource),
    )

    class Intake:
        def __init__(self, *_args, **_kwargs):
            pass

        async def accept_created_comment(self, event):
            captured["event"] = event
            return GitHubCommandReceipt(
                event.repository_id,
                event.comment_id,
                event.issue_number,
                datetime.now(UTC),
                datetime.now(UTC),
                SimpleNamespace(kind="status", metadata=lambda: {"kind": "status"}),
            )

        def pending(self, *, limit):
            assert limit == 500
            return (
                GitHubCommandReceipt(
                    17,
                    29,
                    23,
                    datetime.now(UTC),
                    datetime.now(UTC),
                    SimpleNamespace(kind="status", metadata=lambda: {"kind": "status"}),
                ),
            )

    class Processor:
        def __init__(self, *_args, **_kwargs):
            pass

        async def execute(self, receipt):
            captured["receipt"] = receipt
            return GitHubCommandResult("status", status="queued")

    wake_engine, runtime_engine = Engine(), Engine()
    monkeypatch.setattr(workflow_resources, "_require_base_settings", lambda _settings: None)
    monkeypatch.setattr(
        workflow_resources, "_require_sandbox_configuration", lambda _settings: None
    )
    monkeypatch.setattr(
        resources, "create_wake_worker_engine", lambda *_args, **_kwargs: wake_engine
    )
    monkeypatch.setattr(database, "create_runtime_engine", lambda *_args, **_kwargs: runtime_engine)
    monkeypatch.setattr(CredentialCipher, "from_settings", staticmethod(lambda _settings: object()))
    monkeypatch.setattr(storage_module, "SupabaseStorage", Storage)
    monkeypatch.setattr(sessions, "GitHubUserTokenStore", lambda *_args: object())
    monkeypatch.setattr(github_auth, "GitHubAuthorizationClient", lambda **_kwargs: object())
    monkeypatch.setattr(github_auth, "GitHubPublicationFactsProvider", lambda _auth: object())
    monkeypatch.setattr(transitions, "TransitionService", lambda *_args: object())
    monkeypatch.setattr(approvals, "ApprovalService", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(provider_credentials, "CredentialService", lambda *_args: object())
    monkeypatch.setattr(admission, "AdmissionService", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(cancellation, "CancellationService", lambda *_args: object())
    monkeypatch.setattr(
        scheduler,
        "RuntimeRecoveryService",
        lambda *_args: SimpleNamespace(
            recover_due=lambda **kwargs: (
                captured.setdefault("callback_order", []).append(("recover", kwargs["limit"])) or 0
            )
        ),
    )
    monkeypatch.setattr(github_commands, "GitHubCommandService", Intake)
    monkeypatch.setattr(github_command_execution, "GitHubCommandProcessor", Processor)
    monkeypatch.setattr(resources, "BoundedWakeWorker", Worker)

    async def build_workflow(_settings, _owner, _run):
        captured.setdefault("callback_order", []).append("build")
        captured["workflow_identity"] = (_owner, _run)
        return run_resource

    monkeypatch.setattr(workflow_resources, "build_workflow_resources", build_workflow)
    settings = SimpleNamespace(
        enable_wake_worker=True,
        enable_public_admission=True,
        environment="test",
        wake_worker_database_url=SecretStr("postgresql://continuum_wake_worker:pw@localhost/db"),
        runtime_database_url=SecretStr("postgresql://continuum_runtime:pw@localhost/db"),
        encryption_key=SecretStr("key"),
        github_app_id=73,
        github_app_private_key=SecretStr("private"),
        supabase_url="https://storage.example",
        supabase_secret_key=SecretStr("storage-secret"),
        supabase_bucket="private",
    )

    built = asyncio.run(resources.build_wake_worker_resources(settings))
    process_inbox = captured["process_inbox"]
    run_workflow = captured["run_workflow"]

    async def exercise_callbacks() -> None:
        assert (
            await process_inbox(
                InboxWork(uuid4(), "issue_comment", _payload(), owner_id, github_app_id=72)
            )
            == "inert"
        )
        assert (
            await process_inbox(
                InboxWork(uuid4(), "issue_comment", _payload(), owner_id, github_app_id=73)
            )
            == "processed"
        )
        assert captured["event"].comment_id == 29

        edited_payload = _payload()
        edited_payload["action"] = "edited"
        assert (
            await process_inbox(
                InboxWork(
                    uuid4(),
                    "issue_comment",
                    edited_payload,
                    None,
                    github_app_id=73,
                    command_state="completed",
                )
            )
            == "processed"
        )
        assert (
            await process_inbox(
                InboxWork(
                    uuid4(),
                    "issue_comment",
                    edited_payload,
                    owner_id,
                    github_app_id=73,
                    command_state="accepted_unresolved",
                )
            )
            == "processed"
        )
        assert captured["receipt"].repository_id == 17
        assert (
            await process_inbox(
                InboxWork(
                    uuid4(),
                    "issue_comment",
                    _payload(),
                    owner_id,
                    github_app_id=73,
                    command_state="accepted_unresolved",
                )
            )
            == "processed"
        )
        assert (
            await process_inbox(
                InboxWork(
                    uuid4(),
                    "issue_comment",
                    edited_payload,
                    None,
                    github_app_id=73,
                    command_state="expired",
                )
            )
            == "rejected"
        )
        try:
            await process_inbox(
                InboxWork(
                    uuid4(),
                    "issue_comment",
                    edited_payload,
                    None,
                    github_app_id=73,
                    command_state="accepted_unresolved",
                )
            )
        except GitHubCommandError as error:
            assert error.code == "github_command_result_pending" and error.retryable
        else:
            raise AssertionError("unresolved accepted command must stay pending")

        run_receipt = await run_workflow(RunWork(owner_id, run_id), 3, 12.5)
        assert run_receipt is result
        assert captured["workflow_identity"] == (owner_id, run_id)
        assert run_resource.closed is True
        assert captured["callback_order"] == [("recover", 3), "build"]

        await built.aclose()

    asyncio.run(exercise_callbacks())
    assert built.storage.closed is True
    assert runtime_engine.disposed is True
    assert wake_engine.disposed is True


async def _async_result(value):
    return value


async def _close(resource):
    resource.closed = True
