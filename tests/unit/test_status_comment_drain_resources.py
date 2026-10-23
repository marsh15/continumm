from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import SecretStr

from continuum_api.runtime import wake_worker_resources as resources
from continuum_api.runtime.github_status_comments import (
    GitHubStatusCommentError,
    GitHubStatusCommentPublisher,
)


class _Engine:
    def dispose(self) -> None:
        pass


class _Storage:
    async def aclose(self) -> None:
        pass


class _Ledger:
    snapshot = {
        "event_sequence": 7,
        "github_repository_id": 17,
        "issue_number": 23,
        "status": "succeeded",
        "revision": 4,
    }

    def __init__(self, _engine, principal) -> None:
        self.principal = principal

    def current_snapshot(self, run_id):
        self.run_id = run_id
        return self.snapshot


class _Publisher:
    instances: list[_Publisher] = []
    fail_publish = False

    def __init__(self, principal, ledger, approvals, transport, console_url) -> None:
        self.principal = principal
        self.ledger = ledger
        self.console_url = console_url
        self.published = []
        self.reconciled = []
        self.__class__.instances.append(self)

    async def publish(self, **kwargs):
        if self.fail_publish:
            raise RuntimeError("provider request details must not escape")
        self.published.append(kwargs)
        return "3001"

    async def reconcile(self, run_id, event_sequence):
        self.reconciled.append((run_id, event_sequence))
        return "3001"


def _setup_resources(monkeypatch):
    from continuum_api.auth import github as github_auth
    from continuum_api.auth import sessions
    from continuum_api.connectivity import storage as storage_module
    from continuum_api.credentials import CredentialCipher
    from continuum_api.runtime import (
        approvals,
        database,
        github_status_comments,
        transitions,
        workflow_resources,
    )

    wake_engine, runtime_engine = _Engine(), _Engine()
    captured = {}

    class Worker:
        def __init__(self, store, process_inbox, run_workflow, *, publish_status):
            captured["publish_status"] = publish_status

    monkeypatch.setattr(workflow_resources, "_require_base_settings", lambda _settings: None)
    monkeypatch.setattr(
        workflow_resources, "_require_sandbox_configuration", lambda _settings: None
    )
    monkeypatch.setattr(resources, "create_wake_worker_engine", lambda *_a, **_k: wake_engine)
    monkeypatch.setattr(database, "create_runtime_engine", lambda *_a, **_k: runtime_engine)
    monkeypatch.setattr(CredentialCipher, "from_settings", staticmethod(lambda _settings: object()))
    monkeypatch.setattr(storage_module, "SupabaseStorage", lambda *_a, **_k: _Storage())
    monkeypatch.setattr(sessions, "GitHubUserTokenStore", lambda *_a, **_k: object())
    monkeypatch.setattr(github_auth, "GitHubAuthorizationClient", lambda **_k: object())
    monkeypatch.setattr(github_auth, "GitHubPublicationFactsProvider", lambda *_a: object())
    monkeypatch.setattr(transitions, "TransitionService", lambda *_a: object())
    monkeypatch.setattr(approvals, "ApprovalService", lambda *_a, **_k: object())
    monkeypatch.setattr(github_status_comments, "GitHubStatusCommentLedger", _Ledger)
    monkeypatch.setattr(github_status_comments, "GitHubStatusCommentPublisher", _Publisher)
    monkeypatch.setattr(resources, "BoundedWakeWorker", Worker)
    _Publisher.instances.clear()
    _Publisher.fail_publish = False

    settings = SimpleNamespace(
        enable_wake_worker=True,
        environment="test",
        wake_worker_database_url=SecretStr("postgresql://continuum_wake_worker:pw@localhost/db"),
        runtime_database_url=SecretStr("postgresql://continuum_runtime:pw@localhost/db"),
        encryption_key=SecretStr("key"),
        github_app_id=73,
        github_app_private_key=SecretStr("private"),
        github_public_origin="https://console.example.test",
        supabase_url="https://storage.example",
        supabase_secret_key=SecretStr("storage-secret"),
        supabase_bucket="private",
    )
    built = asyncio.run(resources.build_wake_worker_resources(settings))
    return built, captured["publish_status"]


def test_status_callback_publishes_only_when_selected_event_is_still_current(monkeypatch) -> None:
    built, publish_status = _setup_resources(monkeypatch)
    owner_id, run_id = uuid4(), uuid4()
    work = SimpleNamespace(
        owner_id=owner_id,
        run_id=run_id,
        event_sequence=7,
        snapshot_key=b"snapshot-key",
        reconcile_only=False,
    )

    assert asyncio.run(publish_status(work)) is True
    publisher = _Publisher.instances[-1]
    assert publisher.principal.account_id == owner_id
    assert publisher.ledger.run_id == run_id
    assert publisher.console_url == "https://console.example.test"
    assert publisher.published == [
        {
            "run_id": run_id,
            "repository_id": 17,
            "issue_number": 23,
            "status": "succeeded",
            "revision": 4,
            "expected_event_sequence": 7,
        }
    ]

    _Ledger.snapshot = {**_Ledger.snapshot, "event_sequence": 8}
    assert asyncio.run(publish_status(work)) is False
    assert _Publisher.instances[-1].published == []
    _Ledger.snapshot = {**_Ledger.snapshot, "event_sequence": 7}
    asyncio.run(built.aclose())


def test_status_callback_reconciles_and_contains_publisher_failures(monkeypatch) -> None:
    built, publish_status = _setup_resources(monkeypatch)
    work = SimpleNamespace(
        owner_id=uuid4(),
        run_id=uuid4(),
        event_sequence=2,
        snapshot_key=b"old-snapshot",
        reconcile_only=True,
    )
    assert asyncio.run(publish_status(work)) is True
    assert _Publisher.instances[-1].reconciled == [(work.run_id, 2)]

    _Publisher.fail_publish = True
    fresh = SimpleNamespace(**{**vars(work), "reconcile_only": False, "event_sequence": 7})
    assert asyncio.run(publish_status(fresh)) is False
    asyncio.run(built.aclose())


def test_invalid_status_console_origin_fails_before_engine_creation(monkeypatch) -> None:
    from continuum_api.runtime import workflow_resources

    attempted_engine_creation = False

    def create_engine(*_args, **_kwargs):
        nonlocal attempted_engine_creation
        attempted_engine_creation = True
        raise AssertionError("engines must not be created for invalid public origin")

    monkeypatch.setattr(workflow_resources, "_require_base_settings", lambda _settings: None)
    monkeypatch.setattr(
        workflow_resources, "_require_sandbox_configuration", lambda _settings: None
    )
    monkeypatch.setattr(resources, "create_wake_worker_engine", create_engine)
    settings = SimpleNamespace(enable_wake_worker=True, github_public_origin="http://insecure")

    with pytest.raises(
        resources.WakeWorkerConfigurationError, match="github_public_origin_invalid"
    ):
        asyncio.run(resources.build_wake_worker_resources(settings))
    assert attempted_engine_creation is False


class _ReconcileLedger:
    def __init__(self, effect):
        self.effect = effect
        self.snapshot_reads = 0
        self.adopted = []

    def read_effect(self, run_id, event_sequence):
        assert (run_id, event_sequence) == (self.effect.run_id, self.effect.event_sequence)
        return self.effect

    def current_snapshot(self, run_id):
        self.snapshot_reads += 1
        assert run_id == self.effect.run_id
        return {
            "github_repository_id": self.effect.repository_id,
            "issue_number": self.effect.issue_number,
            "github_user_id": 31,
        }

    def adopt_existing(self, effect, remote_id):
        self.adopted.append(remote_id)
        return SimpleNamespace(remote_comment_id=remote_id)


class _Transport:
    def __init__(self, remote_id):
        self.remote_id = remote_id
        self.lookups = []
        self.creates = []

    async def find_status_comment(self, repository_id, github_user_id, issue_number, marker):
        self.lookups.append((repository_id, github_user_id, issue_number, marker))
        return self.remote_id

    async def create_status_comment(self, *args):
        self.creates.append(args)
        return "3002"


def _reconcile_publisher(effect, transport):
    approvals = SimpleNamespace(
        assert_current_repository_write_access=lambda repository_id, github_user_id: _async_none(
            repository_id, github_user_id
        )
    )
    ledger = _ReconcileLedger(effect)
    publisher = GitHubStatusCommentPublisher(
        SimpleNamespace(account_id=effect.owner_id),
        ledger,
        approvals,
        transport,
        "https://console.example.test",
    )
    return publisher, ledger


async def _async_none(*_args):
    return None


def _effect(status):
    return SimpleNamespace(
        owner_id=uuid4(),
        run_id=uuid4(),
        event_sequence=3,
        repository_id=17,
        issue_number=23,
        status_snapshot="running",
        revision=2,
        marker="<!-- continuum:status:run:3 -->",
        body="old body",
        request_sha256=b"digest",
        effect_status=status,
        dispatch_token=uuid4() if status in {"unknown", "dispatching"} else None,
        remote_comment_id="3001" if status == "confirmed" else None,
    )


def test_reconcile_returns_confirmed_id_after_current_owner_run_check_without_lookup() -> None:
    effect = _effect("confirmed")
    transport = _Transport(None)
    publisher, ledger = _reconcile_publisher(effect, transport)

    assert asyncio.run(publisher.reconcile(effect.run_id, effect.event_sequence)) == "3001"
    assert ledger.snapshot_reads == 1
    assert transport.lookups == []
    assert transport.creates == []


def test_reconcile_adopts_old_unknown_comment_but_never_creates() -> None:
    effect = _effect("unknown")
    transport = _Transport("4001")
    publisher, ledger = _reconcile_publisher(effect, transport)

    assert asyncio.run(publisher.reconcile(effect.run_id, effect.event_sequence)) == "4001"
    assert ledger.adopted == ["4001"]
    assert transport.lookups == [(17, 31, 23, effect.marker)]
    assert transport.creates == []


@pytest.mark.parametrize("status", ["unknown", "dispatching"])
def test_reconcile_missing_comment_stays_unresolved_and_never_creates(status) -> None:
    effect = _effect(status)
    transport = _Transport(None)
    publisher, _ledger = _reconcile_publisher(effect, transport)

    with pytest.raises(GitHubStatusCommentError, match="github_status_comment_effect_unresolved"):
        asyncio.run(publisher.reconcile(effect.run_id, effect.event_sequence))
    assert transport.creates == []


def test_reconcile_refuses_prepared_effect_without_remote_lookup() -> None:
    effect = _effect("prepared")
    transport = _Transport("4001")
    publisher, ledger = _reconcile_publisher(effect, transport)

    with pytest.raises(GitHubStatusCommentError, match="github_status_comment_effect_unresolved"):
        asyncio.run(publisher.reconcile(effect.run_id, effect.event_sequence))
    assert ledger.snapshot_reads == 1
    assert transport.lookups == []
    assert transport.creates == []


def test_publish_rejects_a_new_event_before_approval_or_remote_lookup() -> None:
    run_id = uuid4()

    class Ledger:
        def current_snapshot(self, requested_run_id):
            assert requested_run_id == run_id
            return {
                "status": "running",
                "revision": 5,
                "github_repository_id": 17,
                "issue_number": 23,
                "github_user_id": 31,
                "event_sequence": 9,
                "approval_id": None,
                "approval_sha256": None,
                "approval_expires_at": None,
            }

        def prepare(self, **_kwargs):
            raise AssertionError("a different event must not prepare an effect")

    class Approvals:
        calls = 0

        async def assert_current_repository_write_access(self, *_args):
            self.calls += 1

    approvals = Approvals()
    transport = _Transport(None)
    publisher = GitHubStatusCommentPublisher(
        SimpleNamespace(account_id=uuid4()),
        Ledger(),
        approvals,
        transport,
        "https://console.example.test",
    )

    with pytest.raises(GitHubStatusCommentError, match="github_status_comment_run_changed"):
        asyncio.run(
            publisher.publish(
                run_id=run_id,
                repository_id=17,
                issue_number=23,
                status="running",
                revision=5,
                expected_event_sequence=8,
            )
        )
    assert approvals.calls == 0
    assert transport.lookups == []
    assert transport.creates == []
