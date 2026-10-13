"""T15 checkpoint storage lifecycle tests use only bounded local fixtures."""

from __future__ import annotations

import asyncio
import gzip
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
import pytest

from continuum_api.artifacts.archives import ArchiveFile, create_archive
from continuum_api.artifacts.checkpoints import (
    CheckpointStorageError,
    CheckpointStorageService,
)
from continuum_api.connectivity.storage import (
    StorageFailure,
    SupabaseStorage,
)
from continuum_api.repositories.preflight import (
    GitHubRepositoryAdapter,
    RepositoryAccessProof,
    RepositoryPreflightError,
)
from continuum_api.runtime.artifact_persistence import (
    ArtifactCapacityExceeded,
    ArtifactIdentityConflict,
    ArtifactReference,
    ArtifactUploadReservation,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost
from continuum_api.runtime.persistence import Principal, RunStatus
from continuum_api.runtime.scheduler import FailureClass
from continuum_api.runtime.transitions import TransitionReceipt

_PROJECT_URL = "https://project-ref.supabase.co"
_SECRET = "sb_secret_local-test-key"
_BUCKET = "private-checkpoints"
_ARCHIVE = create_archive((ArchiveFile("src/main.py", b"print('ready')\n"),))


def _async_test(function: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(function)
    def run(*args: Any, **kwargs: Any) -> Any:
        return asyncio.run(function(*args, **kwargs))

    return run


class _FakePersistence:
    def __init__(
        self,
        *,
        status: str = "reserved",
        is_new: bool = True,
        reference: ArtifactReference | None = None,
        reserve_error: Exception | None = None,
    ) -> None:
        self.reservation = ArtifactUploadReservation(
            uuid4(), "checkpoints/00000000-0000-0000-0000-000000000001.tar.gz", is_new, status
        )
        self.reference = reference
        self.reserve_error = reserve_error
        self.calls: list[tuple[Any, ...]] = []
        self.mark_uploading_result = True
        self.intent_sha256: bytes | None = None

    def reserve_upload(self, *args: Any) -> ArtifactUploadReservation:
        self.calls.append(("reserve", *args))
        if self.reserve_error is not None:
            raise self.reserve_error
        requested_intent = args[-1]
        if self.intent_sha256 is not None and self.intent_sha256 != requested_intent:
            raise ArtifactIdentityConflict()
        self.intent_sha256 = requested_intent
        return self.reservation

    def get_upload_access_context(self, *_args: Any) -> tuple[int, int]:
        self.calls.append(("get_upload_access_context", *_args))
        return (123456, 987654)

    def mark_uploading(self, *args: Any) -> bool:
        self.calls.append(("mark_uploading", *args))
        if self.mark_uploading_result:
            self.reservation = replace(self.reservation, status="uploading", is_new=False)
        return self.mark_uploading_result

    def mark_unknown(self, *args: Any) -> None:
        self.calls.append(("mark_unknown", *args))
        self.reservation = replace(self.reservation, status="unknown", is_new=False)

    def mark_orphan(self, *args: Any) -> None:
        self.calls.append(("mark_orphan", *args))
        self.reservation = replace(self.reservation, status="orphan", is_new=False)

    def mark_verified(self, *args: Any) -> None:
        self.calls.append(("mark_verified", *args))
        self.reservation = replace(self.reservation, status="verified", is_new=False)

    def release_absent(self, *args: Any) -> None:
        self.calls.append(("release_absent", *args))
        self.reservation = replace(self.reservation, status="released", is_new=False)

    def get_committed(self, *_args: Any) -> ArtifactReference:
        self.calls.append(("get_committed", *_args))
        if self.reference is None:
            raise AssertionError("test did not provide a committed artifact reference")
        return self.reference


class _FakeTransitions:
    def __init__(self, *, error: Exception | None = None, adopted: bool = False) -> None:
        self.error = error
        self.adopted = adopted
        self.calls: list[dict[str, Any]] = []

    def commit_result(self, **kwargs: Any) -> TransitionReceipt:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return _transition_receipt(artifact_id=kwargs.get("artifact_id"), adopted=self.adopted)


class _FakeRecovery:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple[ExecutionLease, FailureClass]] = []

    def fail_attempt(self, *, lease: ExecutionLease, failure_class: FailureClass) -> None:
        self.calls.append((lease, failure_class))
        if self.error is not None:
            raise self.error


class _AllowVerifier:
    def __init__(self, *, actor_can_write: bool = True, app_can_access: bool = True) -> None:
        self.actor_can_write = actor_can_write
        self.app_can_access = app_can_access
        self.calls: list[tuple[int, int]] = []

    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
        self.calls.append((repository_id, github_user_id))
        return RepositoryAccessProof(
            repository_id,
            github_user_id,
            self.actor_can_write,
            self.app_can_access,
        )


class _DenyVerifier:
    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
        del repository_id, github_user_id
        raise RepositoryPreflightError("repository_authorization_pending")


class _StorageFixture:
    def __init__(
        self,
        *,
        post_status: int = 201,
        post_delay_seconds: float = 0.0,
        get_responses: list[tuple[int, bytes, dict[str, str]]] | None = None,
    ) -> None:
        self.post_status = post_status
        self.post_delay_seconds = post_delay_seconds
        self.get_responses = list(
            get_responses if get_responses is not None else [(200, _ARCHIVE.archive, {})]
        )
        self.requests: list[httpx.Request] = []
        self.storage = SupabaseStorage(
            _PROJECT_URL,
            _SECRET,
            _BUCKET,
            transport=httpx.MockTransport(self.handle),
            fixture_byte_limit=10 * 1024 * 1024,
        )

    async def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if "/bucket/" in request.url.path:
            return httpx.Response(200, json={"public": False}, request=request)
        if request.method == "POST":
            if self.post_delay_seconds:
                await asyncio.sleep(self.post_delay_seconds)
            return httpx.Response(self.post_status, request=request)
        if request.method == "GET":
            status, body, headers = self.get_responses.pop(0)
            return httpx.Response(status, content=body, headers=headers, request=request)
        raise AssertionError(f"unexpected request {request.method} {request.url.path}")

    async def aclose(self) -> None:
        await self.storage.aclose()


def _principal_and_lease() -> tuple[Principal, ExecutionLease]:
    owner_id = uuid4()
    return Principal(owner_id), ExecutionLease(
        owner_id=owner_id,
        run_id=uuid4(),
        step_id=uuid4(),
        job_id=uuid4(),
        attempt_id=uuid4(),
        generation=1,
        lease_owner="unit-checkpoint-worker",
        lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )


def _service(
    storage: _StorageFixture,
    persistence: _FakePersistence | None = None,
    transitions: _FakeTransitions | None = None,
    recovery: _FakeRecovery | None = None,
    *,
    access_verifier: object | None = None,
    github_requests: list[httpx.Request] | None = None,
    github_response: dict[str, Any] | None = None,
    deadline_seconds: float = 60.0,
) -> tuple[CheckpointStorageService, _FakePersistence, _FakeTransitions, _FakeRecovery]:
    chosen_persistence = persistence or _FakePersistence()
    chosen_transitions = transitions or _FakeTransitions()
    chosen_recovery = recovery or _FakeRecovery()
    requests = github_requests if github_requests is not None else []
    response = github_response or {
        "id": 123456,
        "name": "continuum-fixture",
        "owner": {"login": "fixture-owner"},
        "size": 1,
        "private": False,
        "visibility": "public",
        "archived": False,
        "fork": False,
    }

    def github_adapter_factory() -> GitHubRepositoryAdapter:
        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            assert request.url.scheme == "https"
            assert request.url.host == "api.github.com"
            return httpx.Response(200, json=response, request=request)

        return GitHubRepositoryAdapter(github_transport=httpx.MockTransport(handler))

    service = CheckpointStorageService(
        cast(Any, chosen_persistence),
        storage.storage,
        cast(Any, chosen_transitions),
        cast(Any, chosen_recovery),
        access_verifier=cast(Any, access_verifier or _AllowVerifier()),
        github_adapter_factory=github_adapter_factory,
        deadline_seconds=deadline_seconds,
    )
    return service, chosen_persistence, chosen_transitions, chosen_recovery


def _transition_receipt(
    *, artifact_id: UUID | None = None, adopted: bool = False
) -> TransitionReceipt:
    return TransitionReceipt(
        result_id=uuid4(),
        run_id=uuid4(),
        step_id=uuid4(),
        job_id=uuid4(),
        result_sha256=b"r" * 32,
        result={"accepted": True},
        run_status=RunStatus.WAITING_APPROVAL,
        event_sequence=3,
        next_step_id=None,
        next_job_id=None,
        adopted=adopted,
        artifact_id=artifact_id,
    )


@_async_test
async def test_new_checkpoint_posts_once_reads_back_and_commits_after_verification() -> None:
    storage = _StorageFixture()
    persistence = _FakePersistence()
    transitions = _FakeTransitions()
    verifier = _AllowVerifier()
    github_requests: list[httpx.Request] = []
    service, _, _, _ = _service(
        storage,
        persistence,
        transitions,
        access_verifier=verifier,
        github_requests=github_requests,
    )
    principal, lease = _principal_and_lease()

    receipt = await service.commit_checkpoint(
        principal,
        lease,
        "tool:one",
        (ArchiveFile("src/main.py", b"print('ready')\n"),),
        result={"tool": "finished"},
        target_run_status=RunStatus.WAITING_APPROVAL,
    )

    requests = storage.requests
    assert [request.method for request in requests] == ["GET", "POST", "GET"]
    post_request = requests[1]
    assert post_request.headers["x-upsert"] == "false"
    assert post_request.headers["accept-encoding"] == "identity"
    assert post_request.read() == _ARCHIVE.archive
    assert all(request.headers["accept-encoding"] == "identity" for request in requests)
    assert receipt.artifact_id == persistence.reservation.upload_id
    assert receipt.archive_sha256 == _ARCHIVE.archive_sha256
    assert receipt.tree_sha256 == _ARCHIVE.manifest.tree_sha256
    assert receipt.uploaded is True
    assert receipt.adopted is False
    assert [call[0] for call in persistence.calls] == [
        "reserve",
        "get_upload_access_context",
        "mark_uploading",
        "get_upload_access_context",
        "mark_verified",
    ]
    assert verifier.calls == [(123456, 987654), (123456, 987654)]
    assert [request.url.path for request in github_requests] == [
        "/repositories/123456",
        "/repositories/123456",
    ]
    assert transitions.calls[0]["artifact_id"] == receipt.artifact_id
    assert transitions.calls[0]["artifact_id"] == receipt.transition.artifact_id
    await storage.aclose()


@_async_test
async def test_total_deadline_cancels_slow_post_and_replay_never_posts_again() -> None:
    storage = _StorageFixture(post_delay_seconds=0.3, get_responses=[])
    persistence = _FakePersistence()
    service, _, transitions, _ = _service(
        storage,
        persistence,
        deadline_seconds=0.1,
    )
    principal, lease = _principal_and_lease()
    files = (ArchiveFile("src/main.py", b"ready"),)

    with pytest.raises(CheckpointStorageError, match="artifact_checkpoint_timeout"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            files,
            result={"tool": "finished"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )
    assert persistence.reservation.status == "uploading"
    storage.post_delay_seconds = 0
    storage.get_responses.append((404, b"", {}))
    with pytest.raises(CheckpointStorageError, match="artifact_upload_outcome_unknown"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            files,
            result={"tool": "finished"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )

    assert [request.method for request in storage.requests].count("POST") == 1
    assert transitions.calls == []
    await storage.aclose()


@_async_test
async def test_capacity_rejection_happens_before_any_storage_request() -> None:
    storage = _StorageFixture()
    persistence = _FakePersistence(reserve_error=ArtifactCapacityExceeded())
    service, _, transitions, _ = _service(storage, persistence)
    principal, lease = _principal_and_lease()

    with pytest.raises(CheckpointStorageError, match="artifact_capacity_exceeded"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            (ArchiveFile("src/main.py", b"ready"),),
            result={"tool": "finished"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )

    assert storage.requests == []
    assert transitions.calls == []
    await storage.aclose()


@_async_test
async def test_unknown_post_and_404_are_charged_and_replayed_read_only() -> None:
    storage = _StorageFixture(
        post_status=500,
        get_responses=[(404, b"", {}), (404, b"", {})],
    )
    persistence = _FakePersistence()
    service, _, transitions, _ = _service(storage, persistence)
    principal, lease = _principal_and_lease()
    files = (ArchiveFile("src/main.py", b"print('ready')\n"),)

    for _ in range(2):
        with pytest.raises(CheckpointStorageError, match="artifact_upload_outcome_unknown"):
            await service.commit_checkpoint(
                principal,
                lease,
                "tool:one",
                files,
                result={"tool": "finished"},
                target_run_status=RunStatus.WAITING_APPROVAL,
            )

    assert [request.method for request in storage.requests].count("POST") == 1
    assert [request.method for request in storage.requests].count("GET") == 5
    assert [call[0] for call in persistence.calls].count("mark_unknown") >= 2
    assert not any(call[0] == "release_absent" for call in persistence.calls)
    assert transitions.calls == []
    await storage.aclose()


@_async_test
async def test_unknown_upload_retry_with_changed_transition_intent_never_reposts() -> None:
    storage = _StorageFixture(post_status=500, get_responses=[(404, b"", {})])
    persistence = _FakePersistence()
    service, _, _, _ = _service(storage, persistence)
    principal, lease = _principal_and_lease()
    files = (ArchiveFile("src/main.py", b"print('ready')\n"),)

    with pytest.raises(CheckpointStorageError, match="artifact_upload_outcome_unknown"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            files,
            result={"tool": "first-result"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )
    requests_after_first_attempt = len(storage.requests)

    with pytest.raises(CheckpointStorageError, match="artifact_checkpoint_identity_conflict"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            files,
            result={"tool": "changed-result"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )

    assert len(storage.requests) == requests_after_first_attempt
    assert [request.method for request in storage.requests].count("POST") == 1
    await storage.aclose()


@_async_test
async def test_invalid_transition_plan_is_rejected_before_reservation_or_storage() -> None:
    storage = _StorageFixture()
    persistence = _FakePersistence()
    service, _, _, _ = _service(storage, persistence)
    principal, lease = _principal_and_lease()

    with pytest.raises(
        CheckpointStorageError, match="activity_result_cannot_set_terminal_run_status"
    ):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            (ArchiveFile("src/main.py", b"ready"),),
            result={"tool": "finished"},
            target_run_status=RunStatus.SUCCEEDED,
        )

    assert persistence.calls == []
    assert storage.requests == []
    await storage.aclose()


@_async_test
async def test_conflict_409_and_get_404_never_refund_reservation() -> None:
    storage = _StorageFixture(post_status=409, get_responses=[(404, b"", {})])
    persistence = _FakePersistence()
    service, _, _, _ = _service(storage, persistence)
    principal, lease = _principal_and_lease()

    with pytest.raises(CheckpointStorageError, match="artifact_upload_outcome_unknown"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            (ArchiveFile("src/main.py", b"print('ready')\n"),),
            result={"tool": "finished"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )

    assert not any(call[0] == "release_absent" for call in persistence.calls)
    assert any(call[0] == "mark_unknown" for call in persistence.calls)
    await storage.aclose()


@_async_test
@pytest.mark.parametrize("status", [408, 425, 429])
async def test_proxy_timeout_statuses_remain_unknown_and_charged(status: int) -> None:
    storage = _StorageFixture(post_status=status, get_responses=[(404, b"", {})])
    persistence = _FakePersistence()
    service, _, _, _ = _service(storage, persistence)
    principal, lease = _principal_and_lease()

    with pytest.raises(CheckpointStorageError, match="artifact_upload_outcome_unknown"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            (ArchiveFile("src/main.py", b"ready"),),
            result={"tool": "finished"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )

    assert not any(call[0] == "release_absent" for call in persistence.calls)
    assert [request.method for request in storage.requests].count("POST") == 1
    await storage.aclose()


@_async_test
async def test_definitive_client_failure_plus_404_releases_only_known_absence() -> None:
    storage = _StorageFixture(post_status=400, get_responses=[(404, b"", {})])
    persistence = _FakePersistence()
    service, _, transitions, _ = _service(storage, persistence)
    principal, lease = _principal_and_lease()

    with pytest.raises(CheckpointStorageError, match="artifact_upload_rejected"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            (ArchiveFile("src/main.py", b"print('ready')\n"),),
            result={"tool": "finished"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )

    release = next(call for call in persistence.calls if call[0] == "release_absent")
    assert release[-1] == "known_failure_and_absence"
    assert transitions.calls == []
    await storage.aclose()


@_async_test
async def test_encoded_response_is_rejected_before_storage_body_streaming() -> None:
    body = gzip.compress(b"x" * (1024 * 1024), mtime=0)
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=body,
            headers={"content-encoding": "gzip"},
            request=request,
        )

    storage = SupabaseStorage(
        _PROJECT_URL,
        _SECRET,
        _BUCKET,
        transport=httpx.MockTransport(handler),
        fixture_byte_limit=64,
    )
    try:
        with pytest.raises(StorageFailure) as raised:
            await storage.download("checkpoints/example.tar.gz")
        assert raised.value.code == "download_content_encoding_unsupported"
        assert requests[0].headers["accept-encoding"] == "identity"
    finally:
        await storage.aclose()


@_async_test
async def test_committed_lost_ack_retry_is_read_only_even_with_expired_lease() -> None:
    storage = _StorageFixture(get_responses=[])
    persistence = _FakePersistence(status="committed", is_new=False)
    transitions = _FakeTransitions(adopted=True)
    service, _, _, _ = _service(storage, persistence, transitions)
    principal, lease = _principal_and_lease()
    expired_lease = replace(lease, lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))

    receipt = await service.commit_checkpoint(
        principal,
        expired_lease,
        "tool:one",
        (ArchiveFile("src/main.py", b"print('ready')\n"),),
        result={"tool": "finished"},
        target_run_status=RunStatus.WAITING_APPROVAL,
    )

    assert storage.requests == []
    assert receipt.adopted is True
    assert receipt.uploaded is False
    assert transitions.calls[0]["artifact_id"] == persistence.reservation.upload_id
    await storage.aclose()


@_async_test
async def test_bad_readback_marks_orphan_and_never_accepts_result() -> None:
    storage = _StorageFixture(get_responses=[(200, b"not-the-archive", {})])
    persistence = _FakePersistence()
    recovery = _FakeRecovery()
    service, _, transitions, _ = _service(storage, persistence, recovery=recovery)
    principal, lease = _principal_and_lease()

    with pytest.raises(CheckpointStorageError, match="artifact_object_verification_failed"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            (ArchiveFile("src/main.py", b"print('ready')\n"),),
            result={"tool": "finished"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )

    assert any(call[0] == "mark_orphan" for call in persistence.calls)
    assert transitions.calls == []
    assert recovery.calls == [(lease, FailureClass.CORRUPT_ARTIFACT)]
    await storage.aclose()


@_async_test
async def test_recovery_write_failure_is_logged_without_replacing_corrupt_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    storage = _StorageFixture(get_responses=[(200, b"not-the-archive", {})])
    persistence = _FakePersistence()
    recovery = _FakeRecovery(error=RuntimeError("do-not-log-this-body"))
    service, _, transitions, _ = _service(
        storage,
        persistence,
        recovery=recovery,
    )
    principal, lease = _principal_and_lease()

    with pytest.raises(CheckpointStorageError, match="artifact_object_verification_failed"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            (ArchiveFile("src/main.py", b"print('ready')\n"),),
            result={"tool": "finished"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )

    assert recovery.calls == [(lease, FailureClass.CORRUPT_ARTIFACT)]
    assert "checkpoint_corrupt_recovery_failed" in caplog.text
    assert "RuntimeError" in caplog.text
    assert "do-not-log-this-body" not in caplog.text
    assert transitions.calls == []
    await storage.aclose()


@_async_test
async def test_stale_lease_after_verified_upload_marks_orphan_and_does_not_attach() -> None:
    storage = _StorageFixture()
    persistence = _FakePersistence()
    transitions = _FakeTransitions(error=LeaseLost("expired"))
    service, _, _, _ = _service(storage, persistence, transitions)
    principal, lease = _principal_and_lease()

    with pytest.raises(CheckpointStorageError, match="artifact_lease_lost"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            (ArchiveFile("src/main.py", b"print('ready')\n"),),
            result={"tool": "finished"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )

    assert any(call[0] == "mark_verified" for call in persistence.calls)
    assert any(call[0] == "mark_orphan" for call in persistence.calls)
    await storage.aclose()


@_async_test
async def test_restore_default_access_verifier_denies_before_private_object_read() -> None:
    principal, lease = _principal_and_lease()
    reference = _reference()
    storage = _StorageFixture(get_responses=[])
    persistence = _FakePersistence(reference=reference)
    service = CheckpointStorageService(
        cast(Any, persistence),
        storage.storage,
        cast(Any, _FakeTransitions()),
        cast(Any, _FakeRecovery()),
    )

    with pytest.raises(CheckpointStorageError, match="repository_authorization_pending"):
        await service.restore_archive(principal, lease, reference.artifact_id)

    assert [call[0] for call in persistence.calls] == ["get_committed"]
    assert storage.requests == []
    await storage.aclose()


@_async_test
async def test_pending_access_before_first_post_releases_never_dispatched_reservation() -> None:
    storage = _StorageFixture()
    persistence = _FakePersistence()
    service = CheckpointStorageService(
        cast(Any, persistence),
        storage.storage,
        cast(Any, _FakeTransitions()),
        cast(Any, _FakeRecovery()),
    )
    principal, lease = _principal_and_lease()

    with pytest.raises(CheckpointStorageError, match="repository_authorization_pending"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            (ArchiveFile("src/main.py", b"ready"),),
            result={"tool": "finished"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )

    assert any(call[0] == "release_absent" for call in persistence.calls)
    assert storage.requests == []
    await storage.aclose()


@_async_test
async def test_private_repository_metadata_rejects_before_first_storage_request() -> None:
    storage = _StorageFixture()
    persistence = _FakePersistence()
    github_requests: list[httpx.Request] = []
    service, _, transitions, _ = _service(
        storage,
        persistence,
        github_requests=github_requests,
        github_response={
            "id": 123456,
            "name": "continuum-fixture",
            "owner": {"login": "fixture-owner"},
            "size": 1,
            "private": True,
            "visibility": "private",
            "archived": False,
            "fork": False,
        },
    )
    principal, lease = _principal_and_lease()

    with pytest.raises(CheckpointStorageError, match="repository_not_public"):
        await service.commit_checkpoint(
            principal,
            lease,
            "tool:one",
            (ArchiveFile("src/main.py", b"ready"),),
            result={"tool": "finished"},
            target_run_status=RunStatus.WAITING_APPROVAL,
        )

    assert len(github_requests) == 1
    assert storage.requests == []
    assert any(call[0] == "release_absent" for call in persistence.calls)
    assert transitions.calls == []
    await storage.aclose()


@_async_test
async def test_restore_requires_both_actor_write_and_app_access_before_download() -> None:
    principal, lease = _principal_and_lease()
    reference = _reference()
    storage = _StorageFixture(get_responses=[])
    verifier = _AllowVerifier(app_can_access=False)
    persistence = _FakePersistence(reference=reference)
    service, _, _, _ = _service(storage, persistence, access_verifier=verifier)

    with pytest.raises(CheckpointStorageError, match="repository_access_denied"):
        await service.restore_archive(principal, lease, reference.artifact_id)

    assert verifier.calls == [(reference.repo_id, reference.github_user_id)]
    assert storage.requests == []
    await storage.aclose()


@_async_test
async def test_restore_revalidates_public_repository_and_verifies_tree_before_return() -> None:
    principal, lease = _principal_and_lease()
    reference = _reference()
    storage = _StorageFixture(get_responses=[(200, _ARCHIVE.archive, {})])
    verifier = _AllowVerifier()
    github_requests: list[httpx.Request] = []
    service, persistence, _, _ = _service(
        storage,
        access_verifier=verifier,
        github_requests=github_requests,
    )
    persistence.reference = reference

    restored = await service.restore_archive(principal, lease, reference.artifact_id)

    assert restored.manifest.tree_sha256 == reference.tree_sha256
    assert restored.archive_sha256 == reference.archive_sha256
    assert verifier.calls == [(reference.repo_id, reference.github_user_id)]
    assert [request.url.path for request in github_requests] == ["/repositories/123456"]
    assert [request.method for request in storage.requests] == ["GET", "GET"]
    await storage.aclose()


@_async_test
@pytest.mark.parametrize(
    ("status", "body", "expected_failure"),
    [
        (404, b"", True),
        (200, b"damaged", True),
        (403, b"", False),
    ],
)
async def test_restore_only_marks_missing_or_corrupt_content_as_corrupt(
    status: int,
    body: bytes,
    expected_failure: bool,
) -> None:
    principal, lease = _principal_and_lease()
    reference = _reference()
    storage = _StorageFixture(get_responses=[(status, body, {})])
    recovery = _FakeRecovery()
    github_requests: list[httpx.Request] = []
    service, persistence, _, _ = _service(
        storage,
        recovery=recovery,
        access_verifier=_AllowVerifier(),
        github_requests=github_requests,
    )
    persistence.reference = reference

    with pytest.raises(CheckpointStorageError) as raised:
        await service.restore_archive(principal, lease, reference.artifact_id)

    if expected_failure:
        assert raised.value.code == "corrupt_artifact"
        assert recovery.calls == [(lease, FailureClass.CORRUPT_ARTIFACT)]
    else:
        assert raised.value.code == "artifact_download_unavailable"
        assert recovery.calls == []
    await storage.aclose()


def _reference() -> ArtifactReference:
    return ArtifactReference(
        artifact_id=uuid4(),
        repo_id=123456,
        object_key="checkpoints/00000000-0000-0000-0000-000000000001.tar.gz",
        archive_sha256=_ARCHIVE.archive_sha256,
        tree_sha256=_ARCHIVE.manifest.tree_sha256,
        format_version=_ARCHIVE.manifest.format_version,
        compressed_bytes=len(_ARCHIVE.archive),
        source_bytes=sum(entry.size for entry in _ARCHIVE.manifest.entries),
        file_count=len(_ARCHIVE.manifest.entries),
        github_user_id=987654,
        content_expires_at=datetime.now(UTC) + timedelta(days=7),
    )
