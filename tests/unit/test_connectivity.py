"""Focused adapter and smoke-coordinator behavior for T03."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import replace
from uuid import UUID, uuid4

import httpx
import pytest

from continuum_api.connectivity.database import (
    ClaimResult,
    DatabaseFailure,
    EffectState,
    SmokeClaim,
    sqlalchemy_url,
)
from continuum_api.connectivity.smoke import (
    ConnectivitySmoke,
    HostedSmokeConfig,
    SmokeConfigurationError,
    maybe_run_startup_smoke,
    report_json,
)
from continuum_api.connectivity.storage import StorageFailure, SupabaseStorage

_SECRET = "sb_secret_T03_test_secret_value"
_FIXTURE = b"Continumm T03 private storage connectivity fixture v1\n"
_PROJECT_URL = "https://project-ref.supabase.co"
_BUCKET = "continuum-t03-private"


def _storage(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    byte_limit: int = 1024,
    deadline: float = 1.0,
) -> tuple[SupabaseStorage, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return handler(request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(record))
    return (
        SupabaseStorage(
            _PROJECT_URL,
            _SECRET,
            _BUCKET,
            client=client,
            fixture_byte_limit=byte_limit,
            request_deadline_seconds=deadline,
        ),
        requests,
    )


def test_sqlalchemy_url_selects_psycopg_and_hides_password() -> None:
    url = sqlalchemy_url("postgresql://probe:local-password@localhost:5432/continuum_t03_test")

    assert url.drivername == "postgresql+psycopg"
    assert "local-password" not in str(url)
    assert url.password == "local-password"


def test_storage_round_trip_uses_private_authenticated_rest_and_no_bearer_key() -> None:
    authenticated_gets = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal authenticated_gets
        assert request.headers["apikey"] == _SECRET
        assert "authorization" not in request.headers
        if request.method == "GET" and request.url.path.endswith(f"/bucket/{_BUCKET}"):
            return httpx.Response(200, json={"id": _BUCKET, "public": False})
        if request.method == "POST":
            assert request.url.path.endswith(f"/object/{_BUCKET}/t03-probes/probe.bin")
            assert request.headers["x-upsert"] == "false"
            assert request.read() == _FIXTURE
            return httpx.Response(200, json={"Key": "probe.bin"})
        if request.method == "GET" and "/object/authenticated/" in request.url.path:
            authenticated_gets += 1
            if authenticated_gets == 1:
                return httpx.Response(200, content=_FIXTURE)
            return httpx.Response(404)
        if request.method == "DELETE":
            assert json.loads(request.read()) == {"prefixes": ["t03-probes/probe.bin"]}
            return httpx.Response(200, json={"message": "Successfully deleted"})
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    storage, requests = _storage(handler)
    try:

        async def exercise() -> None:
            await storage.check_private_bucket()
            await storage.upload("t03-probes/probe.bin", _FIXTURE)
            assert await storage.download("t03-probes/probe.bin") == _FIXTURE
            await storage.delete("t03-probes/probe.bin")
            await storage.verify_absent("t03-probes/probe.bin")

        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())

    assert [request.method for request in requests] == ["GET", "POST", "GET", "DELETE", "GET"]


def test_public_bucket_stops_before_object_mutation() -> None:
    storage, requests = _storage(lambda request: httpx.Response(200, json={"public": True}))
    try:

        async def exercise() -> None:
            with pytest.raises(StorageFailure, match="bucket_not_private"):
                await storage.check_private_bucket()

        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())

    assert [request.method for request in requests] == ["GET"]


def test_fixture_limit_rejects_before_upload() -> None:
    storage, requests = _storage(lambda request: httpx.Response(200), byte_limit=8)
    try:

        async def exercise() -> None:
            with pytest.raises(StorageFailure, match="fixture_too_large"):
                await storage.upload("t03-probes/probe.bin", _FIXTURE)

        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())

    assert requests == []


def test_upload_timeout_is_unknown_and_not_retried() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            raise httpx.ReadTimeout("secret must not escape")
        return httpx.Response(200, json={"public": False})

    storage, requests = _storage(handler)
    try:

        async def exercise() -> None:
            with pytest.raises(StorageFailure) as raised:
                await storage.upload("t03-probes/probe.bin", _FIXTURE)
            assert raised.value.code == "upload_outcome_unknown"
            assert raised.value.effect is EffectState.UNKNOWN
            assert "secret must not escape" not in str(raised.value)

        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())
    assert len(requests) == 1


def test_oversized_upload_response_is_unknown_and_not_retried() -> None:
    storage, requests = _storage(
        lambda _request: httpx.Response(200, headers={"content-length": "9000"})
    )
    try:

        async def exercise() -> None:
            with pytest.raises(StorageFailure) as raised:
                await storage.upload("t03-probes/probe.bin", _FIXTURE)
            assert raised.value.code == "upload_response_too_large"
            assert raised.value.effect is EffectState.UNKNOWN

        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())
    assert len(requests) == 1


class _SlowStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b'{"public":'
        await asyncio.sleep(0.03)
        yield b"false}"

    async def aclose(self) -> None:
        pass


def test_stream_total_deadline_stops_a_drip_response() -> None:
    storage, requests = _storage(
        lambda _request: httpx.Response(200, stream=_SlowStream()),
        deadline=0.01,
    )
    try:

        async def exercise() -> None:
            with pytest.raises(StorageFailure, match="bucket_check_timeout"):
                await storage.check_private_bucket()

        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())

    assert len(requests) == 1


@pytest.mark.parametrize(
    "project_url",
    [
        "http://project-ref.supabase.co",
        "https://user:password@project-ref.supabase.co",
        "https://project-ref.supabase.co/path",
        "https://project-ref.supabase.co?token=secret",
        "https://attacker.example",
    ],
)
def test_storage_rejects_noncanonical_or_untrusted_project_urls(project_url: str) -> None:
    with pytest.raises(StorageFailure) as raised:
        SupabaseStorage(project_url, _SECRET, _BUCKET)

    assert raised.value.code == "storage_url_invalid"
    assert "password" not in str(raised.value)
    assert _SECRET not in repr(raised.value)


@pytest.mark.parametrize("method", ["download", "delete"])
def test_invalid_object_keys_are_rejected_before_request(method: str) -> None:
    storage, requests = _storage(lambda _request: httpx.Response(200))
    try:

        async def exercise() -> None:
            with pytest.raises(StorageFailure, match="object_key_invalid"):
                await getattr(storage, method)("t03-probes/../outside.bin")

        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())

    assert requests == []


def test_hosted_config_requires_explicit_render_and_run_id_without_repr_secrets() -> None:
    env = {
        "CONTINUUM_ENABLE_HOSTED_SMOKE": "1",
        "CONTINUUM_ENVIRONMENT": "production",
        "RENDER": "true",
        "RENDER_SERVICE_ID": "srv-private-id",
        "RENDER_EXTERNAL_URL": "https://service.onrender.com",
        "RENDER_SERVICE_TYPE": "web",
        "CONTINUUM_T03_HOSTED_SMOKE_ID": str(uuid4()),
        "CONTINUUM_DATABASE_URL": "postgresql://postgres:db-secret@pooler.example:5432/postgres",
        "CONTINUUM_SUPABASE_URL": _PROJECT_URL,
        "CONTINUUM_SUPABASE_SECRET_KEY": _SECRET,
        "CONTINUUM_SUPABASE_BUCKET": _BUCKET,
    }
    config = HostedSmokeConfig.from_environment(env)
    assert config is not None
    assert _SECRET not in repr(config)
    assert "db-secret" not in repr(config)

    invalid = {**env, "RENDER": "false"}
    with pytest.raises(SmokeConfigurationError, match="smoke_requires_render"):
        HostedSmokeConfig.from_environment(invalid)


def test_startup_refusal_and_report_never_include_configured_secrets() -> None:
    env = {
        "CONTINUUM_ENABLE_HOSTED_SMOKE": "1",
        "CONTINUUM_ENVIRONMENT": "production",
        "RENDER": "false",
        "CONTINUUM_T03_HOSTED_SMOKE_ID": str(uuid4()),
        "CONTINUUM_SUPABASE_SECRET_KEY": _SECRET,
        "CONTINUUM_DATABASE_URL": "postgresql://postgres:db-secret@pooler.example:5432/postgres",
    }

    report = maybe_run_startup_smoke(env)

    assert report is not None
    rendered = report_json(report)
    assert report.result_code == "smoke_requires_render"
    assert _SECRET not in rendered
    assert "db-secret" not in rendered


class _FakeDatabase:
    def __init__(self, *, existing_status: str | None = None) -> None:
        self.existing_status = existing_status
        self.claim: SmokeClaim | None = None
        self.finished: tuple[str, str] | None = None
        self.calls: list[str] = []
        self.probe_failure: DatabaseFailure | None = None

    def claim_run(self, run_id: UUID, probe_id: UUID, object_key: str) -> ClaimResult:
        self.calls.append("claim")
        if self.existing_status is not None:
            self.claim = SmokeClaim(
                run_id=run_id,
                probe_id=probe_id,
                object_key=object_key,
                status=self.existing_status,
                result_code="ok" if self.existing_status == "completed" else None,
            )
            return ClaimResult(acquired=False, claim=self.claim)
        if self.claim is not None:
            return ClaimResult(acquired=False, claim=self.claim)
        self.claim = SmokeClaim(
            run_id=run_id,
            probe_id=probe_id,
            object_key=object_key,
            status="claimed",
            result_code=None,
        )
        return ClaimResult(acquired=True, claim=self.claim)

    def run_probe_row(self, probe_id: UUID) -> None:
        self.calls.append("probe")
        if self.probe_failure is not None:
            raise self.probe_failure

    def finish_claim(self, run_id: UUID, *, status: str, result_code: str) -> None:
        self.calls.append("finish")
        self.finished = (status, result_code)
        if self.claim is not None:
            self.claim = replace(self.claim, status=status, result_code=result_code)


class _FakeStorage:
    def __init__(
        self,
        *,
        failure_at: str | None = None,
        failure: StorageFailure | None = None,
        download_content: bytes = _FIXTURE,
    ) -> None:
        self.failure_at = failure_at
        self.failure = failure
        self.download_content = download_content
        self.calls: list[str] = []

    def _call(self, name: str) -> None:
        self.calls.append(name)
        if self.failure_at == name and self.failure is not None:
            raise self.failure

    async def check_private_bucket(self) -> None:
        self._call("bucket")

    async def upload(self, object_key: str, content: bytes) -> None:
        assert object_key.startswith("t03-probes/")
        assert content == _FIXTURE
        self._call("upload")

    async def download(self, object_key: str) -> bytes:
        assert object_key.startswith("t03-probes/")
        self._call("download")
        return self.download_content

    async def delete(self, object_key: str) -> None:
        assert object_key.startswith("t03-probes/")
        self._call("delete")

    async def verify_absent(self, object_key: str) -> None:
        assert object_key.startswith("t03-probes/")
        self._call("verify_absent")


def test_coordinator_completes_and_cleans_both_probes() -> None:
    database = _FakeDatabase()
    storage = _FakeStorage()
    report = asyncio.run(ConnectivitySmoke(database, storage).run(uuid4()))

    assert report.status == "completed"
    assert report.result_code == "ok"
    assert report.database_result == "verified_and_cleaned"
    assert report.storage_result == "verified_and_cleaned"
    assert report.sha256_match is True
    assert storage.calls == ["bucket", "upload", "download", "delete", "verify_absent"]
    assert database.finished == ("completed", "ok")


def test_coordinator_preserves_unknown_upload_without_delete_or_retry() -> None:
    database = _FakeDatabase()
    storage = _FakeStorage(
        failure_at="upload",
        failure=StorageFailure("upload_outcome_unknown", effect=EffectState.UNKNOWN),
    )
    report = asyncio.run(ConnectivitySmoke(database, storage).run(uuid4()))

    assert report.status == "unknown"
    assert report.result_code == "upload_outcome_unknown"
    assert report.storage_result == "unknown_upload"
    assert storage.calls == ["bucket", "upload"]
    assert database.finished == ("unknown", "upload_outcome_unknown")


def test_coordinator_cleans_uploaded_object_after_download_failure() -> None:
    database = _FakeDatabase()
    storage = _FakeStorage(
        failure_at="download",
        failure=StorageFailure("download_transport"),
    )
    report = asyncio.run(ConnectivitySmoke(database, storage).run(uuid4()))

    assert report.status == "failed"
    assert report.result_code == "download_transport"
    assert report.storage_result == "failed_and_cleaned"
    assert storage.calls == ["bucket", "upload", "download", "delete", "verify_absent"]


def test_claimed_run_id_is_never_replayed() -> None:
    run_id = uuid4()
    database = _FakeDatabase()
    first = asyncio.run(ConnectivitySmoke(database, _FakeStorage()).run(run_id))
    assert first.status == "completed"

    storage = _FakeStorage()
    second = asyncio.run(ConnectivitySmoke(database, storage).run(run_id))

    assert second.status == "completed"
    assert second.result_code == "ok"
    assert storage.calls == []


def test_existing_unresolved_claim_is_never_replayed() -> None:
    storage = _FakeStorage()
    report = asyncio.run(
        ConnectivitySmoke(_FakeDatabase(existing_status="claimed"), storage).run(uuid4())
    )

    assert report.status == "claimed"
    assert report.result_code == "prior_run_unresolved"
    assert storage.calls == []


def test_lost_claim_commit_stops_before_any_probe_mutation() -> None:
    class LostClaimDatabase(_FakeDatabase):
        def claim_run(self, run_id: UUID, probe_id: UUID, object_key: str) -> ClaimResult:
            raise DatabaseFailure("claim_commit_unknown", effect=EffectState.UNKNOWN)

    storage = _FakeStorage()
    report = asyncio.run(ConnectivitySmoke(LostClaimDatabase(), storage).run(uuid4()))

    assert report.status == "unknown"
    assert report.database_result == "claim_unknown"
    assert report.probe_id is not None
    assert report.object_key is not None
    assert storage.calls == []


def test_hash_mismatch_is_reported_and_object_cleanup_is_verified() -> None:
    storage = _FakeStorage(download_content=b"different fixture")
    report = asyncio.run(ConnectivitySmoke(_FakeDatabase(), storage).run(uuid4()))

    assert report.status == "failed"
    assert report.result_code == "download_hash_mismatch"
    assert report.sha256_match is False
    assert report.storage_result == "failed_and_cleaned"
    assert storage.calls == ["bucket", "upload", "download", "delete", "verify_absent"]
