"""One-shot T03 connectivity smoke, intended for a Render startup opt-in."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Protocol
from uuid import UUID, uuid4

from continuum_api.connectivity.database import (
    DatabaseFailure,
    EffectState,
    SmokeClaim,
    T03Database,
    create_database_engine,
)
from continuum_api.connectivity.storage import StorageFailure, SupabaseStorage

_FIXTURE = b"Continumm T03 private storage connectivity fixture v1\n"


class SmokeConfigurationError(RuntimeError):
    """A safe configuration error that never retains input values."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class StoragePort(Protocol):
    """The small surface consumed by the smoke coordinator."""

    async def check_private_bucket(self) -> None: ...

    async def upload(self, object_key: str, content: bytes) -> None: ...

    async def download(self, object_key: str) -> bytes: ...

    async def delete(self, object_key: str) -> None: ...

    async def verify_absent(self, object_key: str) -> None: ...


@dataclass(frozen=True, slots=True)
class HostedSmokeConfig:
    """Validated settings for one Render-hosted attempt."""

    run_id: UUID
    database_url: str = field(repr=False)
    supabase_url: str = field(repr=False)
    supabase_secret_key: str = field(repr=False)
    bucket: str

    @classmethod
    def from_environment(cls, environ: Mapping[str, str]) -> HostedSmokeConfig | None:
        enabled = environ.get("CONTINUUM_ENABLE_HOSTED_SMOKE", "0")
        if enabled in {"0", "false", "False", ""}:
            return None
        if enabled != "1":
            raise SmokeConfigurationError("smoke_flag_invalid")

        if environ.get("CONTINUUM_ENVIRONMENT") != "production":
            raise SmokeConfigurationError("smoke_requires_production")
        if environ.get("RENDER") != "true":
            raise SmokeConfigurationError("smoke_requires_render")
        if not environ.get("RENDER_SERVICE_ID") or not environ.get("RENDER_EXTERNAL_URL"):
            raise SmokeConfigurationError("smoke_render_metadata_missing")
        if environ.get("RENDER_SERVICE_TYPE") != "web":
            raise SmokeConfigurationError("smoke_requires_render_web_service")
        if environ.get("IS_PULL_REQUEST") == "true":
            raise SmokeConfigurationError("smoke_pull_request_service_forbidden")

        raw_run_id = environ.get("CONTINUUM_T03_HOSTED_SMOKE_ID", "")
        try:
            run_id = UUID(raw_run_id)
        except (ValueError, AttributeError):
            raise SmokeConfigurationError("smoke_run_id_invalid") from None

        database_url = environ.get("CONTINUUM_DATABASE_URL", "")
        supabase_url = environ.get("CONTINUUM_SUPABASE_URL", "")
        secret_key = environ.get("CONTINUUM_SUPABASE_SECRET_KEY", "")
        bucket = environ.get("CONTINUUM_SUPABASE_BUCKET", "")
        if not database_url:
            raise SmokeConfigurationError("database_url_missing")
        if not supabase_url:
            raise SmokeConfigurationError("storage_url_missing")
        if not secret_key:
            raise SmokeConfigurationError("storage_secret_missing")
        if not bucket:
            raise SmokeConfigurationError("storage_bucket_missing")

        return cls(
            run_id=run_id,
            database_url=database_url,
            supabase_url=supabase_url,
            supabase_secret_key=secret_key,
            bucket=bucket,
        )


@dataclass(frozen=True, slots=True)
class SmokeReport:
    """Sanitized evidence fields; never includes configuration or raw exceptions."""

    run_id: UUID
    probe_id: UUID | None
    object_key: str | None
    status: str
    result_code: str
    database_result: str
    storage_result: str
    sha256_match: bool | None
    render_runtime_confirmed: bool
    teardown_result: str = "not_applicable"

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": str(self.run_id),
            "probe_id": str(self.probe_id) if self.probe_id else None,
            "object_key": self.object_key,
            "status": self.status,
            "result_code": _safe_report_code(self.result_code),
            "database_result": self.database_result,
            "storage_result": self.storage_result,
            "sha256_match": self.sha256_match,
            "render_runtime_confirmed": self.render_runtime_confirmed,
            "teardown_result": self.teardown_result,
        }


class ConnectivitySmoke:
    """Coordinate the durable claim, PostgreSQL probe, and private object proof."""

    def __init__(self, database: T03Database, storage: StoragePort) -> None:
        self._database = database
        self._storage = storage

    async def run(
        self,
        run_id: UUID,
        *,
        render_runtime_confirmed: bool = False,
        probe_id: UUID | None = None,
        object_key: str | None = None,
    ) -> SmokeReport:
        if (probe_id is None) != (object_key is None):
            raise ValueError("probe ID and object key must be supplied together")
        proposed_probe_id = probe_id or uuid4()
        proposed_object_key = object_key or f"t03-probes/{uuid4()}.bin"

        try:
            claim_result = self._database.claim_run(
                run_id,
                proposed_probe_id,
                proposed_object_key,
            )
        except DatabaseFailure as error:
            return SmokeReport(
                run_id=run_id,
                probe_id=proposed_probe_id,
                object_key=proposed_object_key,
                status="unknown" if error.effect is EffectState.UNKNOWN else "failed",
                result_code=error.code,
                database_result="claim_unknown"
                if error.effect is EffectState.UNKNOWN
                else "claim_failed",
                storage_result="not_run",
                sha256_match=None,
                render_runtime_confirmed=render_runtime_confirmed,
            )
        except Exception:
            return SmokeReport(
                run_id=run_id,
                probe_id=proposed_probe_id,
                object_key=proposed_object_key,
                status="unknown",
                result_code="claim_commit_unknown",
                database_result="claim_unknown",
                storage_result="not_run",
                sha256_match=None,
                render_runtime_confirmed=render_runtime_confirmed,
            )

        claim = claim_result.claim
        if not claim_result.acquired:
            return SmokeReport(
                run_id=claim.run_id,
                probe_id=claim.probe_id,
                object_key=claim.object_key,
                status=claim.status,
                result_code=claim.result_code or _prior_claim_code(claim),
                database_result="prior_claim_not_rerun",
                storage_result="prior_claim_not_rerun",
                sha256_match=None,
                render_runtime_confirmed=render_runtime_confirmed,
            )

        database_result = "not_run"
        storage_result = "not_run"
        sha256_match: bool | None = None
        status = "failed"
        result_code = "smoke_internal_error"

        try:
            self._database.run_probe_row(claim.probe_id)
            database_result = "verified_and_cleaned"
        except DatabaseFailure as error:
            database_result = _database_phase_result(error)
            status = "unknown" if error.effect is EffectState.UNKNOWN else "failed"
            result_code = error.code
            return self._finalize(
                claim,
                status=status,
                result_code=result_code,
                database_result=database_result,
                storage_result=storage_result,
                sha256_match=sha256_match,
                render_runtime_confirmed=render_runtime_confirmed,
            )
        except Exception:
            return self._finalize(
                claim,
                status="unknown",
                result_code="smoke_internal_error",
                database_result="unknown_probe_effect",
                storage_result=storage_result,
                sha256_match=sha256_match,
                render_runtime_confirmed=render_runtime_confirmed,
            )

        uploaded = False
        upload_attempted = False
        operation = "bucket"
        failure: StorageFailure | None = None
        try:
            await self._storage.check_private_bucket()
            operation = "upload"
            upload_attempted = True
            await self._storage.upload(claim.object_key, _FIXTURE)
            uploaded = True
            operation = "download"
            downloaded = await self._storage.download(claim.object_key)
            expected_digest = hashlib.sha256(_FIXTURE).digest()
            actual_digest = hashlib.sha256(downloaded).digest()
            sha256_match = actual_digest == expected_digest and downloaded == _FIXTURE
            if not sha256_match:
                raise StorageFailure("download_hash_mismatch")
        except StorageFailure as error:
            failure = error
        except Exception:
            failure = StorageFailure(
                "storage_internal_error",
                effect=EffectState.UNKNOWN
                if operation == "upload" and upload_attempted
                else EffectState.KNOWN,
            )

        cleanup_attempted = False
        cleanup_verified = False
        if uploaded:
            cleanup_attempted = True
            try:
                await self._storage.delete(claim.object_key)
                await self._storage.verify_absent(claim.object_key)
                cleanup_verified = True
            except StorageFailure as error:
                failure = error
            except Exception:
                failure = StorageFailure(
                    "delete_verify_outcome_unknown",
                    effect=EffectState.UNKNOWN,
                )
            else:
                storage_result = "verified_and_cleaned" if failure is None else "failed_and_cleaned"

        if failure is None:
            status = "completed"
            result_code = "ok"
            storage_result = "verified_and_cleaned"
        else:
            status = "unknown" if failure.effect is EffectState.UNKNOWN else "failed"
            result_code = failure.code
            if not uploaded:
                storage_result = (
                    "unknown_upload"
                    if failure.effect is EffectState.UNKNOWN
                    else "failed_before_upload"
                )
            elif cleanup_verified:
                storage_result = "failed_and_cleaned"
            elif cleanup_attempted:
                storage_result = (
                    "cleanup_unknown" if failure.effect is EffectState.UNKNOWN else "cleanup_failed"
                )
            elif storage_result == "not_run":
                storage_result = "uploaded_cleanup_unconfirmed"

        return self._finalize(
            claim,
            status=status,
            result_code=result_code,
            database_result=database_result,
            storage_result=storage_result,
            sha256_match=sha256_match,
            render_runtime_confirmed=render_runtime_confirmed,
        )

    def _finalize(
        self,
        claim: SmokeClaim,
        *,
        status: str,
        result_code: str,
        database_result: str,
        storage_result: str,
        sha256_match: bool | None,
        render_runtime_confirmed: bool,
    ) -> SmokeReport:
        try:
            self._database.finish_claim(
                claim.run_id,
                status=status,
                result_code=_safe_report_code(result_code),
            )
        except Exception:
            status = "unknown"
            result_code = "claim_finalize_unknown"

        return SmokeReport(
            run_id=claim.run_id,
            probe_id=claim.probe_id,
            object_key=claim.object_key,
            status=status,
            result_code=_safe_report_code(result_code),
            database_result=database_result,
            storage_result=storage_result,
            sha256_match=sha256_match,
            render_runtime_confirmed=render_runtime_confirmed,
        )


async def _run_configured_smoke_async(environ: Mapping[str, str]) -> SmokeReport | None:
    """Run the explicitly armed smoke without logging credentials or exceptions."""
    config = HostedSmokeConfig.from_environment(environ)
    if config is None:
        return None

    probe_id = uuid4()
    object_key = f"t03-probes/{uuid4()}.bin"
    engine = None
    storage: SupabaseStorage | None = None
    report: SmokeReport | None = None
    teardown_failed = False
    try:
        try:
            engine = create_database_engine(config.database_url, require_ssl=True)
        except DatabaseFailure as error:
            report = SmokeReport(
                run_id=config.run_id,
                probe_id=probe_id,
                object_key=object_key,
                status="failed",
                result_code=error.code,
                database_result="not_run",
                storage_result="not_run",
                sha256_match=None,
                render_runtime_confirmed=True,
            )
        except Exception:
            report = SmokeReport(
                run_id=config.run_id,
                probe_id=probe_id,
                object_key=object_key,
                status="failed",
                result_code="database_engine_unavailable",
                database_result="not_run",
                storage_result="not_run",
                sha256_match=None,
                render_runtime_confirmed=True,
            )

        if engine is not None:
            database = T03Database(engine)
            try:
                storage = SupabaseStorage(
                    config.supabase_url,
                    config.supabase_secret_key,
                    config.bucket,
                )
            except StorageFailure as error:
                report = SmokeReport(
                    run_id=config.run_id,
                    probe_id=probe_id,
                    object_key=object_key,
                    status="failed",
                    result_code=error.code,
                    database_result="not_run",
                    storage_result="not_run",
                    sha256_match=None,
                    render_runtime_confirmed=True,
                )
            except Exception:
                report = SmokeReport(
                    run_id=config.run_id,
                    probe_id=probe_id,
                    object_key=object_key,
                    status="failed",
                    result_code="storage_internal_error",
                    database_result="not_run",
                    storage_result="not_run",
                    sha256_match=None,
                    render_runtime_confirmed=True,
                )

            if storage is not None:
                try:
                    report = await ConnectivitySmoke(database, storage).run(
                        config.run_id,
                        render_runtime_confirmed=True,
                        probe_id=probe_id,
                        object_key=object_key,
                    )
                except Exception:
                    # The IDs were included in the durable claim before probe mutations began.
                    report = SmokeReport(
                        run_id=config.run_id,
                        probe_id=probe_id,
                        object_key=object_key,
                        status="unknown",
                        result_code="smoke_internal_error",
                        database_result="outcome_unknown_after_claim",
                        storage_result="outcome_unknown_after_claim",
                        sha256_match=None,
                        render_runtime_confirmed=True,
                    )
    finally:
        if storage is not None:
            try:
                await storage.aclose()
            except Exception:
                teardown_failed = True
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                teardown_failed = True

    if report is None:
        return None
    teardown_started = storage is not None or engine is not None
    teardown_result = "failed" if teardown_failed else "ok" if teardown_started else "not_needed"
    return replace(report, teardown_result=teardown_result)


def run_configured_smoke(environ: Mapping[str, str]) -> SmokeReport | None:
    """Synchronous startup wrapper; invoked before the API event loop begins."""
    return asyncio.run(_run_configured_smoke_async(environ))


def maybe_run_startup_smoke(environ: Mapping[str, str] | None = None) -> SmokeReport | None:
    """Return one safe report or None when T03 has not been explicitly armed."""
    source = os.environ if environ is None else environ
    try:
        return run_configured_smoke(source)
    except SmokeConfigurationError as error:
        run_id = _safe_uuid(source.get("CONTINUUM_T03_HOSTED_SMOKE_ID"))
        return SmokeReport(
            run_id=run_id or UUID(int=0),
            probe_id=None,
            object_key=None,
            status="failed",
            result_code=error.code,
            database_result="not_run",
            storage_result="not_run",
            sha256_match=None,
            render_runtime_confirmed=source.get("RENDER") == "true",
        )
    except DatabaseFailure as error:
        return SmokeReport(
            run_id=_safe_uuid(source.get("CONTINUUM_T03_HOSTED_SMOKE_ID")) or UUID(int=0),
            probe_id=None,
            object_key=None,
            status="unknown" if error.effect is EffectState.UNKNOWN else "failed",
            result_code=error.code,
            database_result="claim_unknown"
            if error.effect is EffectState.UNKNOWN
            else "claim_failed",
            storage_result="not_run",
            sha256_match=None,
            render_runtime_confirmed=source.get("RENDER") == "true",
        )
    except StorageFailure as error:
        return SmokeReport(
            run_id=_safe_uuid(source.get("CONTINUUM_T03_HOSTED_SMOKE_ID")) or UUID(int=0),
            probe_id=None,
            object_key=None,
            status="unknown" if error.effect is EffectState.UNKNOWN else "failed",
            result_code=error.code,
            database_result="not_run",
            storage_result="not_run",
            sha256_match=None,
            render_runtime_confirmed=source.get("RENDER") == "true",
        )
    except Exception:
        # Startup must never print a driver/client traceback that could include configuration.
        return SmokeReport(
            run_id=_safe_uuid(source.get("CONTINUUM_T03_HOSTED_SMOKE_ID")) or UUID(int=0),
            probe_id=None,
            object_key=None,
            status="failed",
            result_code="smoke_internal_error",
            database_result="not_run",
            storage_result="not_run",
            sha256_match=None,
            render_runtime_confirmed=source.get("RENDER") == "true",
        )


def _database_phase_result(error: DatabaseFailure) -> str:
    if error.effect is EffectState.UNKNOWN:
        return "unknown_probe_effect"
    if error.code.startswith("probe_cleanup"):
        return "probe_cleanup_failed"
    return "probe_failed"


def _prior_claim_code(claim: SmokeClaim) -> str:
    if claim.status == "completed":
        return "prior_run_completed"
    if claim.status == "failed":
        return "prior_run_failed"
    if claim.status == "unknown":
        return "prior_run_unknown"
    return "prior_run_unresolved"


def _safe_report_code(value: str) -> str:
    if value.isascii() and value.replace("_", "").isalnum() and len(value) <= 64:
        return value
    return "smoke_internal_error"


def _safe_uuid(value: str | None) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(value)
    except (ValueError, AttributeError):
        return None


def report_json(report: SmokeReport) -> str:
    """Render the safe report as one compact JSON log message."""
    return json.dumps(report.as_dict(), separators=(",", ":"), sort_keys=True)


def cli_main() -> int:
    """CLI entrypoint used by `make smoke-hosted` on a Render process only."""
    environ = os.environ
    if environ.get("RENDER") != "true" or not environ.get("RENDER_SERVICE_ID"):
        print('{"status":"refused","result_code":"render_runtime_required"}')
        return 2
    if environ.get("CONTINUUM_ENABLE_HOSTED_SMOKE") != "1":
        print('{"status":"refused","result_code":"smoke_not_enabled"}')
        return 2
    report = maybe_run_startup_smoke(environ)
    if report is None:
        print('{"status":"refused","result_code":"smoke_not_enabled"}')
        return 2
    print(report_json(report))
    return 0 if report.status == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(cli_main())
