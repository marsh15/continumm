"""Explicit opt-in worker for existing approved GitHub publication jobs."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TextIO, cast
from uuid import UUID

_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_STOP_REASONS = frozenset({"idle", "waiting", "job_limit", "time_limit"})


class WorkerConfigurationError(RuntimeError):
    """A sanitized worker configuration failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(slots=True)
class PublicationWorkerResources:
    runner: Any
    storage: Any
    engine: Any

    async def aclose(self) -> None:
        try:
            await self.storage.aclose()
        finally:
            self.engine.dispose()


def main(
    argv: list[str] | None = None,
    *,
    settings_loader: Callable[[], Any] | None = None,
    resources_factory: Callable[[Any, UUID], Any] | None = None,
    stdout: TextIO | None = None,
) -> int:
    """Run one bounded publication or retention drain after explicit selection."""
    args = _parser().parse_args(argv)
    output = stdout or sys.stdout
    if args.retention:
        return _run_retention_drain(args, settings_loader=settings_loader, stdout=output)
    if args.account_id is None or args.run_id is None:
        _parser().error("--account-id and --run-id are required for publication work")
    if not args.execute_publication:
        _write_json(
            output,
            {
                "run_id": str(args.run_id),
                "jobs_started": 0,
                "stop_reason": "execution_required",
            },
        )
        return 2

    try:
        receipt = asyncio.run(
            _execute(
                args,
                settings_loader=settings_loader,
                resources_factory=resources_factory,
            )
        )
        record = _receipt_record(args.run_id, receipt, args.max_jobs)
    except Exception as error:
        _write_json(
            output,
            {
                "run_id": str(args.run_id),
                "jobs_started": None,
                "stop_reason": "error",
                "error_code": _error_code(error),
            },
        )
        return 1
    _write_json(output, record)
    return 0


def _run_retention_drain(
    args: argparse.Namespace,
    *,
    settings_loader: Callable[[], Any] | None,
    stdout: TextIO,
) -> int:
    try:
        if settings_loader is None:
            from continuum_api.settings import get_settings

            settings_loader = get_settings
        settings = settings_loader()
        database_url = getattr(settings, "runtime_database_url", None)
        if not _has_secret(database_url):
            raise WorkerConfigurationError("runtime_database_unavailable")
        from continuum_api.runtime.database import create_runtime_engine
        from continuum_api.runtime.retention import RetentionService

        engine = create_runtime_engine(cast(Any, database_url).get_secret_value())
        try:
            adapter = None
            if args.delete_objects:
                from continuum_api.connectivity.storage import SupabaseStorage
                from continuum_api.runtime.retention_storage import StorageDeletionAdapter

                if not settings.supabase_url or not _has_secret(settings.supabase_secret_key):
                    raise WorkerConfigurationError("retention_storage_unavailable")
                storage_url = cast(str, settings.supabase_url)
                storage_secret = cast(Any, settings.supabase_secret_key).get_secret_value()
                storage_bucket = settings.supabase_bucket
                adapter = StorageDeletionAdapter(
                    lambda: SupabaseStorage(storage_url, storage_secret, storage_bucket)
                )
            receipt = RetentionService.drain_due(
                engine, limit=args.max_jobs, deletion_adapter=adapter
            )
        finally:
            engine.dispose()
    except Exception as error:
        _write_json(
            stdout,
            {
                "owners_processed": None,
                "stop_reason": "error",
                "error_code": _error_code(error),
            },
        )
        return 1
    _write_json(
        stdout,
        {
            "owners_processed": receipt.owners,
            "expired_runs": receipt.expired_runs,
            "scrubbed_runs": receipt.scrubbed_runs,
            "webhook_bodies_scrubbed": receipt.webhook_bodies_scrubbed,
            "external_cleanup_attempted": receipt.cleanup.attempted,
            "external_cleanup_pending": receipt.cleanup.pending,
            "external_cleanup_unsupported": receipt.cleanup.unsupported,
            "stop_reason": "drained",
        },
    )
    return 0


async def _execute(
    args: argparse.Namespace,
    *,
    settings_loader: Callable[[], Any] | None,
    resources_factory: Callable[[Any, UUID], Any] | None,
) -> Any:
    if settings_loader is None:
        from continuum_api.settings import get_settings

        settings_loader = get_settings
    if resources_factory is None:
        resources_factory = _build_publication_resources

    settings = settings_loader()
    resources = resources_factory(settings, args.account_id)
    if inspect.isawaitable(resources):
        resources = await resources
    try:
        return await resources.runner.run_admitted(
            args.run_id,
            max_jobs=args.max_jobs,
            max_seconds=args.max_seconds,
        )
    finally:
        try:
            await resources.aclose()
        except Exception:
            # Cleanup failures must not replace the durable runner receipt.
            pass


async def _build_publication_resources(
    settings: Any, account_id: UUID
) -> PublicationWorkerResources:
    from continuum_api.artifacts.archives import MAX_COMPRESSED_BYTES
    from continuum_api.auth.github import (
        GitHubAuthorizationClient,
        GitHubPublicationFactsProvider,
    )
    from continuum_api.auth.sessions import GitHubUserTokenStore
    from continuum_api.connectivity.storage import SupabaseStorage
    from continuum_api.credentials import CredentialCipher
    from continuum_api.publication.github import GitHubPublicationTransport
    from continuum_api.publication.github_pr import GitHubPRTransport
    from continuum_api.runtime.activity_runner import ActivityRunner
    from continuum_api.runtime.approvals import ApprovalService
    from continuum_api.runtime.database import create_runtime_engine
    from continuum_api.runtime.external_ledger import ExternalOperationService
    from continuum_api.runtime.github_pr_publication import GitHubPullRequestService
    from continuum_api.runtime.github_publication import GitHubBranchPublicationService
    from continuum_api.runtime.persistence import Principal
    from continuum_api.runtime.publication_activity import ApprovedPublicationActivityHandler
    from continuum_api.runtime.transitions import TransitionService

    _require_worker_settings(settings)
    engine = create_runtime_engine(settings.runtime_database_url.get_secret_value())
    storage = None
    try:
        storage = SupabaseStorage(
            settings.supabase_url,
            settings.supabase_secret_key.get_secret_value(),
            settings.supabase_bucket,
            fixture_byte_limit=MAX_COMPRESSED_BYTES,
        )
        principal = Principal(account_id)
        cipher = CredentialCipher.from_settings(settings)
        tokens = GitHubUserTokenStore(engine, cipher, account_id)
        authorization = GitHubAuthorizationClient(
            app_id=settings.github_app_id,
            app_private_key=settings.github_app_private_key,
            user_tokens=tokens,
        )
        transitions = TransitionService(engine, principal)
        approvals = ApprovalService(
            engine,
            principal,
            transitions,
            storage,
            publication_facts=GitHubPublicationFactsProvider(authorization),
        )
        ledger = ExternalOperationService(engine, principal)
        branch_service = GitHubBranchPublicationService(
            approvals, ledger, GitHubPublicationTransport(authorization)
        )
        pull_request_service = GitHubPullRequestService(
            approvals, ledger, GitHubPRTransport(authorization)
        )
        handler = ApprovedPublicationActivityHandler(
            engine,
            principal,
            transitions,
            branch_service,
            pull_request_service,
        )
        runner = ActivityRunner(
            engine,
            principal,
            {"publish_approved": handler},
            lease_owner=f"publication-worker:{os.getpid()}",
        )
        return PublicationWorkerResources(runner, storage, engine)
    except Exception:
        if storage is not None:
            try:
                await storage.aclose()
            except Exception:
                pass
        engine.dispose()
        raise


def _require_worker_settings(settings: Any) -> None:
    if not getattr(settings, "enable_github_auth", False):
        raise WorkerConfigurationError("github_auth_disabled")
    if not _has_secret(getattr(settings, "runtime_database_url", None)):
        raise WorkerConfigurationError("runtime_database_unavailable")
    if not _has_secret(getattr(settings, "encryption_key", None)):
        raise WorkerConfigurationError("credential_cipher_unavailable")
    if type(getattr(settings, "github_app_id", None)) is not int or settings.github_app_id <= 0:
        raise WorkerConfigurationError("github_app_configuration_invalid")
    if not _has_secret(getattr(settings, "github_app_private_key", None)):
        raise WorkerConfigurationError("github_app_configuration_invalid")
    if not isinstance(getattr(settings, "supabase_url", None), str) or not settings.supabase_url:
        raise WorkerConfigurationError("publication_storage_unavailable")
    if not _has_secret(getattr(settings, "supabase_secret_key", None)):
        raise WorkerConfigurationError("publication_storage_unavailable")


def _has_secret(value: Any) -> bool:
    return callable(getattr(value, "get_secret_value", None)) and bool(
        value.get_secret_value().strip()
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m continuum_api.worker",
        description=(
            "Run a bounded drain of existing approved GitHub publication work. "
            "Exit status 0 means only that the bounded drain ended."
        ),
    )
    parser.add_argument("--account-id", type=UUID)
    parser.add_argument("--run-id", type=UUID)
    parser.add_argument("--max-jobs", type=_max_jobs, default=1)
    parser.add_argument(
        "--delete-objects",
        action="store_true",
        help="Explicitly delete due private objects during --retention",
    )
    parser.add_argument("--max-seconds", type=_max_seconds, default=30.0)
    execution = parser.add_mutually_exclusive_group()
    execution.add_argument(
        "--execute-publication",
        action="store_true",
        help="explicitly authorize this bounded publication worker invocation",
    )
    execution.add_argument(
        "--retention",
        action="store_true",
        help="explicitly authorize one bounded expiry and deletion cleanup drain",
    )
    return parser


def _max_jobs(value: str) -> int:
    parsed = int(value)
    if not 1 <= parsed <= 100:
        raise argparse.ArgumentTypeError("must be between 1 and 100")
    return parsed


def _max_seconds(value: str) -> float:
    parsed = float(value)
    if not 0 < parsed <= 300:
        raise argparse.ArgumentTypeError("must be greater than 0 and at most 300")
    return parsed


def _receipt_record(run_id: UUID, receipt: Any, max_jobs: int) -> dict[str, Any]:
    jobs_started = receipt.jobs_started
    stop_reason = getattr(receipt.stop_reason, "value", receipt.stop_reason)
    if (
        receipt.run_id != run_id
        or type(jobs_started) is not int
        or not 0 <= jobs_started <= max_jobs
        or stop_reason not in _STOP_REASONS
    ):
        raise WorkerConfigurationError("worker_receipt_invalid")
    return {
        "run_id": str(run_id),
        "jobs_started": jobs_started,
        "stop_reason": stop_reason,
    }


def _error_code(error: Exception) -> str:
    code = getattr(error, "code", None)
    return code if isinstance(code, str) and _ERROR_CODE.fullmatch(code) else "worker_failed"


def _write_json(output: TextIO, record: dict[str, Any]) -> None:
    output.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
