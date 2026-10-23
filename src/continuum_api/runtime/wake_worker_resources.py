"""Production dependency graph for the opt-in bounded wake worker."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import SecretStr
from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import URL, make_url

from continuum_api.runtime.inbox_events import (
    parse_public_issue_comment,
    parse_public_issue_comment_identity,
)
from continuum_api.runtime.wake_worker import BoundedWakeWorker
from continuum_api.runtime.wake_worker_store import (
    InboxWork,
    PostgresWakeWorkerStore,
    StatusWork,
)
from continuum_api.runtime.wake_worker_store import (
    RunWork as StoreRunWork,
)

logger = logging.getLogger(__name__)


class WakeWorkerConfigurationError(RuntimeError):
    """A sanitized wake worker configuration failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(slots=True)
class WakeWorkerResources:
    """Wake worker and the shared resources retained for its callbacks."""

    worker: BoundedWakeWorker
    wake_engine: Engine
    runtime_engine: Engine
    storage: Any
    _closed: bool = False

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self.storage.aclose()
        finally:
            try:
                self.runtime_engine.dispose()
            finally:
                self.wake_engine.dispose()


def create_wake_worker_engine(database_url: str, *, production: bool) -> Engine:
    """Create a pool for the dedicated worker role after strict URL validation."""
    url = _validated_worker_url(database_url, production=production)
    return create_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=3,
        pool_pre_ping=True,
        pool_recycle=300,
        hide_parameters=True,
        connect_args={
            "application_name": "continuumm-wake-worker",
            "connect_timeout": 5,
            "options": "-c statement_timeout=5000 -c lock_timeout=2000",
        },
    )


def _validated_worker_url(database_url: str, *, production: bool) -> URL:
    try:
        url = make_url(database_url)
        parts = urlsplit(database_url)
        if (
            url.drivername not in {"postgres", "postgresql", "postgresql+psycopg"}
            or not url.host
            or url.username != "continuum_wake_worker"
            or not url.password
            or not url.database
            or parts.fragment
            or url.query.keys() - {"sslmode", "sslrootcert"}
        ):
            raise ValueError
        try:
            local_host = ipaddress.ip_address(url.host).is_loopback
        except ValueError:
            local_host = url.host.rstrip(".").lower() == "localhost"
        sslmode = url.query.get("sslmode")
        sslrootcert = url.query.get("sslrootcert")
        if production or not local_host:
            if sslmode not in {None, "verify-full"} or sslrootcert not in {None, "system"}:
                raise ValueError
            url = url.set(query={"sslmode": "verify-full", "sslrootcert": "system"})
        elif sslmode not in {None, "disable", "verify-full"}:
            raise ValueError
        elif sslmode == "verify-full" and sslrootcert not in {None, "system"}:
            raise ValueError
        elif sslmode is None:
            url = url.set(query={"sslmode": "disable"})
        return url.set(drivername="postgresql+psycopg")
    except Exception:
        raise WakeWorkerConfigurationError("wake_worker_database_url_invalid") from None


async def build_wake_worker_resources(settings: Any) -> WakeWorkerResources:
    """Build a production worker without performing network/provider operations.

    All configuration is checked before an engine or hosted client is created.
    GitHub verification, command execution, and workflow activity begin only from
    the worker's bounded callbacks after the durable wake has been claimed.
    """
    if getattr(settings, "enable_wake_worker", False) is not True:
        raise WakeWorkerConfigurationError("wake_worker_disabled")

    from continuum_api.auth.github import GitHubAuthorizationClient, GitHubPublicationFactsProvider
    from continuum_api.auth.sessions import GitHubUserTokenStore
    from continuum_api.connectivity.storage import SupabaseStorage
    from continuum_api.credentials import CredentialCipher
    from continuum_api.runtime.admission import AdmissionService
    from continuum_api.runtime.approvals import ApprovalService
    from continuum_api.runtime.cancellation import CancellationService
    from continuum_api.runtime.database import create_runtime_engine
    from continuum_api.runtime.github_command_execution import GitHubCommandProcessor
    from continuum_api.runtime.github_commands import GitHubCommandError, GitHubCommandService
    from continuum_api.runtime.github_status_comments import (
        GitHubStatusCommentLedger,
        GitHubStatusCommentPublisher,
    )
    from continuum_api.runtime.persistence import Principal
    from continuum_api.runtime.provider_credentials import CredentialService
    from continuum_api.runtime.scheduler import RuntimeRecoveryService
    from continuum_api.runtime.transitions import TransitionService
    from continuum_api.runtime.workflow_resources import (
        _require_base_settings,
        _require_sandbox_configuration,
        build_workflow_resources,
    )
    from continuum_api.sandbox.config import WorkflowSandboxSettings

    try:
        _require_base_settings(settings)
        _require_sandbox_configuration(WorkflowSandboxSettings())
        if not _valid_public_origin(getattr(settings, "github_public_origin", None)):
            raise WakeWorkerConfigurationError("github_public_origin_invalid")
    except Exception as error:
        code = getattr(error, "code", None) or str(error)
        if not _safe_code(code):
            code = "wake_worker_configuration_invalid"
        raise WakeWorkerConfigurationError(code) from None
    wake_url = getattr(settings, "wake_worker_database_url", None)
    if not isinstance(wake_url, SecretStr) or not wake_url.get_secret_value().strip():
        raise WakeWorkerConfigurationError("wake_worker_database_unavailable")
    try:
        wake_url_text = wake_url.get_secret_value()
        cipher = CredentialCipher.from_settings(settings)
        wake_engine = create_wake_worker_engine(
            wake_url_text,
            production=getattr(settings, "environment", "development") == "production",
        )
        runtime_engine = create_runtime_engine(settings.runtime_database_url.get_secret_value())
    except WakeWorkerConfigurationError:
        raise
    except Exception:
        if "runtime_engine" in locals():
            runtime_engine.dispose()
        if "wake_engine" in locals():
            wake_engine.dispose()
        raise WakeWorkerConfigurationError("wake_worker_configuration_invalid") from None

    storage: Any | None = None
    try:
        storage = SupabaseStorage(
            settings.supabase_url,
            settings.supabase_secret_key.get_secret_value(),
            settings.supabase_bucket,
        )
        wake_store = PostgresWakeWorkerStore(wake_engine, github_app_id=settings.github_app_id)

        async def publish_status(work: StatusWork) -> bool:
            """Publish or reconcile one status under its persisted owner identity."""
            owner_id = _uuid(work.owner_id)
            run_id = _uuid(work.run_id)
            if owner_id is None or run_id is None:
                return False
            principal = Principal(owner_id)
            try:
                tokens = GitHubUserTokenStore(runtime_engine, cipher, owner_id)
                authorization = GitHubAuthorizationClient(
                    app_id=settings.github_app_id,
                    app_private_key=settings.github_app_private_key,
                    user_tokens=tokens,
                )
                transitions = TransitionService(runtime_engine, principal)
                publication_facts = GitHubPublicationFactsProvider(authorization)
                approvals = ApprovalService(
                    runtime_engine,
                    principal,
                    transitions,
                    storage,
                    publication_facts=publication_facts,
                )
                ledger = GitHubStatusCommentLedger(runtime_engine, principal)
                publisher = GitHubStatusCommentPublisher(
                    principal,
                    ledger,
                    approvals,
                    authorization,
                    settings.github_public_origin,
                )
                if work.reconcile_only:
                    await publisher.reconcile(run_id, work.event_sequence)
                    return True

                snapshot = ledger.current_snapshot(run_id)
                if snapshot["event_sequence"] != work.event_sequence:
                    return False
                await publisher.publish(
                    run_id=run_id,
                    repository_id=snapshot["github_repository_id"],
                    issue_number=snapshot["issue_number"],
                    status=snapshot["status"],
                    revision=snapshot["revision"],
                    expected_event_sequence=work.event_sequence,
                )
                return True
            except Exception as error:
                # Status publication is best-effort and must not fail a run or
                # inbox callback. Log the exception type only; provider errors
                # may carry sensitive request details.
                logger.warning("wake_status_comment_failed error_type=%s", type(error).__name__)
                return False

        async def process_inbox(work: InboxWork) -> str:
            if (
                work.event_name != "issue_comment"
                or getattr(work, "github_app_id", None) != settings.github_app_id
            ):
                return "inert"
            identity = parse_public_issue_comment_identity(work.payload)
            if identity is None:
                return "inert"
            command_state = getattr(work, "command_state", "none")
            if command_state == "expired":
                return "rejected"
            if command_state == "accepted_unresolved" and work.owner_id is None:
                raise GitHubCommandError("github_command_result_pending", retryable=True)
            if command_state == "completed":
                return "processed"
            if command_state not in {"none", "accepted_unresolved"}:
                raise WakeWorkerConfigurationError("wake_worker_command_state_invalid")
            event = parse_public_issue_comment(work.payload)
            if work.owner_id is None:
                return "inert"
            owner_id = _uuid(work.owner_id)
            if owner_id is None:
                return "inert"
            principal = Principal(owner_id)
            tokens = GitHubUserTokenStore(runtime_engine, cipher, owner_id)
            authorization = GitHubAuthorizationClient(
                app_id=settings.github_app_id,
                app_private_key=settings.github_app_private_key,
                user_tokens=tokens,
            )
            transitions = TransitionService(runtime_engine, principal)
            publication_facts = GitHubPublicationFactsProvider(authorization)
            approvals = ApprovalService(
                runtime_engine,
                principal,
                transitions,
                storage,
                publication_facts=publication_facts,
            )
            credentials = CredentialService(runtime_engine, principal, cipher)
            admissions = AdmissionService(
                runtime_engine,
                principal,
                publication_facts,
                credentials,
                enabled=getattr(settings, "enable_public_admission", False) is True,
            )
            cancellations = CancellationService(runtime_engine, principal)
            intake = GitHubCommandService(runtime_engine, principal, authorization)
            processor = GitHubCommandProcessor(
                runtime_engine,
                principal,
                approvals,
                cancellations,
                admissions,
            )
            try:
                receipt = await intake.accept_created_comment(event) if event is not None else None
                if receipt is None and command_state == "accepted_unresolved":
                    repository_id, comment_id, _author_id = identity
                    pending = await asyncio.to_thread(intake.pending, limit=500)
                    receipt = next(
                        (
                            candidate
                            for candidate in pending
                            if candidate.repository_id == repository_id
                            and candidate.comment_id == comment_id
                        ),
                        None,
                    )
                if receipt is None:
                    if command_state == "accepted_unresolved":
                        raise GitHubCommandError("github_command_result_pending", retryable=True)
                    return "inert"
                result = await processor.execute(receipt)
                # Command result persistence is part of execute. Status comments
                # run in the separate bounded phase after inbox and workflow work.
                return "rejected" if result.kind == "rejected" else "processed"
            except GitHubCommandError as error:
                if error.retryable or _temporarily_unverifiable(error.code):
                    raise
                if error.code in {"github_command_authorization_changed", "github_comment_stale"}:
                    return "rejected"
                raise

        async def run_workflow(work: StoreRunWork, max_jobs: int, max_seconds: float) -> Any:
            owner_id = _uuid(work.owner_id)
            run_id = _uuid(work.run_id)
            if owner_id is None or run_id is None:
                raise WakeWorkerConfigurationError("wake_worker_run_identity_invalid")
            await asyncio.to_thread(
                RuntimeRecoveryService(runtime_engine, Principal(owner_id)).recover_due,
                limit=max(1, min(max_jobs, 25)),
            )
            resources = await build_workflow_resources(settings, owner_id, run_id)
            try:
                return await resources.runner.run_admitted(
                    run_id, max_jobs=max_jobs, max_seconds=max_seconds
                )
            finally:
                await resources.aclose()

        worker = BoundedWakeWorker(
            wake_store, process_inbox, run_workflow, publish_status=publish_status
        )
        return WakeWorkerResources(worker, wake_engine, runtime_engine, storage)
    except Exception:
        if storage is not None:
            try:
                await storage.aclose()
            except Exception:
                pass
        runtime_engine.dispose()
        wake_engine.dispose()
        raise WakeWorkerConfigurationError("wake_worker_resource_construction_failed") from None


def _uuid(value: object) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if not isinstance(value, str):
        return None
    try:
        return UUID(value)
    except ValueError:
        return None


def _safe_code(value: object) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 64
        and value[0].islower()
        and value.replace("_", "").isalnum()
    )


def _valid_public_origin(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parts = urlsplit(value)
        return (
            parts.scheme == "https"
            and bool(parts.hostname)
            and parts.username is None
            and parts.password is None
            and not parts.query
            and not parts.fragment
        )
    except ValueError:
        return False


def _temporarily_unverifiable(code: str) -> bool:
    markers = (
        "unavailable",
        "pending",
        "timeout",
        "storage",
        "unverifiable",
        "authorization",
        "provider",
        "rate_limit",
        "request_limit",
        "response_limit",
    )
    return any(marker in code for marker in markers)
