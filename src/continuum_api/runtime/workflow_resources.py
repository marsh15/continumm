"""Production resource graph for one owner-scoped admitted workflow run."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from continuum_api.artifacts.archives import MAX_COMPRESSED_BYTES
from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.auth.github import (
    GitHubAccessVerifier,
    GitHubAuthorizationClient,
    GitHubPublicationFactsProvider,
)
from continuum_api.auth.sessions import GitHubUserTokenStore
from continuum_api.connectivity.storage import SupabaseStorage
from continuum_api.credentials import CredentialCipher, CredentialProvider
from continuum_api.publication.github import GitHubPublicationTransport
from continuum_api.publication.github_pr import GitHubPRTransport
from continuum_api.runtime.activity_runner import ActivityRunner
from continuum_api.runtime.agent_loop import (
    ANTHROPIC_MODEL_ID,
    OPENAI_MODEL_ID,
    DurableAgentLoopService,
)
from continuum_api.runtime.approvals import ApprovalService
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.database import create_runtime_engine
from continuum_api.runtime.external_ledger import ExternalOperationService
from continuum_api.runtime.github_pr_publication import GitHubPullRequestService
from continuum_api.runtime.github_publication import GitHubBranchPublicationService
from continuum_api.runtime.github_source_fetcher import GitHubSourceFetcher
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.runtime.provider_credentials import CredentialService
from continuum_api.runtime.sandbox_allocations import SandboxAllocationStore
from continuum_api.runtime.scheduler import RuntimeRecoveryService
from continuum_api.runtime.source_preparation import SourceFetcher
from continuum_api.runtime.transitions import TransitionService
from continuum_api.runtime.trusted_tool_policy import PersistedToolPolicyContextResolver
from continuum_api.runtime.workflow_registry import build_workflow_activity_runner
from continuum_api.runtime.workspace_recovery import WorkspaceRecoveryService
from continuum_api.sandbox.allocation_service import SandboxAllocationService
from continuum_api.sandbox.config import (
    ALLOCATION_MEMORY_MB,
    ALLOCATION_VCPUS,
    SandboxAllocationLimits,
    VercelCredentials,
    WorkflowSandboxSettings,
)
from continuum_api.sandbox.node_profile import NodeProfileService
from continuum_api.sandbox.python_profile import PythonProfileService
from continuum_api.sandbox.vercel import VercelSandboxProvider
from continuum_api.validation.service import ValidationService


class WorkflowConfigurationError(RuntimeError):
    """Sanitized production worker configuration failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(slots=True)
class WorkflowResources:
    """Bound runner plus the process resources that must outlive its work."""

    runner: ActivityRunner
    storage: Any
    engine: Engine
    model_adapter: Any | None = None
    _closed: bool = False

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self.model_adapter is not None:
                close = getattr(self.model_adapter, "close", None)
                if callable(close):
                    result = close()
                    if hasattr(result, "__await__"):
                        await result
        finally:
            try:
                await self.storage.aclose()
            finally:
                self.engine.dispose()


@dataclass(frozen=True, slots=True)
class _AdmissionPins:
    profile_id: str
    profile_version: str
    provider: CredentialProvider
    model_id: str


async def build_workflow_resources(
    settings: Any, account_id: UUID, run_id: UUID
) -> WorkflowResources:
    """Construct trusted services for one active, pinned run without provider I/O.

    The sole construction-time database read is bounded to this owner and run.
    GitHub, Supabase, model, and Vercel clients are only wired here; their network
    sessions start inside individual activities after durable authorization.
    """

    _require_base_settings(settings)
    sandbox_settings = WorkflowSandboxSettings()
    credentials, provider_period_id = _require_sandbox_configuration(sandbox_settings)
    engine = create_runtime_engine(settings.runtime_database_url.get_secret_value())
    storage: Any | None = None
    model_adapter: Any | None = None
    try:
        pins = _read_admission_pins(engine, account_id, run_id)
        _require_current_provider_period(engine, provider_period_id)
        image = _require_profile_image(sandbox_settings, pins)

        # All required configuration is checked before constructing hosted clients.
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
        github_access = GitHubAccessVerifier(authorization)

        recovery = RuntimeRecoveryService(engine, principal)
        artifact_persistence = ArtifactPersistence(engine)
        transitions = TransitionService(
            engine, principal, artifact_persistence=artifact_persistence
        )
        checkpoints = CheckpointStorageService(
            artifact_persistence,
            storage,
            transitions,
            recovery,
            access_verifier=github_access,
        )
        credential_service = CredentialService(engine, principal, cipher)

        allocation_store = SandboxAllocationStore(engine)

        def provider_factory(
            limits: SandboxAllocationLimits,
        ) -> VercelSandboxProvider:
            if (
                limits.image != image
                or limits.vcpus != ALLOCATION_VCPUS
                or limits.memory_mb != ALLOCATION_MEMORY_MB
            ):
                raise WorkflowConfigurationError("workflow_sandbox_limits_mismatch")
            return VercelSandboxProvider(limits=limits, credentials=credentials)

        allocations = SandboxAllocationService(
            allocation_store,
            image=image,
            provider_period_id=provider_period_id,
            provider_factory=provider_factory,
        )
        workspace_recovery = WorkspaceRecoveryService(
            engine,
            principal,
            checkpoints,
            transitions,
            allocation_store=allocation_store,
            allocation_reconciler=allocations,
        )
        agent_service = DurableAgentLoopService(
            engine,
            principal,
            transitions,
            workspace_recovery=workspace_recovery,
            checkpoints=checkpoints,
            credential_service=credential_service,
        )
        python_profile = PythonProfileService(
            allocation_store,
            checkpoints,
            allocations,
            image=image if pins.profile_id == "python-pytest" else None,
            provider_period_id=provider_period_id,
            provider_factory=provider_factory,
        )
        node_profile = NodeProfileService(
            allocation_store,
            checkpoints,
            allocations,
            image=image if pins.profile_id == "node-npm" else None,
            provider_period_id=provider_period_id,
            provider_factory=provider_factory,
        )
        validation = ValidationService(
            python_profile,
            node_profile,
            allocations,
        )

        approvals = ApprovalService(
            engine,
            principal,
            transitions,
            storage,
            publication_facts=GitHubPublicationFactsProvider(authorization),
        )
        ledger = ExternalOperationService(engine, principal)
        branch_publication = GitHubBranchPublicationService(
            approvals, ledger, GitHubPublicationTransport(authorization)
        )
        pull_request_publication = GitHubPullRequestService(
            approvals, ledger, GitHubPRTransport(authorization)
        )
        fetch_source: SourceFetcher = GitHubSourceFetcher(engine, principal, authorization)
        tool_policy = PersistedToolPolicyContextResolver(
            engine, principal, checkpoints, github_access
        )
        model_adapter = _model_adapter(pins.provider)
        runner = build_workflow_activity_runner(
            engine,
            principal,
            checkpoints=checkpoints,
            fetch_source=fetch_source,
            agent_service=agent_service,
            model_adapter=model_adapter,
            resolve_tool_policy_context=tool_policy,
            resolve_validation_policy_context=tool_policy.for_validation,
            validation=validation,
            approvals=approvals,
            transitions=transitions,
            branch_publication=branch_publication,
            pull_request_publication=pull_request_publication,
            lease_owner=f"workflow-worker:{os.getpid()}:{run_id}",
        )
        return WorkflowResources(runner, storage, engine, model_adapter)
    except BaseException:
        if model_adapter is not None:
            try:
                close = getattr(model_adapter, "close", None)
                if callable(close):
                    result = close()
                    if hasattr(result, "__await__"):
                        await result
            except BaseException:
                pass
        if storage is not None:
            try:
                await storage.aclose()
            except BaseException:
                pass
        engine.dispose()
        raise


def _require_base_settings(settings: Any) -> None:
    if not getattr(settings, "enable_github_auth", False):
        raise WorkflowConfigurationError("github_auth_disabled")
    if not _has_secret(getattr(settings, "runtime_database_url", None)):
        raise WorkflowConfigurationError("runtime_database_unavailable")
    if not _has_secret(getattr(settings, "encryption_key", None)):
        raise WorkflowConfigurationError("credential_cipher_unavailable")
    if type(getattr(settings, "github_app_id", None)) is not int or settings.github_app_id <= 0:
        raise WorkflowConfigurationError("github_app_configuration_invalid")
    if not _has_secret(getattr(settings, "github_app_private_key", None)):
        raise WorkflowConfigurationError("github_app_configuration_invalid")
    if not isinstance(getattr(settings, "supabase_url", None), str) or not settings.supabase_url:
        raise WorkflowConfigurationError("workflow_storage_unavailable")
    if not _has_secret(getattr(settings, "supabase_secret_key", None)):
        raise WorkflowConfigurationError("workflow_storage_unavailable")


def _require_sandbox_configuration(
    settings: WorkflowSandboxSettings,
) -> tuple[VercelCredentials, str]:
    try:
        return settings.require_control_plane()
    except ValueError as error:
        raise WorkflowConfigurationError(
            _error_code(error, "workflow_sandbox_configuration_invalid")
        ) from None


def _require_profile_image(settings: WorkflowSandboxSettings, pins: _AdmissionPins) -> str:
    try:
        return settings.require_profile_image(pins.profile_id)
    except ValueError as error:
        raise WorkflowConfigurationError(
            _error_code(error, "workflow_profile_configuration_invalid")
        ) from None


def _read_admission_pins(engine: Engine, account_id: UUID, run_id: UUID) -> _AdmissionPins:
    if not isinstance(account_id, UUID) or not isinstance(run_id, UUID):
        raise WorkflowConfigurationError("workflow_identity_invalid")
    try:
        with engine.begin() as connection:
            principal = Principal(account_id)
            RuntimeRepository(connection, principal)
            row = (
                connection.execute(
                    text(
                        "SELECT run.status, run.deadline_at, run.created_at, "
                        "run.content_expires_at, run.content_deleted_at, "
                        "account.status AS account_status, pins.profile_id, "
                        "pins.profile_version, pins.provider, pins.model_id, "
                        "binding.provider AS credential_provider "
                        "FROM runtime_private.runs AS run "
                        "JOIN runtime_private.users AS account ON account.user_id=run.owner_id "
                        "JOIN runtime_private.run_admission_pins AS pins "
                        "ON pins.owner_id=run.owner_id AND pins.run_id=run.run_id "
                        "LEFT JOIN runtime_private.run_credentials AS binding "
                        "ON binding.owner_id=run.owner_id AND binding.run_id=run.run_id "
                        "WHERE run.owner_id=:owner AND run.run_id=:run"
                    ),
                    {"owner": account_id, "run": run_id},
                )
                .mappings()
                .one_or_none()
            )
            now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
    except Exception:
        raise WorkflowConfigurationError("workflow_admission_unavailable") from None
    if row is None:
        raise WorkflowConfigurationError("workflow_run_not_found")
    deadline = row["deadline_at"] or (row["created_at"] + timedelta(days=7))
    if row["account_status"] != "active":
        raise WorkflowConfigurationError("workflow_account_inactive")
    if (
        row["status"]
        not in {
            "queued",
            "running",
            "waiting_approval",
            "waiting_credentials",
            "waiting_capacity",
            "needs_attention",
        }
        or deadline <= now
        or row["content_deleted_at"] is not None
        or row["content_expires_at"] <= now
    ):
        raise WorkflowConfigurationError("workflow_run_inactive")
    provider_models = {
        "openai": (CredentialProvider.OPENAI, OPENAI_MODEL_ID),
        "anthropic": (CredentialProvider.ANTHROPIC, ANTHROPIC_MODEL_ID),
    }
    expected = provider_models.get(row["provider"])
    if (
        expected is None
        or row["model_id"] != expected[1]
        or (
            row["credential_provider"] is not None and row["credential_provider"] != row["provider"]
        )
    ):
        raise WorkflowConfigurationError("workflow_provider_pin_invalid")
    if row["profile_id"] not in {"python-pytest", "node-npm"} or row["profile_version"] != "1":
        raise WorkflowConfigurationError("workflow_profile_pin_invalid")
    return _AdmissionPins(
        profile_id=row["profile_id"],
        profile_version=row["profile_version"],
        provider=expected[0],
        model_id=expected[1],
    )


def _require_current_provider_period(engine: Engine, configured_period_id: str) -> None:
    try:
        with engine.begin() as connection:
            period_rows = connection.execute(
                text(
                    "SELECT period.provider_period_id, usage.allocation_class "
                    "FROM runtime_private.sandbox_provider_periods AS period "
                    "LEFT JOIN runtime_private.sandbox_period_usage AS usage "
                    "USING (provider_period_id) "
                    "WHERE period.starts_at <= clock_timestamp() "
                    "AND period.ends_at > clock_timestamp() "
                    "ORDER BY period.provider_period_id, usage.allocation_class LIMIT 5"
                )
            ).all()
            periods = [(str(row[0]), row[1]) for row in period_rows]
    except Exception:
        raise WorkflowConfigurationError("workflow_provider_period_unavailable") from None
    if (
        len(periods) != 2
        or any(row[0] != configured_period_id for row in periods)
        or {row[1] for row in periods} != {"public", "operator"}
    ):
        raise WorkflowConfigurationError("workflow_provider_period_mismatch")


def _model_adapter(provider: CredentialProvider) -> Any:
    if provider is CredentialProvider.OPENAI:
        from continuum_api.adapters.openai import OpenAIModelAdapter

        return OpenAIModelAdapter()
    if provider is CredentialProvider.ANTHROPIC:
        from continuum_api.adapters.anthropic import AnthropicModelAdapter

        return AnthropicModelAdapter()
    raise WorkflowConfigurationError("workflow_provider_unsupported")


def _has_secret(value: Any) -> bool:
    return callable(getattr(value, "get_secret_value", None)) and bool(
        value.get_secret_value().strip()
    )


def _error_code(error: ValueError, fallback: str) -> str:
    candidate = str(error)
    if (
        candidate
        and candidate.replace("_", "").isalnum()
        and candidate[0].islower()
        and len(candidate) <= 64
    ):
        return candidate
    return fallback
