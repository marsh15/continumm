"""Fail-closed configuration and admission pin checks for workflow workers."""

from __future__ import annotations

import asyncio
import base64
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine

from continuum_api.credentials import CredentialProvider
from continuum_api.runtime import workflow_resources as resources
from continuum_api.sandbox.config import WorkflowSandboxSettings

IMAGE = "registry.example/continuum@sha256:" + "a" * 64


def _base_settings() -> SimpleNamespace:
    from pydantic import SecretStr

    return SimpleNamespace(
        enable_github_auth=True,
        runtime_database_url=SecretStr("postgresql://continuum_runtime:secret@127.0.0.1/db"),
        encryption_key=SecretStr("key"),
        github_app_id=123,
        github_app_private_key=SecretStr("private-key"),
        supabase_url="https://project.supabase.co",
        supabase_secret_key=SecretStr("supabase-secret"),
        supabase_bucket="checkpoints",
    )


def test_workflow_sandbox_configuration_requires_only_selected_profile_image() -> None:
    settings = WorkflowSandboxSettings(
        vercel_token="token",
        vercel_team_id="team",
        vercel_project_id="project",
        sandbox_provider_period_id="period-a",
        python_profile_image=IMAGE,
    )

    assert settings.require_control_plane()[1] == "period-a"
    assert settings.require_profile_image("python-pytest") == IMAGE
    with pytest.raises(ValueError, match="node_profile_image_unconfigured"):
        settings.require_profile_image("node-npm")
    with pytest.raises(ValueError, match="workflow_profile_unsupported"):
        settings.require_profile_image("other")


def test_admission_pin_read_is_owner_scoped_and_allows_missing_key(monkeypatch) -> None:
    account_id, run_id = uuid4(), uuid4()
    now = datetime.now(UTC)
    row = _admission_row(now)
    row["status"] = "waiting_credentials"
    row["credential_provider"] = None
    seen_principals = []

    class Repository:
        def __init__(self, connection, principal):
            seen_principals.append(principal.account_id)

    class Connection:
        def execute(self, statement, parameters=None):
            sql = str(statement)
            if "SELECT clock_timestamp()" in sql:
                return SimpleNamespace(scalar_one=lambda: now)
            assert parameters == {"owner": account_id, "run": run_id}
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))

    class Engine:
        @contextmanager
        def begin(self):
            yield Connection()

    monkeypatch.setattr(resources, "RuntimeRepository", Repository)
    pins = resources._read_admission_pins(Engine(), account_id, run_id)

    assert seen_principals == [account_id]
    assert pins.provider is CredentialProvider.OPENAI
    assert pins.model_id == resources.OPENAI_MODEL_ID
    assert pins.profile_id == "python-pytest"


@pytest.mark.parametrize(
    ("change", "expected_code"),
    [
        ({"content_deleted_at": datetime.now(UTC)}, "workflow_run_inactive"),
        ({"content_expires_at": datetime.now(UTC) - timedelta(seconds=1)}, "workflow_run_inactive"),
        ({"deadline_at": datetime.now(UTC) - timedelta(seconds=1)}, "workflow_run_inactive"),
        ({"credential_provider": "anthropic"}, "workflow_provider_pin_invalid"),
        ({"model_id": "unadmitted-model"}, "workflow_provider_pin_invalid"),
        ({"profile_version": "2"}, "workflow_profile_pin_invalid"),
    ],
)
def test_admission_pins_reject_stale_or_mismatched_state(
    monkeypatch, change, expected_code
) -> None:
    now = datetime.now(UTC)
    row = _admission_row(now)
    row.update(change)

    class Repository:
        def __init__(self, connection, principal):
            pass

    class Connection:
        def execute(self, statement, parameters=None):
            if "SELECT clock_timestamp()" in str(statement):
                return SimpleNamespace(scalar_one=lambda: now)
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))

    class Engine:
        @contextmanager
        def begin(self):
            yield Connection()

    monkeypatch.setattr(resources, "RuntimeRepository", Repository)
    with pytest.raises(resources.WorkflowConfigurationError) as error:
        resources._read_admission_pins(Engine(), uuid4(), uuid4())
    assert error.value.code == expected_code


def test_provider_period_requires_one_current_period_and_both_pool_rows() -> None:
    rows = [("period-a", "public"), ("period-a", "operator")]
    _assert_period_rows(rows, "period-a")
    for bad_rows, configured in (
        (rows + [("period-b", "public")], "period-a"),
        ([("period-a", "public")], "period-a"),
        (rows, "period-b"),
    ):
        with pytest.raises(resources.WorkflowConfigurationError) as error:
            _assert_period_rows(bad_rows, configured)
        assert error.value.code == "workflow_provider_period_mismatch"


def test_factory_closes_created_resources_when_construction_is_cancelled(monkeypatch) -> None:
    account_id, run_id = uuid4(), uuid4()
    closed = []

    class Engine:
        def dispose(self):
            closed.append("engine")

    class Storage:
        async def aclose(self):
            closed.append("storage")

    engine, storage = Engine(), Storage()
    sandbox_settings = WorkflowSandboxSettings(
        vercel_token="token",
        vercel_team_id="team",
        vercel_project_id="project",
        sandbox_provider_period_id="period-a",
        python_profile_image=IMAGE,
    )
    monkeypatch.setattr(resources, "WorkflowSandboxSettings", lambda: sandbox_settings)
    monkeypatch.setattr(resources, "create_runtime_engine", lambda _url: engine)
    monkeypatch.setattr(
        resources,
        "_read_admission_pins",
        lambda *_args: resources._AdmissionPins(
            "python-pytest", "1", CredentialProvider.OPENAI, resources.OPENAI_MODEL_ID
        ),
    )
    monkeypatch.setattr(resources, "_require_current_provider_period", lambda *_args: None)
    monkeypatch.setattr(resources, "SupabaseStorage", lambda *_args, **_kwargs: storage)
    monkeypatch.setattr(resources.CredentialCipher, "from_settings", lambda _settings: object())

    class CancelledTokenStore:
        def __init__(self, *_args, **_kwargs):
            raise asyncio.CancelledError

    monkeypatch.setattr(resources, "GitHubUserTokenStore", CancelledTokenStore)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(resources.build_workflow_resources(_base_settings(), account_id, run_id))
    assert closed == ["storage", "engine"]


@pytest.mark.parametrize(
    ("profile_id", "provider", "adapter_type"),
    [
        ("python-pytest", CredentialProvider.OPENAI, "OpenAIModelAdapter"),
        ("node-npm", CredentialProvider.ANTHROPIC, "AnthropicModelAdapter"),
    ],
)
@pytest.mark.parametrize("evaluation", [False, True])
def test_factory_wires_admitted_profile_and_provider_without_hosted_calls(
    monkeypatch, profile_id, provider, adapter_type, evaluation
) -> None:
    account_id, run_id = uuid4(), uuid4()
    closed = []
    pins = resources._AdmissionPins(
        profile_id,
        "1",
        provider,
        resources.OPENAI_MODEL_ID
        if provider is CredentialProvider.OPENAI
        else resources.ANTHROPIC_MODEL_ID,
    )
    sandbox_settings = WorkflowSandboxSettings(
        vercel_token="token",
        vercel_team_id="team",
        vercel_project_id="project",
        sandbox_provider_period_id="period-a",
        python_profile_image=IMAGE,
        node_profile_image=IMAGE.replace("a" * 64, "b" * 64),
    )
    settings = _base_settings()
    from pydantic import SecretStr

    settings.encryption_key = SecretStr(base64.urlsafe_b64encode(b"k" * 32).decode())
    engine = create_engine("sqlite://")
    monkeypatch.setattr(engine, "dispose", lambda: closed.append("engine"))

    class Storage:
        async def aclose(self):
            closed.append("storage")

    storage = Storage()
    calls = {}
    runner = object()
    monkeypatch.setattr(resources, "WorkflowSandboxSettings", lambda: sandbox_settings)
    monkeypatch.setattr(resources, "create_runtime_engine", lambda _url: engine)
    monkeypatch.setattr(resources, "_read_admission_pins", lambda *_args: pins)
    monkeypatch.setattr(resources, "_require_current_provider_period", lambda *_args: None)
    monkeypatch.setattr(resources, "SupabaseStorage", lambda *_args, **_kwargs: storage)
    monkeypatch.setattr(
        resources,
        "build_workflow_activity_runner",
        lambda *_args, **kwargs: calls.update(kwargs) or runner,
    )

    def unexpected_sandbox_provider(*_args, **_kwargs):
        pytest.fail("sandbox provider must be created only inside an admitted activity")

    monkeypatch.setattr(resources, "VercelSandboxProvider", unexpected_sandbox_provider)
    built = asyncio.run(
        resources.build_workflow_resources(settings, account_id, run_id, evaluation=evaluation)
    )

    validation = calls["validation"]
    assert calls["evaluation"] is evaluation
    assert validation._allocation_class == ("operator" if evaluation else "public")
    assert built.runner is runner
    if evaluation:
        assert built.hidden_checks is not None
        assert built.hidden_checks._profiles == validation._profiles
        assert built.hidden_checks._creator is validation._allocation_creator
    else:
        assert built.hidden_checks is None
    assert type(calls["model_adapter"]).__name__ == adapter_type
    assert validation._profiles["python-pytest"].configured_image == (
        IMAGE if profile_id == "python-pytest" else None
    )
    assert validation._profiles["node-npm"].configured_image == (
        sandbox_settings.node_profile_image if profile_id == "node-npm" else None
    )
    asyncio.run(built.aclose())
    asyncio.run(built.aclose())
    assert closed == ["storage", "engine"]


def _assert_period_rows(rows, configured):
    class Connection:
        def execute(self, statement):
            return SimpleNamespace(all=lambda: rows)

    class Engine:
        @contextmanager
        def begin(self):
            yield Connection()

    resources._require_current_provider_period(Engine(), configured)


def _admission_row(now: datetime) -> dict[str, object]:
    return {
        "status": "queued",
        "deadline_at": now + timedelta(hours=1),
        "created_at": now,
        "content_expires_at": now + timedelta(days=1),
        "content_deleted_at": None,
        "account_status": "active",
        "profile_id": "python-pytest",
        "profile_version": "1",
        "provider": "openai",
        "model_id": resources.OPENAI_MODEL_ID,
        "credential_provider": "openai",
    }
