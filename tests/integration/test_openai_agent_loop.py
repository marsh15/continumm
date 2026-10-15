"""T24 synthetic provider calls through durable turns and encrypted dispatch grants."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from openai import AuthenticationError
from pydantic import SecretStr
from sqlalchemy import text
from test_agent_loop import (
    T22Database,
    _checkpoint_service,
    _seed_agent_run,
    _storage_fixture,
)
from test_agent_loop import (
    t22_database as t22_database,
)

from continuum_api.adapters.openai import OpenAIModelAdapter
from continuum_api.credentials import CredentialCipher, CredentialProvider, CredentialScope
from continuum_api.runtime.agent_loop import (
    OPENAI_MODEL_ID,
    AgentLoopError,
    DurableAgentLoopService,
)
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.provider_credentials import CredentialService
from continuum_api.runtime.transitions import TransitionService


@pytest.mark.parametrize("response_case", ["known", "unknown", "auth_rejected"])
def test_missing_credential_resumes_and_accepted_response_reuses_without_dispatch(
    t22_database: T22Database, tmp_path: Path, response_case: str
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    queue = QueueService(store.runtime_engine, run.principal)
    lease = queue.claim_one("t24-before-credential")
    assert lease is not None
    storage, _ = _storage_fixture(objects)
    checkpoints = _checkpoint_service(
        store.runtime_engine, run.principal, ArtifactPersistence(store.runtime_engine), storage
    )
    credentials = CredentialService(
        store.runtime_engine, run.principal, CredentialCipher("v1", {"v1": os.urandom(32)})
    )
    service = DurableAgentLoopService(
        store.runtime_engine,
        run.principal,
        TransitionService(store.runtime_engine, run.principal),
        checkpoints=checkpoints,
        credential_service=credentials,
    )
    sent: list[dict[str, Any]] = []
    secret = "synthetic-t24-integration-key"

    class Client:
        @property
        def responses(self) -> Client:
            return self

        async def create(self, **kwargs: Any) -> Any:
            sent.append(kwargs)
            if response_case == "auth_rejected":
                raise AuthenticationError(
                    "synthetic-sensitive-provider-message",
                    response=httpx.Response(
                        401, request=httpx.Request("POST", "https://api.openai.com/v1/responses")
                    ),
                    body=None,
                )
            return SimpleNamespace(
                id="resp_t24_recover",
                model=OPENAI_MODEL_ID,
                status="completed",
                output=[],
                usage=(
                    None
                    if response_case == "unknown"
                    else SimpleNamespace(input_tokens=10, output_tokens=2)
                ),
                _request_id="req_t24_recover",
            )

        async def close(self) -> None:
            pass

    def factory(**kwargs: Any) -> Client:
        assert kwargs["api_key"] == secret
        assert kwargs["max_retries"] == 0
        return Client()

    adapter = OpenAIModelAdapter(client_factory=factory)
    turn_id = None

    async def exercise() -> None:
        nonlocal turn_id
        intent = await service.prepare_turn(
            lease,
            "t24-credential-resume",
            context={"objective": "inspect only"},
            tool_schemas=(),
            baseline_artifact_id=run.base_artifact_id,
            model_id=OPENAI_MODEL_ID,
        )
        turn_id = intent.turn_id
        with pytest.raises(AgentLoopError, match="credential_"):
            await service.infer_or_reuse(lease, turn_id, adapter)
        assert sent == []
        with store.admin_engine.connect() as connection:
            row = connection.execute(
                text("SELECT status FROM runtime_private.agent_turns WHERE turn_id=:turn"),
                {"turn": turn_id},
            ).scalar_one()
            assert row == "prepared"
            assert (
                connection.execute(
                    text("SELECT status FROM runtime_private.runs WHERE run_id=:run"),
                    {"run": run.run_id},
                ).scalar_one()
                == "waiting_credentials"
            )
        saved = credentials.save(
            CredentialProvider.OPENAI,
            SecretStr(secret),
            scope=CredentialScope.RUN,
            run_id=run.run_id,
        )
        credentials.bind_run(run.run_id, CredentialProvider.OPENAI, saved.credential_id)
        resumed = queue.claim_one("t24-after-credential")
        assert resumed is not None and resumed.job_id == lease.job_id
        if response_case == "auth_rejected":
            with pytest.raises(AgentLoopError, match="openai_authentication_failed"):
                await service.infer_or_reuse(resumed, turn_id, adapter)
            assert len(sent) == 1
            with store.admin_engine.connect() as connection:
                row = (
                    connection.execute(
                        text(
                            "SELECT status,response,response_error_code FROM "
                            "runtime_private.agent_turns WHERE turn_id=:turn"
                        ),
                        {"turn": turn_id},
                    )
                    .mappings()
                    .one()
                )
                assert row["status"] == "invalid_response"
                assert row["response_error_code"] == "openai_authentication_failed"
                assert secret not in str(row["response"])
                assert "synthetic-sensitive-provider-message" not in str(row["response"])
                assert (
                    connection.execute(
                        text(
                            "SELECT count(*) FROM runtime_private.agent_invocations "
                            "WHERE turn_id=:turn"
                        ),
                        {"turn": turn_id},
                    ).scalar_one()
                    == 0
                )
            return
        accepted = await service.infer_or_reuse(resumed, turn_id, adapter)
        assert accepted.response.model_id == OPENAI_MODEL_ID
        assert accepted.response.provider_request_id == "req_t24_recover"
        assert accepted.response.input_tokens == (None if response_case == "unknown" else 10)
        assert not accepted.adopted and len(sent) == 1
        replay = await service.infer_or_reuse(
            resumed, turn_id, OpenAIModelAdapter(client_factory=factory)
        )
        assert replay.adopted and len(sent) == 1
        assert secret not in replay.response.model_dump_json()
        with store.admin_engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.credential_dispatches "
                        "WHERE run_id=:run"
                    ),
                    {"run": run.run_id},
                ).scalar_one()
                == 1
            )

    try:
        asyncio.run(exercise())
    finally:
        asyncio.run(storage.aclose())
        # The shared T22 fixture also proves downgrade to its pre-provider schema.
        # Remove this test's incompatible rows rather than rewriting their identities.
        if turn_id is not None:
            with store.admin_engine.begin() as connection:
                connection.execute(
                    text("DELETE FROM runtime_private.agent_invocations WHERE turn_id=:turn"),
                    {"turn": turn_id},
                )
                connection.execute(
                    text("DELETE FROM runtime_private.agent_turns WHERE turn_id=:turn"),
                    {"turn": turn_id},
                )
