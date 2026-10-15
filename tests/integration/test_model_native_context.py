"""Two-turn native provider continuation through the durable T26 tool path."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from anthropic.types import TextBlock, ThinkingBlock, ToolUseBlock
from pydantic import SecretStr
from sqlalchemy import text
from test_agent_loop import (
    T22Database,
    _checkpoint_service,
    _seed_agent_run,
    _storage_fixture,
)
from test_agent_loop import t22_database as t22_database

from continuum_api.adapters.anthropic import AnthropicModelAdapter
from continuum_api.adapters.openai import OpenAIModelAdapter
from continuum_api.credentials import CredentialCipher, CredentialProvider, CredentialScope
from continuum_api.runtime.agent_loop import (
    ANTHROPIC_MODEL_ID,
    OPENAI_MODEL_ID,
    AgentLoopError,
    DurableAgentLoopService,
)
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.persistence import ReplayPolicy, RunStatus, RuntimeRepository
from continuum_api.runtime.provider_credentials import CredentialService
from continuum_api.runtime.transitions import NextWork, TransitionService
from continuum_api.runtime.workspace_recovery import WorkspaceRecoveryService
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import ToolPolicyContext

_SECRET = "synthetic-native-context-key"
_TASK_CONTEXT = {"task": "Read src/app.py and report its answer function."}
_TOOL_SCHEMAS = (
    {
        "name": "read_file",
        "description": "Read a bounded source file.",
        "parameters": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": False,
        },
    },
)


class _SdkValue:
    def __init__(self, **values: Any) -> None:
        self._values = values
        for key, value in values.items():
            setattr(self, key, value)

    def model_dump(self, *, mode: str = "python", exclude_none: bool = False) -> dict[str, Any]:
        del mode
        if exclude_none:
            return {
                key: _model_dump_value(value)
                for key, value in self._values.items()
                if value is not None
            }
        return {key: _model_dump_value(value) for key, value in self._values.items()}


def _model_dump_value(value: Any) -> Any:
    if isinstance(value, _SdkValue):
        return value.model_dump(mode="json", exclude_none=True)
    if isinstance(value, list):
        return [_model_dump_value(item) for item in value]
    if isinstance(value, tuple):
        return [_model_dump_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _model_dump_value(item) for key, item in value.items()}
    return value


class _ProviderClient:
    def __init__(self, responses: list[Any], sent: list[dict[str, Any]]) -> None:
        self._responses = responses
        self._sent = sent
        self.responses = self
        self.messages = self

    async def create(self, **kwargs: Any) -> Any:
        self._sent.append(kwargs)
        return self._responses[len(self._sent) - 1]

    async def close(self) -> None:
        return None


def _provider_responses(provider: CredentialProvider) -> list[Any]:
    if provider is CredentialProvider.OPENAI:
        first = [
            _SdkValue(type="reasoning", encrypted_content="opaque-encrypted-reasoning"),
            _SdkValue(
                type="message",
                role="assistant",
                content=[_SdkValue(type="output_text", text="I will inspect the file.")],
            ),
            _SdkValue(
                type="function_call",
                call_id="call_read_source",
                name="read_file",
                arguments='{"path":"src/app.py"}',
            ),
        ]
        second = [
            _SdkValue(
                type="message",
                role="assistant",
                content=[_SdkValue(type="output_text", text="The function returns base.")],
            )
        ]
        return [
            SimpleNamespace(
                id="resp_native_1",
                model=OPENAI_MODEL_ID,
                status="completed",
                output=first,
                usage=SimpleNamespace(input_tokens=20, output_tokens=5),
                _request_id="req_native_1",
            ),
            SimpleNamespace(
                id="resp_native_2",
                model=OPENAI_MODEL_ID,
                status="completed",
                output=second,
                usage=SimpleNamespace(input_tokens=30, output_tokens=6),
                _request_id="req_native_2",
            ),
        ]

    return [
        SimpleNamespace(
            id="msg_native_1",
            model=ANTHROPIC_MODEL_ID,
            stop_reason="tool_use",
            content=[
                ThinkingBlock(
                    type="thinking", thinking="opaque signed thought", signature="sig_native_1"
                ),
                TextBlock(type="text", text="I will inspect the file."),
                ToolUseBlock(
                    type="tool_use",
                    id="toolu_read_source",
                    name="read_file",
                    input={"path": "src/app.py"},
                ),
            ],
            usage=SimpleNamespace(
                input_tokens=20,
                output_tokens=5,
                cache_read_input_tokens=0,
                cache_creation_input_tokens=0,
            ),
            _request_id="req_native_1",
        ),
        SimpleNamespace(
            id="msg_native_2",
            model=ANTHROPIC_MODEL_ID,
            stop_reason="end_turn",
            content=[TextBlock(type="text", text="The function returns base.")],
            usage=SimpleNamespace(
                input_tokens=30,
                output_tokens=6,
                cache_read_input_tokens=0,
                cache_creation_input_tokens=0,
            ),
            _request_id="req_native_2",
        ),
    ]


def _policy(run: Any) -> ToolPolicyContext:
    return ToolPolicyContext(
        capabilities=frozenset(ToolCapability),
        profile_id="python-pytest",
        profile_version="1",
        actor_can_write_repository=True,
        app_can_access_repository=True,
        stage=ToolStage.AGENT_WORK,
        run_budget_available=True,
        baseline_tree_sha256=run.baseline_tree_sha256,
        baseline_source_paths=frozenset(run.baseline_source_paths),
        baseline_test_paths=frozenset(run.baseline_test_paths),
        baseline_paths_verified=True,
        protected_paths=frozenset({"pyproject.toml", "tests/test_app.py"}),
    )


def _native_prior_and_assertions(
    provider: CredentialProvider, sent: list[dict[str, Any]], accepted_result: dict[str, Any]
) -> None:
    if provider is CredentialProvider.OPENAI:
        items = sent[1]["input"]
        prior = items[:-1]
        assert [item["type"] for item in prior] == [
            "message",
            "reasoning",
            "message",
            "function_call",
            "function_call_output",
        ]
        assert prior[0]["role"] == "user"
        assert prior[1]["encrypted_content"] == "opaque-encrypted-reasoning"
        assert prior[3]["call_id"] == "call_read_source"
        output = prior[4]
        assert output["call_id"] == "call_read_source"
        assert json.loads(output["output"]) == accepted_result
        assert "openai_continuation_items" not in items[-1]["content"][0]["text"]
        assert (
            sum(
                "src/app.py" in item.get("content", [{}])[0].get("text", "")
                for item in items
                if item.get("role") == "user"
            )
            == 2
        )
        return

    messages = sent[1]["messages"]
    assert [message["role"] for message in messages] == ["user", "assistant", "user", "user"]
    assistant = messages[1]
    assert assistant["content"][0]["signature"] == "sig_native_1"
    assert assistant["content"][2]["id"] == "toolu_read_source"
    tool_result = messages[2]["content"][0]
    assert tool_result["tool_use_id"] == "toolu_read_source"
    assert json.loads(tool_result["content"]) == accepted_result
    assert "anthropic_continuation_messages" not in messages[-1]["content"]
    assert (
        sum(
            "src/app.py" in message["content"]
            for message in messages
            if isinstance(message["content"], str)
        )
        == 2
    )


@pytest.mark.parametrize("provider", [CredentialProvider.OPENAI, CredentialProvider.ANTHROPIC])
def test_two_turn_native_context_is_correlated_and_digest_checked(
    t22_database: T22Database, tmp_path: Path, provider: CredentialProvider
) -> None:
    store = t22_database
    objects = tmp_path / "objects"
    run = _seed_agent_run(store, objects)
    queue = QueueService(store.runtime_engine, run.principal)
    persistence = ArtifactPersistence(store.runtime_engine)
    storage, _storage_requests = _storage_fixture(objects)
    checkpoints = _checkpoint_service(store.runtime_engine, run.principal, persistence, storage)
    transitions = TransitionService(
        store.runtime_engine, run.principal, artifact_persistence=persistence
    )
    workspace_recovery = WorkspaceRecoveryService(
        store.runtime_engine, run.principal, checkpoints, transitions
    )
    credentials = CredentialService(
        store.runtime_engine,
        run.principal,
        CredentialCipher("v1", {"v1": os.urandom(32)}),
    )
    credential = credentials.save(
        provider,
        SecretStr(_SECRET),
        scope=CredentialScope.RUN,
        run_id=run.run_id,
    )
    credentials.bind_run(run.run_id, provider, credential.credential_id)
    service = DurableAgentLoopService(
        store.runtime_engine,
        run.principal,
        transitions,
        workspace_recovery=workspace_recovery,
        checkpoints=checkpoints,
        credential_service=credentials,
    )
    sent: list[dict[str, Any]] = []
    responses = _provider_responses(provider)

    def client_factory(**kwargs: Any) -> _ProviderClient:
        assert kwargs["api_key"] == _SECRET
        assert kwargs["max_retries"] == 0
        return _ProviderClient(responses, sent)

    adapter = (
        OpenAIModelAdapter(client_factory=client_factory)
        if provider is CredentialProvider.OPENAI
        else AnthropicModelAdapter(client_factory=client_factory)
    )
    model_id = OPENAI_MODEL_ID if provider is CredentialProvider.OPENAI else ANTHROPIC_MODEL_ID

    async def exercise() -> None:
        first_lease = queue.claim_one("t26-native-first")
        assert first_lease is not None and first_lease.job_id == run.turn_job_id
        first_intent = await service.prepare_turn(
            first_lease,
            "agent-turn:primary",
            context=_TASK_CONTEXT,
            tool_schemas=_TOOL_SCHEMAS,
            baseline_artifact_id=run.base_artifact_id,
            model_id=model_id,
        )
        first_receipt = await service.infer_or_reuse(first_lease, first_intent.turn_id, adapter)
        assert first_receipt.response.calls[0].provider_call_id == (
            "call_read_source" if provider is CredentialProvider.OPENAI else "toolu_read_source"
        )
        transition = await service.complete_turn(first_lease, first_intent.turn_id)
        assert transition.next_job_id is not None

        tool_lease = queue.claim_one("t26-native-tool")
        assert tool_lease is not None and tool_lease.job_id == transition.next_job_id
        invocation = (await service.pending_invocations(tool_lease, first_intent.turn_id))[0]
        tool_receipt = await service.execute_tool_invocation(
            tool_lease,
            first_intent.turn_id,
            invocation.invocation_id,
            policy_context=_policy(run),
        )
        assert tool_receipt.transition.result["tool_result"]["path"] == "src/app.py"
        accepted_result = tool_receipt.transition.result["tool_result"]

        second_lease = queue.claim_one("t26-native-second")
        assert (
            second_lease is not None and second_lease.job_id == tool_receipt.transition.next_job_id
        )
        second_key = f"agent-turn:{first_intent.turn_id.hex}:next"
        second_intent = await service.prepare_turn(
            second_lease,
            second_key,
            context=_TASK_CONTEXT,
            tool_schemas=_TOOL_SCHEMAS,
            baseline_artifact_id=run.base_artifact_id,
            model_id=model_id,
        )
        second_receipt = await service.infer_or_reuse(second_lease, second_intent.turn_id, adapter)
        assert second_receipt.response.output_text == "The function returns base."
        _native_prior_and_assertions(provider, sent, accepted_result)

        with store.runtime_engine.begin() as connection:
            RuntimeRepository(connection, run.principal)
            revision = int(
                connection.execute(
                    text(
                        "SELECT revision FROM runtime_private.steps "
                        "WHERE owner_id=:owner AND run_id=:run AND step_id=:step"
                    ),
                    {"owner": run.owner_id, "run": run.run_id, "step": second_lease.step_id},
                ).scalar_one()
            )
        second_transition = await service.complete_turn(
            second_lease,
            second_intent.turn_id,
            next_work=NextWork(
                "t26-native-after-corruption-check",
                "agent_turn",
                revision=revision,
                replay_policy=ReplayPolicy.REPLAY_SAFE,
            ),
            target_run_status=RunStatus.RUNNING,
        )
        assert second_transition.next_job_id is not None

        # Simulate a payload mutation with its transition digest left untouched.
        with store.admin_engine.begin() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT result_id, payload FROM runtime_private.accepted_results "
                        "WHERE owner_id=:owner AND run_id=:run "
                        "AND payload->>'agent_invocation_id'=:invocation"
                    ),
                    {
                        "owner": run.owner_id,
                        "run": run.run_id,
                        "invocation": str(invocation.invocation_id),
                    },
                )
                .mappings()
                .one()
            )
            tampered = dict(row["payload"])
            tampered["tool_result"] = dict(tampered["tool_result"])
            tampered["tool_result"]["content"] += " tampered"
            updated = connection.execute(
                text(
                    "UPDATE runtime_private.accepted_results SET payload=CAST(:payload AS jsonb) "
                    "WHERE result_id=:result_id"
                ),
                {
                    "payload": json.dumps(
                        tampered, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                    ),
                    "result_id": row["result_id"],
                },
            )
            assert updated.rowcount == 1

        third_lease = queue.claim_one("t26-native-tampered-history")
        assert third_lease is not None and third_lease.job_id == second_transition.next_job_id
        with pytest.raises(AgentLoopError, match="agent_invocation_result_corrupt"):
            await service.prepare_turn(
                third_lease,
                "t26-native-after-corruption-check",
                context=_TASK_CONTEXT,
                tool_schemas=_TOOL_SCHEMAS,
                baseline_artifact_id=run.base_artifact_id,
                model_id=model_id,
            )
        with store.admin_engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": run.owner_id, "run": run.run_id},
                ).scalar_one()
                == 2
            )
        assert len(sent) == 2

    try:
        asyncio.run(exercise())
    finally:
        try:
            asyncio.run(storage.aclose())
        finally:
            with store.admin_engine.begin() as connection:
                connection.execute(
                    text(
                        "DELETE FROM runtime_private.agent_invocations "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": run.owner_id, "run": run.run_id},
                )
                connection.execute(
                    text(
                        "DELETE FROM runtime_private.agent_turns "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": run.owner_id, "run": run.run_id},
                )
