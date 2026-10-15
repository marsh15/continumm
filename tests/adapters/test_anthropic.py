"""Synthetic protocol tests for the Anthropic Messages API adapter."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from pydantic import SecretStr

from continuum_api.adapters.anthropic import AnthropicAdapterError, AnthropicModelAdapter
from continuum_api.credentials import CredentialProvider
from continuum_api.runtime.agent_loop import (
    ANTHROPIC_MODEL_ID,
    AgentLoopError,
    ScriptedModelRequest,
    _validate_complete_response,
)
from continuum_api.runtime.provider_credentials import CredentialDispatchGrant


@dataclass
class _Block:
    type: str
    id: str | None = None
    name: str | None = None
    input: dict[str, Any] | None = None
    text: str | None = None
    thinking: str | None = None
    signature: str | None = None
    data: str | None = None

    def model_dump(self, *, mode: str, exclude_none: bool) -> dict[str, Any]:
        del mode, exclude_none
        fields = {
            "type": self.type,
            "id": self.id,
            "name": self.name,
            "input": self.input,
            "text": self.text,
            "thinking": self.thinking,
            "signature": self.signature,
            "data": self.data,
        }
        return {key: value for key, value in fields.items() if value is not None}


class _FakeMessages:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    async def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class _FakeClient:
    def __init__(self, response: Any) -> None:
        self.messages = _FakeMessages(response)
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _request(context: dict[str, Any] | None = None) -> ScriptedModelRequest:
    return ScriptedModelRequest(
        turn_id=uuid4(),
        model_id=ANTHROPIC_MODEL_ID,
        context=context or {"objective": "inspect"},
        tool_schemas=(
            {
                "type": "function",
                "name": "list_files",
                "description": "List workspace files",
                "parameters": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                    "additionalProperties": False,
                },
            },
        ),
        request_sha256="b" * 64,
        max_output_tokens=128,
    )


def _grant(
    request: ScriptedModelRequest, *, secret: str = "sk-ant-test-never-log"
) -> CredentialDispatchGrant:
    return CredentialDispatchGrant(
        grant_id=uuid4(),
        credential_id=uuid4(),
        provider=CredentialProvider.ANTHROPIC,
        request_id=request.turn_id,
        secret=SecretStr(secret),
    )


def _factory_for(client: _FakeClient, received: list[dict[str, Any]]):
    def create(**kwargs: Any) -> _FakeClient:
        received.append(kwargs)
        return client

    return create


def _response(
    *,
    content: list[Any] | None = None,
    stop_reason: str | None = "end_turn",
    usage: Any = None,
    stop_details: Any = None,
) -> Any:
    if usage is None:
        usage = SimpleNamespace(
            input_tokens=22,
            cache_read_input_tokens=3,
            cache_creation_input_tokens=4,
            output_tokens=8,
            output_tokens_details=SimpleNamespace(thinking_tokens=2),
        )
    return SimpleNamespace(
        id="msg_123",
        model=ANTHROPIC_MODEL_ID,
        content=content or [_Block(type="text", text="Ready")],
        stop_reason=stop_reason,
        stop_details=stop_details,
        usage=usage,
        _request_id="req_123",
    )


def test_messages_request_is_bounded_sequential_and_normalizes_usage_and_tool_ids() -> None:
    request = _request()
    response = _response(
        content=[
            _Block(type="tool_use", id="toolu_123", name="list_files", input={"path": "."}),
            _Block(type="text", text="Done"),
        ],
        stop_reason="tool_use",
    )
    client = _FakeClient(response)
    factory_args: list[dict[str, Any]] = []
    adapter = AnthropicModelAdapter(client_factory=_factory_for(client, factory_args))

    normalized = _run(adapter.complete_with_grant(request, _grant(request)))

    sent = client.messages.calls[0]
    assert factory_args[0]["max_retries"] == 0
    assert factory_args[0]["timeout"] == 45.0
    assert sent["model"] == ANTHROPIC_MODEL_ID
    assert sent["max_tokens"] == 128
    assert sent["tool_choice"] == {"type": "auto", "disable_parallel_tool_use": True}
    assert sent["tools"][0]["strict"] is True
    assert sent["tools"][0]["input_schema"] == request.tool_schemas[0]["parameters"]
    assert sent["messages"][-1]["role"] == "user"
    assert normalized.complete is True
    assert normalized.stop_reason == "tool_calls"
    assert normalized.calls[0].provider_call_id == "toolu_123"
    assert normalized.calls[0].arguments_json == '{"path":"."}'
    assert normalized.input_tokens == 29
    assert normalized.cached_input_tokens == 3
    assert normalized.cache_creation_input_tokens == 4
    assert normalized.output_tokens == 8
    assert normalized.reasoning_output_tokens == 2
    assert normalized.response_id == "msg_123"
    assert normalized.provider_request_id == "req_123"
    assert normalized.latency_ms is not None
    assert normalized.continuation == {
        "assistant_message": {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "toolu_123",
                    "name": "list_files",
                    "input": {"path": "."},
                },
                {"type": "text", "text": "Done"},
            ],
        },
        "message_id": "msg_123",
    }
    assert client.closed is True


def test_continuation_preserves_thinking_signature_and_tool_result_order() -> None:
    assistant = {
        "role": "assistant",
        "content": [
            {"type": "thinking", "thinking": "private thought", "signature": "sig_keep_exact"},
            {"type": "redacted_thinking", "data": "opaque_payload"},
            {"type": "tool_use", "id": "toolu_prev", "name": "list_files", "input": {"path": "."}},
        ],
    }
    tool_result = {
        "role": "user",
        "content": [{"type": "tool_result", "tool_use_id": "toolu_prev", "content": "done"}],
    }
    request = _request(
        {"objective": "continue", "anthropic_continuation_messages": [assistant, tool_result]}
    )
    client = _FakeClient(_response(content=[], usage=None))
    adapter = AnthropicModelAdapter(client_factory=_factory_for(client, []))

    _run(adapter.complete_with_grant(request, _grant(request)))

    messages = client.messages.calls[0]["messages"]
    assert messages[:2] == [assistant, tool_result]
    assert "anthropic_continuation_messages" not in messages[-1]["content"]


@pytest.mark.parametrize(
    ("stop_reason", "complete", "normalized_reason"),
    [
        ("end_turn", True, "stop"),
        ("max_tokens", False, "max_tokens"),
        ("stop_sequence", False, "stop_sequence"),
        ("pause_turn", False, "pause_turn"),
        ("compaction", False, "compaction"),
        ("model_context_window_exceeded", False, "model_context_window_exceeded"),
    ],
)
def test_stop_reason_normalization_requires_complete_non_tool_outcomes(
    stop_reason: str, complete: bool, normalized_reason: str
) -> None:
    request = _request()
    client = _FakeClient(_response(stop_reason=stop_reason))
    adapter = AnthropicModelAdapter(client_factory=_factory_for(client, []))

    normalized = _run(adapter.complete_with_grant(request, _grant(request)))

    assert normalized.complete is complete
    assert normalized.stop_reason == normalized_reason
    if not complete:
        with pytest.raises(AgentLoopError, match="agent_response_incomplete"):
            _validate_complete_response(normalized, ANTHROPIC_MODEL_ID, request.turn_id, 128)


def test_refusal_is_distinct_and_does_not_create_tool_calls() -> None:
    request = _request()
    client = _FakeClient(
        _response(
            content=[_Block(type="text", text="I cannot help with that request.")],
            stop_reason="end_turn",
            stop_details=SimpleNamespace(type="refusal"),
        )
    )
    adapter = AnthropicModelAdapter(client_factory=_factory_for(client, []))

    normalized = _run(adapter.complete_with_grant(request, _grant(request)))

    assert normalized.output_text == "I cannot help with that request."
    assert normalized.stop_reason == "refusal"
    assert normalized.calls == ()
    with pytest.raises(AgentLoopError, match="agent_response_refusal"):
        _validate_complete_response(normalized, ANTHROPIC_MODEL_ID, request.turn_id, 128)


def test_refusal_stop_reason_without_stop_details_is_still_classified() -> None:
    request = _request()
    client = _FakeClient(
        _response(
            content=[_Block(type="text", text="I cannot help with that request.")],
            stop_reason="refusal",
            stop_details=None,
        )
    )
    adapter = AnthropicModelAdapter(client_factory=_factory_for(client, []))

    normalized = _run(adapter.complete_with_grant(request, _grant(request)))

    assert normalized.complete is True
    assert normalized.stop_reason == "refusal"
    with pytest.raises(AgentLoopError, match="agent_response_refusal"):
        _validate_complete_response(normalized, ANTHROPIC_MODEL_ID, request.turn_id, 128)


def test_unknown_usage_stays_unknown() -> None:
    request = _request()
    client = _FakeClient(_response(usage=SimpleNamespace(input_tokens=None, output_tokens=None)))
    adapter = AnthropicModelAdapter(client_factory=_factory_for(client, []))

    normalized = _run(adapter.complete_with_grant(request, _grant(request)))

    assert normalized.input_tokens is None
    assert normalized.output_tokens is None


@pytest.mark.parametrize(
    ("cache_read", "cache_creation"),
    [(None, 4), (3, None)],
)
def test_unknown_cache_category_keeps_total_usage_unknown(
    cache_read: int | None, cache_creation: int | None
) -> None:
    request = _request()
    usage = SimpleNamespace(
        input_tokens=22,
        cache_read_input_tokens=cache_read,
        cache_creation_input_tokens=cache_creation,
        output_tokens=8,
        output_tokens_details=SimpleNamespace(thinking_tokens=2),
    )
    client = _FakeClient(_response(usage=usage))
    adapter = AnthropicModelAdapter(client_factory=_factory_for(client, []))

    normalized = _run(adapter.complete_with_grant(request, _grant(request)))

    assert normalized.input_tokens is None
    assert normalized.output_tokens is None
    assert normalized.cached_input_tokens is None
    assert normalized.cache_creation_input_tokens is None


def test_grant_is_request_bound_one_use_and_redacted() -> None:
    request = _request()
    client = _FakeClient(_response())
    adapter = AnthropicModelAdapter(client_factory=_factory_for(client, []))
    grant = _grant(request)

    _run(adapter.complete_with_grant(request, grant))
    with pytest.raises(AnthropicAdapterError, match="anthropic_credential_grant_already_used"):
        _run(adapter.complete_with_grant(request, grant))
    assert "sk-ant-test-never-log" not in repr(grant)
    assert "sk-ant-test-never-log" not in repr(adapter)
    assert len(client.messages.calls) == 1


def test_invalid_schema_fails_before_provider_dispatch() -> None:
    request = _request()
    invalid = request.model_copy(
        update={
            "tool_schemas": (
                {
                    "type": "function",
                    "name": "list_files",
                    "parameters": {
                        "type": "object",
                        "properties": {"path": {"type": "string"}},
                        "required": [],
                        "additionalProperties": False,
                    },
                },
            )
        }
    )
    client = _FakeClient(_response())
    adapter = AnthropicModelAdapter(client_factory=_factory_for(client, []))

    with pytest.raises(AnthropicAdapterError, match="anthropic_tool_schema_not_strict"):
        adapter.validate_request(invalid)

    assert client.messages.calls == []


def test_provider_error_does_not_echo_remote_body() -> None:
    request = _request()
    client = _FakeClient(RuntimeError("sk-ant-test-never-log response body"))
    adapter = AnthropicModelAdapter(client_factory=_factory_for(client, []))

    with pytest.raises(
        AnthropicAdapterError, match="anthropic_provider_error_outcome_unknown"
    ) as error:
        _run(adapter.complete_with_grant(request, _grant(request)))

    assert "sk-ant-test-never-log" not in str(error.value)
    assert client.closed is True


def _run(awaitable: Any) -> Any:
    import asyncio

    return asyncio.run(awaitable)
