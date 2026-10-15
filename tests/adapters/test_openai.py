"""Synthetic protocol tests for the OpenAI Responses API adapter."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from pydantic import SecretStr

from continuum_api.adapters.openai import OpenAIAdapterError, OpenAIModelAdapter
from continuum_api.credentials import CredentialProvider
from continuum_api.runtime.agent_loop import (
    OPENAI_MODEL_ID,
    AgentLoopError,
    ScriptedModelRequest,
    ScriptedModelResponse,
    _validate_complete_response,
)
from continuum_api.runtime.provider_credentials import CredentialDispatchGrant


@dataclass
class _OutputItem:
    type: str
    call_id: str | None = None
    name: str | None = None
    arguments: str | None = None
    content: list[Any] | None = None
    text: str | None = None

    def model_dump(self, *, mode: str, exclude_none: bool) -> dict[str, Any]:
        del mode, exclude_none
        if self.type == "message":
            return {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "Ready"}],
            }
        return {
            "type": self.type,
            "call_id": self.call_id,
            "name": self.name,
            "arguments": self.arguments,
        }


class _FakeResponses:
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
        self.responses = _FakeResponses(response)
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _request(context: dict[str, Any] | None = None) -> ScriptedModelRequest:
    return ScriptedModelRequest(
        turn_id=uuid4(),
        model_id=OPENAI_MODEL_ID,
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
        request_sha256="a" * 64,
        max_output_tokens=128,
    )


def _grant(
    request: ScriptedModelRequest, *, secret: str = "sk-test-never-log"
) -> CredentialDispatchGrant:
    return CredentialDispatchGrant(
        grant_id=uuid4(),
        credential_id=uuid4(),
        provider=CredentialProvider.OPENAI,
        request_id=request.turn_id,
        secret=SecretStr(secret),
    )


def _factory_for(client: _FakeClient, received: list[dict[str, Any]]):
    def create(**kwargs: Any) -> _FakeClient:
        received.append(kwargs)
        return client

    return create


def test_responses_request_is_bounded_stateless_and_normalizes_ordered_calls() -> None:
    request = _request()
    response = SimpleNamespace(
        id="resp_123",
        model=OPENAI_MODEL_ID,
        status="completed",
        output=[
            _OutputItem(
                type="function_call",
                call_id="call_123",
                name="list_files",
                arguments='{"path":"."}',
            ),
            _OutputItem(type="message", content=[]),
        ],
        usage=SimpleNamespace(
            input_tokens=22,
            output_tokens=8,
            input_tokens_details=SimpleNamespace(cached_tokens=3),
            output_tokens_details=SimpleNamespace(reasoning_tokens=2),
        ),
        _request_id="req_123",
    )
    client = _FakeClient(response)
    factory_args: list[dict[str, Any]] = []
    adapter = OpenAIModelAdapter(client_factory=_factory_for(client, factory_args))

    normalized = _run(adapter.complete_with_grant(request, _grant(request)))

    sent = client.responses.calls[0]
    assert factory_args[0]["max_retries"] == 0
    assert factory_args[0]["timeout"] == 45.0
    assert sent["model"] == OPENAI_MODEL_ID
    assert sent["max_output_tokens"] == 128
    assert sent["store"] is False
    assert sent["parallel_tool_calls"] is False
    assert sent["tools"][0]["strict"] is True
    assert sent["input"][-1]["role"] == "user"
    assert normalized.complete is True
    assert normalized.stop_reason == "tool_calls"
    assert normalized.calls[0].provider_call_id == "call_123"
    assert normalized.input_tokens == 22
    assert normalized.output_tokens == 8
    assert normalized.cached_input_tokens == 3
    assert normalized.reasoning_output_tokens == 2
    assert normalized.response_id == "resp_123"
    assert normalized.provider_request_id == "req_123"
    assert normalized.latency_ms is not None
    assert client.closed is True


def test_unknown_usage_is_preserved_and_response_items_are_continuable() -> None:
    request = _request()
    response = SimpleNamespace(
        id="resp_456",
        model=OPENAI_MODEL_ID,
        status="completed",
        output=[_OutputItem(type="message", content=[])],
        usage=None,
        _request_id=None,
    )
    client = _FakeClient(response)
    adapter = OpenAIModelAdapter(client_factory=_factory_for(client, []))

    normalized = _run(adapter.complete_with_grant(request, _grant(request)))

    assert normalized.input_tokens is None
    assert normalized.output_tokens is None
    assert normalized.continuation == {
        "items": [response.output[0].model_dump(mode="json", exclude_none=True)]
    }


def test_continuation_items_are_sent_as_api_items_not_duplicated_in_context_text() -> None:
    prior = {"type": "function_call_output", "call_id": "call_prev", "output": "done"}
    request = _request({"objective": "continue", "openai_continuation_items": [prior]})
    response = SimpleNamespace(
        id="resp_789", model=OPENAI_MODEL_ID, status="completed", output=[], usage=None
    )
    client = _FakeClient(response)
    adapter = OpenAIModelAdapter(client_factory=_factory_for(client, []))

    _run(adapter.complete_with_grant(request, _grant(request)))

    inputs = client.responses.calls[0]["input"]
    assert inputs[0] == prior
    assert "openai_continuation_items" not in inputs[-1]["content"][0]["text"]


def test_incomplete_provider_response_cannot_become_a_tool_response() -> None:
    request = _request()
    response = SimpleNamespace(
        id="resp_abc",
        model=OPENAI_MODEL_ID,
        status="incomplete",
        incomplete_details=SimpleNamespace(reason="max_output_tokens"),
        output=[
            _OutputItem(
                type="function_call",
                call_id="call_abc",
                name="list_files",
                arguments='{"path":"."}',
            )
        ],
        usage=SimpleNamespace(input_tokens=1, output_tokens=128),
        _request_id="req_abc",
    )
    client = _FakeClient(response)
    adapter = OpenAIModelAdapter(client_factory=_factory_for(client, []))

    normalized = _run(adapter.complete_with_grant(request, _grant(request)))

    assert normalized.complete is False
    assert normalized.stop_reason == "max_output_tokens"
    assert len(normalized.calls) == 1


def test_credential_grant_is_bound_to_one_request_and_never_rendered() -> None:
    request = _request()
    client = _FakeClient(
        SimpleNamespace(
            id="resp_abc", model=OPENAI_MODEL_ID, status="completed", output=[], usage=None
        )
    )
    adapter = OpenAIModelAdapter(client_factory=_factory_for(client, []))
    grant = _grant(request)

    _run(adapter.complete_with_grant(request, grant))
    with pytest.raises(OpenAIAdapterError, match="openai_credential_grant_already_used"):
        _run(adapter.complete_with_grant(request, grant))
    assert "sk-test-never-log" not in repr(grant)
    assert "sk-test-never-log" not in repr(adapter)
    assert len(client.responses.calls) == 1


def test_provider_error_is_classified_without_echoing_remote_body() -> None:
    request = _request()
    client = _FakeClient(RuntimeError("secret body sk-test-never-log"))
    adapter = OpenAIModelAdapter(client_factory=_factory_for(client, []))

    with pytest.raises(OpenAIAdapterError, match="openai_provider_error_outcome_unknown") as error:
        _run(adapter.complete_with_grant(request, _grant(request)))

    assert "sk-test-never-log" not in str(error.value)
    assert client.closed is True


def test_unknown_usage_is_accepted_without_being_counted_as_zero() -> None:
    request = _request()
    response = ScriptedModelResponse(
        complete=True,
        stop_reason="stop",
        model_id=OPENAI_MODEL_ID,
        response_id="resp_unknown",
        output_text="Done",
        calls=(),
        input_tokens=None,
        output_tokens=None,
    )

    assert _validate_complete_response(response, OPENAI_MODEL_ID, request.turn_id, 128) == []
    assert response.input_tokens is None and response.output_tokens is None


def test_non_strict_nested_function_schema_is_rejected_before_dispatch() -> None:
    request = _request()
    nested_schema = {
        "type": "function",
        "name": "list_files",
        "parameters": {
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {"hidden": {"type": "boolean"}},
                    "required": ["hidden"],
                }
            },
            "required": ["filter"],
            "additionalProperties": False,
        },
    }
    request = request.model_copy(update={"tool_schemas": (nested_schema,)})
    client = _FakeClient(
        SimpleNamespace(
            id="resp_nested", model=OPENAI_MODEL_ID, status="completed", output=[], usage=None
        )
    )
    adapter = OpenAIModelAdapter(client_factory=_factory_for(client, []))

    with pytest.raises(OpenAIAdapterError, match="openai_tool_schema_not_strict"):
        adapter.validate_request(request)

    assert client.responses.calls == []


def test_refusal_text_is_preserved_and_refusal_cannot_dispatch_tools() -> None:
    request = _request()

    class _Refusal:
        type = "refusal"
        refusal = "I cannot help with that request."

    class _RefusalMessage:
        type = "message"
        content = [_Refusal()]

        def model_dump(self, *, mode: str, exclude_none: bool) -> dict[str, Any]:
            del mode, exclude_none
            return {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "refusal", "refusal": self.content[0].refusal}],
            }

    response = SimpleNamespace(
        id="resp_refusal",
        model=OPENAI_MODEL_ID,
        status="completed",
        output=[_RefusalMessage()],
        usage=None,
        _request_id="req_refusal",
    )
    client = _FakeClient(response)
    adapter = OpenAIModelAdapter(client_factory=_factory_for(client, []))

    normalized = _run(adapter.complete_with_grant(request, _grant(request)))

    assert normalized.output_text == "I cannot help with that request."
    assert normalized.stop_reason == "refusal"
    assert normalized.calls == ()
    with pytest.raises(AgentLoopError, match="agent_response_refusal"):
        _validate_complete_response(normalized, OPENAI_MODEL_ID, request.turn_id, 128)


def _run(awaitable: Any) -> Any:
    import asyncio

    return asyncio.run(awaitable)
