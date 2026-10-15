from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr

from continuum_api.adapters.openai import OpenAIModelAdapter
from continuum_api.runtime.agent_context import (
    AgentContextError,
    build_agent_context,
    canonicalize_initial_context,
    context_source_sha256,
)
from continuum_api.runtime.agent_loop import (
    ANTHROPIC_MODEL_ID,
    OPENAI_MODEL_ID,
    ScriptedModelRequest,
)

_BUDGET = {
    "requests_remaining": 10,
    "input_tokens_remaining": 20_000,
    "output_tokens_remaining": 4_000,
    "usage_uncertain": False,
}
_INITIAL = {"task": "inspect source", "constraints": ["no tests edits"]}
_SCRIPTED_MODEL_ID = "scripted-v1"


def _turn(
    model: str,
    context: dict[str, Any],
    *,
    call_id: str = "call-1",
    result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    call = {"provider_call_id": call_id, "name": "list_files"}
    continuation: dict[str, Any] | None
    if model == OPENAI_MODEL_ID:
        continuation = {
            "items": [
                {"type": "reasoning", "encrypted_content": "ciphertext"},
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "Inspecting files."}],
                },
                {
                    "type": "function_call",
                    "call_id": call_id,
                    "name": "list_files",
                    "arguments": "{}",
                },
            ]
        }
    elif model == ANTHROPIC_MODEL_ID:
        continuation = {
            "assistant_message": {
                "role": "assistant",
                "content": [
                    {"type": "thinking", "thinking": "private", "signature": "signed"},
                    {"type": "text", "text": "Inspecting files."},
                    {"type": "tool_use", "id": call_id, "name": "list_files", "input": {}},
                ],
            },
            "message_id": "message-1",
        }
    else:
        continuation = None
    payload = {
        "agent_turn_id": "turn-1",
        "agent_invocation_id": "invocation-1",
        "provider_call_id": call_id,
        "ordinal": 0,
        "tool_name": "list_files",
        "tool_result": result or {"paths": ["src/main.py"]},
    }
    return {
        "model_id": model,
        "context": context,
        "response": {
            "complete": True,
            "stop_reason": "tool_calls",
            "model_id": model,
            "output_text": "Inspecting files.",
            "calls": [call],
            "continuation": continuation,
        },
        "invocations": [
            {
                "provider_call_id": call_id,
                "tool_name": "list_files",
                "arguments": {},
                "accepted_payload": payload,
            }
        ],
    }


@pytest.mark.parametrize("model", [OPENAI_MODEL_ID, ANTHROPIC_MODEL_ID])
def test_provider_history_is_correlated_and_appended_once(model: str) -> None:
    first_request = build_agent_context(
        initial_context=_INITIAL,
        accepted_turns=[],
        model_id=model,
        budget=_BUDGET,
    )
    accepted = _turn(model, first_request)
    second_request = build_agent_context(
        initial_context=_INITIAL,
        accepted_turns=[accepted],
        model_id=model,
        budget=_BUDGET,
    )
    second_turn = _turn(model, second_request, call_id="call-2", result={"paths": ["README.md"]})
    next_request = build_agent_context(
        initial_context=_INITIAL,
        accepted_turns=[accepted, second_turn],
        model_id=model,
        budget=_BUDGET,
    )

    history_key = (
        "openai_continuation_items"
        if model == OPENAI_MODEL_ID
        else "anthropic_continuation_messages"
    )
    history = next_request[history_key]
    assert len(history) == (10 if model == OPENAI_MODEL_ID else 6)
    assert "Inspecting files." in str(history)
    assert "ciphertext" in str(history) or "signed" in str(history)
    assert [entry["tools"][0]["result"] for entry in next_request["recent_results"]] == [
        {"paths": ["src/main.py"]},
        {"paths": ["README.md"]},
    ]

    # The prior request appears once in history and is not copied into the new
    # current-user context; the adapter will append that current context once.
    assert str(history).count("inspect source") == 2
    assert history_key in next_request
    if model == OPENAI_MODEL_ID:
        OpenAIModelAdapter().validate_request(
            ScriptedModelRequest(
                turn_id=uuid4(),
                model_id=model,
                context=next_request,
                tool_schemas=(),
                request_sha256="a" * 64,
            )
        )


@pytest.mark.parametrize("model", [OPENAI_MODEL_ID, ANTHROPIC_MODEL_ID])
def test_older_visible_results_use_deterministic_structured_summaries(model: str) -> None:
    accepted_turns: list[dict[str, Any]] = []
    context = build_agent_context(
        initial_context=_INITIAL,
        accepted_turns=accepted_turns,
        model_id=model,
        budget=_BUDGET,
    )
    for index in range(4):
        text = f"completed step {index + 1}"
        continuation: dict[str, Any]
        if model == OPENAI_MODEL_ID:
            continuation = {
                "items": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": text}],
                    }
                ]
            }
        else:
            continuation = {
                "assistant_message": {
                    "role": "assistant",
                    "content": [{"type": "text", "text": text}],
                },
                "message_id": f"message-{index + 1}",
            }
        accepted_turns.append(
            {
                "model_id": model,
                "context": context,
                "response": {
                    "complete": True,
                    "stop_reason": "stop",
                    "model_id": model,
                    "output_text": text,
                    "calls": [],
                    "continuation": continuation,
                },
                "invocations": [],
            }
        )
        context = build_agent_context(
            initial_context=_INITIAL,
            accepted_turns=accepted_turns,
            model_id=model,
            budget=_BUDGET,
        )
    assert [row["turn"] for row in context["recent_results"]] == [2, 3, 4]
    summary = context["older_result_summaries"]
    assert summary[0]["turn"] == 1
    assert summary[0]["response_excerpt"] == "completed step 1"
    assert len(summary[0]["response_sha256"]) == 64
    assert summary[0]["tools_excerpt"] == "[]"
    assert len(summary[0]["tools_sha256"]) == 64
    assert context == build_agent_context(
        initial_context=_INITIAL,
        accepted_turns=accepted_turns,
        model_id=model,
        budget=_BUDGET,
    )


def test_initial_context_rejects_caller_provenance_and_credentials() -> None:
    for context in (
        {**_INITIAL, "accepted_results": []},
        {**_INITIAL, "history": []},
        {**_INITIAL, "proof": "trusted"},
        {**_INITIAL, "credential": "value"},
        {"task": SecretStr("secret")},
        {"task": "inspect", "policy": {"access_token": "secret"}},
    ):
        with pytest.raises(AgentContextError):
            build_agent_context(
                initial_context=context,
                accepted_turns=[],
                model_id=OPENAI_MODEL_ID,
                budget=_BUDGET,
            )


def test_scripted_model_uses_visible_results_without_native_history() -> None:
    first_request = build_agent_context(
        initial_context=_INITIAL,
        accepted_turns=[],
        model_id=_SCRIPTED_MODEL_ID,
        budget=_BUDGET,
    )
    accepted = _turn(_SCRIPTED_MODEL_ID, first_request)
    next_request = build_agent_context(
        initial_context=_INITIAL,
        accepted_turns=[accepted],
        model_id=_SCRIPTED_MODEL_ID,
        budget=_BUDGET,
    )
    assert "openai_continuation_items" not in next_request
    assert "anthropic_continuation_messages" not in next_request
    assert next_request["recent_results"][0]["response"] == "Inspecting files."
    assert next_request["recent_results"][0]["tools"][0]["result"] == {"paths": ["src/main.py"]}


def test_missing_or_unaccepted_tool_result_fails_closed() -> None:
    prior_context = build_agent_context(
        initial_context=_INITIAL,
        accepted_turns=[],
        model_id=OPENAI_MODEL_ID,
        budget=_BUDGET,
    )
    accepted = _turn(OPENAI_MODEL_ID, prior_context)
    accepted["invocations"][0]["accepted_payload"] = None
    with pytest.raises(AgentContextError) as error:
        build_agent_context(
            initial_context=_INITIAL,
            accepted_turns=[accepted],
            model_id=OPENAI_MODEL_ID,
            budget=_BUDGET,
        )
    assert error.value.code == "agent_context_tool_result_missing"


def test_history_must_match_prior_context_and_model() -> None:
    prior_context = build_agent_context(
        initial_context=_INITIAL,
        accepted_turns=[],
        model_id=OPENAI_MODEL_ID,
        budget=_BUDGET,
    )
    for tampered_context in (
        {
            **prior_context,
            "openai_continuation_items": [{"role": "user", "content": "unexpected"}],
        },
        {**prior_context, "recent_results": [{"turn": 4, "response": "untrusted"}]},
    ):
        accepted = _turn(OPENAI_MODEL_ID, tampered_context)
        with pytest.raises(AgentContextError) as error:
            build_agent_context(
                initial_context=_INITIAL,
                accepted_turns=[accepted],
                model_id=OPENAI_MODEL_ID,
                budget=_BUDGET,
            )
        assert error.value.code == "agent_context_history_invalid"


def test_native_continuation_is_never_silently_truncated() -> None:
    prior_context = build_agent_context(
        initial_context=_INITIAL,
        accepted_turns=[],
        model_id=OPENAI_MODEL_ID,
        budget=_BUDGET,
    )
    accepted = _turn(OPENAI_MODEL_ID, prior_context)
    accepted["response"]["continuation"]["items"][0]["encrypted_content"] = "x" * 8_000
    with pytest.raises(AgentContextError) as error:
        build_agent_context(
            initial_context=_INITIAL,
            accepted_turns=[accepted],
            model_id=OPENAI_MODEL_ID,
            budget=_BUDGET,
            context_limit_bytes=1_024,
        )
    assert error.value.code == "agent_context_size_limit"


def test_malformed_native_call_correlation_is_rejected() -> None:
    prior_context = build_agent_context(
        initial_context=_INITIAL,
        accepted_turns=[],
        model_id=OPENAI_MODEL_ID,
        budget=_BUDGET,
    )
    accepted = _turn(OPENAI_MODEL_ID, prior_context)
    accepted["invocations"][0]["provider_call_id"] = "different-call"
    with pytest.raises(AgentContextError):
        build_agent_context(
            initial_context=_INITIAL,
            accepted_turns=[accepted],
            model_id=OPENAI_MODEL_ID,
            budget=_BUDGET,
        )


def test_pinned_context_canonicalization_and_source_digest_are_stable() -> None:
    turn_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    invocation_id = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
    assert canonicalize_initial_context(_INITIAL) == _INITIAL
    digest = context_source_sha256(
        initial_context=_INITIAL,
        accepted_turn_ids=[turn_id],
        accepted_invocation_ids=[invocation_id],
    )
    assert digest == context_source_sha256(
        initial_context=dict(_INITIAL),
        accepted_turn_ids=[str(turn_id)],
        accepted_invocation_ids=[str(invocation_id)],
    )
    with pytest.raises(AgentContextError):
        context_source_sha256(
            initial_context=_INITIAL,
            accepted_turn_ids=["not-a-database-id"],
            accepted_invocation_ids=[],
        )
