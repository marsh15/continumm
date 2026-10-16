"""Fail-closed construction of bounded, provider-native agent context."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from pydantic import SecretStr

MAX_AGENT_CONTEXT_BYTES = 65_536
_INITIAL_KEYS = frozenset({"task", "objective", "constraints", "policy"})
_OPENAI_HISTORY = "openai_continuation_items"
_ANTHROPIC_HISTORY = "anthropic_continuation_messages"
_SCRIPTED_MODEL = "scripted-v1"
_SECRET_KEY = re.compile(
    r"(?:secret|credential|password|authorization|api[_-]?key|grant|private[_-]?key|"
    r"(?:access|refresh|session|bearer|id)[_-]?token|(?:^|[_-])token(?:$|[_-]))",
    re.I,
)
_SAFE_PROVIDER_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_ALLOWED_TOOL_NAMES = frozenset({"list_files", "read_file", "search_text", "apply_patch", "diff"})
_RECENT_TURNS = 3
_SUMMARY_TEXT_CHARS = 512
_OPENAI_HISTORY_ITEMS = 20
_ANTHROPIC_HISTORY_MESSAGES = 40


class AgentContextError(ValueError):
    """Sanitized context assembly failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def canonicalize_initial_context(initial_context: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and return the exact pinned task/policy context allowlist."""
    try:
        return _validate_initial(initial_context)
    except AgentContextError:
        raise
    except (TypeError, ValueError, RecursionError, OverflowError):
        raise AgentContextError("agent_context_initial_invalid") from None


def context_source_sha256(
    *,
    initial_context: Mapping[str, Any],
    accepted_turn_ids: Sequence[UUID | str],
    accepted_invocation_ids: Sequence[UUID | str],
    revision_feedback: Mapping[str, Any] | None = None,
) -> str:
    """Hash pinned inputs and ordered database identities for context provenance."""
    payload: dict[str, Any] = {
        "initial_context": canonicalize_initial_context(initial_context),
        "accepted_turn_ids": _canonical_row_ids(accepted_turn_ids),
        "accepted_invocation_ids": _canonical_row_ids(accepted_invocation_ids),
    }
    feedback = _validate_revision_feedback(revision_feedback)
    if feedback is not None:
        payload["revision_feedback"] = feedback
    try:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError, RecursionError, OverflowError):
        raise AgentContextError("agent_context_source_invalid") from None
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_agent_context(
    *,
    initial_context: Mapping[str, Any],
    accepted_turns: Sequence[Mapping[str, Any]],
    model_id: str,
    budget: Mapping[str, Any],
    revision_feedback: Mapping[str, Any] | None = None,
    context_limit_bytes: int = MAX_AGENT_CONTEXT_BYTES,
) -> dict[str, Any]:
    """Build context only from pinned inputs and accepted durable turn records.

    Each accepted turn has ``model_id``, persisted ``context`` and normalized
    ``response`` mappings, and ordered ``invocations``. An invocation carries
    ``provider_call_id``, ``tool_name``, and its accepted ``accepted_payload``.
    The caller must load and verify these records under its durable run lock.
    """
    try:
        pinned = _validate_initial(initial_context)
        _validate_budget(budget)
        feedback = _validate_revision_feedback(revision_feedback)
        openai_model_id, anthropic_model_id, scripted_model_id = _model_ids()
        if model_id not in {openai_model_id, anthropic_model_id, scripted_model_id}:
            raise AgentContextError("agent_context_model_invalid")
        if (
            type(context_limit_bytes) is not int
            or not 1 <= context_limit_bytes <= MAX_AGENT_CONTEXT_BYTES
        ):
            raise AgentContextError("agent_context_size_limit")
        if not isinstance(accepted_turns, Sequence) or isinstance(accepted_turns, (str, bytes)):
            raise AgentContextError("agent_context_history_invalid")

        key = _history_key(model_id)
        history: list[dict[str, Any]] = []
        visible: list[dict[str, Any]] = []
        for index, turn in enumerate(accepted_turns):
            if not isinstance(turn, Mapping) or set(turn) != {
                "model_id",
                "context",
                "response",
                "invocations",
            }:
                raise AgentContextError("agent_context_history_invalid")
            turn_model = turn.get("model_id")
            context = turn.get("context")
            response = turn.get("response")
            invocations = turn.get("invocations")
            if turn_model != model_id:
                raise AgentContextError("agent_context_model_mismatch")
            if not isinstance(context, Mapping) or not isinstance(response, Mapping):
                raise AgentContextError("agent_context_history_invalid")
            if not isinstance(invocations, Sequence) or isinstance(invocations, (str, bytes)):
                raise AgentContextError("agent_context_history_invalid")
            public_context = _public_prior_context(context, key, history, pinned, visible)
            visible.append(_visible_turn(index, response, invocations))
            history = _append_native_turn(model_id, history, public_context, response, invocations)
        history_limit = (
            _OPENAI_HISTORY_ITEMS if model_id == openai_model_id else _ANTHROPIC_HISTORY_MESSAGES
        )
        if len(history) > history_limit:
            raise AgentContextError("agent_context_continuation_limit")

        current = dict(pinned)
        current["budget"] = dict(budget)
        current.update(_visible_context(visible))
        if feedback is not None:
            current["revision_feedback"] = feedback
        if history and key is not None:
            current[key] = history
        _validate_json_value(current)
        if _request_context_size(current, model_id) > context_limit_bytes:
            raise AgentContextError("agent_context_size_limit")
        return current
    except AgentContextError:
        raise
    except (TypeError, ValueError, KeyError, RecursionError, OverflowError):
        raise AgentContextError("agent_context_invalid") from None


def _validate_initial(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(key not in _INITIAL_KEYS for key in value):
        raise AgentContextError("agent_context_initial_invalid")
    if not value or not ({"task", "objective"} & set(value)):
        raise AgentContextError("agent_context_initial_invalid")
    result = dict(value)
    for key, item in result.items():
        if not isinstance(key, str):
            raise AgentContextError("agent_context_initial_invalid")
        _validate_json_value(item)
    return result


def _validate_budget(value: Mapping[str, Any]) -> None:
    expected = {
        "requests_remaining",
        "input_tokens_remaining",
        "output_tokens_remaining",
        "usage_uncertain",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise AgentContextError("agent_context_budget_invalid")
    for key in expected - {"usage_uncertain"}:
        if type(value[key]) is not int or value[key] < 0:
            raise AgentContextError("agent_context_budget_invalid")
    if type(value["usage_uncertain"]) is not bool:
        raise AgentContextError("agent_context_budget_invalid")


def _history_key(model_id: str) -> str | None:
    openai_model_id, anthropic_model_id, scripted_model_id = _model_ids()
    if model_id == openai_model_id:
        return _OPENAI_HISTORY
    if model_id == anthropic_model_id:
        return _ANTHROPIC_HISTORY
    if model_id == scripted_model_id:
        return None
    raise AgentContextError("agent_context_model_invalid")


def _model_ids() -> tuple[str, str, str]:
    # Lazy import keeps this module safe for the durable loop to import.
    from continuum_api.runtime.agent_loop import (
        _SCRIPTED_MODEL_ID,
        ANTHROPIC_MODEL_ID,
        OPENAI_MODEL_ID,
    )

    return OPENAI_MODEL_ID, ANTHROPIC_MODEL_ID, _SCRIPTED_MODEL_ID


def _canonical_row_ids(values: Sequence[UUID | str]) -> list[str]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise AgentContextError("agent_context_source_invalid")
    try:
        result = [str(value if isinstance(value, UUID) else UUID(value)) for value in values]
    except (TypeError, ValueError, AttributeError):
        raise AgentContextError("agent_context_source_invalid") from None
    if len(set(result)) != len(result):
        raise AgentContextError("agent_context_source_invalid")
    return result


def _public_prior_context(
    context: Mapping[str, Any],
    history_key: str | None,
    expected_history: list[dict[str, Any]],
    pinned: Mapping[str, Any],
    prior_visible: list[dict[str, Any]],
) -> dict[str, Any]:
    budget = context.get("budget")
    if not isinstance(budget, Mapping):
        raise AgentContextError("agent_context_history_invalid")
    _validate_budget(budget)
    expected: dict[str, Any] = {**pinned, "budget": dict(budget)}
    expected.update(_visible_context(prior_visible))
    feedback = _validate_revision_feedback(context.get("revision_feedback"))
    if feedback is not None:
        expected["revision_feedback"] = feedback
    if expected_history and history_key is not None:
        expected[history_key] = expected_history
    if dict(context) != expected:
        raise AgentContextError("agent_context_history_invalid")
    return {key: item for key, item in expected.items() if key != history_key}


def _validate_revision_feedback(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {
        "feedback_id",
        "revision",
        "feedback",
        "feedback_sha256",
    }:
        raise AgentContextError("agent_context_revision_feedback_invalid")
    feedback = value["feedback"]
    revision = value["revision"]
    digest = value["feedback_sha256"]
    try:
        feedback_id = str(UUID(str(value["feedback_id"])))
        encoded = feedback.encode("utf-8", errors="strict")
    except (AttributeError, TypeError, ValueError, UnicodeEncodeError):
        raise AgentContextError("agent_context_revision_feedback_invalid") from None
    if (
        type(revision) is not int
        or revision not in {1, 2}
        or not isinstance(feedback, str)
        or not feedback.strip()
        or len(encoded) > 16_384
        or not isinstance(digest, str)
        or not re.fullmatch(r"[0-9a-f]{64}", digest)
        or hashlib.sha256(encoded).hexdigest() != digest
    ):
        raise AgentContextError("agent_context_revision_feedback_invalid")
    return {
        "feedback_id": feedback_id,
        "revision": revision,
        "feedback": feedback,
        "feedback_sha256": digest,
    }


def _append_native_turn(
    model_id: str,
    prior: list[dict[str, Any]],
    public_context: dict[str, Any],
    response: Mapping[str, Any],
    invocations: Sequence[Any],
) -> list[dict[str, Any]]:
    openai_model_id, _, scripted_model_id = _model_ids()
    if response.get("complete") is not True or response.get("model_id") != model_id:
        raise AgentContextError("agent_context_response_incomplete")
    stop_reason = response.get("stop_reason")
    calls = response.get("calls")
    continuation = response.get("continuation")
    output_text = response.get("output_text")
    if not isinstance(calls, list | tuple) or not isinstance(output_text, str):
        raise AgentContextError("agent_context_response_invalid")
    call_pairs = [_call_pair(call) for call in calls]
    invocation_pairs = [
        _invocation_pair(invocation, expected_ordinal=index)
        for index, invocation in enumerate(invocations)
    ]
    if call_pairs != invocation_pairs:
        raise AgentContextError("agent_context_tool_result_missing")
    if stop_reason != ("tool_calls" if call_pairs else "stop"):
        raise AgentContextError("agent_context_response_invalid")

    encoded_context = _json_text(public_context)
    if model_id == scripted_model_id:
        if continuation is not None:
            raise AgentContextError("agent_context_continuation_invalid")
        return prior
    if not isinstance(continuation, Mapping):
        raise AgentContextError("agent_context_continuation_invalid")
    if model_id == openai_model_id:
        items = continuation.get("items")
        if not isinstance(items, list) or not items:
            raise AgentContextError("agent_context_continuation_invalid")
        if any(
            not isinstance(item, Mapping)
            or item.get("type") not in {"reasoning", "message", "function_call"}
            for item in items
        ):
            raise AgentContextError("agent_context_continuation_invalid")
        function_calls = [item for item in items if item.get("type") == "function_call"]
        if [(item.get("call_id"), item.get("name")) for item in function_calls] != call_pairs:
            raise AgentContextError("agent_context_continuation_invalid")
        continuation_text = "".join(
            text
            for item in items
            if item.get("type") == "message"
            for content in item.get("content", [])
            if isinstance(item.get("content"), list)
            and isinstance(content, Mapping)
            and content.get("type") in {"output_text", "refusal"}
            and isinstance((text := content.get("text", content.get("refusal"))), str)
        )
        if continuation_text != output_text:
            raise AgentContextError("agent_context_continuation_invalid")
        new_items: list[dict[str, Any]] = [
            *prior,
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": encoded_context}],
            },
            *[dict(item) for item in items],
        ]
        for invocation in invocations:
            call_id, _ = _invocation_pair(invocation)
            payload = invocation["accepted_payload"] if isinstance(invocation, Mapping) else None
            result = payload.get("tool_result") if isinstance(payload, Mapping) else None
            new_items.append(
                {
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": _json_text(result),
                }
            )
        return new_items

    assistant_message = continuation.get("assistant_message")
    if (
        not isinstance(assistant_message, Mapping)
        or assistant_message.get("role") != "assistant"
        or not isinstance(assistant_message.get("content"), list)
    ):
        raise AgentContextError("agent_context_continuation_invalid")
    blocks = assistant_message["content"]
    if any(
        not isinstance(block, Mapping)
        or block.get("type") not in {"thinking", "redacted_thinking", "text", "tool_use"}
        for block in blocks
    ):
        raise AgentContextError("agent_context_continuation_invalid")
    tool_uses = [
        block for block in blocks if isinstance(block, Mapping) and block.get("type") == "tool_use"
    ]
    if [(block.get("id"), block.get("name")) for block in tool_uses] != call_pairs:
        raise AgentContextError("agent_context_continuation_invalid")
    continuation_text = "".join(
        block["text"]
        for block in blocks
        if block.get("type") == "text" and isinstance(block.get("text"), str)
    )
    if continuation_text != output_text:
        raise AgentContextError("agent_context_continuation_invalid")
    new_messages: list[dict[str, Any]] = [
        *prior,
        {"role": "user", "content": encoded_context},
        dict(assistant_message),
    ]
    if invocations:
        results = []
        for invocation in invocations:
            call_id, _ = _invocation_pair(invocation)
            payload = invocation["accepted_payload"]
            results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": call_id,
                    "content": _json_text(payload["tool_result"]),
                    "is_error": False,
                }
            )
        new_messages.append({"role": "user", "content": results})
    return new_messages


def _call_pair(value: Any) -> tuple[str, str]:
    if not isinstance(value, Mapping):
        raise AgentContextError("agent_context_response_invalid")
    call_id = value.get("provider_call_id")
    name = value.get("name")
    if (
        not isinstance(call_id, str)
        or _SAFE_PROVIDER_ID.fullmatch(call_id) is None
        or not isinstance(name, str)
        or name not in _ALLOWED_TOOL_NAMES
    ):
        raise AgentContextError("agent_context_response_invalid")
    return call_id, name


def _invocation_pair(value: Any, *, expected_ordinal: int | None = None) -> tuple[str, str]:
    if not isinstance(value, Mapping) or set(value) != {
        "provider_call_id",
        "tool_name",
        "arguments",
        "accepted_payload",
    }:
        raise AgentContextError("agent_context_invocation_invalid")
    call_id = value.get("provider_call_id")
    name = value.get("tool_name")
    payload = value.get("accepted_payload")
    if (
        not isinstance(call_id, str)
        or _SAFE_PROVIDER_ID.fullmatch(call_id) is None
        or not isinstance(name, str)
        or name not in _ALLOWED_TOOL_NAMES
        or not isinstance(payload, Mapping)
        or payload.get("provider_call_id") != call_id
        or payload.get("tool_name") != name
        or "tool_result" not in payload
        or (expected_ordinal is not None and payload.get("ordinal") != expected_ordinal)
    ):
        raise AgentContextError("agent_context_tool_result_missing")
    _validate_json_value(value["arguments"])
    _validate_json_value(payload)
    _validate_json_value(payload["tool_result"])
    return call_id, name


def _visible_turn(
    index: int, response: Mapping[str, Any], invocations: Sequence[Any]
) -> dict[str, Any]:
    output = response.get("output_text")
    if not isinstance(output, str):
        raise AgentContextError("agent_context_response_invalid")
    tools = []
    for invocation in invocations:
        _invocation_pair(invocation)
        payload = invocation["accepted_payload"]
        result = payload["tool_result"]
        tools.append({"name": invocation["tool_name"], "result": result})
    return {"turn": index + 1, "response": output, "tools": tools}


def _visible_context(turns: list[dict[str, Any]]) -> dict[str, Any]:
    recent = turns[-_RECENT_TURNS:]
    older = turns[:-_RECENT_TURNS]
    context: dict[str, Any] = {"recent_results": recent}
    if older:
        summaries = []
        for turn in older:
            encoded_tools = _json_text(turn["tools"])
            summaries.append(
                {
                    "turn": turn["turn"],
                    "response_excerpt": turn["response"][:_SUMMARY_TEXT_CHARS],
                    "response_sha256": hashlib.sha256(turn["response"].encode("utf-8")).hexdigest(),
                    "tools_excerpt": encoded_tools[:_SUMMARY_TEXT_CHARS],
                    "tools_sha256": hashlib.sha256(encoded_tools.encode("utf-8")).hexdigest(),
                }
            )
        context["older_result_summaries"] = summaries
    return context


def _request_context_size(context: Mapping[str, Any], model_id: str) -> int:
    openai_model_id, _, scripted_model_id = _model_ids()
    history_key = _history_key(model_id)
    history = context.get(history_key, []) if history_key is not None else []
    public = (
        {key: item for key, item in context.items() if key != history_key}
        if history_key is not None
        else dict(context)
    )
    # Include wrappers used by the adapters for the current native user input.
    context_text = _json_text(public)
    if model_id == openai_model_id:
        current = {"role": "user", "content": [{"type": "input_text", "text": context_text}]}
    elif model_id == scripted_model_id:
        return len(context_text.encode("utf-8"))
    else:
        current = {"role": "user", "content": context_text}
    return len((_json_text(history) + _json_text(current)).encode("utf-8"))


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _validate_json_value(value: Any) -> None:
    if isinstance(value, SecretStr):
        raise AgentContextError("agent_context_credential_rejected")
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise AgentContextError("agent_context_invalid")
            if key not in {"input_tokens", "output_tokens"} and _SECRET_KEY.search(key):
                raise AgentContextError("agent_context_credential_rejected")
            _validate_json_value(item)
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _validate_json_value(item)
        return
    if value is None or type(value) in {str, int, float, bool}:
        return
    raise AgentContextError("agent_context_invalid")
