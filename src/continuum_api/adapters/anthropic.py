"""Bounded Anthropic Messages API adapter for the durable agent loop."""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from anthropic import APIConnectionError, APIStatusError, APITimeoutError, AsyncAnthropic
from pydantic import SecretStr

from continuum_api.credentials import CredentialProvider
from continuum_api.runtime.agent_loop import (
    ANTHROPIC_MODEL_ID,
    MAX_AGENT_OUTPUT_TOKENS,
    ScriptedModelRequest,
    ScriptedModelResponse,
    ScriptedToolCall,
)
from continuum_api.runtime.provider_credentials import CredentialDispatchGrant

_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_MAX_TIMEOUT_SECONDS = 60.0
_MAX_TOOLS = 16
_MAX_CALLS = 10
_MAX_CONTEXT_BYTES = 65_536
_ALLOWED_TOOLS = frozenset({"list_files", "read_file", "search_text", "apply_patch", "diff"})


class AnthropicAdapterError(RuntimeError):
    """Sanitized provider failure; deliberately excludes SDK response bodies."""

    def __init__(self, code: str, *, outcome_unknown: bool = True) -> None:
        self.code = code
        self.outcome_unknown = outcome_unknown
        super().__init__(code)


class _MessagesClient(Protocol):
    @property
    def messages(self) -> Any: ...

    async def close(self) -> None: ...


class AnthropicModelAdapter:
    """Calls the pinned Messages API once with one-use T23 credential grants."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 45.0,
        client_factory: Callable[..., _MessagesClient] = AsyncAnthropic,
    ) -> None:
        if not 1.0 <= timeout_seconds <= _MAX_TIMEOUT_SECONDS:
            raise AnthropicAdapterError("anthropic_timeout_invalid")
        self._timeout_seconds = timeout_seconds
        self._client_factory = client_factory
        self._used_request_ids: set[str] = set()

    def __repr__(self) -> str:
        return "AnthropicModelAdapter(credential=[REDACTED])"

    async def close(self) -> None:
        return None

    async def complete(self, request: ScriptedModelRequest) -> ScriptedModelResponse:
        raise AnthropicAdapterError("anthropic_credential_grant_required")

    def validate_request(self, request: ScriptedModelRequest) -> None:
        if request.model_id != ANTHROPIC_MODEL_ID:
            raise AnthropicAdapterError("anthropic_model_unsupported")
        if not 1 <= request.max_output_tokens <= MAX_AGENT_OUTPUT_TOKENS:
            raise AnthropicAdapterError("anthropic_output_limit_invalid")
        if len(request.tool_schemas) > _MAX_TOOLS:
            raise AnthropicAdapterError("anthropic_tool_limit_exceeded")
        _messages(request.context)
        _tools(request.tool_schemas)

    async def complete_with_grant(
        self, request: ScriptedModelRequest, grant: CredentialDispatchGrant
    ) -> ScriptedModelResponse:
        self.validate_request(request)
        if grant.provider is not CredentialProvider.ANTHROPIC:
            raise AnthropicAdapterError("anthropic_credential_provider_mismatch")
        if str(grant.request_id) != str(request.turn_id):
            raise AnthropicAdapterError("anthropic_credential_grant_request_mismatch")
        if str(request.turn_id) in self._used_request_ids:
            raise AnthropicAdapterError("anthropic_credential_grant_already_used")
        if not isinstance(grant.secret, SecretStr) or not grant.secret.get_secret_value().strip():
            raise AnthropicAdapterError("anthropic_credential_unavailable")
        self._used_request_ids.add(str(request.turn_id))

        client = self._client_factory(
            api_key=grant.secret.get_secret_value(),
            max_retries=0,
            timeout=self._timeout_seconds,
        )
        try:
            started_at = time.monotonic()
            async with asyncio.timeout(self._timeout_seconds):
                response = await client.messages.create(
                    model=ANTHROPIC_MODEL_ID,
                    max_tokens=request.max_output_tokens,
                    messages=_messages(request.context),
                    tools=_tools(request.tool_schemas),
                    tool_choice={"type": "auto", "disable_parallel_tool_use": True},
                    timeout=self._timeout_seconds,
                )
        except TimeoutError:
            raise AnthropicAdapterError("anthropic_timeout_outcome_unknown") from None
        except APITimeoutError:
            raise AnthropicAdapterError("anthropic_timeout_outcome_unknown") from None
        except APIConnectionError:
            raise AnthropicAdapterError("anthropic_connection_outcome_unknown") from None
        except APIStatusError as error:
            status = error.status_code
            if status in {401, 403}:
                code = "anthropic_authentication_failed"
                outcome_unknown = False
            elif status == 429:
                code = "anthropic_rate_limited_outcome_unknown"
                outcome_unknown = True
            elif status >= 500:
                code = "anthropic_server_error_outcome_unknown"
                outcome_unknown = True
            elif status in {400, 404, 422}:
                code = "anthropic_request_rejected"
                outcome_unknown = False
            else:
                code = "anthropic_provider_error_outcome_unknown"
                outcome_unknown = True
            raise AnthropicAdapterError(code, outcome_unknown=outcome_unknown) from None
        except Exception:
            raise AnthropicAdapterError("anthropic_provider_error_outcome_unknown") from None
        finally:
            await client.close()

        elapsed_ms = min(600_000, max(0, round((time.monotonic() - started_at) * 1000)))
        try:
            return _normalize_response(response, elapsed_ms)
        except (TypeError, ValueError, KeyError):
            raise AnthropicAdapterError("anthropic_response_invalid") from None


def _messages(context: dict[str, Any]) -> list[dict[str, Any]]:
    prior = context.get("anthropic_continuation_messages", [])
    if not isinstance(prior, list) or len(prior) > 40:
        raise AnthropicAdapterError("anthropic_continuation_invalid")
    if any(
        not isinstance(message, dict)
        or message.get("role") not in {"user", "assistant"}
        or "content" not in message
        for message in prior
    ):
        raise AnthropicAdapterError("anthropic_continuation_invalid")
    public_context = {
        key: value for key, value in context.items() if key != "anthropic_continuation_messages"
    }
    try:
        encoded_prior = json.dumps(prior, ensure_ascii=False, separators=(",", ":"))
        encoded_context = json.dumps(public_context, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError, RecursionError):
        raise AnthropicAdapterError("anthropic_context_invalid") from None
    if len(encoded_prior.encode("utf-8")) + len(encoded_context.encode("utf-8")) > (
        _MAX_CONTEXT_BYTES
    ):
        raise AnthropicAdapterError("anthropic_context_size_limit")
    return [
        *prior,
        {"role": "user", "content": encoded_context},
    ]


def _tools(schemas: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    tools: list[dict[str, Any]] = []
    seen: set[str] = set()
    for schema in schemas:
        raw = schema.get("function", schema)
        if not isinstance(raw, Mapping):
            raise AnthropicAdapterError("anthropic_tool_schema_invalid")
        name = raw.get("name")
        parameters = raw.get("parameters")
        description = raw.get("description", "")
        if (
            not isinstance(name, str)
            or name not in _ALLOWED_TOOLS
            or name in seen
            or not isinstance(parameters, dict)
            or not isinstance(description, str)
            or len(description) > 1024
        ):
            raise AnthropicAdapterError("anthropic_tool_schema_invalid")
        _strict_schema(parameters)
        tools.append(
            {
                "name": name,
                "description": description,
                "input_schema": parameters,
                "strict": True,
            }
        )
        seen.add(name)
    return tools


def _strict_schema(schema: dict[str, Any]) -> None:
    if schema.get("type") != "object":
        raise AnthropicAdapterError("anthropic_tool_schema_invalid")
    properties = schema.get("properties", {})
    required = schema.get("required", [])
    if (
        not isinstance(properties, dict)
        or not isinstance(required, list)
        or set(required) != set(properties)
        or schema.get("additionalProperties") is not False
    ):
        raise AnthropicAdapterError("anthropic_tool_schema_not_strict")
    _validate_nested_schema(schema)


def _validate_nested_schema(schema: dict[str, Any]) -> None:
    if schema.get("type") == "object":
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        if (
            not isinstance(properties, dict)
            or not isinstance(required, list)
            or set(required) != set(properties)
            or schema.get("additionalProperties") is not False
        ):
            raise AnthropicAdapterError("anthropic_tool_schema_not_strict")
    for key in ("properties", "$defs", "definitions", "patternProperties"):
        children = schema.get(key)
        if children is None:
            continue
        if not isinstance(children, dict):
            raise AnthropicAdapterError("anthropic_tool_schema_invalid")
        for child in children.values():
            if not isinstance(child, dict):
                raise AnthropicAdapterError("anthropic_tool_schema_invalid")
            _validate_nested_schema(child)
    for key in ("items", "not", "if", "then", "else", "contains"):
        child = schema.get(key)
        if child is not None:
            if not isinstance(child, dict):
                raise AnthropicAdapterError("anthropic_tool_schema_invalid")
            _validate_nested_schema(child)
    for key in ("anyOf", "oneOf", "allOf", "prefixItems"):
        children = schema.get(key)
        if children is not None:
            if not isinstance(children, list):
                raise AnthropicAdapterError("anthropic_tool_schema_invalid")
            for child in children:
                if not isinstance(child, dict):
                    raise AnthropicAdapterError("anthropic_tool_schema_invalid")
                _validate_nested_schema(child)


def _normalize_response(response: Any, elapsed_ms: int) -> ScriptedModelResponse:
    response_id = _safe_id(getattr(response, "id", None), "anthropic_response_id_invalid")
    if getattr(response, "model", None) != ANTHROPIC_MODEL_ID:
        raise ValueError("model_mismatch")
    blocks = getattr(response, "content", None)
    if not isinstance(blocks, (list, tuple)):
        raise ValueError("content_invalid")
    text_parts: list[str] = []
    calls: list[ScriptedToolCall] = []
    raw_blocks: list[dict[str, Any]] = []
    for block in blocks:
        block_type = getattr(block, "type", None)
        if block_type == "text":
            text_value = getattr(block, "text", None)
            if not isinstance(text_value, str):
                raise ValueError("text_invalid")
            text_parts.append(text_value)
        elif block_type == "tool_use":
            tool_id = _safe_id(getattr(block, "id", None), "anthropic_tool_use_id_invalid")
            name = getattr(block, "name", None)
            arguments = getattr(block, "input", None)
            if not isinstance(name, str) or not isinstance(arguments, dict):
                raise ValueError("tool_use_invalid")
            arguments_json = json.dumps(
                arguments,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
            calls.append(
                ScriptedToolCall(
                    provider_call_id=tool_id,
                    name=name,
                    arguments_json=arguments_json,
                )
            )
        if not hasattr(block, "model_dump"):
            raise ValueError("content_block_not_serializable")
        dumped = block.model_dump(mode="json", exclude_none=True)
        if not isinstance(dumped, dict):
            raise ValueError("content_block_invalid")
        raw_blocks.append(dumped)
    if len(calls) > _MAX_CALLS:
        raise ValueError("tool_call_limit")
    if len({call.provider_call_id for call in calls}) != len(calls):
        raise ValueError("duplicate_tool_call")
    if sum(map(len, text_parts)) > 65_536:
        raise ValueError("text_size_limit")

    usage = getattr(response, "usage", None)
    base_input = getattr(usage, "input_tokens", None) if usage is not None else None
    output_tokens = getattr(usage, "output_tokens", None) if usage is not None else None
    cache_read = getattr(usage, "cache_read_input_tokens", None) if usage is not None else None
    cache_creation = (
        getattr(usage, "cache_creation_input_tokens", None) if usage is not None else None
    )
    if (
        type(base_input) is not int
        or type(output_tokens) is not int
        or type(cache_read) is not int
        or type(cache_creation) is not int
    ):
        input_tokens = output_tokens = None
        cached_input_tokens = cache_creation_input_tokens = reasoning_output_tokens = None
    else:
        input_tokens = base_input + cache_read + cache_creation
        cached_input_tokens = cache_read
        cache_creation_input_tokens = cache_creation
        details = getattr(usage, "output_tokens_details", None)
        reasoning_tokens = (
            getattr(details, "thinking_tokens", None) if details is not None else None
        )
        reasoning_output_tokens = reasoning_tokens if type(reasoning_tokens) is int else None

    stop_reason_raw = getattr(response, "stop_reason", None)
    stop_details = getattr(response, "stop_details", None)
    refusal = getattr(stop_details, "type", None) == "refusal" or stop_reason_raw == "refusal"
    if refusal:
        complete, stop_reason = True, "refusal"
    elif stop_reason_raw == "tool_use" and calls:
        complete, stop_reason = True, "tool_calls"
    elif stop_reason_raw == "end_turn" and not calls:
        complete, stop_reason = True, "stop"
    elif isinstance(stop_reason_raw, str):
        complete, stop_reason = False, stop_reason_raw[:32]
    else:
        complete, stop_reason = False, "incomplete"
    request_id = getattr(response, "_request_id", None)
    request_id = _safe_id(request_id, "anthropic_request_id_invalid") if request_id else None
    return ScriptedModelResponse(
        complete=complete,
        stop_reason=stop_reason,
        model_id=ANTHROPIC_MODEL_ID,
        response_id=response_id,
        provider_request_id=request_id,
        output_text="".join(text_parts),
        calls=tuple(calls),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_input_tokens=cached_input_tokens,
        cache_creation_input_tokens=cache_creation_input_tokens,
        reasoning_output_tokens=reasoning_output_tokens,
        latency_ms=elapsed_ms,
        continuation={
            "assistant_message": {"role": "assistant", "content": raw_blocks},
            "message_id": response_id,
        },
    )


def _safe_id(value: object, code: str) -> str:
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise ValueError(code)
    return value
