"""Bounded OpenAI Responses API adapter for the durable agent loop."""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from openai import APIConnectionError, APIStatusError, APITimeoutError, AsyncOpenAI
from pydantic import SecretStr

from continuum_api.credentials import CredentialProvider
from continuum_api.runtime.agent_loop import (
    MAX_AGENT_OUTPUT_TOKENS,
    OPENAI_MODEL_ID,
    ScriptedModelRequest,
    ScriptedModelResponse,
    ScriptedToolCall,
)
from continuum_api.runtime.provider_credentials import CredentialDispatchGrant

_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_MAX_TIMEOUT_SECONDS = 60.0
_MAX_TOOLS = 16
_MAX_CALLS = 10
_ALLOWED_TOOLS = frozenset({"list_files", "read_file", "search_text", "apply_patch", "diff"})


class OpenAIAdapterError(RuntimeError):
    """Sanitized provider failure; deliberately excludes SDK response bodies."""

    def __init__(self, code: str, *, outcome_unknown: bool = True) -> None:
        self.code = code
        self.outcome_unknown = outcome_unknown
        super().__init__(code)


class _ResponsesClient(Protocol):
    @property
    def responses(self) -> Any: ...

    async def close(self) -> None: ...


class OpenAIModelAdapter:
    """Calls the pinned model once; orchestration owns authorization and replay policy."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 45.0,
        client_factory: Callable[..., _ResponsesClient] = AsyncOpenAI,
    ) -> None:
        if not 1.0 <= timeout_seconds <= _MAX_TIMEOUT_SECONDS:
            raise OpenAIAdapterError("openai_timeout_invalid")
        self._timeout_seconds = timeout_seconds
        self._client_factory = client_factory
        self._used_request_ids: set[str] = set()

    def __repr__(self) -> str:
        return "OpenAIModelAdapter(credential=[REDACTED])"

    async def close(self) -> None:
        return None

    async def complete(self, request: ScriptedModelRequest) -> ScriptedModelResponse:
        raise OpenAIAdapterError("openai_credential_grant_required")

    def validate_request(self, request: ScriptedModelRequest) -> None:
        if request.model_id != OPENAI_MODEL_ID:
            raise OpenAIAdapterError("openai_model_unsupported")
        if not 1 <= request.max_output_tokens <= MAX_AGENT_OUTPUT_TOKENS:
            raise OpenAIAdapterError("openai_output_limit_invalid")
        if len(request.tool_schemas) > _MAX_TOOLS:
            raise OpenAIAdapterError("openai_tool_limit_exceeded")
        _input_items(request.context)
        _tools(request.tool_schemas)

    async def complete_with_grant(
        self, request: ScriptedModelRequest, grant: CredentialDispatchGrant
    ) -> ScriptedModelResponse:
        self.validate_request(request)
        if grant.provider is not CredentialProvider.OPENAI:
            raise OpenAIAdapterError("openai_credential_provider_mismatch")
        if str(grant.request_id) != str(request.turn_id):
            raise OpenAIAdapterError("openai_credential_grant_request_mismatch")
        if str(request.turn_id) in self._used_request_ids:
            raise OpenAIAdapterError("openai_credential_grant_already_used")
        self._used_request_ids.add(str(request.turn_id))
        if not isinstance(grant.secret, SecretStr) or not grant.secret.get_secret_value().strip():
            raise OpenAIAdapterError("openai_credential_unavailable")
        client = self._client_factory(
            api_key=grant.secret.get_secret_value(),
            max_retries=0,
            timeout=self._timeout_seconds,
        )
        try:
            started_at = time.monotonic()
            async with asyncio.timeout(self._timeout_seconds):
                response = await client.responses.create(
                    model=OPENAI_MODEL_ID,
                    input=_input_items(request.context),
                    tools=_tools(request.tool_schemas),
                    max_output_tokens=request.max_output_tokens,
                    parallel_tool_calls=False,
                    store=False,
                    include=["reasoning.encrypted_content"],
                    timeout=self._timeout_seconds,
                )
        except TimeoutError:
            raise OpenAIAdapterError("openai_timeout_outcome_unknown") from None
        except APITimeoutError:
            raise OpenAIAdapterError("openai_timeout_outcome_unknown") from None
        except APIConnectionError:
            raise OpenAIAdapterError("openai_connection_outcome_unknown") from None
        except APIStatusError as error:
            status = error.status_code
            if status in {401, 403}:
                code = "openai_authentication_failed"
            elif status == 429:
                code = "openai_rate_limited_outcome_unknown"
            elif status >= 500:
                code = "openai_server_error_outcome_unknown"
            elif status in {400, 404, 422}:
                code = "openai_request_rejected"
            else:
                code = "openai_provider_error_outcome_unknown"
            raise OpenAIAdapterError(
                code, outcome_unknown=code != "openai_authentication_failed"
            ) from None
        except Exception:
            raise OpenAIAdapterError("openai_provider_error_outcome_unknown") from None
        finally:
            await client.close()

        elapsed_ms = min(600_000, max(0, round((time.monotonic() - started_at) * 1000)))
        try:
            return _normalize_response(response, elapsed_ms)
        except (TypeError, ValueError, KeyError):
            raise OpenAIAdapterError("openai_response_invalid") from None


def _input_items(context: dict[str, Any]) -> list[dict[str, Any]]:
    prior: object = context.get("openai_continuation_items", [])
    if not isinstance(prior, list) or len(prior) > 20:
        raise OpenAIAdapterError("openai_continuation_invalid")
    if any(not isinstance(item, dict) or not _valid_continuation_item(item) for item in prior):
        raise OpenAIAdapterError("openai_continuation_invalid")
    public_context = {
        key: value for key, value in context.items() if key != "openai_continuation_items"
    }
    try:
        encoded_prior = json.dumps(prior, ensure_ascii=False, separators=(",", ":"))
        encoded_context = json.dumps(public_context, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError, RecursionError):
        raise OpenAIAdapterError("openai_context_invalid") from None
    total_size = len(encoded_prior.encode("utf-8")) + len(encoded_context.encode("utf-8"))
    if total_size > 65_536:
        raise OpenAIAdapterError("openai_context_size_limit")
    return [
        *prior,
        {"role": "user", "content": [{"type": "input_text", "text": encoded_context}]},
    ]


def _valid_continuation_item(item: dict[str, Any]) -> bool:
    item_type = item.get("type")
    return item_type in {
        "reasoning",
        "message",
        "function_call",
        "function_call_output",
    }


def _tools(schemas: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    tools: list[dict[str, Any]] = []
    seen: set[str] = set()
    for schema in schemas:
        raw = schema.get("function", schema)
        if not isinstance(raw, Mapping):
            raise OpenAIAdapterError("openai_tool_schema_invalid")
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
            raise OpenAIAdapterError("openai_tool_schema_invalid")
        normalized = _strict_schema(parameters)
        tools.append(
            {
                "type": "function",
                "name": name,
                "description": description,
                "parameters": normalized,
                "strict": True,
            }
        )
        seen.add(name)
    return tools


def _strict_schema(schema: dict[str, Any]) -> dict[str, Any]:
    if schema.get("type") != "object":
        raise OpenAIAdapterError("openai_tool_schema_invalid")
    properties = schema.get("properties", {})
    if not isinstance(properties, dict):
        raise OpenAIAdapterError("openai_tool_schema_invalid")
    required = schema.get("required", list(properties))
    if (
        not isinstance(required, list)
        or set(required) != set(properties)
        or schema.get("additionalProperties", False) is not False
    ):
        raise OpenAIAdapterError("openai_tool_schema_not_strict")
    for value in properties.values():
        if not isinstance(value, dict):
            raise OpenAIAdapterError("openai_tool_schema_invalid")
        _validate_nested_schema(value)
    result = dict(schema)
    result["required"] = required
    result["additionalProperties"] = False
    return result


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
            raise OpenAIAdapterError("openai_tool_schema_not_strict")
    for key in ("properties", "$defs", "definitions", "patternProperties"):
        children = schema.get(key)
        if children is None:
            continue
        if not isinstance(children, dict):
            raise OpenAIAdapterError("openai_tool_schema_invalid")
        for child in children.values():
            if not isinstance(child, dict):
                raise OpenAIAdapterError("openai_tool_schema_invalid")
            _validate_nested_schema(child)
    for key in ("items", "not", "if", "then", "else", "contains"):
        child = schema.get(key)
        if child is not None:
            if not isinstance(child, dict):
                raise OpenAIAdapterError("openai_tool_schema_invalid")
            _validate_nested_schema(child)
    for key in ("anyOf", "oneOf", "allOf", "prefixItems"):
        children = schema.get(key)
        if children is not None:
            if not isinstance(children, list):
                raise OpenAIAdapterError("openai_tool_schema_invalid")
            for child in children:
                if not isinstance(child, dict):
                    raise OpenAIAdapterError("openai_tool_schema_invalid")
                _validate_nested_schema(child)


def _normalize_response(response: Any, elapsed_ms: int) -> ScriptedModelResponse:
    response_id = _safe_id(getattr(response, "id", None), "openai_response_id_invalid")
    model_id = getattr(response, "model", None)
    if model_id != OPENAI_MODEL_ID:
        raise ValueError("model_mismatch")
    status = getattr(response, "status", None)
    complete = status == "completed"
    output: object = getattr(response, "output", None)
    if not isinstance(output, (list, tuple)):
        raise ValueError("output_invalid")
    text_parts: list[str] = []
    calls: list[ScriptedToolCall] = []
    continuation: list[dict[str, Any]] = []
    refusal_seen = False
    for item in output:
        item_type = getattr(item, "type", None)
        if item_type == "message":
            for content in getattr(item, "content", ()):
                if getattr(content, "type", None) == "output_text":
                    text_value = getattr(content, "text", "")
                    if isinstance(text_value, str):
                        text_parts.append(text_value)
                elif getattr(content, "type", None) == "refusal":
                    refusal_value = getattr(content, "refusal", None)
                    if isinstance(refusal_value, str):
                        text_parts.append(refusal_value)
                        refusal_seen = True
        elif item_type == "function_call":
            call_id = _safe_id(getattr(item, "call_id", None), "openai_tool_call_id_invalid")
            name = getattr(item, "name", None)
            arguments = getattr(item, "arguments", None)
            if not isinstance(name, str) or not isinstance(arguments, str):
                raise ValueError("function_call_invalid")
            calls.append(
                ScriptedToolCall(provider_call_id=call_id, name=name, arguments_json=arguments)
            )
        if hasattr(item, "model_dump"):
            dumped = item.model_dump(mode="json", exclude_none=True)
            if isinstance(dumped, dict):
                continuation.append(dumped)
    if len(calls) > _MAX_CALLS:
        raise ValueError("tool_call_limit")
    if len(set(call.provider_call_id for call in calls)) != len(calls):
        raise ValueError("duplicate_tool_call")
    if sum(map(len, text_parts)) > 65_536:
        raise ValueError("text_size_limit")

    usage = getattr(response, "usage", None)
    input_tokens = getattr(usage, "input_tokens", None) if usage is not None else None
    output_tokens = getattr(usage, "output_tokens", None) if usage is not None else None
    if type(input_tokens) is not int or type(output_tokens) is not int:
        input_tokens = output_tokens = None
        cached_input_tokens = reasoning_output_tokens = None
    else:
        details_in = getattr(usage, "input_tokens_details", None)
        details_out = getattr(usage, "output_tokens_details", None)
        cached_input_tokens = getattr(details_in, "cached_tokens", None)
        reasoning_output_tokens = getattr(details_out, "reasoning_tokens", None)
        if type(cached_input_tokens) is not int:
            cached_input_tokens = None
        if type(reasoning_output_tokens) is not int:
            reasoning_output_tokens = None

    incomplete_details = getattr(response, "incomplete_details", None)
    if complete and refusal_seen:
        stop_reason = "refusal"
    elif complete:
        stop_reason = "tool_calls" if calls else "stop"
    elif incomplete_details is not None and isinstance(
        getattr(incomplete_details, "reason", None), str
    ):
        stop_reason = incomplete_details.reason[:32]
    else:
        stop_reason = "incomplete"
    request_id = getattr(response, "_request_id", None)
    request_id = _safe_id(request_id, "openai_request_id_invalid") if request_id else None
    return ScriptedModelResponse(
        complete=complete,
        stop_reason=stop_reason,
        model_id=OPENAI_MODEL_ID,
        response_id=response_id,
        provider_request_id=request_id,
        output_text="".join(text_parts),
        calls=tuple(calls),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_input_tokens=cached_input_tokens,
        reasoning_output_tokens=reasoning_output_tokens,
        latency_ms=elapsed_ms,
        continuation={"items": continuation} if continuation else None,
    )


def _safe_id(value: object, code: str) -> str:
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise ValueError(code)
    return value
