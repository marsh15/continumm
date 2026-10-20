"""Fixed model-facing schemas for the trusted source tools."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from continuum_api.tools.contracts import (
    ApplyPatchArguments,
    DiffArguments,
    ListFilesArguments,
    ReadFileArguments,
    SearchTextArguments,
)

_ARGUMENT_MODELS = (
    ("list_files", "List files and directories in the immutable source tree.", ListFilesArguments),
    ("read_file", "Read a bounded page from a source file.", ReadFileArguments),
    ("search_text", "Search source files for a text query.", SearchTextArguments),
    ("apply_patch", "Apply literal edits to source files.", ApplyPatchArguments),
    ("diff", "Show the current candidate changes.", DiffArguments),
)


def fixed_model_tool_schemas() -> tuple[dict[str, Any], ...]:
    """Return the complete fixed tool set in the durable agent schema format."""
    schemas: list[dict[str, Any]] = []
    for name, description, model in _ARGUMENT_MODELS:
        raw = model.model_json_schema()
        definitions = raw.pop("$defs", {})
        properties = raw.get("properties")
        if not isinstance(properties, dict):
            raise ValueError("agent_tool_schema_invalid")
        properties.pop("name", None)
        raw["required"] = list(properties)
        raw["additionalProperties"] = False
        parameters = _strip_schema_defaults(_inline_definitions(raw, definitions))
        schemas.append({"name": name, "description": description, "parameters": parameters})
    return tuple(deepcopy(schemas))


def _inline_definitions(value: Any, definitions: dict[str, Any]) -> Any:
    if isinstance(value, list):
        return [_inline_definitions(item, definitions) for item in value]
    if not isinstance(value, dict):
        return value
    reference = value.get("$ref")
    if reference is not None:
        prefix = "#/$defs/"
        if not isinstance(reference, str) or not reference.startswith(prefix):
            raise ValueError("agent_tool_schema_invalid")
        definition = definitions.get(reference[len(prefix) :])
        if not isinstance(definition, dict):
            raise ValueError("agent_tool_schema_invalid")
        return _inline_definitions(deepcopy(definition), definitions)
    return {key: _inline_definitions(item, definitions) for key, item in value.items()}


def _strip_schema_defaults(value: Any) -> Any:
    if isinstance(value, list):
        return [_strip_schema_defaults(item) for item in value]
    if not isinstance(value, dict):
        return value
    return {key: _strip_schema_defaults(item) for key, item in value.items() if key != "default"}
