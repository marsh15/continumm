"""Strict model-facing file tool schemas and bounded result shapes."""

from __future__ import annotations

import json
import re
import unicodedata
from enum import StrEnum
from typing import Annotated, Any, Literal, Self, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

MAX_REPOSITORY_SOURCE_BYTES = 50 * 1024 * 1024
MAX_SOURCE_FILE_BYTES = 1 * 1024 * 1024
MAX_READ_PAGE_BYTES = 16 * 1024
MAX_LIST_ENTRIES = 100
MAX_SEARCH_QUERY_BYTES = 512
MAX_SEARCH_MATCHES = 100
MAX_SEARCH_CONTEXT_LINES = 5
MAX_SEARCH_SNIPPET_CHARS = 128
MAX_SEARCH_SCAN_BYTES = 10 * 1024 * 1024
MAX_SEARCH_FILES_SCANNED = 1_000
MAX_PATCH_FILES = 10
MAX_PATCH_TEXT_BYTES = 64 * 1024
MAX_TOOL_ARGUMENT_BYTES = 64 * 1024
MAX_TOOL_RESULT_BYTES = 64 * 1024
MAX_DIFF_CONTEXT_LINES = 10

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DRIVE_PATH = re.compile(r"^[a-zA-Z]:")


class ToolContractError(ValueError):
    """A stable, sanitized schema or path validation failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class ToolCapability(StrEnum):
    LIST_FILES = "filesystem.list_files"
    READ_FILE = "filesystem.read_file"
    SEARCH_TEXT = "filesystem.search_text"
    APPLY_PATCH = "filesystem.apply_patch"
    DIFF = "filesystem.diff"


class ToolStage(StrEnum):
    AGENT_WORK = "agent_work"
    VALIDATION = "validation"
    CANDIDATE_REVIEW = "candidate_review"
    WAITING_APPROVAL = "waiting_approval"
    PUBLISHING = "publishing"
    TERMINAL = "terminal"


def normalize_source_path(value: str, *, allow_root: bool = False) -> str:
    """Validate one relative, normalized POSIX path from an immutable source tree."""
    if not isinstance(value, str) or not value or "\0" in value or "\\" in value:
        raise ToolContractError("tool_path_invalid")
    if value == "." and allow_root:
        return value
    try:
        encoded = value.encode("utf-8", errors="strict")
    except UnicodeEncodeError:
        raise ToolContractError("tool_path_invalid") from None
    if (
        len(encoded) > 255
        or value.startswith("/")
        or value.startswith("~/")
        or _DRIVE_PATH.match(value) is not None
        or unicodedata.normalize("NFC", value) != value
        or any(unicodedata.category(character) in {"Cc", "Cf", "Cs"} for character in value)
    ):
        raise ToolContractError("tool_path_invalid")
    parts = value.split("/")
    if any(not part or part in {".", ".."} for part in parts):
        raise ToolContractError("tool_path_invalid")
    return value


class _StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, frozen=True, revalidate_instances="always"
    )


class _StrictResultModel(_StrictModel):
    @model_validator(mode="after")
    def bound_serialized_result(self) -> Self:
        if len(self.model_dump_json(warnings=False).encode("utf-8")) > MAX_TOOL_RESULT_BYTES:
            raise ToolContractError("tool_result_size_limit")
        return self


class ListFilesArguments(_StrictModel):
    name: Literal["list_files"]
    path: str = "."
    max_entries: int = Field(default=MAX_LIST_ENTRIES, ge=1, le=MAX_LIST_ENTRIES)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return normalize_source_path(value, allow_root=True)


class ReadFileArguments(_StrictModel):
    name: Literal["read_file"]
    path: str = Field(min_length=1, max_length=255)
    offset_bytes: int = Field(default=0, ge=0, le=MAX_SOURCE_FILE_BYTES)
    max_bytes: int = Field(default=MAX_READ_PAGE_BYTES, ge=1, le=MAX_READ_PAGE_BYTES)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return normalize_source_path(value)


class SearchTextArguments(_StrictModel):
    name: Literal["search_text"]
    query: str = Field(min_length=1, max_length=MAX_SEARCH_QUERY_BYTES)
    path_prefix: str | None = Field(default=None, max_length=255)
    max_matches: int = Field(default=MAX_SEARCH_MATCHES, ge=1, le=MAX_SEARCH_MATCHES)
    context_lines: int = Field(default=2, ge=0, le=MAX_SEARCH_CONTEXT_LINES)
    case_sensitive: bool = True
    max_scan_bytes: int = Field(default=MAX_SEARCH_SCAN_BYTES, ge=1, le=MAX_SEARCH_SCAN_BYTES)
    max_files_scanned: int = Field(
        default=MAX_SEARCH_FILES_SCANNED, ge=1, le=MAX_SEARCH_FILES_SCANNED
    )

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        try:
            encoded = value.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            raise ToolContractError("tool_search_query_invalid") from None
        if "\0" in value or "\n" in value or "\r" in value or len(encoded) > MAX_SEARCH_QUERY_BYTES:
            raise ToolContractError("tool_search_query_invalid")
        return value

    @field_validator("path_prefix")
    @classmethod
    def validate_prefix(cls, value: str | None) -> str | None:
        return None if value is None else normalize_source_path(value, allow_root=True)


class TextEdit(_StrictModel):
    """Literal single-occurrence replacement; empty preimage is only for create/empty files."""

    path: str = Field(min_length=1, max_length=255)
    expected_text: str = Field(max_length=MAX_PATCH_TEXT_BYTES)
    replacement_text: str = Field(max_length=MAX_PATCH_TEXT_BYTES)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return normalize_source_path(value)

    @field_validator("expected_text", "replacement_text")
    @classmethod
    def validate_text(cls, value: str) -> str:
        try:
            encoded = value.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            raise ToolContractError("tool_patch_text_invalid") from None
        if "\0" in value or len(encoded) > MAX_PATCH_TEXT_BYTES:
            raise ToolContractError("tool_patch_text_invalid")
        return value

    @model_validator(mode="after")
    def require_change(self) -> TextEdit:
        if self.expected_text == self.replacement_text or (
            not self.expected_text and not self.replacement_text
        ):
            raise ToolContractError("tool_patch_edit_invalid")
        return self


class ApplyPatchArguments(_StrictModel):
    name: Literal["apply_patch"]
    edits: list[TextEdit] = Field(min_length=1, max_length=MAX_PATCH_FILES)

    @model_validator(mode="after")
    def bound_edit_set(self) -> ApplyPatchArguments:
        paths = [edit.path for edit in self.edits]
        if len({path.casefold() for path in paths}) != len(paths):
            raise ToolContractError("tool_patch_duplicate_path")
        total_bytes = sum(
            len(edit.expected_text.encode("utf-8")) + len(edit.replacement_text.encode("utf-8"))
            for edit in self.edits
        )
        if total_bytes > MAX_PATCH_TEXT_BYTES:
            raise ToolContractError("tool_patch_size_limit")
        return self


class DiffArguments(_StrictModel):
    name: Literal["diff"]
    path: str | None = Field(default=None, max_length=255)
    context_lines: int = Field(default=3, ge=0, le=MAX_DIFF_CONTEXT_LINES)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str | None) -> str | None:
        return None if value is None else normalize_source_path(value, allow_root=True)


type ToolCall = Annotated[
    ListFilesArguments
    | ReadFileArguments
    | SearchTextArguments
    | ApplyPatchArguments
    | DiffArguments,
    Field(discriminator="name"),
]
TOOL_CALL_ADAPTER: TypeAdapter[Any] = TypeAdapter(cast(Any, ToolCall))


class FileEntry(_StrictModel):
    path: str = Field(min_length=1, max_length=255)
    kind: Literal["file", "directory"]
    size_bytes: int = Field(ge=0, le=MAX_REPOSITORY_SOURCE_BYTES)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return normalize_source_path(value)


class ListFilesResult(_StrictResultModel):
    name: Literal["list_files"] = "list_files"
    path: str = Field(default=".", min_length=1, max_length=255)
    entries: list[FileEntry] = Field(max_length=MAX_LIST_ENTRIES)
    truncated: bool

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return normalize_source_path(value, allow_root=True)


class ReadFileResult(_StrictResultModel):
    name: Literal["read_file"] = "read_file"
    path: str = Field(min_length=1, max_length=255)
    offset_bytes: int = Field(ge=0, le=MAX_SOURCE_FILE_BYTES)
    content: str = Field(max_length=MAX_READ_PAGE_BYTES)
    next_offset_bytes: int | None = Field(default=None, ge=0, le=MAX_SOURCE_FILE_BYTES)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return normalize_source_path(value)

    @field_validator("content")
    @classmethod
    def bound_content_bytes(cls, value: str) -> str:
        try:
            content_bytes = value.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            raise ToolContractError("tool_read_result_invalid") from None
        if len(content_bytes) > MAX_READ_PAGE_BYTES:
            raise ToolContractError("tool_read_result_size_limit")
        return value


class SearchMatch(_StrictModel):
    path: str = Field(min_length=1, max_length=255)
    line: int = Field(ge=1)
    snippet: str = Field(max_length=MAX_SEARCH_SNIPPET_CHARS)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return normalize_source_path(value)


class SearchTextResult(_StrictResultModel):
    name: Literal["search_text"] = "search_text"
    query: str = Field(min_length=1, max_length=MAX_SEARCH_QUERY_BYTES)
    matches: list[SearchMatch] = Field(max_length=MAX_SEARCH_MATCHES)
    skipped_binary_files: int = Field(ge=0, le=MAX_SEARCH_FILES_SCANNED)
    skipped_oversized_files: int = Field(ge=0, le=MAX_SEARCH_FILES_SCANNED)
    truncated: bool

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        if (
            "\0" in value
            or "\n" in value
            or "\r" in value
            or len(value.encode("utf-8")) > MAX_SEARCH_QUERY_BYTES
        ):
            raise ToolContractError("tool_search_result_invalid")
        return value


class ApplyPatchResult(_StrictResultModel):
    name: Literal["apply_patch"] = "apply_patch"
    candidate_archive_sha256: str = Field(pattern=_SHA256.pattern)
    candidate_tree_sha256: str = Field(pattern=_SHA256.pattern)
    changed_paths: list[str] = Field(max_length=MAX_PATCH_FILES)

    @field_validator("changed_paths")
    @classmethod
    def validate_changed_paths(cls, value: list[str]) -> list[str]:
        normalized = [normalize_source_path(path) for path in value]
        if len({path.casefold() for path in normalized}) != len(normalized):
            raise ToolContractError("tool_result_paths_invalid")
        return normalized


class DiffResult(_StrictResultModel):
    name: Literal["diff"] = "diff"
    text: str = Field(max_length=MAX_TOOL_RESULT_BYTES)
    changed_paths: list[str] = Field(max_length=MAX_PATCH_FILES)
    truncated: bool

    @field_validator("changed_paths")
    @classmethod
    def validate_changed_paths(cls, value: list[str]) -> list[str]:
        normalized = [normalize_source_path(path) for path in value]
        if len({path.casefold() for path in normalized}) != len(normalized):
            raise ToolContractError("tool_result_paths_invalid")
        return normalized

    @field_validator("text")
    @classmethod
    def bound_diff_bytes(cls, value: str) -> str:
        try:
            diff_bytes = value.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            raise ToolContractError("tool_diff_result_invalid") from None
        if len(diff_bytes) > MAX_TOOL_RESULT_BYTES:
            raise ToolContractError("tool_diff_result_size_limit")
        return value


type ToolResult = Annotated[
    ListFilesResult | ReadFileResult | SearchTextResult | ApplyPatchResult | DiffResult,
    Field(discriminator="name"),
]
TOOL_RESULT_ADAPTER: TypeAdapter[Any] = TypeAdapter(cast(Any, ToolResult))


def parse_tool_call(value: object) -> ToolCall:
    """Strictly validate a bounded JSON-shaped model call before dispatch."""
    try:
        encoded = (
            value.model_dump_json(warnings=False).encode("utf-8")
            if isinstance(value, BaseModel)
            else json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError):
        raise ToolContractError("tool_arguments_invalid") from None
    if len(encoded) > MAX_TOOL_ARGUMENT_BYTES:
        raise ToolContractError("tool_arguments_size_limit")
    try:
        return cast(ToolCall, TOOL_CALL_ADAPTER.validate_json(encoded, strict=True))
    except ValidationError:
        raise ToolContractError("tool_arguments_invalid") from None


def parse_tool_result(value: object) -> ToolResult:
    """Validate executor output and enforce the T08 accepted-result JSON ceiling."""
    try:
        encoded = (
            value.model_dump_json(warnings=False).encode("utf-8")
            if isinstance(value, BaseModel)
            else json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        if len(encoded) > MAX_TOOL_RESULT_BYTES:
            raise ToolContractError("tool_result_size_limit")
        result = TOOL_RESULT_ADAPTER.validate_json(encoded, strict=True)
    except ToolContractError:
        raise
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError, ValidationError):
        raise ToolContractError("tool_result_invalid") from None
    if len(result.model_dump_json(warnings=False).encode("utf-8")) > MAX_TOOL_RESULT_BYTES:
        raise ToolContractError("tool_result_size_limit")
    return cast(ToolResult, result)
