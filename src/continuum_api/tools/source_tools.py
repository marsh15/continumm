"""Bounded, pure file tools over T14-verified source archives.

The module never opens a repository path on the API host and never executes
source. Every operation revalidates the canonical archive bytes and manifest
pins before reading them. Edits produce a new candidate archive in memory;
durable checkpointing is deliberately a separate caller responsibility.
"""

from __future__ import annotations

import ast
import configparser
import difflib
import fnmatch
import hashlib
import json
import re
import shlex
import tomllib
import unicodedata
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import Literal

from continuum_api.artifacts.archives import (
    MAX_FILES,
    MAX_PATH_BYTES,
    ArchiveError,
    ArchiveFile,
    ArchiveManifest,
    VerifiedArchive,
    create_archive,
    verify_archive,
)
from continuum_api.tools.contracts import (
    ApplyPatchArguments,
    DiffArguments,
    FileEntry,
    ListFilesArguments,
    ReadFileArguments,
    SearchTextArguments,
    ToolContractError,
    ToolResult,
    parse_tool_call,
    parse_tool_result,
)
from continuum_api.tools.contracts import (
    ApplyPatchResult as ContractApplyPatchResult,
)
from continuum_api.tools.contracts import (
    DiffResult as ContractDiffResult,
)
from continuum_api.tools.contracts import (
    ListFilesResult as ContractListFilesResult,
)
from continuum_api.tools.contracts import (
    ReadFileResult as ContractReadFileResult,
)
from continuum_api.tools.contracts import (
    SearchMatch as ContractSearchMatch,
)
from continuum_api.tools.contracts import (
    SearchTextResult as ContractSearchTextResult,
)
from continuum_api.tools.policy import (
    ToolPolicyContext,
    authorize_tool_call,
    check_tool_context_gate,
)

MAX_TOOL_OUTPUT_BYTES = 64 * 1024
MAX_LIST_ENTRIES = 100
MAX_READ_FILE_BYTES = 1024 * 1024
MAX_READ_PAGE_BYTES = 16 * 1024
MAX_SEARCH_QUERY_BYTES = 512
MAX_SEARCH_SOURCE_BYTES = 10 * 1024 * 1024
MAX_SEARCH_FILE_BYTES = 1024 * 1024
MAX_SEARCH_FILE_LINES = 20_000
MAX_SEARCH_TOTAL_LINES = 100_000
MAX_SEARCH_MATCHES = 100
MAX_SEARCH_FILES = 1_000
MAX_SEARCH_CONTEXT_LINES = 5
MAX_SEARCH_SNIPPET_CHARS = 128
MAX_PATCH_FILES = 10
MAX_PATCH_TEXT_BYTES = 64 * 1024
MAX_PATCH_FILE_BYTES = 1024 * 1024
MAX_DIFF_CONTEXT_LINES = 10
MAX_DIFF_FILE_BYTES = 1024 * 1024
MAX_DIFF_INPUT_BYTES = 2 * 1024 * 1024
MAX_DIFF_LINE_COUNT = 2_000
MAX_TEST_CONFIG_BYTES = 256 * 1024

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PYTHON_TEST_FILE = re.compile(r"^(?:test_[^/]+\.py|[^/]+_test\.py)$", re.IGNORECASE)
_NODE_TEST_FILE = re.compile(
    r"^(?:test(?:[-_][^/]+)?|[^/]+[-_]test|[^/]+\.(?:test|spec))\.(?:[cm]?[jt]sx?|[cm]ts)$",
    re.IGNORECASE,
)
_PYTHON_TEST_CONFIGS = frozenset(
    {"pyproject.toml", "pytest.ini", ".pytest.ini", "tox.ini", "setup.cfg"}
)
_NODE_TEST_CONFIGS = (
    "package.json",
    "package-lock.json",
    ".mocharc.*",
    "jest.config.*",
    "vitest.config.*",
    "vite.config.*",
    "ava.config.*",
    "tap.config.*",
    "karma.conf.*",
    "babel.config.*",
    "tsconfig*.json",
    ".nycrc*",
    "eslint.config.*",
    "webpack.config.*",
    "rollup.config.*",
)
_HARNESS_BASENAMES = frozenset(
    {
        "conftest.py",
        "pytest.ini",
        ".pytest.ini",
        "tox.ini",
        "setup.cfg",
        "setup.py",
        "makefile",
        "gnumakefile",
        "justfile",
        ".justfile",
        "taskfile.yml",
        "taskfile.yaml",
        "gruntfile.js",
        "gruntfile.cjs",
        "gruntfile.mjs",
        "gulpfile.js",
        "gulpfile.cjs",
        "gulpfile.mjs",
    }
)
_HARNESS_SCRIPT_SUFFIXES = (".sh", ".bash", ".ps1", ".cmd", ".bat")
_NODE_TEST_RUNNERS = frozenset(
    {
        "ava",
        "jasmine",
        "jest",
        "mocha",
        "node",
        "npx",
        "nyc",
        "tap",
        "tsx",
        "ts-node",
        "tsc",
        "uvu",
        "vitest",
        "web-test-runner",
        "npm",
        "sh",
        "bash",
        "make",
        "gmake",
        "just",
        "grunt",
        "gulp",
        "cross-env",
        "env",
        "prettier",
        "eslint",
    }
)


class SourceToolError(RuntimeError):
    """Stable, source-free error code for the in-memory tools."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    """Canonical archive bytes and their T14 identities."""

    archive: bytes
    archive_sha256: str
    tree_sha256: str
    manifest: ArchiveManifest


@dataclass(frozen=True, slots=True)
class SourceEntry:
    path: str
    kind: Literal["file", "directory"]
    size_bytes: int


@dataclass(frozen=True, slots=True)
class ListFilesResult:
    path: str
    entries: tuple[SourceEntry, ...]
    truncated: bool


@dataclass(frozen=True, slots=True)
class ReadFileResult:
    path: str
    offset_bytes: int
    content: str
    next_offset_bytes: int | None


@dataclass(frozen=True, slots=True)
class SearchMatch:
    path: str
    line: int
    snippet: str


@dataclass(frozen=True, slots=True)
class SearchTextResult:
    query: str
    matches: tuple[SearchMatch, ...]
    skipped_binary_files: int
    skipped_oversized_files: int
    truncated: bool


@dataclass(frozen=True, slots=True)
class TextEdit:
    path: str
    expected_text: str
    replacement_text: str


@dataclass(frozen=True, slots=True)
class PatchPreimage:
    path: str
    sha256: str | None


@dataclass(frozen=True, slots=True)
class PatchCandidate:
    """An internal candidate archive; it is not a durable artifact reference."""

    source: SourceSnapshot
    base_tree_sha256: str
    changed_paths: tuple[str, ...]
    preimages: tuple[PatchPreimage, ...]

    @property
    def archive_sha256(self) -> str:
        return self.source.archive_sha256

    @property
    def tree_sha256(self) -> str:
        return self.source.tree_sha256


@dataclass(frozen=True, slots=True)
class DiffResult:
    text: str
    changed_paths: tuple[str, ...]
    truncated: bool


@dataclass(frozen=True, slots=True)
class ToolExecutionResult:
    """Bounded model response plus internal, uncommitted candidate state."""

    result: ToolResult
    candidate: PatchCandidate | None


def source_paths(source: SourceSnapshot) -> tuple[str, ...]:
    """Return canonical source paths from a revalidated immutable manifest."""
    verified = _validate_snapshot(source)
    return tuple(entry.path for entry in verified.manifest.entries)


def source_file_sizes(source: SourceSnapshot) -> dict[str, int]:
    """Return path-to-size metadata from the revalidated T14 manifest."""
    verified = _validate_snapshot(source)
    return {entry.path: entry.size for entry in verified.manifest.entries}


def discover_baseline_test_paths(
    source: SourceSnapshot,
    profile_id: str,
    *,
    expected_tree_sha256: str,
) -> frozenset[str]:
    """Discover initial tests and their configured harness before tool access."""
    verified = _validate_snapshot(source)
    _require_digest(expected_tree_sha256, "source_tree_digest_invalid")
    if source.tree_sha256 != expected_tree_sha256:
        raise SourceToolError("source_tree_preimage_mismatch")
    files = {file.path: file for file in verified.files}
    if profile_id == "python-pytest":
        baseline = _discover_python_baseline(files)
    elif profile_id == "node-npm":
        baseline = _discover_node_baseline(files)
    else:
        raise SourceToolError("source_tool_profile_unsupported")
    if not baseline <= set(files):
        raise SourceToolError("source_tool_test_discovery_invalid")
    return frozenset(baseline)


def verify_source_archive(
    archive: bytes,
    *,
    expected_archive_sha256: str,
    expected_tree_sha256: str,
    expected_manifest: ArchiveManifest,
) -> SourceSnapshot:
    """Validate compressed bytes, tree digest, and the entire T14 manifest."""
    _require_digest(expected_archive_sha256, "source_archive_digest_invalid")
    _require_digest(expected_tree_sha256, "source_tree_digest_invalid")
    if not isinstance(expected_manifest, ArchiveManifest):
        raise SourceToolError("source_manifest_invalid")
    verified = _verify_archive_bytes(
        archive,
        expected_archive_sha256=expected_archive_sha256,
        expected_tree_sha256=expected_tree_sha256,
        expected_manifest=expected_manifest,
    )
    return SourceSnapshot(
        archive=archive,
        archive_sha256=verified.archive_sha256,
        tree_sha256=verified.manifest.tree_sha256,
        manifest=verified.manifest,
    )


def snapshot_from_verified_archive(
    verified: VerifiedArchive,
    *,
    expected_archive_sha256: str,
    expected_tree_sha256: str,
    expected_manifest: ArchiveManifest | None = None,
) -> SourceSnapshot:
    """Rebuild and recheck T15's verified bytes against their durable pins."""
    _require_digest(expected_archive_sha256, "source_archive_digest_invalid")
    _require_digest(expected_tree_sha256, "source_tree_digest_invalid")
    if not isinstance(verified, VerifiedArchive):
        raise SourceToolError("source_archive_invalid")
    manifest = expected_manifest or verified.manifest
    if verified.archive_sha256 != expected_archive_sha256 or verified.manifest != manifest:
        raise SourceToolError("source_archive_pin_mismatch")
    if verified.manifest.tree_sha256 != expected_tree_sha256:
        raise SourceToolError("source_tree_pin_mismatch")
    try:
        bundle = create_archive(verified.files)
    except ArchiveError as error:
        raise SourceToolError(error.code) from None
    if bundle.archive_sha256 != expected_archive_sha256 or bundle.manifest != manifest:
        raise SourceToolError("source_archive_pin_mismatch")
    return verify_source_archive(
        bundle.archive,
        expected_archive_sha256=expected_archive_sha256,
        expected_tree_sha256=expected_tree_sha256,
        expected_manifest=manifest,
    )


def verify_patch_candidate(
    baseline: VerifiedArchive,
    candidate: PatchCandidate,
) -> VerifiedArchive:
    """Revalidate a T19 candidate and its cumulative lineage against a T14 base."""
    if not isinstance(baseline, VerifiedArchive) or not isinstance(candidate, PatchCandidate):
        raise SourceToolError("source_tool_candidate_lineage_mismatch")
    try:
        baseline_bundle = create_archive(baseline.files)
        baseline_verified = verify_archive(
            baseline_bundle.archive,
            expected_archive_sha256=baseline.archive_sha256,
            expected_tree_sha256=baseline.manifest.tree_sha256,
            expected_manifest=baseline.manifest,
        )
    except ArchiveError as error:
        raise SourceToolError(error.code) from None
    _validate_candidate(
        baseline_verified,
        baseline_verified.manifest.tree_sha256,
        candidate,
    )
    return _validate_snapshot(candidate.source)


def list_files(
    source: SourceSnapshot,
    *,
    readable_paths: Collection[str],
    path: str = ".",
    max_entries: int = MAX_LIST_ENTRIES,
) -> ListFilesResult:
    """Trusted helper: readable_paths must come from a ToolPolicyDecision."""
    verified = _validate_snapshot(source)
    selected_prefix = _normalize_prefix(path)
    if type(max_entries) is not int or not 1 <= max_entries <= MAX_LIST_ENTRIES:
        raise SourceToolError("source_tool_list_limit_invalid")
    readable = _normalize_path_set(readable_paths, "source_tool_read_authorization_required")
    prefix = "" if selected_prefix == "." else selected_prefix
    children: dict[str, SourceEntry] = {}
    for file in verified.files:
        if file.path not in readable:
            continue
        if prefix and file.path != prefix and not file.path.startswith(prefix + "/"):
            continue
        if prefix and file.path == prefix:
            children[file.path] = SourceEntry(file.path, "file", len(file.content))
            continue
        relative = file.path[len(prefix) + 1 :] if prefix else file.path
        child_name, separator, _ = relative.partition("/")
        child_path = f"{prefix}/{child_name}" if prefix else child_name
        if separator:
            children.setdefault(child_path, SourceEntry(child_path, "directory", 0))
        else:
            children[child_path] = SourceEntry(child_path, "file", len(file.content))
    selected = sorted(children.values(), key=lambda item: item.path.encode("utf-8"))
    rows = tuple(selected[:max_entries])
    truncated = len(selected) > len(rows)
    while (
        rows
        and _json_size(_list_result_payload(selected_prefix, rows, truncated))
        > MAX_TOOL_OUTPUT_BYTES
    ):
        rows = rows[:-1]
        truncated = True
    return ListFilesResult(selected_prefix, rows, truncated)


def read_file(
    source: SourceSnapshot,
    path: str,
    *,
    readable_paths: Collection[str],
    offset_bytes: int = 0,
    max_bytes: int = MAX_READ_PAGE_BYTES,
) -> ReadFileResult:
    """Trusted helper: readable_paths must come from a ToolPolicyDecision."""
    verified = _validate_snapshot(source)
    selected_path = _normalize_path(path)
    readable = _normalize_path_set(readable_paths, "source_tool_read_authorization_required")
    if selected_path not in readable:
        raise SourceToolError("source_tool_read_not_authorized")
    if type(offset_bytes) is not int or offset_bytes < 0:
        raise SourceToolError("source_tool_read_offset_invalid")
    if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_READ_PAGE_BYTES:
        raise SourceToolError("source_tool_read_limit_invalid")
    file = _find_file(verified.files, selected_path)
    if file is None:
        raise SourceToolError("source_tool_file_not_found")
    if len(file.content) > MAX_READ_FILE_BYTES:
        raise SourceToolError("source_tool_file_too_large")
    _decode_text(file.content)
    if offset_bytes > len(file.content) or not _is_utf8_boundary(file.content, offset_bytes):
        raise SourceToolError("source_tool_read_offset_invalid")
    page_limit = min(max_bytes, MAX_READ_PAGE_BYTES)
    end = _utf8_page_end(file.content, offset_bytes, page_limit)
    page = file.content[offset_bytes:end].decode("utf-8", errors="strict")
    next_offset_bytes = end if end < len(file.content) else None
    payload = {
        "name": "read_file",
        "path": selected_path,
        "offset_bytes": offset_bytes,
        "content": page,
        "next_offset_bytes": next_offset_bytes,
    }
    while page and _json_size(payload) > MAX_TOOL_OUTPUT_BYTES:
        end = _utf8_page_end(file.content, offset_bytes, max(4, (end - offset_bytes) // 2))
        page = file.content[offset_bytes:end].decode("utf-8", errors="strict")
        payload["content"] = page
        next_offset_bytes = end if end < len(file.content) else None
        payload["next_offset_bytes"] = next_offset_bytes
    if _json_size(payload) > MAX_TOOL_OUTPUT_BYTES:
        raise SourceToolError("source_tool_output_limit")
    return ReadFileResult(
        path=selected_path,
        offset_bytes=offset_bytes,
        content=page,
        next_offset_bytes=next_offset_bytes,
    )


def search_text(
    source: SourceSnapshot,
    query: str,
    *,
    readable_paths: Collection[str],
    path_prefix: str | None = None,
    max_matches: int = MAX_SEARCH_MATCHES,
    context_lines: int = 2,
    case_sensitive: bool = True,
    max_scan_bytes: int = MAX_SEARCH_SOURCE_BYTES,
    max_files_scanned: int = MAX_SEARCH_FILES,
) -> SearchTextResult:
    """Trusted helper: bounds and readable_paths must come from parsed policy."""
    verified = _validate_snapshot(source)
    needle = _validated_query(query)
    if type(max_matches) is not int or not 1 <= max_matches <= MAX_SEARCH_MATCHES:
        raise SourceToolError("source_tool_search_limit_invalid")
    if type(context_lines) is not int or not 0 <= context_lines <= MAX_SEARCH_CONTEXT_LINES:
        raise SourceToolError("source_tool_search_context_invalid")
    if type(case_sensitive) is not bool:
        raise SourceToolError("source_tool_search_options_invalid")
    if type(max_scan_bytes) is not int or not 1 <= max_scan_bytes <= MAX_SEARCH_SOURCE_BYTES:
        raise SourceToolError("source_tool_search_limit_invalid")
    if type(max_files_scanned) is not int or not 1 <= max_files_scanned <= MAX_SEARCH_FILES:
        raise SourceToolError("source_tool_search_limit_invalid")
    prefix = _normalize_prefix(path_prefix) if path_prefix is not None else "."
    readable = _normalize_path_set(readable_paths, "source_tool_read_authorization_required")
    prefix_value = "" if prefix == "." else prefix
    matches: list[SearchMatch] = []
    scanned_bytes = 0
    skipped_binary = 0
    skipped_oversized = 0
    truncated = False
    scanned_lines = 0
    files_scanned = 0
    target = needle if case_sensitive else needle.casefold()
    for file in verified.files:
        if file.path not in readable:
            continue
        if (
            prefix_value
            and file.path != prefix_value
            and not file.path.startswith(prefix_value + "/")
        ):
            continue
        if files_scanned >= max_files_scanned:
            truncated = True
            break
        files_scanned += 1
        if len(file.content) > MAX_SEARCH_FILE_BYTES:
            skipped_oversized += 1
            truncated = True
            continue
        if scanned_bytes + len(file.content) > max_scan_bytes:
            truncated = True
            break
        scanned_bytes += len(file.content)
        try:
            text = _decode_text(file.content)
        except SourceToolError:
            skipped_binary += 1
            truncated = True
            continue
        line_count = _text_line_count(text)
        if (
            line_count > MAX_SEARCH_FILE_LINES
            or scanned_lines + line_count > MAX_SEARCH_TOTAL_LINES
        ):
            skipped_oversized += 1
            truncated = True
            continue
        scanned_lines += line_count
        lines = text.splitlines()
        for line_index, line in enumerate(lines):
            haystack = line if case_sensitive else line.casefold()
            if target not in haystack:
                continue
            first = max(0, line_index - context_lines)
            last = min(len(lines), line_index + context_lines + 1)
            snippet = _search_snippet(lines, line_index, first, last)
            match = SearchMatch(file.path, line_index + 1, snippet)
            candidate_matches = (*matches, match)
            payload = _search_result_payload(
                needle,
                candidate_matches,
                skipped_binary,
                skipped_oversized,
                False,
            )
            if len(candidate_matches) > max_matches or _json_size(payload) > MAX_TOOL_OUTPUT_BYTES:
                truncated = True
                break
            matches.append(match)
            if len(matches) >= max_matches:
                truncated = True
                break
        if truncated:
            break
    payload = _search_result_payload(
        needle,
        tuple(matches),
        skipped_binary,
        skipped_oversized,
        truncated,
    )
    if _json_size(payload) > MAX_TOOL_OUTPUT_BYTES:
        raise SourceToolError("source_tool_output_limit")
    return SearchTextResult(needle, tuple(matches), skipped_binary, skipped_oversized, truncated)


def apply_text_edits(
    source: SourceSnapshot,
    edits: Sequence[object],
    *,
    expected_tree_sha256: str,
    baseline_tree_sha256: str | None = None,
    authorized_write_paths: Collection[str],
) -> PatchCandidate:
    """Trusted helper; never pass model-supplied write paths as authorization."""
    verified = _validate_snapshot(source)
    _require_digest(expected_tree_sha256, "source_tree_digest_invalid")
    if expected_tree_sha256 != source.tree_sha256:
        raise SourceToolError("source_tree_preimage_mismatch")
    base_tree_sha256 = source.tree_sha256 if baseline_tree_sha256 is None else baseline_tree_sha256
    _require_digest(base_tree_sha256, "source_tree_digest_invalid")
    if not isinstance(edits, Sequence) or isinstance(edits, (str, bytes)):
        raise SourceToolError("source_tool_patch_invalid")
    if not 1 <= len(edits) <= MAX_PATCH_FILES:
        raise SourceToolError("source_tool_patch_limit_invalid")
    if not isinstance(authorized_write_paths, Collection) or isinstance(
        authorized_write_paths, (str, bytes)
    ):
        raise SourceToolError("source_tool_write_authorization_required")
    normalized_authorized = {_normalize_path(path) for path in authorized_write_paths}

    patch_bytes = 0
    seen_paths: set[str] = set()
    file_map = {file.path: file for file in verified.files}
    replacements: dict[str, ArchiveFile] = {}
    preimages: list[PatchPreimage] = []
    for edit in edits:
        path_value = getattr(edit, "path", None)
        expected_text = getattr(edit, "expected_text", None)
        replacement_text = getattr(edit, "replacement_text", None)
        if (
            not isinstance(path_value, str)
            or not isinstance(expected_text, str)
            or not isinstance(replacement_text, str)
        ):
            raise SourceToolError("source_tool_patch_invalid")
        if expected_text == replacement_text:
            raise SourceToolError("source_tool_patch_noop")
        path = _normalize_path(path_value)
        if path not in normalized_authorized:
            raise SourceToolError("source_tool_write_not_authorized")
        folded_path = path.casefold()
        if folded_path in seen_paths:
            raise SourceToolError("source_tool_patch_duplicate_path")
        seen_paths.add(folded_path)
        old_bytes = _encode_patch_text(expected_text)
        new_bytes = _encode_patch_text(replacement_text)
        patch_bytes += len(old_bytes) + len(new_bytes)
        if patch_bytes > MAX_PATCH_TEXT_BYTES:
            raise SourceToolError("source_tool_patch_text_limit")

        old_file = file_map.get(path)
        if expected_text == "":
            if old_file is not None and old_file.content:
                raise SourceToolError("source_tool_create_target_exists")
            if not new_bytes:
                raise SourceToolError("source_tool_empty_create")
            if len(new_bytes) > MAX_PATCH_FILE_BYTES:
                raise SourceToolError("source_tool_patch_file_limit")
            replacement = ArchiveFile(
                path,
                new_bytes,
                old_file.mode if old_file is not None else 0o644,
            )
            preimages.append(
                PatchPreimage(
                    path,
                    hashlib.sha256(old_file.content).hexdigest() if old_file is not None else None,
                )
            )
        else:
            if old_file is None:
                raise SourceToolError("source_tool_file_not_found")
            if len(old_file.content) > MAX_PATCH_FILE_BYTES:
                raise SourceToolError("source_tool_patch_file_limit")
            old_text = _decode_text(old_file.content)
            first_occurrence = old_text.find(expected_text)
            if first_occurrence < 0 or old_text.find(expected_text, first_occurrence + 1) >= 0:
                raise SourceToolError("source_tool_preimage_match_not_unique")
            new_text = (
                old_text[:first_occurrence]
                + replacement_text
                + old_text[first_occurrence + len(expected_text) :]
            )
            new_content = _encode_patch_text(new_text)
            if not new_content or len(new_content) > MAX_PATCH_FILE_BYTES:
                raise SourceToolError("source_tool_patch_file_limit")
            replacement = ArchiveFile(path, new_content, old_file.mode)
            preimages.append(PatchPreimage(path, hashlib.sha256(old_file.content).hexdigest()))
        replacements[path] = replacement

    candidate_files = [file for file in verified.files if file.path not in replacements]
    candidate_files.extend(replacements.values())
    try:
        bundle = create_archive(candidate_files)
    except ArchiveError as error:
        raise SourceToolError(error.code) from None
    candidate = verify_source_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    changed_paths = tuple(sorted(replacements, key=lambda item: item.encode("utf-8")))
    result_payload = {
        "name": "apply_patch",
        "archive_sha256": candidate.archive_sha256,
        "tree_sha256": candidate.tree_sha256,
        "changed_paths": changed_paths,
    }
    if _json_size(result_payload) > MAX_TOOL_OUTPUT_BYTES:
        raise SourceToolError("source_tool_output_limit")
    return PatchCandidate(candidate, base_tree_sha256, changed_paths, tuple(preimages))


def diff_sources(
    base: SourceSnapshot,
    current: SourceSnapshot,
    *,
    readable_paths: Collection[str],
    path: str | None = None,
    context_lines: int = 3,
) -> DiffResult:
    """Trusted helper: readable_paths must come from a ToolPolicyDecision."""
    base_verified = _validate_snapshot(base)
    current_verified = _validate_snapshot(current)
    if type(context_lines) is not int or not 0 <= context_lines <= MAX_DIFF_CONTEXT_LINES:
        raise SourceToolError("source_tool_diff_context_invalid")
    if path is None or path == ".":
        selected_path = None
    else:
        selected_path = _normalize_path(path)
    readable = _normalize_path_set(readable_paths, "source_tool_read_authorization_required")
    if selected_path is not None and selected_path not in readable:
        raise SourceToolError("source_tool_read_not_authorized")
    base_files = {file.path: file for file in base_verified.files}
    current_files = {file.path: file for file in current_verified.files}
    all_paths = set(base_files) | set(current_files)
    changed = [
        item
        for item in all_paths
        if item in readable
        if (selected_path is None or item == selected_path)
        and _file_content(base_files.get(item)) != _file_content(current_files.get(item))
    ]
    changed.sort(key=lambda item: item.encode("utf-8"))
    if selected_path is not None and selected_path not in all_paths:
        raise SourceToolError("source_tool_file_not_found")
    if len(changed) > MAX_PATCH_FILES:
        raise SourceToolError("source_tool_diff_file_limit")

    total_diff_input = 0
    line_count = 0
    output = bytearray()
    truncated = False
    for changed_path in changed:
        before_file = base_files.get(changed_path)
        after_file = current_files.get(changed_path)
        before_bytes = _file_content(before_file)
        after_bytes = _file_content(after_file)
        if before_bytes is None:
            before_bytes = b""
        if after_bytes is None:
            after_bytes = b""
        if len(before_bytes) > MAX_DIFF_FILE_BYTES or len(after_bytes) > MAX_DIFF_FILE_BYTES:
            raise SourceToolError("source_tool_diff_file_limit")
        total_diff_input += len(before_bytes) + len(after_bytes)
        if total_diff_input > MAX_DIFF_INPUT_BYTES:
            raise SourceToolError("source_tool_diff_input_limit")
        before_text = _decode_text(before_bytes) if before_bytes else ""
        after_text = _decode_text(after_bytes) if after_bytes else ""
        before_count = _text_line_count(before_text)
        after_count = _text_line_count(after_text)
        line_count += before_count + after_count
        if line_count > MAX_DIFF_LINE_COUNT:
            raise SourceToolError("source_tool_diff_input_limit")
        before_lines = before_text.splitlines(keepends=True)
        after_lines = after_text.splitlines(keepends=True)
        # unified_diff uses SequenceMatcher's bounded autojunk heuristic; input
        # bytes and line counts are capped above to keep worst-case work finite.
        diff_lines = difflib.unified_diff(
            before_lines,
            after_lines,
            fromfile=f"a/{changed_path}" if before_file is not None else "/dev/null",
            tofile=f"b/{changed_path}" if after_file is not None else "/dev/null",
            n=context_lines,
        )
        for line in diff_lines:
            encoded = line.encode("utf-8")
            remaining = MAX_TOOL_OUTPUT_BYTES - len(output)
            if len(encoded) > remaining:
                output.extend(_utf8_prefix(encoded, max(0, remaining)))
                truncated = True
                break
            output.extend(encoded)
        if truncated:
            break
    result = DiffResult(output.decode("utf-8", errors="strict"), tuple(changed), truncated)
    while _json_size(_diff_result_payload(result)) > MAX_TOOL_OUTPUT_BYTES:
        current_bytes = result.text.encode("utf-8")
        next_limit = max(0, len(current_bytes) // 2)
        if next_limit >= len(current_bytes):
            next_limit = max(0, len(current_bytes) - 1)
        clipped = _utf8_prefix(current_bytes, next_limit).decode("utf-8", errors="strict")
        result = DiffResult(clipped, tuple(changed), True)
    return result


def execute_tool_call(
    call_payload: object,
    context: ToolPolicyContext,
    baseline: SourceSnapshot,
    *,
    current_candidate: PatchCandidate | None = None,
) -> ToolExecutionResult:
    """Validate a model call, derive policy, and execute only over snapshots.

    This is the model-facing entry point. It accepts no allowed-path lists; the
    only permissions used below come from the server-owned ToolPolicyContext.
    """
    try:
        call = parse_tool_call(call_payload)
    except ToolContractError as error:
        raise SourceToolError(error.code) from None
    gate = check_tool_context_gate(call, context)
    if not gate.allowed:
        raise SourceToolError(gate.code)

    baseline_verified = _validate_snapshot(baseline)
    baseline_paths = frozenset(file.path for file in baseline_verified.files)
    if context.baseline_source_paths != baseline_paths:
        raise SourceToolError("source_tool_baseline_paths_mismatch")
    if context.baseline_tree_sha256 != baseline_verified.manifest.tree_sha256:
        raise SourceToolError("source_tool_baseline_tree_mismatch")
    discovered_tests = discover_baseline_test_paths(
        baseline,
        context.profile_id,
        expected_tree_sha256=baseline_verified.manifest.tree_sha256,
    )
    if not discovered_tests or not discovered_tests <= context.baseline_test_paths:
        raise SourceToolError("source_tool_baseline_tests_incomplete")
    if current_candidate is not None:
        _validate_candidate(baseline_verified, baseline.tree_sha256, current_candidate)
        current = current_candidate.source
    else:
        current = baseline
    current_verified = _validate_snapshot(current)
    paths = tuple(entry.path for entry in current_verified.manifest.entries)
    sizes = {entry.path: entry.size for entry in current_verified.manifest.entries}

    try:
        decision = authorize_tool_call(call, context, paths, sizes)
    except (ToolContractError, TypeError, ValueError):
        raise SourceToolError("source_tool_policy_failed") from None
    if not decision.allowed:
        raise SourceToolError(decision.code)
    if decision.baseline_tree_sha256 != baseline.tree_sha256:
        raise SourceToolError("source_tool_baseline_tree_mismatch")
    if not decision.readable_paths <= set(paths):
        raise SourceToolError("source_tool_policy_path_mismatch")

    candidate = current_candidate
    if isinstance(call, ListFilesArguments):
        listed = list_files(
            current,
            readable_paths=decision.readable_paths,
            path=call.path,
            max_entries=call.max_entries,
        )
        result: object = ContractListFilesResult(
            path=listed.path,
            entries=[
                FileEntry(path=item.path, kind=item.kind, size_bytes=item.size_bytes)
                for item in listed.entries
            ],
            truncated=listed.truncated,
        )
    elif isinstance(call, ReadFileArguments):
        read = read_file(
            current,
            call.path,
            readable_paths=decision.readable_paths,
            offset_bytes=call.offset_bytes,
            max_bytes=call.max_bytes,
        )
        result = ContractReadFileResult(
            path=read.path,
            offset_bytes=read.offset_bytes,
            content=read.content,
            next_offset_bytes=read.next_offset_bytes,
        )
    elif isinstance(call, SearchTextArguments):
        search = search_text(
            current,
            call.query,
            readable_paths=decision.readable_paths,
            path_prefix=call.path_prefix,
            max_matches=call.max_matches,
            context_lines=call.context_lines,
            case_sensitive=call.case_sensitive,
            max_scan_bytes=call.max_scan_bytes,
            max_files_scanned=call.max_files_scanned,
        )
        result = ContractSearchTextResult(
            query=search.query,
            matches=[
                ContractSearchMatch(path=item.path, line=item.line, snippet=item.snippet)
                for item in search.matches
            ],
            skipped_binary_files=search.skipped_binary_files,
            skipped_oversized_files=search.skipped_oversized_files,
            truncated=search.truncated,
        )
    elif isinstance(call, ApplyPatchArguments):
        per_call_candidate = apply_text_edits(
            current,
            call.edits,
            expected_tree_sha256=current.tree_sha256,
            baseline_tree_sha256=baseline.tree_sha256,
            authorized_write_paths=decision.write_paths,
        )
        candidate = _candidate_from_baseline(baseline_verified, baseline, per_call_candidate.source)
        result = ContractApplyPatchResult(
            candidate_archive_sha256=candidate.archive_sha256,
            candidate_tree_sha256=candidate.tree_sha256,
            changed_paths=list(candidate.changed_paths),
        )
    elif isinstance(call, DiffArguments):
        diff = diff_sources(
            baseline,
            current,
            readable_paths=decision.readable_paths,
            path=call.path,
            context_lines=call.context_lines,
        )
        result = ContractDiffResult(
            text=diff.text,
            changed_paths=list(diff.changed_paths),
            truncated=diff.truncated,
        )
    else:
        raise SourceToolError("source_tool_unsupported_call")

    try:
        parsed_result = parse_tool_result(result)
    except ToolContractError as error:
        raise SourceToolError(error.code) from None
    return ToolExecutionResult(parsed_result, candidate)


def _discover_python_baseline(files: dict[str, ArchiveFile]) -> set[str]:
    baseline: set[str] = set()
    configured_testpaths: list[str] = []
    configured_python_files: list[str] = []
    configured_addopts: list[str] = []
    for path, file in files.items():
        parts = tuple(part.casefold() for part in path.split("/"))
        basename = parts[-1]
        if _is_harness_path(path):
            baseline.add(path)
        if (
            any(part in {"test", "tests", "__tests__"} for part in parts[:-1])
            or _PYTHON_TEST_FILE.fullmatch(basename) is not None
            or basename == "conftest.py"
        ):
            baseline.add(path)
        if basename in _PYTHON_TEST_CONFIGS:
            baseline.add(path)
            if len(file.content) > MAX_TEST_CONFIG_BYTES:
                raise SourceToolError("source_tool_test_discovery_incomplete")
            if basename == "pyproject.toml":
                try:
                    project = tomllib.loads(_decode_text(file.content))
                except (tomllib.TOMLDecodeError, SourceToolError, RecursionError):
                    raise SourceToolError("source_tool_test_discovery_invalid") from None
                pytest_options = (
                    project.get("tool", {}).get("pytest", {}).get("ini_options", {})
                    if isinstance(project.get("tool", {}), dict)
                    else {}
                )
                if isinstance(pytest_options, dict):
                    configured_testpaths.extend(_string_values(pytest_options.get("testpaths")))
                    configured_python_files.extend(
                        _string_values(pytest_options.get("python_files"))
                    )
                    configured_addopts.extend(_string_values(pytest_options.get("addopts")))
            elif basename in {"pytest.ini", ".pytest.ini", "tox.ini", "setup.cfg"}:
                parser = configparser.ConfigParser(interpolation=None, strict=False)
                try:
                    parser.read_string(_decode_text(file.content))
                except (configparser.Error, SourceToolError):
                    raise SourceToolError("source_tool_test_discovery_invalid") from None
                for section in ("pytest", "tool:pytest"):
                    if parser.has_section(section):
                        configured_testpaths.extend(
                            _split_config_values(parser.get(section, "testpaths", fallback=""))
                        )
                        configured_python_files.extend(
                            _split_config_values(parser.get(section, "python_files", fallback=""))
                        )
                        configured_addopts.extend(
                            _split_config_values(parser.get(section, "addopts", fallback=""))
                        )
    normalized_testpaths = tuple(_clean_config_path(item) for item in configured_testpaths)
    if (
        len(normalized_testpaths) > 256
        or len(configured_python_files) > 256
        or len(configured_addopts) > 256
    ):
        raise SourceToolError("source_tool_test_discovery_incomplete")
    for pattern in configured_python_files:
        if (
            len(pattern.encode("utf-8")) > MAX_PATH_BYTES
            or sum(pattern.count(character) for character in "*?[]") > 16
        ):
            raise SourceToolError("source_tool_test_discovery_incomplete")
    for path in files:
        if any(
            prefix == "." or path == prefix or path.startswith(prefix + "/")
            for prefix in normalized_testpaths
        ):
            baseline.add(path)
        basename = path.rsplit("/", 1)[-1]
        if any(fnmatch.fnmatchcase(basename, pattern) for pattern in configured_python_files):
            baseline.add(path)
    plugin_names = _pytest_plugin_names(files, baseline, configured_addopts)
    for module in plugin_names:
        resolved = _resolve_python_module(module, files)
        if resolved is not None:
            baseline.add(resolved)
    return baseline


def _discover_node_baseline(files: dict[str, ArchiveFile]) -> set[str]:
    baseline: set[str] = set()
    for path in files:
        parts = tuple(part.casefold() for part in path.split("/"))
        basename = parts[-1]
        if _is_harness_path(path):
            baseline.add(path)
        if (
            any(part in {"test", "tests", "__tests__", "spec", "specs"} for part in parts[:-1])
            or _NODE_TEST_FILE.fullmatch(basename) is not None
            or basename in {"test.js", "test.cjs", "test.mjs", "spec.js", "spec.cjs", "spec.mjs"}
        ):
            baseline.add(path)
        if any(fnmatch.fnmatchcase(basename, pattern.casefold()) for pattern in _NODE_TEST_CONFIGS):
            baseline.add(path)

    package_file = files.get("package.json")
    if package_file is None:
        raise SourceToolError("source_tool_test_discovery_invalid")
    if len(package_file.content) > MAX_TEST_CONFIG_BYTES:
        raise SourceToolError("source_tool_test_discovery_incomplete")
    baseline.add("package.json")
    if "package-lock.json" in files:
        baseline.add("package-lock.json")
    try:
        package = json.loads(
            package_file.content,
            object_pairs_hook=_unique_json_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, SourceToolError, RecursionError):
        raise SourceToolError("source_tool_test_discovery_invalid") from None
    if not isinstance(package, dict):
        raise SourceToolError("source_tool_test_discovery_invalid")
    runner_configured = any(key in package for key in ("jest", "mocha", "ava", "tap", "nyc"))
    opaque_runner_config = any(
        path.rsplit("/", 1)[-1]
        .casefold()
        .startswith(
            (
                "jest.config.",
                "vitest.config.",
                "vite.config.",
                ".mocharc.",
                "ava.config.",
                "tap.config.",
                "karma.conf.",
            )
        )
        for path in files
    )
    if runner_configured or opaque_runner_config:
        # Framework configuration can select arbitrary project files as tests.
        # Pin executable source conservatively instead of guessing its glob.
        baseline.update(
            path
            for path in files
            if path.casefold().endswith(
                (".js", ".cjs", ".mjs", ".jsx", ".ts", ".cts", ".mts", ".tsx")
            )
        )
    scripts = package.get("scripts", {})
    if not isinstance(scripts, dict):
        raise SourceToolError("source_tool_test_discovery_invalid")
    test_script = scripts.get("test")
    if not isinstance(test_script, str) or not test_script.strip():
        raise SourceToolError("source_tool_test_discovery_invalid")
    try:
        if len(test_script.encode("utf-8", errors="strict")) > 16 * 1024:
            raise SourceToolError("source_tool_test_discovery_incomplete")
    except UnicodeEncodeError:
        raise SourceToolError("source_tool_test_discovery_invalid") from None
    _scan_node_test_command(test_script, files, scripts, baseline, set(), 0)
    return baseline


def _is_harness_path(path: str) -> bool:
    parts = tuple(part.casefold() for part in path.split("/"))
    basename = parts[-1]
    return (
        basename in _HARNESS_BASENAMES
        or basename.endswith(_HARNESS_SCRIPT_SUFFIXES)
        or parts[0] in {".github", ".circleci", ".buildkite", ".azure-pipelines"}
    )


def _pytest_plugin_names(
    files: dict[str, ArchiveFile], baseline: set[str], configured_addopts: Sequence[str]
) -> set[str]:
    names: set[str] = set()
    for index, token in enumerate(configured_addopts):
        if token in {"--doctest-modules", "--doctest-glob"} or token.startswith("--doctest-glob="):
            raise SourceToolError("source_tool_test_discovery_incomplete")
        if token == "-p":
            if index + 1 >= len(configured_addopts):
                raise SourceToolError("source_tool_test_discovery_incomplete")
            names.add(configured_addopts[index + 1])
        elif token.startswith("-p") and token != "-p":
            names.add(token[2:])
    python_files = {
        path
        for path in baseline
        if path.endswith(".py")
        and (
            path.rsplit("/", 1)[-1].casefold() == "conftest.py"
            or _PYTHON_TEST_FILE.fullmatch(path.rsplit("/", 1)[-1]) is not None
            or any(
                part.casefold() in {"test", "tests", "__tests__"} for part in path.split("/")[:-1]
            )
        )
    }
    total_bytes = 0
    if len(python_files) > 1_000:
        raise SourceToolError("source_tool_test_discovery_incomplete")
    for path in python_files:
        content = files[path].content
        if len(content) > MAX_PATCH_FILE_BYTES:
            raise SourceToolError("source_tool_test_discovery_incomplete")
        total_bytes += len(content)
        if total_bytes > 8 * 1024 * 1024:
            raise SourceToolError("source_tool_test_discovery_incomplete")
        try:
            module = ast.parse(_decode_text(content), filename=path)
        except (SyntaxError, SourceToolError):
            # Invalid tests cannot import a plugin at runtime, and are already
            # immutable baseline files.
            continue
        for node in ast.walk(module):
            targets: list[ast.expr] = []
            if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == "pytest_plugins"
                for target in node.targets
            ):
                targets.append(node.value)
            elif (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id == "pytest_plugins"
            ):
                if node.value is not None:
                    targets.append(node.value)
            for value in targets:
                literals = _literal_plugin_list(value)
                if literals is None:
                    raise SourceToolError("source_tool_test_discovery_incomplete")
                names.update(literals)
    return names


def _literal_plugin_list(value: ast.expr) -> tuple[str, ...] | None:
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return (value.value,)
    if isinstance(value, (ast.List, ast.Tuple, ast.Set)):
        names: list[str] = []
        for item in value.elts:
            if not isinstance(item, ast.Constant) or not isinstance(item.value, str):
                return None
            names.append(item.value)
        return tuple(names)
    return None


def _resolve_python_module(module: str, files: dict[str, ArchiveFile]) -> str | None:
    if not isinstance(module, str) or len(module) > 512:
        raise SourceToolError("source_tool_test_discovery_incomplete")
    if module.startswith("no:"):
        return None
    module = module.removeprefix(".")
    components = module.split(".")
    if not components or any(
        not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in components
    ):
        raise SourceToolError("source_tool_test_discovery_incomplete")
    stem = "/".join(components)
    prefixes = ("", "src/", "tests/")
    for prefix in prefixes:
        for candidate in (f"{prefix}{stem}.py", f"{prefix}{stem}/__init__.py"):
            if candidate in files:
                return candidate
    if len(components) > 1:
        for prefix in prefixes:
            for end in range(1, len(components)):
                ancestor = f"{prefix}{'/'.join(components[:end])}/"
                if any(path.startswith(ancestor) for path in files):
                    raise SourceToolError("source_tool_test_discovery_incomplete")
    return None


def _scan_node_test_command(
    command: str,
    files: dict[str, ArchiveFile],
    scripts: dict[str, object],
    baseline: set[str],
    visited: set[str],
    depth: int,
) -> None:
    try:
        command_size = len(command.encode("utf-8", errors="strict"))
    except UnicodeEncodeError:
        raise SourceToolError("source_tool_test_discovery_incomplete") from None
    if depth > 8 or command_size > 16 * 1024:
        raise SourceToolError("source_tool_test_discovery_incomplete")
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|")
        lexer.whitespace_split = True
        lexer.commenters = ""
        tokens = list(lexer)
    except ValueError:
        raise SourceToolError("source_tool_test_discovery_incomplete") from None
    if not tokens:
        raise SourceToolError("source_tool_test_discovery_incomplete")
    if len(tokens) > 256:
        raise SourceToolError("source_tool_test_discovery_incomplete")
    if any("$" in token or "`" in token for token in tokens):
        raise SourceToolError("source_tool_test_discovery_incomplete")
    groups: list[list[str]] = [[]]
    for token in tokens:
        if token in {"&&", "||", ";", "|"}:
            if not groups[-1]:
                raise SourceToolError("source_tool_test_discovery_incomplete")
            groups.append([])
        elif token in {"&", "(", ")"} or any(
            token.startswith(operator) and token != operator for operator in ("&&", "||", ";", "|")
        ):
            raise SourceToolError("source_tool_test_discovery_incomplete")
        else:
            groups[-1].append(token)
    if len(groups) > 64 or any(not group for group in groups):
        raise SourceToolError("source_tool_test_discovery_incomplete")
    for group in groups:
        _scan_node_command_group(group, files, scripts, baseline, visited, depth)


def _scan_node_command_group(
    tokens: list[str],
    files: dict[str, ArchiveFile],
    scripts: dict[str, object],
    baseline: set[str],
    visited: set[str],
    depth: int,
) -> None:
    while tokens and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", tokens[0]):
        tokens = tokens[1:]
    if not tokens:
        raise SourceToolError("source_tool_test_discovery_incomplete")
    command = tokens[0].rsplit("/", 1)[-1]
    if command in {"env", "cross-env"}:
        index = 1
        while index < len(tokens) and ("=" in tokens[index] or tokens[index].startswith("--")):
            index += 1
        if index >= len(tokens):
            raise SourceToolError("source_tool_test_discovery_incomplete")
        return _scan_node_command_group(
            tokens[index:], files, scripts, baseline, visited, depth + 1
        )
    if command in {"npm", "npx"}:
        if len(tokens) >= 3 and tokens[1] == "run":
            name = tokens[2]
            if name in visited:
                raise SourceToolError("source_tool_test_discovery_incomplete")
            nested = scripts.get(name)
            if not isinstance(nested, str):
                raise SourceToolError("source_tool_test_discovery_incomplete")
            _scan_node_test_command(
                nested,
                files,
                scripts,
                baseline,
                visited | {name},
                depth + 1,
            )
            for token in tokens[3:]:
                _record_node_path_argument(token, files, baseline)
            return
        if command == "npx" and len(tokens) >= 2:
            return _scan_node_command_group(
                tokens[1:], files, scripts, baseline, visited, depth + 1
            )
        raise SourceToolError("source_tool_test_discovery_incomplete")
    if command in {"sh", "bash"}:
        if len(tokens) < 2:
            raise SourceToolError("source_tool_test_discovery_incomplete")
        script = _record_node_path_argument(tokens[1], files, baseline)
        if script is None or not script.endswith((".sh", ".bash")):
            raise SourceToolError("source_tool_test_discovery_incomplete")
        _scan_shell_harness(files[script].content, files, baseline, visited | {script}, depth + 1)
        return
    if command in {"make", "gmake"}:
        makefile = next(
            (path for path in ("GNUmakefile", "makefile", "Makefile") if path in files), None
        )
        if makefile is None:
            raise SourceToolError("source_tool_test_discovery_incomplete")
        baseline.add(makefile)
        _scan_make_harness(files[makefile].content, files, baseline, visited, depth + 1)
        for token in tokens[1:]:
            _record_node_path_argument(token, files, baseline)
        return
    if command not in _NODE_TEST_RUNNERS:
        raise SourceToolError("source_tool_test_discovery_incomplete")
    if command == "node":
        if any(
            token in {"-e", "--eval", "-p", "--print", "-r", "--require", "--import"}
            for token in tokens
        ):
            raise SourceToolError("source_tool_test_discovery_incomplete")
        if "--test" in tokens:
            test_index = tokens.index("--test")
            arguments = tokens[test_index + 1 :]
            index = 0
            value_options = {
                "--test-name-pattern",
                "--test-skip-pattern",
                "--test-concurrency",
                "--test-timeout",
            }
            while index < len(arguments):
                token = arguments[index]
                if token in value_options:
                    index += 2
                    continue
                if token.startswith("-"):
                    index += 1
                    continue
                _record_node_path_argument(token, files, baseline)
                index += 1
        else:
            entry = next((token for token in tokens[1:] if not token.startswith("-")), None)
            if entry is None:
                raise SourceToolError("source_tool_test_discovery_incomplete")
            if _record_node_path_argument(entry, files, baseline, resolve_directory=True) is None:
                raise SourceToolError("source_tool_test_discovery_incomplete")
    for token in tokens[1:]:
        if command != "node":
            _record_node_path_argument(token, files, baseline)


def _record_node_path_argument(
    token: str,
    files: dict[str, ArchiveFile],
    baseline: set[str],
    *,
    resolve_directory: bool = False,
) -> str | None:
    candidate = token.removeprefix("./")
    if candidate.startswith("-") or not candidate:
        return None
    try:
        candidate_size = len(candidate.encode("utf-8", errors="strict"))
    except UnicodeEncodeError:
        raise SourceToolError("source_tool_test_discovery_incomplete") from None
    if any(char in candidate for char in "*$?[]{}"):
        if (
            candidate_size > MAX_PATH_BYTES
            or sum(candidate.count(character) for character in "*?[]{}") > 16
        ):
            raise SourceToolError("source_tool_test_discovery_incomplete")
        if (
            candidate.startswith("/")
            or "\\" in candidate
            or any(component == ".." for component in candidate.split("/"))
        ):
            raise SourceToolError("source_tool_test_discovery_incomplete")
        patterns = [candidate]
        if candidate.startswith("**/"):
            patterns.append(candidate[3:])
        matches = [
            path
            for path in files
            if any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)
        ]
        if not matches or len(matches) > 1_000:
            raise SourceToolError("source_tool_test_discovery_incomplete")
        baseline.update(matches)
        return matches[0] if len(matches) == 1 else None
    if candidate in files:
        baseline.add(candidate)
        return candidate
    prefix = candidate.rstrip("/") + "/"
    children = [path for path in files if path.startswith(prefix)]
    if not children:
        if _looks_like_path_reference(candidate):
            raise SourceToolError("source_tool_test_discovery_incomplete")
        return None
    if not resolve_directory:
        baseline.update(children)
        return None
    entrypoint: str | None = None
    package_path = prefix + "package.json"
    if package_path in files:
        if len(files[package_path].content) > MAX_TEST_CONFIG_BYTES:
            raise SourceToolError("source_tool_test_discovery_incomplete")
        try:
            package = json.loads(files[package_path].content, object_pairs_hook=_unique_json_object)
        except (UnicodeDecodeError, json.JSONDecodeError, SourceToolError, RecursionError):
            raise SourceToolError("source_tool_test_discovery_incomplete") from None
        if not isinstance(package, dict):
            raise SourceToolError("source_tool_test_discovery_incomplete")
        main = package.get("main", "index.js")
        if not isinstance(main, str):
            raise SourceToolError("source_tool_test_discovery_incomplete")
        entrypoint = prefix + main.removeprefix("./")
    else:
        entrypoint = next(
            (
                prefix + name
                for name in ("index.js", "index.cjs", "index.mjs")
                if prefix + name in files
            ),
            None,
        )
    if entrypoint not in files:
        raise SourceToolError("source_tool_test_discovery_incomplete")
    baseline.add(entrypoint)
    return entrypoint


def _looks_like_path_reference(value: str) -> bool:
    return (
        "/" in value
        or "\\" in value
        or value.startswith((".", "~"))
        or value.casefold().endswith(
            (".js", ".cjs", ".mjs", ".jsx", ".ts", ".cts", ".mts", ".tsx", ".json", ".sh", ".bash")
        )
    )


def _scan_shell_harness(
    content: bytes,
    files: dict[str, ArchiveFile],
    baseline: set[str],
    visited: set[str],
    depth: int,
) -> None:
    if depth > 8 or len(content) > 64 * 1024:
        raise SourceToolError("source_tool_test_discovery_incomplete")
    try:
        text = _decode_text(content)
    except SourceToolError:
        raise SourceToolError("source_tool_test_discovery_incomplete") from None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-"):
            line = line[1:].lstrip()
        if line.startswith("@"):
            line = line[1:].lstrip()
        if "$" in line or "`" in line or "<<" in line:
            raise SourceToolError("source_tool_test_discovery_incomplete")
        _scan_node_test_command(line, files, {}, baseline, visited, depth + 1)


def _scan_make_harness(
    content: bytes,
    files: dict[str, ArchiveFile],
    baseline: set[str],
    visited: set[str],
    depth: int,
) -> None:
    if depth > 8 or len(content) > 64 * 1024:
        raise SourceToolError("source_tool_test_discovery_incomplete")
    try:
        text = _decode_text(content)
    except SourceToolError:
        raise SourceToolError("source_tool_test_discovery_incomplete") from None
    saw_recipe = False
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("."):
            continue
        if stripped.startswith(("include ", "-include ", "sinclude ")):
            raise SourceToolError("source_tool_test_discovery_incomplete")
        if not raw_line.startswith(("\t", " ")):
            # Target and variable declarations are configuration, already
            # immutable. Only statically visible recipe lines are inspected.
            continue
        line = stripped
        if line.startswith("-"):
            line = line[1:].lstrip()
        if line.startswith("@"):
            line = line[1:].lstrip()
        if "$" in line or "`" in line:
            raise SourceToolError("source_tool_test_discovery_incomplete")
        saw_recipe = True
        _scan_node_test_command(line, files, {}, baseline, visited, depth + 1)
    if not saw_recipe:
        raise SourceToolError("source_tool_test_discovery_incomplete")


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SourceToolError("source_tool_test_discovery_invalid")
        result[key] = value
    return result


def _string_values(value: object) -> list[str]:
    if isinstance(value, str):
        return _split_config_values(value)
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return [item for value_item in value for item in _split_config_values(value_item)]
    if value is None:
        return []
    raise SourceToolError("source_tool_test_discovery_invalid")


def _split_config_values(value: str) -> list[str]:
    return [piece for piece in re.split(r"[\s,]+", value.strip()) if piece]


def _clean_config_path(value: str) -> str:
    path = value.strip().removeprefix("./").rstrip("/")
    if path == ".":
        return "."
    if not path or any(character in path for character in "*?[]"):
        raise SourceToolError("source_tool_test_discovery_invalid")
    return _normalize_path(path)


def _search_snippet(lines: Sequence[str], focus: int, first: int, last: int) -> str:
    focused = _bounded_snippet(lines[focus], focus=True)
    if len(focused) > MAX_SEARCH_SNIPPET_CHARS:
        focused = focused[: MAX_SEARCH_SNIPPET_CHARS - 1] + "…"
    selected = {focus: focused}
    remaining = MAX_SEARCH_SNIPPET_CHARS - len(focused)
    context_indexes = sorted(
        (index for index in range(first, last) if index != focus),
        key=lambda index: (abs(index - focus), index),
    )
    for index in context_indexes:
        if remaining <= 1:
            break
        length = min(32, remaining - 1)
        text = lines[index]
        if len(text) > length:
            text = text[: max(0, length - 1)] + "…"
        if not text:
            continue
        selected[index] = text
        remaining -= len(text) + 1
    return "\n".join(selected[index] for index in sorted(selected))


def _validate_snapshot(source: SourceSnapshot) -> VerifiedArchive:
    if not isinstance(source, SourceSnapshot):
        raise SourceToolError("source_archive_invalid")
    _require_digest(source.archive_sha256, "source_archive_digest_invalid")
    _require_digest(source.tree_sha256, "source_tree_digest_invalid")
    return _verify_archive_bytes(
        source.archive,
        expected_archive_sha256=source.archive_sha256,
        expected_tree_sha256=source.tree_sha256,
        expected_manifest=source.manifest,
    )


def _validate_candidate(
    baseline: VerifiedArchive,
    baseline_tree_sha256: str,
    candidate: PatchCandidate,
) -> None:
    if (
        not isinstance(candidate, PatchCandidate)
        or candidate.base_tree_sha256 != baseline_tree_sha256
    ):
        raise SourceToolError("source_tool_candidate_lineage_mismatch")
    current = _validate_snapshot(candidate.source)
    before = {file.path: file for file in baseline.files}
    after = {file.path: file for file in current.files}
    if set(before) - set(after):
        raise SourceToolError("source_tool_candidate_lineage_mismatch")
    changed_paths = tuple(
        sorted(
            (
                path
                for path in after
                if path not in before
                or after[path].content != before[path].content
                or after[path].mode != before[path].mode
            ),
            key=lambda item: item.encode("utf-8"),
        )
    )
    if len(changed_paths) > MAX_PATCH_FILES or candidate.changed_paths != changed_paths:
        raise SourceToolError("source_tool_candidate_lineage_mismatch")
    if not changed_paths and candidate.source.tree_sha256 != baseline_tree_sha256:
        raise SourceToolError("source_tool_candidate_lineage_mismatch")
    preimages = {preimage.path: preimage.sha256 for preimage in candidate.preimages}
    if len(preimages) != len(candidate.preimages) or set(preimages) != set(changed_paths):
        raise SourceToolError("source_tool_candidate_lineage_mismatch")
    for path in changed_paths:
        original = before.get(path)
        current_file = after[path]
        if original is None:
            if preimages[path] is not None or current_file.mode != 0o644:
                raise SourceToolError("source_tool_candidate_lineage_mismatch")
        elif (
            preimages[path] != hashlib.sha256(original.content).hexdigest()
            or current_file.mode != original.mode
        ):
            raise SourceToolError("source_tool_candidate_lineage_mismatch")


def _candidate_from_baseline(
    baseline: VerifiedArchive,
    baseline_source: SourceSnapshot,
    candidate_source: SourceSnapshot,
) -> PatchCandidate:
    """Record cumulative candidate provenance against the immutable baseline."""
    candidate = _validate_snapshot(candidate_source)
    before = {file.path: file for file in baseline.files}
    after = {file.path: file for file in candidate.files}
    if set(before) - set(after):
        raise SourceToolError("source_tool_candidate_lineage_mismatch")
    changed_paths = tuple(
        sorted(
            (
                path
                for path in after
                if path not in before
                or after[path].content != before[path].content
                or after[path].mode != before[path].mode
            ),
            key=lambda item: item.encode("utf-8"),
        )
    )
    if len(changed_paths) > MAX_PATCH_FILES:
        raise SourceToolError("source_tool_patch_cumulative_limit")
    preimages = tuple(
        PatchPreimage(
            path,
            None if path not in before else hashlib.sha256(before[path].content).hexdigest(),
        )
        for path in changed_paths
    )
    return PatchCandidate(
        source=candidate_source,
        base_tree_sha256=baseline_source.tree_sha256,
        changed_paths=changed_paths,
        preimages=preimages,
    )


def _verify_archive_bytes(
    archive: bytes,
    *,
    expected_archive_sha256: str,
    expected_tree_sha256: str,
    expected_manifest: ArchiveManifest,
) -> VerifiedArchive:
    try:
        return verify_archive(
            archive,
            expected_archive_sha256=expected_archive_sha256,
            expected_tree_sha256=expected_tree_sha256,
            expected_manifest=expected_manifest,
        )
    except ArchiveError as error:
        raise SourceToolError(error.code) from None


def _find_file(files: Sequence[ArchiveFile], path: str) -> ArchiveFile | None:
    for file in files:
        if file.path == path:
            return file
    return None


def _normalize_path(path: str) -> str:
    if (
        not isinstance(path, str)
        or not path
        or path in {".", ".."}
        or path.startswith(("/", "\\"))
        or "\\" in path
        or "\0" in path
        or re.match(r"^[A-Za-z]:", path) is not None
        or unicodedata.normalize("NFC", path) != path
    ):
        raise SourceToolError("source_tool_path_invalid")
    try:
        if len(path.encode("utf-8", errors="strict")) > MAX_PATH_BYTES:
            raise SourceToolError("source_tool_path_invalid")
    except UnicodeError:
        raise SourceToolError("source_tool_path_invalid") from None
    components = path.split("/")
    if any(item in {"", ".", ".."} for item in components):
        raise SourceToolError("source_tool_path_invalid")
    return path


def _normalize_prefix(path: str) -> str:
    if path == ".":
        return "."
    if not isinstance(path, str):
        raise SourceToolError("source_tool_path_invalid")
    normalized = path[:-1] if path.endswith("/") else path
    return _normalize_path(normalized)


def _normalize_path_set(paths: Collection[str], error_code: str) -> set[str]:
    if not isinstance(paths, Collection) or isinstance(paths, (str, bytes)):
        raise SourceToolError(error_code)
    if len(paths) > MAX_FILES:
        raise SourceToolError("source_tool_path_limit")
    return {_normalize_path(path) for path in paths}


def _decode_text(content: bytes) -> str:
    if not isinstance(content, bytes) or b"\0" in content:
        raise SourceToolError("source_tool_binary_file")
    try:
        return content.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise SourceToolError("source_tool_text_encoding_invalid") from None


def _encode_patch_text(value: str) -> bytes:
    if not isinstance(value, str) or "\0" in value:
        raise SourceToolError("source_tool_patch_text_invalid")
    try:
        return value.encode("utf-8", errors="strict")
    except UnicodeError:
        raise SourceToolError("source_tool_patch_text_invalid") from None


def _validated_query(query: str) -> str:
    encoded = _encode_patch_text(query)
    if not encoded or len(encoded) > MAX_SEARCH_QUERY_BYTES or "\n" in query or "\r" in query:
        raise SourceToolError("source_tool_search_query_invalid")
    return query


def _is_utf8_boundary(content: bytes, offset: int) -> bool:
    if offset in {0, len(content)}:
        return True
    return content[offset] & 0xC0 != 0x80


def _utf8_page_end(content: bytes, offset: int, limit: int) -> int:
    end = min(len(content), offset + limit)
    while end > offset and end < len(content) and not _is_utf8_boundary(content, end):
        end -= 1
    if end == offset and offset < len(content):
        raise SourceToolError("source_tool_read_page_too_small")
    return end


def _bounded_snippet(line: str, *, focus: bool) -> str:
    if len(line) <= MAX_SEARCH_SNIPPET_CHARS:
        return line
    if not focus:
        return line[: MAX_SEARCH_SNIPPET_CHARS - 1] + "…"
    start = max(0, (len(line) - MAX_SEARCH_SNIPPET_CHARS) // 2)
    end = min(len(line), start + MAX_SEARCH_SNIPPET_CHARS)
    return ("…" if start else "") + line[start:end] + ("…" if end < len(line) else "")


def _text_line_count(text: str) -> int:
    if not text:
        return 0
    separators = sum(
        text.count(marker)
        for marker in ("\n", "\r", "\v", "\f", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029")
    )
    separators -= text.count("\r\n")
    if text.endswith(("\n", "\r", "\v", "\f", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029")):
        return separators
    return separators + 1


def _list_result_payload(
    path: str, entries: Sequence[SourceEntry], truncated: bool
) -> dict[str, object]:
    return {
        "name": "list_files",
        "path": path,
        "entries": [
            {"path": item.path, "kind": item.kind, "size_bytes": item.size_bytes}
            for item in entries
        ],
        "truncated": truncated,
    }


def _search_result_payload(
    query: str,
    matches: Sequence[SearchMatch],
    skipped_binary_files: int,
    skipped_oversized_files: int,
    truncated: bool,
) -> dict[str, object]:
    return {
        "name": "search_text",
        "query": query,
        "matches": [
            {"path": item.path, "line": item.line, "snippet": item.snippet} for item in matches
        ],
        "skipped_binary_files": skipped_binary_files,
        "skipped_oversized_files": skipped_oversized_files,
        "truncated": truncated,
    }


def _diff_result_payload(result: DiffResult) -> dict[str, object]:
    return {
        "name": "diff",
        "text": result.text,
        "changed_paths": list(result.changed_paths),
        "truncated": result.truncated,
    }


def _json_size(value: object) -> int:
    return len(json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("utf-8"))


def _file_content(file: ArchiveFile | None) -> bytes | None:
    return file.content if file is not None else None


def _utf8_prefix(content: bytes | bytearray, limit: int) -> bytes:
    prefix = bytes(content[:limit])
    while prefix:
        try:
            prefix.decode("utf-8", errors="strict")
            return prefix
        except UnicodeDecodeError as error:
            if error.reason != "unexpected end of data" or error.start < len(prefix) - 4:
                prefix = prefix[: error.start]
                break
            prefix = prefix[: error.start]
    return prefix


def _require_digest(value: str, code: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise SourceToolError(code)
