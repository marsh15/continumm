"""Strict tool schema and trusted policy tests for immutable source trees."""

from __future__ import annotations

import pytest

from continuum_api.tools.contracts import (
    MAX_READ_PAGE_BYTES,
    MAX_SOURCE_FILE_BYTES,
    MAX_TOOL_RESULT_BYTES,
    ApplyPatchArguments,
    DiffArguments,
    FileEntry,
    ListFilesArguments,
    ListFilesResult,
    ReadFileArguments,
    SearchTextArguments,
    TextEdit,
    ToolCapability,
    ToolContractError,
    ToolStage,
    normalize_source_path,
    parse_tool_call,
    parse_tool_result,
)
from continuum_api.tools.policy import (
    ToolPolicyContext,
    authorize_tool_call,
    check_tool_context_gate,
    is_conventional_test_path,
)

_TREE_SHA = "a" * 64
_BASELINE_PATHS = frozenset(
    {
        "src/app.py",
        "tests/test_original.py",
        "package.json",
        "package-lock.json",
        "scripts/check.js",
        ".env.local",
        ".pypirc",
        ".config/gcloud/application_default_credentials.json",
        "conftest.py",
    }
)
_CAPABILITIES = frozenset(ToolCapability)


def _context(**overrides: object) -> ToolPolicyContext:
    values: dict[str, object] = {
        "capabilities": _CAPABILITIES,
        "profile_id": "python-pytest",
        "profile_version": "1",
        "actor_can_write_repository": True,
        "app_can_access_repository": True,
        "stage": ToolStage.AGENT_WORK,
        "run_budget_available": True,
        "baseline_tree_sha256": _TREE_SHA,
        "baseline_source_paths": _BASELINE_PATHS,
        "baseline_test_paths": frozenset({"tests/test_original.py", "scripts/check.js"}),
        "baseline_paths_verified": True,
        "protected_paths": frozenset(),
    }
    values.update(overrides)
    return ToolPolicyContext(**values)  # type: ignore[arg-type]


def _source_sizes(paths: frozenset[str] = _BASELINE_PATHS) -> dict[str, int]:
    return {path: 20 for path in paths}


def _authorize(
    call: object,
    *,
    context: ToolPolicyContext | None = None,
    paths: frozenset[str] = _BASELINE_PATHS,
    sizes: dict[str, int] | None = None,
):
    return authorize_tool_call(
        call,
        context or _context(),
        paths,
        _source_sizes(paths) if sizes is None else sizes,
    )


def test_tool_call_union_is_strict_and_has_no_shell_or_network_variant() -> None:
    valid = parse_tool_call({"name": "search_text", "query": "needle"})
    assert isinstance(valid, SearchTextArguments)
    assert valid.query == "needle"

    with pytest.raises(ToolContractError, match="tool_arguments_invalid"):
        parse_tool_call({"name": "shell_exec", "command": "cat secret"})
    with pytest.raises(ToolContractError, match="tool_arguments_invalid"):
        parse_tool_call({"name": "read_file", "path": "src/app.py", "url": "https://x"})
    with pytest.raises(ToolContractError, match="tool_arguments_invalid"):
        parse_tool_call(
            {
                "name": "read_file",
                "path": "src/app.py",
                "capabilities": ["filesystem.apply_patch"],
                "actor_can_write_repository": True,
            }
        )
    with pytest.raises(ToolContractError, match="tool_arguments_invalid"):
        parse_tool_call({"name": "read_file", "path": 12})
    with pytest.raises(ToolContractError, match="tool_arguments_invalid"):
        parse_tool_call({"name": "read_file", "path": "../secret"})


def test_cheap_context_gate_has_no_path_authority_and_denies_before_source_access() -> None:
    allowed = check_tool_context_gate(
        {"name": "read_file", "path": "src/app.py"},
        _context(),
    )
    assert allowed.allowed
    assert allowed.code == "tool_context_allowed"
    assert allowed.readable_paths == frozenset()
    assert allowed.write_paths == frozenset()

    denied = check_tool_context_gate(
        {"name": "read_file", "path": "src/app.py"},
        _context(run_budget_available=False),
    )
    assert not denied.allowed
    assert denied.code == "tool_run_budget_exhausted"


@pytest.mark.parametrize(
    "path",
    [
        "../secret",
        "src/../../secret",
        "/etc/passwd",
        "C:/private.txt",
        "C:relative-drive.txt",
        "src\\private.py",
        "src//double.py",
        "src/./relative.py",
        "src/e\u0301.py",
        "src/\x00bad.py",
    ],
)
def test_source_paths_reject_traversal_absolute_and_ambiguous_forms(path: str) -> None:
    with pytest.raises(ToolContractError, match="tool_path_invalid"):
        normalize_source_path(path)


def test_node_24_root_level_test_patterns_are_baseline_protected() -> None:
    for path in (
        "test-entry.js",
        "test-entry.cjs",
        "test_entry.mjs",
        "runner-test.js",
        "runner_test.cjs",
        "suite.test.mjs",
    ):
        assert is_conventional_test_path(path)


def test_listing_can_name_large_source_assets_but_read_pages_remain_small() -> None:
    assert MAX_SOURCE_FILE_BYTES == 1024 * 1024
    assert MAX_READ_PAGE_BYTES == 16 * 1024
    # A listing entry reports source size and remains serializable under the
    # accepted-result ceiling even when the source itself is not readable.
    listed = ListFilesResult(
        entries=[FileEntry(path="assets/image.bin", kind="file", size_bytes=20 * 1024 * 1024)],
        truncated=False,
    )
    assert listed.entries[0].size_bytes == 20 * 1024 * 1024
    assert len(parse_tool_result(listed).model_dump_json().encode()) <= MAX_TOOL_RESULT_BYTES


def test_allowed_file_tools_receive_only_trusted_paths_and_baseline_identity() -> None:
    list_decision = _authorize(ListFilesArguments(name="list_files"))
    assert list_decision.allowed
    assert list_decision.baseline_tree_sha256 == _TREE_SHA
    assert ".env.local" not in list_decision.readable_paths
    assert ".pypirc" not in list_decision.readable_paths
    assert ".config/gcloud/application_default_credentials.json" not in list_decision.readable_paths
    assert "src/app.py" in list_decision.readable_paths

    read_decision = _authorize(ReadFileArguments(name="read_file", path="src/app.py"))
    assert read_decision.allowed
    assert read_decision.write_paths == frozenset()

    literal_search = _authorize(SearchTextArguments(name="search_text", query="(?i)token"))
    assert literal_search.allowed
    assert literal_search.readable_paths == list_decision.readable_paths

    diff_decision = _authorize(DiffArguments(name="diff"))
    assert diff_decision.allowed
    assert diff_decision.write_paths == frozenset()


def test_read_of_secret_or_oversized_file_is_denied() -> None:
    for secret_path in (
        ".env.local",
        ".pypirc",
        ".config/gcloud/application_default_credentials.json",
    ):
        secret = _authorize(ReadFileArguments(name="read_file", path=secret_path))
        assert not secret.allowed
        assert secret.code == "tool_path_unavailable"

    sizes = _source_sizes()
    sizes["src/app.py"] = MAX_SOURCE_FILE_BYTES + 1
    large_file = _authorize(ReadFileArguments(name="read_file", path="src/app.py"), sizes=sizes)
    assert not large_file.allowed
    assert large_file.code == "tool_file_size_limit"


def test_patch_can_add_regression_test_but_not_change_baseline_or_harness() -> None:
    permitted = ApplyPatchArguments(
        name="apply_patch",
        edits=[
            TextEdit(path="src/app.py", expected_text="old", replacement_text="fixed"),
            TextEdit(
                path="tests/test_regression.py",
                expected_text="",
                replacement_text="def test_regression():\n    assert True\n",
            ),
        ],
    )
    allowed = _authorize(permitted)
    assert allowed.allowed
    assert allowed.write_paths == frozenset({"src/app.py", "tests/test_regression.py"})

    for protected_path in (
        "tests/test_original.py",
        "scripts/check.js",
        "package.json",
        "package-lock.json",
        "requirements.lock",
        "requirements-dev.txt",
        "constraints-test.txt",
        "pyproject.toml",
        "yarn.lock",
        ".github/workflows/ci.yml",
        "conftest.py",
        "Makefile",
        "Taskfile.yml",
        "scripts/run-tests.sh",
        "pytest.ini",
        "jest.config.js",
        "vite.config.ts",
        ".mocharc.json",
        "ava.config.js",
        "tap.config.js",
        "karma.conf.js",
        ".nycrc.json",
        "tsconfig.json",
        ".env.local",
    ):
        denied = _authorize(
            ApplyPatchArguments(
                name="apply_patch",
                edits=[TextEdit(path=protected_path, expected_text="x", replacement_text="y")],
            )
        )
        assert not denied.allowed, protected_path


@pytest.mark.parametrize(
    "context",
    [
        _context(capabilities=frozenset({ToolCapability.LIST_FILES})),
        _context(actor_can_write_repository=False),
        _context(app_can_access_repository=False),
        _context(stage=ToolStage.VALIDATION),
        _context(run_budget_available=False),
        _context(baseline_paths_verified=False),
        _context(baseline_test_paths=frozenset()),
        _context(profile_id="unsupported-profile"),
    ],
)
def test_tool_policy_is_intersection_of_capability_profile_access_stage_and_budget(
    context: ToolPolicyContext,
) -> None:
    denied = _authorize(ReadFileArguments(name="read_file", path="src/app.py"), context=context)
    assert not denied.allowed
    assert not denied.readable_paths


def test_write_path_case_collisions_and_inconsistent_source_metadata_fail_closed() -> None:
    edit = ApplyPatchArguments(
        name="apply_patch",
        edits=[TextEdit(path="SRC/app.py", expected_text="x", replacement_text="y")],
    )
    assert _authorize(edit).code == "tool_path_conflict"

    collided_paths = frozenset({"src/app.py", "SRC/App.py"})
    collided = _authorize(ListFilesArguments(name="list_files"), paths=collided_paths)
    assert not collided.allowed
    assert collided.code == "tool_source_tree_invalid"

    missing_size = _authorize(ListFilesArguments(name="list_files"), sizes={"src/app.py": 1})
    assert not missing_size.allowed
    assert missing_size.code == "tool_source_tree_invalid"


def test_pydantic_constructed_or_mutated_models_are_revalidated_before_policy() -> None:
    valid = ApplyPatchArguments(
        name="apply_patch",
        edits=[TextEdit(path="src/app.py", expected_text="old", replacement_text="new")],
    )
    object.__setattr__(valid, "edits", [*valid.edits, *valid.edits])
    with pytest.raises(ToolContractError, match="tool_arguments_invalid"):
        parse_tool_call(valid)

    forged_edit = TextEdit.model_construct(
        path="../secret", expected_text="", replacement_text="steal"
    )
    forged = ApplyPatchArguments.model_construct(name="apply_patch", edits=[forged_edit])
    with pytest.raises(ToolContractError, match="tool_arguments_invalid"):
        parse_tool_call(forged)

    forged_result = ListFilesResult.model_construct(entries=[], truncated=False)
    object.__setattr__(
        forged_result,
        "entries",
        [FileEntry.model_construct(path="../secret", kind="file", size_bytes=1)],
    )
    with pytest.raises(ToolContractError, match="tool_result_invalid"):
        parse_tool_result(forged_result)


def test_tool_results_cannot_exceed_the_accepted_result_json_ceiling() -> None:
    oversized = {
        "name": "search_text",
        "matches": [
            {"path": "src/app.py", "line_number": index + 1, "line": "x" * 8_192}
            for index in range(100)
        ],
        "truncated": True,
    }
    with pytest.raises(ToolContractError, match="tool_result_size_limit"):
        parse_tool_result(oversized)
