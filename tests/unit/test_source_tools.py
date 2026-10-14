"""Bounded source tools operate only on pinned in-memory source archives."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from continuum_api.artifacts.archives import ArchiveFile, create_archive, verify_archive
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import ToolPolicyContext
from continuum_api.tools.source_tools import (
    MAX_TOOL_OUTPUT_BYTES,
    PatchCandidate,
    PatchPreimage,
    SourceSnapshot,
    SourceToolError,
    apply_text_edits,
    diff_sources,
    discover_baseline_test_paths,
    execute_tool_call,
    read_file,
    search_text,
    snapshot_from_verified_archive,
    source_paths,
    verify_patch_candidate,
    verify_source_archive,
)
from continuum_api.tools.source_tools import (
    TextEdit as SourceTextEdit,
)


def _snapshot(*files: ArchiveFile) -> SourceSnapshot:
    bundle = create_archive(files)
    return verify_source_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )


def _python_snapshot(*extra: ArchiveFile) -> SourceSnapshot:
    return _snapshot(
        ArchiveFile(
            "pyproject.toml",
            b"[tool.pytest.ini_options]\ntestpaths = ['tests']\naddopts = '-ra'\n",
        ),
        ArchiveFile("src/app.py", b"def greet():\n    return 'hello'\n"),
        ArchiveFile("tests/test_original.py", b"from src.app import greet\n"),
        *extra,
    )


def _context(snapshot: SourceSnapshot, *, tests: frozenset[str] | None = None) -> ToolPolicyContext:
    discovered = discover_baseline_test_paths(
        snapshot,
        "python-pytest",
        expected_tree_sha256=snapshot.tree_sha256,
    )
    return ToolPolicyContext(
        capabilities=frozenset(ToolCapability),
        profile_id="python-pytest",
        profile_version="1",
        actor_can_write_repository=True,
        app_can_access_repository=True,
        stage=ToolStage.AGENT_WORK,
        run_budget_available=True,
        baseline_tree_sha256=snapshot.tree_sha256,
        baseline_source_paths=frozenset(source_paths(snapshot)),
        baseline_test_paths=tests if tests is not None else discovered,
        baseline_paths_verified=True,
        protected_paths=frozenset(),
    )


def test_archive_pins_and_manifest_are_reverified_before_source_use() -> None:
    snapshot = _python_snapshot()
    t14_verified = verify_archive(
        snapshot.archive,
        expected_archive_sha256=snapshot.archive_sha256,
        expected_tree_sha256=snapshot.tree_sha256,
        expected_manifest=snapshot.manifest,
    )
    verified = snapshot_from_verified_archive(
        t14_verified,
        expected_archive_sha256=snapshot.archive_sha256,
        expected_tree_sha256=snapshot.tree_sha256,
        expected_manifest=snapshot.manifest,
    )
    assert verified.tree_sha256 == snapshot.tree_sha256

    with pytest.raises(SourceToolError, match="archive_digest_mismatch"):
        verify_source_archive(
            snapshot.archive,
            expected_archive_sha256="0" * 64,
            expected_tree_sha256=snapshot.tree_sha256,
            expected_manifest=snapshot.manifest,
        )


def test_read_pages_are_utf8_aligned_and_binary_source_is_rejected() -> None:
    snapshot = _python_snapshot(
        ArchiveFile("src/unicode.txt", "a🙂b".encode()),
        ArchiveFile("src/blob.bin", b"\xff\x00"),
    )
    allowed = set(source_paths(snapshot))
    first = read_file(snapshot, "src/unicode.txt", readable_paths=allowed, max_bytes=3)
    assert first.content == "a"
    assert first.next_offset_bytes == 1
    second = read_file(
        snapshot,
        "src/unicode.txt",
        readable_paths=allowed,
        offset_bytes=1,
        max_bytes=4,
    )
    assert second.content == "🙂"
    assert second.next_offset_bytes == 5
    with pytest.raises(SourceToolError, match="source_tool_binary_file"):
        read_file(snapshot, "src/blob.bin", readable_paths=allowed)
    with pytest.raises(SourceToolError, match="source_tool_read_not_authorized"):
        read_file(snapshot, "src/app.py", readable_paths={"tests/test_original.py"})


def test_literal_search_obeys_file_and_byte_budgets_and_skips_binary() -> None:
    snapshot = _python_snapshot(
        ArchiveFile("src/a.txt", b"needle one\nneedle two\n"),
        ArchiveFile("src/b.bin", b"\xffneedle\n"),
        ArchiveFile("src/c.txt", b"needle three\n"),
    )
    all_paths = set(source_paths(snapshot))
    limited = search_text(
        snapshot,
        "needle",
        readable_paths=all_paths,
        path_prefix="src",
        max_matches=1,
        max_files_scanned=1,
    )
    assert len(limited.matches) == 1
    assert limited.truncated

    found = search_text(
        snapshot,
        "needle",
        readable_paths=all_paths,
        path_prefix="src",
        case_sensitive=False,
        context_lines=5,
    )
    assert found.skipped_binary_files == 1
    assert found.truncated
    assert all(len(match.snippet) <= 128 for match in found.matches)

    byte_limited = search_text(
        snapshot,
        "needle",
        readable_paths=all_paths,
        path_prefix="src",
        max_scan_bytes=1,
    )
    assert byte_limited.matches == ()
    assert byte_limited.truncated


def test_patch_is_atomic_exact_and_returns_only_a_new_verified_candidate() -> None:
    snapshot = _python_snapshot(
        ArchiveFile("src/empty.txt", b"", 0o755),
        ArchiveFile("src/ambiguous.txt", b"aaa"),
    )
    candidate = apply_text_edits(
        snapshot,
        [
            SourceTextEdit("src/app.py", "hello", "hi"),
            SourceTextEdit("src/new.py", "", "value = 1\n"),
            SourceTextEdit("src/empty.txt", "", "created"),
        ],
        expected_tree_sha256=snapshot.tree_sha256,
        authorized_write_paths={"src/app.py", "src/new.py", "src/empty.txt"},
    )
    assert isinstance(candidate, PatchCandidate)
    assert candidate.base_tree_sha256 == snapshot.tree_sha256
    assert set(candidate.changed_paths) == {"src/app.py", "src/new.py", "src/empty.txt"}
    assert snapshot.tree_sha256 != candidate.tree_sha256
    assert [item.sha256 for item in candidate.preimages if item.path == "src/new.py"] == [None]
    assert hashlib.sha256(b"").hexdigest() == next(
        item.sha256 for item in candidate.preimages if item.path == "src/empty.txt"
    )
    original_files = verify_archive(snapshot.archive).files
    assert b"hello" in next(file.content for file in original_files if file.path == "src/app.py")
    assert candidate.source.tree_sha256 == candidate.tree_sha256

    with pytest.raises(SourceToolError, match="source_tool_preimage_match_not_unique"):
        apply_text_edits(
            snapshot,
            [SourceTextEdit("src/ambiguous.txt", "aa", "b")],
            expected_tree_sha256=snapshot.tree_sha256,
            authorized_write_paths={"src/ambiguous.txt"},
        )

    with pytest.raises(SourceToolError, match="source_tool_patch_file_limit"):
        apply_text_edits(
            snapshot,
            [SourceTextEdit("src/app.py", "def greet():\n    return 'hello'\n", "")],
            expected_tree_sha256=snapshot.tree_sha256,
            authorized_write_paths={"src/app.py"},
        )

    with pytest.raises(SourceToolError, match="source_tree_preimage_mismatch"):
        apply_text_edits(
            snapshot,
            [SourceTextEdit("src/app.py", "hello", "hi")],
            expected_tree_sha256="0" * 64,
            authorized_write_paths={"src/app.py"},
        )

    with pytest.raises(SourceToolError, match="source_tool_file_not_found"):
        apply_text_edits(
            snapshot,
            [
                SourceTextEdit("src/app.py", "hello", "hi"),
                SourceTextEdit("src/missing.py", "missing", "x"),
            ],
            expected_tree_sha256=snapshot.tree_sha256,
            authorized_write_paths={"src/app.py", "src/missing.py"},
        )
    assert snapshot.tree_sha256 != candidate.tree_sha256


def test_patch_candidate_is_revalidated_against_baseline_and_recorded_preimages() -> None:
    snapshot = _python_snapshot()
    baseline = verify_archive(
        snapshot.archive,
        expected_archive_sha256=snapshot.archive_sha256,
        expected_tree_sha256=snapshot.tree_sha256,
        expected_manifest=snapshot.manifest,
    )
    candidate = apply_text_edits(
        snapshot,
        [SourceTextEdit("src/app.py", "return 'hello'", "return 'hi'")],
        expected_tree_sha256=snapshot.tree_sha256,
        authorized_write_paths={"src/app.py"},
    )

    verified = verify_patch_candidate(baseline, candidate)

    assert verified.archive_sha256 == candidate.source.archive_sha256
    assert candidate.changed_paths == ("src/app.py",)
    assert (
        candidate.preimages[0].sha256
        == hashlib.sha256(
            next(file.content for file in baseline.files if file.path == "src/app.py")
        ).hexdigest()
    )

    bad_preimage = replace(
        candidate,
        preimages=(PatchPreimage("src/app.py", "0" * 64),),
    )
    with pytest.raises(SourceToolError, match="source_tool_candidate_lineage_mismatch"):
        verify_patch_candidate(baseline, bad_preimage)


def test_dispatcher_derives_policy_filters_sensitive_paths_and_protects_baseline() -> None:
    snapshot = _python_snapshot(ArchiveFile(".env", b"SECRET=hidden\n"))
    context = _context(snapshot)
    listed = execute_tool_call({"name": "list_files"}, context, snapshot).result
    assert listed.name == "list_files"
    assert ".env" not in {item.path for item in listed.entries}

    insensitive = execute_tool_call(
        {
            "name": "search_text",
            "query": "HELLO",
            "path_prefix": "src",
            "case_sensitive": False,
        },
        context,
        snapshot,
    ).result
    assert insensitive.name == "search_text"
    assert insensitive.matches[0].path == "src/app.py"

    patched = execute_tool_call(
        {
            "name": "apply_patch",
            "edits": [
                {
                    "path": "src/app.py",
                    "expected_text": "hello",
                    "replacement_text": "world",
                }
            ],
        },
        context,
        snapshot,
    )
    assert patched.result.name == "apply_patch"
    assert patched.candidate is not None

    with pytest.raises(SourceToolError, match="tool_protected_path_denied"):
        execute_tool_call(
            {
                "name": "apply_patch",
                "edits": [
                    {
                        "path": "tests/test_original.py",
                        "expected_text": "greet",
                        "replacement_text": "broken",
                    }
                ],
            },
            context,
            snapshot,
        )

    incomplete = _context(snapshot, tests=frozenset({"pyproject.toml"}))
    with pytest.raises(SourceToolError, match="source_tool_baseline_tests_incomplete"):
        execute_tool_call({"name": "list_files"}, incomplete, snapshot)

    wrong_paths = ToolPolicyContext(
        capabilities=context.capabilities,
        profile_id=context.profile_id,
        profile_version=context.profile_version,
        actor_can_write_repository=context.actor_can_write_repository,
        app_can_access_repository=context.app_can_access_repository,
        stage=context.stage,
        run_budget_available=context.run_budget_available,
        baseline_tree_sha256=context.baseline_tree_sha256,
        baseline_source_paths=frozenset({"src/app.py", "tests/test_original.py"}),
        baseline_test_paths=frozenset({"tests/test_original.py"}),
        baseline_paths_verified=True,
        protected_paths=frozenset(),
    )
    with pytest.raises(SourceToolError, match="source_tool_baseline_paths_mismatch"):
        execute_tool_call({"name": "list_files"}, wrong_paths, snapshot)

    valid_candidate = patched.candidate
    assert valid_candidate is not None
    forged_candidate = PatchCandidate(
        source=valid_candidate.source,
        base_tree_sha256="0" * 64,
        changed_paths=valid_candidate.changed_paths,
        preimages=valid_candidate.preimages,
    )
    with pytest.raises(SourceToolError, match="source_tool_candidate_lineage_mismatch"):
        execute_tool_call(
            {"name": "list_files"},
            context,
            snapshot,
            current_candidate=forged_candidate,
        )


def test_dispatcher_tracks_cumulative_baseline_changes_across_patch_calls() -> None:
    snapshot = _python_snapshot()
    context = _context(snapshot)
    first = execute_tool_call(
        {
            "name": "apply_patch",
            "edits": [{"path": "src/app.py", "expected_text": "hello", "replacement_text": "hi"}],
        },
        context,
        snapshot,
    )
    assert first.candidate is not None
    assert first.candidate.changed_paths == ("src/app.py",)

    second = execute_tool_call(
        {
            "name": "apply_patch",
            "edits": [
                {"path": "src/app.py", "expected_text": "hi", "replacement_text": "world"},
                {"path": "src/new.py", "expected_text": "", "replacement_text": "value = 1\n"},
            ],
        },
        context,
        snapshot,
        current_candidate=first.candidate,
    )
    assert second.candidate is not None
    assert second.candidate.changed_paths == ("src/app.py", "src/new.py")
    baseline_app_sha = hashlib.sha256(
        next(
            file.content
            for file in verify_archive(snapshot.archive).files
            if file.path == "src/app.py"
        )
    ).hexdigest()
    assert {item.path: item.sha256 for item in second.candidate.preimages} == {
        "src/app.py": baseline_app_sha,
        "src/new.py": None,
    }

    third = execute_tool_call(
        {
            "name": "apply_patch",
            "edits": [
                {"path": "src/app.py", "expected_text": "world", "replacement_text": "hello"}
            ],
        },
        context,
        snapshot,
        current_candidate=second.candidate,
    )
    assert third.candidate is not None
    assert third.candidate.changed_paths == ("src/new.py",)
    assert {item.path for item in third.candidate.preimages} == {"src/new.py"}

    changed = execute_tool_call(
        {
            "name": "apply_patch",
            "edits": [
                {"path": "src/app.py", "expected_text": "hello", "replacement_text": "temporary"}
            ],
        },
        context,
        snapshot,
    )
    assert changed.candidate is not None
    reverted = execute_tool_call(
        {
            "name": "apply_patch",
            "edits": [
                {"path": "src/app.py", "expected_text": "temporary", "replacement_text": "hello"}
            ],
        },
        context,
        snapshot,
        current_candidate=changed.candidate,
    )
    assert reverted.result.name == "apply_patch"
    assert reverted.result.changed_paths == []
    assert reverted.candidate is not None
    assert reverted.candidate.changed_paths == ()
    assert reverted.candidate.preimages == ()
    assert reverted.candidate.tree_sha256 == snapshot.tree_sha256
    assert reverted.candidate.archive_sha256 == snapshot.archive_sha256


def test_diff_uses_only_readable_files_and_caps_json_escaped_control_text() -> None:
    baseline = _python_snapshot(ArchiveFile("src/control.txt", b"before\n"))
    candidate = apply_text_edits(
        baseline,
        [SourceTextEdit("src/control.txt", "before", "\x01" * 60_000)],
        expected_tree_sha256=baseline.tree_sha256,
        authorized_write_paths={"src/control.txt"},
    )
    diff = diff_sources(
        baseline,
        candidate.source,
        readable_paths={"src/control.txt"},
    )
    encoded = json.dumps(
        {
            "name": "diff",
            "text": diff.text,
            "changed_paths": list(diff.changed_paths),
            "truncated": diff.truncated,
        },
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode()
    assert diff.truncated
    assert len(encoded) <= MAX_TOOL_OUTPUT_BYTES
    assert diff.changed_paths == ("src/control.txt",)

    hidden = diff_sources(
        baseline,
        candidate.source,
        readable_paths={"src/app.py"},
    )
    assert hidden.text == ""
    assert hidden.changed_paths == ()


def test_baseline_discovery_pins_node_directory_entrypoints_and_custom_harnesses() -> None:
    node = _snapshot(
        ArchiveFile("package.json", b'{"scripts":{"test":"node src"}}'),
        ArchiveFile("package-lock.json", b"{}"),
        ArchiveFile("src/index.js", b"console.log('tests')\n"),
        ArchiveFile("src/test_plugin.js", b"module.exports = {}\n"),
    )
    discovered = discover_baseline_test_paths(
        node,
        "node-npm",
        expected_tree_sha256=node.tree_sha256,
    )
    assert "package.json" in discovered
    assert "package-lock.json" in discovered
    assert "src/index.js" in discovered

    shell_harness = _snapshot(
        ArchiveFile("package.json", b'{"scripts":{"test":"make test"}}'),
        ArchiveFile("package-lock.json", b"{}"),
        ArchiveFile("Makefile", b".PHONY: test\ntest:\n\t@sh scripts/run-tests.sh\n"),
        ArchiveFile("scripts/run-tests.sh", b"npx vitest run ./scripts/check.js\n"),
        ArchiveFile("scripts/check.js", b"export {}\n"),
        ArchiveFile("src/app.js", b"export const value = 1\n"),
        ArchiveFile("tests/app.test.js", b"test('ok', () => {})\n"),
    )
    shell_baseline = discover_baseline_test_paths(
        shell_harness,
        "node-npm",
        expected_tree_sha256=shell_harness.tree_sha256,
    )
    assert {"Makefile", "scripts/run-tests.sh", "scripts/check.js"} <= shell_baseline

    node_glob = _snapshot(
        ArchiveFile(
            "package.json", b'{"scripts":{"test":"node --test \\"**/*.integration.js\\""}}'
        ),
        ArchiveFile("package-lock.json", b"{}"),
        ArchiveFile("src/api.integration.js", b"test('api', () => {})\n"),
        ArchiveFile("src/app.js", b"export {}\n"),
    )
    glob_baseline = discover_baseline_test_paths(
        node_glob,
        "node-npm",
        expected_tree_sha256=node_glob.tree_sha256,
    )
    assert "src/api.integration.js" in glob_baseline

    node_default_tests = _snapshot(
        ArchiveFile("package.json", b'{"scripts":{"test":"node --test"}}'),
        ArchiveFile("package-lock.json", b"{}"),
        ArchiveFile("test-fast.cjs", b"test('fast', () => {})\n"),
        ArchiveFile("module-test.mjs", b"test('slow', () => {})\n"),
        ArchiveFile("test/data.json", b'{"expected":true}\n'),
    )
    default_tests = discover_baseline_test_paths(
        node_default_tests,
        "node-npm",
        expected_tree_sha256=node_default_tests.tree_sha256,
    )
    assert {"test-fast.cjs", "module-test.mjs", "test/data.json"} <= default_tests

    custom_jest = _snapshot(
        ArchiveFile("package.json", b'{"scripts":{"test":"jest"}}'),
        ArchiveFile("package-lock.json", b"{}"),
        ArchiveFile("jest.config.js", b"module.exports = { testMatch: ['**/*.integration.js'] }\n"),
        ArchiveFile("src/service.integration.js", b"test('service', () => {})\n"),
        ArchiveFile("src/app.js", b"export const value = 1\n"),
    )
    custom = discover_baseline_test_paths(
        custom_jest,
        "node-npm",
        expected_tree_sha256=custom_jest.tree_sha256,
    )
    assert {"jest.config.js", "src/service.integration.js", "src/app.js"} <= custom

    custom_vitest = _snapshot(
        ArchiveFile("package.json", b'{"scripts":{"test":"vitest run"}}'),
        ArchiveFile("package-lock.json", b"{}"),
        ArchiveFile(
            "vite.config.ts", b"export default { test: { include: ['src/*.integration.js'] } }\n"
        ),
        ArchiveFile("src/service.integration.js", b"test('service', () => {})\n"),
        ArchiveFile("src/app.js", b"export const value = 1\n"),
    )
    vitest_baseline = discover_baseline_test_paths(
        custom_vitest,
        "node-npm",
        expected_tree_sha256=custom_vitest.tree_sha256,
    )
    assert {"vite.config.ts", "src/service.integration.js", "src/app.js"} <= vitest_baseline

    unknown = _snapshot(
        ArchiveFile("package.json", b'{"scripts":{"test":"custom-runner src"}}'),
        ArchiveFile("package-lock.json", b"{}"),
        ArchiveFile("src/index.js", b"export {}\n"),
    )
    with pytest.raises(SourceToolError, match="source_tool_test_discovery_incomplete"):
        discover_baseline_test_paths(
            unknown,
            "node-npm",
            expected_tree_sha256=unknown.tree_sha256,
        )

    dynamic = _snapshot(
        ArchiveFile("package.json", b'{"scripts":{"test":"node --test $TEST_GLOB"}}'),
        ArchiveFile("package-lock.json", b"{}"),
        ArchiveFile("test/example.test.js", b"test('ok', () => {})\n"),
    )
    with pytest.raises(SourceToolError, match="source_tool_test_discovery_incomplete"):
        discover_baseline_test_paths(
            dynamic,
            "node-npm",
            expected_tree_sha256=dynamic.tree_sha256,
        )


def test_python_pytest_plugin_modules_are_part_of_immutable_baseline() -> None:
    plugin = _snapshot(
        ArchiveFile("pyproject.toml", b"[tool.pytest.ini_options]\ntestpaths = ['tests']\n"),
        ArchiveFile("tests/test_case.py", b"def test_case(): pass\n"),
        ArchiveFile("tests/conftest.py", b"pytest_plugins = ['src.test_plugin']\n"),
        ArchiveFile("src/test_plugin.py", b"def pytest_configure(config): pass\n"),
    )
    discovered = discover_baseline_test_paths(
        plugin,
        "python-pytest",
        expected_tree_sha256=plugin.tree_sha256,
    )
    assert "src/test_plugin.py" in discovered
    plugin_context = ToolPolicyContext(
        capabilities=frozenset(ToolCapability),
        profile_id="python-pytest",
        profile_version="1",
        actor_can_write_repository=True,
        app_can_access_repository=True,
        stage=ToolStage.AGENT_WORK,
        run_budget_available=True,
        baseline_tree_sha256=plugin.tree_sha256,
        baseline_source_paths=frozenset(source_paths(plugin)),
        baseline_test_paths=discovered,
        baseline_paths_verified=True,
        protected_paths=frozenset(),
    )
    with pytest.raises(SourceToolError, match="tool_protected_path_denied"):
        execute_tool_call(
            {
                "name": "apply_patch",
                "edits": [
                    {
                        "path": "src/test_plugin.py",
                        "expected_text": "pass",
                        "replacement_text": "raise RuntimeError()",
                    }
                ],
            },
            plugin_context,
            plugin,
        )

    dynamic = _snapshot(
        ArchiveFile("pyproject.toml", b"[tool.pytest.ini_options]\ntestpaths = ['tests']\n"),
        ArchiveFile("tests/test_case.py", b"def test_case(): pass\n"),
        ArchiveFile("tests/conftest.py", b"pytest_plugins = load_plugins()\n"),
    )
    with pytest.raises(SourceToolError, match="source_tool_test_discovery_incomplete"):
        discover_baseline_test_paths(
            dynamic,
            "python-pytest",
            expected_tree_sha256=dynamic.tree_sha256,
        )

    unresolved_local = _snapshot(
        ArchiveFile("pyproject.toml", b"[tool.pytest.ini_options]\ntestpaths = ['tests']\n"),
        ArchiveFile("tests/test_case.py", b"def test_case(): pass\n"),
        ArchiveFile("tests/conftest.py", b"pytest_plugins = ['src.local_plugin']\n"),
        ArchiveFile("src/app.py", b"def app(): pass\n"),
    )
    with pytest.raises(SourceToolError, match="source_tool_test_discovery_incomplete"):
        discover_baseline_test_paths(
            unresolved_local,
            "python-pytest",
            expected_tree_sha256=unresolved_local.tree_sha256,
        )

    doctest = _snapshot(
        ArchiveFile(
            "pyproject.toml",
            b"[tool.pytest.ini_options]\ntestpaths = ['tests']\naddopts = '--doctest-modules'\n",
        ),
        ArchiveFile("tests/test_case.py", b"def test_case(): pass\n"),
        ArchiveFile("src/app.py", b"def value():\n    return 1\n"),
    )
    with pytest.raises(SourceToolError, match="source_tool_test_discovery_incomplete"):
        discover_baseline_test_paths(
            doctest,
            "python-pytest",
            expected_tree_sha256=doctest.tree_sha256,
        )

    oversized_config = _snapshot(
        ArchiveFile(
            "pyproject.toml",
            b"[tool.pytest.ini_options]\ntestpaths = ['tests']\n#" + b"x" * (256 * 1024),
        ),
        ArchiveFile("tests/test_case.py", b"def test_case(): pass\n"),
    )
    with pytest.raises(SourceToolError, match="source_tool_test_discovery_incomplete"):
        discover_baseline_test_paths(
            oversized_config,
            "python-pytest",
            expected_tree_sha256=oversized_config.tree_sha256,
        )
