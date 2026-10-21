"""Twelve trusted fixture repairs through production archive/edit boundaries.

This measures deterministic harness behavior, not model coding ability. Only the
literal fixtures in this file execute locally; arbitrary repository code belongs
in the managed sandbox and is not accepted by this command.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from continuum_api.artifacts.archives import ArchiveFile, create_archive, verify_archive
from continuum_api.tools.source_tools import TextEdit, apply_text_edits, verify_source_archive


@dataclass(frozen=True)
class Case:
    case_id: str
    language: str
    before: str
    after: str
    checks: tuple[tuple[object, object], ...]


_REPAIRS = (
    ("double", "x", "x * 2", "x * 2", ((0, 0), (3, 6), (-4, -8))),
    ("absolute", "x", "abs(x)", "Math.abs(x)", ((-3, 3), (0, 0), (8, 8))),
    (
        "clamp",
        "x",
        "max(0, min(10, x))",
        "Math.max(0, Math.min(10, x))",
        ((-1, 0), (3, 3), (11, 10)),
    ),
    (
        "reverse",
        "x",
        "x[::-1]",
        "Array.from(x).reverse().join('')",
        (("ab", "ba"), ("", ""), ("abc", "cba")),
    ),
    ("count", "0", "len(x)", "x.length", (([], 0), ([1, 2], 2), ([0], 1))),
    ("even", "True", "x % 2 == 0", "x % 2 === 0", ((0, True), (2, True), (3, False))),
)


def corpus() -> tuple[Case, ...]:
    result = []
    for name, wrong, python, javascript, checks in _REPAIRS:
        for language, expression in (("python", python), ("node", javascript)):
            before_expr = "true" if language == "node" and wrong == "True" else wrong
            if language == "python":
                before = f"def solve(x):\n    return {before_expr}\n"
                after = f"def solve(x):\n    return {expression}\n"
            else:
                before = f"exports.solve = x => {before_expr};\n"
                after = f"exports.solve = x => {expression};\n"
            result.append(Case(f"{language}-{name}", language, before, after, checks))
    return tuple(result)


def _check(case: Case, directory: Path, source: bytes, hidden: Path) -> bool:
    extension = "py" if case.language == "python" else "cjs"
    target = directory / f"solution.{extension}"
    target.write_bytes(source)
    if case.language == "python":
        checker = hidden / "check.py"
        checker.write_text(
            "import importlib.util, json, sys\n"
            "spec=importlib.util.spec_from_file_location('candidate',sys.argv[1])\n"
            "module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)\n"
            f"checks=json.loads({json.dumps(json.dumps(case.checks))})\n"
            "assert all(module.solve(value)==expected for value,expected in checks)\n"
        )
        command = [sys.executable, "-I", str(checker), str(target)]
    else:
        node = shutil.which("node")
        if node is None:
            raise RuntimeError("node_required_for_corpus")
        checker = hidden / "check.cjs"
        checker.write_text(
            "const {solve}=require(process.argv[2]);\n"
            f"const checks={json.dumps(case.checks)};\n"
            "if(!checks.every(([x,y])=>JSON.stringify(solve(x))===JSON.stringify(y)))process.exit(1);\n"
        )
        command = [node, "--no-addons", "--disable-proto=throw", str(checker), str(target)]
    # Credentials/environment are never inherited by fixture subprocesses.
    result = subprocess.run(
        command,
        cwd=directory,
        env={"PATH": os.defpath},
        capture_output=True,
        timeout=5,
        check=False,
    )
    return result.returncode == 0


def run_case(case: Case) -> dict[str, object]:
    if case not in corpus():
        raise ValueError("only_trusted_literal_fixtures_may_execute_locally")
    started = time.perf_counter()
    path = "solution.py" if case.language == "python" else "solution.cjs"
    original_test = ArchiveFile("tests/original.txt", b"protected baseline fixture\n")
    bundle = create_archive((ArchiveFile(path, case.before.encode()), original_test))
    baseline = verify_source_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    candidate = apply_text_edits(
        baseline,
        [TextEdit(path, case.before, case.after)],
        expected_tree_sha256=baseline.tree_sha256,
        authorized_write_paths={path},
    )
    files = {item.path: item.content for item in verify_archive(candidate.source.archive).files}
    assert files[original_test.path] == original_test.content
    with tempfile.TemporaryDirectory(prefix="continuum-eval-") as temporary:
        root = Path(temporary)
        workspace = root / "workspace"
        hidden = root / "hidden"
        workspace.mkdir()
        hidden.mkdir()
        baseline_passed = _check(case, workspace, case.before.encode(), hidden)
        candidate_passed = _check(case, workspace, files[path], hidden)
    assert not baseline_passed, "a corpus baseline must expose its intended regression"
    assert candidate_passed
    return {
        "case_id": case.case_id,
        "language": case.language,
        "baseline_passed": baseline_passed,
        "candidate_passed": candidate_passed,
        "baseline_tree_sha256": baseline.tree_sha256,
        "candidate_tree_sha256": candidate.tree_sha256,
        "hidden_check_count": len(case.checks),
        "seconds": time.perf_counter() - started,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path(".artifacts/evaluation.json"))
    options = parser.parse_args()
    results = [run_case(case) for case in corpus()]
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "runner_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "working_tree_dirty": bool(
            subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
        ),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "python": platform.python_version(),
        "node": subprocess.check_output(
            [shutil.which("node") or "node", "--version"], text=True
        ).strip(),
        "corpus_sha256": hashlib.sha256(repr(corpus()).encode()).hexdigest(),
        "kind": "deterministic_fixture_pipeline",
        "model_calls": 0,
        "live_smokes": {
            "openai_python": "not_run",
            "openai_node": "not_run",
            "anthropic_python": "not_run",
            "anthropic_node": "not_run",
        },
        "limitations": [
            "scripted known repairs; no model quality score",
            "trusted fixtures execute locally; no managed sandbox measurement",
            "hidden checks are outside the writable workspace",
            "live protocol execution requires admitted runs, explicit opt-ins and service setup",
        ],
        "results": results,
    }
    options.report.parent.mkdir(parents=True, exist_ok=True)
    options.report.write_text(json.dumps(report, indent=2) + "\n")
    print(f"{len(results)} deterministic fixture repairs passed; live smokes not run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
