from __future__ import annotations

import base64
import json
import shutil
import subprocess
from pathlib import Path

import pytest

from continuum_api.sandbox.hidden_probe import run as run_python_probe

PYTHON_FIXTURE = (
    "import fractions\nimport random\nfrom dataclasses import dataclass\n"
    "@dataclass\nclass Value:\n    x: int\n"
    'def solve(value):\n    print("x" * 1000000)\n    '
    'return {"seen": Value(value).x, "fraction": str(fractions.Fraction(1, 3)), '
    '"random": random.Random(1).randint(1, 10)}\n'
)
NODE_FIXTURE = "exports.solve = (value) => ({ seen: value });\n"


def _chunks(inputs: str) -> list[str]:
    payload = base64.b64encode(inputs.encode()).decode()
    return [payload[offset : offset + 4096] for offset in range(0, len(payload), 4096)]


def test_python_bootstrap_runs_literal_fixture_and_emits_only_values(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "solution.py").write_text(PYTHON_FIXTURE, encoding="utf-8")
    inputs = '[{"n":1},null]'
    args = ["--entry", "solution.py", "--inputs-b64", *_chunks(inputs)]
    assert run_python_probe(args, workspace=workspace, deps=tmp_path / "deps") == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        "protocol_version": 1,
        "values": [
            {"seen": {"n": 1}, "fraction": "1/3", "random": 3},
            {"seen": None, "fraction": "1/3", "random": 3},
        ],
    }
    assert captured.err == ""


def test_python_bootstrap_suppresses_fixture_failure_details(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "solution.py").write_text(
        'def solve(value):\n    raise RuntimeError("private detail")\n', encoding="utf-8"
    )
    result = run_python_probe(
        ["--entry", "solution.py", "--inputs-b64", *_chunks("[1]")],
        workspace=workspace,
        deps=tmp_path / "deps",
    )
    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "probe_execution_failed\n"


def test_python_bootstrap_rejects_symlink_entry(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "fixture.py"
    outside.write_text(PYTHON_FIXTURE, encoding="utf-8")
    (workspace / "solution.py").symlink_to(outside)
    assert (
        run_python_probe(
            ["--entry", "solution.py", "--inputs-b64", *_chunks("[1]")],
            workspace=workspace,
            deps=tmp_path / "deps",
        )
        == 2
    )
    assert capsys.readouterr().err == "probe_entry_invalid\n"


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is unavailable")
def test_node_bootstrap_runs_only_literal_fixture(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "solution.cjs").write_text(NODE_FIXTURE, encoding="utf-8")
    runner = Path(__file__).parents[2] / "src/continuum_api/sandbox/hidden_probe.cjs"
    args = ["--entry", "solution.cjs", "--inputs-b64", *_chunks('[{"n":2}]')]
    script = (
        "const runner = require(process.argv[1]); "
        "process.exitCode = runner.run(JSON.parse(process.argv[2]), process.argv[3]);"
    )
    completed = subprocess.run(
        ["node", "-e", script, str(runner), json.dumps(args), str(workspace)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0
    assert json.loads(completed.stdout) == {"protocol_version": 1, "values": [{"seen": {"n": 2}}]}
    assert completed.stderr == ""


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is unavailable")
@pytest.mark.parametrize(
    "source",
    [
        "exports.solve = () => undefined;\n",
        "exports.solve = () => NaN;\n",
        "exports.solve = () => 1n;\n",
    ],
)
def test_node_bootstrap_rejects_non_json_values(tmp_path: Path, source: str) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "solution.cjs").write_text(source, encoding="utf-8")
    runner = Path(__file__).parents[2] / "src/continuum_api/sandbox/hidden_probe.cjs"
    args = ["--entry", "solution.cjs", "--inputs-b64", *_chunks("[null]")]
    script = (
        "process.exitCode = require(process.argv[1]).run("
        "JSON.parse(process.argv[2]), process.argv[3]);"
    )
    completed = subprocess.run(
        ["node", "-e", script, str(runner), json.dumps(args), str(workspace)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 2
    assert completed.stdout == ""
    assert completed.stderr == "probe_result_invalid\n"
