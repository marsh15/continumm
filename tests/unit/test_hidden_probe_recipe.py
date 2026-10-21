from __future__ import annotations

import base64
import dataclasses

import pytest

from continuum_api.sandbox.hidden_probe_recipe import HiddenProbe
from continuum_api.validation.hidden_checks import HiddenCheckError


def test_python_and_node_recipes_are_fixed_and_carry_inputs_only() -> None:
    py = HiddenProbe("python-pytest", "1", "src/solution.py", '[ {"x": 1} ]')
    assert py.command_id == "python-hidden-probe-v1"
    assert py.inputs_json == '[{"x":1}]'
    assert py.argv() == (
        "/usr/local/bin/python3.13",
        "-I",
        "-S",
        "/opt/continuum/hidden_probe.py",
        "--entry",
        "src/solution.py",
        "--inputs-b64",
        base64.b64encode(b'[{"x":1}]').decode(),
    )
    node = HiddenProbe("node-npm", "1", "src/solution.cjs", "[null]")
    assert node.command_id == "node-hidden-probe-v1"
    assert node.argv()[:5] == (
        "/usr/local/bin/node",
        "--no-addons",
        "--disable-proto=throw",
        "/opt/continuum/hidden_probe.cjs",
        "--entry",
    )
    assert "expected" not in " ".join(py.argv()).lower()
    with pytest.raises(dataclasses.FrozenInstanceError):
        py.entry_path = "other.py"  # type: ignore[misc]


def test_chunks_obey_argument_and_chunk_caps() -> None:
    probe = HiddenProbe("python-pytest", "1", "solution.py", '["' + ("x" * 60_000) + '"]')
    argv = probe.argv()
    chunks = argv[argv.index("--inputs-b64") + 1 :]
    assert len(chunks) <= 32
    assert len(argv) <= 32
    assert all(1 <= len(chunk) <= 4096 for chunk in chunks)
    assert base64.b64decode("".join(chunks)).decode() == probe.inputs_json
    with pytest.raises(HiddenCheckError, match="hidden_json_size_limit"):
        HiddenProbe("python-pytest", "1", "solution.py", '["' + ("x" * 65_536) + '"]')


@pytest.mark.parametrize(
    "profile,version,path,inputs",
    [
        ("custom", "1", "solution.py", "[null]"),
        ([], "1", "solution.py", "[null]"),
        ("python-pytest", [], "solution.py", "[null]"),
        ("python-pytest", "2", "solution.py", "[null]"),
        ("python-pytest", "1", "../solution.py", "[null]"),
        ("python-pytest", "1", "/solution.py", "[null]"),
        ("python-pytest", "1", "dir\\solution.py", "[null]"),
        ("python-pytest", "1", "solution.cjs", "[null]"),
        ("node-npm", "1", "node_modules/solution.cjs", "[null]"),
        ("python-pytest", "1", "solution.py", "{}"),
        ("python-pytest", "1", "solution.py", "[]"),
        ("python-pytest", "1", "solution.py", "[null,]"),
        ("python-pytest", "1", "solution.py", "[1e999]"),
    ],
)
def test_recipe_rejects_unsupported_or_malformed_values(
    profile: object, version: object, path: str, inputs: str
) -> None:
    with pytest.raises(HiddenCheckError):
        HiddenProbe(profile, version, path, inputs)
