"""Image-side runner for fixed Python hidden input probes."""

from __future__ import annotations

import base64
import contextlib
import importlib.util
import json
import math
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

_WORKSPACE = Path("/vercel/sandbox/workspace")
_DEPS = Path("/opt/continuum/deps")
_MAX_INPUT_BYTES = 65_536
_MAX_CHUNK_CHARS = 4096
_MAX_CHUNKS = 32
_SAFE_PATH = re.compile(r"^[A-Za-z0-9_./-]+$")


class _DiscardText:
    """Writable sink for candidate diagnostics with constant memory use."""

    def write(self, text: str) -> int:
        return len(text)

    def flush(self) -> None:
        return None


_DISCARD_TEXT = _DiscardText()


class ProbeFailure(Exception):
    """Internal stable failure code; never includes candidate-controlled data."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _parse_args(argv: Sequence[str]) -> tuple[str, str]:
    if len(argv) < 4 or argv[0:1] != ["--entry"] or argv[2:3] != ["--inputs-b64"]:
        raise ProbeFailure("probe_arguments_invalid")
    entry = argv[1]
    chunks = argv[3:]
    if not 1 <= len(chunks) <= _MAX_CHUNKS or any(
        not chunk or len(chunk) > _MAX_CHUNK_CHARS for chunk in chunks
    ):
        raise ProbeFailure("probe_arguments_invalid")
    if (
        not entry
        or "\x00" in entry
        or "\\" in entry
        or entry.startswith("/")
        or not _SAFE_PATH.fullmatch(entry)
        or any(part in {"", ".", ".."} for part in entry.split("/"))
        or not entry.endswith(".py")
    ):
        raise ProbeFailure("probe_entry_invalid")
    try:
        raw = base64.b64decode("".join(chunks), validate=True)
        if len(raw) > _MAX_INPUT_BYTES:
            raise ProbeFailure("probe_inputs_size_limit")
        inputs = json.loads(raw.decode("utf-8"))
    except ProbeFailure:
        raise
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise ProbeFailure("probe_inputs_invalid") from None
    if not isinstance(inputs, list) or not 1 <= len(inputs) <= 32:
        raise ProbeFailure("probe_inputs_invalid")
    return entry, json.dumps(inputs, ensure_ascii=False, separators=(",", ":"))


def _entry_file(workspace: Path, entry: str) -> Path:
    root = workspace.resolve(strict=True)
    target = root.joinpath(*entry.split("/"))
    current = root
    for part in entry.split("/"):
        current = current / part
        if current.is_symlink():
            raise ProbeFailure("probe_entry_invalid")
    try:
        resolved = target.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError):
        raise ProbeFailure("probe_entry_invalid") from None
    if not resolved.is_file() or resolved.suffix != ".py":
        raise ProbeFailure("probe_entry_invalid")
    return resolved


def _validate_json(value: Any, depth: int = 0, seen: set[int] | None = None) -> None:
    if depth > 16:
        raise ProbeFailure("probe_result_invalid")
    if value is None or isinstance(value, (bool, str)):
        if isinstance(value, str):
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                raise ProbeFailure("probe_result_invalid") from None
        return
    if isinstance(value, int) and not isinstance(value, bool):
        if abs(value) > 2**53 - 1:
            raise ProbeFailure("probe_result_invalid")
        return
    if isinstance(value, float):
        if not math.isfinite(value) or (value.is_integer() and abs(value) > 2**53 - 1):
            raise ProbeFailure("probe_result_invalid")
        return
    if not isinstance(value, (list, dict)):
        raise ProbeFailure("probe_result_invalid")
    active = seen if seen is not None else set()
    identity = id(value)
    if identity in active:
        raise ProbeFailure("probe_result_invalid")
    active.add(identity)
    try:
        if isinstance(value, list):
            for item in value:
                _validate_json(item, depth + 1, active)
        else:
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ProbeFailure("probe_result_invalid")
                _validate_json(key, depth + 1, active)
                _validate_json(item, depth + 1, active)
    finally:
        active.remove(identity)


def run(
    argv: Sequence[str] | None = None, *, workspace: Path = _WORKSPACE, deps: Path = _DEPS
) -> int:
    """Run a probe. Workspace/deps are injectable only for trusted harnesses."""
    try:
        entry, inputs_json = _parse_args(list(sys.argv[1:] if argv is None else argv))
        entry_file = _entry_file(workspace, entry)
        import_roots = [str(deps), str(workspace / "src"), str(workspace)]
        sys.path[:] = [
            *import_roots,
            *(path for path in sys.path if path not in import_roots),
        ]
        spec = importlib.util.spec_from_file_location("continuum_hidden_probe_entry", entry_file)
        if spec is None or spec.loader is None:
            raise ProbeFailure("probe_entry_invalid")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        with (
            contextlib.redirect_stdout(_DISCARD_TEXT),
            contextlib.redirect_stderr(_DISCARD_TEXT),
        ):
            spec.loader.exec_module(module)
            solve = getattr(module, "solve", None)
            if not callable(solve):
                raise ProbeFailure("probe_entry_invalid")
            values = [solve(item) for item in json.loads(inputs_json)]
        _validate_json(values)
        output = json.dumps(
            {"protocol_version": 1, "values": values},
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        if len(output.encode("utf-8")) > _MAX_INPUT_BYTES:
            raise ProbeFailure("probe_result_size_limit")
        sys.stdout.write(output)
        return 0
    except ProbeFailure as error:
        sys.stderr.write(error.code + "\n")
        return 2
    except Exception:
        sys.stderr.write("probe_execution_failed\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(run())
