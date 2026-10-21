"""Fixed, non-configurable recipes for isolated hidden input probes."""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass, field
from pathlib import PurePosixPath

from continuum_api.validation.hidden_checks import HiddenCheckCase, HiddenCheckError

_MAX_INPUT_BYTES = 65_536
_CHUNK_CHARS = 4096
_MAX_ARGUMENTS = 32
_ALLOWED_PROFILE = {
    ("python-pytest", "1"): (".py", "python-hidden-probe-v1"),
    ("node-npm", "1"): (".cjs", "node-hidden-probe-v1"),
}
_SAFE_PATH = re.compile(r"^[A-Za-z0-9_./-]+$")


@dataclass(frozen=True, slots=True)
class HiddenProbe:
    """A fixed probe invocation containing only public inputs and an operator path."""

    profile_id: str
    profile_version: str
    entry_path: str
    inputs_json: str = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.profile_id, str) or not isinstance(self.profile_version, str):
            raise HiddenCheckError("hidden_probe_profile_unsupported")
        recipe = _ALLOWED_PROFILE.get((self.profile_id, self.profile_version))
        if recipe is None:
            raise HiddenCheckError("hidden_probe_profile_unsupported")
        suffix, _ = recipe
        path = self.entry_path
        if (
            not isinstance(path, str)
            or not path
            or "\x00" in path
            or "\\" in path
            or path.startswith("/")
            or _SAFE_PATH.fullmatch(path) is None
        ):
            raise HiddenCheckError("hidden_probe_entry_invalid")
        parts = path.split("/")
        if any(part in {"", ".", "..", ".git", "node_modules"} for part in parts):
            raise HiddenCheckError("hidden_probe_entry_invalid")
        if PurePosixPath(path).as_posix() != path or not path.endswith(suffix):
            raise HiddenCheckError("hidden_probe_entry_invalid")
        case = HiddenCheckCase("input", self.inputs_json, "null")
        try:
            inputs = json.loads(case.input_json)
        except (TypeError, ValueError):
            raise HiddenCheckError("hidden_probe_inputs_invalid") from None
        if not isinstance(inputs, list) or not 1 <= len(inputs) <= 32:
            raise HiddenCheckError("hidden_probe_inputs_invalid")
        encoded = case.input_json.encode("utf-8")
        if len(encoded) > _MAX_INPUT_BYTES:
            raise HiddenCheckError("hidden_probe_inputs_size_limit")
        object.__setattr__(self, "inputs_json", case.input_json)

    @property
    def command_id(self) -> str:
        return _ALLOWED_PROFILE[(self.profile_id, self.profile_version)][1]

    def argv(self) -> tuple[str, ...]:
        """Return the complete fixed executable and arguments for this profile."""
        payload = base64.b64encode(self.inputs_json.encode("utf-8")).decode("ascii")
        chunks = tuple(
            payload[offset : offset + _CHUNK_CHARS]
            for offset in range(0, len(payload), _CHUNK_CHARS)
        )
        if (self.profile_id, self.profile_version) == ("python-pytest", "1"):
            prefix = (
                "/usr/local/bin/python3.13",
                "-I",
                "-S",
                "/opt/continuum/hidden_probe.py",
            )
        else:
            prefix = (
                "/usr/local/bin/node",
                "--no-addons",
                "--disable-proto=throw",
                "/opt/continuum/hidden_probe.cjs",
            )
        argv = (*prefix, "--entry", self.entry_path, "--inputs-b64", *chunks)
        if len(argv) > _MAX_ARGUMENTS:
            raise HiddenCheckError("hidden_probe_argument_limit")
        return argv
