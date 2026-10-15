"""Run maintained tests with an atomically created, private pytest base directory."""

from __future__ import annotations

import subprocess
import sys
import tempfile


def main() -> int:
    if any(value == "--basetemp" or value.startswith("--basetemp=") for value in sys.argv[1:]):
        raise SystemExit("The private pytest base directory cannot be overridden")
    with tempfile.TemporaryDirectory(prefix="continuum-pytest-") as directory:
        return subprocess.call(
            [sys.executable, "-m", "pytest", "--basetemp", directory, *sys.argv[1:]]
        )


if __name__ == "__main__":
    raise SystemExit(main())
