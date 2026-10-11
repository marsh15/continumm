"""Export the application OpenAPI schema or fail when its committed copy drifts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from continuum_api.app import app  # noqa: E402

SCHEMA_PATH = ROOT / "frontend" / "openapi.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if frontend/openapi.json does not match the current FastAPI schema",
    )
    args = parser.parse_args()

    schema_text = json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n"
    if args.check:
        if not SCHEMA_PATH.exists() or SCHEMA_PATH.read_text() != schema_text:
            print("frontend/openapi.json is stale; run `make openapi`.", file=sys.stderr)
            return 1
        print("frontend/openapi.json is current.")
        return 0

    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.write_text(schema_text)
    print(f"Exported {SCHEMA_PATH.relative_to(ROOT)}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
