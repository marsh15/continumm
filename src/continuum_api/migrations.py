"""Run database migrations without exposing connection details in command output."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config

KNOWN_REVISIONS = frozenset(
    {
        "base",
        "head",
        "20261011_01",
        "20261011_02",
        "20261011_03",
        "20261011_04",
        "20261011_05",
        "20261011_06",
        "20261011_07",
        "20261012_08",
        "20261012_09",
        "20261012_10",
        "20261013_11",
        "20261015_12",
        "20261015_13",
        "20261015_14",
        "20261015_15",
        "20261015_16",
        "20261016_17",
        "20261017_18",
        "20261017_19",
        "20261018_20",
        "20261019_21",
        "20261019_22",
        "20261019_23",
    }
)


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="continuum-migrate")
    parser.add_argument("operation", choices=("upgrade", "downgrade"))
    parser.add_argument("revision", choices=sorted(KNOWN_REVISIONS))
    parsed = parser.parse_args(arguments)

    if not os.environ.get("CONTINUUM_MIGRATION_DATABASE_URL"):
        print("migration_database_url_required", file=sys.stderr)
        return 2

    project_root = Path(__file__).resolve().parents[2]
    try:
        alembic_config = Config(str(project_root / "alembic.ini"))
        if parsed.operation == "upgrade":
            command.upgrade(alembic_config, parsed.revision)
        else:
            command.downgrade(alembic_config, parsed.revision)
    except Exception:
        print("migration_failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
