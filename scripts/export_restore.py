"""Create an encrypted owner export or validate one in an offline quarantine."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from uuid import UUID

from continuum_api.portable_exports import (
    PortableExportError,
    cleanup_invalidated_exports,
    export_owner,
    parse_export_key,
    restore_check,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="continuum-export-restore")
    subparsers = parser.add_subparsers(dest="operation", required=True)
    export_parser = subparsers.add_parser("export", help="write one encrypted account export")
    export_parser.add_argument("--owner-id", type=UUID, required=True)
    export_parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(
            os.environ.get("CONTINUUM_EXPORT_DIR", str(Path.home() / ".continuum" / "exports"))
        ),
    )
    check_parser = subparsers.add_parser(
        "restore-check", help="verify and quarantine-check an export"
    )
    check_parser.add_argument("--file", type=Path, required=True)
    check_parser.add_argument(
        "--quarantine-dir",
        type=Path,
        default=Path(
            os.environ.get(
                "CONTINUUM_QUARANTINE_DIR", str(Path.home() / ".continuum" / "quarantine")
            )
        ),
    )
    args = parser.parse_args(argv)
    try:
        key_value = os.environ.get("CONTINUUM_EXPORT_KEY", "")
        key = parse_export_key(key_value)
        from continuum_api.settings import get_settings

        settings = get_settings()
        if settings.runtime_database_url is None:
            raise PortableExportError("runtime_database_unavailable")
        from continuum_api.runtime.database import create_runtime_engine

        engine = create_runtime_engine(settings.runtime_database_url.get_secret_value())
        storage = None
        try:
            if args.operation == "export":
                if not settings.supabase_url or settings.supabase_secret_key is None:
                    raise PortableExportError("export_storage_unavailable")
                from continuum_api.connectivity.storage import SupabaseStorage
                from continuum_api.runtime.persistence import Principal

                cleanup_invalidated_exports(engine, args.output_dir, key)
                storage = SupabaseStorage(
                    settings.supabase_url,
                    settings.supabase_secret_key.get_secret_value(),
                    settings.supabase_bucket,
                    fixture_byte_limit=10 * 1024 * 1024,
                )
                result = export_owner(
                    engine,
                    Principal(args.owner_id),
                    storage,
                    args.output_dir,
                    key,
                )
            else:
                result = restore_check(args.file, engine, args.quarantine_dir, key)
        finally:
            if storage is not None:
                asyncio.run(storage.aclose())
            engine.dispose()
    except PortableExportError as error:
        print(json.dumps({"ok": False, "error_code": error.code}, separators=(",", ":")))
        return 1
    except Exception:
        print(
            json.dumps({"ok": False, "error_code": "export_restore_failed"}, separators=(",", ":"))
        )
        return 1
    print(json.dumps({"ok": True, **result}, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
