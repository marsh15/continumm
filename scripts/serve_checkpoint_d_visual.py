"""Serve retained synthetic Checkpoint D timelines on loopback for review only.

This wrapper loads credentials created by the isolated local integration test.
It injects fixed test principals and must never be used for a deployed service.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from uuid import UUID

if os.environ.get("CONTINUUM_ENVIRONMENT") == "production":
    raise SystemExit("Checkpoint D visual fixture refuses production environment")
for _name in tuple(os.environ):
    if _name.startswith("CONTINUUM_") or _name == "PORT":
        os.environ.pop(_name, None)

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_DATABASE = "continuum_checkpoint_d_test"
VISUAL_HOST = "127.0.0.1"
VISUAL_PORT = 8013


def main() -> None:
    from fastapi import HTTPException
    from sqlalchemy.engine import make_url

    from continuum_api.app import create_app
    from continuum_api.runtime.database import create_runtime_engine
    from continuum_api.runtime.persistence import Principal

    if len(sys.argv) != 2:
        raise SystemExit("usage: serve_checkpoint_d_visual.py PRIVATE_RETAINED_DIRECTORY")
    requested_directory = Path(sys.argv[1]).expanduser()
    if requested_directory.is_symlink():
        raise SystemExit("retained fixture directory must not be a symlink")
    directory = requested_directory.resolve()
    tmp_root = Path("/tmp").resolve()
    if not directory.is_relative_to(tmp_root) or directory == tmp_root or directory.is_symlink():
        raise SystemExit("retained fixture must be a real directory under /tmp")
    if directory.stat().st_mode & 0o077:
        raise SystemExit("retained fixture directory permissions must be 0700")

    state_path = directory / "visual_state.json"
    url_path = directory / "runtime_database_url"
    if state_path.is_symlink() or url_path.is_symlink():
        raise SystemExit("retained fixture files must not be symlinks")
    if state_path.stat().st_mode & 0o077 or url_path.stat().st_mode & 0o077:
        raise SystemExit("retained fixture files must be mode 0600")

    state = json.loads(state_path.read_text(encoding="utf-8"))
    if state.get("database_name") != EXPECTED_DATABASE:
        raise SystemExit("retained fixture database identity is invalid")
    runtime_url_text = url_path.read_text(encoding="utf-8").strip()
    runtime_url = make_url(runtime_url_text)
    if (
        runtime_url.host not in {"127.0.0.1", "localhost", "::1"}
        or runtime_url.database != EXPECTED_DATABASE
        or runtime_url.username != "continuum_runtime"
        or runtime_url.query != {"sslmode": "disable"}
    ):
        raise SystemExit("retained fixture database URL is not a loopback runtime login")

    principals = {
        UUID(record["run_id"]): Principal(UUID(record["owner_id"])) for record in state["runs"]
    }
    if not principals:
        raise SystemExit("retained fixture has no run records")
    engine = create_runtime_engine(runtime_url_text)

    def resolve_test_principal(request: object) -> Principal:
        run_id = UUID(request.path_params["run_id"])  # type: ignore[attr-defined]
        principal = principals.get(run_id)
        if principal is None:
            raise HTTPException(status_code=401, detail="authentication_required")
        return principal

    application = create_app(
        static_dir=ROOT / "frontend" / "dist",
        principal_resolver=resolve_test_principal,
        runtime_engine=engine,
    )
    import uvicorn

    pid_path = directory / "app.pid"
    pid_path.write_text(f"{os.getpid()}\n", encoding="ascii")
    pid_path.chmod(0o600)
    print(f"Serving synthetic Checkpoint D timelines at http://{VISUAL_HOST}:{VISUAL_PORT}")
    try:
        uvicorn.run(
            application,
            host=VISUAL_HOST,
            port=VISUAL_PORT,
            log_config=None,
            access_log=False,
        )
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
