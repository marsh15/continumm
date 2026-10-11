"""The migration wrapper never prints database URLs or driver error details."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_migration_wrapper_requires_explicit_migration_database_url() -> None:
    project_root = Path(__file__).resolve().parents[2]
    environment = os.environ.copy()
    environment.pop("CONTINUUM_MIGRATION_DATABASE_URL", None)
    environment["CONTINUUM_TEST_DATABASE_URL"] = "postgresql://unused:test@localhost/unused"

    result = subprocess.run(
        [sys.executable, "-m", "continuum_api.migrations", "upgrade", "head"],
        cwd=project_root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "migration_database_url_required" in result.stderr
    assert "unused" not in result.stdout + result.stderr


def test_migration_wrapper_redacts_malformed_database_url() -> None:
    project_root = Path(__file__).resolve().parents[2]
    environment = os.environ.copy()
    environment["CONTINUUM_MIGRATION_DATABASE_URL"] = (
        "postgresql://migration_user:sentinel_password@localhost:bad_port/db?token=sentinel_param"
    )

    result = subprocess.run(
        [sys.executable, "-m", "continuum_api.migrations", "upgrade", "head"],
        cwd=project_root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "migration_failed" in output
    assert "sentinel_password" not in output
    assert "sentinel_param" not in output


def test_migration_wrapper_rejects_unknown_revision_before_connecting() -> None:
    project_root = Path(__file__).resolve().parents[2]
    environment = os.environ.copy()
    environment["CONTINUUM_MIGRATION_DATABASE_URL"] = "postgresql://unused:test@localhost/unused"

    result = subprocess.run(
        [sys.executable, "-m", "continuum_api.migrations", "upgrade", "20261011_99"],
        cwd=project_root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "invalid choice" in result.stderr
    assert "migration_failed" not in result.stdout + result.stderr
    assert "unused" not in result.stdout + result.stderr
