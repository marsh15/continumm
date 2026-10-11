"""Security properties for environment-backed settings."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from pydantic import ValidationError

from continuum_api.settings import Settings


def test_development_settings_bind_to_loopback_by_default() -> None:
    settings = Settings(_env_file=None)

    assert settings.environment == "development"
    assert settings.host == "127.0.0.1"
    assert settings.port == 8000


def test_development_cannot_bind_a_public_interface() -> None:
    with pytest.raises(ValidationError, match="loopback"):
        Settings(_env_file=None, host="0.0.0.0")

    with pytest.raises(ValidationError, match="loopback"):
        Settings(_env_file=None, host="192.168.1.10")


@pytest.mark.parametrize("port", [0, 65536])
def test_port_must_be_valid_tcp_port(port: int) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, port=port)


def test_production_requires_explicit_secrets_and_bind_address() -> None:
    secret = "db-password-must-not-appear"

    with pytest.raises(ValidationError) as error:
        Settings(_env_file=None, environment="production", database_url=f"postgres://u:{secret}@db")

    message = str(error.value)
    assert "CONTINUUM_HOST" in message
    assert "CONTINUUM_ENCRYPTION_KEY" in message
    assert secret not in message


def test_production_secret_values_have_masked_representations() -> None:
    settings = Settings(
        _env_file=None,
        environment="production",
        host="0.0.0.0",
        database_url="postgresql://user:very-private-password@db.example.test/app",
        encryption_key="very-private-encryption-key",
    )

    assert "very-private-password" not in repr(settings)
    assert "very-private-encryption-key" not in repr(settings)


def test_startup_reports_missing_settings_without_printing_secret_values() -> None:
    secret = "startup-secret-sentinel"
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "CONTINUUM_ENVIRONMENT": "production",
        "CONTINUUM_HOST": "0.0.0.0",
        "CONTINUUM_DATABASE_URL": f"postgresql://user:{secret}@db.example.test/app",
    }

    result = subprocess.run(
        [sys.executable, "-m", "continuum_api"],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=10,
    )

    assert result.returncode == 2
    assert "CONTINUUM_ENCRYPTION_KEY" in result.stdout
    assert secret not in result.stdout + result.stderr
