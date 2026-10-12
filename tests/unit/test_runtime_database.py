"""Configuration safety for the runtime-only PostgreSQL connection."""

from __future__ import annotations

import pytest

from continuum_api.runtime.database import _validated_url


def test_runtime_database_requires_the_runtime_role_and_loopback_or_verified_tls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CONTINUUM_ENVIRONMENT", "development")

    local_url = _validated_url("postgresql://continuum_runtime:pw@localhost/app")
    assert local_url.username == "continuum_runtime"
    assert local_url.query == {"sslmode": "disable"}

    remote_url = _validated_url(
        "postgresql://continuum_runtime:pw@db.example.test/app"
        "?sslmode=verify-full&sslrootcert=system"
    )
    assert remote_url.query == {"sslmode": "verify-full", "sslrootcert": "system"}

    with pytest.raises(ValueError, match="runtime_database_url_invalid_or_unsafe"):
        _validated_url("postgresql://postgres:pw@localhost/app")
    with pytest.raises(ValueError, match="runtime_database_url_invalid_or_unsafe"):
        _validated_url("postgresql://continuum_runtime:pw@db.example.test/app?sslmode=require")
    with pytest.raises(ValueError, match="runtime_database_url_invalid_or_unsafe"):
        _validated_url("postgresql://continuum_runtime:pw@localhost/app?options=-c%20role=postgres")


def test_production_runtime_connection_always_verifies_tls_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CONTINUUM_ENVIRONMENT", "production")

    url = _validated_url("postgresql://continuum_runtime:pw@db.example.test/app")

    assert url.query == {"sslmode": "verify-full", "sslrootcert": "system"}
