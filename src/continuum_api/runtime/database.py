"""Conservative connection factory for the separately privileged runtime role."""

from __future__ import annotations

import ipaddress
import os
from urllib.parse import urlsplit

from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import URL, make_url


def create_runtime_engine(database_url: str) -> Engine:
    """Validate the runtime login URL and create its bounded PostgreSQL pool.

    The URL must contain the dedicated ``continuum_runtime`` credentials. This
    factory never falls back to migration, wake, or ingress connection settings.
    """
    url = _validated_url(database_url)
    return create_engine(
        url,
        pool_size=2,
        max_overflow=0,
        pool_timeout=3,
        pool_pre_ping=True,
        pool_recycle=300,
        hide_parameters=True,
        connect_args={
            "application_name": "continumm-run-api",
            "connect_timeout": 5,
            "options": "-c statement_timeout=5000 -c lock_timeout=2000",
        },
    )


def _validated_url(database_url: str) -> URL:
    try:
        url = make_url(database_url)
        parts = urlsplit(database_url)
        environment_is_production = os.environ.get("CONTINUUM_ENVIRONMENT") == "production"
        if url.drivername not in {"postgres", "postgresql", "postgresql+psycopg"}:
            raise ValueError
        if (
            not url.host
            or url.username != "continuum_runtime"
            or not url.password
            or not url.database
        ):
            raise ValueError
        if parts.fragment or url.query.keys() - {"sslmode", "sslrootcert"}:
            raise ValueError
        try:
            is_loopback = ipaddress.ip_address(url.host).is_loopback
        except ValueError:
            is_loopback = url.host.rstrip(".").lower() == "localhost"

        sslmode = url.query.get("sslmode")
        sslrootcert = url.query.get("sslrootcert")
        if not isinstance(sslmode, (str, type(None))) or not isinstance(
            sslrootcert, (str, type(None))
        ):
            raise ValueError
        if environment_is_production or not is_loopback:
            if sslmode not in {None, "verify-full"}:
                raise ValueError
            if sslrootcert not in {None, "system"}:
                raise ValueError
            url = url.set(query={"sslmode": "verify-full", "sslrootcert": "system"})
        elif sslmode not in {None, "disable", "verify-full"}:
            raise ValueError
        elif sslmode == "verify-full" and sslrootcert not in {None, "system"}:
            raise ValueError
        elif sslmode is None:
            url = url.set(query={"sslmode": "disable"})
        return url.set(drivername="postgresql+psycopg")
    except Exception:
        raise ValueError("runtime_database_url_invalid_or_unsafe") from None
