"""Optional real-Postgres T03 adapter proof, restricted to a disposable local DB."""

from __future__ import annotations

import os
from collections.abc import Iterator
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.engine import make_url

from continuum_api.connectivity.database import T03Database, create_database_engine


@pytest.fixture
def local_t03_database() -> Iterator[tuple[T03Database, Engine]]:
    database_url = os.environ.get("CONTINUUM_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_TEST_DATABASE_URL to a disposable local continuum_t03_test DB")

    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != "continuum_t03_test":
        pytest.fail("integration tests require a local database named continuum_t03_test")

    engine = create_database_engine(database_url, require_ssl=False)
    try:
        with engine.begin() as connection:
            connection.execute(text("DROP TABLE IF EXISTS public.t03_hosted_smoke_claims"))
            connection.execute(text("DROP TABLE IF EXISTS public.t03_connectivity_probe"))
            connection.execute(
                text(
                    """
                    CREATE TABLE public.t03_connectivity_probe (
                        probe_id uuid PRIMARY KEY,
                        created_at timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
            )
            connection.execute(
                text(
                    """
                    CREATE TABLE public.t03_hosted_smoke_claims (
                        run_id uuid PRIMARY KEY,
                        probe_id uuid NOT NULL UNIQUE,
                        object_key text NOT NULL UNIQUE,
                        status text NOT NULL CHECK (
                            status IN ('claimed', 'completed', 'failed', 'unknown')
                        ),
                        result_code text,
                        claimed_at timestamptz NOT NULL DEFAULT now(),
                        finished_at timestamptz
                    )
                    """
                )
            )
            connection.execute(
                text("ALTER TABLE public.t03_connectivity_probe ENABLE ROW LEVEL SECURITY")
            )
            connection.execute(
                text("ALTER TABLE public.t03_hosted_smoke_claims ENABLE ROW LEVEL SECURITY")
            )
        yield T03Database(engine), engine
    finally:
        with engine.begin() as connection:
            connection.execute(text("DROP TABLE IF EXISTS public.t03_hosted_smoke_claims"))
            connection.execute(text("DROP TABLE IF EXISTS public.t03_connectivity_probe"))
        engine.dispose()


def test_postgres_claim_commits_probe_is_freshly_read_and_cleaned(
    local_t03_database: tuple[T03Database, Engine],
) -> None:
    database, engine = local_t03_database
    run_id = uuid4()
    probe_id = uuid4()
    object_key = f"t03-probes/{uuid4()}.bin"

    claim = database.claim_run(run_id, probe_id, object_key)
    assert claim.acquired is True
    assert claim.claim.status == "claimed"

    database.run_probe_row(probe_id)
    with engine.connect() as connection:
        remaining_probe = connection.execute(
            text("SELECT probe_id FROM public.t03_connectivity_probe WHERE probe_id = :probe_id"),
            {"probe_id": probe_id},
        ).scalar_one_or_none()
        stored_claim = (
            connection.execute(
                text(
                    """
                SELECT run_id, probe_id, object_key, status, result_code
                FROM public.t03_hosted_smoke_claims
                WHERE run_id = :run_id
                """
                ),
                {"run_id": run_id},
            )
            .mappings()
            .one()
        )

    assert remaining_probe is None
    assert stored_claim["probe_id"] == probe_id
    assert stored_claim["object_key"] == object_key
    assert stored_claim["status"] == "claimed"

    database.finish_claim(run_id, status="completed", result_code="ok")
    repeated = database.claim_run(run_id, uuid4(), f"t03-probes/{uuid4()}.bin")

    assert repeated.acquired is False
    assert repeated.claim.status == "completed"
    assert repeated.claim.result_code == "ok"
    assert repeated.claim.probe_id == probe_id
    assert repeated.claim.object_key == object_key


def test_postgres_t03_tables_have_rls_enabled(
    local_t03_database: tuple[T03Database, Engine],
) -> None:
    _, engine = local_t03_database
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT relname, relrowsecurity
                FROM pg_class
                WHERE relname IN ('t03_connectivity_probe', 't03_hosted_smoke_claims')
                """
            )
        ).all()

    assert {row[0]: row[1] for row in rows} == {
        "t03_connectivity_probe": True,
        "t03_hosted_smoke_claims": True,
    }
