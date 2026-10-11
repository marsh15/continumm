"""Small SQLAlchemy Core adapter for the hosted T03 probe."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url


class EffectState(StrEnum):
    """Knowledge about whether a remote mutation committed."""

    KNOWN = "known"
    UNKNOWN = "unknown"


class DatabaseFailure(RuntimeError):
    """A redacted database result with no driver exception attached."""

    def __init__(self, code: str, *, effect: EffectState = EffectState.KNOWN) -> None:
        super().__init__(code)
        self.code = code
        self.effect = effect


@dataclass(frozen=True, slots=True)
class SmokeClaim:
    """Durable T03 claim and the probe identifiers reserved by that claim."""

    run_id: UUID
    probe_id: UUID
    object_key: str
    status: str
    result_code: str | None


@dataclass(frozen=True, slots=True)
class ClaimResult:
    """Whether this process consumed a new hosted smoke run ID."""

    acquired: bool
    claim: SmokeClaim


def sqlalchemy_url(database_url: str) -> URL:
    """Return a psycopg 3 URL without exposing its credentials in errors."""
    try:
        url = make_url(database_url)
    except Exception:
        raise DatabaseFailure("database_url_invalid") from None

    if url.drivername in {"postgres", "postgresql"}:
        return url.set(drivername="postgresql+psycopg")
    if url.drivername == "postgresql+psycopg":
        return url
    raise DatabaseFailure("database_driver_unsupported")


def create_database_engine(database_url: str, *, require_ssl: bool = True) -> Engine:
    """Create a single-connection Core engine suitable for the Free web service."""
    url = sqlalchemy_url(database_url)
    return create_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=3,
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={
            "sslmode": "require" if require_ssl else "disable",
            "connect_timeout": 5,
            "options": "-c statement_timeout=5000",
        },
    )


class T03Database:
    """Database operations limited to one-shot claims and a probe row."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def claim_run(self, run_id: UUID, probe_id: UUID, object_key: str) -> ClaimResult:
        """Commit a durable one-shot marker before performing external mutations."""
        inserted = False
        claim: SmokeClaim | None = None
        try:
            with self._engine.begin() as connection:
                row = (
                    connection.execute(
                        text(
                            """
                        INSERT INTO public.t03_hosted_smoke_claims
                            (run_id, probe_id, object_key, status)
                        VALUES (:run_id, :probe_id, :object_key, 'claimed')
                        ON CONFLICT (run_id) DO NOTHING
                        RETURNING run_id, probe_id, object_key, status, result_code
                        """
                        ),
                        {
                            "run_id": run_id,
                            "probe_id": probe_id,
                            "object_key": object_key,
                        },
                    )
                    .mappings()
                    .one_or_none()
                )

                if row is not None:
                    inserted = True
                    claim = _claim_from_row(row)
                else:
                    existing = (
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
                        .one_or_none()
                    )
                    if existing is None:
                        raise DatabaseFailure("claim_conflict_unresolved")
                    claim = _claim_from_row(existing)
        except DatabaseFailure:
            raise
        except Exception:
            # The INSERT may have committed even if its acknowledgment was lost.
            raise DatabaseFailure("claim_commit_unknown", effect=EffectState.UNKNOWN) from None

        if claim is None:
            raise DatabaseFailure("claim_result_missing")
        return ClaimResult(acquired=inserted, claim=claim)

    def run_probe_row(self, probe_id: UUID) -> None:
        """Commit, read from a new physical connection, then clean one probe row."""
        try:
            with self._engine.begin() as connection:
                connection.execute(
                    text(
                        """
                        INSERT INTO public.t03_connectivity_probe (probe_id)
                        VALUES (:probe_id)
                        """
                    ),
                    {"probe_id": probe_id},
                )
        except Exception:
            # Never guess whether the INSERT committed after a lost response.
            raise DatabaseFailure("probe_commit_unknown", effect=EffectState.UNKNOWN) from None

        self._engine.dispose()
        try:
            with self._engine.connect() as connection:
                stored_id = connection.execute(
                    text(
                        """
                        SELECT probe_id FROM public.t03_connectivity_probe
                        WHERE probe_id = :probe_id
                        """
                    ),
                    {"probe_id": probe_id},
                ).scalar_one_or_none()
        except Exception:
            self._cleanup_probe_row(probe_id)
            raise DatabaseFailure("probe_read_failed") from None

        if stored_id != probe_id:
            self._cleanup_probe_row(probe_id)
            raise DatabaseFailure("probe_readback_mismatch")

        self._cleanup_probe_row(probe_id)

    def finish_claim(self, run_id: UUID, *, status: str, result_code: str) -> None:
        """Record the final known outcome without retrying an uncertain update."""
        if status not in {"completed", "failed", "unknown"}:
            raise ValueError("invalid T03 claim status")
        if not _safe_result_code(result_code):
            raise ValueError("invalid T03 result code")

        try:
            with self._engine.begin() as connection:
                result = connection.execute(
                    text(
                        """
                        UPDATE public.t03_hosted_smoke_claims
                        SET status = :status,
                            result_code = :result_code,
                            finished_at = now()
                        WHERE run_id = :run_id AND status = 'claimed'
                        """
                    ),
                    {
                        "run_id": run_id,
                        "status": status,
                        "result_code": result_code,
                    },
                )
                if result.rowcount != 1:
                    raise DatabaseFailure("claim_finalize_conflict")
        except DatabaseFailure:
            raise
        except Exception:
            raise DatabaseFailure("claim_finalize_unknown", effect=EffectState.UNKNOWN) from None

    def _cleanup_probe_row(self, probe_id: UUID) -> None:
        """Delete only the exact T03 row and verify its absence with a new connection."""
        try:
            with self._engine.begin() as connection:
                result = connection.execute(
                    text(
                        """
                        DELETE FROM public.t03_connectivity_probe
                        WHERE probe_id = :probe_id
                        """
                    ),
                    {"probe_id": probe_id},
                )
                if result.rowcount != 1:
                    raise DatabaseFailure("probe_cleanup_conflict")
        except DatabaseFailure:
            raise
        except Exception:
            raise DatabaseFailure("probe_cleanup_unknown", effect=EffectState.UNKNOWN) from None

        self._engine.dispose()
        try:
            with self._engine.connect() as connection:
                remaining = connection.execute(
                    text(
                        """
                        SELECT probe_id FROM public.t03_connectivity_probe
                        WHERE probe_id = :probe_id
                        """
                    ),
                    {"probe_id": probe_id},
                ).scalar_one_or_none()
        except Exception:
            raise DatabaseFailure(
                "probe_cleanup_verify_failed", effect=EffectState.UNKNOWN
            ) from None
        if remaining is not None:
            raise DatabaseFailure("probe_cleanup_not_confirmed", effect=EffectState.UNKNOWN)


def _claim_from_row(row: object) -> SmokeClaim:
    values = row  # SQLAlchemy's RowMapping is intentionally kept at this boundary.
    return SmokeClaim(
        run_id=values["run_id"],  # type: ignore[index]
        probe_id=values["probe_id"],  # type: ignore[index]
        object_key=values["object_key"],  # type: ignore[index]
        status=values["status"],  # type: ignore[index]
        result_code=values["result_code"],  # type: ignore[index]
    )


def _safe_result_code(value: str) -> bool:
    return value.isascii() and value.replace("_", "").isalnum() and len(value) <= 64
