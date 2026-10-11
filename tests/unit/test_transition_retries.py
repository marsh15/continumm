"""Unit coverage for T08's database-only transaction retry policy."""

from __future__ import annotations

from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError

from continuum_api.runtime.persistence import Principal, RunStatus
from continuum_api.runtime.transitions import NextWork, TransitionReceipt, TransitionService


class _DriverError(Exception):
    def __init__(self, sqlstate: str) -> None:
        self.sqlstate = sqlstate


@pytest.mark.parametrize("sqlstate", ["40001", "40P01"])
def test_transient_database_errors_restart_the_transaction(sqlstate: str, monkeypatch) -> None:
    service = _service()
    receipt = _receipt()
    calls = 0
    sleeps: list[float] = []
    jitter_bounds: list[tuple[float, float]] = []

    def commit_once(**_arguments) -> TransitionReceipt:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise _database_error(sqlstate)
        return receipt

    monkeypatch.setattr(service, "_commit_once", commit_once)
    monkeypatch.setattr("continuum_api.runtime.transitions.time.sleep", sleeps.append)
    monkeypatch.setattr(
        "continuum_api.runtime.transitions.random.uniform",
        lambda lower, upper: jitter_bounds.append((lower, upper)) or upper,
    )

    recovered = _commit(service)

    assert recovered is receipt
    assert calls == 3
    assert sleeps == [0.01, 0.02]
    assert jitter_bounds == [(0, 0.01), (0, 0.02)]


def test_retry_budget_is_three_transactions(monkeypatch) -> None:
    service = _service()
    calls = 0
    sleeps: list[float] = []

    def commit_once(**_arguments) -> TransitionReceipt:
        nonlocal calls
        calls += 1
        raise _database_error("40P01")

    monkeypatch.setattr(service, "_commit_once", commit_once)
    monkeypatch.setattr("continuum_api.runtime.transitions.time.sleep", sleeps.append)
    monkeypatch.setattr("continuum_api.runtime.transitions.random.uniform", lambda _low, high: high)

    with pytest.raises(DBAPIError):
        _commit(service)

    assert calls == 3
    assert sleeps == [0.01, 0.02]


def test_connection_loss_is_not_retried(monkeypatch) -> None:
    service = _service()
    calls = 0
    sleeps: list[float] = []

    def commit_once(**_arguments) -> TransitionReceipt:
        nonlocal calls
        calls += 1
        raise _database_error("08006")

    monkeypatch.setattr(service, "_commit_once", commit_once)
    monkeypatch.setattr("continuum_api.runtime.transitions.time.sleep", sleeps.append)

    with pytest.raises(DBAPIError):
        _commit(service)

    assert calls == 1
    assert sleeps == []


def _service() -> TransitionService:
    return TransitionService(cast(Engine, object()), Principal(uuid4()))


def _receipt() -> TransitionReceipt:
    run_id, step_id, job_id, result_id = uuid4(), uuid4(), uuid4(), uuid4()
    return TransitionReceipt(
        result_id=result_id,
        run_id=run_id,
        step_id=step_id,
        job_id=job_id,
        result_sha256=b"r" * 32,
        result={"ok": True},
        run_status=RunStatus.RUNNING,
        event_sequence=1,
        next_step_id=uuid4(),
        next_job_id=uuid4(),
        adopted=False,
    )


def _database_error(sqlstate: str) -> DBAPIError:
    return DBAPIError("test statement", {}, _DriverError(sqlstate), connection_invalidated=False)


def _commit(service: TransitionService) -> TransitionReceipt:
    return service.commit_result(
        run_id=uuid4(),
        step_id=uuid4(),
        job_id=uuid4(),
        result={"ok": True},
        target_run_status=RunStatus.RUNNING,
        next_work=NextWork("next:unit", "unit"),
    )
