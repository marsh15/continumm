"""Opt-in and bounded command-line behavior for the publication worker."""

from __future__ import annotations

import asyncio
import io
import json
from dataclasses import dataclass
from uuid import uuid4

import pytest

from continuum_api.worker import main


@dataclass
class _Receipt:
    run_id: object
    jobs_started: int
    stop_reason: str


class _Runner:
    def __init__(self, receipt: _Receipt | None = None, error: Exception | None = None) -> None:
        self.receipt = receipt
        self.error = error
        self.calls = []

    async def run_admitted(self, run_id, *, max_jobs, max_seconds):
        self.calls.append((run_id, max_jobs, max_seconds))
        if self.error is not None:
            raise self.error
        return self.receipt


class _Resources:
    def __init__(self, runner: _Runner) -> None:
        self.runner = runner
        self.closed = 0

    async def aclose(self) -> None:
        self.closed += 1


def _args(account_id, run_id, *extra: str) -> list[str]:
    return [
        "--account-id",
        str(account_id),
        "--run-id",
        str(run_id),
        *extra,
    ]


def test_publication_execution_refuses_before_loading_settings() -> None:
    account_id, run_id = uuid4(), uuid4()
    output = io.StringIO()

    def settings_loader():
        raise AssertionError("settings must not be read without the opt-in flag")

    status = main(
        _args(account_id, run_id),
        settings_loader=settings_loader,
        stdout=output,
    )

    assert status == 2
    assert json.loads(output.getvalue()) == {
        "run_id": str(run_id),
        "jobs_started": 0,
        "stop_reason": "execution_required",
    }


def test_explicit_execution_runs_only_the_requested_bounded_drain() -> None:
    account_id, run_id = uuid4(), uuid4()
    runner = _Runner(_Receipt(run_id, 2, "waiting"))
    resources = _Resources(runner)
    output = io.StringIO()
    loaded = []

    status = main(
        _args(
            account_id,
            run_id,
            "--execute-publication",
            "--max-jobs",
            "3",
            "--max-seconds",
            "17",
        ),
        settings_loader=lambda: loaded.append("settings") or object(),
        resources_factory=lambda settings, owner: resources,
        stdout=output,
    )

    assert status == 0
    assert loaded == ["settings"]
    assert runner.calls == [(run_id, 3, 17.0)]
    assert resources.closed == 1
    assert json.loads(output.getvalue()) == {
        "run_id": str(run_id),
        "jobs_started": 2,
        "stop_reason": "waiting",
    }


def test_failed_runner_returns_only_a_sanitized_error_code_and_closes_resources() -> None:
    account_id, run_id = uuid4(), uuid4()
    runner = _Runner(error=RuntimeError("secret-token-value"))
    resources = _Resources(runner)
    output = io.StringIO()

    status = main(
        _args(account_id, run_id, "--execute-publication"),
        settings_loader=lambda: object(),
        resources_factory=lambda settings, owner: resources,
        stdout=output,
    )

    assert status == 1
    assert resources.closed == 1
    assert "secret-token-value" not in output.getvalue()
    assert json.loads(output.getvalue()) == {
        "run_id": str(run_id),
        "jobs_started": None,
        "stop_reason": "error",
        "error_code": "worker_failed",
    }


def test_job_limit_is_rejected_before_settings_are_loaded() -> None:
    account_id, run_id = uuid4(), uuid4()
    output = io.StringIO()

    with pytest.raises(SystemExit):
        main(
            _args(
                account_id,
                run_id,
                "--execute-publication",
                "--max-jobs",
                "101",
            ),
            settings_loader=lambda: pytest.fail("settings must not be loaded"),
            stdout=output,
        )

    assert not output.getvalue()


def test_async_factory_is_supported_and_settings_failure_is_sanitized() -> None:
    account_id, run_id = uuid4(), uuid4()
    output = io.StringIO()
    resources = _Resources(_Runner(_Receipt(run_id, 0, "idle")))

    async def factory(settings, owner):
        await asyncio.sleep(0)
        return resources

    status = main(
        _args(account_id, run_id, "--execute-publication"),
        settings_loader=lambda: object(),
        resources_factory=factory,
        stdout=output,
    )

    assert status == 0
    assert resources.closed == 1
    assert json.loads(output.getvalue())["stop_reason"] == "idle"

    output = io.StringIO()
    status = main(
        _args(account_id, run_id, "--execute-publication"),
        settings_loader=lambda: (_ for _ in ()).throw(RuntimeError("database-password-value")),
        stdout=output,
    )

    assert status == 1
    assert "database-password-value" not in output.getvalue()
