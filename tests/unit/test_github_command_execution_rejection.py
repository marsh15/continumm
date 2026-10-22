from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from continuum_api.runtime.github_command_execution import (
    GitHubCommandProcessor,
    GitHubCommandResult,
)
from continuum_api.runtime.github_commands import (
    GitHubCommandError,
    GitHubCommandReceipt,
    NormalizedGitHubCommand,
)


def _receipt() -> GitHubCommandReceipt:
    now = datetime.now(UTC)
    return GitHubCommandReceipt(
        17,
        29,
        23,
        now,
        now + timedelta(days=1),
        NormalizedGitHubCommand("status", target_id=str(uuid4())),
    )


def _processor() -> GitHubCommandProcessor:
    return object.__new__(GitHubCommandProcessor)


def test_record_rejection_persists_fixed_result_after_bound_receipt_check(monkeypatch) -> None:
    processor = _processor()
    receipt = _receipt()
    seen: list[object] = []
    monkeypatch.setattr(processor, "_load_bound_command", lambda item: _async(SimpleNamespace()))
    monkeypatch.setattr(processor, "_stored_result", lambda _item: _async(None))

    async def store(item, result):
        seen.extend([item, result])

    monkeypatch.setattr(processor, "_store_result", store)

    result = asyncio.run(processor.record_rejection(receipt))

    assert result == GitHubCommandResult("rejected", status="rejected")
    assert seen[0] is receipt
    assert seen[1] == GitHubCommandResult("rejected", status="rejected")


def test_record_rejection_adopts_an_existing_result_without_overwrite(monkeypatch) -> None:
    processor = _processor()
    receipt = _receipt()
    prior = {"kind": "status", "status": "queued", "run_id": str(uuid4()), "revision": 3}
    monkeypatch.setattr(processor, "_load_bound_command", lambda _item: _async(SimpleNamespace()))
    monkeypatch.setattr(processor, "_stored_result", lambda _item: _async(prior))

    async def unexpected_store(*_args):
        raise AssertionError("existing result must remain immutable")

    monkeypatch.setattr(processor, "_store_result", unexpected_store)

    result = asyncio.run(processor.record_rejection(receipt))

    assert result.kind == "status"
    assert result.status == "queued"
    assert result.duplicate is True


@pytest.mark.parametrize(
    "code",
    [
        "github_command_target_not_found",
        "github_command_approval_stale",
        "approval_expired",
    ],
)
def test_execute_records_known_permanent_command_rejections(monkeypatch, code: str) -> None:
    processor = _processor()
    receipt = _receipt()
    rejection = GitHubCommandResult("rejected", status="rejected")

    async def fail(_receipt):
        raise GitHubCommandError(code)

    async def record(item):
        assert item is receipt
        return rejection

    monkeypatch.setattr(processor, "_execute", fail)
    monkeypatch.setattr(processor, "record_rejection", record)

    assert asyncio.run(processor.execute(receipt)) == rejection


@pytest.mark.parametrize(
    "error",
    [
        GitHubCommandError("github_command_storage_unavailable"),
        GitHubCommandError("repository_authorization_pending"),
        GitHubCommandError("synthetic_unknown_failure"),
    ],
)
def test_execute_leaves_transient_or_unknown_errors_pending(monkeypatch, error) -> None:
    processor = _processor()
    receipt = _receipt()

    async def fail(_receipt):
        raise error

    async def unexpected_rejection(_receipt):
        raise AssertionError("transient or unknown errors must not be consumed")

    monkeypatch.setattr(processor, "_execute", fail)
    monkeypatch.setattr(processor, "record_rejection", unexpected_rejection)

    with pytest.raises(GitHubCommandError) as caught:
        asyncio.run(processor.execute(receipt))
    assert caught.value.code == error.code


async def _async(value):
    return value
