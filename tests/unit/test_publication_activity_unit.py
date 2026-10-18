"""Bounded accepted transitions for approved publication jobs."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace
from uuid import uuid4

import pytest

from continuum_api.runtime.persistence import RunStatus
from continuum_api.runtime.publication_activity import (
    ApprovedPublicationActivityHandler,
    _PublicationBinding,
)
from continuum_api.runtime.transitions import InvalidTransition


@dataclass
class _Transitions:
    calls: list[dict]
    completion_error: Exception | None = None

    def commit_result(self, **kwargs):
        self.calls.append(kwargs)
        return kwargs

    def complete_publication(self, lease, approval_id, manifest_sha256):
        if self.completion_error is not None:
            raise self.completion_error
        receipt = (lease, approval_id, manifest_sha256)
        self.calls.append({"complete_publication": receipt})
        return receipt


class _Branch:
    def __init__(self, status: str, branch_status: str | None = None) -> None:
        self.status = status
        self.branch_status = branch_status or status
        self.calls = []

    async def publish(self, lease, approval_id, digest):
        self.calls.append((lease, approval_id, digest))
        return SimpleNamespace(status=self.status, branch_status=self.branch_status)


class _PullRequest:
    def __init__(self, status: str) -> None:
        self.status = status
        self.remote_state = status if status in {"closed", "merged"} else "open"
        self.pr_number = 19
        self.pr_id = 231
        self.calls = []

    async def publish(self, lease, approval_id, digest):
        self.calls.append((lease, approval_id, digest))
        return SimpleNamespace(
            status=self.status,
            remote_state=self.remote_state,
            pr_number=self.pr_number,
            pr_id=self.pr_id,
        )


def _handler(
    branch_status: str,
    pr_status: str = "open",
    *,
    remote_branch_status: str | None = None,
):
    transitions = _Transitions([])
    branch = _Branch(
        branch_status,
        remote_branch_status or ("confirmed" if branch_status == "published" else None),
    )
    pull_request = _PullRequest(pr_status)
    handler = ApprovedPublicationActivityHandler(
        engine=None,
        principal=SimpleNamespace(account_id=uuid4()),
        transitions=transitions,
        branch_publication=branch,
        pull_request_publication=pull_request,
    )
    binding = _PublicationBinding(
        approval_id=uuid4(), manifest_sha256="a" * 64, revision=3, branch_slice=2
    )
    handler._load_binding = lambda _lease: binding
    return handler, transitions, branch, pull_request, binding


def test_pending_branch_slice_accepts_one_durable_continuation() -> None:
    handler, transitions, branch, pull_request, binding = _handler("pending")
    lease = object()

    asyncio.run(handler(lease))

    assert len(branch.calls) == 1
    assert not pull_request.calls
    accepted = transitions.calls[0]
    assert accepted["target_run_status"] is RunStatus.RUNNING
    assert accepted["next_work"].logical_key == (
        f"publish-approved:{binding.approval_id.hex}:part:3"
    )
    assert accepted["next_work"].activity == "publish_approved"


def test_unresolved_branch_needs_attention_without_creating_a_pr() -> None:
    handler, transitions, _branch, pull_request, _binding = _handler("unknown")

    asyncio.run(handler(object()))

    assert not pull_request.calls
    assert transitions.calls[0]["target_run_status"] is RunStatus.NEEDS_ATTENTION
    assert transitions.calls[0]["result"]["status"] == "needs_attention"


def test_published_label_without_confirmed_branch_needs_attention() -> None:
    handler, transitions, _branch, pull_request, _binding = _handler(
        "published", remote_branch_status="unknown"
    )

    asyncio.run(handler(object()))

    assert not pull_request.calls
    assert transitions.calls[0]["target_run_status"] is RunStatus.NEEDS_ATTENTION


def test_confirmed_branch_and_exact_pr_state_are_required_for_success() -> None:
    handler, transitions, _branch, pull_request, binding = _handler("published", "merged")
    lease = object()

    asyncio.run(handler(lease))

    assert pull_request.calls == [(lease, binding.approval_id, binding.manifest_sha256)]
    assert transitions.calls[0]["complete_publication"] == (
        lease,
        binding.approval_id,
        binding.manifest_sha256,
    )


@pytest.mark.parametrize("status", ["created", "adopted"])
def test_created_or_adopted_open_pr_can_complete(status: str) -> None:
    handler, transitions, _branch, _pull_request, binding = _handler("published", status)

    asyncio.run(handler(object()))

    assert transitions.calls[0]["complete_publication"][1:] == (
        binding.approval_id,
        binding.manifest_sha256,
    )


def test_unresolved_pr_needs_attention_after_confirmed_branch() -> None:
    handler, transitions, _branch, _pull_request, _binding = _handler("published", "unknown")

    asyncio.run(handler(object()))

    assert transitions.calls[0]["target_run_status"] is RunStatus.NEEDS_ATTENTION
    assert transitions.calls[0]["result"]["phase"] == "pull_request"


def test_fenced_completion_rejection_moves_to_attention() -> None:
    handler, transitions, _branch, _pull_request, _binding = _handler("published", "created")
    transitions.completion_error = InvalidTransition("publication_completion_unresolved_work")

    asyncio.run(handler(object()))

    assert transitions.calls[-1]["target_run_status"] is RunStatus.NEEDS_ATTENTION
    assert transitions.calls[-1]["result"]["phase"] == "completion"
