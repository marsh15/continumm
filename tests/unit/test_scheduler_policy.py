"""Unit tests for the T10 durable retry and replay policy."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from continuum_api.runtime.persistence import AttemptStatus, RunStatus, StepStatus
from continuum_api.runtime.scheduler import FailureClass, plan_failure


def test_transient_read_has_three_total_attempts_and_full_jitter() -> None:
    now = datetime(2026, 10, 11, tzinfo=UTC)
    bounds: list[tuple[float, float]] = []

    first = plan_failure(
        FailureClass.TRANSIENT_READ,
        0,
        database_now=now,
        run_deadline=None,
        retry_after=None,
        jitter=lambda low, high: bounds.append((low, high)) or high,
    )
    second = plan_failure(
        FailureClass.TRANSIENT_READ,
        1,
        database_now=now,
        run_deadline=None,
        retry_after=None,
        jitter=lambda low, high: bounds.append((low, high)) or low,
    )
    exhausted = plan_failure(
        FailureClass.TRANSIENT_READ,
        2,
        database_now=now,
        run_deadline=None,
        retry_after=None,
        jitter=lambda _low, _high: pytest.fail("exhausted attempts must not sleep"),
    )

    assert bounds == [(0.0, 1.0), (0.0, 2.0)]
    assert first.retry_scheduled and first.failure_count == 1
    assert first.available_at == now + timedelta(seconds=1)
    assert second.retry_scheduled and second.failure_count == 2
    assert second.available_at == now
    assert not exhausted.retry_scheduled and exhausted.failure_count == 3
    assert exhausted.job_status == "failed" and exhausted.run_status is RunStatus.FAILED


def test_retry_after_overrides_shorter_jitter_and_deadline_prevents_retry() -> None:
    now = datetime(2026, 10, 11, tzinfo=UTC)
    provider_delay = now + timedelta(seconds=10)
    scheduled = plan_failure(
        FailureClass.TRANSIENT_READ,
        0,
        database_now=now,
        run_deadline=now + timedelta(seconds=20),
        retry_after=provider_delay,
        jitter=lambda _low, _high: 0,
    )
    denied = plan_failure(
        FailureClass.TRANSIENT_READ,
        0,
        database_now=now,
        run_deadline=provider_delay,
        retry_after=provider_delay,
        jitter=lambda _low, _high: 0,
    )

    assert scheduled.retry_scheduled and scheduled.available_at == provider_delay
    assert not denied.retry_scheduled
    assert denied.event_type == "job.retry_rejected_by_deadline"
    assert denied.job_status == "failed" and denied.run_status is RunStatus.FAILED


@pytest.mark.parametrize(
    ("failure_class", "run_status", "attempt_status"),
    [
        (
            FailureClass.MISSING_CREDENTIAL,
            RunStatus.WAITING_CREDENTIALS,
            AttemptStatus.PROTOCOL_FAILURE,
        ),
        (
            FailureClass.RESOURCE_EXHAUSTION,
            RunStatus.WAITING_CAPACITY,
            AttemptStatus.PROTOCOL_FAILURE,
        ),
    ],
)
def test_wait_classes_do_not_spend_failure_budget(
    failure_class: FailureClass, run_status: RunStatus, attempt_status: AttemptStatus
) -> None:
    now = datetime(2026, 10, 11, tzinfo=UTC)
    decision = plan_failure(
        failure_class,
        2,
        database_now=now,
        run_deadline=None,
        retry_after=None,
    )

    assert decision.run_status is run_status
    assert decision.attempt_status is attempt_status
    assert decision.failure_count == 2
    assert decision.job_status == "queued"
    assert decision.step_status is StepStatus.QUEUED
    assert not decision.retry_scheduled
    if failure_class is FailureClass.RESOURCE_EXHAUSTION:
        assert decision.capacity_resume_at is None


def test_capacity_wait_only_auto_resumes_at_an_explicit_retry_after() -> None:
    now = datetime(2026, 10, 11, tzinfo=UTC)
    resume_at = now + timedelta(minutes=2)
    decision = plan_failure(
        FailureClass.RESOURCE_EXHAUSTION,
        0,
        database_now=now,
        run_deadline=now + timedelta(minutes=10),
        retry_after=resume_at,
    )

    assert decision.capacity_resume_at == resume_at
    assert decision.available_at == now
    assert decision.run_status is RunStatus.WAITING_CAPACITY


@pytest.mark.parametrize(
    ("failure_class", "run_status", "attempt_status"),
    [
        (FailureClass.INVALID_INPUT, RunStatus.FAILED, AttemptStatus.DEFINITIVE_FAILURE),
        (FailureClass.UNSUPPORTED_ENVIRONMENT, RunStatus.FAILED, AttemptStatus.PROTOCOL_FAILURE),
        (FailureClass.UNKNOWN_EFFECT, RunStatus.NEEDS_ATTENTION, AttemptStatus.UNKNOWN),
        (FailureClass.MODEL_RESPONSE_LOST, RunStatus.NEEDS_ATTENTION, AttemptStatus.UNKNOWN),
        (FailureClass.REMOTE_CONFLICT, RunStatus.NEEDS_ATTENTION, AttemptStatus.PROTOCOL_FAILURE),
        (FailureClass.CORRUPT_ARTIFACT, RunStatus.NEEDS_ATTENTION, AttemptStatus.PROTOCOL_FAILURE),
    ],
)
def test_unsafe_or_terminal_failure_classes_never_retry(
    failure_class: FailureClass, run_status: RunStatus, attempt_status: AttemptStatus
) -> None:
    decision = plan_failure(
        failure_class,
        0,
        database_now=datetime(2026, 10, 11, tzinfo=UTC),
        run_deadline=None,
        retry_after=None,
        jitter=lambda _low, _high: pytest.fail("nontransient failures must not sleep"),
    )

    assert decision.run_status is run_status
    assert decision.attempt_status is attempt_status
    assert decision.job_status == "failed" and decision.step_status is StepStatus.FAILED
    assert not decision.retry_scheduled
