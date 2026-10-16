"""Revision feedback changes durable context without rewriting the original task."""

from __future__ import annotations

import hashlib
import json
from uuid import uuid4

import pytest

from continuum_api.runtime.agent_context import (
    AgentContextError,
    build_agent_context,
    context_source_sha256,
)

_BUDGET = {
    "requests_remaining": 20,
    "input_tokens_remaining": 50_000,
    "output_tokens_remaining": 20_000,
    "usage_uncertain": False,
}


def _feedback() -> dict:
    message = "Add a boundary test for café input"
    return {
        "feedback_id": str(uuid4()),
        "revision": 1,
        "feedback": message,
        "feedback_sha256": hashlib.sha256(message.encode()).hexdigest(),
    }


def test_initial_revision_keeps_pre_rework_source_hash() -> None:
    initial = {"task": "Keep the admitted task fixed"}
    prior = {"initial_context": initial, "accepted_turn_ids": [], "accepted_invocation_ids": []}
    expected = hashlib.sha256(
        json.dumps(
            prior,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()
    assert (
        context_source_sha256(
            initial_context=initial, accepted_turn_ids=(), accepted_invocation_ids=()
        )
        == expected
    )


def test_feedback_is_hashed_and_added_without_changing_the_pinned_task() -> None:
    initial = {"task": "Keep the admitted task fixed", "constraints": ["No new dependencies"]}
    feedback = _feedback()
    context = build_agent_context(
        initial_context=initial,
        accepted_turns=(),
        model_id="scripted-v1",
        budget=_BUDGET,
        revision_feedback=feedback,
    )
    assert context["task"] == initial["task"]
    assert context["constraints"] == initial["constraints"]
    assert context["revision_feedback"] == feedback
    original = context_source_sha256(
        initial_context=initial, accepted_turn_ids=(), accepted_invocation_ids=()
    )
    updated = context_source_sha256(
        initial_context=initial,
        accepted_turn_ids=(),
        accepted_invocation_ids=(),
        revision_feedback=feedback,
    )
    assert original != updated


@pytest.mark.parametrize(
    "field,value",
    [
        ("feedback_sha256", "0" * 64),
        ("revision", 3),
        ("feedback", "é" * 8193),
    ],
)
def test_invalid_feedback_cannot_enter_provider_context(field: str, value: object) -> None:
    feedback = {**_feedback(), field: value}
    with pytest.raises(AgentContextError):
        build_agent_context(
            initial_context={"task": "Bounded change"},
            accepted_turns=(),
            model_id="scripted-v1",
            budget=_BUDGET,
            revision_feedback=feedback,
        )


@pytest.mark.parametrize("provider", ["openai", "anthropic"])
def test_new_feedback_survives_native_history_from_previous_revision(provider: str) -> None:
    from test_agent_context import _turn

    from continuum_api.runtime.agent_loop import ANTHROPIC_MODEL_ID, OPENAI_MODEL_ID

    model = OPENAI_MODEL_ID if provider == "openai" else ANTHROPIC_MODEL_ID
    initial = {"task": "Keep the admitted task fixed"}
    first_feedback = _feedback()
    previous = build_agent_context(
        initial_context=initial,
        accepted_turns=(),
        model_id=model,
        budget=_BUDGET,
        revision_feedback=first_feedback,
    )
    second_text = "Now handle the final boundary too"
    second_feedback = {
        **_feedback(),
        "revision": 2,
        "feedback": second_text,
        "feedback_sha256": hashlib.sha256(second_text.encode()).hexdigest(),
    }
    current = build_agent_context(
        initial_context=initial,
        accepted_turns=(_turn(model, previous),),
        model_id=model,
        budget=_BUDGET,
        revision_feedback=second_feedback,
    )
    assert current["revision_feedback"] == second_feedback
    assert current["task"] == initial["task"]
    assert first_feedback["feedback"] in json.dumps(current, ensure_ascii=False)
