"""Live evaluation CLI gates and private report handling without hosted calls."""

from __future__ import annotations

import asyncio
import io
import json
from uuid import uuid4

import pytest

from scripts import eval_live


def _args(*extra: str) -> list[str]:
    return [
        "--provider",
        "openai",
        "--account-id",
        str(uuid4()),
        "--python-run-id",
        str(uuid4()),
        "--node-run-id",
        str(uuid4()),
        *extra,
    ]


def _paid_args(*extra: str) -> list[str]:
    return _args("--execute-live", "--allow-model-charges", "--allow-operator-sandbox", *extra)


def _no_settings():
    pytest.fail("settings must not be loaded")


@pytest.mark.parametrize(
    "flags",
    [
        [],
        ["--execute-live"],
        ["--execute-live", "--allow-model-charges"],
        ["--execute-live", "--allow-operator-sandbox"],
    ],
)
def test_cost_and_mode_gates_refuse_before_settings(flags: list[str]) -> None:
    output = io.StringIO()
    result = eval_live.main(_args(*flags), settings_loader=_no_settings, stdout=output)
    assert result == 2
    assert json.loads(output.getvalue())["outcome"] == "not_run"


def test_execute_forwards_exact_ids_and_bounds_and_requires_ready_report(monkeypatch) -> None:
    arguments = _paid_args("--max-jobs", "7", "--max-seconds", "60")
    output = io.StringIO()
    seen = []
    settings = object()
    monkeypatch.setattr(eval_live, "_revision", lambda: "a" * 40)
    monkeypatch.setattr(eval_live, "_dirty", lambda: True)

    async def evaluate(config, owner, provider, python_run, node_run, **bounds):
        seen.append((config, str(owner), provider, str(python_run), str(node_run), bounds))
        return {"protocol_smoke_ready": True, "independent_hidden_checks": "not_run"}

    result = eval_live.main(
        arguments, settings_loader=lambda: settings, evaluator=evaluate, stdout=output
    )
    assert result == 0
    assert seen == [
        (
            settings,
            arguments[3],
            "openai",
            arguments[5],
            arguments[7],
            {
                "execute": True,
                "max_jobs": 7,
                "max_seconds": 60.0,
            },
        )
    ]
    report = json.loads(output.getvalue())
    assert report["execution_requested"] is True
    assert report["schema_version"] == 1
    assert report["kind"] == "live_protocol_smoke"
    assert report["reported_at"].endswith("+00:00")
    assert report["task_correctness"] == "not_assessed"
    assert report["source_revision"] == "a" * 40
    assert report["working_tree_dirty"] is True
    assert report["independent_hidden_checks"] == "not_run"


@pytest.mark.parametrize("state", ["passed", "failed", "invalid", "unknown", "partial"])
def test_cli_preserves_known_evaluator_hidden_state(state: str) -> None:
    output = io.StringIO()
    result = eval_live.main(
        _paid_args(),
        settings_loader=lambda: object(),
        evaluator=lambda *_args, **_kwargs: {
            "protocol_smoke_ready": True,
            "independent_hidden_checks": state,
        },
        stdout=output,
    )
    assert result == 0
    assert json.loads(output.getvalue())["independent_hidden_checks"] == state


def test_cli_discards_unrecognized_evaluator_hidden_state() -> None:
    output = io.StringIO()
    eval_live.main(
        _paid_args(),
        settings_loader=lambda: object(),
        evaluator=lambda *_args, **_kwargs: {
            "protocol_smoke_ready": True,
            "independent_hidden_checks": "candidate_claimed_pass",
        },
        stdout=output,
    )
    assert json.loads(output.getvalue())["independent_hidden_checks"] == "not_run"


def test_incomplete_drain_does_not_pass_live_gate() -> None:
    output = io.StringIO()
    result = eval_live.main(
        _paid_args(),
        settings_loader=lambda: object(),
        evaluator=lambda *_args, **_kwargs: {
            "outcome": "incomplete",
            "protocol_smoke_ready": False,
        },
        stdout=output,
    )
    assert result == 3
    assert json.loads(output.getvalue())["outcome"] == "incomplete"


def test_observation_needs_no_spend_flags_and_writes_new_private_file(tmp_path) -> None:
    path = tmp_path / "report.json"
    seen = []
    result = eval_live.main(
        _args("--observe-only", "--output", str(path)),
        settings_loader=lambda: object(),
        evaluator=lambda *_args, **options: seen.append(options) or {"protocol_smoke_ready": False},
        stdout=io.StringIO(),
    )
    assert result == 0
    assert seen[0]["execute"] is False
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text())["execution_requested"] is False


def test_existing_report_is_never_overwritten_or_executed(tmp_path) -> None:
    path = tmp_path / "report.json"
    path.write_text("original evidence")
    output = io.StringIO()
    result = eval_live.main(
        _paid_args("--output", str(path)), settings_loader=_no_settings, stdout=output
    )
    assert result == 2
    assert path.read_text() == "original evidence"
    assert json.loads(output.getvalue())["error_code"] == "live_evaluation_output_unavailable"


def test_duplicate_run_ids_refuse_before_settings() -> None:
    run_id = str(uuid4())
    arguments = [
        "--provider",
        "openai",
        "--account-id",
        str(uuid4()),
        "--python-run-id",
        run_id,
        "--node-run-id",
        run_id,
        "--observe-only",
    ]
    output = io.StringIO()
    assert eval_live.main(arguments, settings_loader=_no_settings, stdout=output) == 2
    assert json.loads(output.getvalue())["error_code"] == "live_evaluation_distinct_runs_required"


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--max-jobs", "0"),
        ("--max-jobs", "101"),
        ("--max-seconds", "nan"),
        ("--max-seconds", "inf"),
        ("--max-seconds", "301"),
    ],
)
def test_invalid_bounds_refuse_before_settings(flag: str, value: str) -> None:
    with pytest.raises(SystemExit) as error:
        eval_live.main(_paid_args(flag, value), settings_loader=_no_settings)
    assert error.value.code == 2


def test_evaluator_failure_is_sanitized_and_never_claims_zero_calls() -> None:
    output = io.StringIO()

    def failure(*_args, **_kwargs):
        raise RuntimeError("private-provider-key")

    result = eval_live.main(
        _paid_args(), settings_loader=lambda: object(), evaluator=failure, stdout=output
    )
    assert result == 1
    report = json.loads(output.getvalue())
    assert report == {"outcome": "unknown", "error_code": "live_evaluation_failed"}
    assert "private-provider-key" not in output.getvalue()


def test_interruption_records_unknown_outcome_in_private_report(tmp_path) -> None:
    path = tmp_path / "interrupted.json"

    async def interrupted(*_args, **_kwargs):
        raise asyncio.CancelledError

    result = eval_live.main(
        _paid_args("--output", str(path)),
        settings_loader=lambda: object(),
        evaluator=interrupted,
        stdout=io.StringIO(),
    )
    assert result == 130
    assert json.loads(path.read_text()) == {
        "outcome": "unknown",
        "error_code": "live_evaluation_interrupted",
    }
