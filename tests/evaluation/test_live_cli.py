"""Live evaluation CLI gates and private report handling without hosted calls."""

from __future__ import annotations

import asyncio
import io
import json
import sys
import types
from uuid import uuid4

import pytest

from continuum_api.sandbox.hidden_probe_recipe import HiddenProbe
from continuum_api.validation.hidden_checks import HiddenCheckCase, HiddenCheckSuite
from continuum_api.validation.managed_hidden_checks import _invocation_digest
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


class _Catalog:
    file_sha256 = "f" * 64

    def __call__(self, *_args, **_kwargs):
        return True

    def report_matches(self, report):
        accepted = report.get("accepted_hidden_checks", {})
        return {name: accepted.get(name) is True for name in ("python", "node")}


def _catalog_import(monkeypatch, loader):
    module = types.ModuleType("continuum_api.runtime.hidden_suite_catalog")
    module.load_hidden_suites = loader
    monkeypatch.setitem(sys.modules, module.__name__, module)


def _write_real_catalog(path, arguments) -> None:
    document = {
        "schema_version": 1,
        "account_id": arguments[3],
        "provider": arguments[1],
        "suites": [],
    }
    for run_id, profile_id, entry_path in (
        (arguments[5], "python-pytest", "src/solution.py"),
        (arguments[7], "node-npm", "src/solution.cjs"),
    ):
        document["suites"].append(
            {
                "run_id": run_id,
                "repository_id": 123,
                "base_commit_sha": "a" * 40,
                "profile_id": profile_id,
                "profile_version": "1",
                "suite_id": f"{profile_id}-suite",
                "entry_path": entry_path,
                "checks": [{"check_id": "case-1", "input": [1], "expected": {"ok": True}}],
            }
        )
    path.write_text(json.dumps(document), encoding="utf-8")
    path.chmod(0o600)


def _matching_snapshot(run_id, profile_id, suite_id, entry_path):
    checks = (HiddenCheckCase("case-1", "[1]", '{"ok":true}'),)
    suite = HiddenCheckSuite(suite_id, profile_id, "1", "b" * 64, "c" * 64, checks)
    probe = HiddenProbe(profile_id, "1", entry_path, suite.public_inputs_json())
    return {
        "independent_hidden_checks": "passed",
        "run_id": run_id,
        "profile_id": profile_id,
        "profile_version": "1",
        "hidden_check_suite_id": suite_id,
        "hidden_check_suite_sha256": suite.digest,
        "hidden_check_count": 1,
        "hidden_check_passed_count": 1,
        "hidden_check_repository_id": 123,
        "hidden_check_base_commit_sha": "a" * 40,
        "hidden_check_candidate_archive_sha256": "b" * 64,
        "hidden_check_candidate_tree_sha256": "c" * 64,
        "hidden_check_invocation_sha256": _invocation_digest(
            suite.digest, entry_path, probe.command_id
        ),
    }


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
    assert report["hidden_checks_requested"] is False
    assert "hidden_suite_matches" not in report


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


def test_hidden_suites_are_rejected_for_observation_before_loader_and_report_creation(
    tmp_path, monkeypatch
) -> None:
    report_path = tmp_path / "report.json"
    _catalog_import(monkeypatch, lambda *_args, **_kwargs: pytest.fail("catalog must not load"))
    output = io.StringIO()
    result = eval_live.main(
        _args(
            "--observe-only",
            "--hidden-suites",
            str(tmp_path / "suites.json"),
            "--output",
            str(report_path),
        ),
        settings_loader=_no_settings,
        stdout=output,
    )
    assert result == 2
    assert json.loads(output.getvalue()) == {
        "outcome": "not_run",
        "error_code": "live_hidden_suites_execute_required",
    }
    assert not report_path.exists()


def test_invalid_hidden_suite_catalog_is_rejected_before_settings_and_report_creation(
    tmp_path, monkeypatch
) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir(exist_ok=True)
    monkeypatch.setattr(eval_live, "PROJECT_ROOT", checkout)
    report_path = tmp_path / "report.json"
    suite_path = tmp_path / "private-suites.json"
    suite_path.write_text('{"private_expected_value":"do not disclose"}')
    suite_path.chmod(0o600)
    output = io.StringIO()
    result = eval_live.main(
        _paid_args("--hidden-suites", str(suite_path), "--output", str(report_path)),
        settings_loader=_no_settings,
        stdout=output,
    )
    assert result == 2
    assert json.loads(output.getvalue()) == {
        "outcome": "not_run",
        "error_code": "live_hidden_suites_invalid",
    }
    assert str(suite_path) not in output.getvalue()
    assert "private_expected_value" not in output.getvalue()
    assert not report_path.exists()


def test_hidden_suite_identity_mismatch_uses_real_catalog_loader(tmp_path, monkeypatch) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir(exist_ok=True)
    monkeypatch.setattr(eval_live, "PROJECT_ROOT", checkout)
    suite_path = tmp_path / "private-suites.json"
    arguments = _paid_args("--hidden-suites", str(suite_path))
    _write_real_catalog(suite_path, arguments)
    payload = json.loads(suite_path.read_text())
    payload["account_id"] = str(uuid4())
    suite_path.write_text(json.dumps(payload), encoding="utf-8")
    suite_path.chmod(0o600)
    output = io.StringIO()

    result = eval_live.main(arguments, settings_loader=_no_settings, stdout=output)

    assert result == 2
    assert json.loads(output.getvalue()) == {
        "outcome": "not_run",
        "error_code": "live_hidden_suites_invalid",
    }
    assert str(suite_path) not in output.getvalue()


def test_hidden_suite_selector_and_match_metadata_gate_success(tmp_path, monkeypatch) -> None:
    catalog = _Catalog()
    suite_path = tmp_path / "suites.json"
    suite_path.write_text("{}")
    suite_path.chmod(0o600)
    observed_load = []

    def load(path, **identity):
        observed_load.append((path, identity))
        return catalog

    _catalog_import(monkeypatch, load)
    seen = []

    def evaluator(*args, **kwargs):
        seen.append((args, kwargs))
        return {
            "outcome": "protocol_smoke_ready",
            "protocol_smoke_ready": True,
            "accepted_hidden_checks": {"python": True, "node": True},
        }

    output = io.StringIO()
    arguments = _paid_args("--hidden-suites", str(suite_path))
    result = eval_live.main(
        arguments, settings_loader=lambda: object(), evaluator=evaluator, stdout=output
    )
    assert result == 0
    assert observed_load[0][0] == suite_path
    assert observed_load[0][1]["project_root"] == eval_live.PROJECT_ROOT
    assert seen[0][1]["hidden_check_selector"] is catalog
    report = json.loads(output.getvalue())
    assert report["hidden_checks_requested"] is True
    assert report["hidden_suites_sha256"] == catalog.file_sha256
    assert report["hidden_suite_matches"] == {"python": True, "node": True}
    assert report["hidden_suite_ready"] is True
    assert report["task_correctness"] == "not_assessed"


@pytest.mark.parametrize(
    "accepted,expected_outcome",
    [
        ({"python": True, "node": False}, "incomplete"),
        ({"python": True}, "incomplete"),
    ],
)
def test_protocol_ready_without_both_hidden_suites_returns_incomplete(
    tmp_path, monkeypatch, accepted, expected_outcome
) -> None:
    suite_path = tmp_path / "suites.json"
    suite_path.write_text("{}")
    _catalog_import(monkeypatch, lambda *_args, **_kwargs: _Catalog())
    output = io.StringIO()
    result = eval_live.main(
        _paid_args("--hidden-suites", str(suite_path)),
        settings_loader=lambda: object(),
        evaluator=lambda *_args, **_kwargs: {
            "outcome": "protocol_smoke_ready",
            "protocol_smoke_ready": True,
            "accepted_hidden_checks": accepted,
        },
        stdout=output,
    )
    assert result == 3
    report = json.loads(output.getvalue())
    assert report["outcome"] == expected_outcome
    assert report["protocol_smoke_ready"] is True
    assert report["hidden_suite_ready"] is False


def test_hidden_suite_mismatch_preserves_failed_outcome(tmp_path, monkeypatch) -> None:
    suite_path = tmp_path / "suites.json"
    suite_path.write_text("{}")
    _catalog_import(monkeypatch, lambda *_args, **_kwargs: _Catalog())
    output = io.StringIO()
    result = eval_live.main(
        _paid_args("--hidden-suites", str(suite_path)),
        settings_loader=lambda: object(),
        evaluator=lambda *_args, **_kwargs: {
            "outcome": "failed",
            "protocol_smoke_ready": False,
            "accepted_hidden_checks": {"python": False, "node": False},
        },
        stdout=output,
    )
    assert result == 3
    assert json.loads(output.getvalue())["outcome"] == "failed"


def test_real_catalog_matches_only_the_exact_suite_snapshot(tmp_path, monkeypatch) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir(exist_ok=True)
    monkeypatch.setattr(eval_live, "PROJECT_ROOT", checkout)
    suite_path = tmp_path / "suites.json"
    arguments = _paid_args("--hidden-suites", str(suite_path))
    _write_real_catalog(suite_path, arguments)
    node_snapshot = _matching_snapshot(arguments[7], "node-npm", "node-npm-suite", "src/other.cjs")
    output = io.StringIO()

    result = eval_live.main(
        arguments,
        settings_loader=lambda: object(),
        evaluator=lambda *_args, **_kwargs: {
            "outcome": "protocol_smoke_ready",
            "protocol_smoke_ready": True,
            "cases": {
                "python": {
                    "snapshot": _matching_snapshot(
                        arguments[5], "python-pytest", "python-pytest-suite", "src/solution.py"
                    )
                },
                "node": {"snapshot": node_snapshot},
            },
        },
        stdout=output,
    )

    assert result == 3
    report = json.loads(output.getvalue())
    assert report["hidden_suite_matches"] == {"python": True, "node": False}
    assert report["hidden_suite_ready"] is False
    assert report["outcome"] == "incomplete"


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


def test_private_catalog_and_both_exact_accepted_suites_qualify(tmp_path, monkeypatch):
    import hashlib

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    monkeypatch.setattr(eval_live, "PROJECT_ROOT", checkout)
    path = tmp_path / "suites.json"
    arguments = _paid_args("--hidden-suites", str(path))
    _write_real_catalog(path, arguments)
    seen = []

    def evaluator(*args, **kwargs):
        seen.append(kwargs["hidden_check_selector"])
        return {
            "protocol_smoke_ready": True,
            "outcome": "protocol_smoke_ready",
            "independent_hidden_checks": "passed",
            "cases": {
                "python": {
                    "snapshot": _matching_snapshot(
                        arguments[5], "python-pytest", "python-pytest-suite", "src/solution.py"
                    )
                },
                "node": {
                    "snapshot": _matching_snapshot(
                        arguments[7], "node-npm", "node-npm-suite", "src/solution.cjs"
                    )
                },
            },
        }

    output = io.StringIO()
    assert (
        eval_live.main(
            arguments, settings_loader=lambda: object(), evaluator=evaluator, stdout=output
        )
        == 0
    )
    report = json.loads(output.getvalue())
    assert callable(seen[0])
    assert report["hidden_suite_matches"] == {"python": True, "node": True}
    assert report["hidden_suite_ready"] is True
    assert report["hidden_suites_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert report["task_correctness"] == "not_assessed"
    assert "expected" not in output.getvalue()
    assert str(path) not in output.getvalue()
