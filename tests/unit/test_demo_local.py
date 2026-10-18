from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.demo_local import (
    CASES,
    FAULT_CASES,
    DemoCase,
    DemoConfigError,
    _run_case,
    _write_report,
    junit_result,
    make_report,
    validate_environment,
)


def _environment() -> dict[str, str]:
    return {
        "CONFIRM_DISPOSABLE_FAULT_DATABASES": "1",
        "CONTINUUM_CHECKPOINT_D_TEST_DATABASE_URL": (
            "postgresql://demo:secret@127.0.0.1/continuum_checkpoint_d_test"
        ),
        "CONTINUUM_T22_TEST_DATABASE_URL": "postgresql://demo:secret@localhost/continuum_t22_test",
    }


def test_demo_guard_requires_explicit_confirmation() -> None:
    environment = _environment()
    environment["CONFIRM_DISPOSABLE_FAULT_DATABASES"] = "0"

    with pytest.raises(DemoConfigError, match="explicit disposable-database confirmation"):
        validate_environment(environment)


@pytest.mark.parametrize(
    ("variable", "value", "message"),
    [
        (
            "CONTINUUM_CHECKPOINT_D_TEST_DATABASE_URL",
            "postgresql://demo:secret@example.com/continuum_checkpoint_d_test",
            "loopback host",
        ),
        (
            "CONTINUUM_T22_TEST_DATABASE_URL",
            "postgresql://demo:secret@127.0.0.1/production",
            "continuum_t22_test",
        ),
        (
            "CONTINUUM_T22_TEST_DATABASE_URL",
            "not a database url with password=secret",
            "valid PostgreSQL URL",
        ),
    ],
)
def test_demo_guard_rejects_unsafe_database_targets(
    variable: str, value: str, message: str
) -> None:
    environment = _environment()
    environment[variable] = value

    with pytest.raises(DemoConfigError, match=message) as failure:
        validate_environment(environment)

    assert "secret" not in str(failure.value)


@pytest.mark.parametrize(
    "variable",
    ["PGHOST", "PGHOSTADDR", "PGPORT", "PGDATABASE", "PGSERVICE", "PGSERVICEFILE", "PGOPTIONS"],
)
def test_demo_guard_rejects_libpq_routing_overrides(variable: str) -> None:
    environment = _environment()
    environment[variable] = "redirected"

    with pytest.raises(DemoConfigError, match="routing environment overrides"):
        validate_environment(environment)


def test_demo_guard_rejects_url_query_routing_and_retention_path() -> None:
    environment = _environment()
    environment["CONTINUUM_T22_TEST_DATABASE_URL"] += "?hostaddr=203.0.113.4"
    with pytest.raises(DemoConfigError, match="URL query parameters"):
        validate_environment(environment)

    environment = _environment()
    environment["CONTINUUM_CHECKPOINT_D_RETAIN_PATH"] = "/tmp/retained"
    with pytest.raises(DemoConfigError, match="retained runtime credentials"):
        validate_environment(environment)


@pytest.mark.parametrize(
    ("xml", "returncode", "expected"),
    [
        ('<testsuite><testcase name="case" /></testsuite>', 0, "passed"),
        ('<testsuite><testcase name="case"><skipped /></testcase></testsuite>', 0, "skipped"),
        (
            '<testsuite><testcase name="case"><failure /></testcase></testsuite>',
            1,
            "failed",
        ),
        ('<testsuite><testcase name="case" /></testsuite>', 1, "failed"),
    ],
)
def test_junit_result_uses_actual_case_outcome(
    tmp_path: Path, xml: str, returncode: int, expected: str
) -> None:
    report = tmp_path / "result.xml"
    report.write_text(xml, encoding="utf-8")

    assert junit_result(report, returncode) == expected


def test_run_case_executes_runner_and_reads_its_junit_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner = tmp_path / "fake_runner.py"
    runner.write_text(
        "import pathlib, sys\n"
        "report = next(arg.split('=', 1)[1] for arg in sys.argv if arg.startswith('--junitxml='))\n"
        "pathlib.Path(report).write_text('<testsuite><testcase name=\"demo\" /></testsuite>')\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.demo_local.TEST_RUNNER", runner)

    assert _run_case(DemoCase("smoke", "tests/example.py::test_case")) == "passed"


def test_evidence_report_is_bounded_and_contains_only_case_results() -> None:
    cases = [{"id": case.case_id, "result": "passed"} for case in CASES]
    report = make_report("a" * 40, True, "2026-10-18T00:00:00Z", cases)
    encoded = json.dumps(report, sort_keys=True)

    assert report["local_only"] is True
    assert report["live_not_run"] is True
    assert report["result"] == "passed"
    assert report["worktree_dirty"] is True
    assert report["tested_source_reproducible"] is False
    assert len(cases) == 5
    assert len(FAULT_CASES) == 3
    assert len(encoded) < 4096
    assert "secret" not in encoded


def test_report_output_is_new_private_file(tmp_path: Path) -> None:
    target = tmp_path / "evidence.json"

    _write_report("{}\n", target)

    assert target.read_text(encoding="utf-8") == "{}\n"
    assert target.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        _write_report("{}\n", target)
