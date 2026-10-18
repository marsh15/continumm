"""Run the bounded local T34 fault and review demonstration."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_RUNNER = PROJECT_ROOT / "scripts" / "run_tests.py"
CONFIRMATION = "CONFIRM_DISPOSABLE_FAULT_DATABASES"
DATABASES = {
    "CONTINUUM_CHECKPOINT_D_TEST_DATABASE_URL": "continuum_checkpoint_d_test",
    "CONTINUUM_T22_TEST_DATABASE_URL": "continuum_t22_test",
}
LIBPQ_ROUTING_VARIABLES = (
    "PGHOST",
    "PGHOSTADDR",
    "PGPORT",
    "PGDATABASE",
    "PGSERVICE",
    "PGSERVICEFILE",
    "PGOPTIONS",
)
RETAINED_CREDENTIAL_PATH = "CONTINUUM_CHECKPOINT_D_RETAIN_PATH"


@dataclass(frozen=True, slots=True)
class DemoCase:
    case_id: str
    node_id: str


CASES = (
    DemoCase(
        "worker_sigkill_checkpoint_recovery",
        "tests/integration/test_runtime_demonstration.py::"
        "test_sigkill_keeps_checkpoint_and_only_recovered_generation_commits",
    ),
    DemoCase(
        "paused_stale_worker_rejected",
        "tests/integration/test_runtime_demonstration.py::"
        "test_sigstop_rejects_stale_intermediate_and_final_writes",
    ),
    DemoCase(
        "lost_pr_create_response_reconciled",
        "tests/faults/test_pr_reconciliation.py::"
        "test_lost_create_response_and_repeated_invisible_reads_never_create_twice",
    ),
    DemoCase(
        "review_revision_approval_and_normal_publication",
        "tests/integration/test_end_to_end_demonstration.py::"
        "test_local_review_to_publication_attention_and_reconciliation[normal]",
    ),
    DemoCase(
        "review_revision_approval_and_response_lost_reconciliation",
        "tests/integration/test_end_to_end_demonstration.py::"
        "test_local_review_to_publication_attention_and_reconciliation[response-lost]",
    ),
)
FAULT_CASES = CASES[:3]


class DemoConfigError(ValueError):
    """The local demonstration is not safely configured."""


def validate_environment(environment: Mapping[str, str]) -> None:
    if environment.get(CONFIRMATION) != "1":
        raise DemoConfigError("explicit disposable-database confirmation is required")
    if any(environment.get(variable) for variable in LIBPQ_ROUTING_VARIABLES):
        raise DemoConfigError("libpq routing environment overrides are not allowed")
    if environment.get(RETAINED_CREDENTIAL_PATH):
        raise DemoConfigError("retained runtime credentials are not allowed for this demo")

    for variable, expected_database in DATABASES.items():
        raw_url = environment.get(variable)
        if not raw_url:
            raise DemoConfigError(f"{variable} is required")
        try:
            url = make_url(raw_url)
        except (ArgumentError, TypeError, ValueError):
            raise DemoConfigError(f"{variable} is not a valid PostgreSQL URL") from None
        if url.get_backend_name() != "postgresql":
            raise DemoConfigError(f"{variable} must use PostgreSQL")
        if url.query:
            raise DemoConfigError(f"{variable} must not include URL query parameters")
        if url.host not in {"127.0.0.1", "localhost", "::1"}:
            raise DemoConfigError(f"{variable} must use a loopback host")
        if url.database != expected_database:
            raise DemoConfigError(f"{variable} must target {expected_database}")


def junit_result(path: Path, returncode: int) -> str:
    """Return one actual pytest outcome without exposing captured test output."""
    if not path.is_file():
        return "failed"
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return "failed"
    testcases = root.findall(".//testcase")
    if len(testcases) != 1:
        return "failed"
    testcase = testcases[0]
    if testcase.find("skipped") is not None:
        return "skipped"
    if testcase.find("failure") is not None or testcase.find("error") is not None:
        return "failed"
    return "passed" if returncode == 0 else "failed"


def make_report(
    commit: str,
    worktree_dirty: bool,
    timestamp: str,
    results: list[dict[str, str]],
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "commit": commit,
        "worktree_dirty": worktree_dirty,
        "tested_source_reproducible": not worktree_dirty,
        "timestamp_utc": timestamp,
        "local_only": True,
        "live_not_run": True,
        "result": "passed" if all(case["result"] == "passed" for case in results) else "failed",
        "cases": results,
    }


def _repository_state() -> tuple[str, bool]:
    result = subprocess.run(
        ["git", "-C", str(PROJECT_ROOT), "rev-parse", "--verify", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    commit = result.stdout.strip()
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise RuntimeError("unable to resolve the repository commit id")
    status = subprocess.run(
        ["git", "-C", str(PROJECT_ROOT), "status", "--porcelain", "--untracked-files=all"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return commit, bool(status.stdout.strip())


def _run_case(case: DemoCase) -> str:
    with tempfile.TemporaryDirectory(prefix="continuum-demo-local-") as temporary:
        junit_path = Path(temporary) / "result.xml"
        try:
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(TEST_RUNNER),
                    "-q",
                    f"--junitxml={junit_path}",
                    case.node_id,
                ],
                cwd=PROJECT_ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            try:
                _stdout, _stderr = process.communicate(timeout=300)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.communicate()
                return "failed"
        except OSError:
            return "failed"
        return junit_result(junit_path, process.returncode)


def _preflight_output(path: Path | None) -> None:
    if path is None:
        return
    if not path.parent.is_dir() or path.exists() or path.is_symlink():
        raise DemoConfigError("report output must name a new file in an existing directory")


def _write_report(encoded: str, path: Path | None) -> None:
    if path is None:
        print(encoded, end="")
        return
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as report_file:
        report_file.write(encoded)
        report_file.flush()
        os.fsync(report_file.fileno())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="write the evidence JSON to a new file (default: stdout)",
    )
    parser.add_argument(
        "--faults-only",
        action="store_true",
        help="run only the worker-death, stale-worker, and lost-response proofs",
    )
    arguments = parser.parse_args(argv)
    try:
        validate_environment(os.environ)
        _preflight_output(arguments.output)
        commit, worktree_dirty = _repository_state()
    except (DemoConfigError, OSError, RuntimeError, subprocess.SubprocessError) as exc:
        print(f"demo-local: {exc}", file=sys.stderr)
        return 2

    results: list[dict[str, str]] = []
    cases = FAULT_CASES if arguments.faults_only else CASES
    for case in cases:
        outcome = _run_case(case)
        results.append({"id": case.case_id, "result": outcome})
        print(f"{case.case_id}: {outcome}", file=sys.stderr)

    report = make_report(
        commit,
        worktree_dirty,
        datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        results,
    )
    encoded = json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n"
    if len(encoded.encode("utf-8")) > 4096:
        print("demo-local: evidence report exceeded its size limit", file=sys.stderr)
        return 2
    try:
        _write_report(encoded, arguments.output)
    except OSError:
        print("demo-local: unable to write the evidence report", file=sys.stderr)
        return 2
    return 0 if report["result"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
