"""Run the local failure matrix serially; never report deployed gaps as passed."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import platform
import signal
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs/fault-matrix.json"


def _run_bounded(
    command: list[str], environment: dict[str, str]
) -> subprocess.CompletedProcess[str]:
    with subprocess.Popen(
        command,
        cwd=ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    ) as process:
        try:
            stdout, stderr = process.communicate(timeout=600)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                stdout, stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
            return subprocess.CompletedProcess(
                command, 124, stdout, stderr + "\nfault_group_timeout"
            )
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path(".artifacts/fault-matrix.json"))
    parser.add_argument(
        "--groups", nargs="*", help="Optional named subset; omitted groups remain NOT_RUN"
    )
    args = parser.parse_args()
    matrix = json.loads(MANIFEST.read_text())
    if os.environ.get("CONFIRM_DISPOSABLE_FAULT_DATABASES") != "1":
        print("explicit disposable PostgreSQL cluster confirmation required", file=sys.stderr)
        return 2
    environment = os.environ.copy()
    if any(
        environment.get(name)
        for name in (
            "PGHOST",
            "PGHOSTADDR",
            "PGPORT",
            "PGDATABASE",
            "PGSERVICE",
            "PGSERVICEFILE",
            "PGOPTIONS",
        )
    ):
        print("libpq routing overrides are not allowed", file=sys.stderr)
        return 2
    base = environment.get("CONTINUUM_FAULT_DATABASE_BASE_URL")
    selected = args.groups or list(matrix["groups"])
    if any(group not in matrix["groups"] for group in selected):
        print("unknown fault group", file=sys.stderr)
        return 2
    try:
        for name in selected:
            group = matrix["groups"][name]
            variable = group["database_env"]
            if variable is None:
                continue
            if not environment.get(variable) and base:
                environment[variable] = (
                    make_url(base)
                    .set(database=group["database_name"])
                    .render_as_string(hide_password=False)
                )
            url = make_url(environment.get(variable, ""))
            if (
                url.host not in {"127.0.0.1", "localhost", "::1"}
                or url.database != group["database_name"]
            ):
                raise ValueError
    except Exception:
        print(
            "matrix requires the documented disposable loopback PostgreSQL databases",
            file=sys.stderr,
        )
        return 2
    results = {}
    with Path("/tmp/continuum-remaining-pg-test.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with tempfile.TemporaryDirectory(prefix="continuum-fault-matrix-") as temporary:
            for name in selected:
                group = matrix["groups"][name]
                junit = Path(temporary) / f"{name}.xml"
                completed = _run_bounded(
                    [
                        sys.executable,
                        str(ROOT / "scripts/run_tests.py"),
                        "-q",
                        group["node"],
                        f"--junitxml={junit}",
                    ],
                    environment,
                )
                cases = ET.parse(junit).getroot().findall(".//testcase") if junit.exists() else []
                skipped = sum(case.find("skipped") is not None for case in cases)
                passed = completed.returncode == 0 and bool(cases) and skipped == 0
                observations = [
                    {
                        "test": case.attrib.get("name"),
                        "properties": {
                            p.attrib["name"]: p.attrib.get("value")
                            for p in case.findall("./properties/property")
                        },
                    }
                    for case in cases
                ]
                results[name] = {
                    "outcome": "passed" if passed else "failed",
                    "tests": len(cases),
                    "skipped": skipped,
                    "authoritative_observations": observations,
                    "source_sha256": hashlib.sha256(
                        (ROOT / group["node"]).read_bytes()
                    ).hexdigest(),
                }
                print(f"{name}: {results[name]['outcome']} ({len(cases)} cases)", flush=True)
                if not passed:
                    # Tests use sanitized errors; keep output local for diagnosis.
                    (Path(temporary) / f"{name}.log").write_text(
                        completed.stdout + completed.stderr
                    )
                    print((completed.stdout + completed.stderr)[-6000:], file=sys.stderr)
                    break
    rows = []
    for row in matrix["rows"]:
        statuses = [results.get(group, {}).get("outcome", "not_run") for group in row["groups"]]
        outcome = (
            "not_run_deployed"
            if not statuses
            else (
                "passed_local_assertions"
                if all(s == "passed" for s in statuses)
                else "not_verified"
            )
        )
        rows.append({**row, "outcome": outcome})
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "python": platform.python_version(),
        "groups": results,
        "matrix": rows,
        "limitations": [
            "Local fixtures do not establish deployed failure behavior.",
            "Rows are backed by test assertions; properties contain measured "
            "intermediate/final states where instrumented.",
            "No credentials, prompts, or source bodies are recorded.",
        ],
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    return (
        0
        if all(result["outcome"] == "passed" for result in results.values())
        and len(results) == len(selected)
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
