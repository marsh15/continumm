"""Explicit, bounded protocol smokes for two already-admitted provider runs."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO
from uuid import UUID

from continuum_api.runtime.activity_runner import MAX_RUNNER_JOBS, MAX_RUNNER_SECONDS

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SAFE_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


def main(
    argv: list[str] | None = None,
    *,
    settings_loader: Callable[[], Any] | None = None,
    evaluator: Callable[..., Any] | None = None,
    stdout: TextIO | None = None,
) -> int:
    options = _parser().parse_args(argv)
    output = stdout or sys.stdout
    if not (options.execute_live or options.observe_only):
        _emit(output, {"outcome": "not_run", "error_code": "live_evaluation_mode_required"})
        return 2
    if options.execute_live and not (
        options.allow_model_charges and options.allow_operator_sandbox
    ):
        _emit(output, {"outcome": "not_run", "error_code": "live_evaluation_cost_opt_in_required"})
        return 2
    if not all((options.account_id, options.python_run_id, options.node_run_id)):
        _parser().error("--account-id, --python-run-id and --node-run-id are required")
    if options.python_run_id == options.node_run_id:
        _emit(
            output, {"outcome": "not_run", "error_code": "live_evaluation_distinct_runs_required"}
        )
        return 2

    report_file: TextIO | None = None
    try:
        if options.output is not None:
            options.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            descriptor = os.open(options.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            report_file = os.fdopen(descriptor, "w", encoding="utf-8")
    except OSError:
        _emit(output, {"outcome": "not_run", "error_code": "live_evaluation_output_unavailable"})
        return 2

    try:
        try:
            if settings_loader is None:
                from continuum_api.settings import get_settings

                settings_loader = get_settings
            if evaluator is None:
                from continuum_api.runtime.live_evaluation import run_live_evaluation

                evaluator = run_live_evaluation
            settings = settings_loader()
            report = asyncio.run(_evaluate(evaluator, settings, options))
            if not isinstance(report, dict):
                raise ValueError("invalid_report")
            report = {
                **report,
                "schema_version": 1,
                "kind": "live_protocol_smoke",
                "reported_at": datetime.now(UTC).isoformat(),
                "independent_hidden_checks": "not_run",
                "task_correctness": "not_assessed",
                "execution_requested": options.execute_live,
                "source_revision": _revision(),
                "working_tree_dirty": _dirty(),
            }
            # Serialize before writing so invalid values cannot manufacture a partial success.
            encoded = json.dumps(report, allow_nan=False, sort_keys=True, indent=2) + "\n"
            status = 0 if options.observe_only or report.get("protocol_smoke_ready") is True else 3
        except (asyncio.CancelledError, KeyboardInterrupt):
            encoded = (
                json.dumps({"outcome": "unknown", "error_code": "live_evaluation_interrupted"})
                + "\n"
            )
            status = 130
        except Exception as error:
            code = getattr(error, "code", None)
            if not isinstance(code, str) or _SAFE_CODE.fullmatch(code) is None:
                code = "live_evaluation_failed"
            encoded = json.dumps({"outcome": "unknown", "error_code": code}) + "\n"
            status = 1
        if report_file is not None:
            report_file.write(encoded)
            report_file.flush()
            report_file.close()
            report_file = None
        output.write(encoded)
        return status
    except OSError:
        _emit(output, {"outcome": "unknown", "error_code": "live_evaluation_output_failed"})
        return 1
    finally:
        if report_file is not None:
            try:
                report_file.close()
            except OSError:
                pass


async def _evaluate(evaluator: Callable[..., Any], settings: Any, options: Any) -> Any:
    report = evaluator(
        settings,
        options.account_id,
        options.provider,
        options.python_run_id,
        options.node_run_id,
        execute=options.execute_live,
        max_jobs=options.max_jobs,
        max_seconds=options.max_seconds,
    )
    return await report if inspect.isawaitable(report) else report


def _revision() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        value = result.stdout.strip()
        return value if result.returncode == 0 and re.fullmatch(r"[0-9a-f]{40,64}", value) else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def _dirty() -> bool | None:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return bool(result.stdout.strip()) if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def _emit(output: TextIO, value: dict[str, Any]) -> None:
    output.write(json.dumps(value, sort_keys=True) + "\n")


def _jobs(value: str) -> int:
    result = int(value)
    if not 1 <= result <= MAX_RUNNER_JOBS:
        raise argparse.ArgumentTypeError(f"must be between 1 and {MAX_RUNNER_JOBS}")
    return result


def _seconds(value: str) -> float:
    result = float(value)
    if not 0 < result <= MAX_RUNNER_SECONDS:
        raise argparse.ArgumentTypeError(f"must be greater than 0 and at most {MAX_RUNNER_SECONDS}")
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=("openai", "anthropic"), required=True)
    parser.add_argument("--account-id", type=UUID)
    parser.add_argument("--python-run-id", type=UUID)
    parser.add_argument("--node-run-id", type=UUID)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--execute-live", action="store_true")
    modes.add_argument("--observe-only", action="store_true")
    parser.add_argument("--allow-model-charges", action="store_true")
    parser.add_argument("--allow-operator-sandbox", action="store_true")
    parser.add_argument(
        "--max-jobs", type=_jobs, default=1, help="per run, not shared between runs"
    )
    parser.add_argument("--max-seconds", type=_seconds, default=30.0, help="per run")
    parser.add_argument(
        "--output", type=Path, help="new private report file; existing files are refused"
    )
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
