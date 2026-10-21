"""Immutable hidden-oracle contracts and digest-only comparison receipts.

This module validates output from an already isolated runner. It never executes
candidate code, accesses a network, or accepts a candidate-provided pass claim.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

MAX_JSON_BYTES = 65_536
MAX_CASES = 32
MAX_NESTING = 16
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_SAFE_PROFILES = {"python-pytest": "1", "node-npm": "1"}
_MAX_SAFE_INTEGER = 2**53 - 1


class HiddenCheckError(ValueError):
    """Stable contract error that does not include supplied data."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _pairs_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise HiddenCheckError("hidden_json_duplicate_key")
        result[key] = value
    return result


def _reject_constant(_: str) -> Any:
    raise HiddenCheckError("hidden_json_non_finite_number")


def _parse_float(token: str) -> float:
    try:
        exact = Decimal(token)
        value = float(token)
    except (InvalidOperation, OverflowError, ValueError):
        raise HiddenCheckError("hidden_json_number_precision") from None
    if not math.isfinite(value) or exact != Decimal(str(value)):
        raise HiddenCheckError("hidden_json_number_precision")
    return value


def _check_value(value: Any, depth: int = 0) -> None:
    if isinstance(value, (dict, list)):
        if depth >= MAX_NESTING:
            raise HiddenCheckError("hidden_json_nesting_limit")
        if isinstance(value, dict):
            for key, child in value.items():
                _check_value(key, depth + 1)
                _check_value(child, depth + 1)
        else:
            for child in value:
                _check_value(child, depth + 1)
    elif isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            raise HiddenCheckError("hidden_json_unicode_invalid") from None
    elif isinstance(value, int) and not isinstance(value, bool):
        if abs(value) > _MAX_SAFE_INTEGER:
            raise HiddenCheckError("hidden_json_integer_unsafe")
    elif isinstance(value, float) and not math.isfinite(value):
        raise HiddenCheckError("hidden_json_non_finite_number")
    elif isinstance(value, float) and value.is_integer() and abs(value) > _MAX_SAFE_INTEGER:
        raise HiddenCheckError("hidden_json_integer_unsafe")


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _parse_json(raw: str, code: str) -> Any:
    if not isinstance(raw, str):
        raise HiddenCheckError(code)
    if len(raw) > MAX_JSON_BYTES:
        raise HiddenCheckError("hidden_json_size_limit")
    try:
        encoded = raw.encode("utf-8")
    except UnicodeEncodeError:
        raise HiddenCheckError(code) from None
    if len(encoded) > MAX_JSON_BYTES:
        raise HiddenCheckError("hidden_json_size_limit")
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_pairs_no_duplicates,
            parse_constant=_reject_constant,
            parse_float=_parse_float,
        )
    except HiddenCheckError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError):
        raise HiddenCheckError(code) from None
    _check_value(value)
    return value


@dataclass(frozen=True, slots=True)
class HiddenCheckCase:
    check_id: str
    input_json: str = field(repr=False)
    expected_json: str = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.check_id, str) or _SAFE_ID.fullmatch(self.check_id) is None:
            raise HiddenCheckError("hidden_check_id_invalid")
        input_value = _parse_json(self.input_json, "hidden_check_input_invalid")
        expected_value = _parse_json(self.expected_json, "hidden_check_expected_invalid")
        object.__setattr__(self, "input_json", _canonical_json(input_value))
        object.__setattr__(self, "expected_json", _canonical_json(expected_value))


@dataclass(frozen=True, slots=True)
class HiddenCheckSuite:
    suite_id: str
    profile_id: str
    profile_version: str
    candidate_archive_sha256: str
    candidate_tree_sha256: str
    cases: tuple[HiddenCheckCase, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.suite_id, str) or _SAFE_ID.fullmatch(self.suite_id) is None:
            raise HiddenCheckError("hidden_suite_id_invalid")
        if (
            not isinstance(self.profile_id, str)
            or not isinstance(self.profile_version, str)
            or _SAFE_PROFILES.get(self.profile_id) != self.profile_version
        ):
            raise HiddenCheckError("hidden_profile_unsupported")
        if any(
            not isinstance(digest, str) or _SHA256.fullmatch(digest) is None
            for digest in (self.candidate_archive_sha256, self.candidate_tree_sha256)
        ):
            raise HiddenCheckError("hidden_candidate_digest_invalid")
        if not isinstance(self.cases, tuple) or not 1 <= len(self.cases) <= MAX_CASES:
            raise HiddenCheckError("hidden_case_count_invalid")
        if any(not isinstance(case, HiddenCheckCase) for case in self.cases):
            raise HiddenCheckError("hidden_case_invalid")
        if len({case.check_id for case in self.cases}) != len(self.cases):
            raise HiddenCheckError("hidden_check_id_duplicate")
        total = len(self._canonical_suite().encode("utf-8"))
        if total > MAX_JSON_BYTES:
            raise HiddenCheckError("hidden_suite_size_limit")

    def _canonical_suite(self) -> str:
        return _canonical_json(
            {
                "candidate_archive_sha256": self.candidate_archive_sha256,
                "candidate_tree_sha256": self.candidate_tree_sha256,
                "cases": [
                    {
                        "check_id": case.check_id,
                        "expected": json.loads(case.expected_json),
                        "input": json.loads(case.input_json),
                    }
                    for case in self.cases
                ],
                "profile_id": self.profile_id,
                "profile_version": self.profile_version,
                "protocol_version": 1,
                "suite_id": self.suite_id,
            }
        )

    def public_inputs_json(self) -> str:
        """Return canonical case inputs without exposing expected values."""
        return _canonical_json([json.loads(case.input_json) for case in self.cases])

    @property
    def digest(self) -> str:
        """SHA-256 of the canonical suite, including hidden expected values."""
        return hashlib.sha256(self._canonical_suite().encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class HiddenCheckResult:
    outcome: Literal["passed", "failed", "invalid", "unknown"]
    passed_count: int
    total_count: int
    suite_sha256: str
    candidate_archive_sha256: str
    candidate_tree_sha256: str
    stdout_sha256: str
    stderr_sha256: str
    code: str

    def as_dict(self) -> dict[str, str | int]:
        return {
            "outcome": self.outcome,
            "passed_count": self.passed_count,
            "total_count": self.total_count,
            "suite_sha256": self.suite_sha256,
            "candidate_archive_sha256": self.candidate_archive_sha256,
            "candidate_tree_sha256": self.candidate_tree_sha256,
            "stdout_sha256": self.stdout_sha256,
            "stderr_sha256": self.stderr_sha256,
            "code": self.code,
        }


def _equal_json(left: Any, right: Any) -> bool:
    """Compare JSON values with bool distinct from numbers and equal numeric values."""
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(
            _equal_json(left[key], right[key]) for key in left
        )
    if isinstance(left, list):
        return len(left) == len(right) and all(
            _equal_json(a, b) for a, b in zip(left, right, strict=True)
        )
    return bool(left == right)


def _digest_text(value: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    for offset in range(0, len(value), 4096):
        chunk = value[offset : offset + 4096].encode("utf-8", errors="replace")
        digest.update(chunk)
        byte_count += len(chunk)
    return digest.hexdigest(), byte_count


def compare_hidden_output(
    suite: HiddenCheckSuite,
    *,
    candidate_archive_sha256: str,
    candidate_tree_sha256: str,
    exit_code: int | None,
    stdout: str,
    stderr: str = "",
) -> HiddenCheckResult:
    """Compare the runner's exact protocol and return hashes, never output text."""
    safe_archive = (
        candidate_archive_sha256
        if isinstance(candidate_archive_sha256, str) and _SHA256.fullmatch(candidate_archive_sha256)
        else ""
    )
    safe_tree = (
        candidate_tree_sha256
        if isinstance(candidate_tree_sha256, str) and _SHA256.fullmatch(candidate_tree_sha256)
        else ""
    )
    stdout_digest, stdout_bytes = (
        _digest_text(stdout) if isinstance(stdout, str) else (hashlib.sha256(b"").hexdigest(), 0)
    )
    stderr_digest, stderr_bytes = (
        _digest_text(stderr) if isinstance(stderr, str) else (hashlib.sha256(b"").hexdigest(), 0)
    )

    def result(
        outcome: Literal["passed", "failed", "invalid", "unknown"],
        count: int,
        code: str,
    ) -> HiddenCheckResult:
        return HiddenCheckResult(
            outcome=outcome,
            passed_count=count,
            total_count=len(suite.cases),
            suite_sha256=suite.digest,
            candidate_archive_sha256=safe_archive,
            candidate_tree_sha256=safe_tree,
            stdout_sha256=stdout_digest,
            stderr_sha256=stderr_digest,
            code=code,
        )

    if (
        not isinstance(candidate_archive_sha256, str)
        or not isinstance(candidate_tree_sha256, str)
        or _SHA256.fullmatch(candidate_archive_sha256) is None
        or _SHA256.fullmatch(candidate_tree_sha256) is None
    ):
        return result("invalid", 0, "candidate_digest_invalid")
    if candidate_archive_sha256 != suite.candidate_archive_sha256:
        return result("invalid", 0, "candidate_archive_mismatch")
    if candidate_tree_sha256 != suite.candidate_tree_sha256:
        return result("invalid", 0, "candidate_tree_mismatch")
    if not isinstance(stdout, str) or not isinstance(stderr, str):
        return result("invalid", 0, "hidden_output_invalid")
    if stdout_bytes > MAX_JSON_BYTES or stderr_bytes > MAX_JSON_BYTES:
        return result("invalid", 0, "hidden_output_size_limit")
    if exit_code is None:
        return result("unknown", 0, "exit_code_missing")
    if (
        isinstance(exit_code, bool)
        or not isinstance(exit_code, int)
        or not -255 <= exit_code <= 255
    ):
        return result("invalid", 0, "exit_code_invalid")
    if exit_code != 0:
        return result("failed", 0, "candidate_exit_nonzero")
    try:
        payload = _parse_json(stdout, "hidden_output_json_invalid")
        if not isinstance(payload, dict) or set(payload) != {"protocol_version", "values"}:
            return result("invalid", 0, "hidden_output_protocol_invalid")
        if type(payload["protocol_version"]) is not int or payload["protocol_version"] != 1:
            return result("invalid", 0, "hidden_output_protocol_invalid")
        values = payload["values"]
        if not isinstance(values, list) or len(values) != len(suite.cases):
            return result("invalid", 0, "hidden_output_values_invalid")
        passed = sum(
            _equal_json(actual, json.loads(case.expected_json))
            for actual, case in zip(values, suite.cases, strict=True)
        )
        if passed == len(suite.cases):
            return result("passed", passed, "hidden_checks_passed")
        return result("failed", passed, "hidden_check_mismatch")
    except HiddenCheckError as error:
        code = (
            "hidden_output_size_limit"
            if error.code == "hidden_json_size_limit"
            else "hidden_output_invalid"
        )
        return result("invalid", 0, code)
