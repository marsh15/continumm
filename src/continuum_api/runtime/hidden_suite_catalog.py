"""Load private, operator-provided hidden suites for CLI evaluation runs.

The catalog is read once and treated as trusted input. It contains expected
answers, so all catalog failures use stable codes and never echo paths or data.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID

from continuum_api.repositories.preflight import EligibilityReceipt
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.validation_activity import (
    CandidateValidationInput,
    HiddenCheckSelection,
)
from continuum_api.sandbox.hidden_probe_recipe import HiddenProbe
from continuum_api.sandbox.validation.preparation import ValidationSourceAuthorization
from continuum_api.validation.hidden_checks import (
    HiddenCheckCase,
    HiddenCheckError,
    HiddenCheckSuite,
    _parse_json,
)
from continuum_api.validation.managed_hidden_checks import _invocation_digest

_MAX_FILE_BYTES = 65_536
_BASE_SHA = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_SCHEMA_KEYS = {"schema_version", "account_id", "provider", "suites"}
_SPEC_KEYS = {
    "run_id",
    "repository_id",
    "base_commit_sha",
    "profile_id",
    "profile_version",
    "suite_id",
    "entry_path",
    "checks",
}
_CHECK_KEYS = {"check_id", "input", "expected"}


class HiddenSuiteCatalogError(ValueError):
    """Stable, redacted error for invalid or inaccessible catalog files."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class _SuiteSpec:
    run_id: UUID
    repository_id: int
    base_commit_sha: str
    profile_id: str
    profile_version: str
    suite_id: str
    entry_path: str
    checks: tuple[HiddenCheckCase, ...] = field(repr=False)


@dataclass(frozen=True, slots=True)
class LoadedHiddenSuites:
    """Immutable private catalog and its run-bound selector/report verifier."""

    file_sha256: str
    _specs: tuple[_SuiteSpec, ...] = field(repr=False)
    _account_id: UUID = field(repr=False)
    _provider: str = field(repr=False)
    _python_run_id: UUID = field(repr=False)
    _node_run_id: UUID = field(repr=False)

    def __call__(
        self, lease: ExecutionLease, inputs: CandidateValidationInput
    ) -> HiddenCheckSelection:
        if not isinstance(lease, ExecutionLease) or not isinstance(
            inputs, CandidateValidationInput
        ):
            raise HiddenSuiteCatalogError("hidden_catalog_selection_invalid")
        if lease.owner_id != self._account_id:
            raise HiddenSuiteCatalogError("hidden_catalog_owner_mismatch")
        if lease.run_id not in {self._python_run_id, self._node_run_id}:
            raise HiddenSuiteCatalogError("hidden_catalog_run_mismatch")
        spec = next((item for item in self._specs if item.run_id == lease.run_id), None)
        if spec is None:
            raise HiddenSuiteCatalogError("hidden_catalog_run_mismatch")
        try:
            eligibility = inputs.eligibility
            source = inputs.source
            source_eligibility = source.eligibility
            if (
                not isinstance(source, ValidationSourceAuthorization)
                or not isinstance(eligibility, EligibilityReceipt)
                or not isinstance(source_eligibility, EligibilityReceipt)
                or eligibility != source_eligibility
                or eligibility.repository_id != spec.repository_id
                or eligibility.commit_sha != spec.base_commit_sha
                or eligibility.profile_id != spec.profile_id
                or eligibility.profile_version != spec.profile_version
                or source.profile_id != spec.profile_id
                or source.profile_version != spec.profile_version
                or inputs.baseline_artifact_id != source.baseline_artifact_id
                or spec.entry_path not in {item.path for item in source.candidate_manifest.entries}
            ):
                raise HiddenSuiteCatalogError("hidden_catalog_source_mismatch")
            suite = HiddenCheckSuite(
                spec.suite_id,
                spec.profile_id,
                spec.profile_version,
                source.candidate_archive_sha256,
                source.candidate_source_tree_sha256,
                spec.checks,
            )
            HiddenProbe(
                spec.profile_id,
                spec.profile_version,
                spec.entry_path,
                suite.public_inputs_json(),
            ).argv()
            return HiddenCheckSelection(suite, spec.entry_path)
        except HiddenSuiteCatalogError:
            raise
        except (AttributeError, TypeError, ValueError, HiddenCheckError):
            raise HiddenSuiteCatalogError("hidden_catalog_source_mismatch") from None

    def report_matches(self, report: Mapping[str, Any]) -> dict[str, bool]:
        """Verify only recorded snapshots against the catalog's exact suite pins."""
        results = {"python": False, "node": False}
        if not isinstance(report, Mapping):
            return results
        cases = report.get("cases")
        if not isinstance(cases, Mapping):
            return results
        for label, profile_id, pinned_run_id in (
            ("python", "python-pytest", self._python_run_id),
            ("node", "node-npm", self._node_run_id),
        ):
            try:
                spec = next(item for item in self._specs if item.run_id == pinned_run_id)
                case = cases.get(label)
                if not isinstance(case, Mapping):
                    continue
                snapshot = case.get("snapshot")
                if not isinstance(snapshot, Mapping):
                    continue
                results[label] = _snapshot_matches(snapshot, spec, profile_id, pinned_run_id)
            except (StopIteration, TypeError, ValueError, KeyError):
                results[label] = False
        return results


def load_hidden_suites(
    path: Path,
    *,
    account_id: UUID,
    provider: str,
    python_run_id: UUID,
    node_run_id: UUID,
    project_root: Path,
) -> LoadedHiddenSuites:
    """Read and validate a private catalog without evaluating its contents."""
    if (
        not isinstance(account_id, UUID)
        or not isinstance(python_run_id, UUID)
        or not isinstance(node_run_id, UUID)
        or python_run_id == node_run_id
        or not isinstance(provider, str)
        or provider not in {"openai", "anthropic"}
        or not isinstance(path, Path)
        or not isinstance(project_root, Path)
    ):
        raise HiddenSuiteCatalogError("hidden_catalog_identity_invalid")
    raw = _read_private_file(path, project_root)
    file_sha256 = hashlib.sha256(raw).hexdigest()
    try:
        document = _parse_json(raw.decode("utf-8"), "hidden_catalog_json_invalid")
        specs = _validate_document(document, account_id, provider, python_run_id, node_run_id)
    except HiddenSuiteCatalogError:
        raise
    except (UnicodeDecodeError, HiddenCheckError, TypeError, ValueError):
        raise HiddenSuiteCatalogError("hidden_catalog_schema_invalid") from None
    return LoadedHiddenSuites(
        file_sha256,
        specs,
        account_id,
        provider,
        python_run_id,
        node_run_id,
    )


def _read_private_file(path: Path, project_root: Path) -> bytes:
    try:
        root = project_root.resolve(strict=True)
        resolved = path.resolve(strict=True)
        if resolved.is_relative_to(root):
            raise HiddenSuiteCatalogError("hidden_catalog_file_invalid")
        before = path.lstat()
        if stat.S_ISLNK(before.st_mode):
            raise HiddenSuiteCatalogError("hidden_catalog_file_invalid")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        fd = os.open(path, flags)
        try:
            opened = os.fstat(fd)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.getuid()
                or opened.st_nlink != 1
                or stat.S_IMODE(opened.st_mode) != 0o600
                or opened.st_dev != before.st_dev
                or opened.st_ino != before.st_ino
            ):
                raise HiddenSuiteCatalogError("hidden_catalog_file_invalid")
            chunks: list[bytes] = []
            total = 0
            while total <= _MAX_FILE_BYTES:
                chunk = os.read(fd, min(8192, _MAX_FILE_BYTES + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
            if total > _MAX_FILE_BYTES:
                raise HiddenSuiteCatalogError("hidden_catalog_size_limit")
            return b"".join(chunks)
        finally:
            os.close(fd)
    except HiddenSuiteCatalogError:
        raise
    except (OSError, RuntimeError, ValueError):
        raise HiddenSuiteCatalogError("hidden_catalog_file_invalid") from None


def _validate_document(
    document: Any,
    account_id: UUID,
    provider: str,
    python_run_id: UUID,
    node_run_id: UUID,
) -> tuple[_SuiteSpec, _SuiteSpec]:
    if not isinstance(document, dict) or set(document) != _SCHEMA_KEYS:
        raise HiddenSuiteCatalogError("hidden_catalog_schema_invalid")
    if (
        type(document["schema_version"]) is not int
        or document["schema_version"] != 1
        or _uuid(document["account_id"]) != account_id
        or document["provider"] != provider
        or type(document["suites"]) is not list
        or len(document["suites"]) != 2
    ):
        raise HiddenSuiteCatalogError("hidden_catalog_identity_mismatch")
    specs = tuple(_parse_spec(item) for item in document["suites"])
    by_profile = {spec.profile_id: spec for spec in specs}
    if (
        len(by_profile) != 2
        or set(by_profile) != {"python-pytest", "node-npm"}
        or by_profile["python-pytest"].run_id != python_run_id
        or by_profile["node-npm"].run_id != node_run_id
        or len({spec.run_id for spec in specs}) != 2
    ):
        raise HiddenSuiteCatalogError("hidden_catalog_identity_mismatch")
    return by_profile["python-pytest"], by_profile["node-npm"]


def _parse_spec(value: Any) -> _SuiteSpec:
    if not isinstance(value, dict) or set(value) != _SPEC_KEYS:
        raise HiddenSuiteCatalogError("hidden_catalog_schema_invalid")
    run_id = _uuid(value["run_id"])
    repository_id = value["repository_id"]
    base_commit_sha = value["base_commit_sha"]
    profile_id = value["profile_id"]
    profile_version = value["profile_version"]
    suite_id = value["suite_id"]
    entry_path = value["entry_path"]
    if (
        type(repository_id) is not int
        or repository_id <= 0
        or not isinstance(base_commit_sha, str)
        or _BASE_SHA.fullmatch(base_commit_sha) is None
        or (profile_id, profile_version) not in {("python-pytest", "1"), ("node-npm", "1")}
        or not isinstance(suite_id, str)
        or not isinstance(entry_path, str)
        or len(entry_path.encode("utf-8")) > 4096
        or type(value["checks"]) is not list
        or not 1 <= len(value["checks"]) <= 32
    ):
        raise HiddenSuiteCatalogError("hidden_catalog_schema_invalid")
    try:
        # Let the canonical suite contract enforce the safe suite identifier.
        HiddenCheckSuite(
            suite_id,
            profile_id,
            profile_version,
            "0" * 64,
            "0" * 64,
            (HiddenCheckCase("probe", "null", "null"),),
        )
        HiddenProbe(profile_id, profile_version, entry_path, "[null]").argv()
        checks = tuple(_parse_check(check) for check in value["checks"])
        if len({case.check_id for case in checks}) != len(checks):
            raise HiddenSuiteCatalogError("hidden_catalog_schema_invalid")
        # Construction applies the exact canonical suite byte limit before storing.
        prepared = HiddenCheckSuite(
            suite_id, profile_id, profile_version, "0" * 64, "0" * 64, checks
        )
        HiddenProbe(profile_id, profile_version, entry_path, prepared.public_inputs_json()).argv()
    except HiddenSuiteCatalogError:
        raise
    except (HiddenCheckError, TypeError, ValueError):
        raise HiddenSuiteCatalogError("hidden_catalog_schema_invalid") from None
    return _SuiteSpec(
        run_id,
        repository_id,
        base_commit_sha,
        profile_id,
        profile_version,
        suite_id,
        entry_path,
        checks,
    )


def _parse_check(value: Any) -> HiddenCheckCase:
    if not isinstance(value, dict) or set(value) != _CHECK_KEYS:
        raise HiddenSuiteCatalogError("hidden_catalog_schema_invalid")
    try:
        return HiddenCheckCase(
            value["check_id"],
            _canonical(value["input"]),
            _canonical(value["expected"]),
        )
    except (HiddenCheckError, TypeError, ValueError):
        raise HiddenSuiteCatalogError("hidden_catalog_schema_invalid") from None


def _canonical(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _uuid(value: Any) -> UUID:
    if not isinstance(value, str):
        raise HiddenSuiteCatalogError("hidden_catalog_schema_invalid")
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError):
        raise HiddenSuiteCatalogError("hidden_catalog_schema_invalid") from None
    if str(parsed) != value:
        raise HiddenSuiteCatalogError("hidden_catalog_schema_invalid")
    return parsed


def _snapshot_matches(
    snapshot: Mapping[str, Any], spec: _SuiteSpec, profile_id: str, run_id: UUID
) -> bool:
    if (
        snapshot.get("independent_hidden_checks") != "passed"
        or snapshot.get("run_id") != str(run_id)
        or snapshot.get("profile_id") != profile_id
        or snapshot.get("profile_version") != spec.profile_version
        or snapshot.get("hidden_check_suite_id") != spec.suite_id
        or type(snapshot.get("hidden_check_count")) is not int
        or snapshot.get("hidden_check_count") != len(spec.checks)
        or type(snapshot.get("hidden_check_passed_count")) is not int
        or snapshot.get("hidden_check_passed_count") != len(spec.checks)
        or type(snapshot.get("hidden_check_repository_id")) is not int
        or snapshot.get("hidden_check_repository_id") != spec.repository_id
        or snapshot.get("hidden_check_base_commit_sha") != spec.base_commit_sha
    ):
        return False
    archive_sha = snapshot.get("hidden_check_candidate_archive_sha256")
    tree_sha = snapshot.get("hidden_check_candidate_tree_sha256")
    if not _is_sha256(archive_sha) or not _is_sha256(tree_sha):
        return False
    assert isinstance(archive_sha, str) and isinstance(tree_sha, str)
    try:
        suite = HiddenCheckSuite(
            spec.suite_id,
            spec.profile_id,
            spec.profile_version,
            archive_sha,
            tree_sha,
            spec.checks,
        )
        probe = HiddenProbe(
            spec.profile_id, spec.profile_version, spec.entry_path, suite.public_inputs_json()
        )
        invocation = _invocation_digest(suite.digest, spec.entry_path, probe.command_id)
    except (HiddenCheckError, TypeError, ValueError):
        return False
    return (
        snapshot.get("hidden_check_suite_sha256") == suite.digest
        and snapshot.get("hidden_check_invocation_sha256") == invocation
    )


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None
