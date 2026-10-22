from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from continuum_api.artifacts.archives import ArchiveFile, create_archive
from continuum_api.repositories.preflight import EligibilityReceipt
from continuum_api.runtime.hidden_suite_catalog import (
    HiddenSuiteCatalogError,
    LoadedHiddenSuites,
    load_hidden_suites,
)
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.validation_activity import CandidateValidationInput
from continuum_api.sandbox.hidden_probe_recipe import HiddenProbe
from continuum_api.sandbox.validation.preparation import ValidationSourceAuthorization
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import ToolPolicyContext
from continuum_api.validation.hidden_checks import HiddenCheckSuite
from continuum_api.validation.managed_hidden_checks import _invocation_digest

ACCOUNT = UUID("11111111-1111-4111-8111-111111111111")
PYTHON_RUN = UUID("22222222-2222-4222-8222-222222222222")
NODE_RUN = UUID("33333333-3333-4333-8333-333333333333")
COMMIT = "a" * 40
ARCHIVE = "b" * 64
TREE = "c" * 64


def document(
    *,
    account_id: UUID = ACCOUNT,
    provider: str = "openai",
    python_run_id: UUID = PYTHON_RUN,
    node_run_id: UUID = NODE_RUN,
    python_entry: str = "src/solution.py",
    node_entry: str = "src/solution.cjs",
) -> dict[str, object]:
    suites = []
    for run_id, profile_id, entry_path in (
        (python_run_id, "python-pytest", python_entry),
        (node_run_id, "node-npm", node_entry),
    ):
        suites.append(
            {
                "run_id": str(run_id),
                "repository_id": 123,
                "base_commit_sha": COMMIT,
                "profile_id": profile_id,
                "profile_version": "1",
                "suite_id": f"{profile_id}-suite",
                "entry_path": entry_path,
                "checks": [
                    {"check_id": "case-1", "input": [1], "expected": {"ok": True}},
                    {"check_id": "case-2", "input": [{"x": "a"}], "expected": ["yes"]},
                ],
            }
        )
    return {
        "schema_version": 1,
        "account_id": str(account_id),
        "provider": provider,
        "suites": suites,
    }


def write_catalog(root: Path, payload: object, *, name: str = "catalog.json") -> Path:
    path = root / name
    if isinstance(payload, bytes):
        path.write_bytes(payload)
    else:
        path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    path.chmod(0o600)
    return path


def load(path: Path, root: Path) -> LoadedHiddenSuites:
    project = root / "checkout"
    project.mkdir(exist_ok=True)
    return load_hidden_suites(
        path,
        account_id=ACCOUNT,
        provider="openai",
        python_run_id=PYTHON_RUN,
        node_run_id=NODE_RUN,
        project_root=project,
    )


def lease(run_id: UUID = PYTHON_RUN, owner_id: UUID = ACCOUNT) -> ExecutionLease:
    now = datetime.now(UTC)
    return ExecutionLease(owner_id, run_id, uuid4(), uuid4(), uuid4(), 1, "worker", now)


def candidate_inputs(
    *,
    repository_id: int = 123,
    commit_sha: str = COMMIT,
    profile_id: str = "python-pytest",
    version: str = "1",
) -> CandidateValidationInput:
    eligibility = EligibilityReceipt(
        repository_id,
        commit_sha,
        "d" * 40,
        profile_id,
        version,
        1,
        10,
        (),
        0,
    )
    entry = "src/solution.py" if profile_id == "python-pytest" else "src/solution.cjs"
    archive = create_archive((ArchiveFile(entry, b"trusted fixture data\n"),))
    baseline_id = uuid4()
    context = ToolPolicyContext(
        capabilities=frozenset(ToolCapability),
        profile_id=profile_id,
        profile_version=version,
        actor_can_write_repository=True,
        app_can_access_repository=True,
        stage=ToolStage.AGENT_WORK,
        run_budget_available=True,
        baseline_tree_sha256=archive.manifest.tree_sha256,
        baseline_source_paths=frozenset({entry}),
        baseline_test_paths=frozenset(),
        baseline_paths_verified=True,
        protected_paths=frozenset(),
    )
    source = ValidationSourceAuthorization(
        baseline_artifact_id=baseline_id,
        eligibility=eligibility,
        profile_id=profile_id,
        profile_version=version,
        baseline_archive_sha256=archive.archive_sha256,
        baseline_source_tree_sha256=archive.manifest.tree_sha256,
        protected_manifest_sha256="0" * 64,
        lineage_sha256="0" * 64,
        candidate_archive=archive.archive,
        candidate_manifest=archive.manifest,
        candidate_archive_sha256=archive.archive_sha256,
        candidate_source_tree_sha256=archive.manifest.tree_sha256,
        candidate_file_count=1,
        candidate_source_bytes=len(b"trusted fixture data\n"),
        changed_paths=(),
        preimages=(),
        tool_policy_context=context,
    )
    return CandidateValidationInput(baseline_id, uuid4(), eligibility, source)


def test_load_and_select_binds_trusted_suite_to_candidate_source(tmp_path: Path) -> None:
    path = write_catalog(tmp_path, document())
    loaded = load(path, tmp_path)

    inputs = candidate_inputs()
    selection = loaded(lease(), inputs)

    assert loaded.file_sha256
    assert selection.entry_path == "src/solution.py"
    assert selection.suite.candidate_archive_sha256 == inputs.source.candidate_archive_sha256
    assert selection.suite.candidate_tree_sha256 == inputs.source.candidate_source_tree_sha256
    assert selection.suite.cases[0].input_json == "[1]"
    assert selection.suite.cases[0].expected_json == '{"ok":true}'


@pytest.mark.parametrize(
    "mutate",
    [
        lambda doc: doc.update(extra=True),
        lambda doc: doc.update(schema_version=True),
        lambda doc: doc.update(account_id=str(uuid4())),
        lambda doc: doc.update(provider="anthropic"),
        lambda doc: doc["suites"][0].update(unexpected=True),
        lambda doc: doc["suites"][0].update(repository_id=True),
        lambda doc: doc["suites"][0].update(base_commit_sha="A" * 40),
        lambda doc: doc["suites"][0].update(entry_path="x" * 4096 + ".py"),
        lambda doc: doc["suites"][0]["checks"][0].update(unexpected=True),
    ],
)
def test_rejects_invalid_schema_and_pins(
    tmp_path: Path, mutate: Callable[[dict[str, object]], None]
) -> None:
    payload = document()
    mutate(payload)
    with pytest.raises(HiddenSuiteCatalogError):
        load(write_catalog(tmp_path, payload), tmp_path)


def test_strict_parser_rejects_duplicate_keys_and_nonfinite_numbers(tmp_path: Path) -> None:
    duplicate = b'{"schema_version":1,"schema_version":1}'
    with pytest.raises(HiddenSuiteCatalogError):
        load(write_catalog(tmp_path, duplicate), tmp_path)
    payload = json.dumps(document()).replace('"input": [1]', '"input": [NaN]')
    with pytest.raises(HiddenSuiteCatalogError):
        load(write_catalog(tmp_path, payload.encode()), tmp_path)


@pytest.mark.parametrize("mode", [0o400, 0o700, 0o640, 0o604, 0o666])
def test_rejects_group_or_other_permissions(tmp_path: Path, mode: int) -> None:
    path = write_catalog(tmp_path, document())
    path.chmod(mode)
    with pytest.raises(HiddenSuiteCatalogError):
        load(path, tmp_path)


def test_rejects_symlink_outside_project_and_fifo(tmp_path: Path) -> None:
    path = write_catalog(tmp_path, document())
    outside = tmp_path.parent / f"{tmp_path.name}-outside.json"
    outside.write_text(json.dumps(document()), encoding="utf-8")
    outside.chmod(0o600)
    link = tmp_path / "link.json"
    link.symlink_to(path)
    with pytest.raises(HiddenSuiteCatalogError):
        load(link, tmp_path)
    assert load(outside, tmp_path).file_sha256
    outside.unlink()
    checkout_file = write_catalog(tmp_path / "checkout", document())
    with pytest.raises(HiddenSuiteCatalogError):
        load(checkout_file, tmp_path)

    fifo = tmp_path / "pipe"
    os.mkfifo(fifo, 0o600)
    with pytest.raises(HiddenSuiteCatalogError):
        load(fifo, tmp_path)


def test_rejects_oversized_and_hardlinked_files(tmp_path: Path) -> None:
    oversized = write_catalog(tmp_path, b" " * 65_537, name="large.json")
    with pytest.raises(HiddenSuiteCatalogError) as error:
        load(oversized, tmp_path)
    assert error.value.code == "hidden_catalog_size_limit"

    path = write_catalog(tmp_path, document(), name="linked.json")
    second = tmp_path / "second.json"
    os.link(path, second)
    with pytest.raises(HiddenSuiteCatalogError):
        load(path, tmp_path)


def test_selector_enforces_owner_run_repository_commit_and_profile(tmp_path: Path) -> None:
    loaded = load(write_catalog(tmp_path, document()), tmp_path)
    for invalid_lease, inputs in (
        (lease(owner_id=uuid4()), candidate_inputs()),
        (lease(uuid4()), candidate_inputs()),
        (lease(), candidate_inputs(repository_id=99)),
        (lease(), candidate_inputs(commit_sha="e" * 40)),
        (lease(), candidate_inputs(profile_id="node-npm")),
    ):
        with pytest.raises(HiddenSuiteCatalogError):
            loaded(invalid_lease, inputs)


def snapshot(
    loaded: LoadedHiddenSuites,
    *,
    entry_path: str = "src/solution.py",
    archive: str = ARCHIVE,
    tree: str = TREE,
) -> dict[str, object]:
    spec = loaded._specs[0]
    suite = HiddenCheckSuite(
        spec.suite_id, spec.profile_id, spec.profile_version, archive, tree, spec.checks
    )
    probe = HiddenProbe(
        spec.profile_id, spec.profile_version, entry_path, suite.public_inputs_json()
    )
    return {
        "independent_hidden_checks": "passed",
        "run_id": str(PYTHON_RUN),
        "profile_id": "python-pytest",
        "profile_version": "1",
        "hidden_check_suite_id": spec.suite_id,
        "hidden_check_suite_sha256": suite.digest,
        "hidden_check_count": len(spec.checks),
        "hidden_check_passed_count": len(spec.checks),
        "hidden_check_repository_id": 123,
        "hidden_check_base_commit_sha": COMMIT,
        "hidden_check_candidate_archive_sha256": archive,
        "hidden_check_candidate_tree_sha256": tree,
        "hidden_check_invocation_sha256": _invocation_digest(
            suite.digest, entry_path, probe.command_id
        ),
    }


def test_report_matches_only_actual_bound_snapshot_metadata(tmp_path: Path) -> None:
    loaded = load(write_catalog(tmp_path, document()), tmp_path)
    report = {"cases": {"python": {"snapshot": snapshot(loaded)}}}
    assert loaded.report_matches(report) == {"python": True, "node": False}

    wrong_entry = snapshot(loaded, entry_path="other.py")
    assert (
        loaded.report_matches({"cases": {"python": {"snapshot": wrong_entry}}})["python"] is False
    )

    wrong_source = snapshot(loaded, archive="f" * 64)
    original_source = snapshot(loaded)
    wrong_source["hidden_check_suite_sha256"] = original_source["hidden_check_suite_sha256"]
    wrong_source["hidden_check_invocation_sha256"] = original_source[
        "hidden_check_invocation_sha256"
    ]
    assert (
        loaded.report_matches({"cases": {"python": {"snapshot": wrong_source}}})["python"] is False
    )

    stale = snapshot(loaded)
    stale["hidden_check_passed_count"] = 1
    assert loaded.report_matches({"cases": {"python": {"snapshot": stale}}})["python"] is False

    no_snapshot = {"cases": {"python": {"independent_hidden_checks": "passed"}}}
    assert loaded.report_matches(no_snapshot) == {"python": False, "node": False}


def test_catalog_requires_current_owner_and_remains_an_immutable_snapshot(tmp_path, monkeypatch):
    import hashlib

    path = write_catalog(tmp_path, document())
    original = path.read_bytes()
    owner = os.getuid()
    with monkeypatch.context() as local:
        local.setattr(os, "getuid", lambda: owner + 1)
        with pytest.raises(HiddenSuiteCatalogError, match="hidden_catalog_file_invalid"):
            load(path, tmp_path)
    loaded = load(path, tmp_path)
    changed = document()
    changed["suites"][0]["checks"][0]["expected"] = "replacement-answer"
    path.write_text(json.dumps(changed), encoding="utf-8")
    selection = loaded(lease(), candidate_inputs())
    assert selection.suite.cases[0].expected_json == '{"ok":true}'
    assert loaded.file_sha256 == hashlib.sha256(original).hexdigest()
    assert "replacement-answer" not in repr(loaded)


@pytest.mark.parametrize(
    "field", ["hidden_check_count", "hidden_check_passed_count", "hidden_check_repository_id"]
)
def test_report_numeric_pins_do_not_coerce_float_values(tmp_path, field):
    loaded = load(write_catalog(tmp_path, document()), tmp_path)
    recorded = snapshot(loaded)
    recorded[field] = float(recorded[field])
    assert loaded.report_matches({"cases": {"python": {"snapshot": recorded}}})["python"] is False


def test_wrong_expected_answers_do_not_match_a_prior_passing_suite(tmp_path):
    path = write_catalog(tmp_path, document())
    original = load(path, tmp_path)
    recorded = snapshot(original)
    changed = document()
    changed["suites"][0]["checks"][0]["expected"] = "different-answer"
    updated = load(write_catalog(tmp_path, changed), tmp_path)
    assert original.report_matches({"cases": {"python": {"snapshot": recorded}}})["python"] is True
    assert updated.report_matches({"cases": {"python": {"snapshot": recorded}}})["python"] is False
