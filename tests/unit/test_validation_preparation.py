"""Candidate authorizations revalidate real baseline receipts and protected lineage."""

from __future__ import annotations

import base64
import json
import subprocess
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from continuum_api.artifacts.archives import (
    ArchiveFile,
    VerifiedArchive,
    create_archive,
    verify_archive,
)
from continuum_api.repositories.preflight import EligibilityReceipt
from continuum_api.sandbox.node_bootstrap_recipe import NODE_PROFILE_ID
from continuum_api.sandbox.node_profile import (
    NodeProfileError,
    _build_node_candidate_profile_plan,
)
from continuum_api.sandbox.python_bootstrap_recipe import PYTHON_PROFILE_ID
from continuum_api.sandbox.python_profile import (
    PythonProfileError,
    _build_python_candidate_profile_plan,
)
from continuum_api.sandbox.validation import prepare_validation_source
from continuum_api.sandbox.validation.preparation import _candidate_delta, _lineage_digest
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import ToolPolicyContext
from continuum_api.tools.source_tools import (
    TextEdit,
    apply_text_edits,
    discover_baseline_test_paths,
    snapshot_from_verified_archive,
    source_paths,
)

_NODE_INTEGRITY = "sha512-" + base64.b64encode(bytes(64)).decode("ascii")


def _source_files(profile: str) -> tuple[ArchiveFile, ...]:
    if profile == "python-pytest":
        return (
            ArchiveFile("module.py", b"value = 1\n"),
            ArchiveFile("requirements.lock", b"pytest==8.3.5 --hash=sha256:" + b"a" * 64 + b"\n"),
            ArchiveFile("tests/test_original.py", b"def test_original():\n    assert True\n"),
        )

    package = {
        "name": "validation-fixture",
        "version": "1.0.0",
        "packageManager": "npm@11.19.0",
        "scripts": {"test": "node --test"},
        "dependencies": {"is-number": "7.0.0"},
    }
    lock = {
        "name": package["name"],
        "version": package["version"],
        "lockfileVersion": 3,
        "requires": True,
        "packages": {
            "": {
                "name": package["name"],
                "version": package["version"],
                "dependencies": package["dependencies"],
            },
            "node_modules/is-number": {
                "version": "7.0.0",
                "resolved": "https://registry.npmjs.org/is-number/-/is-number-7.0.0.tgz",
                "integrity": _NODE_INTEGRITY,
            },
        },
    }
    return (
        ArchiveFile("package.json", json.dumps(package, sort_keys=True).encode()),
        ArchiveFile("package-lock.json", json.dumps(lock, sort_keys=True).encode()),
        ArchiveFile("src/index.js", b"export const value = 1;\n"),
        ArchiveFile(
            "tests/original.test.js",
            b"import test from 'node:test';\ntest('original', () => {});\n",
        ),
    )


def _eligibility_from_git(
    tmp_path: Path,
    profile: str,
    files: tuple[ArchiveFile, ...],
) -> EligibilityReceipt:
    repository = tmp_path / profile
    repository.mkdir()
    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    for file in files:
        path = repository / file.path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(file.content)
        path.chmod(file.mode)
    subprocess.run(["git", "-C", str(repository), "add", "--all"], check=True)
    tree_sha = subprocess.run(
        ["git", "-C", str(repository), "write-tree"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    staged = subprocess.run(
        ["git", "-C", str(repository), "ls-files", "--stage"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    blob_shas = {parts[3]: parts[1] for line in staged if len(parts := line.split()) == 4}
    manifest_paths = (
        ("requirements.lock",)
        if profile == PYTHON_PROFILE_ID
        else ("package.json", "package-lock.json")
    )
    return EligibilityReceipt(
        repository_id=731,
        commit_sha="c" * 40,
        tree_sha=tree_sha,
        profile_id=profile,
        profile_version="1",
        file_count=len(files),
        source_bytes=sum(len(file.content) for file in files),
        manifest_blobs=tuple((path, blob_shas[path]) for path in manifest_paths),
        package_count=1,
    )


def _prepared(
    tmp_path: Path,
    profile: str,
) -> tuple[EligibilityReceipt, VerifiedArchive, object, object, str]:
    files = _source_files(profile)
    eligibility = _eligibility_from_git(tmp_path, profile, files)
    bundle = create_archive(files)
    baseline_verified = verify_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    baseline = snapshot_from_verified_archive(
        baseline_verified,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    discovered = discover_baseline_test_paths(
        baseline,
        profile,
        expected_tree_sha256=baseline.tree_sha256,
    )
    context = ToolPolicyContext(
        capabilities=frozenset(ToolCapability),
        profile_id=profile,
        profile_version="1",
        actor_can_write_repository=True,
        app_can_access_repository=True,
        stage=ToolStage.AGENT_WORK,
        run_budget_available=True,
        baseline_tree_sha256=baseline.tree_sha256,
        baseline_source_paths=frozenset(source_paths(baseline)),
        baseline_test_paths=discovered,
        baseline_paths_verified=True,
        protected_paths=frozenset(),
    )
    source_path, expected, replacement = (
        ("module.py", "value = 1", "value = 2")
        if profile == PYTHON_PROFILE_ID
        else ("src/index.js", "value = 1", "value = 2")
    )
    candidate = apply_text_edits(
        baseline,
        [TextEdit(source_path, expected, replacement)],
        expected_tree_sha256=baseline.tree_sha256,
        authorized_write_paths={source_path},
    )
    baseline_artifact_id = uuid4()
    authorization = prepare_validation_source(
        eligibility,
        baseline_artifact_id,
        baseline_verified,
        candidate,
        context,
    )
    return eligibility, baseline_verified, baseline, authorization, baseline_artifact_id


@pytest.mark.parametrize("profile", [PYTHON_PROFILE_ID, NODE_PROFILE_ID])
def test_recomputed_candidate_lineage_cannot_authorize_an_original_test_edit(
    tmp_path: Path,
    profile: str,
) -> None:
    eligibility, baseline_verified, baseline, authorization, artifact_id = _prepared(
        tmp_path,
        profile,
    )
    test_path = (
        "tests/test_original.py" if profile == PYTHON_PROFILE_ID else "tests/original.test.js"
    )
    before = {file.path: file for file in baseline_verified.files}
    protected_content = (
        b"def test_original():\n    assert False\n"
        if profile == PYTHON_PROFILE_ID
        else b"import test from 'node:test';\ntest('changed', () => {});\n"
    )
    forged_files = tuple(
        ArchiveFile(file.path, protected_content, file.mode) if file.path == test_path else file
        for file in baseline_verified.files
    )
    bundle = create_archive(forged_files)
    forged_verified = verify_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    forged_snapshot = snapshot_from_verified_archive(
        forged_verified,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    delta = _candidate_delta(baseline_verified, forged_snapshot)
    forged_lineage = _lineage_digest(
        baseline_artifact_id=artifact_id,
        eligibility=eligibility,
        profile_id=authorization.profile_id,
        profile_version=authorization.profile_version,
        baseline_archive_sha256=authorization.baseline_archive_sha256,
        baseline_source_tree_sha256=authorization.baseline_source_tree_sha256,
        protected_manifest_sha256=authorization.protected_manifest_sha256,
        candidate_archive_sha256=forged_verified.archive_sha256,
        candidate_source_tree_sha256=forged_verified.manifest.tree_sha256,
        candidate_file_count=len(forged_verified.manifest.entries),
        candidate_source_bytes=sum(entry.size for entry in forged_verified.manifest.entries),
        changed_paths=delta.changed_paths,
        preimages=delta.preimages,
        tool_context=authorization.tool_policy_context,
    )
    forged_authorization = replace(
        authorization,
        candidate_archive=bundle.archive,
        candidate_manifest=forged_verified.manifest,
        candidate_archive_sha256=forged_verified.archive_sha256,
        candidate_source_tree_sha256=forged_verified.manifest.tree_sha256,
        candidate_file_count=len(forged_verified.manifest.entries),
        candidate_source_bytes=sum(entry.size for entry in forged_verified.manifest.entries),
        changed_paths=delta.changed_paths,
        preimages=delta.preimages,
        lineage_sha256=forged_lineage,
    )
    assert forged_authorization.lineage_sha256 != authorization.lineage_sha256
    assert test_path in forged_authorization.changed_paths
    assert before[test_path].content != protected_content

    if profile == PYTHON_PROFILE_ID:
        builder = _build_python_candidate_profile_plan
        error_type = PythonProfileError
        expected_code = "python_profile_candidate_authorization_invalid"
    else:
        builder = _build_node_candidate_profile_plan
        error_type = NodeProfileError
        expected_code = "node_profile_candidate_authorization_invalid"

    with pytest.raises(error_type, match=expected_code):
        builder(
            eligibility,
            baseline_verified,
            forged_authorization,
            baseline_artifact_id=artifact_id,
        )
