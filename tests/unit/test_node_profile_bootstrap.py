"""Synthetic-data coverage for the trusted Node/npm profile bootstrap."""

from __future__ import annotations

import base64
import hashlib
import json
import stat
import urllib.error
from pathlib import Path
from typing import Any

import pytest

from continuum_api.artifacts.archives import ArchiveFile, create_archive, verify_archive
from continuum_api.sandbox import node_bootstrap
from continuum_api.sandbox.node_bootstrap import (
    NodeBootstrapError,
    _scan_node_modules,
    extract_verified_archive,
    parse_node_lock,
    probe_network,
    validate_installed_tree,
)
from continuum_api.sandbox.node_bootstrap_recipe import (
    BOOTSTRAP_PATH,
    CONTROL_DIRECTORY,
    NODE_EXECUTABLE,
    NPM_CLI_PATH,
    NPM_REGISTRY_URL,
    bootstrap_argv,
    extract_argv,
    network_probe_argv,
    npm_ci_argv,
    npm_ci_bootstrap_argv,
    npm_ci_environment,
    npm_test_argv,
    npm_test_environment,
    validate_install_argv,
    verify_runtime_argv,
)


def _lock_documents(
    *,
    lockfile_version: int = 3,
    package: dict[str, Any] | None = None,
    entry: dict[str, Any] | None = None,
    extra_packages: dict[str, dict[str, Any]] | None = None,
) -> tuple[bytes, bytes]:
    package_data = package or {
        "name": "fixture-app",
        "version": "1.0.0",
        "packageManager": "npm@11.19.0",
        "scripts": {"test": "node test.js"},
        "dependencies": {"tiny-package": "1.2.3"},
    }
    package_entry = entry or {
        "version": "1.2.3",
        "resolved": "https://registry.npmjs.org/tiny-package/-/tiny-package-1.2.3.tgz",
        "integrity": "sha512-" + base64.b64encode(b"x" * 64).decode("ascii"),
    }
    root = {
        "name": package_data["name"],
        "version": package_data["version"],
    }
    if "dependencies" in package_data:
        root["dependencies"] = package_data["dependencies"]
    locked_packages = {"": root, "node_modules/tiny-package": package_entry}
    if extra_packages:
        locked_packages.update(extra_packages)
    lock = {
        "name": package_data["name"],
        "version": package_data["version"],
        "lockfileVersion": lockfile_version,
        "requires": True,
        "packages": locked_packages,
    }
    return (
        json.dumps(package_data, sort_keys=True, separators=(",", ":")).encode(),
        json.dumps(lock, sort_keys=True, separators=(",", ":")).encode(),
    )


def _valid_lock(package: dict[str, Any] | None = None, *, version: int = 3) -> tuple[bytes, str]:
    package_bytes, lock_bytes = _lock_documents(package=package, lockfile_version=version)
    return lock_bytes, hashlib.sha256(lock_bytes).hexdigest()


def test_node_lock_accepts_supported_v2_and_v3_public_registry_locks() -> None:
    for version in (2, 3):
        package_json, package_lock = _lock_documents(lockfile_version=version)
        lock = parse_node_lock(
            package_json,
            package_lock,
            expected_lockfile_sha256=hashlib.sha256(package_lock).hexdigest(),
        )
        assert lock.package_count == 1
        assert lock.packages[0].name == "tiny-package"
        assert lock.lockfile_sha256 == hashlib.sha256(package_lock).hexdigest()


def test_package_count_deduplicates_identical_physical_lock_entries() -> None:
    duplicate = {
        "version": "1.2.3",
        "resolved": "https://registry.npmjs.org/tiny-package/-/tiny-package-1.2.3.tgz",
        "integrity": "sha512-" + base64.b64encode(b"x" * 64).decode("ascii"),
        "name": "tiny-package",
    }
    package_json, package_lock = _lock_documents(
        extra_packages={"node_modules/parent/node_modules/tiny-package": duplicate}
    )

    lock = parse_node_lock(
        package_json,
        package_lock,
        expected_lockfile_sha256=hashlib.sha256(package_lock).hexdigest(),
    )

    assert len(lock.packages) == 2
    assert lock.package_count == 1


@pytest.mark.parametrize(
    "package_name",
    [
        "chrome-aws-lambda",
        "@sparticuz/chromium",
        "@sparticuz/chromium-min",
        "@puppeteer/browsers",
    ],
)
def test_node_lock_rejects_known_browser_runtime_packages(package_name: str) -> None:
    scoped_path = package_name.replace("@", "") if package_name.startswith("@") else package_name
    lock_path = f"node_modules/{scoped_path}"
    package = {
        "name": "fixture-app",
        "version": "1.0.0",
        "packageManager": "npm@11.19.0",
        "scripts": {"test": "node test.js"},
        "dependencies": {package_name: "1.2.3"},
    }
    # Scoped npm lock keys retain the leading @ directory.
    if package_name.startswith("@"):
        lock_path = f"node_modules/{package_name}"
    browser_entry = {
        "name": package_name,
        "version": "1.2.3",
        "resolved": f"https://registry.npmjs.org/{package_name}/-/{package_name.split('/')[-1]}-1.2.3.tgz",
        "integrity": "sha512-" + base64.b64encode(b"b" * 64).decode("ascii"),
    }
    package_json, package_lock = _lock_documents(
        package=package,
        extra_packages={lock_path: browser_entry},
    )

    with pytest.raises(NodeBootstrapError, match="browser_dependency_unsupported"):
        parse_node_lock(
            package_json,
            package_lock,
            expected_lockfile_sha256=hashlib.sha256(package_lock).hexdigest(),
        )


@pytest.mark.parametrize(
    ("change", "expected_code"),
    [
        ({"resolved": "https://evil.example/pkg.tgz"}, "package_source_unsupported"),
        ({"resolved": "git+https://github.com/example/pkg.git"}, "package_source_unsupported"),
        (
            {"resolved": "https://registry.npmjs.org/pkg.tgz", "link": True},
            "package_source_unsupported",
        ),
        (
            {"resolved": "https://registry.npmjs.org/pkg.tgz", "inBundle": True},
            "package_source_unsupported",
        ),
        (
            {"resolved": "https://registry.npmjs.org/pkg.tgz", "hasShrinkwrap": True},
            "package_source_unsupported",
        ),
        (
            {"resolved": "https://registry.npmjs.org/pkg.tgz", "os": ["linux"]},
            "platform_package_unsupported",
        ),
        (
            {"resolved": "https://registry.npmjs.org/pkg.tgz", "hasInstallScript": True},
            "lifecycle_hook_unsupported",
        ),
        (
            {"resolved": "https://registry.npmjs.org/pkg.tgz", "integrity": None},
            "package_integrity_missing",
        ),
        (
            {"resolved": "https://registry.npmjs.org/pkg.tgz", "integrity": "sha1-abc"},
            "package_integrity_missing",
        ),
    ],
)
def test_node_lock_rejects_unsupported_package_sources_and_hooks(
    change: dict[str, Any], expected_code: str
) -> None:
    entry = {
        "version": "1.2.3",
        "resolved": "https://registry.npmjs.org/tiny-package/-/tiny-package-1.2.3.tgz",
        "integrity": "sha512-" + base64.b64encode(b"x" * 64).decode("ascii"),
    }
    entry.update(change)
    package_json, package_lock = _lock_documents(entry=entry)

    with pytest.raises(NodeBootstrapError, match=expected_code):
        parse_node_lock(
            package_json,
            package_lock,
            expected_lockfile_sha256=hashlib.sha256(package_lock).hexdigest(),
        )


@pytest.mark.parametrize(
    ("package_change", "repo_files", "contents", "expected_code"),
    [
        ({"workspaces": ["packages/*"]}, (), {}, "workspaces_unsupported"),
        ({"packageManager": "npm@10.0.0"}, (), {}, "package_manager_unsupported"),
        (
            {"scripts": {"test": "node test.js", "posttest": "echo extra"}},
            (),
            {},
            "lifecycle_hook_unsupported",
        ),
        ({}, (".npmrc",), {}, "npm_config_unsupported"),
        ({}, (".nvmrc",), {".nvmrc": b"20\n"}, "version_file_unsupported"),
    ],
)
def test_node_lock_rejects_unsupported_project_configuration(
    package_change: dict[str, Any],
    repo_files: tuple[str, ...],
    contents: dict[str, bytes],
    expected_code: str,
) -> None:
    base = {
        "name": "fixture-app",
        "version": "1.0.0",
        "packageManager": "npm@11.19.0",
        "scripts": {"test": "node test.js"},
        "dependencies": {"tiny-package": "1.2.3"},
    }
    base.update(package_change)
    package_json, package_lock = _lock_documents(package=base)
    with pytest.raises(NodeBootstrapError, match=expected_code):
        parse_node_lock(
            package_json,
            package_lock,
            expected_lockfile_sha256=hashlib.sha256(package_lock).hexdigest(),
            repository_files=repo_files,
            version_file_contents=contents,
        )


def test_node_lock_accepts_matching_node_version_file_and_rejects_lock_digest_mismatch() -> None:
    package_json, package_lock = _lock_documents()
    lock = parse_node_lock(
        package_json,
        package_lock,
        expected_lockfile_sha256=hashlib.sha256(package_lock).hexdigest(),
        repository_files=(".node-version",),
        version_file_contents={".node-version": b"v24.21.0\n"},
    )
    assert lock.package_count == 1
    with pytest.raises(NodeBootstrapError, match="lock_digest_mismatch"):
        parse_node_lock(package_json, package_lock, expected_lockfile_sha256="0" * 64)


def test_node_lock_rejects_unsafe_package_paths_and_duplicate_json_keys() -> None:
    package_json, package_lock = _lock_documents(
        extra_packages={
            "node_modules/../../outside": {
                "name": "outside",
                "version": "1.2.3",
                "resolved": "https://registry.npmjs.org/outside/-/outside-1.2.3.tgz",
                "integrity": "sha512-" + base64.b64encode(b"c" * 64).decode("ascii"),
            }
        }
    )
    with pytest.raises(NodeBootstrapError, match="lock_package_path_invalid"):
        parse_node_lock(
            package_json,
            package_lock,
            expected_lockfile_sha256=hashlib.sha256(package_lock).hexdigest(),
        )

    duplicate = b'{"name":"fixture","name":"attacker","scripts":{"test":"node test.js"}}'
    _, valid_lock = _lock_documents()
    with pytest.raises(NodeBootstrapError, match="package_json_invalid"):
        parse_node_lock(
            duplicate,
            valid_lock,
            expected_lockfile_sha256=hashlib.sha256(valid_lock).hexdigest(),
        )


def test_archive_extraction_verifies_t14_tree_and_preserves_modes(tmp_path: Path) -> None:
    package_json, package_lock = _lock_documents()
    bundle = create_archive(
        (
            ArchiveFile("package.json", package_json),
            ArchiveFile("package-lock.json", package_lock),
            ArchiveFile("src/check.js", b"console.log('fixture')\n", 0o755),
            ArchiveFile("sitecustomize.py", b"raise RuntimeError('data only')\n"),
        )
    )
    workspace = tmp_path / "workspace"
    receipt = extract_verified_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_lockfile_sha256=hashlib.sha256(package_lock).hexdigest(),
        destination=workspace,
        verifier=verify_archive,
    )

    assert receipt.file_count == 4
    assert receipt.lockfile_sha256 == hashlib.sha256(package_lock).hexdigest()
    assert (workspace / "src/check.js").read_bytes() == b"console.log('fixture')\n"
    assert stat.S_IMODE((workspace / "src/check.js").stat().st_mode) == 0o755
    assert (workspace / "sitecustomize.py").exists()


def test_archive_extraction_rejects_nonempty_and_symlink_targets(tmp_path: Path) -> None:
    package_json, package_lock = _lock_documents()
    bundle = create_archive(
        (
            ArchiveFile("package.json", package_json),
            ArchiveFile("package-lock.json", package_lock),
        )
    )
    nonempty = tmp_path / "nonempty"
    nonempty.mkdir()
    marker = nonempty / "preserve.txt"
    marker.write_bytes(b"preserve")
    arguments = {
        "expected_archive_sha256": bundle.archive_sha256,
        "expected_tree_sha256": bundle.manifest.tree_sha256,
        "expected_lockfile_sha256": hashlib.sha256(package_lock).hexdigest(),
        "verifier": verify_archive,
    }
    with pytest.raises(NodeBootstrapError, match="workspace_not_empty"):
        extract_verified_archive(bundle.archive, destination=nonempty, **arguments)
    assert marker.read_bytes() == b"preserve"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["nonempty"]

    actual = tmp_path / "actual"
    actual.mkdir()
    (actual / "preserve.txt").write_bytes(b"target")
    link = tmp_path / "linked"
    link.symlink_to(actual, target_is_directory=True)
    with pytest.raises(NodeBootstrapError, match="workspace_not_directory"):
        extract_verified_archive(bundle.archive, destination=link, **arguments)
    assert link.is_symlink()
    assert (actual / "preserve.txt").read_bytes() == b"target"


class _ProbeResponse:
    status = 200

    def __init__(self, url: str) -> None:
        self._url = url
        self.read_sizes: list[int] = []
        self.closed = False

    def geturl(self) -> str:
        return self._url

    def read(self, size: int) -> bytes:
        self.read_sizes.append(size)
        return b"registry fixture body"

    def close(self) -> None:
        self.closed = True


class _ProbeOpener:
    def __init__(
        self, response: _ProbeResponse | None = None, error: Exception | None = None
    ) -> None:
        self.response = response
        self.error = error
        self.url: str | None = None
        self.timeout: float | None = None

    def open(self, request: Any, *, timeout: float) -> _ProbeResponse:
        self.url = request.full_url
        self.timeout = timeout
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return self.response


def test_network_probe_is_bounded_and_denial_requires_connection_failure() -> None:
    response = _ProbeResponse("https://registry.npmjs.org/-/ping")
    opener = _ProbeOpener(response)
    receipt = probe_network("allowed", opener=opener)
    assert receipt.observed_allowed
    assert opener.url == "https://registry.npmjs.org/-/ping"
    assert opener.timeout == 2.0
    assert response.read_sizes == [256]
    assert response.closed

    reachable = _ProbeResponse("https://registry.npmjs.org/-/ping")
    with pytest.raises(NodeBootstrapError, match="egress_not_denied"):
        probe_network("denied", opener=_ProbeOpener(reachable))
    assert reachable.closed

    denied = probe_network("denied", opener=_ProbeOpener(error=urllib.error.URLError("blocked")))
    assert denied.expected == "denied"
    assert not denied.observed_allowed


def test_network_probe_rejects_redirected_allowed_response() -> None:
    with pytest.raises(NodeBootstrapError, match="registry_probe_failed"):
        probe_network("allowed", opener=_ProbeOpener(_ProbeResponse("https://evil.invalid/")))


def _write_installed_fixture(workspace: Path, *, native: bool = False) -> tuple[bytes, str]:
    package_json, package_lock = _lock_documents()
    (workspace / "package.json").write_bytes(package_json)
    (workspace / "package-lock.json").write_bytes(package_lock)
    package_dir = workspace / "node_modules/tiny-package"
    (package_dir / "bin").mkdir(parents=True)
    (package_dir / "package.json").write_text(
        json.dumps({"name": "tiny-package", "version": "1.2.3", "bin": {"tiny": "bin/tiny.js"}}),
        encoding="utf-8",
    )
    (package_dir / "bin/tiny.js").write_text("process.exit(0)\n", encoding="utf-8")
    if native:
        (package_dir / "addon.node").write_bytes(b"synthetic native payload")
    bin_directory = workspace / "node_modules/.bin"
    bin_directory.mkdir()
    (bin_directory / "tiny").symlink_to("../tiny-package/bin/tiny.js")
    return package_lock, hashlib.sha256(package_lock).hexdigest()


def test_validate_installed_tree_checks_real_files_without_importing_packages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    package_lock, lock_sha256 = _write_installed_fixture(workspace)
    monkeypatch.setattr(node_bootstrap, "WORKSPACE_PATH", workspace)
    monkeypatch.setattr(node_bootstrap, "PACKAGE_JSON_PATH", workspace / "package.json")
    monkeypatch.setattr(node_bootstrap, "PACKAGE_LOCK_PATH", workspace / "package-lock.json")
    monkeypatch.setattr(node_bootstrap, "NODE_MODULES_PATH", workspace / "node_modules")

    receipt = validate_installed_tree(lock_sha256)

    assert receipt.lockfile_sha256 == hashlib.sha256(package_lock).hexdigest()
    assert receipt.package_count == 1
    assert receipt.file_count >= 2
    assert receipt.total_bytes > 0


def test_validate_installed_tree_rejects_native_files_and_outside_links(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    _, lock_sha256 = _write_installed_fixture(workspace, native=True)
    monkeypatch.setattr(node_bootstrap, "WORKSPACE_PATH", workspace)
    monkeypatch.setattr(node_bootstrap, "PACKAGE_JSON_PATH", workspace / "package.json")
    monkeypatch.setattr(node_bootstrap, "PACKAGE_LOCK_PATH", workspace / "package-lock.json")
    monkeypatch.setattr(node_bootstrap, "NODE_MODULES_PATH", workspace / "node_modules")

    with pytest.raises(NodeBootstrapError, match="native_dependency_unsupported"):
        validate_installed_tree(lock_sha256)

    native = workspace / "node_modules/tiny-package/addon.node"
    native.unlink()
    extensionless = workspace / "node_modules/tiny-package/runner"
    extensionless.write_bytes(b"\x7fELF" + b"synthetic payload")
    with pytest.raises(NodeBootstrapError, match="native_dependency_unsupported"):
        validate_installed_tree(lock_sha256)
    extensionless.unlink()
    browser_asset = workspace / "node_modules/tiny-package/chromium.br"
    browser_asset.write_bytes(b"synthetic compressed browser asset")
    with pytest.raises(NodeBootstrapError, match="browser_dependency_unsupported"):
        validate_installed_tree(lock_sha256)
    browser_asset.unlink()
    (workspace / "node_modules/.bin/tiny").unlink()
    outside = tmp_path / "outside.js"
    outside.write_text("not in node_modules", encoding="utf-8")
    (workspace / "node_modules/.bin/tiny").symlink_to(outside)
    with pytest.raises(NodeBootstrapError, match="install_link_unsupported"):
        validate_installed_tree(lock_sha256)


def test_install_scan_bounds_directory_entries_and_depth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node_modules = tmp_path / "node_modules"
    node_modules.mkdir()
    for name in ("a", "b", "c"):
        (node_modules / name).mkdir()
    monkeypatch.setattr(node_bootstrap, "NODE_MODULES_PATH", node_modules)
    monkeypatch.setattr(node_bootstrap, "MAX_NPM_ENTRIES", 2)
    with pytest.raises(NodeBootstrapError, match="install_tree_entry_limit"):
        _scan_node_modules()

    monkeypatch.setattr(node_bootstrap, "MAX_NPM_ENTRIES", 100)
    monkeypatch.setattr(node_bootstrap, "MAX_NPM_DEPTH", 1)
    (node_modules / "a" / "deep").mkdir()
    with pytest.raises(NodeBootstrapError, match="install_tree_depth_limit"):
        _scan_node_modules()


def test_recipe_uses_isolated_bootstrap_and_fixed_registry_commands() -> None:
    assert verify_runtime_argv() == (
        "/usr/local/bin/python3.13",
        "-I",
        "-S",
        BOOTSTRAP_PATH,
        "verify-runtime",
    )
    assert extract_argv("a" * 64, "b" * 64, "c" * 64)[-6:] == (
        "--archive-sha256",
        "a" * 64,
        "--tree-sha256",
        "b" * 64,
        "--lockfile-sha256",
        "c" * 64,
    )
    assert network_probe_argv("allowed")[-2:] == ("--expected", "allowed")
    assert bootstrap_argv("verify-runtime")[3] == BOOTSTRAP_PATH
    assert npm_ci_bootstrap_argv("d" * 64)[-2:] == ("--lockfile-sha256", "d" * 64)
    assert validate_install_argv("e" * 64)[-2:] == ("--lockfile-sha256", "e" * 64)
    assert npm_ci_argv()[:2] == (NODE_EXECUTABLE, NPM_CLI_PATH)
    assert "ci" in npm_ci_argv()
    assert "--ignore-scripts" in npm_ci_argv()
    assert f"--registry={NPM_REGISTRY_URL}" in npm_ci_argv()
    assert npm_ci_argv()[2:] == node_bootstrap.NPM_CI_ARGUMENTS
    assert dict(npm_ci_environment()) == node_bootstrap._npm_environment()
    assert npm_test_argv() == (NODE_EXECUTABLE, NPM_CLI_PATH, "--ignore-scripts", "test")
    assert npm_ci_environment()["HOME"] == f"{CONTROL_DIRECTORY}/home"
    assert npm_test_environment()["CI"] == "true"
    assert "NODE_OPTIONS" not in npm_test_environment()


def test_npm_empty_config_paths_are_distinct_and_image_owned() -> None:
    from continuum_api.sandbox.node_bootstrap import _npm_environment
    from continuum_api.sandbox.node_bootstrap_recipe import npm_ci_environment, npm_test_environment

    for environment in (_npm_environment(), npm_ci_environment(), npm_test_environment()):
        user = environment["NPM_CONFIG_USERCONFIG"]
        global_config = environment["NPM_CONFIG_GLOBALCONFIG"]
        assert user == "/opt/continuum/npm-user-empty.npmrc"
        assert global_config == "/opt/continuum/npm-global-empty.npmrc"
        assert user != global_config
    argv = npm_ci_argv()
    assert "--userconfig=/opt/continuum/npm-user-empty.npmrc" in argv
    assert "--globalconfig=/opt/continuum/npm-global-empty.npmrc" in argv
