"""Trusted Node/npm setup phases for the immutable Node profile image.

Run only from `/opt/continuum` with the image's base Python 3.13.12 using
`-I -S`. Repository files remain data through extraction, dependency install,
and install-tree validation; the driver starts the explicit test script only
after the managed session proves deny-all egress.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import http.client
import importlib.util
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

NODE_PROFILE_VERSION = "1"
PYTHON_RUNTIME = (3, 13, 12)
NODE_RUNTIME_VERSION = "24.21.0"
NPM_RUNTIME_VERSION = "11.19.0"
TARGET_MACHINE = "x86_64"

CONTROL_DIRECTORY = Path("/opt/continuum")
BOOTSTRAP_PATH = CONTROL_DIRECTORY / "node_bootstrap.py"
TRUSTED_ARCHIVER_PATH = CONTROL_DIRECTORY / "trusted" / "archives.py"
ARCHIVE_PATH = CONTROL_DIRECTORY / "input" / "source.tar.gz"
NPM_CACHE_PATH = CONTROL_DIRECTORY / "npm-cache"
WORKSPACE_PATH = Path("/vercel/sandbox/workspace")
PACKAGE_JSON_PATH = WORKSPACE_PATH / "package.json"
PACKAGE_LOCK_PATH = WORKSPACE_PATH / "package-lock.json"
NODE_MODULES_PATH = WORKSPACE_PATH / "node_modules"
NODE_EXECUTABLE = Path("/usr/local/bin/node")
NPM_CLI_PATH = Path("/usr/local/lib/node_modules/npm/bin/npm-cli.js")
NPM_REGISTRY_URL = "https://registry.npmjs.org/"
NPM_PROBE_URL = "https://registry.npmjs.org/-/ping"
NPM_CI_ARGUMENTS = (
    "ci",
    "--ignore-scripts",
    "--no-audit",
    "--no-fund",
    "--engine-strict",
    f"--registry={NPM_REGISTRY_URL}",
    "--userconfig=/dev/null",
    "--globalconfig=/dev/null",
    f"--cache={NPM_CACHE_PATH}",
    "--fetch-retries=0",
    "--fetch-timeout=15000",
    "--maxsockets=4",
)

MAX_ARCHIVE_BYTES = 10 * 1024 * 1024
MAX_JSON_BYTES = 256 * 1024
MAX_PACKAGE_JSON_BYTES = 256 * 1024
MAX_LOCKED_PACKAGES = 1_000
MAX_UNIQUE_NODE_PACKAGES = 24
MAX_NPM_FILES = 50_000
MAX_NPM_ENTRIES = 100_000
MAX_NPM_DIRECTORIES = 20_000
MAX_NPM_DEPTH = 64
MAX_NPM_FILE_BYTES = 50 * 1024 * 1024
MAX_NODE_MODULES_BYTES = 512 * 1024 * 1024
MAX_SOURCE_ENTRIES = 10_000
MAX_SOURCE_DEPTH = 128
MAX_NPM_SECONDS = 120
PROBE_TIMEOUT_SECONDS = 2.0
PROBE_RESPONSE_BYTES = 256

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SRI_SHA512 = re.compile(r"^sha512-([A-Za-z0-9+/]+={0,2})$")
_SAFE_PACKAGE_NAME = re.compile(r"^(?:@[a-z0-9._~-]+/)?[a-z0-9._~-]+$")
_PACKAGE_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.!+_-]{0,127}$")
_UNSUPPORTED_SPEC_PREFIXES = (
    "file:",
    "link:",
    "workspace:",
    "git:",
    "git+",
    "github:",
    "gitlab:",
    "bitbucket:",
    "http:",
    "https:",
    "npm:",
)
_INSTALL_HOOKS = frozenset(
    {
        "prepublish",
        "prepare",
        "prepublishOnly",
        "prepack",
        "postpack",
        "publish",
        "postpublish",
        "preinstall",
        "install",
        "postinstall",
        "preuninstall",
        "uninstall",
        "postuninstall",
        "preversion",
        "version",
        "postversion",
    }
)
_TEST_HOOKS = frozenset({"pretest", "posttest"})
_NATIVE_SUFFIXES = frozenset(
    {
        ".a",
        ".c",
        ".cc",
        ".cpp",
        ".cxx",
        ".dylib",
        ".dll",
        ".gyp",
        ".gypi",
        ".h",
        ".hh",
        ".hpp",
        ".m",
        ".mm",
        ".node",
        ".o",
        ".obj",
        ".so",
    }
)
_NATIVE_BUILD_BASENAMES = frozenset({"binding.gyp", "cmakelists.txt", "makefile"})
_NATIVE_BINARY_MAGICS = (
    b"\x7fELF",
    b"MZ",
    b"\xfe\xed\xfa\xce",
    b"\xce\xfa\xed\xfe",
    b"\xfe\xed\xfa\xcf",
    b"\xcf\xfa\xed\xfe",
    b"\xca\xfe\xba\xbe",
    b"\xbe\xba\xfe\xca",
    b"\xca\xfe\xba\xbf",
    b"\xbf\xba\xfe\xca",
)
_BROWSER_PACKAGES = frozenset(
    {
        "@puppeteer/browsers",
        "@sparticuz/chromium",
        "@sparticuz/chromium-min",
        "chrome-aws-lambda",
        "chromedriver",
        "cypress",
        "electron",
        "geckodriver",
        "playwright",
        "playwright-core",
        "puppeteer",
        "puppeteer-core",
        "selenium-webdriver",
    }
)
_BROWSER_ASSET_BASENAMES = frozenset(
    {
        "chromium.br",
        "chrome-headless-shell-linux64.zip",
        "chrome-headless-shell-mac-x64.zip",
        "chrome-headless-shell-win64.zip",
        "chrome-linux.zip",
        "chrome-mac.zip",
        "chrome-win.zip",
        "swiftshader.tar.br",
    }
)


class NodeBootstrapError(RuntimeError):
    """Stable, redacted failure code for trusted Node profile setup."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class LockedNodePackage:
    name: str
    version: str
    lock_path: str
    optional: bool
    resolved_url: str
    integrity: str


@dataclass(frozen=True, slots=True)
class NodeLock:
    lockfile_sha256: str
    package_count: int
    packages: tuple[LockedNodePackage, ...]


@dataclass(frozen=True, slots=True)
class NodeExtractionReceipt:
    archive_sha256: str
    tree_sha256: str
    lockfile_sha256: str
    file_count: int
    source_bytes: int


@dataclass(frozen=True, slots=True)
class NodeInstallReceipt:
    lockfile_sha256: str
    package_count: int
    file_count: int
    total_bytes: int


@dataclass(frozen=True, slots=True)
class ProbeReceipt:
    expected: Literal["allowed", "denied"]
    observed_allowed: bool


def verify_runtime_identity(
    *,
    command_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> dict[str, str]:
    """Bind the helper to the image's exact Python, Node, npm and platform."""
    if (
        sys.implementation.name != "cpython"
        or tuple(sys.version_info[:3]) != PYTHON_RUNTIME
        or platform.system() != "Linux"
        or platform.machine().lower() not in {TARGET_MACHINE, "amd64"}
        or not sys.flags.isolated
        or not sys.flags.no_site
    ):
        raise NodeBootstrapError("node_profile_runtime_identity_invalid")
    runner = command_runner or subprocess.run
    node_version = _run_version((str(NODE_EXECUTABLE), "--version"), runner=runner)
    npm_version = _run_version(
        (str(NODE_EXECUTABLE), str(NPM_CLI_PATH), "--version"),
        runner=runner,
    )
    if node_version != f"v{NODE_RUNTIME_VERSION}" or npm_version != NPM_RUNTIME_VERSION:
        raise NodeBootstrapError("node_profile_runtime_identity_invalid")
    return {
        "machine": TARGET_MACHINE,
        "node_version": NODE_RUNTIME_VERSION,
        "npm_version": NPM_RUNTIME_VERSION,
        "profile_version": NODE_PROFILE_VERSION,
    }


def parse_node_lock(
    package_json_bytes: bytes,
    package_lock_bytes: bytes,
    *,
    expected_lockfile_sha256: str,
    repository_files: Iterable[str] = (),
    version_file_contents: Mapping[str, bytes] | None = None,
) -> NodeLock:
    """Validate the supported single-package public-registry npm lock subset."""
    _validate_sha256(expected_lockfile_sha256, "node_profile_lock_digest_invalid")
    if (
        not isinstance(package_json_bytes, bytes)
        or not isinstance(package_lock_bytes, bytes)
        or len(package_json_bytes) > MAX_PACKAGE_JSON_BYTES
        or len(package_lock_bytes) > MAX_JSON_BYTES
    ):
        raise NodeBootstrapError("node_profile_manifest_size_invalid")
    actual_lock_sha256 = hashlib.sha256(package_lock_bytes).hexdigest()
    if actual_lock_sha256 != expected_lockfile_sha256:
        raise NodeBootstrapError("node_profile_lock_digest_mismatch")
    package_json = _load_json_object(package_json_bytes, "node_profile_package_json_invalid")
    package_lock = _load_json_object(package_lock_bytes, "node_profile_package_lock_invalid")

    repository_paths = tuple(repository_files)
    file_basenames = {Path(path).name.casefold() for path in repository_paths}
    if ".npmrc" in file_basenames:
        raise NodeBootstrapError("node_profile_npm_config_unsupported")
    if "npm-shrinkwrap.json" in file_basenames:
        raise NodeBootstrapError("node_profile_shrinkwrap_unsupported")
    for basename in (".node-version", ".nvmrc"):
        matching = [path for path in repository_paths if Path(path).name.casefold() == basename]
        for path in matching:
            content = (version_file_contents or {}).get(path)
            try:
                selected_version = content.decode("ascii").strip() if content is not None else ""
            except UnicodeDecodeError:
                raise NodeBootstrapError("node_profile_version_file_unsupported") from None
            if selected_version not in {"24", "v24", "24.x", "v24.x", "24.21.0", "v24.21.0"}:
                raise NodeBootstrapError("node_profile_version_file_unsupported")

    if "workspaces" in package_json:
        raise NodeBootstrapError("node_profile_workspaces_unsupported")
    if package_json.get("packageManager") not in (None, f"npm@{NPM_RUNTIME_VERSION}"):
        raise NodeBootstrapError("node_profile_package_manager_unsupported")
    scripts = package_json.get("scripts", {})
    if not isinstance(scripts, dict):
        raise NodeBootstrapError("node_profile_scripts_invalid")
    if not isinstance(scripts.get("test"), str) or not scripts["test"].strip():
        raise NodeBootstrapError("node_profile_test_script_missing")
    if _INSTALL_HOOKS.intersection(scripts) or _TEST_HOOKS.intersection(scripts):
        raise NodeBootstrapError("node_profile_lifecycle_hook_unsupported")

    for field in ("dependencies", "devDependencies", "optionalDependencies", "peerDependencies"):
        dependencies = package_json.get(field, {})
        if dependencies is None:
            continue
        if not isinstance(dependencies, dict):
            raise NodeBootstrapError("node_profile_dependency_manifest_invalid")
        for name, specifier in dependencies.items():
            if not _is_package_name(name) or not isinstance(specifier, str):
                raise NodeBootstrapError("node_profile_dependency_manifest_invalid")
            if _is_unsupported_specifier(specifier):
                raise NodeBootstrapError("node_profile_package_source_unsupported")

    lockfile_version = package_lock.get("lockfileVersion")
    packages = package_lock.get("packages")
    if type(lockfile_version) is not int or lockfile_version not in {2, 3}:
        raise NodeBootstrapError("node_profile_lockfile_version_unsupported")
    if not isinstance(packages, dict) or "" not in packages or not isinstance(packages[""], dict):
        raise NodeBootstrapError("node_profile_lock_packages_invalid")
    root_lock = packages[""]
    for field in ("name", "version"):
        source_value = package_json.get(field)
        lock_value = package_lock.get(field)
        root_value = root_lock.get(field)
        present_values = [
            value for value in (source_value, lock_value, root_value) if value is not None
        ]
        if present_values and (
            len(present_values) != 3
            or not isinstance(source_value, str)
            or source_value != lock_value
            or source_value != root_value
        ):
            raise NodeBootstrapError("node_profile_lock_manifest_mismatch")
    if package_json.get("name") is not None and not _is_package_name(package_json.get("name")):
        raise NodeBootstrapError("node_profile_lock_packages_invalid")
    if package_json.get("version") is not None and (
        not isinstance(package_json.get("version"), str)
        or _PACKAGE_VERSION.fullmatch(package_json["version"]) is None
    ):
        raise NodeBootstrapError("node_profile_lock_packages_invalid")
    if any(key in root_lock for key in ("workspaces", "bundledDependencies", "bundleDependencies")):
        raise NodeBootstrapError("node_profile_workspaces_unsupported")
    for field in (
        "dependencies",
        "devDependencies",
        "optionalDependencies",
        "peerDependencies",
    ):
        declared = package_json.get(field, {}) or {}
        locked = root_lock.get(field, {}) or {}
        if not isinstance(locked, dict):
            raise NodeBootstrapError("node_profile_lock_packages_invalid")
        if declared != locked:
            raise NodeBootstrapError("node_profile_lock_manifest_mismatch")
        if field in {
            "dependencies",
            "devDependencies",
            "optionalDependencies",
            "peerDependencies",
        }:
            for name in declared:
                if f"node_modules/{name}" not in packages:
                    raise NodeBootstrapError("node_profile_lock_manifest_mismatch")

    lock_packages: list[LockedNodePackage] = []
    package_records: dict[tuple[str, str], tuple[str, str]] = {}
    seen_paths: set[str] = set()
    for lock_path, raw_entry in packages.items():
        if lock_path == "":
            continue
        if not isinstance(lock_path, str) or not isinstance(raw_entry, dict):
            raise NodeBootstrapError("node_profile_lock_packages_invalid")
        canonical_path = _validate_lock_package_path(lock_path)
        if canonical_path in seen_paths:
            raise NodeBootstrapError("node_profile_lock_packages_invalid")
        seen_paths.add(canonical_path)
        if any(raw_entry.get(field) is True for field in ("link", "inBundle", "hasShrinkwrap")):
            raise NodeBootstrapError("node_profile_package_source_unsupported")
        if "os" in raw_entry or "cpu" in raw_entry:
            raise NodeBootstrapError("node_profile_platform_package_unsupported")
        if raw_entry.get("hasInstallScript") is True:
            raise NodeBootstrapError("node_profile_lifecycle_hook_unsupported")
        resolved = raw_entry.get("resolved")
        if not isinstance(resolved, str) or not _is_public_registry_url(resolved):
            raise NodeBootstrapError("node_profile_package_source_unsupported")
        if not _has_sha512_integrity(raw_entry.get("integrity")):
            raise NodeBootstrapError("node_profile_package_integrity_missing")
        package_name = _package_name_from_lock_path(lock_path)
        declared_name = raw_entry.get("name")
        if declared_name is not None and declared_name != package_name:
            raise NodeBootstrapError("node_profile_lock_manifest_mismatch")
        version = raw_entry.get("version")
        if (
            not _is_package_name(package_name)
            or not isinstance(version, str)
            or _PACKAGE_VERSION.fullmatch(version) is None
        ):
            raise NodeBootstrapError("node_profile_lock_packages_invalid")
        if package_name.casefold() in _BROWSER_PACKAGES:
            raise NodeBootstrapError("node_profile_browser_dependency_unsupported")
        integrity = cast(str, raw_entry["integrity"])
        record_identity = (package_name, version)
        package_record = (resolved, integrity)
        previous_record = package_records.get(record_identity)
        if previous_record is not None and previous_record != package_record:
            raise NodeBootstrapError("node_profile_lock_manifest_mismatch")
        package_records[record_identity] = package_record
        lock_packages.append(
            LockedNodePackage(
                name=package_name,
                version=version,
                lock_path=lock_path,
                optional=raw_entry.get("optional") is True,
                resolved_url=resolved,
                integrity=integrity,
            )
        )
        if len(lock_packages) > MAX_LOCKED_PACKAGES:
            raise NodeBootstrapError("node_profile_package_limit")
    if not lock_packages:
        raise NodeBootstrapError("node_profile_lock_packages_empty")
    unique_packages = set(package_records)
    if len(unique_packages) > MAX_UNIQUE_NODE_PACKAGES:
        raise NodeBootstrapError("node_profile_package_limit")
    return NodeLock(actual_lock_sha256, len(unique_packages), tuple(lock_packages))


def extract_verified_archive(
    archive: bytes,
    *,
    expected_archive_sha256: str,
    expected_tree_sha256: str,
    expected_lockfile_sha256: str,
    destination: str | os.PathLike[str],
    verifier: Callable[..., Any] | None = None,
) -> NodeExtractionReceipt:
    """Verify a T14 source archive before atomically extracting its files."""
    _validate_sha256(expected_archive_sha256, "node_profile_archive_digest_invalid")
    _validate_sha256(expected_tree_sha256, "node_profile_tree_digest_invalid")
    _validate_sha256(expected_lockfile_sha256, "node_profile_lock_digest_invalid")
    if not isinstance(archive, bytes) or not 1 <= len(archive) <= MAX_ARCHIVE_BYTES:
        raise NodeBootstrapError("node_profile_archive_size_invalid")
    verified = (verifier or _load_trusted_archive_verifier())(
        archive,
        expected_archive_sha256=expected_archive_sha256,
        expected_tree_sha256=expected_tree_sha256,
    )
    if (
        verified.archive_sha256 != expected_archive_sha256
        or verified.manifest.tree_sha256 != expected_tree_sha256
    ):
        raise NodeBootstrapError("node_profile_archive_digest_mismatch")
    if sum(len(source.content) for source in verified.files) > 50 * 1024 * 1024:
        raise NodeBootstrapError("node_profile_archive_metadata_invalid")
    package_jsons = [source for source in verified.files if source.path == "package.json"]
    package_locks = [source for source in verified.files if source.path == "package-lock.json"]
    if len(package_jsons) != 1 or len(package_locks) != 1:
        raise NodeBootstrapError("node_profile_package_manifests_missing")
    parse_node_lock(
        package_jsons[0].content,
        package_locks[0].content,
        expected_lockfile_sha256=expected_lockfile_sha256,
        repository_files=(source.path for source in verified.files),
        version_file_contents={source.path: source.content for source in verified.files},
    )

    destination_path = Path(destination)
    stage_path: Path | None = None
    try:
        _require_empty_workspace(destination_path)
        stage_path = Path(
            tempfile.mkdtemp(prefix=".continuum-node-source-", dir=destination_path.parent)
        )
        _write_source_files(stage_path, verified.files)
        if destination_path.exists():
            destination_path.rmdir()
        os.rename(stage_path, destination_path)
        stage_path = None
    except NodeBootstrapError:
        raise
    except OSError:
        raise NodeBootstrapError("node_profile_workspace_extract_failed") from None
    finally:
        if stage_path is not None:
            shutil.rmtree(stage_path, ignore_errors=True)
    return NodeExtractionReceipt(
        archive_sha256=verified.archive_sha256,
        tree_sha256=verified.manifest.tree_sha256,
        lockfile_sha256=expected_lockfile_sha256,
        file_count=len(verified.files),
        source_bytes=sum(len(source.content) for source in verified.files),
    )


def validate_workspace_lock(expected_lockfile_sha256: str) -> NodeLock:
    package_json = _read_regular_file(PACKAGE_JSON_PATH, MAX_PACKAGE_JSON_BYTES)
    package_lock = _read_regular_file(PACKAGE_LOCK_PATH, MAX_JSON_BYTES)
    repository_files = _workspace_relative_files()
    version_file_contents: dict[str, bytes] = {}
    for path in repository_files:
        if Path(path).name.casefold() in {".node-version", ".nvmrc"}:
            version_file_contents[path] = _read_regular_file(
                WORKSPACE_PATH / path,
                4_096,
            )
    return parse_node_lock(
        package_json,
        package_lock,
        expected_lockfile_sha256=expected_lockfile_sha256,
        repository_files=repository_files,
        version_file_contents=version_file_contents,
    )


def probe_network(
    expected: Literal["allowed", "denied"],
    *,
    opener: Any | None = None,
) -> ProbeReceipt:
    """Probe one fixed registry endpoint without redirects, proxies or writes."""
    if expected not in {"allowed", "denied"}:
        raise NodeBootstrapError("node_profile_network_expectation_invalid")
    selected_opener = opener or _registry_opener()
    request = urllib.request.Request(
        NPM_PROBE_URL,
        headers={"accept": "application/json", "user-agent": "Continuum-Node-Profile/1"},
        method="GET",
    )
    try:
        response = selected_opener.open(request, timeout=PROBE_TIMEOUT_SECONDS)
    except urllib.error.HTTPError:
        if expected == "denied":
            raise NodeBootstrapError("node_profile_egress_not_denied") from None
        raise NodeBootstrapError("node_profile_registry_probe_failed") from None
    except (urllib.error.URLError, TimeoutError, OSError):
        if expected == "denied":
            return ProbeReceipt(expected="denied", observed_allowed=False)
        raise NodeBootstrapError("node_profile_registry_probe_failed") from None
    try:
        status = getattr(response, "status", None)
        final_url = response.geturl()
        response.read(PROBE_RESPONSE_BYTES)
    finally:
        response.close()
    if expected == "denied":
        raise NodeBootstrapError("node_profile_egress_not_denied")
    if status != 200 or final_url != NPM_PROBE_URL:
        raise NodeBootstrapError("node_profile_registry_probe_failed")
    return ProbeReceipt(expected="allowed", observed_allowed=True)


def npm_ci(lockfile_sha256: str) -> NodeLock:
    """Install the validated lock with trusted npm and all lifecycle hooks off."""
    lock = validate_workspace_lock(lockfile_sha256)
    if not NPM_CLI_PATH.is_file() or not NODE_EXECUTABLE.is_file():
        raise NodeBootstrapError("node_profile_runtime_identity_invalid")
    _require_empty_or_missing_directory(NPM_CACHE_PATH)
    NPM_CACHE_PATH.mkdir(mode=0o700, parents=False, exist_ok=True)
    _run_npm(NPM_CI_ARGUMENTS, cwd=WORKSPACE_PATH)
    return lock


def validate_installed_tree(lockfile_sha256: str) -> NodeInstallReceipt:
    """Inspect npm's installed bytes without importing package code."""
    lock = validate_workspace_lock(lockfile_sha256)
    file_count, total_bytes = _scan_node_modules()
    installed_packages = _verify_locked_package_directories(lock)
    return NodeInstallReceipt(
        lockfile_sha256=lock.lockfile_sha256,
        package_count=installed_packages,
        file_count=file_count,
        total_bytes=total_bytes,
    )


def _verify_locked_package_directories(lock: NodeLock) -> int:
    installed: set[tuple[str, str]] = set()
    for package in lock.packages:
        package_directory = WORKSPACE_PATH.joinpath(*package.lock_path.split("/"))
        metadata = package_directory / "package.json"
        try:
            if package_directory.is_symlink() or not package_directory.is_dir():
                if package.optional:
                    continue
                raise NodeBootstrapError("node_profile_install_tree_mismatch")
            package_json = _read_regular_file(metadata, MAX_PACKAGE_JSON_BYTES)
        except NodeBootstrapError:
            if package.optional and not package_directory.exists():
                continue
            raise
        actual = _load_json_object(package_json, "node_profile_installed_package_invalid")
        if actual.get("name") != package.name or actual.get("version") != package.version:
            raise NodeBootstrapError("node_profile_install_tree_mismatch")
        scripts = actual.get("scripts", {})
        if not isinstance(scripts, dict) or _INSTALL_HOOKS.intersection(scripts):
            raise NodeBootstrapError("node_profile_lifecycle_hook_unsupported")
        installed.add((_canonical_node_name(package.name), package.version))
    if not installed:
        raise NodeBootstrapError("node_profile_install_tree_empty")
    return len(installed)


def _scan_node_modules() -> tuple[int, int]:
    try:
        metadata = NODE_MODULES_PATH.lstat()
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise NodeBootstrapError("node_profile_install_tree_invalid")
    except FileNotFoundError:
        raise NodeBootstrapError("node_profile_install_tree_missing") from None
    files = 0
    total_bytes = 0
    symlinks: list[tuple[Path, str]] = []
    stack: list[tuple[Path, int]] = [(NODE_MODULES_PATH, 0)]
    entries_seen = 0
    directories_seen = 1
    while stack:
        directory, depth = stack.pop()
        try:
            entries = os.scandir(directory)
        except OSError:
            raise NodeBootstrapError("node_profile_install_tree_unreadable") from None
        try:
            with entries:
                for entry in entries:
                    entries_seen += 1
                    if entries_seen > MAX_NPM_ENTRIES:
                        raise NodeBootstrapError("node_profile_install_tree_entry_limit")
                    path = Path(entry.path)
                    relative = path.relative_to(NODE_MODULES_PATH).as_posix()
                    components = _validate_relative_path(relative)
                    try:
                        info = entry.stat(follow_symlinks=False)
                    except OSError:
                        raise NodeBootstrapError("node_profile_install_tree_unreadable") from None
                    if stat.S_ISLNK(info.st_mode):
                        if components[-2:-1] != (".bin",) or not entry.is_symlink():
                            raise NodeBootstrapError("node_profile_install_link_unsupported")
                        symlinks.append((path, os.readlink(path)))
                        continue
                    if stat.S_ISDIR(info.st_mode):
                        if depth >= MAX_NPM_DEPTH:
                            raise NodeBootstrapError("node_profile_install_tree_depth_limit")
                        directories_seen += 1
                        if directories_seen > MAX_NPM_DIRECTORIES:
                            raise NodeBootstrapError("node_profile_install_tree_directory_limit")
                        stack.append((path, depth + 1))
                        continue
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        raise NodeBootstrapError("node_profile_install_entry_unsupported")
                    if info.st_size > MAX_NPM_FILE_BYTES:
                        raise NodeBootstrapError("node_profile_install_file_size_limit")
                    basename = components[-1].casefold()
                    if basename in _BROWSER_ASSET_BASENAMES:
                        raise NodeBootstrapError("node_profile_browser_dependency_unsupported")
                    if (
                        Path(basename).suffix in _NATIVE_SUFFIXES
                        or basename in _NATIVE_BUILD_BASENAMES
                        or _has_native_binary_magic(path)
                    ):
                        raise NodeBootstrapError("node_profile_native_dependency_unsupported")
                    files += 1
                    total_bytes += info.st_size
                    if files > MAX_NPM_FILES or total_bytes > MAX_NODE_MODULES_BYTES:
                        raise NodeBootstrapError("node_profile_install_tree_size_limit")
        except NodeBootstrapError:
            raise
        except OSError:
            raise NodeBootstrapError("node_profile_install_tree_unreadable") from None
    _validate_npm_bin_links(symlinks)
    return files, total_bytes


def _validate_npm_bin_links(symlinks: Sequence[tuple[Path, str]]) -> None:
    root = NODE_MODULES_PATH.resolve()
    for path, target in symlinks:
        target_path = Path(target)
        if target_path.is_absolute():
            raise NodeBootstrapError("node_profile_install_link_unsupported")
        try:
            resolved = (path.parent / target_path).resolve(strict=True)
            resolved.relative_to(root)
            info = resolved.lstat()
        except (OSError, ValueError):
            raise NodeBootstrapError("node_profile_install_link_unsupported") from None
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise NodeBootstrapError("node_profile_install_link_unsupported")


def _run_npm(arguments: Sequence[str], *, cwd: Path) -> None:
    command = (str(NODE_EXECUTABLE), str(NPM_CLI_PATH), *arguments)
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=_npm_environment(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=MAX_NPM_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise NodeBootstrapError("node_profile_npm_timeout") from None
    except OSError:
        raise NodeBootstrapError("node_profile_npm_unavailable") from None
    if result.returncode != 0:
        raise NodeBootstrapError("node_profile_npm_ci_failed")


def _run_version(
    command: Sequence[str],
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]],
) -> str:
    try:
        result = runner(
            command,
            cwd=CONTROL_DIRECTORY,
            env=_npm_environment(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="strict",
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired, UnicodeError):
        raise NodeBootstrapError("node_profile_runtime_identity_invalid") from None
    if result.returncode != 0 or len(result.stdout) > 128:
        raise NodeBootstrapError("node_profile_runtime_identity_invalid")
    return result.stdout.strip()


def _npm_environment() -> dict[str, str]:
    return {
        "CI": "true",
        "HOME": str(CONTROL_DIRECTORY / "home"),
        "LANG": "C.UTF-8",
        "NPM_CONFIG_AUDIT": "false",
        "NPM_CONFIG_CACHE": str(NPM_CACHE_PATH),
        "NPM_CONFIG_FUND": "false",
        "NPM_CONFIG_GLOBALCONFIG": "/dev/null",
        "NPM_CONFIG_UPDATE_NOTIFIER": "false",
        "NPM_CONFIG_USERCONFIG": "/dev/null",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
    }


def _load_json_object(content: bytes, error_code: str) -> dict[str, Any]:
    try:
        value = json.loads(content, object_pairs_hook=_unique_object_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, NodeBootstrapError, RecursionError):
        raise NodeBootstrapError(error_code) from None
    if not isinstance(value, dict):
        raise NodeBootstrapError(error_code)
    return cast(dict[str, Any], value)


def _unique_object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise NodeBootstrapError("node_profile_json_duplicate_key")
        result[key] = value
    return result


def _is_public_registry_url(value: str) -> bool:
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
        if (
            parsed.scheme != "https"
            or parsed.hostname != "registry.npmjs.org"
            or parsed.username is not None
            or parsed.password is not None
            or port not in {None, 443}
            or parsed.query
            or parsed.fragment
            or not parsed.path.startswith("/")
        ):
            return False
        decoded = urllib.parse.unquote(parsed.path, errors="strict")
    except (UnicodeDecodeError, ValueError):
        return False
    if "\\" in decoded or "\0" in decoded or "\r" in decoded or "\n" in decoded:
        return False
    if any(part in {".", ".."} for part in decoded.split("/")):
        return False
    return True


def _has_sha512_integrity(value: object) -> bool:
    if not isinstance(value, str) or len(value) > 256:
        return False
    match = _SRI_SHA512.fullmatch(value)
    if match is None:
        return False
    try:
        decoded = base64.b64decode(match.group(1), validate=True)
    except (ValueError, binascii.Error):
        return False
    return len(decoded) == 64


def _is_package_name(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) <= 214
        and _SAFE_PACKAGE_NAME.fullmatch(value) is not None
    )


def _canonical_node_name(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"[-_.]+", "-", value).casefold()


def _package_name_from_lock_path(lock_path: str) -> str:
    components = lock_path.split("/")
    if any(component in {"", ".", ".."} for component in components):
        raise NodeBootstrapError("node_profile_lock_package_path_invalid")
    index = 0
    while index < len(components):
        if components[index] != "node_modules" or index + 1 >= len(components):
            raise NodeBootstrapError("node_profile_lock_package_path_invalid")
        index += 1
        if components[index].startswith("@"):
            if index + 1 >= len(components):
                raise NodeBootstrapError("node_profile_lock_package_path_invalid")
            package_name = f"{components[index]}/{components[index + 1]}"
            index += 2
        else:
            package_name = components[index]
            index += 1
        if not _is_package_name(package_name):
            raise NodeBootstrapError("node_profile_lock_package_path_invalid")
        if index == len(components):
            return package_name
        if components[index] != "node_modules":
            raise NodeBootstrapError("node_profile_lock_package_path_invalid")
    raise NodeBootstrapError("node_profile_lock_package_path_invalid")


def _is_unsupported_specifier(specifier: str) -> bool:
    lowered = specifier.strip().casefold()
    return not lowered or lowered.startswith(_UNSUPPORTED_SPEC_PREFIXES) or "://" in lowered


def _validate_lock_package_path(path: str) -> str:
    if not path.startswith("node_modules/") or "\\" in path or "\0" in path:
        raise NodeBootstrapError("node_profile_lock_package_path_invalid")
    components = path.split("/")
    if any(not part or part in {".", ".."} for part in components):
        raise NodeBootstrapError("node_profile_lock_package_path_invalid")
    if len(path.encode("utf-8")) > 512:
        raise NodeBootstrapError("node_profile_lock_package_path_invalid")
    return "/".join(components).casefold()


def _validate_relative_path(path: str) -> tuple[str, ...]:
    if (
        not path
        or path.startswith(("/", "\\"))
        or "\\" in path
        or "\0" in path
        or unicodedata.normalize("NFC", path) != path
    ):
        raise NodeBootstrapError("node_profile_install_path_invalid")
    components = tuple(path.split("/"))
    if any(not item or item in {".", ".."} for item in components):
        raise NodeBootstrapError("node_profile_install_path_invalid")
    return components


def _validate_sha256(value: str, error_code: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise NodeBootstrapError(error_code)


def _workspace_relative_files() -> tuple[str, ...]:
    files: list[str] = []
    stack: list[tuple[Path, int]] = [(WORKSPACE_PATH, 0)]
    entries_seen = 0
    directories_seen = 1
    while stack:
        directory, depth = stack.pop()
        try:
            entries = os.scandir(directory)
        except OSError:
            raise NodeBootstrapError("node_profile_workspace_unreadable") from None
        try:
            with entries:
                for entry in entries:
                    entries_seen += 1
                    if entries_seen > MAX_SOURCE_ENTRIES:
                        raise NodeBootstrapError("node_profile_source_entry_limit")
                    path = Path(entry.path)
                    relative = path.relative_to(WORKSPACE_PATH).as_posix()
                    _validate_relative_path(relative)
                    if relative == "node_modules" and entry.is_dir(follow_symlinks=False):
                        continue
                    try:
                        info = entry.stat(follow_symlinks=False)
                    except OSError:
                        raise NodeBootstrapError("node_profile_workspace_unreadable") from None
                    if stat.S_ISDIR(info.st_mode):
                        if depth >= MAX_SOURCE_DEPTH:
                            raise NodeBootstrapError("node_profile_source_depth_limit")
                        directories_seen += 1
                        if directories_seen > MAX_SOURCE_ENTRIES:
                            raise NodeBootstrapError("node_profile_source_entry_limit")
                        stack.append((path, depth + 1))
                    elif stat.S_ISREG(info.st_mode):
                        files.append(relative)
                    elif stat.S_ISLNK(info.st_mode):
                        raise NodeBootstrapError("node_profile_source_link_unsupported")
                    else:
                        raise NodeBootstrapError("node_profile_source_entry_unsupported")
                    if len(files) > 5_000:
                        raise NodeBootstrapError("node_profile_source_file_limit")
        except NodeBootstrapError:
            raise
        except OSError:
            raise NodeBootstrapError("node_profile_workspace_unreadable") from None
    return tuple(files)


def _write_source_files(stage: Path, files: Iterable[Any]) -> None:
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        root_fd = os.open(stage, directory_flags)
    except OSError:
        raise NodeBootstrapError("node_profile_workspace_create_failed") from None
    try:
        for source in files:
            components = _validate_relative_path(source.path)
            if (
                not isinstance(source.content, bytes)
                or type(source.mode) is not int
                or source.mode not in {0o644, 0o755}
            ):
                raise NodeBootstrapError("node_profile_source_entry_invalid")
            parent_fd = os.dup(root_fd)
            try:
                for component in components[:-1]:
                    try:
                        os.mkdir(component, mode=0o755, dir_fd=parent_fd)
                    except FileExistsError:
                        pass
                    child_fd = os.open(component, directory_flags, dir_fd=parent_fd)
                    os.close(parent_fd)
                    parent_fd = child_fd
                file_fd = os.open(
                    components[-1],
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    source.mode,
                    dir_fd=parent_fd,
                )
                try:
                    view = memoryview(source.content)
                    while view:
                        written = os.write(file_fd, view)
                        if written <= 0:
                            raise OSError("short write")
                        view = view[written:]
                    os.fchmod(file_fd, source.mode)
                finally:
                    os.close(file_fd)
            finally:
                os.close(parent_fd)
    except NodeBootstrapError:
        raise
    except OSError:
        raise NodeBootstrapError("node_profile_workspace_extract_failed") from None
    finally:
        os.close(root_fd)


def _require_empty_workspace(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        if not path.parent.is_dir():
            raise NodeBootstrapError("node_profile_workspace_parent_invalid") from None
        return
    except OSError:
        raise NodeBootstrapError("node_profile_workspace_unavailable") from None
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise NodeBootstrapError("node_profile_workspace_not_directory")
    try:
        with os.scandir(path) as entries:
            if next(entries, None) is not None:
                raise NodeBootstrapError("node_profile_workspace_not_empty")
    except NodeBootstrapError:
        raise
    except OSError:
        raise NodeBootstrapError("node_profile_workspace_unavailable") from None


def _require_empty_or_missing_directory(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    except OSError:
        raise NodeBootstrapError("node_profile_directory_unavailable") from None
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise NodeBootstrapError("node_profile_directory_invalid")
    try:
        with os.scandir(path) as entries:
            if next(entries, None) is not None:
                raise NodeBootstrapError("node_profile_directory_not_empty")
    except NodeBootstrapError:
        raise
    except OSError:
        raise NodeBootstrapError("node_profile_directory_unavailable") from None


def _read_regular_file(path: Path, max_bytes: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise NodeBootstrapError("node_profile_file_unavailable") from None
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > max_bytes:
            raise NodeBootstrapError("node_profile_file_invalid")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, max_bytes + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise NodeBootstrapError("node_profile_file_size_limit")
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _has_native_binary_magic(path: Path) -> bool:
    descriptor_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, descriptor_flags)
    except OSError:
        raise NodeBootstrapError("node_profile_install_tree_unreadable") from None
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise NodeBootstrapError("node_profile_install_entry_unsupported")
        prefix = os.read(descriptor, 4)
    except OSError:
        raise NodeBootstrapError("node_profile_install_tree_unreadable") from None
    finally:
        os.close(descriptor)
    return any(prefix.startswith(magic) for magic in _NATIVE_BINARY_MAGICS)


def _load_trusted_archive_verifier() -> Callable[..., Any]:
    try:
        file_info = TRUSTED_ARCHIVER_PATH.lstat()
        if stat.S_ISLNK(file_info.st_mode) or not stat.S_ISREG(file_info.st_mode):
            raise NodeBootstrapError("node_profile_archive_verifier_invalid")
        spec = importlib.util.spec_from_file_location(
            "continuum_image_trusted_archives",
            TRUSTED_ARCHIVER_PATH,
        )
        if spec is None or spec.loader is None:
            raise NodeBootstrapError("node_profile_archive_verifier_unavailable")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        verifier = getattr(module, "verify_archive", None)
        if not callable(verifier):
            raise NodeBootstrapError("node_profile_archive_verifier_invalid")
        return cast(Callable[..., Any], verifier)
    except NodeBootstrapError:
        raise
    except Exception:
        raise NodeBootstrapError("node_profile_archive_verifier_unavailable") from None


def _registry_opener() -> urllib.request.OpenerDirector:
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(
            self,
            request: urllib.request.Request,
            file_pointer: Any,
            code: int,
            message: str,
            headers: http.client.HTTPMessage,
            new_url: str,
        ) -> urllib.request.Request | None:
            return None

    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        urllib.request.HTTPSHandler(),
        NoRedirect(),
    )


def _emit_json(payload: Mapping[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="continuum-node-bootstrap")
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("verify-runtime")

    extract = actions.add_parser("extract")
    extract.add_argument("--archive-sha256", required=True)
    extract.add_argument("--tree-sha256", required=True)
    extract.add_argument("--lockfile-sha256", required=True)

    validate = actions.add_parser("validate-lock")
    validate.add_argument("--lockfile-sha256", required=True)

    probe = actions.add_parser("probe")
    probe.add_argument("--expected", choices=("allowed", "denied"), required=True)

    npm_install = actions.add_parser("npm-ci")
    npm_install.add_argument("--lockfile-sha256", required=True)

    validate_install = actions.add_parser("validate-install")
    validate_install.add_argument("--lockfile-sha256", required=True)

    selected = parser.parse_args(arguments)
    try:
        runtime = verify_runtime_identity()
        if selected.action == "verify-runtime":
            _emit_json(runtime)
        elif selected.action == "extract":
            archive_content = _read_regular_file(ARCHIVE_PATH, MAX_ARCHIVE_BYTES)
            extraction_receipt = extract_verified_archive(
                archive_content,
                expected_archive_sha256=selected.archive_sha256,
                expected_tree_sha256=selected.tree_sha256,
                expected_lockfile_sha256=selected.lockfile_sha256,
                destination=WORKSPACE_PATH,
            )
            _emit_json(
                {
                    "archive_sha256": extraction_receipt.archive_sha256,
                    "file_count": extraction_receipt.file_count,
                    "lockfile_sha256": extraction_receipt.lockfile_sha256,
                    "source_bytes": extraction_receipt.source_bytes,
                    "tree_sha256": extraction_receipt.tree_sha256,
                }
            )
        elif selected.action == "validate-lock":
            lock = validate_workspace_lock(selected.lockfile_sha256)
            _emit_json(
                {"lockfile_sha256": lock.lockfile_sha256, "package_count": lock.package_count}
            )
        elif selected.action == "probe":
            probe_receipt = probe_network(selected.expected)
            _emit_json(
                {
                    "expected": probe_receipt.expected,
                    "observed_allowed": probe_receipt.observed_allowed,
                }
            )
        elif selected.action == "npm-ci":
            lock = npm_ci(selected.lockfile_sha256)
            _emit_json(
                {"lockfile_sha256": lock.lockfile_sha256, "package_count": lock.package_count}
            )
        elif selected.action == "validate-install":
            install_receipt = validate_installed_tree(selected.lockfile_sha256)
            _emit_json(
                {
                    "file_count": install_receipt.file_count,
                    "lockfile_sha256": install_receipt.lockfile_sha256,
                    "package_count": install_receipt.package_count,
                    "total_bytes": install_receipt.total_bytes,
                }
            )
        else:
            raise NodeBootstrapError("node_profile_action_unknown")
    except NodeBootstrapError as error:
        sys.stderr.write(error.code + "\n")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
