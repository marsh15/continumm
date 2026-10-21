"""Fixed Node/npm profile paths and argv shared with the sandbox driver."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

NODE_PROFILE_ID = "node-npm"
NODE_PROFILE_VERSION = "1"
NODE_VERSION = "24.21.0"
NPM_VERSION = "11.19.0"
PYTHON_EXECUTABLE = "/usr/local/bin/python3.13"
NODE_EXECUTABLE = "/usr/local/bin/node"
NPM_EXECUTABLE = "/usr/local/bin/npm"
NPM_CLI_PATH = "/usr/local/lib/node_modules/npm/bin/npm-cli.js"

CONTROL_DIRECTORY = "/opt/continuum"
BOOTSTRAP_PATH = f"{CONTROL_DIRECTORY}/node_bootstrap.py"
RECIPE_PATH = f"{CONTROL_DIRECTORY}/node_bootstrap_recipe.py"
TRUSTED_ARCHIVER_PATH = f"{CONTROL_DIRECTORY}/trusted/archives.py"
ARCHIVE_INPUT_PATH = f"{CONTROL_DIRECTORY}/input/source.tar.gz"
NPM_CACHE_PATH = f"{CONTROL_DIRECTORY}/npm-cache"
WORKSPACE_PATH = "/vercel/sandbox/workspace"
PACKAGE_JSON_PATH = f"{WORKSPACE_PATH}/package.json"
PACKAGE_LOCK_PATH = f"{WORKSPACE_PATH}/package-lock.json"
NPM_REGISTRY_URL = "https://registry.npmjs.org/"
NPM_PROBE_URL = "https://registry.npmjs.org/-/ping"


def bootstrap_argv(action: str, *arguments: str) -> tuple[str, ...]:
    """Build a fixed trusted helper invocation using isolated base Python."""
    if action not in {
        "verify-runtime",
        "extract",
        "validate-lock",
        "probe",
        "npm-ci",
        "validate-install",
    }:
        raise ValueError("node_profile_bootstrap_action_invalid")
    if any("\0" in argument for argument in arguments):
        raise ValueError("node_profile_bootstrap_argument_invalid")
    return (
        PYTHON_EXECUTABLE,
        "-I",
        "-S",
        BOOTSTRAP_PATH,
        action,
        *arguments,
    )


def verify_runtime_argv() -> tuple[str, ...]:
    return bootstrap_argv("verify-runtime")


def extract_argv(
    archive_sha256: str,
    tree_sha256: str,
    lockfile_sha256: str,
) -> tuple[str, ...]:
    return bootstrap_argv(
        "extract",
        "--archive-sha256",
        archive_sha256,
        "--tree-sha256",
        tree_sha256,
        "--lockfile-sha256",
        lockfile_sha256,
    )


def validate_lock_argv(lockfile_sha256: str) -> tuple[str, ...]:
    return bootstrap_argv("validate-lock", "--lockfile-sha256", lockfile_sha256)


def network_probe_argv(expected: str) -> tuple[str, ...]:
    if expected not in {"allowed", "denied"}:
        raise ValueError("node_profile_network_expectation_invalid")
    return bootstrap_argv("probe", "--expected", expected)


def npm_ci_bootstrap_argv(lockfile_sha256: str) -> tuple[str, ...]:
    return bootstrap_argv("npm-ci", "--lockfile-sha256", lockfile_sha256)


def validate_install_argv(lockfile_sha256: str) -> tuple[str, ...]:
    return bootstrap_argv("validate-install", "--lockfile-sha256", lockfile_sha256)


def npm_ci_argv() -> tuple[str, ...]:
    """Return the exact script-disabled, registry-only clean install command."""
    return (
        NODE_EXECUTABLE,
        NPM_CLI_PATH,
        "ci",
        "--ignore-scripts",
        "--no-audit",
        "--no-fund",
        "--engine-strict",
        f"--registry={NPM_REGISTRY_URL}",
        "--userconfig=/opt/continuum/npm-user-empty.npmrc",
        "--globalconfig=/opt/continuum/npm-global-empty.npmrc",
        f"--cache={NPM_CACHE_PATH}",
        "--fetch-retries=0",
        "--fetch-timeout=15000",
        "--maxsockets=4",
    )


def npm_ci_environment() -> Mapping[str, str]:
    """Environment for trusted dependency acquisition with no ambient config."""
    return {
        "CI": "true",
        "HOME": f"{CONTROL_DIRECTORY}/home",
        "LANG": "C.UTF-8",
        "NPM_CONFIG_AUDIT": "false",
        "NPM_CONFIG_CACHE": NPM_CACHE_PATH,
        "NPM_CONFIG_FUND": "false",
        "NPM_CONFIG_GLOBALCONFIG": "/opt/continuum/npm-global-empty.npmrc",
        "NPM_CONFIG_UPDATE_NOTIFIER": "false",
        "NPM_CONFIG_USERCONFIG": "/opt/continuum/npm-user-empty.npmrc",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
    }


def npm_test_argv() -> tuple[str, ...]:
    """Run the explicit test script without npm's pretest/posttest hooks."""
    return (NODE_EXECUTABLE, NPM_CLI_PATH, "--ignore-scripts", "test")


def npm_test_environment() -> Mapping[str, str]:
    """Supply only non-secret, fixed npm/node runtime environment values."""
    return {
        "CI": "true",
        "HOME": f"{CONTROL_DIRECTORY}/home",
        "LANG": "C.UTF-8",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "NPM_CONFIG_GLOBALCONFIG": "/opt/continuum/npm-global-empty.npmrc",
        "NPM_CONFIG_USERCONFIG": "/opt/continuum/npm-user-empty.npmrc",
    }


def command_sequence(values: Sequence[str]) -> tuple[str, ...]:
    selected = tuple(values)
    if not selected or any(not value or "\0" in value for value in selected):
        raise ValueError("node_profile_command_invalid")
    return selected
