"""Fixed Python profile commands and paths shared with the sandbox driver."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

PYTHON_PROFILE_ID = "python-pytest"
PYTHON_PROFILE_VERSION = "1"
PYTHON_VERSION = "3.13.12"
PYTHON_EXECUTABLE = "/usr/local/bin/python3.13"

CONTROL_DIRECTORY = "/opt/continuum"
BOOTSTRAP_PATH = f"{CONTROL_DIRECTORY}/profile_bootstrap.py"
TRUSTED_ARCHIVER_PATH = f"{CONTROL_DIRECTORY}/trusted/archives.py"
ARCHIVE_INPUT_PATH = f"{CONTROL_DIRECTORY}/input/source.tar.gz"
WHEELHOUSE_PATH = f"{CONTROL_DIRECTORY}/wheels"
DEPENDENCY_TARGET_PATH = f"{CONTROL_DIRECTORY}/deps"
WORKSPACE_PATH = "/vercel/sandbox/workspace"
LOCKFILE_PATH = f"{WORKSPACE_PATH}/requirements.lock"
PYPI_INDEX_URL = "https://pypi.org/simple"


def bootstrap_argv(action: str, *arguments: str) -> tuple[str, ...]:
    """Build one fixed trusted helper invocation for `python -I -S`."""
    if action not in {
        "verify-runtime",
        "extract",
        "validate-lock",
        "probe",
        "download-verify",
        "install",
    }:
        raise ValueError("python_profile_bootstrap_action_invalid")
    if any("\0" in argument for argument in arguments):
        raise ValueError("python_profile_bootstrap_argument_invalid")
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


def extract_argv(*, archive_sha256: str, tree_sha256: str, lockfile_sha256: str) -> tuple[str, ...]:
    return bootstrap_argv(
        "extract",
        "--archive-sha256",
        archive_sha256,
        "--tree-sha256",
        tree_sha256,
        "--lockfile-sha256",
        lockfile_sha256,
    )


def validate_lock_argv(*, lockfile_sha256: str) -> tuple[str, ...]:
    return bootstrap_argv("validate-lock", "--lockfile-sha256", lockfile_sha256)


def network_probe_argv(*, expected: str) -> tuple[str, ...]:
    if expected not in {"allowed", "denied"}:
        raise ValueError("python_profile_network_expectation_invalid")
    return bootstrap_argv("probe", "--expected", expected)


def download_verify_argv(*, lockfile_sha256: str) -> tuple[str, ...]:
    return bootstrap_argv("download-verify", "--lockfile-sha256", lockfile_sha256)


def install_argv(*, lockfile_sha256: str) -> tuple[str, ...]:
    return bootstrap_argv("install", "--lockfile-sha256", lockfile_sha256)


def pip_download_argv() -> tuple[str, ...]:
    """Return the fixed base-interpreter pip command for the isolated wheelhouse."""
    return (
        PYTHON_EXECUTABLE,
        "-I",
        "-m",
        "pip",
        "--isolated",
        "download",
        "--require-hashes",
        "--no-deps",
        "--only-binary=:all:",
        "--no-cache-dir",
        "--disable-pip-version-check",
        "--index-url",
        PYPI_INDEX_URL,
        "--dest",
        WHEELHOUSE_PATH,
        "-r",
        LOCKFILE_PATH,
    )


def pip_install_argv() -> tuple[str, ...]:
    """Install only previously verified local wheels into a separate target."""
    return (
        PYTHON_EXECUTABLE,
        "-I",
        "-m",
        "pip",
        "--isolated",
        "install",
        "--require-hashes",
        "--no-deps",
        "--only-binary=:all:",
        "--no-cache-dir",
        "--disable-pip-version-check",
        "--no-index",
        "--find-links",
        WHEELHOUSE_PATH,
        "--target",
        DEPENDENCY_TARGET_PATH,
        "-r",
        LOCKFILE_PATH,
    )


def pytest_argv() -> tuple[str, ...]:
    """Run the verified pytest without prepending the current directory."""
    return (PYTHON_EXECUTABLE, "-P", "-S", "-m", "pytest", "-q")


def pytest_environment() -> Mapping[str, str]:
    """Explicit import roots with Python site startup disabled by `-S`."""
    return {"PYTHONPATH": f"{DEPENDENCY_TARGET_PATH}:{WORKSPACE_PATH}/src:{WORKSPACE_PATH}"}


def command_sequence(values: Sequence[str]) -> tuple[str, ...]:
    """Normalize a service-supplied argv sequence without shell interpretation."""
    selected = tuple(values)
    if not selected or any(not value or "\0" in value for value in selected):
        raise ValueError("python_profile_command_invalid")
    return selected
