"""Trusted authorization policy for the fixed source-inspection tools."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from continuum_api.tools.contracts import (
    MAX_SOURCE_FILE_BYTES,
    ApplyPatchArguments,
    DiffArguments,
    ListFilesArguments,
    ReadFileArguments,
    SearchTextArguments,
    ToolCall,
    ToolCapability,
    ToolContractError,
    ToolStage,
    normalize_source_path,
    parse_tool_call,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SUPPORTED_PROFILES = frozenset({("python-pytest", "1"), ("node-npm", "1")})
_SOURCE_FILE_LIMIT = 5_000
_SOURCE_BYTES_LIMIT = 50 * 1024 * 1024

_NON_SOURCE_COMPONENTS = frozenset(
    {
        ".git",
        ".hg",
        ".svn",
        "node_modules",
        ".venv",
        "venv",
        ".tox",
        ".npm",
        ".yarn",
        ".ssh",
        ".aws",
        ".kube",
        ".docker",
        ".azure",
    }
)
_SECRET_BASENAMES = frozenset(
    {
        ".env",
        ".netrc",
        ".npmrc",
        ".pypirc",
        ".git-credentials",
        ".dockercfg",
        ".boto",
        ".s3cfg",
        "pip.conf",
        "credentials",
        "credentials.json",
        "secrets",
        "secrets.json",
        "id_rsa",
        "id_ed25519",
        "authorized_keys",
        "application_default_credentials.json",
        "service_account.json",
    }
)
_SECRET_SUFFIXES = (".pem", ".key", ".p12", ".pfx", ".keystore")
_PROTECTED_MANIFESTS = frozenset(
    {
        "package.json",
        "package-lock.json",
        "npm-shrinkwrap.json",
        "requirements.lock",
        "requirements.txt",
        "pyproject.toml",
        "uv.lock",
        "poetry.lock",
        "pdm.lock",
        "pipfile",
        "pipfile.lock",
        "yarn.lock",
        "pnpm-lock.yaml",
        "cargo.toml",
        "cargo.lock",
        "go.mod",
        "go.sum",
        "gemfile",
        "gemfile.lock",
        "composer.json",
        "composer.lock",
        "environment.yml",
        "environment.yaml",
        ".python-version",
        ".node-version",
        ".nvmrc",
        ".npmrc",
        ".gitattributes",
        ".lfsconfig",
    }
)
_PROTECTED_TEST_AND_BUILD_CONFIGS = frozenset(
    {
        "conftest.py",
        "makefile",
        "gnumakefile",
        "justfile",
        ".justfile",
        "taskfile.yml",
        "taskfile.yaml",
        "gruntfile.js",
        "gruntfile.cjs",
        "gruntfile.mjs",
        "gulpfile.js",
        "gulpfile.cjs",
        "gulpfile.mjs",
        "pytest.ini",
        ".pytest.ini",
        "tox.ini",
        "setup.cfg",
        "setup.py",
    }
)
_PROTECTED_CI_BASENAMES = frozenset(
    {
        ".gitlab-ci.yml",
        ".travis.yml",
        "azure-pipelines.yml",
        "bitbucket-pipelines.yml",
        "jenkinsfile",
        ".drone.yml",
    }
)
_KNOWN_TEST_FILE = re.compile(
    r"^(?:test_[^/]+\.py|[^/]+_test\.py|test(?:[-_][^/]+)?\.[cm]?[jt]sx?|"
    r"[^/]+[-_]test\.[cm]?[jt]sx?|[^/]+\.(?:test|spec)\.[cm]?[jt]sx?)$",
    re.IGNORECASE,
)
_CAPABILITY_BY_NAME = {
    "list_files": ToolCapability.LIST_FILES,
    "read_file": ToolCapability.READ_FILE,
    "search_text": ToolCapability.SEARCH_TEXT,
    "apply_patch": ToolCapability.APPLY_PATCH,
    "diff": ToolCapability.DIFF,
}


@dataclass(frozen=True, slots=True)
class ToolPolicyContext:
    """Server-derived authorization and baseline data; never model arguments."""

    capabilities: frozenset[ToolCapability]
    profile_id: str
    profile_version: str
    actor_can_write_repository: bool
    app_can_access_repository: bool
    stage: ToolStage
    run_budget_available: bool
    baseline_tree_sha256: str
    baseline_source_paths: frozenset[str]
    baseline_test_paths: frozenset[str]
    baseline_paths_verified: bool
    protected_paths: frozenset[str]

    def __post_init__(self) -> None:
        if not isinstance(self.capabilities, frozenset) or any(
            not isinstance(item, ToolCapability) for item in self.capabilities
        ):
            raise ValueError("tool_policy_capabilities_invalid")
        if not isinstance(self.profile_id, str) or not isinstance(self.profile_version, str):
            raise ValueError("tool_policy_profile_invalid")
        if type(self.actor_can_write_repository) is not bool:
            raise ValueError("tool_policy_account_permission_invalid")
        if type(self.app_can_access_repository) is not bool:
            raise ValueError("tool_policy_app_permission_invalid")
        if not isinstance(self.stage, ToolStage):
            raise ValueError("tool_policy_stage_invalid")
        if type(self.run_budget_available) is not bool:
            raise ValueError("tool_policy_budget_invalid")
        if type(self.baseline_paths_verified) is not bool:
            raise ValueError("tool_policy_baseline_paths_invalid")
        if (
            not isinstance(self.baseline_tree_sha256, str)
            or _SHA256.fullmatch(self.baseline_tree_sha256) is None
        ):
            raise ValueError("tool_policy_baseline_tree_invalid")
        for field_name, paths in (
            ("baseline_source_paths", self.baseline_source_paths),
            ("baseline_test_paths", self.baseline_test_paths),
            ("protected_paths", self.protected_paths),
        ):
            if not isinstance(paths, frozenset) or any(not isinstance(path, str) for path in paths):
                raise ValueError(f"tool_policy_{field_name}_invalid")
            if len(paths) > _SOURCE_FILE_LIMIT:
                raise ValueError(f"tool_policy_{field_name}_limit")
            normalized = {normalize_source_path(path) for path in paths}
            if normalized != set(paths):
                raise ValueError(f"tool_policy_{field_name}_invalid")
            if len({path.casefold() for path in normalized}) != len(normalized):
                raise ValueError(f"tool_policy_{field_name}_invalid")
        if not self.baseline_test_paths <= self.baseline_source_paths:
            raise ValueError("tool_policy_baseline_tests_invalid")


@dataclass(frozen=True, slots=True)
class ToolPolicyDecision:
    allowed: bool
    code: str
    readable_paths: frozenset[str] = frozenset()
    write_paths: frozenset[str] = frozenset()
    baseline_tree_sha256: str | None = None


def is_sensitive_source_path(path: str) -> bool:
    """Identify source paths whose contents must never enter model context."""
    normalized = normalize_source_path(path)
    parts = tuple(component.casefold() for component in normalized.split("/"))
    basename = parts[-1]
    return (
        any(part in _NON_SOURCE_COMPONENTS for part in parts)
        or any(
            parts[index : index + 2] == (".config", "gcloud")
            for index in range(max(0, len(parts) - 1))
        )
        or basename in _SECRET_BASENAMES
        or basename.startswith(".env.")
        or basename.startswith("credentials.")
        or basename.startswith("secrets.")
        or basename.endswith(_SECRET_SUFFIXES)
    )


def is_protected_source_path(path: str, context: ToolPolicyContext) -> bool:
    """Protect manifests, runtime/CI configuration, and the immutable test baseline."""
    normalized = normalize_source_path(path)
    parts = tuple(component.casefold() for component in normalized.split("/"))
    basename = parts[-1]
    if normalized in context.baseline_test_paths:
        return True
    if (
        basename in _PROTECTED_MANIFESTS
        or _is_dependency_manifest_basename(basename)
        or basename in _PROTECTED_CI_BASENAMES
        or basename in _PROTECTED_TEST_AND_BUILD_CONFIGS
        or basename.startswith(
            (
                "jest.config.",
                "vitest.config.",
                "vite.config.",
                ".mocharc.",
                "ava.config.",
                "tap.config.",
                "karma.conf.",
                ".nycrc",
                "babel.config.",
                ".babelrc",
                "webpack.config.",
                "rollup.config.",
            )
        )
        or (basename.startswith("tsconfig") and basename.endswith(".json"))
        or (basename.startswith("eslint.config.") and normalized in context.baseline_test_paths)
        or basename.endswith((".sh", ".bash", ".ps1", ".cmd", ".bat"))
    ):
        return True
    if parts[0] == ".github":
        return True
    if parts[0] in {".circleci", ".buildkite", ".azure-pipelines"}:
        return True
    if any(
        normalized.casefold() == protected.casefold()
        or normalized.casefold().startswith(protected.casefold() + "/")
        for protected in context.protected_paths
    ):
        return True
    return False


def _is_dependency_manifest_basename(basename: str) -> bool:
    return (
        (basename.startswith("requirements") and basename.endswith((".txt", ".lock")))
        or (basename.startswith("constraints") and basename.endswith(".txt"))
        or basename.startswith("conda-lock.")
    )


def is_conventional_test_path(path: str) -> bool:
    """Recognize common test files so baseline protection cannot be omitted."""
    normalized = normalize_source_path(path)
    parts = tuple(component.casefold() for component in normalized.split("/"))
    basename = parts[-1]
    if any(part in {"test", "tests", "__tests__"} for part in parts[:-1]):
        return True
    return _KNOWN_TEST_FILE.fullmatch(basename) is not None


def authorize_tool_call(
    call: ToolCall | object,
    context: ToolPolicyContext,
    source_paths: Iterable[str],
    source_file_sizes: Mapping[str, int],
) -> ToolPolicyDecision:
    """Intersect tool schema, capability, profile, access, stage, budget and paths."""
    try:
        parsed_call = parse_tool_call(call)
    except ToolContractError as error:
        return _deny(error.code)
    context_gate = check_tool_context_gate(parsed_call, context)
    if not context_gate.allowed:
        return context_gate

    tree = _validated_source_tree(source_paths, source_file_sizes)
    if tree is None:
        return _deny("tool_source_tree_invalid")
    normalized_paths, sizes = tree
    discovered_tests = {
        path for path in context.baseline_source_paths if is_conventional_test_path(path)
    }
    if not discovered_tests <= context.baseline_test_paths:
        return _deny("tool_baseline_paths_incomplete")
    readable_paths = frozenset(
        path for path in normalized_paths if not is_sensitive_source_path(path)
    )

    if isinstance(parsed_call, ListFilesArguments):
        return _authorize_read_path(parsed_call.path, readable_paths, context)
    if isinstance(parsed_call, ReadFileArguments):
        if parsed_call.path not in readable_paths:
            return _deny("tool_path_unavailable")
        if sizes.get(parsed_call.path, 0) > MAX_SOURCE_FILE_BYTES:
            return _deny("tool_file_size_limit")
        return _allow(readable_paths, baseline_tree_sha256=context.baseline_tree_sha256)
    if isinstance(parsed_call, SearchTextArguments):
        if parsed_call.path_prefix is not None:
            return _authorize_read_path(parsed_call.path_prefix, readable_paths, context)
        return _allow(readable_paths, baseline_tree_sha256=context.baseline_tree_sha256)
    if isinstance(parsed_call, DiffArguments):
        if parsed_call.path is not None:
            return _authorize_read_path(parsed_call.path, readable_paths, context)
        return _allow(readable_paths, baseline_tree_sha256=context.baseline_tree_sha256)
    if isinstance(parsed_call, ApplyPatchArguments):
        write_paths = frozenset(edit.path for edit in parsed_call.edits)
        for path in write_paths:
            if is_sensitive_source_path(path):
                return _deny("tool_sensitive_path_denied")
            if is_protected_source_path(path, context):
                return _deny("tool_protected_path_denied")
            if _path_conflicts_with_source(path, normalized_paths):
                return _deny("tool_path_conflict")
        return _allow(
            readable_paths,
            write_paths=write_paths,
            baseline_tree_sha256=context.baseline_tree_sha256,
        )
    return _deny("tool_name_unsupported")


def check_tool_context_gate(
    call: ToolCall | object, context: ToolPolicyContext
) -> ToolPolicyDecision:
    """Cheap preflight gate safe to call before decompressing any source archive.

    An allow here is not path authorization and is never sufficient to execute a
    tool. The caller must still verify the pinned source snapshot and obtain the
    full readable/write path decision from `authorize_tool_call`.
    """
    try:
        parsed_call = parse_tool_call(call)
    except ToolContractError as error:
        return _deny(error.code)
    return _context_gate_for_parsed_call(parsed_call, context)


def _context_gate_for_parsed_call(call: ToolCall, context: ToolPolicyContext) -> ToolPolicyDecision:
    if (context.profile_id, context.profile_version) not in _SUPPORTED_PROFILES:
        return _deny("tool_profile_unsupported")
    capability = _CAPABILITY_BY_NAME[call.name]
    if capability not in context.capabilities:
        return _deny("tool_capability_denied")
    if context.stage is not ToolStage.AGENT_WORK:
        return _deny("tool_stage_denied")
    if not context.actor_can_write_repository or not context.app_can_access_repository:
        return _deny("tool_repository_access_denied")
    if not context.run_budget_available:
        return _deny("tool_run_budget_exhausted")
    if not context.baseline_paths_verified:
        return _deny("tool_baseline_paths_unverified")
    if not context.baseline_test_paths:
        return _deny("tool_baseline_paths_incomplete")
    return ToolPolicyDecision(
        allowed=True,
        code="tool_context_allowed",
        baseline_tree_sha256=context.baseline_tree_sha256,
    )


def _authorize_read_path(
    path: str,
    readable_paths: frozenset[str],
    context: ToolPolicyContext,
) -> ToolPolicyDecision:
    if path != "." and is_sensitive_source_path(path):
        return _deny("tool_sensitive_path_denied")
    if path != "." and not _path_is_present_or_directory(path, readable_paths):
        return _deny("tool_path_unavailable")
    return _allow(readable_paths, baseline_tree_sha256=context.baseline_tree_sha256)


def _validated_source_tree(
    source_paths: Iterable[str], source_file_sizes: Mapping[str, int]
) -> tuple[frozenset[str], dict[str, int]] | None:
    try:
        paths = tuple(normalize_source_path(path) for path in source_paths)
    except (ToolContractError, TypeError):
        return None
    if len(paths) > _SOURCE_FILE_LIMIT or len(set(paths)) != len(paths):
        return None
    if len({path.casefold() for path in paths}) != len(paths):
        return None
    try:
        sizes = dict(source_file_sizes)
    except (TypeError, ValueError):
        return None
    if set(sizes) != set(paths) or any(
        type(size) is not int or size < 0 or size > _SOURCE_BYTES_LIMIT for size in sizes.values()
    ):
        return None
    if sum(sizes.values()) > _SOURCE_BYTES_LIMIT:
        return None
    return frozenset(paths), sizes


def _path_is_present_or_directory(path: str, paths: frozenset[str]) -> bool:
    return any(source_path == path or source_path.startswith(path + "/") for source_path in paths)


def _path_conflicts_with_source(path: str, paths: frozenset[str]) -> bool:
    folded = path.casefold()
    for source_path in paths:
        source_folded = source_path.casefold()
        if source_folded == folded and source_path != path:
            return True
        if source_path != path and (
            source_path.startswith(path + "/") or source_folded.startswith(folded + "/")
        ):
            return True
        if path.startswith(source_path + "/") or folded.startswith(source_folded + "/"):
            return True
    return False


def _allow(
    readable_paths: frozenset[str],
    *,
    write_paths: frozenset[str] = frozenset(),
    baseline_tree_sha256: str,
) -> ToolPolicyDecision:
    return ToolPolicyDecision(
        allowed=True,
        code="tool_allowed",
        readable_paths=readable_paths,
        write_paths=write_paths,
        baseline_tree_sha256=baseline_tree_sha256,
    )


def _deny(code: str) -> ToolPolicyDecision:
    return ToolPolicyDecision(allowed=False, code=code)
