"""Fail-closed profile and repository preflight behavior."""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from pydantic import SecretStr

from continuum_api.repositories.preflight import (
    NODE_NPM_PROFILE,
    PYTHON_PYTEST_PROFILE,
    EligibilityReceipt,
    GitHubRepositoryAdapter,
    PreflightReservation,
    RepositoryAccessProof,
    RepositoryPreflightError,
    RepositoryPreflightService,
    _active_python_requirements,
    _parse_node_manifest,
    _parse_python_lock,
    _require_python_target_compatibility,
    _selected_manifest_paths,
    resolve_build_profile,
    validate_commit_sha,
)
from continuum_api.runtime.persistence import Principal


def test_profiles_are_explicit_and_versioned() -> None:
    assert resolve_build_profile("python-pytest", "1") == PYTHON_PYTEST_PROFILE
    assert resolve_build_profile("node-npm", "1") == NODE_NPM_PROFILE
    with pytest.raises(RepositoryPreflightError, match="repository_profile_unsupported"):
        resolve_build_profile("python-pytest", "2")
    with pytest.raises(RepositoryPreflightError, match="repository_profile_unsupported"):
        resolve_build_profile("custom-shell", "1")


def test_commit_sha_requires_full_lowercase_object_id() -> None:
    assert validate_commit_sha("a" * 40) == "a" * 40
    assert validate_commit_sha("a" * 64) == "a" * 64
    for invalid in ("", "a" * 39, "a" * 41, "A" * 40, "a" * 40 + "../x"):
        with pytest.raises(RepositoryPreflightError, match="repository_commit_invalid"):
            validate_commit_sha(invalid)


def test_python_lock_requires_pinned_hashed_pytest_and_public_metadata_shape() -> None:
    hash_value = "b" * 64
    lock = (
        "# continuum requirements.lock v1\n"
        f"pytest==8.3.5 --hash=sha256:{hash_value}\n"
        f"typing-extensions==4.12.2 --hash=sha256:{'c' * 64}\n"
    ).encode()

    packages = _parse_python_lock(lock)

    assert [(item.name, item.version) for item in packages] == [
        ("pytest", "8.3.5"),
        ("typing-extensions", "4.12.2"),
    ]
    assert packages[0].sha256_hashes == {hash_value}


@pytest.mark.parametrize(
    ("lock", "reason"),
    [
        (b"pytest>=8\n", "repository_python_lock_unsupported"),
        (b"pytest==8.3.5\n", "repository_python_lock_unsupported"),
        (
            b"pytest==8.3.5 --hash=sha256:" + b"b" * 64 + b"\n"
            b"pytest==8.3.5 --hash=sha256:" + b"c" * 64 + b"\n",
            "repository_python_lock_duplicate",
        ),
        (
            b"typing-extensions==4.12 --hash=sha256:" + b"b" * 64 + b"\n",
            "repository_python_lock_missing_pytest",
        ),
        (
            b"pytest==8.3.5 --index-url https://private.invalid/simple\n",
            "repository_python_lock_unsupported",
        ),
    ],
)
def test_python_lock_rejects_unsupported_forms(lock: bytes, reason: str) -> None:
    with pytest.raises(RepositoryPreflightError, match=reason):
        _parse_python_lock(lock)


def test_node_lock_requires_single_package_public_registry_and_integrity() -> None:
    integrity = "sha512-" + base64.b64encode(b"i" * 64).decode()
    package_json = json.dumps(
        {"name": "fixture", "version": "1.0.0", "scripts": {"test": "node test.js"}}
    ).encode()
    package_lock = json.dumps(
        {
            "name": "fixture",
            "version": "1.0.0",
            "lockfileVersion": 3,
            "packages": {
                "": {"name": "fixture", "version": "1.0.0"},
                "node_modules/fixture-dep": {
                    "version": "2.0.0",
                    "resolved": "https://registry.npmjs.org/fixture-dep/-/fixture-dep-2.0.0.tgz",
                    "integrity": integrity,
                },
            },
        }
    ).encode()

    packages = _parse_node_manifest(package_json, package_lock)

    assert [(item.name, item.version) for item in packages] == [("fixture-dep", "2.0.0")]
    assert packages[0].integrity == integrity


@pytest.mark.parametrize(
    ("package", "lock", "reason"),
    [
        (
            {"scripts": {"test": "node test.js", "postinstall": "node setup.js"}},
            {"lockfileVersion": 3, "packages": {"": {}}},
            "repository_node_install_lifecycle_unsupported",
        ),
        (
            {"workspaces": ["packages/*"], "scripts": {"test": "node test.js"}},
            {"lockfileVersion": 3, "packages": {"": {}}},
            "repository_node_workspace_unsupported",
        ),
        (
            {"scripts": {"test": "node test.js"}},
            {
                "lockfileVersion": 3,
                "packages": {
                    "": {},
                    "node_modules/private-dep": {
                        "version": "1.0.0",
                        "resolved": "https://packages.private.invalid/pkg.tgz",
                        "integrity": "sha512-" + base64.b64encode(b"i" * 64).decode(),
                    },
                },
            },
            "repository_package_source_unsupported",
        ),
        (
            {
                "scripts": {"test": "node test.js"},
                "devDependencies": {"fixture-dep": "git+https://github.com/acme/fixture-dep.git"},
            },
            {
                "lockfileVersion": 3,
                "packages": {
                    "": {
                        "devDependencies": {
                            "fixture-dep": "git+https://github.com/acme/fixture-dep.git"
                        }
                    }
                },
            },
            "repository_package_source_unsupported",
        ),
        (
            {
                "name": "fixture-app",
                "version": "1.0.0",
                "scripts": {"test": "node test.js"},
            },
            {
                "name": "fixture-app",
                "version": "1.0.0",
                "lockfileVersion": 3,
                "packages": {"": {"name": "different-app", "version": "1.0.0"}},
            },
            "repository_node_lock_mismatch",
        ),
    ],
)
def test_node_lock_rejects_unsupported_configuration(
    package: dict[str, object], lock: dict[str, object], reason: str
) -> None:
    with pytest.raises(RepositoryPreflightError, match=reason):
        _parse_node_manifest(json.dumps(package).encode(), json.dumps(lock).encode())


@pytest.mark.parametrize(
    ("blocked_path", "reason"),
    [
        (".npmrc", "repository_package_source_unsupported"),
        ("config/.npmrc", "repository_package_source_unsupported"),
        ("npm-shrinkwrap.json", "repository_node_lock_unsupported"),
        ("binding.gyp", "repository_node_native_unsupported"),
        ("CMakeLists.txt", "repository_node_native_unsupported"),
        ("native/addon.cpp", "repository_node_native_unsupported"),
        ("native/addon.node", "repository_node_native_unsupported"),
    ],
)
def test_node_tree_rejects_registry_overrides_shrinkwrap_and_native_inputs(
    blocked_path: str,
    reason: str,
) -> None:
    entries = {
        "package.json": {"path": "package.json", "type": "blob"},
        "package-lock.json": {"path": "package-lock.json", "type": "blob"},
        blocked_path: {"path": blocked_path, "type": "blob"},
    }

    with pytest.raises(RepositoryPreflightError, match=reason):
        _selected_manifest_paths(NODE_NPM_PROFILE, entries)


@pytest.mark.parametrize("native_path", ["src/native.dll", "src/native.cpp", "CMakeLists.txt"])
def test_python_tree_rejects_declared_native_inputs(native_path: str) -> None:
    entries = {
        "requirements.lock": {"path": "requirements.lock", "type": "blob"},
        "src/app.py": {"path": "src/app.py", "type": "blob"},
        native_path: {"path": native_path, "type": "blob"},
    }

    with pytest.raises(
        RepositoryPreflightError,
        match="repository_python_native_source_unsupported",
    ):
        _selected_manifest_paths(PYTHON_PYTEST_PROFILE, entries)


_REPOSITORY_ID = 123
_COMMIT_SHA = "a" * 40
_TREE_SHA = "b" * 40
_LOCK_SHA = "c" * 40
_SOURCE_SHA = "d" * 40
_ATTRIBUTES_SHA = "e" * 40
_PYTEST_HASH = "f" * 64
_PLUGGY_SHA = "9" * 40
_PLUGGY_HASH = "8" * 64


def _json_response(
    request: httpx.Request, document: object, *, status: int = 200
) -> httpx.Response:
    return httpx.Response(status, json=document, request=request)


def _python_adapter(
    *,
    repository_document: dict[str, object] | None = None,
    tree_document: dict[str, object] | None = None,
    package_document: dict[str, object] | None = None,
    extra_entries: list[dict[str, object]] | None = None,
    extra_blobs: dict[str, bytes] | None = None,
    github_response_override: dict[str, httpx.Response] | None = None,
    github_token: SecretStr | None = None,
    lock_bytes: bytes | None = None,
) -> tuple[GitHubRepositoryAdapter, list[httpx.Request]]:
    default_lock = (
        f"pytest==8.3.5 --hash=sha256:{_PYTEST_HASH}\npluggy==1.6.0 --hash=sha256:{_PLUGGY_HASH}\n"
    ).encode()
    lock = lock_bytes or default_lock
    source = b"print('fixture')\n"
    blobs = {
        _LOCK_SHA: lock,
        _SOURCE_SHA: source,
        **(extra_blobs or {}),
    }
    entries: list[dict[str, object]] = [
        {
            "path": "requirements.lock",
            "mode": "100644",
            "type": "blob",
            "size": len(lock),
            "sha": _LOCK_SHA,
        },
        {
            "path": "src/app.py",
            "mode": "100644",
            "type": "blob",
            "size": len(source),
            "sha": _SOURCE_SHA,
        },
        *(extra_entries or []),
    ]
    default_tree: dict[str, object] = {
        "sha": _TREE_SHA,
        "truncated": False,
        "tree": entries,
    }
    default_repo: dict[str, object] = {
        "id": _REPOSITORY_ID,
        "name": "project",
        "owner": {"login": "fixture-owner"},
        "private": False,
        "visibility": "public",
        "archived": False,
        "fork": False,
        "size": 1,
    }
    default_pypi: dict[str, object] = {
        "info": {
            "name": "pytest",
            "version": "8.3.5",
            "requires_python": ">=3.8",
            "requires_dist": ["pluggy>=1.5"],
        },
        "urls": [
            {
                "filename": "pytest-8.3.5-py3-none-any.whl",
                "packagetype": "bdist_wheel",
                "requires_python": ">=3.8",
                "digests": {"sha256": _PYTEST_HASH},
                "url": "https://files.pythonhosted.org/packages/pytest.whl",
            }
        ],
    }
    pluggy_pypi: dict[str, object] = {
        "info": {
            "name": "pluggy",
            "version": "1.6.0",
            "requires_python": ">=3.9",
            "requires_dist": None,
        },
        "urls": [
            {
                "filename": "pluggy-1.6.0-py3-none-any.whl",
                "packagetype": "bdist_wheel",
                "requires_python": None,
                "digests": {"sha256": _PLUGGY_HASH},
                "url": "https://files.pythonhosted.org/packages/pluggy.whl",
            }
        ],
    }
    repository_payload = repository_document or default_repo
    tree_payload = tree_document or default_tree
    pypi_payload = package_document or default_pypi
    overrides = github_response_override or {}
    requests: list[httpx.Request] = []

    def github_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.host == "api.github.com"
        assert request.headers["accept-encoding"] == "identity"
        if github_token is None:
            assert "authorization" not in request.headers
        else:
            assert request.headers["authorization"] == (f"Bearer {github_token.get_secret_value()}")
        if request.url.path == f"/repositories/{_REPOSITORY_ID}":
            return overrides.get(request.url.path) or _json_response(request, repository_payload)
        if request.url.path == f"/repos/fixture-owner/project/commits/{_COMMIT_SHA}":
            return _json_response(
                request,
                {"sha": _COMMIT_SHA, "commit": {"tree": {"sha": _TREE_SHA}}},
            )
        if request.url.path == f"/repos/fixture-owner/project/git/trees/{_TREE_SHA}":
            assert request.url.query.decode() == "recursive=1"
            return _json_response(request, tree_payload)
        if "/git/blobs/" in request.url.path:
            blob_sha = request.url.path.rsplit("/", 1)[-1]
            content = blobs.get(blob_sha)
            if content is None:
                return _json_response(request, {"message": "missing"}, status=404)
            return _json_response(
                request,
                {
                    "sha": blob_sha,
                    "size": len(content),
                    "encoding": "base64",
                    "content": base64.b64encode(content).decode(),
                },
            )
        raise AssertionError(f"unexpected GitHub request path: {request.url.path}")

    def pypi_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.host == "pypi.org"
        assert request.headers["accept-encoding"] == "identity"
        assert "authorization" not in request.headers
        if request.url.path == "/pypi/pytest/8.3.5/json":
            return _json_response(request, pypi_payload)
        if request.url.path == "/pypi/pluggy/1.6.0/json":
            return _json_response(request, pluggy_pypi)
        raise AssertionError(f"unexpected PyPI request path: {request.url.path}")

    adapter = GitHubRepositoryAdapter(
        github_transport=httpx.MockTransport(github_handler),
        pypi_transport=httpx.MockTransport(pypi_handler),
        github_token=github_token,
    )
    return adapter, requests


class _FakeState:
    def __init__(self, cached: EligibilityReceipt | None = None) -> None:
        self.reservation = PreflightReservation(
            uuid4(), 901, datetime.now(UTC) + timedelta(minutes=1)
        )
        self.cached = cached
        self.reserved: list[tuple[object, ...]] = []
        self.cache_reads: list[tuple[object, ...]] = []
        self.cache_writes: list[EligibilityReceipt] = []
        self.releases: list[tuple[object, ...]] = []

    def reserve(self, *arguments: object) -> PreflightReservation:
        self.reserved.append(arguments)
        return self.reservation

    def get_cached(self, *arguments: object) -> EligibilityReceipt | None:
        self.cache_reads.append(arguments)
        return self.cached

    def put_cached(
        self, _principal: object, _check_id: object, receipt: EligibilityReceipt
    ) -> None:
        self.cache_writes.append(receipt)

    def release(self, *arguments: object) -> None:
        self.releases.append(arguments)


class _FakeVerifier:
    def __init__(self, *, actor_can_write: bool = True, app_can_access: bool = True) -> None:
        self.actor_can_write = actor_can_write
        self.app_can_access = app_can_access
        self.calls: list[tuple[int, int]] = []

    async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
        self.calls.append((repository_id, github_user_id))
        return RepositoryAccessProof(
            repository_id,
            github_user_id,
            self.actor_can_write,
            self.app_can_access,
        )


def test_public_commit_preflight_uses_fixed_serial_gets_and_caches_only_metadata() -> None:
    adapter, requests = _python_adapter()
    try:

        async def exercise() -> EligibilityReceipt:
            repository = await adapter.current_public_repository(_REPOSITORY_ID)
            return await adapter.inspect_commit(repository, _COMMIT_SHA, PYTHON_PYTEST_PROFILE)

        result = asyncio.run(exercise())
    finally:
        asyncio.run(adapter.aclose())

    assert result.repository_id == _REPOSITORY_ID
    assert result.commit_sha == _COMMIT_SHA
    assert result.tree_sha == _TREE_SHA
    assert result.profile_id == "python-pytest"
    assert result.file_count == 2
    assert result.package_count == 2
    assert result.manifest_blobs == (("requirements.lock", _LOCK_SHA),)
    assert [request.method for request in requests] == ["GET"] * 6
    assert {request.url.host for request in requests} == {"api.github.com", "pypi.org"}
    assert all("authorization" not in request.headers for request in requests)
    assert all(
        request.url.path.startswith(("/repositories/", "/repos/", "/pypi/")) for request in requests
    )


@pytest.mark.parametrize(
    ("repository_document", "tree_document", "reason"),
    [
        (
            {"id": _REPOSITORY_ID, "private": True, "visibility": "private"},
            None,
            "repository_not_public",
        ),
        (
            {"id": _REPOSITORY_ID, "private": False, "visibility": "public", "archived": True},
            None,
            "repository_archived",
        ),
        (
            None,
            {"sha": _TREE_SHA, "truncated": True, "tree": []},
            "repository_tree_truncated",
        ),
        (
            None,
            {
                "sha": _TREE_SHA,
                "truncated": False,
                "tree": [
                    {
                        "path": "escape",
                        "mode": "120000",
                        "type": "blob",
                        "size": 1,
                        "sha": _LOCK_SHA,
                    }
                ],
            },
            "repository_symlink_unsupported",
        ),
        (
            None,
            {
                "sha": _TREE_SHA,
                "truncated": False,
                "tree": [{"path": "vendor", "mode": "160000", "type": "commit", "sha": _LOCK_SHA}],
            },
            "repository_gitlink_unsupported",
        ),
    ],
)
def test_repository_and_tree_eligibility_fail_closed_before_manifest_reads(
    repository_document: dict[str, object] | None,
    tree_document: dict[str, object] | None,
    reason: str,
) -> None:
    adapter, requests = _python_adapter(
        repository_document=repository_document,
        tree_document=tree_document,
    )
    try:

        async def exercise() -> None:
            repository = await adapter.current_public_repository(_REPOSITORY_ID)
            await adapter.inspect_commit(repository, _COMMIT_SHA, PYTHON_PYTEST_PROFILE)

        with pytest.raises(RepositoryPreflightError, match=reason):
            asyncio.run(exercise())
    finally:
        asyncio.run(adapter.aclose())

    expected_requests = 1 if reason in {"repository_not_public", "repository_archived"} else 3
    assert len(requests) == expected_requests


def test_preflight_stops_if_any_gitattributes_declares_lfs() -> None:
    attributes = b"*.bin filter=lfs diff=lfs merge=lfs -text\n"
    adapter, requests = _python_adapter(
        extra_entries=[
            {
                "path": ".gitattributes",
                "mode": "100644",
                "type": "blob",
                "size": len(attributes),
                "sha": _ATTRIBUTES_SHA,
            }
        ],
        extra_blobs={_ATTRIBUTES_SHA: attributes},
    )
    try:

        async def exercise() -> None:
            repository = await adapter.current_public_repository(_REPOSITORY_ID)
            await adapter.inspect_commit(repository, _COMMIT_SHA, PYTHON_PYTEST_PROFILE)

        with pytest.raises(RepositoryPreflightError, match="repository_lfs_unsupported"):
            asyncio.run(exercise())
    finally:
        asyncio.run(adapter.aclose())

    assert len(requests) == 5
    assert not any(request.url.host == "pypi.org" for request in requests)


def test_preflight_fails_when_lock_hash_is_not_a_declared_pure_python_wheel() -> None:
    platform_wheel = {
        "info": {
            "name": "pytest",
            "version": "8.3.5",
            "requires_python": ">=3.8",
            "requires_dist": ["pluggy>=1.5"],
        },
        "urls": [
            {
                "filename": "pytest-8.3.5-cp313-cp313-manylinux_x86_64.whl",
                "packagetype": "bdist_wheel",
                "requires_python": None,
                "digests": {"sha256": _PYTEST_HASH},
                "url": "https://files.pythonhosted.org/packages/pytest.whl",
            }
        ],
    }
    adapter, requests = _python_adapter(package_document=platform_wheel)
    try:

        async def exercise() -> None:
            repository = await adapter.current_public_repository(_REPOSITORY_ID)
            await adapter.inspect_commit(repository, _COMMIT_SHA, PYTHON_PYTEST_PROFILE)

        with pytest.raises(RepositoryPreflightError, match="repository_python_wheel_unsupported"):
            asyncio.run(exercise())
    finally:
        asyncio.run(adapter.aclose())

    assert len(requests) == 5


def test_python_requires_dist_must_be_closed_by_the_hash_pinned_lock() -> None:
    lock = f"pytest==8.3.5 --hash=sha256:{_PYTEST_HASH}\n".encode()
    adapter, requests = _python_adapter(lock_bytes=lock)
    try:

        async def exercise() -> None:
            repository = await adapter.current_public_repository(_REPOSITORY_ID)
            await adapter.inspect_commit(repository, _COMMIT_SHA, PYTHON_PYTEST_PROFILE)

        with pytest.raises(RepositoryPreflightError, match="repository_python_dependency_missing"):
            asyncio.run(exercise())
    finally:
        asyncio.run(adapter.aclose())

    assert len(requests) == 5
    assert requests[-1].url.path == "/pypi/pytest/8.3.5/json"


def test_python_requirement_markers_use_the_declared_target_and_ignore_unselected_extras() -> None:
    active = _active_python_requirements(
        [
            "pluggy>=1.5; sys_platform == 'linux' and python_version == '3.13'",
            "win-only>=1; sys_platform == 'win32'",
            "dev-only>=1; extra == 'dev'",
        ]
    )

    assert [(item.name, item.specifier) for item in active] == [("pluggy", ">=1.5")]
    _require_python_target_compatibility(">=3.13.12")
    with pytest.raises(
        RepositoryPreflightError,
        match="repository_python_version_unsupported",
    ):
        _require_python_target_compatibility("<3.13.12")
    with pytest.raises(RepositoryPreflightError, match="repository_python_extras_unsupported"):
        _active_python_requirements(["pluggy[testing]>=1.5"])


def test_python_release_with_mismatched_transitive_lock_version_is_rejected() -> None:
    packages = _parse_python_lock(
        (
            f"pytest==8.3.5 --hash=sha256:{_PYTEST_HASH}\n"
            f"pluggy==1.4.0 --hash=sha256:{_PLUGGY_HASH}\n"
        ).encode()
    )
    from continuum_api.repositories.preflight import _validate_python_dependency_closure

    with pytest.raises(
        RepositoryPreflightError,
        match="repository_python_dependency_version_mismatch",
    ):
        _validate_python_dependency_closure(
            packages,
            [
                _active_python_requirements(["pluggy>=1.5"])[0],
            ],
        )


def test_preflight_service_requires_trusted_access_before_any_github_read() -> None:
    adapter, requests = _python_adapter()
    state = _FakeState()
    verifier = _FakeVerifier(actor_can_write=False)
    service = RepositoryPreflightService(
        state,
        access_verifier=verifier,
        adapter_factory=lambda: adapter,
    )
    try:
        with pytest.raises(RepositoryPreflightError, match="repository_access_denied"):
            asyncio.run(
                service.check(
                    Principal(uuid4()),
                    repository_id=_REPOSITORY_ID,
                    commit_sha=_COMMIT_SHA,
                    profile_id="python-pytest",
                    profile_version="1",
                )
            )
    finally:
        asyncio.run(adapter.aclose())

    assert verifier.calls == [(_REPOSITORY_ID, state.reservation.github_user_id)]
    assert requests == []
    assert state.cache_reads == []
    assert state.releases[0][2] == "failed"


def test_cleanup_failure_does_not_mask_the_original_preflight_rejection() -> None:
    class ReleaseFails(_FakeState):
        def release(self, *_arguments: object) -> None:
            raise RepositoryPreflightError("repository_preflight_reservation_lost")

    adapter, requests = _python_adapter()
    state = ReleaseFails()
    service = RepositoryPreflightService(
        state,
        access_verifier=_FakeVerifier(actor_can_write=False),
        adapter_factory=lambda: adapter,
    )
    try:
        with pytest.raises(RepositoryPreflightError, match="repository_access_denied"):
            asyncio.run(
                service.check(
                    Principal(uuid4()),
                    repository_id=_REPOSITORY_ID,
                    commit_sha=_COMMIT_SHA,
                    profile_id="python-pytest",
                    profile_version="1",
                )
            )
    finally:
        asyncio.run(adapter.aclose())

    assert requests == []


def test_default_verifier_fails_pending_without_network_or_cache_access() -> None:
    adapter, requests = _python_adapter()
    state = _FakeState()
    service = RepositoryPreflightService(state, adapter_factory=lambda: adapter)
    try:
        with pytest.raises(RepositoryPreflightError, match="repository_authorization_pending"):
            asyncio.run(
                service.check(
                    Principal(uuid4()),
                    repository_id=_REPOSITORY_ID,
                    commit_sha=_COMMIT_SHA,
                    profile_id="python-pytest",
                    profile_version="1",
                )
            )
    finally:
        asyncio.run(adapter.aclose())

    assert requests == []
    assert state.cache_reads == []
    assert state.releases[0][2] == "failed"


def test_current_access_and_public_metadata_are_rechecked_before_cache_hit() -> None:
    cached = EligibilityReceipt(
        repository_id=_REPOSITORY_ID,
        commit_sha=_COMMIT_SHA,
        tree_sha=_TREE_SHA,
        profile_id="python-pytest",
        profile_version="1",
        file_count=2,
        source_bytes=8,
        manifest_blobs=(("requirements.lock", _LOCK_SHA),),
        package_count=1,
    )
    adapter, requests = _python_adapter()
    state = _FakeState(cached)
    verifier = _FakeVerifier()
    service = RepositoryPreflightService(
        state,
        access_verifier=verifier,
        adapter_factory=lambda: adapter,
    )
    try:
        result = asyncio.run(
            service.check(
                Principal(uuid4()),
                repository_id=_REPOSITORY_ID,
                commit_sha=_COMMIT_SHA,
                profile_id="python-pytest",
                profile_version="1",
            )
        )
    finally:
        asyncio.run(adapter.aclose())

    assert result.cache_hit is True
    assert verifier.calls == [(_REPOSITORY_ID, state.reservation.github_user_id)]
    assert state.cache_reads
    assert len(requests) == 1
    assert requests[0].url.path == f"/repositories/{_REPOSITORY_ID}"
    assert state.releases[0][2] == "completed"


def test_rate_limit_response_is_not_retried_and_redirect_is_not_followed() -> None:
    adapter, requests = _python_adapter(
        github_response_override={
            f"/repositories/{_REPOSITORY_ID}": httpx.Response(
                429,
                headers={"retry-after": "60"},
            )
        }
    )
    try:

        async def exercise() -> None:
            await adapter.current_public_repository(_REPOSITORY_ID)

        with pytest.raises(RepositoryPreflightError, match="repository_read_rate_limited"):
            asyncio.run(exercise())
    finally:
        asyncio.run(adapter.aclose())

    assert len(requests) == 1


def test_redirect_is_rejected_without_following_or_forwarding_the_token() -> None:
    secret_token = SecretStr("github-token-sentinel")
    adapter, requests = _python_adapter(
        github_token=secret_token,
        github_response_override={
            f"/repositories/{_REPOSITORY_ID}": httpx.Response(
                302,
                headers={"location": "https://attacker.invalid/collect"},
            )
        },
    )
    try:

        async def exercise() -> None:
            await adapter.current_public_repository(_REPOSITORY_ID)

        with pytest.raises(
            RepositoryPreflightError, match="repository_redirect_unsupported"
        ) as error:
            asyncio.run(exercise())
    finally:
        asyncio.run(adapter.aclose())

    assert len(requests) == 1
    assert requests[0].url.host == "api.github.com"
    assert requests[0].headers["authorization"] == "Bearer github-token-sentinel"
    assert "github-token-sentinel" not in str(error.value)


def test_optional_github_token_never_reaches_public_package_hosts() -> None:
    secret_token = SecretStr("github-token-sentinel")
    adapter, requests = _python_adapter(github_token=secret_token)
    try:

        async def exercise() -> None:
            repository = await adapter.current_public_repository(_REPOSITORY_ID)
            await adapter.inspect_commit(repository, _COMMIT_SHA, PYTHON_PYTEST_PROFILE)

        asyncio.run(exercise())
    finally:
        asyncio.run(adapter.aclose())

    github_requests = [request for request in requests if request.url.host == "api.github.com"]
    pypi_requests = [request for request in requests if request.url.host == "pypi.org"]
    assert github_requests
    assert all(
        request.headers["authorization"] == "Bearer github-token-sentinel"
        for request in github_requests
    )
    assert len(pypi_requests) == 2
    assert "authorization" not in pypi_requests[0].headers


def test_chunked_or_advertised_metadata_overflow_stops_within_the_byte_budget() -> None:
    requests: list[httpx.Request] = []

    def oversized(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"x" * (2 * 1024 * 1024 + 1), request=request)

    adapter = GitHubRepositoryAdapter(github_transport=httpx.MockTransport(oversized))
    try:
        with pytest.raises(RepositoryPreflightError, match="repository_metadata_byte_limit"):
            asyncio.run(adapter.current_public_repository(_REPOSITORY_ID))
    finally:
        asyncio.run(adapter.aclose())

    assert len(requests) == 1


def test_compressed_metadata_is_rejected_before_httpx_decoding() -> None:
    metadata = {
        "id": _REPOSITORY_ID,
        "name": "project",
        "owner": {"login": "fixture-owner"},
        "private": False,
        "visibility": "public",
        "archived": False,
        "fork": False,
        "size": 1,
        "ignored": "x" * (3 * 1024 * 1024),
    }
    compressed = gzip.compress(json.dumps(metadata).encode())
    response = httpx.Response(
        200,
        content=compressed,
        headers={"content-encoding": "gzip", "content-type": "application/json"},
        request=httpx.Request("GET", f"https://api.github.com/repositories/{_REPOSITORY_ID}"),
    )
    adapter, requests = _python_adapter(
        github_response_override={f"/repositories/{_REPOSITORY_ID}": response}
    )
    try:
        with pytest.raises(
            RepositoryPreflightError,
            match="repository_response_encoding_unsupported",
        ):
            asyncio.run(adapter.current_public_repository(_REPOSITORY_ID))
    finally:
        asyncio.run(adapter.aclose())

    assert len(requests) == 1
    assert requests[0].headers["accept-encoding"] == "identity"


def test_drip_response_is_stopped_by_the_absolute_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    class DripStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"id":'
            await asyncio.sleep(1.0)
            yield b"123}"

        async def aclose(self) -> None:
            return None

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, stream=DripStream(), request=request)

    monkeypatch.setattr("continuum_api.repositories.preflight.CHECK_DEADLINE_SECONDS", 0.5)
    adapter = GitHubRepositoryAdapter(github_transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(RepositoryPreflightError, match="repository_preflight_timeout"):
            asyncio.run(adapter.current_public_repository(_REPOSITORY_ID))
    finally:
        asyncio.run(adapter.aclose())

    assert len(requests) == 1


def test_aggregate_request_budget_denies_before_a_33rd_get() -> None:
    dependency_names = [f"fixture-dep-{index}" for index in range(24)]
    dependency_specs = {name: "1.0.0" for name in dependency_names}
    package_json = json.dumps(
        {
            "name": "fixture-app",
            "version": "1.0.0",
            "scripts": {"test": "node test.js"},
            "dependencies": dependency_specs,
        }
    ).encode()
    package_entries = {
        "": {
            "name": "fixture-app",
            "version": "1.0.0",
            "dependencies": dependency_specs,
        }
    }
    for index, dependency_name in enumerate(dependency_names):
        package_entries[f"node_modules/{dependency_name}"] = {
            "version": "1.0.0",
            "resolved": (
                f"https://registry.npmjs.org/{dependency_name}/-/{dependency_name}-1.0.0.tgz"
            ),
            "integrity": "sha512-" + base64.b64encode(bytes([index + 1]) * 64).decode(),
        }
    package_lock = json.dumps(
        {
            "name": "fixture-app",
            "version": "1.0.0",
            "lockfileVersion": 3,
            "packages": package_entries,
        }
    ).encode()
    lock_hashes = {
        _LOCK_SHA: package_json,
        _SOURCE_SHA: package_lock,
    }
    tree_entries: list[dict[str, object]] = [
        {
            "path": "package.json",
            "mode": "100644",
            "type": "blob",
            "size": len(package_json),
            "sha": _LOCK_SHA,
        },
        {
            "path": "package-lock.json",
            "mode": "100644",
            "type": "blob",
            "size": len(package_lock),
            "sha": _SOURCE_SHA,
        },
    ]
    for index in range(4):
        blob_sha = f"{index + 1:x}" * 40
        content = b"*.txt text\n"
        tree_entries.append(
            {
                "path": f"directory-{index}/.gitattributes",
                "mode": "100644",
                "type": "blob",
                "size": len(content),
                "sha": blob_sha,
            }
        )
        lock_hashes[blob_sha] = content

    requests: list[httpx.Request] = []

    def github_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == f"/repositories/{_REPOSITORY_ID}":
            return _json_response(
                request,
                {
                    "id": _REPOSITORY_ID,
                    "name": "project",
                    "owner": {"login": "fixture-owner"},
                    "private": False,
                    "visibility": "public",
                    "archived": False,
                    "fork": False,
                    "size": 1,
                },
            )
        if request.url.path.endswith(f"/commits/{_COMMIT_SHA}"):
            return _json_response(
                request, {"sha": _COMMIT_SHA, "commit": {"tree": {"sha": _TREE_SHA}}}
            )
        if request.url.path.endswith(f"/trees/{_TREE_SHA}"):
            return _json_response(
                request,
                {"sha": _TREE_SHA, "truncated": False, "tree": tree_entries},
            )
        blob_sha = request.url.path.rsplit("/", 1)[-1]
        content = lock_hashes.get(blob_sha)
        if content is None:
            raise AssertionError("request budget should stop before any package metadata request")
        return _json_response(
            request,
            {
                "sha": blob_sha,
                "size": len(content),
                "encoding": "base64",
                "content": base64.b64encode(content).decode(),
            },
        )

    adapter = GitHubRepositoryAdapter(github_transport=httpx.MockTransport(github_handler))
    try:

        async def exercise() -> None:
            repository = await adapter.current_public_repository(_REPOSITORY_ID)
            await adapter.inspect_commit(repository, _COMMIT_SHA, NODE_NPM_PROFILE)

        with pytest.raises(RepositoryPreflightError, match="repository_request_limit"):
            asyncio.run(exercise())
    finally:
        asyncio.run(adapter.aclose())

    assert len(requests) == 9
    assert all(request.url.host == "api.github.com" for request in requests)


def test_access_verifier_is_inside_the_absolute_preflight_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SlowVerifier(_FakeVerifier):
        async def verify(self, *, repository_id: int, github_user_id: int) -> RepositoryAccessProof:
            await asyncio.sleep(0.3)
            return await super().verify(repository_id=repository_id, github_user_id=github_user_id)

    monkeypatch.setattr("continuum_api.repositories.preflight.CHECK_DEADLINE_SECONDS", 0.01)
    adapter, requests = _python_adapter()
    state = _FakeState()
    service = RepositoryPreflightService(
        state,
        access_verifier=SlowVerifier(),
        adapter_factory=lambda: adapter,
    )
    try:
        with pytest.raises(RepositoryPreflightError, match="repository_preflight_timeout"):
            asyncio.run(
                service.check(
                    Principal(uuid4()),
                    repository_id=_REPOSITORY_ID,
                    commit_sha=_COMMIT_SHA,
                    profile_id="python-pytest",
                    profile_version="1",
                )
            )
    finally:
        asyncio.run(adapter.aclose())

    assert requests == []
    assert state.releases[0][2] == "failed"


def test_single_package_node_profile_accepts_public_locked_registry_artifacts() -> None:
    integrity = "sha512-" + base64.b64encode(b"n" * 64).decode()
    tarball = "https://registry.npmjs.org/vitest/-/vitest-1.0.0.tgz"
    package_json = json.dumps(
        {
            "name": "fixture-app",
            "version": "1.0.0",
            "scripts": {"test": "vitest run"},
            "devDependencies": {"vitest": "1.0.0"},
        }
    ).encode()
    package_lock = json.dumps(
        {
            "name": "fixture-app",
            "version": "1.0.0",
            "lockfileVersion": 3,
            "packages": {
                "": {
                    "name": "fixture-app",
                    "version": "1.0.0",
                    "devDependencies": {"vitest": "1.0.0"},
                },
                "node_modules/vitest": {
                    "version": "1.0.0",
                    "resolved": tarball,
                    "integrity": integrity,
                },
            },
        }
    ).encode()
    npm_document = {
        "name": "vitest",
        "version": "1.0.0",
        "dist": {"tarball": tarball, "integrity": integrity},
        "scripts": {},
    }
    entries = [
        {
            "path": "package.json",
            "mode": "100644",
            "type": "blob",
            "size": len(package_json),
            "sha": _LOCK_SHA,
        },
        {
            "path": "package-lock.json",
            "mode": "100644",
            "type": "blob",
            "size": len(package_lock),
            "sha": _SOURCE_SHA,
        },
    ]
    requests: list[httpx.Request] = []

    def github_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.host == "api.github.com"
        assert request.headers["accept-encoding"] == "identity"
        if request.url.path == f"/repositories/{_REPOSITORY_ID}":
            repository_doc = {
                "id": _REPOSITORY_ID,
                "name": "project",
                "owner": {"login": "fixture-owner"},
                "private": False,
                "visibility": "public",
                "archived": False,
                "fork": False,
                "size": 1,
            }
            return _json_response(request, repository_doc)
        if request.url.path.endswith(f"/commits/{_COMMIT_SHA}"):
            return _json_response(
                request, {"sha": _COMMIT_SHA, "commit": {"tree": {"sha": _TREE_SHA}}}
            )
        if request.url.path.endswith(f"/trees/{_TREE_SHA}"):
            return _json_response(request, {"sha": _TREE_SHA, "truncated": False, "tree": entries})
        blob_content = {_LOCK_SHA: package_json, _SOURCE_SHA: package_lock}.get(
            request.url.path.rsplit("/", 1)[-1]
        )
        if blob_content is not None:
            blob_sha = request.url.path.rsplit("/", 1)[-1]
            return _json_response(
                request,
                {
                    "sha": blob_sha,
                    "size": len(blob_content),
                    "encoding": "base64",
                    "content": base64.b64encode(blob_content).decode(),
                },
            )
        raise AssertionError(f"unexpected GitHub path: {request.url.path}")

    def npm_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.host == "registry.npmjs.org"
        assert request.headers["accept-encoding"] == "identity"
        assert "authorization" not in request.headers
        assert request.url.path == "/vitest/1.0.0"
        return _json_response(request, npm_document)

    adapter = GitHubRepositoryAdapter(
        github_transport=httpx.MockTransport(github_handler),
        npm_transport=httpx.MockTransport(npm_handler),
    )
    try:

        async def exercise() -> EligibilityReceipt:
            repository = await adapter.current_public_repository(_REPOSITORY_ID)
            return await adapter.inspect_commit(repository, _COMMIT_SHA, NODE_NPM_PROFILE)

        result = asyncio.run(exercise())
    finally:
        asyncio.run(adapter.aclose())

    assert result.profile_id == "node-npm"
    assert result.package_count == 1
    assert set(result.manifest_blobs) == {
        ("package.json", _LOCK_SHA),
        ("package-lock.json", _SOURCE_SHA),
    }
    assert len(requests) == 6


def test_cache_receipt_round_trip_rejects_malformed_or_oversized_metadata() -> None:
    receipt = EligibilityReceipt(
        repository_id=_REPOSITORY_ID,
        commit_sha=_COMMIT_SHA,
        tree_sha=_TREE_SHA,
        profile_id="python-pytest",
        profile_version="1",
        file_count=2,
        source_bytes=8,
        manifest_blobs=(("requirements.lock", _LOCK_SHA),),
        package_count=1,
    )

    restored = EligibilityReceipt.from_cache_payload(receipt.to_cache_payload())

    assert restored.cache_hit is True
    assert restored.manifest_blobs == receipt.manifest_blobs
    malformed = {**receipt.to_cache_payload(), "source_bytes": 50 * 1024 * 1024 + 1}
    with pytest.raises(RepositoryPreflightError, match="repository_preflight_cache_corrupt"):
        EligibilityReceipt.from_cache_payload(malformed)
