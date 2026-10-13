"""Pure synthetic-data checks for trusted Python profile preparation."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import stat
import struct
import urllib.error
import zipfile
from pathlib import Path
from typing import Any

import pytest

from continuum_api.artifacts.archives import ArchiveFile, create_archive, verify_archive
from continuum_api.sandbox.profile_bootstrap import (
    MAX_WHEEL_EXPANDED_BYTES,
    ProfileBootstrapError,
    _verify_wheel_contents,
    extract_verified_archive,
    parse_requirements_lock,
    probe_network,
    verify_wheelhouse,
)
from continuum_api.sandbox.python_bootstrap_recipe import (
    PYTHON_EXECUTABLE,
    bootstrap_argv,
    install_argv,
    network_probe_argv,
    pytest_argv,
    pytest_environment,
)


def _wheel_bytes(
    *,
    extra_files: dict[str, bytes] | None = None,
) -> bytes:
    dist_info = "tiny_package-1.2.3.dist-info"
    files = {
        f"{dist_info}/METADATA": (b"Metadata-Version: 2.1\nName: tiny-package\nVersion: 1.2.3\n\n"),
        f"{dist_info}/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: fixture\nRoot-Is-Purelib: true\nTag: py3-none-any\n\n"
        ),
        "tiny_package/__init__.py": b"VALUE = 1\n",
    }
    if extra_files:
        files.update(extra_files)
    record_path = f"{dist_info}/RECORD"
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for path, content in sorted(files.items()):
        digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest())
        writer.writerow((path, f"sha256={digest.rstrip(b'=').decode('ascii')}", str(len(content))))
    writer.writerow((record_path, "", ""))
    files[record_path] = record.getvalue().encode("utf-8")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path, content in sorted(files.items()):
            archive.writestr(path, content)
    return buffer.getvalue()


def _forge_central_uncompressed_size(wheel: bytes, path: str, size: int) -> bytes:
    forged = bytearray(wheel)
    with zipfile.ZipFile(io.BytesIO(wheel)) as archive:
        central_offset = archive.start_dir
        while forged[central_offset : central_offset + 4] == b"PK\x01\x02":
            values = struct.Struct(zipfile.structCentralDir).unpack_from(forged, central_offset)
            name_length = values[zipfile._CD_FILENAME_LENGTH]
            extra_length = values[zipfile._CD_EXTRA_FIELD_LENGTH]
            comment_length = values[zipfile._CD_COMMENT_LENGTH]
            name_start = central_offset + zipfile.sizeCentralDir
            member_name = bytes(forged[name_start : name_start + name_length]).decode("utf-8")
            if member_name == path:
                struct.pack_into("<L", forged, central_offset + 24, size)
                return bytes(forged)
            central_offset += zipfile.sizeCentralDir + name_length + extra_length + comment_length
    raise AssertionError(f"central record not found for {path}")


def test_lock_parser_accepts_only_flat_hash_pinned_pytest_closure() -> None:
    digest = "a" * 64
    locked = parse_requirements_lock(
        f"pytest==8.3.5 --hash=sha256:{digest}\n"
        f"iniconfig==2.0.0 --hash=sha256:{'b' * 64}\n".encode()
    )

    assert [(item.name, item.version) for item in locked] == [
        ("pytest", "8.3.5"),
        ("iniconfig", "2.0.0"),
    ]

    for invalid in (
        b"pytest>=8 --hash=sha256:" + b"a" * 64,
        b"pytest==8.3.5\n",
        b"pytest==8.3.5 --hash=sha256:" + b"a" * 64 + b"\npytest==8.3.5 --hash=sha256:" + b"b" * 64,
        b"some-package==1.0 --hash=sha256:" + b"a" * 64,
    ):
        with pytest.raises(ProfileBootstrapError):
            parse_requirements_lock(invalid)


def test_hash_pinned_pure_wheel_is_verified_before_install(tmp_path: Path) -> None:
    wheel = _wheel_bytes()
    wheel_hash = hashlib.sha256(wheel).hexdigest()
    pytest_wheel = _rename_wheel(_wheel_bytes(), "pytest", "8.3.5")
    pytest_hash = hashlib.sha256(pytest_wheel).hexdigest()
    lockfile = (
        f"tiny-package==1.2.3 --hash=sha256:{wheel_hash}\n"
        f"pytest==8.3.5 --hash=sha256:{pytest_hash}\n"
    ).encode()
    lockfile_hash = hashlib.sha256(lockfile).hexdigest()
    wheelhouse = tmp_path / "wheels"
    wheelhouse.mkdir()
    (wheelhouse / "tiny_package-1.2.3-py3-none-any.whl").write_bytes(wheel)
    (wheelhouse / "pytest-8.3.5-py3-none-any.whl").write_bytes(pytest_wheel)

    verified = verify_wheelhouse(
        lockfile,
        expected_lockfile_sha256=lockfile_hash,
        wheelhouse=wheelhouse,
    )

    assert [wheel.name for wheel in verified] == ["pytest", "tiny-package"]
    assert all(wheel.expanded_bytes > 0 for wheel in verified)


def test_forged_small_central_size_cannot_truncate_inflated_member() -> None:
    payload_path = "tiny_package/data.txt"
    wheel = _wheel_bytes(extra_files={payload_path: b"x" * 4096})
    forged = _forge_central_uncompressed_size(wheel, payload_path, 1)

    with pytest.raises(ProfileBootstrapError):
        _verify_wheel_contents(forged, "tiny-package", "1.2.3")


def test_wheelhouse_rejects_files_that_collide_after_install(tmp_path: Path) -> None:
    first = _wheel_bytes()
    second = _rename_wheel(
        _wheel_bytes(),
        "pytest",
        "8.3.5",
        extra_files={"tiny_package/__init__.py": b"collision"},
    )
    first_hash = hashlib.sha256(first).hexdigest()
    second_hash = hashlib.sha256(second).hexdigest()
    lockfile = (
        f"tiny-package==1.2.3 --hash=sha256:{first_hash}\n"
        f"pytest==8.3.5 --hash=sha256:{second_hash}\n"
    ).encode()
    wheelhouse = tmp_path / "wheels"
    wheelhouse.mkdir()
    (wheelhouse / "tiny_package-1.2.3-py3-none-any.whl").write_bytes(first)
    (wheelhouse / "pytest-8.3.5-py3-none-any.whl").write_bytes(second)

    with pytest.raises(ProfileBootstrapError, match="path_collision"):
        verify_wheelhouse(
            lockfile,
            expected_lockfile_sha256=hashlib.sha256(lockfile).hexdigest(),
            wheelhouse=wheelhouse,
        )


def test_actual_inflated_size_is_capped_even_when_central_size_is_forged() -> None:
    payload_path = "tiny_package/data.bin"
    wheel = _wheel_bytes(extra_files={payload_path: b"0" * (MAX_WHEEL_EXPANDED_BYTES + 1)})
    forged = _forge_central_uncompressed_size(wheel, payload_path, 1)

    with pytest.raises(ProfileBootstrapError, match="expansion_limit"):
        _verify_wheel_contents(forged, "tiny-package", "1.2.3")


def test_verified_archive_extracts_only_canonical_files_and_modes(tmp_path: Path) -> None:
    lockfile = b"pytest==8.3.5 --hash=sha256:" + b"a" * 64 + b"\n"
    bundle = create_archive(
        (
            ArchiveFile("requirements.lock", lockfile, 0o644),
            ArchiveFile("src/check.py", b"print('fixture')\n", 0o755),
            ArchiveFile("sitecustomize.py", b"raise RuntimeError('must stay data')\n"),
        )
    )
    workspace = tmp_path / "workspace"
    receipt = extract_verified_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_lockfile_sha256=hashlib.sha256(lockfile).hexdigest(),
        destination=workspace,
        verifier=verify_archive,
    )

    assert receipt.file_count == 3
    assert (workspace / "src/check.py").read_bytes() == b"print('fixture')\n"
    assert stat.S_IMODE((workspace / "src/check.py").stat().st_mode) == 0o755
    assert stat.S_IMODE((workspace / "requirements.lock").stat().st_mode) == 0o644
    assert (workspace / "sitecustomize.py").exists()


def test_extraction_rejects_nonempty_or_symlink_workspace_without_touching_target(
    tmp_path: Path,
) -> None:
    lockfile = b"pytest==8.3.5 --hash=sha256:" + b"b" * 64 + b"\n"
    bundle = create_archive((ArchiveFile("requirements.lock", lockfile),))
    nonempty = tmp_path / "nonempty"
    nonempty.mkdir()
    marker = nonempty / "keep.txt"
    marker.write_bytes(b"keep")
    with pytest.raises(ProfileBootstrapError, match="workspace_not_empty"):
        extract_verified_archive(
            bundle.archive,
            expected_archive_sha256=bundle.archive_sha256,
            expected_tree_sha256=bundle.manifest.tree_sha256,
            expected_lockfile_sha256=hashlib.sha256(lockfile).hexdigest(),
            destination=nonempty,
            verifier=verify_archive,
        )
    assert marker.read_bytes() == b"keep"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["nonempty"]

    actual = tmp_path / "actual"
    actual.mkdir()
    (actual / "keep.txt").write_bytes(b"target")
    link = tmp_path / "linked"
    link.symlink_to(actual, target_is_directory=True)
    with pytest.raises(ProfileBootstrapError, match="workspace_not_directory"):
        extract_verified_archive(
            bundle.archive,
            expected_archive_sha256=bundle.archive_sha256,
            expected_tree_sha256=bundle.manifest.tree_sha256,
            expected_lockfile_sha256=hashlib.sha256(lockfile).hexdigest(),
            destination=link,
            verifier=verify_archive,
        )
    assert link.is_symlink()
    assert (actual / "keep.txt").read_bytes() == b"target"


@pytest.mark.parametrize(
    "path",
    [
        "tiny_package/evil.pth",
        "tiny_package/sitecustomize.py",
        "tiny_package/native.so",
        "tiny_package-1.2.3.data/scripts/evil",
    ],
)
def test_wheel_rejects_startup_native_and_spread_files(path: str) -> None:
    wheel = _wheel_bytes(extra_files={path: b"synthetic fixture"})

    with pytest.raises(ProfileBootstrapError):
        _verify_wheel_contents(wheel, "tiny-package", "1.2.3")


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
        return b"pypi fixture body"

    def close(self) -> None:
        self.closed = True


class _ProbeOpener:
    def __init__(
        self,
        response: _ProbeResponse | None = None,
        error: Exception | None = None,
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


def test_network_probe_requires_reachability_for_allowed_and_failure_for_denied() -> None:
    allowed_response = _ProbeResponse("https://pypi.org/simple/pytest/")
    allowed_opener = _ProbeOpener(allowed_response)
    receipt = probe_network("allowed", opener=allowed_opener)

    assert receipt.observed_allowed
    assert allowed_opener.url == "https://pypi.org/simple/pytest/"
    assert allowed_opener.timeout == 2.0
    assert allowed_response.read_sizes == [256]
    assert allowed_response.closed

    reachable_response = _ProbeResponse("https://pypi.org/simple/pytest/")
    with pytest.raises(ProfileBootstrapError, match="egress_not_denied"):
        probe_network("denied", opener=_ProbeOpener(reachable_response))
    assert reachable_response.closed

    denied = probe_network("denied", opener=_ProbeOpener(error=urllib.error.URLError("blocked")))
    assert not denied.observed_allowed


def test_network_probe_rejects_redirected_allowed_response() -> None:
    response = _ProbeResponse("https://attacker.example/redirected")
    with pytest.raises(ProfileBootstrapError, match="pypi_probe_failed"):
        probe_network("allowed", opener=_ProbeOpener(response))


def _rename_wheel(
    content: bytes,
    name: str,
    version: str,
    *,
    extra_files: dict[str, bytes] | None = None,
) -> bytes:
    # Pytest fixture metadata uses a different identity while preserving the
    # same deliberately minimal pure-wheel shape.
    dist_info = f"{name.replace('-', '_')}-{version}.dist-info"
    old_dist_info = "tiny_package-1.2.3.dist-info"
    files: dict[str, bytes] = {}
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        for path in archive.namelist():
            if path.endswith("/METADATA"):
                payload = (
                    archive.read(path)
                    .replace(b"tiny-package", name.encode())
                    .replace(b"1.2.3", version.encode())
                )
                files[f"{dist_info}/METADATA"] = payload
            elif path.endswith("/WHEEL"):
                files[f"{dist_info}/WHEEL"] = archive.read(path)
            elif path.endswith("/RECORD"):
                continue
            else:
                installed_path = path
                if path.startswith("tiny_package/"):
                    installed_path = (
                        f"{name.replace('-', '_')}/{path.removeprefix('tiny_package/')}"
                    )
                files[installed_path] = archive.read(path)
    if extra_files:
        files.update(extra_files)
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    record_path = f"{dist_info}/RECORD"
    for path, payload in sorted(files.items()):
        digest = base64.urlsafe_b64encode(hashlib.sha256(payload).digest())
        writer.writerow((path, f"sha256={digest.rstrip(b'=').decode()}", len(payload)))
    writer.writerow((record_path, "", ""))
    files[record_path] = record.getvalue().encode()
    renamed = io.BytesIO()
    with zipfile.ZipFile(renamed, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path, payload in sorted(files.items()):
            archive.writestr(path, payload)
    assert old_dist_info not in files
    return renamed.getvalue()


def test_recipe_keeps_bootstrap_isolated_and_prevents_pytest_cwd_shadowing() -> None:
    assert bootstrap_argv("verify-runtime") == (
        PYTHON_EXECUTABLE,
        "-I",
        "-S",
        "/opt/continuum/profile_bootstrap.py",
        "verify-runtime",
    )
    assert network_probe_argv(expected="denied")[-2:] == ("--expected", "denied")
    assert install_argv(lockfile_sha256="b" * 64)[-2:] == ("--lockfile-sha256", "b" * 64)
    assert pytest_argv() == (PYTHON_EXECUTABLE, "-P", "-S", "-m", "pytest", "-q")
    assert pytest_environment()["PYTHONPATH"].split(":") == [
        "/opt/continuum/deps",
        "/vercel/sandbox/workspace/src",
        "/vercel/sandbox/workspace",
    ]
