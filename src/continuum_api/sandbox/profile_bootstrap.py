"""Trusted Python-profile setup commands, copied into the immutable sandbox image.

This module uses only the standard library. Its CLI runs from the image-owned
`/opt/continuum` directory with base Python `-I -S`; repository files are data
until the final, explicitly bounded test command.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import http.client
import importlib.util
import io
import json
import os
import platform
import re
import shutil
import stat
import struct
import subprocess
import sys
import tempfile
import unicodedata
import urllib.error
import urllib.request
import zipfile
import zlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from email.parser import BytesParser
from pathlib import Path
from typing import Any, Literal, cast

PROFILE_VERSION = "1"
TARGET_PYTHON = (3, 13, 12)
TARGET_MACHINE = "x86_64"

CONTROL_DIRECTORY = Path("/opt/continuum")
BOOTSTRAP_PATH = CONTROL_DIRECTORY / "profile_bootstrap.py"
TRUSTED_ARCHIVER_PATH = CONTROL_DIRECTORY / "trusted" / "archives.py"
ARCHIVE_PATH = CONTROL_DIRECTORY / "input" / "source.tar.gz"
WHEELHOUSE_PATH = CONTROL_DIRECTORY / "wheels"
DEPENDENCY_TARGET_PATH = CONTROL_DIRECTORY / "deps"
WORKSPACE_PATH = Path("/vercel/sandbox/workspace")
LOCKFILE_PATH = WORKSPACE_PATH / "requirements.lock"

PYPI_INDEX_URL = "https://pypi.org/simple"
PYPI_PROBE_URL = "https://pypi.org/simple/pytest/"

MAX_ARCHIVE_BYTES = 10 * 1024 * 1024
MAX_LOCKFILE_BYTES = 256 * 1024
MAX_LOCKED_PACKAGES = 24
MAX_WHEEL_BYTES = 25 * 1024 * 1024
MAX_TOTAL_WHEEL_BYTES = 100 * 1024 * 1024
MAX_WHEEL_EXPANDED_BYTES = 50 * 1024 * 1024
MAX_WHEEL_ENTRIES = 10_000
MAX_WHEEL_METADATA_BYTES = 1024 * 1024
MAX_PIP_SECONDS = 120
PROBE_TIMEOUT_SECONDS = 2.0
PROBE_RESPONSE_BYTES = 256

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ZIP_LOCAL_HEADER = struct.Struct("<4s2B4HL2L2H")
_ZIP_LOCAL_HEADER_SIZE = 30
_ZIP_LOCAL_SIGNATURE = b"PK\x03\x04"
_LOCK_LINE = re.compile(
    r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)=="
    r"(?P<version>[A-Za-z0-9][A-Za-z0-9.!+_-]{0,127})"
    r"(?P<hashes>(?:\s+--hash=sha256:[0-9a-f]{64})+)$"
)
_NATIVE_SUFFIXES = frozenset(
    {
        ".a",
        ".dll",
        ".dylib",
        ".node",
        ".o",
        ".obj",
        ".pyd",
        ".so",
    }
)
_CONTROL_BASENAMES = frozenset(
    {"archives.py", "profile_bootstrap.py", "python_bootstrap_recipe.py"}
)


class ProfileBootstrapError(RuntimeError):
    """A stable, redacted failure code from trusted profile preparation."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class LockedPackage:
    name: str
    version: str
    sha256_hashes: frozenset[str]


@dataclass(frozen=True, slots=True)
class VerifiedWheel:
    filename: str
    name: str
    version: str
    sha256: str
    compressed_bytes: int
    expanded_bytes: int
    installed_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExtractionReceipt:
    archive_sha256: str
    tree_sha256: str
    requirements_lock_sha256: str
    file_count: int
    source_bytes: int


@dataclass(frozen=True, slots=True)
class ProbeReceipt:
    expected: Literal["allowed", "denied"]
    observed_allowed: bool


def verify_runtime_identity() -> dict[str, str]:
    """Check the fixed interpreter/platform without importing site or workspace code."""
    if (
        sys.implementation.name != "cpython"
        or tuple(sys.version_info[:3]) != TARGET_PYTHON
        or platform.system() != "Linux"
        or platform.machine().lower() not in {TARGET_MACHINE, "amd64"}
    ):
        raise ProfileBootstrapError("python_profile_runtime_identity_invalid")
    if not sys.flags.isolated or not sys.flags.no_site:
        raise ProfileBootstrapError("python_profile_trusted_mode_required")
    return {
        "implementation": "CPython",
        "machine": TARGET_MACHINE,
        "profile_version": PROFILE_VERSION,
        "python_version": ".".join(str(part) for part in TARGET_PYTHON),
    }


def parse_requirements_lock(content: bytes) -> tuple[LockedPackage, ...]:
    """Parse the narrow, flat T13 requirements-lock format without packaging."""
    if not isinstance(content, bytes) or len(content) > MAX_LOCKFILE_BYTES:
        raise ProfileBootstrapError("python_profile_lockfile_size_invalid")
    try:
        text = content.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise ProfileBootstrapError("python_profile_lockfile_encoding_invalid") from None

    packages: list[LockedPackage] = []
    names: set[str] = set()
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _LOCK_LINE.fullmatch(line)
        if match is None:
            raise ProfileBootstrapError("python_profile_lockfile_unsupported")
        name = _canonical_package_name(match.group("name"))
        if name in names:
            raise ProfileBootstrapError("python_profile_lockfile_duplicate")
        names.add(name)
        hashes = frozenset(re.findall(r"--hash=sha256:([0-9a-f]{64})", match.group("hashes")))
        if not hashes:
            raise ProfileBootstrapError("python_profile_lockfile_unhashed")
        packages.append(LockedPackage(name, match.group("version"), hashes))
        if len(packages) > MAX_LOCKED_PACKAGES:
            raise ProfileBootstrapError("python_profile_lockfile_package_limit")
    if not packages or "pytest" not in names:
        raise ProfileBootstrapError("python_profile_lockfile_missing_pytest")
    return tuple(packages)


def validate_lockfile(content: bytes, expected_sha256: str) -> tuple[LockedPackage, ...]:
    _validate_digest(expected_sha256, "python_profile_lockfile_digest_invalid")
    actual_sha256 = hashlib.sha256(content).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ProfileBootstrapError("python_profile_lockfile_digest_mismatch")
    return parse_requirements_lock(content)


def extract_verified_archive(
    archive: bytes,
    *,
    expected_archive_sha256: str,
    expected_tree_sha256: str,
    expected_lockfile_sha256: str,
    destination: str | os.PathLike[str],
    verifier: Callable[..., Any] | None = None,
) -> ExtractionReceipt:
    """Verify the complete archive before atomically publishing source files."""
    _validate_digest(expected_archive_sha256, "python_profile_archive_digest_invalid")
    _validate_digest(expected_tree_sha256, "python_profile_tree_digest_invalid")
    _validate_digest(expected_lockfile_sha256, "python_profile_lockfile_digest_invalid")
    if not isinstance(archive, bytes) or not 1 <= len(archive) <= MAX_ARCHIVE_BYTES:
        raise ProfileBootstrapError("python_profile_archive_size_invalid")

    verified = (verifier or _load_trusted_archive_verifier())(
        archive,
        expected_archive_sha256=expected_archive_sha256,
        expected_tree_sha256=expected_tree_sha256,
    )
    if verified.archive_sha256 != expected_archive_sha256:
        raise ProfileBootstrapError("python_profile_archive_digest_mismatch")
    if verified.manifest.tree_sha256 != expected_tree_sha256:
        raise ProfileBootstrapError("python_profile_tree_digest_mismatch")

    lockfiles = [source for source in verified.files if source.path == "requirements.lock"]
    if len(lockfiles) != 1:
        raise ProfileBootstrapError("python_profile_lockfile_missing")
    lockfile = lockfiles[0].content
    validate_lockfile(lockfile, expected_lockfile_sha256)

    source_bytes = sum(len(source.content) for source in verified.files)
    if source_bytes > 50 * 1024 * 1024 or len(verified.files) > 5_000:
        raise ProfileBootstrapError("python_profile_archive_metadata_invalid")

    destination_path = Path(destination)
    stage_path: Path | None = None
    try:
        _require_empty_or_missing_directory(destination_path)
        stage_path = Path(
            tempfile.mkdtemp(prefix=".continuum-python-source-", dir=destination_path.parent)
        )
        _write_source_files(stage_path, verified.files)
        # Replace only the empty target checked above. The new workspace is
        # visible all at once; a partial extraction is never run as source.
        if destination_path.exists():
            destination_path.rmdir()
        os.rename(stage_path, destination_path)
        stage_path = None
    except ProfileBootstrapError:
        raise
    except OSError:
        raise ProfileBootstrapError("python_profile_workspace_extract_failed") from None
    finally:
        if stage_path is not None:
            shutil.rmtree(stage_path, ignore_errors=True)

    return ExtractionReceipt(
        archive_sha256=verified.archive_sha256,
        tree_sha256=verified.manifest.tree_sha256,
        requirements_lock_sha256=expected_lockfile_sha256,
        file_count=len(verified.files),
        source_bytes=source_bytes,
    )


def verify_wheelhouse(
    lockfile_content: bytes,
    *,
    expected_lockfile_sha256: str,
    wheelhouse: str | os.PathLike[str],
) -> tuple[VerifiedWheel, ...]:
    """Check each downloaded wheel before pip can install any of them."""
    packages = parse_requirements_lock(lockfile_content)
    if hashlib.sha256(lockfile_content).hexdigest() != expected_lockfile_sha256:
        raise ProfileBootstrapError("python_profile_lockfile_digest_mismatch")
    wheelhouse_path = Path(wheelhouse)
    try:
        wheelhouse_stat = wheelhouse_path.lstat()
        if not stat.S_ISDIR(wheelhouse_stat.st_mode) or stat.S_ISLNK(wheelhouse_stat.st_mode):
            raise ProfileBootstrapError("python_profile_wheelhouse_invalid")
        children = list(os.scandir(wheelhouse_path))
    except FileNotFoundError:
        raise ProfileBootstrapError("python_profile_wheelhouse_missing") from None
    except OSError:
        raise ProfileBootstrapError("python_profile_wheelhouse_unreadable") from None

    expected = {package.name: package for package in packages}
    found: dict[str, VerifiedWheel] = {}
    installed_files: set[str] = set()
    total_bytes = 0
    for child in children:
        if not child.name.endswith(".whl"):
            raise ProfileBootstrapError("python_profile_wheelhouse_entry_unsupported")
        try:
            if not child.is_file(follow_symlinks=False):
                raise ProfileBootstrapError("python_profile_wheelhouse_entry_unsupported")
            content = _read_regular_file(Path(child.path), MAX_WHEEL_BYTES)
        except ProfileBootstrapError:
            raise
        except OSError:
            raise ProfileBootstrapError("python_profile_wheel_unreadable") from None
        total_bytes += len(content)
        if total_bytes > MAX_TOTAL_WHEEL_BYTES:
            raise ProfileBootstrapError("python_profile_wheelhouse_size_limit")

        name, version = _parse_pure_wheel_filename(child.name)
        locked = expected.get(name)
        if locked is None or locked.version != version:
            raise ProfileBootstrapError("python_profile_wheel_identity_mismatch")
        wheel_sha256 = hashlib.sha256(content).hexdigest()
        if wheel_sha256 not in locked.sha256_hashes:
            raise ProfileBootstrapError("python_profile_wheel_digest_mismatch")
        expanded_bytes, installed_paths = _verify_wheel_contents(content, name, version)
        _reject_cross_wheel_path_collisions(installed_files, installed_paths)
        installed_files.update(installed_paths)
        if name in found:
            raise ProfileBootstrapError("python_profile_wheel_duplicate")
        found[name] = VerifiedWheel(
            filename=child.name,
            name=name,
            version=version,
            sha256=wheel_sha256,
            compressed_bytes=len(content),
            expanded_bytes=expanded_bytes,
            installed_paths=installed_paths,
        )
    if set(found) != set(expected):
        raise ProfileBootstrapError("python_profile_wheel_set_mismatch")
    return tuple(found[name] for name in sorted(found))


def probe_network(
    expected: Literal["allowed", "denied"],
    *,
    opener: Any | None = None,
) -> ProbeReceipt:
    """Check one fixed PyPI URL with no proxy, redirect, or unbounded read."""
    if expected not in {"allowed", "denied"}:
        raise ProfileBootstrapError("python_profile_network_expectation_invalid")
    selected_opener = opener or _pypi_opener()
    request = urllib.request.Request(
        PYPI_PROBE_URL,
        headers={"accept": "text/html", "user-agent": "Continuum-Python-Profile/1"},
        method="GET",
    )
    try:
        response = selected_opener.open(request, timeout=PROBE_TIMEOUT_SECONDS)
    except urllib.error.HTTPError:
        # Any HTTP response proves the provider connection is reachable.
        if expected == "denied":
            raise ProfileBootstrapError("python_profile_egress_not_denied") from None
        raise ProfileBootstrapError("python_profile_pypi_probe_failed") from None
    except (urllib.error.URLError, TimeoutError, OSError):
        if expected == "denied":
            return ProbeReceipt(expected="denied", observed_allowed=False)
        raise ProfileBootstrapError("python_profile_pypi_probe_failed") from None

    try:
        status = getattr(response, "status", None)
        final_url = response.geturl()
        # Read only a small prefix to prove the response body is readable.
        response.read(PROBE_RESPONSE_BYTES)
    finally:
        response.close()
    if expected == "denied":
        raise ProfileBootstrapError("python_profile_egress_not_denied")
    if status != 200 or final_url != PYPI_PROBE_URL:
        raise ProfileBootstrapError("python_profile_pypi_probe_failed")
    return ProbeReceipt(expected="allowed", observed_allowed=True)


def _verify_wheel_contents(content: bytes, name: str, version: str) -> tuple[int, tuple[str, ...]]:
    try:
        with zipfile.ZipFile(io.BytesIO(content), mode="r") as archive:
            entries = archive.infolist()
            if not entries or len(entries) > MAX_WHEEL_ENTRIES:
                raise ProfileBootstrapError("python_profile_wheel_entry_limit")
            total_expanded = 0
            file_paths: set[str] = set()
            dir_paths: set[str] = set()
            installed_paths: set[str] = set()
            expected_dist_info = f"{name.replace('-', '_')}-{version}.dist-info"
            metadata_path = f"{expected_dist_info}/METADATA"
            wheel_path = f"{expected_dist_info}/WHEEL"
            record_path = f"{expected_dist_info}/RECORD"

            for info in entries:
                if getattr(info, "orig_filename", info.filename) != info.filename:
                    raise ProfileBootstrapError("python_profile_wheel_path_invalid")
                path = info.filename
                components = _validate_wheel_path(path)
                normalized = "/".join(components)
                if normalized.casefold() in file_paths or normalized.casefold() in dir_paths:
                    raise ProfileBootstrapError("python_profile_wheel_path_collision")
                mode = (info.external_attr >> 16) & 0xFFFF
                file_type = stat.S_IFMT(mode)
                is_dir = info.is_dir()
                if file_type == stat.S_IFLNK or file_type not in {
                    0,
                    stat.S_IFREG,
                    stat.S_IFDIR,
                }:
                    raise ProfileBootstrapError("python_profile_wheel_link_unsupported")
                if (file_type == stat.S_IFDIR) != is_dir and file_type != 0:
                    raise ProfileBootstrapError("python_profile_wheel_entry_unsupported")
                if mode & (stat.S_ISUID | stat.S_ISGID):
                    raise ProfileBootstrapError("python_profile_wheel_mode_unsupported")
                if is_dir:
                    dir_paths.add(normalized.casefold())
                else:
                    file_paths.add(normalized.casefold())
                    _validate_wheel_install_location(
                        components,
                        expected_data_directory=f"{name.replace('-', '_')}-{version}.data",
                    )
                    installed_components = (
                        components[2:] if components[0].endswith(".data") else components
                    )
                    installed_path = "/".join(installed_components).casefold()
                    if installed_path in installed_paths:
                        raise ProfileBootstrapError("python_profile_wheel_path_collision")
                    installed_paths.add(installed_path)
                    suffix = Path(components[-1]).suffix.casefold()
                    if suffix in _NATIVE_SUFFIXES:
                        raise ProfileBootstrapError("python_profile_wheel_native_unsupported")
                    basename = components[-1].casefold()
                    if basename.endswith(".pth") or basename in {
                        "sitecustomize.py",
                        "usercustomize.py",
                        *_CONTROL_BASENAMES,
                    }:
                        raise ProfileBootstrapError("python_profile_wheel_startup_unsupported")
                total_expanded += info.file_size
                if info.file_size < 0 or total_expanded > MAX_WHEEL_EXPANDED_BYTES:
                    raise ProfileBootstrapError("python_profile_wheel_expansion_limit")
                if info.file_size > MAX_WHEEL_METADATA_BYTES and path in {
                    metadata_path,
                    wheel_path,
                    record_path,
                }:
                    raise ProfileBootstrapError("python_profile_wheel_metadata_size_invalid")
                if info.flag_bits & 0x1:
                    raise ProfileBootstrapError("python_profile_wheel_encrypted_unsupported")
                if info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}:
                    raise ProfileBootstrapError("python_profile_wheel_compression_unsupported")

            for path in file_paths | dir_paths:
                parent = path.rsplit("/", 1)[0] if "/" in path else ""
                while parent:
                    if parent in file_paths:
                        raise ProfileBootstrapError("python_profile_wheel_path_collision")
                    parent = parent.rsplit("/", 1)[0] if "/" in parent else ""

            by_exact_name = {info.filename: info for info in entries if not info.is_dir()}
            for required in (metadata_path, wheel_path, record_path):
                if required not in by_exact_name:
                    raise ProfileBootstrapError("python_profile_wheel_metadata_missing")
            verified_contents: dict[str, bytes] = {}
            actual_expanded = 0
            for path, info in by_exact_name.items():
                remaining = MAX_WHEEL_EXPANDED_BYTES - actual_expanded
                member_limit = min(remaining, MAX_WHEEL_EXPANDED_BYTES)
                if path in {metadata_path, wheel_path, record_path}:
                    member_limit = min(member_limit, MAX_WHEEL_METADATA_BYTES)
                member_content = _read_zip_member_bounded(
                    archive,
                    info,
                    max_bytes=member_limit,
                )
                if len(member_content) != info.file_size:
                    raise ProfileBootstrapError("python_profile_wheel_size_mismatch")
                actual_expanded += len(member_content)
                if actual_expanded > MAX_WHEEL_EXPANDED_BYTES:
                    raise ProfileBootstrapError("python_profile_wheel_expansion_limit")
                verified_contents[path] = member_content
            metadata_bytes = verified_contents[metadata_path]
            wheel_bytes = verified_contents[wheel_path]
            record_bytes = verified_contents[record_path]
            metadata = BytesParser().parsebytes(metadata_bytes)
            if (
                _canonical_package_name(metadata.get("Name", "")) != name
                or metadata.get("Version") != version
            ):
                raise ProfileBootstrapError("python_profile_wheel_metadata_mismatch")
            wheel_metadata = BytesParser().parsebytes(wheel_bytes)
            if (wheel_metadata.get("Root-Is-Purelib") or "").lower() != "true":
                raise ProfileBootstrapError("python_profile_wheel_not_purelib")
            tags = wheel_metadata.get_all("Tag", [])
            if tags != ["py3-none-any"]:
                raise ProfileBootstrapError("python_profile_wheel_tag_unsupported")
            _verify_wheel_record(record_bytes, verified_contents, file_paths)
            _reject_cross_wheel_path_collisions(set(), tuple(installed_paths))
            return actual_expanded, tuple(sorted(installed_paths))
    except ProfileBootstrapError:
        raise
    except (OSError, ValueError, zipfile.BadZipFile, RuntimeError):
        raise ProfileBootstrapError("python_profile_wheel_zip_invalid") from None


def _verify_wheel_record(
    record_bytes: bytes,
    contents: Mapping[str, bytes],
    file_paths: set[str],
) -> None:
    try:
        text = record_bytes.decode("utf-8", errors="strict")
        rows = list(csv.reader(io.StringIO(text, newline="")))
    except (UnicodeDecodeError, csv.Error):
        raise ProfileBootstrapError("python_profile_wheel_record_invalid") from None
    if not rows:
        raise ProfileBootstrapError("python_profile_wheel_record_invalid")
    seen: set[str] = set()
    for row in rows:
        if len(row) != 3 or not row[0]:
            raise ProfileBootstrapError("python_profile_wheel_record_invalid")
        components = _validate_wheel_path(row[0])
        path = "/".join(components).casefold()
        if path in seen:
            raise ProfileBootstrapError("python_profile_wheel_record_invalid")
        seen.add(path)
        if path not in file_paths:
            raise ProfileBootstrapError("python_profile_wheel_record_invalid")
        if path.endswith(".dist-info/record"):
            if row[1] or row[2]:
                raise ProfileBootstrapError("python_profile_wheel_record_invalid")
            continue
        if not row[1].startswith("sha256=") or not row[2].isdigit():
            raise ProfileBootstrapError("python_profile_wheel_record_invalid")
        content = contents.get(row[0])
        encoded_digest = row[1][len("sha256=") :]
        if content is None or not re.fullmatch(r"[A-Za-z0-9_-]{43}", encoded_digest):
            raise ProfileBootstrapError("python_profile_wheel_record_invalid")
        expected_digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest())
        expected_encoded_digest = expected_digest.rstrip(b"=").decode("ascii")
        if len(content) != int(row[2]) or encoded_digest != expected_encoded_digest:
            raise ProfileBootstrapError("python_profile_wheel_record_mismatch")
    if len(seen) != len(file_paths):
        raise ProfileBootstrapError("python_profile_wheel_record_invalid")


def _reject_cross_wheel_path_collisions(
    existing_files: set[str], candidate_files: Sequence[str]
) -> None:
    occupied = set(existing_files)
    for candidate in candidate_files:
        parent = candidate.rsplit("/", 1)[0] if "/" in candidate else ""
        while parent:
            if parent in occupied:
                raise ProfileBootstrapError("python_profile_wheel_path_collision")
            parent = parent.rsplit("/", 1)[0] if "/" in parent else ""
        if candidate in occupied or any(
            existing.startswith(candidate + "/") for existing in occupied
        ):
            raise ProfileBootstrapError("python_profile_wheel_path_collision")
        occupied.add(candidate)


def _read_zip_member_bounded(
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    *,
    max_bytes: int,
) -> bytes:
    """Inflate a ZIP member from its raw deflate stream with an actual-byte cap.

    `ZipExtFile.read()` trusts the central-directory uncompressed size. A forged
    small size can make it truncate output after zlib has already expanded a
    larger stream. Decode the bounded compressed range directly and validate
    its actual length and CRC before returning bytes to the wheel parser.
    """
    if max_bytes < 0 or info.compress_size < 0 or info.compress_size > MAX_WHEEL_BYTES:
        raise ProfileBootstrapError("python_profile_wheel_expansion_limit")
    file_pointer = archive.fp
    if file_pointer is None:
        raise ProfileBootstrapError("python_profile_wheel_zip_invalid")
    try:
        file_pointer.seek(info.header_offset)
        header = file_pointer.read(_ZIP_LOCAL_HEADER_SIZE)
        if len(header) != _ZIP_LOCAL_HEADER_SIZE:
            raise ProfileBootstrapError("python_profile_wheel_zip_invalid")
        fields = _ZIP_LOCAL_HEADER.unpack(header)
        if fields[0] != _ZIP_LOCAL_SIGNATURE:
            raise ProfileBootstrapError("python_profile_wheel_zip_invalid")
        local_flags = fields[3]
        local_compression = fields[4]
        filename_length = fields[10]
        extra_length = fields[11]
        filename_bytes = file_pointer.read(filename_length)
        extra_bytes = file_pointer.read(extra_length)
        if len(filename_bytes) != filename_length or len(extra_bytes) != extra_length:
            raise ProfileBootstrapError("python_profile_wheel_zip_invalid")
        filename_encoding = "utf-8" if local_flags & 0x800 else "cp437"
        if (
            filename_bytes.decode(filename_encoding) != info.orig_filename
            or local_flags != info.flag_bits
            or local_compression != info.compress_type
        ):
            raise ProfileBootstrapError("python_profile_wheel_zip_invalid")
        data_start = info.header_offset + _ZIP_LOCAL_HEADER_SIZE + filename_length + extra_length
        data_end = data_start + info.compress_size
        next_record = getattr(info, "_end_offset", archive.start_dir)
        if data_end > next_record or data_end > archive.start_dir:
            raise ProfileBootstrapError("python_profile_wheel_zip_invalid")
        file_pointer.seek(data_start)
        compressed = file_pointer.read(info.compress_size)
        if len(compressed) != info.compress_size:
            raise ProfileBootstrapError("python_profile_wheel_zip_invalid")

        if info.compress_type == zipfile.ZIP_STORED:
            expanded = compressed
        elif info.compress_type == zipfile.ZIP_DEFLATED:
            decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
            output = bytearray()
            for offset in range(0, len(compressed), 64 * 1024):
                pending = compressed[offset : offset + 64 * 1024]
                while pending:
                    allowance = max_bytes + 1 - len(output)
                    if allowance <= 0:
                        raise ProfileBootstrapError("python_profile_wheel_expansion_limit")
                    previous_pending = len(pending)
                    output.extend(decompressor.decompress(pending, allowance))
                    if len(output) > max_bytes:
                        raise ProfileBootstrapError("python_profile_wheel_expansion_limit")
                    pending = decompressor.unconsumed_tail
                    if len(pending) >= previous_pending and not decompressor.eof:
                        raise ProfileBootstrapError("python_profile_wheel_zip_invalid")
                    if decompressor.unused_data:
                        raise ProfileBootstrapError("python_profile_wheel_zip_invalid")
            if not decompressor.eof or decompressor.unconsumed_tail or decompressor.unused_data:
                raise ProfileBootstrapError("python_profile_wheel_zip_invalid")
            expanded = bytes(output)
        else:
            raise ProfileBootstrapError("python_profile_wheel_compression_unsupported")
    except ProfileBootstrapError:
        raise
    except (OSError, UnicodeDecodeError, ValueError, zlib.error, struct.error):
        raise ProfileBootstrapError("python_profile_wheel_zip_invalid") from None
    if len(expanded) > max_bytes:
        raise ProfileBootstrapError("python_profile_wheel_expansion_limit")
    if zlib.crc32(expanded) & 0xFFFFFFFF != info.CRC:
        raise ProfileBootstrapError("python_profile_wheel_crc_mismatch")
    return expanded


def _validate_wheel_install_location(
    components: tuple[str, ...], *, expected_data_directory: str
) -> None:
    data_indices = [index for index, part in enumerate(components) if part.endswith(".data")]
    if not data_indices:
        return
    if len(data_indices) != 1:
        raise ProfileBootstrapError("python_profile_wheel_data_path_unsupported")
    index = data_indices[0]
    if (
        index != 0
        or components[0] != expected_data_directory
        or len(components) < 3
        or components[1] != "purelib"
    ):
        # `.data` categories are installer spread paths. Only purelib remains
        # inside our explicit dependency target; scripts/data/headers are denied.
        raise ProfileBootstrapError("python_profile_wheel_data_path_unsupported")


def _parse_pure_wheel_filename(filename: str) -> tuple[str, str]:
    if not filename.endswith(".whl"):
        raise ProfileBootstrapError("python_profile_wheel_filename_invalid")
    parts = filename[:-4].split("-")
    if len(parts) != 5 or parts[2:] != ["py3", "none", "any"]:
        raise ProfileBootstrapError("python_profile_wheel_tag_unsupported")
    name = _canonical_package_name(parts[0])
    version = parts[1]
    if not name or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.!+_-]{0,127}", version) is None:
        raise ProfileBootstrapError("python_profile_wheel_filename_invalid")
    return name, version


def _validate_wheel_path(path: str) -> tuple[str, ...]:
    if (
        not path
        or path.startswith(("/", "\\"))
        or "\\" in path
        or "\0" in path
        or re.match(r"^[A-Za-z]:", path)
        or unicodedata.normalize("NFC", path) != path
    ):
        raise ProfileBootstrapError("python_profile_wheel_path_invalid")
    is_dir = path.endswith("/")
    components = tuple(path[:-1].split("/") if is_dir else path.split("/"))
    if not components or any(part in {"", ".", ".."} for part in components):
        raise ProfileBootstrapError("python_profile_wheel_path_invalid")
    if len(path.encode("utf-8")) > 255:
        raise ProfileBootstrapError("python_profile_wheel_path_invalid")
    return components


def _write_source_files(stage: Path, files: Iterable[Any]) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        root_fd = os.open(stage, flags)
    except OSError:
        raise ProfileBootstrapError("python_profile_workspace_create_failed") from None
    try:
        for source in files:
            components = _validate_wheel_path(source.path)
            if (
                not isinstance(source.content, bytes)
                or type(source.mode) is not int
                or source.mode not in {0o644, 0o755}
            ):
                raise ProfileBootstrapError("python_profile_source_entry_invalid")
            parent_fd = os.dup(root_fd)
            try:
                for component in components[:-1]:
                    try:
                        os.mkdir(component, mode=0o755, dir_fd=parent_fd)
                    except FileExistsError:
                        pass
                    child_fd = os.open(component, flags, dir_fd=parent_fd)
                    os.close(parent_fd)
                    parent_fd = child_fd
                file_fd = os.open(
                    components[-1],
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    mode=source.mode,
                    dir_fd=parent_fd,
                )
                try:
                    view = memoryview(source.content)
                    while view:
                        written = os.write(file_fd, view)
                        if written <= 0:
                            raise OSError("short file write")
                        view = view[written:]
                    os.fchmod(file_fd, source.mode)
                finally:
                    os.close(file_fd)
            finally:
                os.close(parent_fd)
    except ProfileBootstrapError:
        raise
    except OSError:
        raise ProfileBootstrapError("python_profile_workspace_extract_failed") from None
    finally:
        os.close(root_fd)


def _require_empty_or_missing_directory(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        if not path.parent.is_dir():
            raise ProfileBootstrapError("python_profile_workspace_parent_invalid") from None
        return
    except OSError:
        raise ProfileBootstrapError("python_profile_workspace_unavailable") from None
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise ProfileBootstrapError("python_profile_workspace_not_directory")
    try:
        with os.scandir(path) as entries:
            if next(entries, None) is not None:
                raise ProfileBootstrapError("python_profile_workspace_not_empty")
    except ProfileBootstrapError:
        raise
    except OSError:
        raise ProfileBootstrapError("python_profile_workspace_unavailable") from None


def _read_regular_file(path: Path, max_bytes: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise ProfileBootstrapError("python_profile_file_unavailable") from None
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > max_bytes:
            raise ProfileBootstrapError("python_profile_file_invalid")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > max_bytes:
                raise ProfileBootstrapError("python_profile_file_size_limit")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _load_trusted_archive_verifier() -> Callable[..., Any]:
    try:
        file_stat = TRUSTED_ARCHIVER_PATH.lstat()
        if stat.S_ISLNK(file_stat.st_mode) or not stat.S_ISREG(file_stat.st_mode):
            raise ProfileBootstrapError("python_profile_archive_verifier_invalid")
        spec = importlib.util.spec_from_file_location(
            "continuum_image_trusted_archives", TRUSTED_ARCHIVER_PATH
        )
        if spec is None or spec.loader is None:
            raise ProfileBootstrapError("python_profile_archive_verifier_unavailable")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        verifier = getattr(module, "verify_archive", None)
        if not callable(verifier):
            raise ProfileBootstrapError("python_profile_archive_verifier_invalid")
        return cast(Callable[..., Any], verifier)
    except ProfileBootstrapError:
        raise
    except Exception:
        raise ProfileBootstrapError("python_profile_archive_verifier_unavailable") from None


def _pypi_opener() -> urllib.request.OpenerDirector:
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


def _canonical_package_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _validate_digest(digest: str, error_code: str) -> None:
    if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
        raise ProfileBootstrapError(error_code)


def _subprocess_environment() -> dict[str, str]:
    return {
        "HOME": str(CONTROL_DIRECTORY / "home"),
        "LANG": "C.UTF-8",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "PYTHONDONTWRITEBYTECODE": "1",
    }


def _run_pip(arguments: Sequence[str], *, cwd: Path) -> None:
    if sys.executable != "/usr/local/bin/python3.13":
        raise ProfileBootstrapError("python_profile_runtime_identity_invalid")
    argv = (sys.executable, "-I", "-m", "pip", "--isolated", *arguments)
    try:
        result = subprocess.run(
            argv,
            cwd=cwd,
            env=_subprocess_environment(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=MAX_PIP_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise ProfileBootstrapError("python_profile_pip_timeout") from None
    except OSError:
        raise ProfileBootstrapError("python_profile_pip_unavailable") from None
    if result.returncode != 0:
        raise ProfileBootstrapError("python_profile_pip_failed")


def download_and_verify_wheels(lockfile_sha256: str) -> tuple[VerifiedWheel, ...]:
    lockfile_content = _read_regular_file(LOCKFILE_PATH, MAX_LOCKFILE_BYTES)
    validate_lockfile(lockfile_content, lockfile_sha256)
    _require_empty_or_missing_directory(WHEELHOUSE_PATH)
    WHEELHOUSE_PATH.mkdir(mode=0o700, parents=False, exist_ok=True)
    _run_pip(
        (
            "download",
            "--require-hashes",
            "--no-deps",
            "--only-binary=:all:",
            "--no-cache-dir",
            "--disable-pip-version-check",
            "--index-url",
            PYPI_INDEX_URL,
            "--dest",
            str(WHEELHOUSE_PATH),
            "-r",
            str(LOCKFILE_PATH),
        ),
        cwd=CONTROL_DIRECTORY,
    )
    return verify_wheelhouse(
        lockfile_content,
        expected_lockfile_sha256=lockfile_sha256,
        wheelhouse=WHEELHOUSE_PATH,
    )


def install_verified_wheels(lockfile_sha256: str) -> tuple[VerifiedWheel, ...]:
    lockfile_content = _read_regular_file(LOCKFILE_PATH, MAX_LOCKFILE_BYTES)
    wheels = verify_wheelhouse(
        lockfile_content,
        expected_lockfile_sha256=lockfile_sha256,
        wheelhouse=WHEELHOUSE_PATH,
    )
    _require_empty_or_missing_directory(DEPENDENCY_TARGET_PATH)
    DEPENDENCY_TARGET_PATH.mkdir(mode=0o755, parents=False, exist_ok=True)
    _run_pip(
        (
            "install",
            "--require-hashes",
            "--no-deps",
            "--only-binary=:all:",
            "--no-cache-dir",
            "--disable-pip-version-check",
            "--no-index",
            "--find-links",
            str(WHEELHOUSE_PATH),
            "--target",
            str(DEPENDENCY_TARGET_PATH),
            "-r",
            str(LOCKFILE_PATH),
        ),
        cwd=CONTROL_DIRECTORY,
    )
    return wheels


def _emit_json(payload: Mapping[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="continuum-python-bootstrap")
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

    download = actions.add_parser("download-verify")
    download.add_argument("--lockfile-sha256", required=True)

    install = actions.add_parser("install")
    install.add_argument("--lockfile-sha256", required=True)

    selected = parser.parse_args(arguments)
    try:
        verify_runtime_identity()
        if selected.action == "verify-runtime":
            _emit_json(verify_runtime_identity())
        elif selected.action == "extract":
            archive_bytes = _read_regular_file(ARCHIVE_PATH, MAX_ARCHIVE_BYTES)
            extraction_receipt = extract_verified_archive(
                archive_bytes,
                expected_archive_sha256=selected.archive_sha256,
                expected_tree_sha256=selected.tree_sha256,
                expected_lockfile_sha256=selected.lockfile_sha256,
                destination=WORKSPACE_PATH,
            )
            _emit_json(
                {
                    "archive_sha256": extraction_receipt.archive_sha256,
                    "file_count": extraction_receipt.file_count,
                    "lockfile_sha256": extraction_receipt.requirements_lock_sha256,
                    "source_bytes": extraction_receipt.source_bytes,
                    "tree_sha256": extraction_receipt.tree_sha256,
                }
            )
        elif selected.action == "validate-lock":
            content = _read_regular_file(LOCKFILE_PATH, MAX_LOCKFILE_BYTES)
            packages = validate_lockfile(content, selected.lockfile_sha256)
            _emit_json(
                {"lockfile_sha256": selected.lockfile_sha256, "package_count": len(packages)}
            )
        elif selected.action == "probe":
            probe_receipt = probe_network(selected.expected)
            _emit_json(
                {
                    "expected": probe_receipt.expected,
                    "observed_allowed": probe_receipt.observed_allowed,
                }
            )
        elif selected.action == "download-verify":
            wheels = download_and_verify_wheels(selected.lockfile_sha256)
            _emit_json(
                {
                    "package_count": len(wheels),
                    "total_bytes": sum(wheel.compressed_bytes for wheel in wheels),
                }
            )
        elif selected.action == "install":
            wheels = install_verified_wheels(selected.lockfile_sha256)
            _emit_json({"installed_wheel_count": len(wheels)})
        else:
            raise ProfileBootstrapError("python_profile_action_unknown")
    except ProfileBootstrapError as error:
        sys.stderr.write(error.code + "\n")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
