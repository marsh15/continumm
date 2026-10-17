"""Deterministic source-only tar.gz archives with bounded verification."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
import tarfile
import unicodedata
import zlib
from collections.abc import Buffer, Iterable
from dataclasses import dataclass

MAX_COMPRESSED_BYTES = 10 * 1024 * 1024
MAX_SOURCE_BYTES = 50 * 1024 * 1024
MAX_FILES = 5_000
MAX_PATH_BYTES = 255
# File and directory path nodes; T31 additionally creates a root tree and commit.
MAX_SOURCE_TREE_NODES = 19_998
TAR_BLOCK_BYTES = 512
TAR_RECORD_BYTES = 10 * 1024
# One USTAR header plus worst-case content padding per file, plus final records.
MAX_TAR_BYTES = MAX_SOURCE_BYTES + MAX_FILES * 1024 + TAR_RECORD_BYTES
TREE_FORMAT_VERSION = 1
_TREE_DIGEST_DOMAIN = b"continuum-source-tree-v1\0"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_LFS_POINTER = b"version https://git-lfs.github.com/spec/v1"
_SOURCE_ONLY_EXCLUDED_COMPONENTS = frozenset(
    {
        ".git",
        ".hg",
        ".svn",
        "node_modules",
        ".venv",
        "venv",
        ".tox",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
        ".npm",
        ".yarn",
    }
)


class ArchiveError(RuntimeError):
    """Stable archive rejection code without source contents or OS paths."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class ArchiveFile:
    """One source file with a relative POSIX path and normalized mode."""

    path: str
    content: bytes
    mode: int = 0o644


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    path: str
    mode: int
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class ArchiveManifest:
    format_version: int
    entries: tuple[ManifestEntry, ...]
    tree_sha256: str


@dataclass(frozen=True, slots=True)
class ArchiveBundle:
    archive: bytes
    archive_sha256: str
    manifest: ArchiveManifest


@dataclass(frozen=True, slots=True)
class VerifiedArchive:
    archive_sha256: str
    manifest: ArchiveManifest
    files: tuple[ArchiveFile, ...]


class _LimitedBytesIO(io.BytesIO):
    def __init__(self, limit: int, error_code: str) -> None:
        super().__init__()
        self._limit = limit
        self._error_code = error_code

    def write(self, data: Buffer) -> int:
        if self.tell() + memoryview(data).nbytes > self._limit:
            raise ArchiveError(self._error_code)
        return super().write(data)


def create_archive(files: Iterable[ArchiveFile]) -> ArchiveBundle:
    """Build a reproducible USTAR+gzip source archive and separate tree digest."""
    normalized = _validate_files(files)
    manifest = _make_manifest(normalized)
    tar_stream = _LimitedBytesIO(MAX_TAR_BYTES, "archive_tar_size_limit")
    try:
        with tarfile.open(
            fileobj=tar_stream,
            mode="w:",
            format=tarfile.USTAR_FORMAT,
            encoding="utf-8",
            errors="strict",
        ) as archive:
            for source_file in normalized:
                info = tarfile.TarInfo(source_file.path)
                info.type = tarfile.REGTYPE
                info.mode = source_file.mode
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                info.mtime = 0
                info.size = len(source_file.content)
                info.linkname = ""
                archive.addfile(info, io.BytesIO(source_file.content))
    except ArchiveError:
        raise
    except (tarfile.TarError, OSError, ValueError, UnicodeError):
        raise ArchiveError("archive_create_failed") from None

    compressed = _LimitedBytesIO(MAX_COMPRESSED_BYTES, "archive_compressed_size_limit")
    # GzipFile mtime=0 omits wall-clock variation from the reproducible archive.
    # Source: https://docs.python.org/3.13/library/gzip.html#gzip.GzipFile
    try:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            compresslevel=9,
            fileobj=compressed,
            mtime=0,
        ) as gzip_stream:
            gzip_stream.write(tar_stream.getvalue())
    except ArchiveError:
        raise
    except (OSError, zlib.error):
        raise ArchiveError("archive_create_failed") from None
    archive_bytes = compressed.getvalue()
    return ArchiveBundle(
        archive=archive_bytes,
        archive_sha256=hashlib.sha256(archive_bytes).hexdigest(),
        manifest=manifest,
    )


def verify_archive(
    archive: bytes,
    *,
    expected_archive_sha256: str | None = None,
    expected_tree_sha256: str | None = None,
    expected_manifest: ArchiveManifest | None = None,
) -> VerifiedArchive:
    """Boundedly validate gzip/USTAR bytes and return immutable source entries."""
    if not isinstance(archive, bytes) or not archive:
        raise ArchiveError("archive_invalid")
    if len(archive) > MAX_COMPRESSED_BYTES:
        raise ArchiveError("archive_compressed_size_limit")
    archive_digest = hashlib.sha256(archive).hexdigest()
    _check_expected_digest(expected_archive_sha256, archive_digest, "archive_digest")

    tar_bytes = _inflate_bounded(archive)
    files = _parse_ustar(tar_bytes)
    normalized = _validate_files(files)
    manifest = _make_manifest(normalized)
    _check_expected_digest(expected_tree_sha256, manifest.tree_sha256, "source_tree_digest")
    if expected_manifest is not None and expected_manifest != manifest:
        raise ArchiveError("archive_manifest_mismatch")
    return VerifiedArchive(archive_digest, manifest, normalized)


def _check_expected_digest(expected: str | None, actual: str, label: str) -> None:
    if expected is None:
        return
    if not isinstance(expected, str) or _SHA256_PATTERN.fullmatch(expected) is None:
        raise ArchiveError(f"{label}_invalid")
    if expected != actual:
        raise ArchiveError(f"{label}_mismatch")


def _inflate_bounded(archive: bytes) -> bytes:
    # A single gzip-aware decompressor exposes unused_data/eof and max_length;
    # this rejects concatenated members and caps output before tar parsing.
    # Source: https://docs.python.org/3.13/library/zlib.html#zlib.Decompress.decompress
    decompressor = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
    decompressed = bytearray()
    pending = archive
    try:
        while pending:
            previous_pending = pending
            remaining = MAX_TAR_BYTES - len(decompressed)
            chunk = decompressor.decompress(pending, remaining + 1)
            decompressed.extend(chunk)
            if len(decompressed) > MAX_TAR_BYTES:
                raise ArchiveError("archive_tar_expansion_limit")
            if decompressor.unused_data:
                raise ArchiveError("archive_gzip_trailing_data")
            if decompressor.eof:
                if decompressor.unconsumed_tail:
                    raise ArchiveError("archive_gzip_invalid")
                break
            pending = decompressor.unconsumed_tail
            if not pending:
                break
            if pending == previous_pending and not chunk:
                raise ArchiveError("archive_gzip_invalid")
    except ArchiveError:
        raise
    except zlib.error:
        raise ArchiveError("archive_gzip_invalid") from None
    if not decompressor.eof:
        raise ArchiveError("archive_gzip_invalid")
    if len(decompressed) > MAX_TAR_BYTES:
        raise ArchiveError("archive_tar_expansion_limit")
    return bytes(decompressed)


def _parse_ustar(tar_bytes: bytes) -> tuple[ArchiveFile, ...]:
    """Walk bounded raw USTAR blocks before interpreting any member data."""
    position = 0
    total_source_bytes = 0
    files: list[ArchiveFile] = []
    terminated = False
    while position < len(tar_bytes):
        if len(tar_bytes) - position < TAR_BLOCK_BYTES:
            raise ArchiveError("archive_tar_truncated")
        block = tar_bytes[position : position + TAR_BLOCK_BYTES]
        if not any(block):
            remaining = tar_bytes[position:]
            if (
                len(remaining) < 2 * TAR_BLOCK_BYTES
                or len(remaining) % TAR_BLOCK_BYTES
                or any(remaining)
            ):
                raise ArchiveError("archive_tar_terminator_invalid")
            terminated = True
            break
        if block[257:263] != b"ustar\0" or block[263:265] != b"00":
            raise ArchiveError("archive_tar_format_unsupported")
        # Reject PAX/GNU extension records and all non-regular types before
        # TarInfo.frombuf can interpret any following metadata blocks.
        # Sources: https://docs.python.org/3.13/library/tarfile.html
        # https://docs.python.org/3.13/library/tarfile.html#tarfile.TarInfo.frombuf
        if block[156:157] not in {b"0", b"\0"}:
            raise ArchiveError("archive_tar_type_unsupported")
        if any(block[start] & 0x80 for start in (100, 108, 116, 124, 136, 148, 329, 337)):
            raise ArchiveError("archive_tar_numeric_format_unsupported")
        if (
            _field_has_nonzero_tail(block[0:100])
            or _field_has_nonzero_tail(block[345:500])
            or any(block[157:257])
            or any(block[265:329])
            or any(block[500:512])
        ):
            raise ArchiveError("archive_tar_header_invalid")
        try:
            info = tarfile.TarInfo.frombuf(block, encoding="utf-8", errors="strict")
        except (tarfile.HeaderError, ValueError, UnicodeError):
            raise ArchiveError("archive_tar_header_invalid") from None
        if (
            info.type not in {tarfile.REGTYPE, tarfile.AREGTYPE}
            or info.linkname
            or info.uid != 0
            or info.gid != 0
            or info.uname
            or info.gname
            or info.mtime != 0
            or info.devmajor != 0
            or info.devminor != 0
            or info.mode not in {0o644, 0o755}
        ):
            raise ArchiveError("archive_tar_metadata_unsupported")
        if len(files) >= MAX_FILES:
            raise ArchiveError("archive_file_count_limit")
        if info.size < 0 or total_source_bytes + info.size > MAX_SOURCE_BYTES:
            raise ArchiveError("archive_source_size_limit")

        content_start = position + TAR_BLOCK_BYTES
        content_end = content_start + info.size
        padded_end = (
            content_start + ((info.size + TAR_BLOCK_BYTES - 1) // TAR_BLOCK_BYTES) * TAR_BLOCK_BYTES
        )
        if padded_end > len(tar_bytes):
            raise ArchiveError("archive_tar_truncated")
        content = tar_bytes[content_start:content_end]
        if any(tar_bytes[content_end:padded_end]):
            raise ArchiveError("archive_tar_padding_invalid")
        files.append(ArchiveFile(info.name, content, info.mode))
        total_source_bytes += info.size
        position = padded_end
    if not terminated:
        raise ArchiveError("archive_tar_terminator_missing")
    return tuple(files)


def _field_has_nonzero_tail(field: bytes) -> bool:
    try:
        nul = field.index(0)
    except ValueError:
        return False
    return any(field[nul + 1 :])


def _validate_files(files: Iterable[ArchiveFile]) -> tuple[ArchiveFile, ...]:
    normalized: list[ArchiveFile] = []
    nodes: dict[tuple[str, ...], tuple[tuple[str, ...], bool]] = {}
    total_source_bytes = 0
    for source_file in files:
        if len(normalized) >= MAX_FILES:
            raise ArchiveError("archive_file_count_limit")
        if not isinstance(source_file, ArchiveFile):
            raise ArchiveError("archive_input_invalid")
        path, components = _validate_path(source_file.path)
        if not isinstance(source_file.content, bytes):
            raise ArchiveError("archive_input_invalid")
        if type(source_file.mode) is not int or source_file.mode not in {0o644, 0o755}:
            raise ArchiveError("archive_mode_unsupported")
        if source_file.content.startswith(_LFS_POINTER):
            raise ArchiveError("archive_lfs_pointer_unsupported")
        total_source_bytes += len(source_file.content)
        if total_source_bytes > MAX_SOURCE_BYTES:
            raise ArchiveError("archive_source_size_limit")

        canonical_components = tuple(component.casefold() for component in components)
        for index in range(1, len(components) + 1):
            canonical = canonical_components[:index]
            spelling = components[:index]
            is_file = index == len(components)
            existing = nodes.get(canonical)
            if existing is not None:
                previous_spelling, previous_is_file = existing
                if previous_spelling != spelling:
                    raise ArchiveError("archive_path_collision")
                if previous_is_file:
                    if is_file:
                        raise ArchiveError("archive_path_duplicate")
                    raise ArchiveError("archive_path_conflict")
                if is_file:
                    raise ArchiveError("archive_path_conflict")
            else:
                nodes[canonical] = (spelling, is_file)
                if len(nodes) > MAX_SOURCE_TREE_NODES:
                    raise ArchiveError("archive_tree_node_limit")
        normalized.append(ArchiveFile(path, source_file.content, source_file.mode))

    normalized.sort(key=lambda entry: entry.path.encode("utf-8"))
    return tuple(normalized)


def _validate_path(path: object) -> tuple[str, tuple[str, ...]]:
    if not isinstance(path, str) or not path:
        raise ArchiveError("archive_path_invalid")
    if (
        path.startswith("/")
        or path.startswith("\\")
        or "\\" in path
        or "\0" in path
        or re.match(r"^[A-Za-z]:", path) is not None
    ):
        raise ArchiveError("archive_path_invalid")
    if unicodedata.normalize("NFC", path) != path:
        raise ArchiveError("archive_path_unicode_unsupported")
    try:
        path_bytes = path.encode("utf-8", errors="strict")
    except UnicodeError:
        raise ArchiveError("archive_path_invalid") from None
    if len(path_bytes) > MAX_PATH_BYTES:
        raise ArchiveError("archive_path_too_long")
    components = tuple(path.split("/"))
    if any(component in {"", ".", ".."} for component in components):
        raise ArchiveError("archive_path_invalid")
    if any(component.casefold() in _SOURCE_ONLY_EXCLUDED_COMPONENTS for component in components):
        raise ArchiveError("archive_source_only_path_unsupported")
    if not _ustar_path_supported(components):
        raise ArchiveError("archive_path_ustar_unsupported")
    return path, components


def _ustar_path_supported(components: tuple[str, ...]) -> bool:
    path = "/".join(components)
    if len(path.encode("utf-8")) <= 100:
        return True
    for separator in reversed([index for index, char in enumerate(path) if char == "/"]):
        prefix, name = path[:separator], path[separator + 1 :]
        if len(prefix.encode("utf-8")) <= 155 and len(name.encode("utf-8")) <= 100:
            return True
    return False


def _make_manifest(files: tuple[ArchiveFile, ...]) -> ArchiveManifest:
    entries = tuple(
        ManifestEntry(
            path=source_file.path,
            mode=source_file.mode,
            size=len(source_file.content),
            sha256=hashlib.sha256(source_file.content).hexdigest(),
        )
        for source_file in files
    )
    canonical_document = {
        "format": "continuum-source-tree",
        "version": TREE_FORMAT_VERSION,
        "files": [
            {
                "path": entry.path,
                "mode": f"{entry.mode:04o}",
                "size": entry.size,
                "sha256": entry.sha256,
            }
            for entry in entries
        ],
    }
    canonical_json = json.dumps(
        canonical_document,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    tree_digest = hashlib.sha256(_TREE_DIGEST_DOMAIN + canonical_json).hexdigest()
    return ArchiveManifest(TREE_FORMAT_VERSION, entries, tree_digest)
