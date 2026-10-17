"""Bounded source archive creation and verification tests."""

from __future__ import annotations

import gzip
import io
import tarfile

import pytest

from continuum_api.artifacts.archives import (
    MAX_FILES,
    ArchiveError,
    ArchiveFile,
    create_archive,
    verify_archive,
)


def test_archive_is_reproducible_and_preserves_only_executable_mode() -> None:
    files = (
        ArchiveFile("src/run.py", b"#!/usr/bin/env python3\n", 0o755),
        ArchiveFile("README.md", b"fixture\n", 0o644),
    )

    first = create_archive(files)
    second = create_archive(reversed(files))
    verified = verify_archive(
        first.archive,
        expected_archive_sha256=first.archive_sha256,
        expected_tree_sha256=first.manifest.tree_sha256,
        expected_manifest=first.manifest,
    )

    assert first.archive == second.archive
    assert first.archive_sha256 == second.archive_sha256
    assert first.manifest == second.manifest
    assert first.manifest.format_version == 1
    assert first.manifest.tree_sha256 != first.archive_sha256
    assert [(item.path, item.mode) for item in verified.manifest.entries] == [
        ("README.md", 0o644),
        ("src/run.py", 0o755),
    ]
    assert verified.files == (
        ArchiveFile("README.md", b"fixture\n", 0o644),
        ArchiveFile("src/run.py", b"#!/usr/bin/env python3\n", 0o755),
    )


def test_tree_digest_changes_for_content_and_executable_bit() -> None:
    base = create_archive((ArchiveFile("src/app.py", b"print(1)\n"),))
    content_changed = create_archive((ArchiveFile("src/app.py", b"print(2)\n"),))
    mode_changed = create_archive((ArchiveFile("src/app.py", b"print(1)\n", 0o755),))

    assert base.manifest.tree_sha256 != content_changed.manifest.tree_sha256
    assert base.manifest.tree_sha256 != mode_changed.manifest.tree_sha256


def test_valid_ustar_prefix_long_path_round_trips() -> None:
    path = f"{'a' * 150}/{'b' * 100}"

    bundle = create_archive((ArchiveFile(path, b"content"),))
    restored = verify_archive(bundle.archive, expected_manifest=bundle.manifest)

    assert restored.files[0].path == path


@pytest.mark.parametrize(
    "path",
    [
        "../escape.py",
        "/absolute.py",
        "C:/drive.py",
        "src\\escape.py",
        "./dot.py",
        "src//empty.py",
        "src/../escape.py",
        "src/\0nul.py",
        "e\u0301/source.py",
        "x" * 101 + ".py",
    ],
)
def test_create_rejects_noncanonical_or_unsafe_paths(path: str) -> None:
    with pytest.raises(ArchiveError):
        create_archive((ArchiveFile(path, b"x"),))


@pytest.mark.parametrize(
    "paths",
    [
        ("src/a.py", "src/a.py"),
        ("src/A.py", "src/a.py"),
        ("A/x.py", "a/y.py"),
        ("package", "package/file.py"),
        ("package/file.py", "package"),
    ],
)
def test_create_rejects_duplicate_casefold_and_ancestor_conflicts(paths: tuple[str, str]) -> None:
    with pytest.raises(ArchiveError):
        create_archive(tuple(ArchiveFile(path, b"x") for path in paths))


@pytest.mark.parametrize(
    "path",
    [
        ".git/config",
        ".hg/store",
        "node_modules/pkg/index.js",
        "src/.venv/bin/python",
        "tests/__pycache__/test.pyc",
        "src/.pytest_cache/lastfailed",
    ],
)
def test_create_rejects_source_control_and_dependency_cache_paths(path: str) -> None:
    with pytest.raises(ArchiveError, match="archive_source_only_path_unsupported"):
        create_archive((ArchiveFile(path, b"x"),))


def test_create_rejects_unsupported_modes_and_lfs_pointers() -> None:
    with pytest.raises(ArchiveError, match="archive_mode_unsupported"):
        create_archive((ArchiveFile("src/file.py", b"x", 0o600),))
    with pytest.raises(ArchiveError, match="archive_lfs_pointer_unsupported"):
        create_archive((ArchiveFile("data.bin", b"version https://git-lfs.github.com/spec/v1\n"),))


def _gzip_tar(
    members: tuple[tuple[tarfile.TarInfo, bytes], ...],
    *,
    fmt: int = tarfile.USTAR_FORMAT,
) -> bytes:
    tar_bytes = io.BytesIO()
    with tarfile.open(
        fileobj=tar_bytes,
        mode="w:",
        format=fmt,
        encoding="utf-8",
        errors="strict",
    ) as archive:
        for info, content in members:
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
    return gzip.compress(tar_bytes.getvalue(), mtime=0)


@pytest.mark.parametrize(
    "member_type",
    [
        tarfile.SYMTYPE,
        tarfile.LNKTYPE,
        tarfile.DIRTYPE,
        tarfile.FIFOTYPE,
        tarfile.CHRTYPE,
        tarfile.BLKTYPE,
        tarfile.GNUTYPE_SPARSE,
    ],
)
def test_verify_rejects_links_directories_special_and_sparse_members(member_type: bytes) -> None:
    info = tarfile.TarInfo("src/item")
    info.type = member_type
    info.linkname = "../target" if member_type in {tarfile.SYMTYPE, tarfile.LNKTYPE} else ""
    with pytest.raises(ArchiveError):
        verify_archive(_gzip_tar(((info, b""),)))


def test_verify_rejects_pax_headers_before_tarfile_can_expand_them() -> None:
    info = tarfile.TarInfo("src/" + "long-name-" * 20 + "file.txt")
    with pytest.raises(ArchiveError, match="archive_tar_type_unsupported"):
        verify_archive(_gzip_tar(((info, b"x"),), fmt=tarfile.PAX_FORMAT))


def test_verify_rejects_unaligned_zero_terminator() -> None:
    bundle = create_archive((ArchiveFile("src/file.py", b"safe"),))
    raw_tar = gzip.decompress(bundle.archive) + b"\0"
    with pytest.raises(ArchiveError, match="archive_tar_terminator_invalid"):
        verify_archive(gzip.compress(raw_tar, mtime=0))


def test_verify_rejects_malformed_checksum_and_corrupt_gzip_footer() -> None:
    bundle = create_archive((ArchiveFile("src/file.py", b"safe"),))
    raw_tar = bytearray(gzip.decompress(bundle.archive))
    raw_tar[0] ^= 1
    with pytest.raises(ArchiveError, match="archive_tar_header_invalid"):
        verify_archive(gzip.compress(bytes(raw_tar), mtime=0))

    corrupted_gzip = bundle.archive[:-1] + bytes([bundle.archive[-1] ^ 0xFF])
    with pytest.raises(ArchiveError, match="archive_gzip_invalid"):
        verify_archive(corrupted_gzip)


@pytest.mark.parametrize(
    ("suffix", "reason"),
    [
        (gzip.compress(b"", mtime=0), "archive_gzip_trailing_data"),
        (b"trailing-garbage", "archive_gzip_trailing_data"),
        (b"", "archive_gzip_invalid"),
    ],
)
def test_verify_requires_one_complete_gzip_member(suffix: bytes, reason: str) -> None:
    bundle = create_archive((ArchiveFile("src/file.py", b"safe"),))
    data = bundle.archive[:-4] if not suffix else bundle.archive + suffix

    with pytest.raises(ArchiveError, match=reason):
        verify_archive(data)


def test_verify_rejects_member_traversal_and_lfs_pointer_content() -> None:
    traversal = tarfile.TarInfo("../escape.py")
    with pytest.raises(ArchiveError, match="archive_path_invalid"):
        verify_archive(_gzip_tar(((traversal, b"x"),)))

    pointer = tarfile.TarInfo("data.bin")
    with pytest.raises(ArchiveError, match="archive_lfs_pointer_unsupported"):
        verify_archive(_gzip_tar(((pointer, b"version https://git-lfs.github.com/spec/v1\n"),)))


@pytest.mark.parametrize(
    "paths",
    [
        ("src/file.py", "src/file.py"),
        ("src/A.py", "src/a.py"),
        ("src/package", "src/package/file.py"),
        ("src/A/file.py", "src/a/other.py"),
    ],
)
def test_verify_rejects_duplicate_casefold_and_ancestor_members(paths: tuple[str, str]) -> None:
    members = tuple((tarfile.TarInfo(path), b"x") for path in paths)

    with pytest.raises(ArchiveError):
        verify_archive(_gzip_tar(members))


def test_verify_rejects_tar_header_size_before_reading_member_data() -> None:
    info = tarfile.TarInfo("large.bin")
    info.mode = 0o644
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    info.size = 50 * 1024 * 1024 + 1
    header = info.tobuf(
        format=tarfile.USTAR_FORMAT,
        encoding="utf-8",
        errors="strict",
    )
    malicious = gzip.compress(header + b"\0" * 1024, mtime=0)

    with pytest.raises(ArchiveError, match="archive_source_size_limit"):
        verify_archive(malicious)


def test_verify_rejects_noncanonical_owner_and_time_metadata() -> None:
    info = tarfile.TarInfo("src/file.py")
    info.mode = 0o644
    info.uid = 1
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0

    with pytest.raises(ArchiveError, match="archive_tar_metadata_unsupported"):
        verify_archive(_gzip_tar(((info, b"x"),)))


def test_verify_stops_at_file_count_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("continuum_api.artifacts.archives.MAX_FILES", 2)
    members = tuple((tarfile.TarInfo(f"src/{index}.txt"), b"") for index in range(3))

    with pytest.raises(ArchiveError, match="archive_file_count_limit"):
        verify_archive(_gzip_tar(members))


def test_source_tree_node_limit_is_enforced_for_create_and_verify(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = (ArchiveFile("a/file.txt", b"a"), ArchiveFile("b/file.txt", b"b"))
    monkeypatch.setattr("continuum_api.artifacts.archives.MAX_SOURCE_TREE_NODES", 4)
    bundle = create_archive(files)
    assert len(verify_archive(bundle.archive).files) == 2

    monkeypatch.setattr("continuum_api.artifacts.archives.MAX_SOURCE_TREE_NODES", 3)
    with pytest.raises(ArchiveError, match="archive_tree_node_limit"):
        create_archive(files)
    with pytest.raises(ArchiveError, match="archive_tree_node_limit"):
        verify_archive(bundle.archive)


def test_expected_archive_tree_and_manifest_digests_are_checked() -> None:
    bundle = create_archive((ArchiveFile("src/file.py", b"safe"),))
    wrong = "0" * 64

    with pytest.raises(ArchiveError, match="archive_digest_mismatch"):
        verify_archive(bundle.archive, expected_archive_sha256=wrong)
    with pytest.raises(ArchiveError, match="source_tree_digest_mismatch"):
        verify_archive(bundle.archive, expected_tree_sha256=wrong)
    with pytest.raises(ArchiveError, match="archive_manifest_mismatch"):
        verify_archive(bundle.archive, expected_manifest=create_archive(()).manifest)
    with pytest.raises(ArchiveError, match="archive_digest_invalid"):
        verify_archive(bundle.archive, expected_archive_sha256="not-a-digest")


def test_source_file_and_tar_byte_limits_are_enforced(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("continuum_api.artifacts.archives.MAX_SOURCE_BYTES", 8)
    with pytest.raises(ArchiveError, match="archive_source_size_limit"):
        create_archive((ArchiveFile("src/file.py", b"x" * 9),))

    monkeypatch.setattr("continuum_api.artifacts.archives.MAX_SOURCE_BYTES", 1024)
    monkeypatch.setattr("continuum_api.artifacts.archives.MAX_TAR_BYTES", 1024)
    with pytest.raises(ArchiveError, match="archive_tar_size_limit"):
        create_archive((ArchiveFile("src/file.py", b"x" * 1024),))


def test_creator_enforces_compressed_byte_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("continuum_api.artifacts.archives.MAX_COMPRESSED_BYTES", 32)
    with pytest.raises(ArchiveError, match="archive_compressed_size_limit"):
        create_archive((ArchiveFile("src/noisy.bin", bytes(range(256))),))


def test_compressed_and_decompressed_archive_limits_are_enforced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("continuum_api.artifacts.archives.MAX_COMPRESSED_BYTES", 8)
    with pytest.raises(ArchiveError, match="archive_compressed_size_limit"):
        verify_archive(b"x" * 9)

    monkeypatch.setattr("continuum_api.artifacts.archives.MAX_COMPRESSED_BYTES", 10 * 1024 * 1024)
    monkeypatch.setattr("continuum_api.artifacts.archives.MAX_TAR_BYTES", 1024)
    compressed_bomb = gzip.compress(b"z" * 1025, mtime=0)
    with pytest.raises(ArchiveError, match="archive_tar_expansion_limit"):
        verify_archive(compressed_bomb)


def test_file_count_limit_stops_generator_without_consuming_more_inputs() -> None:
    consumed = 0

    def files():
        nonlocal consumed
        for index in range(MAX_FILES + 100):
            consumed += 1
            yield ArchiveFile(f"src/{index}.txt", b"")

    with pytest.raises(ArchiveError, match="archive_file_count_limit"):
        create_archive(files())
    assert consumed == MAX_FILES + 1
