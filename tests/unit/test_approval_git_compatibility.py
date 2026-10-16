"""Compare frozen publication objects to Git's independent plumbing implementation."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from continuum_api.approvals.manifest import deterministic_commit
from continuum_api.artifacts.archives import ArchiveFile, create_archive, verify_archive


def test_frozen_git_objects_match_git_for_nested_paths_modes_and_unicode(tmp_path: Path) -> None:
    executable = shutil.which("git")
    if executable is None:
        pytest.skip("Git plumbing is required for independent object compatibility proof")
    repository = tmp_path / "trusted-git"
    repository.mkdir()
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_SYSTEM=os.devnull,
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_COUNT="1",
        GIT_CONFIG_KEY_0="core.hooksPath",
        GIT_CONFIG_VALUE_0=os.devnull,
        GIT_AUTHOR_NAME="Continumm Test",
        GIT_AUTHOR_EMAIL="test@example.invalid",
        GIT_AUTHOR_DATE="1792022400 +0000",
        GIT_COMMITTER_NAME="Continumm Test",
        GIT_COMMITTER_EMAIL="test@example.invalid",
        GIT_COMMITTER_DATE="1792022400 +0000",
    )

    def git(*arguments: str, data: bytes = b"") -> bytes:
        return subprocess.run(
            [executable, *arguments],
            cwd=repository,
            env=environment,
            input=data,
            capture_output=True,
            check=True,
            timeout=10,
        ).stdout.strip()

    git("init", "--initial-branch=main", ".")
    empty_tree = git("mktree").decode()
    parent = git("commit-tree", empty_tree, data=b"baseline\n").decode()
    files = (
        ArchiveFile("dir/nested/binary.bin", b"\0\xff\x01"),
        ArchiveFile("dir/a.txt", b"nested\n"),
        ArchiveFile("dir.file", b"prefix sorts before directory\n"),
        ArchiveFile("dir0", b"prefix sorts after directory\n"),
        ArchiveFile("run.sh", b"#!/bin/sh\nexit 0\n", 0o755),
        ArchiveFile("é.txt", b"UTF-8 content\n"),
    )
    bundle = create_archive(files)
    verified = verify_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
    )
    for file in files:
        oid = git("hash-object", "-w", "--stdin", data=file.content).decode()
        git(
            "update-index",
            "--add",
            "--cacheinfo",
            "100755" if file.mode == 0o755 else "100644",
            oid,
            file.path,
        )
    expected_tree = git("write-tree").decode()
    message = "Freeze verified candidate\n\nExact publication text.\n"
    expected_commit = git(
        "commit-tree", expected_tree, "-p", parent, data=message.encode()
    ).decode()
    actual = deterministic_commit(
        verified,
        parent_sha=parent,
        author_name="Continumm Test",
        author_email="test@example.invalid",
        timestamp=1792022400,
        message=message,
    )
    assert actual.tree_sha == expected_tree
    assert actual.commit_sha == expected_commit
    assert actual.commit_bytes == git("cat-file", "commit", expected_commit) + b"\n"
    assert (
        actual.objects[actual.tree_sha]
        == subprocess.run(
            [executable, "cat-file", "tree", expected_tree],
            cwd=repository,
            env=environment,
            capture_output=True,
            check=True,
            timeout=10,
        ).stdout
    )
