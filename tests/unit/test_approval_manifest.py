from __future__ import annotations

from continuum_api.approvals.manifest import (
    ApprovalManifestError,
    canonical_json,
    deterministic_commit,
    digest_manifest,
    git_object_sha,
    git_tree_objects,
)
from continuum_api.artifacts.archives import ArchiveFile, create_archive, verify_archive


def test_manifest_serialization_is_canonical_and_hashed() -> None:
    encoded, digest = digest_manifest({"z": 1, "a": "é"})
    assert encoded == b'{"a":"\xc3\xa9","z":1}'
    assert digest.hex() == __import__("hashlib").sha256(encoded).hexdigest()
    assert canonical_json({"a": 1}) == b'{"a":1}'


def test_git_tree_and_commit_are_deterministic_from_verified_archive() -> None:
    bundle = create_archive(
        (ArchiveFile("src/main.py", b"print('ok')\n"), ArchiveFile("README.md", b"hello\n"))
    )
    verified = verify_archive(
        bundle.archive,
        expected_archive_sha256=bundle.archive_sha256,
        expected_tree_sha256=bundle.manifest.tree_sha256,
        expected_manifest=bundle.manifest,
    )
    tree, objects = git_tree_objects(verified.files)
    commit = deterministic_commit(
        verified,
        parent_sha="a" * 40,
        author_name="Continumm",
        author_email="noreply@continumm.invalid",
        timestamp=1_797_000_000,
        message="Apply reviewed change\n",
    )
    assert commit.tree_sha == tree
    assert commit.commit_sha == git_object_sha("commit", commit.commit_bytes)
    assert commit.objects[tree] == objects[tree]
    assert (
        deterministic_commit(
            verified,
            parent_sha="a" * 40,
            author_name="Continumm",
            author_email="noreply@continumm.invalid",
            timestamp=1_797_000_000,
            message="Apply reviewed change\n",
        ).commit_sha
        == commit.commit_sha
    )


def test_commit_rejects_unpinned_or_invalid_identity() -> None:
    bundle = create_archive((ArchiveFile("a.py", b"x"),))
    verified = verify_archive(bundle.archive)
    try:
        deterministic_commit(
            verified,
            parent_sha="bad",
            author_name="Continumm",
            author_email="noreply@continumm.invalid",
            timestamp=1,
            message="change",
        )
    except ApprovalManifestError as error:
        assert str(error) == "approval_parent_sha_invalid"
    else:
        raise AssertionError("invalid parent SHA accepted")
