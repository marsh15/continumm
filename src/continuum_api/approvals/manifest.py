"""Canonical approval serialization and deterministic Git object identities."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from continuum_api.artifacts.archives import ArchiveFile, VerifiedArchive


class ApprovalManifestError(ValueError):
    """An approval manifest or candidate source is not canonical."""


def canonical_json(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise ApprovalManifestError("approval_manifest_not_canonical") from None


def digest_manifest(value: Mapping[str, Any]) -> tuple[bytes, bytes]:
    encoded = canonical_json(value)
    return encoded, hashlib.sha256(encoded).digest()


def git_object_sha(kind: str, content: bytes) -> str:
    if kind not in {"blob", "tree", "commit"}:
        raise ApprovalManifestError("approval_git_object_kind_invalid")
    header = f"{kind} {len(content)}\0".encode("ascii")
    return hashlib.sha1(header + content).hexdigest()


def git_tree_objects(files: tuple[ArchiveFile, ...]) -> tuple[str, dict[str, bytes]]:
    """Build Git tree objects using normalized archive modes and Git ordering."""
    if not files:
        raise ApprovalManifestError("approval_candidate_empty")
    nodes: dict[str, dict[str, Any]] = {"": {}}
    objects: dict[str, bytes] = {}
    for item in files:
        parts = item.path.split("/")
        cursor = ""
        parent = ""
        for part in parts[:-1]:
            cursor = f"{cursor}/{part}" if cursor else part
            nodes[parent][part] = None
            nodes.setdefault(cursor, {})
            parent = cursor
        directory = "/".join(parts[:-1])
        nodes.setdefault(directory, {})[parts[-1]] = item
        blob = git_object_sha("blob", item.content)
        objects[blob] = item.content

    def build(directory: str) -> str:
        entries = nodes[directory]
        serialized: list[tuple[str, bytes]] = []
        for name, child in entries.items():
            if isinstance(child, ArchiveFile):
                mode, oid = (
                    (b"100755" if child.mode & 0o111 else b"100644"),
                    git_object_sha("blob", child.content),
                )
                sort_name = name
            else:
                mode, oid = b"40000", build(f"{directory}/{name}".strip("/"))
                sort_name = f"{name}/"
            serialized.append(
                (sort_name, mode + b" " + name.encode("utf-8") + b"\0" + bytes.fromhex(oid))
            )
        body = b"".join(
            value for _, value in sorted(serialized, key=lambda entry: entry[0].encode())
        )
        oid = git_object_sha("tree", body)
        objects[oid] = body
        return oid

    return build(""), objects


@dataclass(frozen=True, slots=True)
class DeterministicCommit:
    tree_sha: str
    commit_sha: str
    commit_bytes: bytes
    objects: Mapping[str, bytes]


def deterministic_commit(
    source: VerifiedArchive,
    *,
    parent_sha: str,
    author_name: str,
    author_email: str,
    timestamp: int,
    message: str,
) -> DeterministicCommit:
    """Compute Git objects solely from verified candidate bytes and fixed metadata."""
    if len(parent_sha) != 40 or any(char not in "0123456789abcdef" for char in parent_sha):
        raise ApprovalManifestError("approval_parent_sha_invalid")
    if type(timestamp) is not int or timestamp < 0:
        raise ApprovalManifestError("approval_timestamp_invalid")
    if not author_name or not author_email or "\n" in author_name + author_email:
        raise ApprovalManifestError("approval_identity_invalid")
    if not message or "\0" in message:
        raise ApprovalManifestError("approval_message_invalid")
    tree_sha, objects = git_tree_objects(source.files)
    identity = f"{author_name} <{author_email}> {timestamp} +0000"
    body = (
        f"tree {tree_sha}\nparent {parent_sha}\nauthor {identity}\n"
        f"committer {identity}\n\n{message}"
    ).encode()
    commit_sha = git_object_sha("commit", body)
    objects[commit_sha] = body
    return DeterministicCommit(tree_sha, commit_sha, body, objects)
