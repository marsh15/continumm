"""Trusted T31 orchestration for approved Git objects and managed branches."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from continuum_api.approvals.manifest import DeterministicCommit, deterministic_commit
from continuum_api.artifacts.archives import (
    MAX_COMPRESSED_BYTES,
    MAX_SOURCE_BYTES,
    ArchiveError,
    VerifiedArchive,
    verify_archive,
)
from continuum_api.publication.github import GitHubPublicationError, GitHubTreeEntry
from continuum_api.runtime.approvals import ApprovalError, ApprovalService
from continuum_api.runtime.external_ledger import (
    ExternalOperationError,
    ExternalOperationReceipt,
    ExternalOperationService,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost

MAX_MUTATIONS_PER_SLICE = 16
MAX_GIT_OBJECTS = 20_000


class GitHubBranchPublicationError(RuntimeError):
    """Sanitized failure from trusted branch publication orchestration."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class GitHubBranchPublicationTransport(Protocol):
    async def current(self, repository_id: int, github_user_id: int) -> Any: ...

    async def get_ref(
        self,
        repository_id: int,
        github_user_id: int,
        branch: str,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> str | None: ...

    async def get_object(
        self,
        repository_id: int,
        github_user_id: int,
        kind: str,
        sha: str,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> str | None: ...

    async def upload_blob(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        content: bytes,
        expected_sha: str,
        expected_owner: str,
        expected_name: str,
        expected_base_branch: str,
        expected_base_sha: str,
    ) -> str: ...

    async def upload_tree(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        entries: Sequence[GitHubTreeEntry],
        expected_sha: str,
        expected_owner: str,
        expected_name: str,
        expected_base_branch: str,
        expected_base_sha: str,
    ) -> str: ...

    async def upload_commit(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        message: str,
        tree_sha: str,
        parent_sha: str,
        author: dict[str, Any],
        committer: dict[str, Any],
        expected_sha: str,
        expected_owner: str,
        expected_name: str,
        expected_base_branch: str,
        expected_base_sha: str,
    ) -> str: ...

    async def create_ref(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        branch: str,
        sha: str,
        expected_owner: str,
        expected_name: str,
        expected_base_branch: str,
        expected_base_sha: str,
    ) -> str: ...


@dataclass(frozen=True, slots=True)
class PublicationReceipt:
    approval_id: UUID
    run_id: UUID
    branch: str
    commit_sha: str
    status: str
    object_statuses: tuple[tuple[str, str], ...]
    branch_status: str


@dataclass(slots=True)
class _MutationBudget:
    limit: int
    used: int = 0

    @property
    def has_capacity(self) -> bool:
        return self.used < self.limit

    def charge(self) -> None:
        if not self.has_capacity:
            raise GitHubBranchPublicationError("github_publication_slice_budget_exhausted")
        self.used += 1


class GitHubBranchPublicationService:
    """Upload only manifest-derived objects and create/adopt its exact branch."""

    def __init__(
        self,
        approvals: ApprovalService,
        ledger: ExternalOperationService,
        transport: GitHubBranchPublicationTransport,
    ) -> None:
        self._approvals = approvals
        self._ledger = ledger
        self._transport = transport

    async def publish(
        self, lease: ExecutionLease, approval_id: UUID, manifest_sha256: str
    ) -> PublicationReceipt:
        try:
            snapshot = self._approvals.publication_snapshot(approval_id, manifest_sha256)
        except ApprovalError as error:
            raise GitHubBranchPublicationError(error.code) from None
        if snapshot.run_id != lease.run_id or lease.owner_id != snapshot.owner_id:
            raise GitHubBranchPublicationError("github_publication_execution_mismatch")

        manifest = snapshot.manifest
        prior_branch = self._ledger.read(snapshot.run_id, "github:managed-branch")
        if prior_branch is not None and prior_branch.status != "prepared":
            return await self._reconcile_prior_branch(snapshot, prior_branch)
        if snapshot.candidate_object_key is None:
            raise GitHubBranchPublicationError("github_publication_candidate_unavailable")
        try:
            source_bytes = await self._approvals.download_publication_candidate(
                snapshot.approval_id, snapshot.manifest_sha256.hex()
            )
            if len(source_bytes) > MAX_COMPRESSED_BYTES:
                raise GitHubBranchPublicationError("github_publication_source_size_limit")
            candidate = verify_archive(
                source_bytes,
                expected_archive_sha256=manifest["candidate"]["archive_sha256"],
                expected_tree_sha256=manifest["candidate"]["source_tree_sha256"],
            )
            if (
                sum(len(item.content) for item in candidate.files) > MAX_SOURCE_BYTES
                or _bounded_git_object_count(candidate) > MAX_GIT_OBJECTS
            ):
                raise GitHubBranchPublicationError("github_publication_git_object_limit")
            commit = deterministic_commit(
                candidate,
                parent_sha=manifest["base"]["commit_sha"],
                author_name=manifest["commit"]["author"]["name"],
                author_email=manifest["commit"]["author"]["email"],
                timestamp=manifest["commit"]["author"]["timestamp"],
                message=manifest["commit"]["message"],
            )
        except (ArchiveError, KeyError, TypeError, ValueError):
            raise GitHubBranchPublicationError("github_publication_candidate_invalid") from None
        except GitHubBranchPublicationError:
            raise
        except ApprovalError as error:
            raise GitHubBranchPublicationError(error.code) from None
        except Exception:
            raise GitHubBranchPublicationError("github_publication_candidate_unavailable") from None
        _validate_commit_against_manifest(commit, manifest)
        if len(commit.objects) > MAX_GIT_OBJECTS:
            raise GitHubBranchPublicationError("github_publication_git_object_limit")

        budget = _MutationBudget(MAX_MUTATIONS_PER_SLICE)
        object_statuses: list[tuple[str, str]] = []
        try:
            for kind, oid, body in _ordered_git_objects(commit):
                status = await self._publish_object(
                    lease, snapshot, kind, oid, body, manifest, budget
                )
                object_statuses.append((oid, status))
                await asyncio.sleep(0)
                if status != "confirmed":
                    return PublicationReceipt(
                        approval_id,
                        snapshot.run_id,
                        manifest["branch"]["name"],
                        commit.commit_sha,
                        status,
                        tuple(object_statuses),
                        "not_started",
                    )
            branch_status = await self._publish_branch(
                lease, snapshot, commit.commit_sha, manifest, budget
            )
        except (ApprovalError, ExternalOperationError, LeaseLost, GitHubPublicationError) as error:
            code = getattr(error, "code", None)
            raise GitHubBranchPublicationError(
                code if isinstance(code, str) else "github_publication_dispatch_unavailable"
            ) from None
        status = "published" if branch_status == "confirmed" else branch_status
        return PublicationReceipt(
            approval_id,
            snapshot.run_id,
            manifest["branch"]["name"],
            commit.commit_sha,
            status,
            tuple(object_statuses),
            branch_status,
        )

    async def _reconcile_prior_branch(
        self, snapshot: Any, receipt: ExternalOperationReceipt
    ) -> PublicationReceipt:
        manifest = snapshot.manifest
        branch = manifest["branch"]["name"]
        commit_sha = manifest["commit"]["sha"]
        if (
            receipt.approval_id != snapshot.approval_id
            or receipt.approval_sha256 != snapshot.manifest_sha256
            or receipt.request_metadata
            != {"operation": "update_managed_branch", "branch": branch, "desired_sha": commit_sha}
        ):
            raise GitHubBranchPublicationError("github_publication_operation_conflict")
        if receipt.status == "conflict":
            branch_status = "conflict"
        elif receipt.status == "definitely_failed":
            branch_status = "definitely_failed"
        else:
            try:
                observed = await self._transport.get_ref(
                    snapshot.repository_id,
                    snapshot.github_user_id,
                    branch,
                    expected_owner=manifest["repository"]["owner"],
                    expected_name=manifest["repository"]["name"],
                )
            except Exception:
                return PublicationReceipt(
                    snapshot.approval_id,
                    snapshot.run_id,
                    branch,
                    commit_sha,
                    "unknown",
                    (),
                    "unknown",
                )
            if observed == commit_sha:
                if receipt.status == "confirmed":
                    branch_status = "confirmed"
                else:
                    branch_status = self._record_observation(
                        snapshot.run_id,
                        "github:managed-branch",
                        receipt,
                        "confirmed",
                        {"branch": branch, "sha": observed},
                    )
            elif observed is not None or receipt.status == "confirmed":
                branch_status = self._record_observation(
                    snapshot.run_id,
                    "github:managed-branch",
                    receipt,
                    "conflict",
                    {"branch": branch, "desired_sha": commit_sha, "observed_sha": observed},
                )
            else:
                branch_status = self._record_observation(
                    snapshot.run_id,
                    "github:managed-branch",
                    receipt,
                    "unknown",
                    {"branch": branch, "desired_sha": commit_sha},
                )
        return PublicationReceipt(
            snapshot.approval_id,
            snapshot.run_id,
            branch,
            commit_sha,
            "published" if branch_status == "confirmed" else branch_status,
            (),
            branch_status,
        )

    async def _publish_object(
        self,
        lease: ExecutionLease,
        snapshot: Any,
        kind: str,
        oid: str,
        body: bytes,
        manifest: dict[str, Any],
        budget: _MutationBudget,
    ) -> str:
        operation_key = f"github:git-object:{oid}"
        metadata = {
            "operation": "write_git_objects",
            "object_kind": kind,
            "object_sha": oid,
        }
        receipt = self._ledger.prepare(
            lease,
            operation_key,
            metadata,
            approval_id=snapshot.approval_id,
            approval_sha256=snapshot.manifest_sha256,
        )
        if receipt.status == "confirmed":
            return "confirmed"
        if receipt.status == "conflict":
            return "conflict"
        if receipt.status in {"dispatching", "unknown"}:
            try:
                observed = await self._transport.get_object(
                    snapshot.repository_id,
                    snapshot.github_user_id,
                    kind,
                    oid,
                    expected_owner=manifest["repository"]["owner"],
                    expected_name=manifest["repository"]["name"],
                )
            except Exception:
                return "unknown"
            if observed == oid:
                return self._observe(
                    lease,
                    operation_key,
                    receipt,
                    "confirmed",
                    {"object_kind": kind, "object_sha": oid},
                )
            return "unknown"
        if receipt.status != "prepared":
            return "unknown"
        if not budget.has_capacity:
            return "pending"
        await self._assert_current_base(snapshot.repository_id, snapshot.github_user_id, manifest)
        await self._approvals.authorize_dispatch(
            snapshot.approval_id,
            snapshot.manifest_sha256.hex(),
            operation="write_git_objects",
            lease=lease,
        )
        dispatch = self._ledger.authorize_dispatch(lease, operation_key)
        if not dispatch.dispatch_allowed:
            return "unknown"
        budget.charge()
        await self._assert_current_base(snapshot.repository_id, snapshot.github_user_id, manifest)
        try:
            result_sha = await self._upload_object(
                snapshot.repository_id,
                snapshot.github_user_id,
                kind,
                oid,
                body,
                manifest,
                expected_owner=manifest["repository"]["owner"],
                expected_name=manifest["repository"]["name"],
            )
        except Exception as error:
            # Lost upload responses remain uncertain. A retry will reconcile the
            # ledger row and cannot send a second mutation under the same key.
            code = getattr(error, "code", None)
            if code in {
                "github_publication_base_changed",
                "github_publication_object_sha_mismatch",
            }:
                return self._observe(
                    lease,
                    operation_key,
                    dispatch,
                    "conflict",
                    {"object_sha": oid, "error_code": code},
                )
            self._observe(lease, operation_key, dispatch, "unknown", {"object_sha": oid})
            return "unknown"
        if result_sha != oid:
            return self._observe(
                lease,
                operation_key,
                dispatch,
                "conflict",
                {"object_kind": kind, "object_sha": oid, "observed_sha": result_sha},
            )
        return self._observe(
            lease,
            operation_key,
            dispatch,
            "confirmed",
            {"object_kind": kind, "object_sha": oid},
        )

    async def _upload_object(
        self,
        repository_id: int,
        github_user_id: int,
        kind: str,
        oid: str,
        body: bytes,
        manifest: dict[str, Any],
        *,
        expected_owner: str,
        expected_name: str,
    ) -> str:
        if kind == "blob":
            return await self._transport.upload_blob(
                repository_id,
                github_user_id,
                content=body,
                expected_sha=oid,
                expected_owner=expected_owner,
                expected_name=expected_name,
                expected_base_branch=manifest["base"]["branch"],
                expected_base_sha=manifest["base"]["commit_sha"],
            )
        if kind == "tree":
            entries = _tree_entries(body)
            return await self._transport.upload_tree(
                repository_id,
                github_user_id,
                entries=entries,
                expected_sha=oid,
                expected_owner=expected_owner,
                expected_name=expected_name,
                expected_base_branch=manifest["base"]["branch"],
                expected_base_sha=manifest["base"]["commit_sha"],
            )
        return await self._transport.upload_commit(
            repository_id,
            github_user_id,
            message=manifest["commit"]["message"],
            tree_sha=manifest["candidate"]["git_tree_sha"],
            parent_sha=manifest["base"]["commit_sha"],
            author=manifest["commit"]["author"],
            committer=manifest["commit"]["committer"],
            expected_sha=oid,
            expected_owner=expected_owner,
            expected_name=expected_name,
            expected_base_branch=manifest["base"]["branch"],
            expected_base_sha=manifest["base"]["commit_sha"],
        )

    async def _publish_branch(
        self,
        lease: ExecutionLease,
        snapshot: Any,
        commit_sha: str,
        manifest: dict[str, Any],
        budget: _MutationBudget,
    ) -> str:
        branch = manifest["branch"]["name"]
        operation_key = "github:managed-branch"
        metadata = {
            "operation": "update_managed_branch",
            "branch": branch,
            "desired_sha": commit_sha,
        }
        receipt = self._ledger.prepare(
            lease,
            operation_key,
            metadata,
            approval_id=snapshot.approval_id,
            approval_sha256=snapshot.manifest_sha256,
        )
        if receipt.status == "conflict":
            return "conflict"
        try:
            observed = await self._transport.get_ref(
                snapshot.repository_id,
                snapshot.github_user_id,
                branch,
                expected_owner=manifest["repository"]["owner"],
                expected_name=manifest["repository"]["name"],
            )
        except Exception:
            return "unknown"
        if observed == commit_sha:
            if receipt.status == "confirmed":
                return "confirmed"
            if receipt.status == "prepared":
                await self._approvals.authorize_dispatch(
                    snapshot.approval_id,
                    snapshot.manifest_sha256.hex(),
                    operation="update_managed_branch",
                    lease=lease,
                )
                dispatch = self._ledger.authorize_dispatch(lease, operation_key)
                if dispatch.dispatch_allowed:
                    return self._observe(
                        lease,
                        operation_key,
                        dispatch,
                        "confirmed",
                        {"branch": branch, "sha": observed},
                    )
                return "unknown"
            if receipt.status in {"dispatching", "unknown"}:
                return self._observe(
                    lease, operation_key, receipt, "confirmed", {"branch": branch, "sha": observed}
                )
            return "unknown"
        if observed is not None:
            if receipt.status in {"dispatching", "unknown", "confirmed"}:
                self._observe(
                    lease, operation_key, receipt, "conflict", {"branch": branch, "sha": observed}
                )
            return "conflict"
        if receipt.status == "confirmed":
            self._observe(
                lease,
                operation_key,
                receipt,
                "conflict",
                {"branch": branch, "desired_sha": commit_sha, "observed_sha": None},
            )
            return "conflict"
        if receipt.status in {"dispatching", "unknown"}:
            self._observe(
                lease,
                operation_key,
                receipt,
                "unknown",
                {"branch": branch, "desired_sha": commit_sha},
            )
            return "unknown"
        if receipt.status != "prepared":
            return "unknown"
        if not budget.has_capacity:
            return "pending"

        # Approval and rights are rechecked immediately before the single ref
        # creation. The operation key is stable for this run and is never retried.
        await self._approvals.authorize_dispatch(
            snapshot.approval_id,
            snapshot.manifest_sha256.hex(),
            operation="update_managed_branch",
            lease=lease,
        )
        dispatch = self._ledger.authorize_dispatch(lease, operation_key)
        if not dispatch.dispatch_allowed:
            return "unknown"
        budget.charge()
        await self._assert_current_base(snapshot.repository_id, snapshot.github_user_id, manifest)
        try:
            created = await self._transport.create_ref(
                snapshot.repository_id,
                snapshot.github_user_id,
                branch=branch,
                sha=commit_sha,
                expected_owner=manifest["repository"]["owner"],
                expected_name=manifest["repository"]["name"],
                expected_base_branch=manifest["base"]["branch"],
                expected_base_sha=manifest["base"]["commit_sha"],
            )
        except Exception:
            created = None
        if created == commit_sha:
            return self._observe(
                lease, operation_key, dispatch, "confirmed", {"branch": branch, "sha": created}
            )

        # A failed/lost response is resolved by reads only. A missing ref is not
        # proof that the authorized request cannot still appear.
        try:
            observed = await self._transport.get_ref(
                snapshot.repository_id,
                snapshot.github_user_id,
                branch,
                expected_owner=manifest["repository"]["owner"],
                expected_name=manifest["repository"]["name"],
            )
        except Exception:
            observed = None
        if observed == commit_sha:
            return self._observe(
                lease, operation_key, dispatch, "confirmed", {"branch": branch, "sha": observed}
            )
        if observed is not None:
            return self._observe(
                lease, operation_key, dispatch, "conflict", {"branch": branch, "sha": observed}
            )
        return self._observe(
            lease, operation_key, dispatch, "unknown", {"branch": branch, "desired_sha": commit_sha}
        )

    async def _assert_current_base(
        self, repository_id: int, github_user_id: int, manifest: dict[str, Any]
    ) -> None:
        try:
            facts = await self._transport.current(repository_id, github_user_id)
        except Exception as error:
            code = getattr(error, "code", None)
            raise GitHubBranchPublicationError(
                code if isinstance(code, str) else "github_publication_rights_unavailable"
            ) from None
        if (
            facts.repository_id != repository_id
            or facts.github_user_id != github_user_id
            or facts.owner != manifest["repository"]["owner"]
            or facts.name != manifest["repository"]["name"]
            or facts.default_branch != manifest["base"]["branch"]
            or facts.base_commit_sha != manifest["base"]["commit_sha"]
        ):
            raise GitHubBranchPublicationError("github_publication_base_changed")

    def _observe(
        self,
        lease: ExecutionLease,
        operation_key: str,
        dispatch: ExternalOperationReceipt,
        outcome: str,
        details: dict[str, Any],
    ) -> str:
        return self._record_observation(lease.run_id, operation_key, dispatch, outcome, details)

    def _record_observation(
        self,
        run_id: UUID,
        operation_key: str,
        dispatch: ExternalOperationReceipt,
        outcome: str,
        details: dict[str, Any],
    ) -> str:
        observed = self._ledger.observe(
            run_id=run_id,
            operation_key=operation_key,
            dispatch_receipt=dispatch,
            observation_key=f"github-publication:{outcome}:{hashlib.sha256(repr(details).encode()).hexdigest()[:20]}",
            outcome=outcome,
            details=details,
        )
        return {
            "confirmed": "confirmed",
            "conflict": "conflict",
            "definitely_failed": "definitely_failed",
        }.get(observed.status, "unknown")


def _validate_commit_against_manifest(
    commit: DeterministicCommit, manifest: dict[str, Any]
) -> None:
    import base64

    expected = manifest["commit"]
    if (
        commit.commit_sha != expected["sha"]
        or commit.tree_sha != manifest["candidate"]["git_tree_sha"]
        or commit.commit_bytes != base64.b64decode(expected["bytes_base64"], validate=True)
        or commit.objects.get(commit.commit_sha) != commit.commit_bytes
    ):
        raise GitHubBranchPublicationError("github_publication_manifest_object_mismatch")
    kinds = _git_object_kinds(commit)
    for oid, body in commit.objects.items():
        kind = kinds[oid]
        if _git_object_sha(kind, body) != oid:
            raise GitHubBranchPublicationError("github_publication_git_object_mismatch")


def _bounded_git_object_count(candidate: VerifiedArchive) -> int:
    """Count blobs, trees and commit before building a potentially huge tree map."""
    directories: set[str] = set()
    for item in candidate.files:
        parts = item.path.split("/")
        for index in range(1, len(parts)):
            directories.add("/".join(parts[:index]))
            count = len(candidate.files) + len(directories) + 2
            if count > MAX_GIT_OBJECTS:
                return count
    return len(candidate.files) + len(directories) + 2


def _ordered_git_objects(commit: DeterministicCommit) -> list[tuple[str, str, bytes]]:
    blobs: list[tuple[str, str, bytes]] = []
    tree_ids = _git_object_kinds(commit)
    tree_depths: dict[str, int] = {}

    def depth(tree_sha: str) -> int:
        if tree_sha in tree_depths:
            return tree_depths[tree_sha]
        body = commit.objects[tree_sha]
        child_depths = [depth(sha) for mode, sha, _name in _parse_tree(body) if mode == "40000"]
        tree_depths[tree_sha] = 1 + max(child_depths, default=0)
        return tree_depths[tree_sha]

    depth(commit.tree_sha)
    trees: list[tuple[str, str, bytes]] = []
    for oid, body in commit.objects.items():
        if oid == commit.commit_sha:
            continue
        if tree_ids[oid] == "tree":
            trees.append(("tree", oid, body))
        else:
            blobs.append(("blob", oid, body))
    trees.sort(key=lambda item: (tree_depths[item[1]], item[1]))
    return [
        *sorted(blobs, key=lambda item: item[1]),
        *trees,
        ("commit", commit.commit_sha, commit.commit_bytes),
    ]


def _git_object_kinds(commit: DeterministicCommit) -> dict[str, str]:
    kinds = {commit.commit_sha: "commit", commit.tree_sha: "tree"}
    pending = [commit.tree_sha]
    while pending:
        tree_sha = pending.pop()
        for mode, child_sha, _name in _parse_tree(commit.objects[tree_sha]):
            if mode == "40000":
                kinds[child_sha] = "tree"
                pending.append(child_sha)
            else:
                kinds[child_sha] = "blob"
    if set(kinds) != set(commit.objects):
        raise GitHubBranchPublicationError("github_publication_git_object_set_invalid")
    return kinds


def _parse_tree(body: bytes) -> list[tuple[str, str, str]]:
    entries: list[tuple[str, str, str]] = []
    cursor = 0
    while cursor < len(body):
        space = body.index(b" ", cursor)
        nul = body.index(b"\0", space + 1)
        mode = body[cursor:space].decode("ascii")
        name = body[space + 1 : nul].decode("utf-8")
        sha = body[nul + 1 : nul + 21].hex()
        if len(body[nul + 1 : nul + 21]) != 20:
            raise GitHubBranchPublicationError("github_publication_git_tree_invalid")
        entries.append((mode, sha, name))
        cursor = nul + 21
    return entries


def _tree_entries(body: bytes) -> tuple[GitHubTreeEntry, ...]:
    entries: list[GitHubTreeEntry] = []
    for mode, sha, path in _parse_tree(body):
        entries.append(
            GitHubTreeEntry(
                mode="040000" if mode in {"40000", "040000"} else mode,
                type="tree" if mode in {"40000", "040000"} else "blob",
                sha=sha,
                path=path,
            )
        )
    return tuple(entries)


def _git_object_sha(kind: str, body: bytes) -> str:
    return hashlib.sha1(f"{kind} {len(body)}\0".encode("ascii") + body).hexdigest()
