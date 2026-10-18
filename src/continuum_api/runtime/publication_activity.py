"""Execute one bounded slice of an approved publication job."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from continuum_api.runtime.approvals import ApprovalError
from continuum_api.runtime.github_pr_publication import (
    GitHubPullRequestPublicationError,
    GitHubPullRequestService,
)
from continuum_api.runtime.github_publication import (
    GitHubBranchPublicationError,
    GitHubBranchPublicationService,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus, RuntimeRepository
from continuum_api.runtime.transitions import InvalidTransition, NextWork, TransitionService

_LOGICAL_KEY = re.compile(
    r"^publish-approved:(?P<approval>[0-9a-f]{32})(?::part:(?P<slice>0|[1-9][0-9]{0,5}))?$"
)
_PR_REMOTE_STATES = frozenset({"open", "closed", "merged"})


@dataclass(frozen=True, slots=True)
class _PublicationBinding:
    approval_id: UUID
    manifest_sha256: str
    revision: int
    branch_slice: int


class ApprovedPublicationActivityHandler:
    """Run branch publication and PR creation for one approved logical step.

    Each invocation calls the branch service once. When T31 reaches its per-call
    mutation budget, the accepted transition creates one stable continuation job
    so every lease remains bounded.
    """

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        transitions: TransitionService,
        branch_publication: GitHubBranchPublicationService,
        pull_request_publication: GitHubPullRequestService,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._transitions = transitions
        self._branch_publication = branch_publication
        self._pull_request_publication = pull_request_publication

    async def __call__(self, lease: ExecutionLease) -> Any:
        binding = self._load_binding(lease)
        if binding is None:
            # Cancellation can race a claimed job. Do not call either publication
            # service once the run has become terminal.
            raise LeaseLost("publication_run_terminal")

        digest = binding.manifest_sha256
        try:
            branch = await self._branch_publication.publish(lease, binding.approval_id, digest)
        except GitHubBranchPublicationError as error:
            return self._attention(lease, binding, "branch", error.code)
        except ApprovalError as error:
            return self._attention(lease, binding, "branch", error.code)

        if branch.status == "pending":
            next_slice = binding.branch_slice + 1
            next_work = NextWork(
                logical_key=(f"publish-approved:{binding.approval_id.hex}:part:{next_slice}"),
                activity="publish_approved",
                revision=binding.revision,
                replay_policy=ReplayPolicy.REPLAY_SAFE,
            )
            return self._transitions.commit_result(
                lease=lease,
                result={
                    "activity": "publish_approved",
                    "approval_id": str(binding.approval_id),
                    "manifest_sha256": digest,
                    "phase": "branch",
                    "status": "pending",
                    "slice": binding.branch_slice,
                },
                target_run_status=RunStatus.RUNNING,
                next_work=next_work,
            )
        if branch.status != "published" or branch.branch_status != "confirmed":
            return self._attention(lease, binding, "branch", _safe_status(branch.status))

        try:
            pull_request = await self._pull_request_publication.publish(
                lease, binding.approval_id, digest
            )
        except GitHubPullRequestPublicationError as error:
            return self._attention(lease, binding, "pull_request", error.code)
        except ApprovalError as error:
            return self._attention(lease, binding, "pull_request", error.code)

        if _is_successful_pull_request(pull_request):
            try:
                return self._transitions.complete_publication(lease, binding.approval_id, digest)
            except InvalidTransition:
                return self._attention(
                    lease, binding, "completion", "publication_completion_unresolved"
                )
        return self._attention(lease, binding, "pull_request", _safe_status(pull_request.status))

    def _load_binding(self, lease: ExecutionLease) -> _PublicationBinding | None:
        if lease.owner_id != self._principal.account_id:
            raise LeaseLost("publication_owner_mismatch")
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = (
                connection.execute(
                    text(
                        "SELECT step.logical_key, step.activity, step.revision, "
                        "run.status AS run_status, run.revision AS run_revision, "
                        "approval.status AS approval_status, "
                        "approval.revision AS approval_revision, "
                        "encode(approval.manifest_sha256, 'hex') AS manifest_sha256 "
                        "FROM runtime_private.steps AS step "
                        "JOIN runtime_private.runs AS run "
                        "ON run.owner_id=step.owner_id AND run.run_id=step.run_id "
                        "JOIN runtime_private.approvals AS approval "
                        "ON approval.owner_id=step.owner_id AND approval.run_id=step.run_id "
                        "AND approval.approval_id=:approval_id "
                        "WHERE step.owner_id=:owner AND step.run_id=:run "
                        "AND step.step_id=:step"
                    ),
                    {
                        "approval_id": _approval_id_from_lease_key(connection, lease),
                        "owner": self._principal.account_id,
                        "run": lease.run_id,
                        "step": lease.step_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise LeaseLost("publication_approval_binding_missing")
        if row["activity"] != "publish_approved":
            raise LeaseLost("publication_activity_mismatch")
        match = _LOGICAL_KEY.fullmatch(row["logical_key"])
        if match is None:
            raise LeaseLost("publication_logical_key_invalid")
        if row["run_status"] != RunStatus.RUNNING.value:
            return None
        if (
            row["approval_status"] != "approved"
            or row["run_revision"] != row["approval_revision"]
            or row["revision"] != row["approval_revision"]
            or not isinstance(row["manifest_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", row["manifest_sha256"])
        ):
            raise LeaseLost("publication_approval_not_current")
        return _PublicationBinding(
            approval_id=UUID(hex=match.group("approval")),
            manifest_sha256=row["manifest_sha256"],
            revision=row["approval_revision"],
            branch_slice=int(match.group("slice") or "0"),
        )

    def _attention(
        self,
        lease: ExecutionLease,
        binding: _PublicationBinding,
        phase: str,
        reason: str,
    ) -> Any:
        return self._transitions.commit_result(
            lease=lease,
            result={
                "activity": "publish_approved",
                "approval_id": str(binding.approval_id),
                "manifest_sha256": binding.manifest_sha256,
                "phase": phase,
                "status": "needs_attention",
                "reason": _safe_status(reason),
            },
            target_run_status=RunStatus.NEEDS_ATTENTION,
        )


def _approval_id_from_lease_key(connection: Any, lease: ExecutionLease) -> UUID:
    row = (
        connection.execute(
            text(
                "SELECT logical_key FROM runtime_private.steps "
                "WHERE owner_id=:owner AND run_id=:run AND step_id=:step"
            ),
            {"owner": lease.owner_id, "run": lease.run_id, "step": lease.step_id},
        )
        .mappings()
        .one_or_none()
    )
    if row is None or not isinstance(row["logical_key"], str):
        raise LeaseLost("publication_logical_key_missing")
    match = _LOGICAL_KEY.fullmatch(row["logical_key"])
    if match is None:
        raise LeaseLost("publication_logical_key_invalid")
    return UUID(hex=match.group("approval"))


def _safe_status(value: Any) -> str:
    return value if isinstance(value, str) and re.fullmatch(r"[a-z_]{1,64}", value) else "unknown"


def _is_successful_pull_request(receipt: Any) -> bool:
    if receipt.remote_state not in _PR_REMOTE_STATES:
        return False
    if receipt.status in {"created", "adopted"}:
        return True
    return receipt.status in {"closed", "merged"} and receipt.status == receipt.remote_state
