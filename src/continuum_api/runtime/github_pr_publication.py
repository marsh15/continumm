"""Create or reconcile the single approved GitHub pull request for a run."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from continuum_api.publication.github_pr import (
    GitHubPullRequest,
)
from continuum_api.runtime.approvals import ApprovalError, ApprovalService
from continuum_api.runtime.external_ledger import (
    ExternalOperationError,
    ExternalOperationReceipt,
    ExternalOperationService,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost
from continuum_api.runtime.persistence import EventActor

OPERATION_KEY = "github:pull-request"
_REFERENCE_OBSERVATION_KEY = "pull-request-reference"
_UNKNOWN_OBSERVATION_KEY = "pull-request-unknown"


class GitHubPullRequestPublicationError(RuntimeError):
    """Sanitized failure from trusted pull-request publication orchestration."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class GitHubPullRequestTransport(Protocol):
    async def list_pull_requests(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> tuple[GitHubPullRequest, ...]: ...

    async def get_pull_request(
        self,
        repository_id: int,
        github_user_id: int,
        number: int,
        *,
        expected_owner: str,
        expected_name: str,
    ) -> GitHubPullRequest: ...

    async def create_pull_request(
        self,
        repository_id: int,
        github_user_id: int,
        *,
        expected_owner: str,
        expected_name: str,
        expected_base_branch: str,
        expected_base_sha: str,
        expected_head_branch: str,
        expected_head_sha: str,
        title: str,
        body: str,
        marker: str,
    ) -> GitHubPullRequest: ...


@dataclass(frozen=True, slots=True)
class GitHubPullRequestReceipt:
    approval_id: UUID
    run_id: UUID
    operation_key: str
    status: str
    pr_number: int | None = None
    pr_id: int | None = None
    html_url: str | None = None
    remote_state: str | None = None


@dataclass(frozen=True, slots=True)
class _ExpectedPR:
    repository_id: int
    github_user_id: int
    owner: str
    name: str
    base_branch: str
    base_sha: str
    head_branch: str
    head_sha: str
    title: str
    body: str
    marker: str

    @classmethod
    def from_snapshot(cls, snapshot: Any) -> _ExpectedPR:
        manifest = snapshot.manifest
        try:
            repository = manifest["repository"]
            base = manifest["base"]
            branch = manifest["branch"]
            commit = manifest["commit"]
            pull_request = manifest["pull_request"]
            expected = cls(
                repository_id=snapshot.repository_id,
                github_user_id=snapshot.github_user_id,
                owner=repository["owner"],
                name=repository["name"],
                base_branch=pull_request["base"],
                base_sha=base["commit_sha"],
                head_branch=pull_request["head"],
                head_sha=commit["sha"],
                title=pull_request["title"],
                body=pull_request["body"],
                marker=pull_request["marker"],
            )
        except (KeyError, TypeError):
            raise GitHubPullRequestPublicationError("github_pr_manifest_invalid") from None
        if (
            type(expected.repository_id) is not int
            or type(expected.github_user_id) is not int
            or not all(
                isinstance(value, str) and value
                for value in (
                    expected.owner,
                    expected.name,
                    expected.base_branch,
                    expected.base_sha,
                    expected.head_branch,
                    expected.head_sha,
                    expected.title,
                    expected.body,
                    expected.marker,
                )
            )
            or expected.marker not in expected.body
            or not isinstance(branch, dict)
            or branch.get("name") != expected.head_branch
            or base.get("branch") != expected.base_branch
        ):
            raise GitHubPullRequestPublicationError("github_pr_manifest_invalid")
        return expected

    def metadata(self) -> dict[str, str | int]:
        return {
            "operation": "create_pull_request",
            "repository_id": self.repository_id,
            "owner": self.owner,
            "name": self.name,
            "title": self.title,
            "body": self.body,
            "base_branch": self.base_branch,
            "base_sha": self.base_sha,
            "head_branch": self.head_branch,
            "head_sha": self.head_sha,
            "marker": self.marker,
        }


class GitHubPullRequestService:
    """Create once, then reconcile only through bounded remote reads."""

    def __init__(
        self,
        approvals: ApprovalService,
        ledger: ExternalOperationService,
        transport: GitHubPullRequestTransport,
    ) -> None:
        self._approvals = approvals
        self._ledger = ledger
        self._transport = transport

    async def publish(
        self, lease: ExecutionLease, approval_id: UUID, manifest_sha256: str
    ) -> GitHubPullRequestReceipt:
        snapshot = self._snapshot(approval_id, manifest_sha256)
        if snapshot.run_id != lease.run_id or lease.owner_id != snapshot.owner_id:
            raise GitHubPullRequestPublicationError("github_pr_execution_mismatch")
        expected = _ExpectedPR.from_snapshot(snapshot)
        try:
            receipt = self._ledger.prepare(
                lease,
                OPERATION_KEY,
                expected.metadata(),
                approval_id=snapshot.approval_id,
                approval_sha256=snapshot.manifest_sha256,
            )
        except (ExternalOperationError, LeaseLost) as error:
            raise GitHubPullRequestPublicationError(_error_code(error)) from None
        try:
            return await self._run(expected, snapshot, receipt, lease=lease, may_dispatch=True)
        except GitHubPullRequestPublicationError as error:
            if error.code == "github_pr_conflict":
                return self._result(snapshot, "conflict")
            raise

    async def reconcile(self, approval_id: UUID, manifest_sha256: str) -> GitHubPullRequestReceipt:
        """Read remote state for an existing operation without a lease or writes."""
        snapshot = self._snapshot(approval_id, manifest_sha256)
        expected = _ExpectedPR.from_snapshot(snapshot)
        receipt = self._ledger.read(snapshot.run_id, OPERATION_KEY)
        if receipt is None:
            return GitHubPullRequestReceipt(
                snapshot.approval_id, snapshot.run_id, OPERATION_KEY, "not_started"
            )
        self._validate_operation(snapshot, expected, receipt)
        try:
            return await self._run(expected, snapshot, receipt, lease=None, may_dispatch=False)
        except GitHubPullRequestPublicationError as error:
            if error.code == "github_pr_conflict":
                return self._result(snapshot, "conflict")
            raise

    def _snapshot(self, approval_id: UUID, manifest_sha256: str) -> Any:
        try:
            return self._approvals.publication_snapshot(approval_id, manifest_sha256)
        except ApprovalError as error:
            raise GitHubPullRequestPublicationError(error.code) from None

    async def _run(
        self,
        expected: _ExpectedPR,
        snapshot: Any,
        receipt: ExternalOperationReceipt,
        *,
        lease: ExecutionLease | None,
        may_dispatch: bool,
    ) -> GitHubPullRequestReceipt:
        self._validate_operation(snapshot, expected, receipt)
        if receipt.status == "conflict":
            return self._result(snapshot, "conflict")
        reference = self._stored_reference(snapshot.run_id)
        stored_lookup_failed = False
        stored_pr: GitHubPullRequest | None = None
        if reference is not None:
            try:
                stored_pr = await self._transport.get_pull_request(
                    expected.repository_id,
                    expected.github_user_id,
                    reference[0],
                    expected_owner=expected.owner,
                    expected_name=expected.name,
                )
            except Exception:
                stored_lookup_failed = True

        try:
            listed = await self._transport.list_pull_requests(
                expected.repository_id,
                expected.github_user_id,
                expected_owner=expected.owner,
                expected_name=expected.name,
            )
        except Exception:
            if receipt.status in {"dispatching", "unknown"}:
                await self._record_unknown(snapshot, receipt)
                return self._result(snapshot, "unknown")
            return self._result(snapshot, "unavailable")

        rows_by_id: dict[int, GitHubPullRequest] = {}
        for row in listed:
            previous = rows_by_id.get(row.id)
            if previous is not None and previous != row:
                await self._record_conflict(snapshot, receipt, [previous, row])
                return self._result(snapshot, "conflict")
            rows_by_id[row.id] = row
        rows = list(rows_by_id.values())
        if stored_pr is not None and all(row.number != stored_pr.number for row in rows):
            rows.append(stored_pr)
        elif stored_pr is not None:
            listed_same_number = next(row for row in rows if row.number == stored_pr.number)
            if _identity_facts(listed_same_number) != _identity_facts(stored_pr):
                await self._record_conflict(snapshot, receipt, [stored_pr, listed_same_number])
                return self._result(snapshot, "conflict")
            rows = [stored_pr if row.number == stored_pr.number else row for row in rows]
        if (
            reference is not None
            and stored_lookup_failed
            and all(row.number != reference[0] for row in rows)
        ):
            # The broad list is complete, but the stored identity itself was not
            # independently readable. Do not infer that it no longer exists.
            return self._result(snapshot, "unknown", reference=reference)

        if reference is not None:
            for row in rows:
                if row.number == reference[0] and (
                    row.id != reference[1] or row.html_url != reference[2]
                ):
                    await self._record_conflict(snapshot, receipt, [row])
                    return self._pr_result(snapshot, "conflict", row)

        matches = [row for row in rows if _is_relevant(row, expected, reference)]
        exact = [row for row in matches if _is_exact(row, expected)]
        if len(matches) > 1 or len(exact) > 1 or (matches and len(exact) != len(matches)):
            await self._record_conflict(snapshot, receipt, matches)
            return self._result(snapshot, "conflict")
        if exact:
            pr = exact[0]
            if receipt.status == "prepared":
                try:
                    receipt = self._ledger.adopt_existing(
                        receipt,
                        observation_key=_REFERENCE_OBSERVATION_KEY,
                        details={"number": pr.number, "id": pr.id, "url": pr.html_url},
                    )
                except ExternalOperationError as error:
                    raise GitHubPullRequestPublicationError(_error_code(error)) from None
            await self._record_reference(snapshot, receipt, pr)
            await self._record_remote_state(snapshot, receipt, pr)
            return self._pr_result(snapshot, _observed_status(pr), pr)

        if receipt.status == "conflict":
            return self._result(snapshot, "conflict")
        if receipt.status == "definitely_failed":
            return self._result(snapshot, "definitely_failed")
        if receipt.status in {"dispatching", "unknown", "confirmed"}:
            if receipt.status in {"dispatching", "unknown"}:
                await self._record_unknown(snapshot, receipt)
            return self._result(snapshot, "unknown")
        if not may_dispatch or lease is None:
            return self._result(snapshot, "not_dispatched")

        try:
            await self._approvals.authorize_dispatch(
                snapshot.approval_id,
                snapshot.manifest_sha256.hex(),
                operation="create_pull_request",
                lease=lease,
            )
            dispatch = self._ledger.authorize_dispatch(lease, OPERATION_KEY)
        except (ApprovalError, ExternalOperationError, LeaseLost) as error:
            return self._result(snapshot, "not_dispatched", error=_error_code(error))
        if not dispatch.dispatch_allowed:
            return self._result(snapshot, "not_dispatched")

        try:
            created = await self._transport.create_pull_request(
                expected.repository_id,
                expected.github_user_id,
                expected_owner=expected.owner,
                expected_name=expected.name,
                expected_base_branch=expected.base_branch,
                expected_base_sha=expected.base_sha,
                expected_head_branch=expected.head_branch,
                expected_head_sha=expected.head_sha,
                title=expected.title,
                body=expected.body,
                marker=expected.marker,
            )
        except Exception:
            # A transport failure after the durable dispatch grant cannot prove
            # that GitHub did not create the PR. Reconciliation stays read-only.
            await self._record_unknown(snapshot, dispatch)
            return self._result(snapshot, "unknown")

        if not _is_exact(created, expected):
            await self._record_conflict(snapshot, dispatch, [created])
            return self._pr_result(snapshot, "conflict", created)
        # Persist the returned identity before the follow-up search. The list may
        # temporarily lag a successful POST response.
        await self._record_reference(snapshot, dispatch, created)
        await self._record_remote_state(snapshot, dispatch, created)
        try:
            after_create = await self._transport.list_pull_requests(
                expected.repository_id,
                expected.github_user_id,
                expected_owner=expected.owner,
                expected_name=expected.name,
            )
        except Exception:
            # The PR is confirmed by the POST response; duplicate detection is
            # still unresolved, and the durable reference prevents another POST.
            return self._pr_result(snapshot, "unknown", created)
        post_by_id: dict[int, GitHubPullRequest] = {}
        for row in after_create:
            previous = post_by_id.get(row.id)
            if previous is not None and previous != row:
                await self._record_conflict(snapshot, dispatch, [previous, row])
                return self._result(snapshot, "conflict")
            post_by_id[row.id] = row
        listed_created = post_by_id.get(created.id)
        if listed_created is not None and _identity_facts(listed_created) != _identity_facts(
            created
        ):
            await self._record_conflict(snapshot, dispatch, [created, listed_created])
            return self._result(snapshot, "conflict")
        post_by_id.setdefault(created.id, created)
        post_matches = [row for row in post_by_id.values() if _is_relevant(row, expected, None)]
        post_exact = [row for row in post_matches if _is_exact(row, expected)]
        if len(post_matches) != 1 or len(post_exact) != 1:
            await self._record_conflict(snapshot, dispatch, post_matches)
            return self._result(snapshot, "conflict")
        pr = post_exact[0]
        return self._pr_result(
            snapshot, "created" if pr.number == created.number else "adopted", pr
        )

    def _stored_reference(self, run_id: UUID) -> tuple[int, int, str] | None:
        try:
            observations = self._ledger.read_observations(
                run_id,
                OPERATION_KEY,
                observation_key=_REFERENCE_OBSERVATION_KEY,
                limit=1,
            )
        except Exception:
            raise GitHubPullRequestPublicationError("github_pr_ledger_unavailable") from None
        if not observations:
            return None
        details = observations[0].details
        number = details.get("number")
        identifier = details.get("id")
        url = details.get("url")
        if type(number) is not int or number <= 0 or type(identifier) is not int or identifier <= 0:
            raise GitHubPullRequestPublicationError("github_pr_reference_corrupt")
        if not isinstance(url, str) or not url.startswith("https://github.com/"):
            raise GitHubPullRequestPublicationError("github_pr_reference_corrupt")
        return number, identifier, url

    async def _record_reference(
        self, snapshot: Any, dispatch: ExternalOperationReceipt, pr: GitHubPullRequest
    ) -> None:
        if dispatch.status == "prepared":
            return
        await self._observe(
            snapshot,
            dispatch,
            _REFERENCE_OBSERVATION_KEY,
            "confirmed",
            {"number": pr.number, "id": pr.id, "url": pr.html_url},
        )

    async def _record_remote_state(
        self, snapshot: Any, dispatch: ExternalOperationReceipt, pr: GitHubPullRequest
    ) -> None:
        if dispatch.status == "prepared":
            return
        state = "merged" if pr.merged else pr.state
        await self._observe(
            snapshot,
            dispatch,
            f"pull-request-state-{state}",
            "confirmed",
            {"number": pr.number, "id": pr.id, "state": state},
        )

    async def _record_unknown(self, snapshot: Any, dispatch: ExternalOperationReceipt) -> None:
        if dispatch.status == "prepared":
            return
        # Keep the observation stable so repeated negative lookups do not grow
        # the ledger or imply that a later negative proves non-creation.
        await self._observe(
            snapshot,
            dispatch,
            _UNKNOWN_OBSERVATION_KEY,
            "unknown",
            {"reason": "dispatch_outcome_unresolved"},
        )

    async def _record_conflict(
        self,
        snapshot: Any,
        dispatch: ExternalOperationReceipt,
        matches: list[GitHubPullRequest],
    ) -> None:
        identities = sorted(
            json.dumps(_identity_facts(item), separators=(",", ":")) for item in matches
        )
        fingerprint = hashlib.sha256("\n".join(identities).encode()).hexdigest()[:20]
        details = {
            "count": len(matches),
            "fingerprint": fingerprint,
            "sample_numbers": sorted(item.number for item in matches)[:20],
        }
        observation_key = f"pull-request-conflict-{fingerprint}"
        if dispatch.status == "prepared":
            self._ledger.adopt_existing(
                dispatch, observation_key=observation_key, details=details, outcome="conflict"
            )
            return
        await self._observe(snapshot, dispatch, observation_key, "conflict", details)

    async def _observe(
        self,
        snapshot: Any,
        dispatch: ExternalOperationReceipt,
        observation_key: str,
        outcome: str,
        details: dict[str, Any],
    ) -> None:
        try:
            observed = self._ledger.observe(
                run_id=snapshot.run_id,
                operation_key=OPERATION_KEY,
                dispatch_receipt=dispatch,
                observation_key=observation_key,
                outcome=outcome,
                details=details,
                actor_type=EventActor.PROVIDER,
                actor_id="github",
            )
            if observed.status == "conflict" and outcome != "conflict":
                raise GitHubPullRequestPublicationError("github_pr_conflict")
        except ExternalOperationError as error:
            # A conflict in the append-only ledger is an attention outcome; it
            # must never fall through into another create attempt.
            raise GitHubPullRequestPublicationError(_error_code(error)) from None

    def _validate_operation(
        self, snapshot: Any, expected: _ExpectedPR, receipt: ExternalOperationReceipt
    ) -> None:
        if (
            receipt.run_id != snapshot.run_id
            or receipt.operation_key != OPERATION_KEY
            or receipt.approval_id != snapshot.approval_id
            or receipt.approval_sha256 != snapshot.manifest_sha256
            or receipt.request_metadata != expected.metadata()
        ):
            raise GitHubPullRequestPublicationError("github_pr_operation_conflict")

    @staticmethod
    def _result(
        snapshot: Any,
        status: str,
        *,
        error: str | None = None,
        reference: tuple[int, int, str] | None = None,
    ) -> GitHubPullRequestReceipt:
        return GitHubPullRequestReceipt(
            approval_id=snapshot.approval_id,
            run_id=snapshot.run_id,
            operation_key=OPERATION_KEY,
            status=status,
            pr_number=reference[0] if reference else None,
            pr_id=reference[1] if reference else None,
            html_url=reference[2] if reference else None,
        )

    @staticmethod
    def _pr_result(snapshot: Any, status: str, pr: GitHubPullRequest) -> GitHubPullRequestReceipt:
        return GitHubPullRequestReceipt(
            approval_id=snapshot.approval_id,
            run_id=snapshot.run_id,
            operation_key=OPERATION_KEY,
            status=status,
            pr_number=pr.number,
            pr_id=pr.id,
            html_url=pr.html_url,
            remote_state="merged" if pr.merged else pr.state,
        )


def _is_exact(pr: GitHubPullRequest, expected: _ExpectedPR) -> bool:
    return (
        isinstance(pr.head_owner, str)
        and pr.head_owner.casefold() == expected.owner.casefold()
        and pr.head_ref == expected.head_branch
        and pr.head_sha == expected.head_sha
        and getattr(pr, "head_repository_id", None) == expected.repository_id
        and pr.base_ref == expected.base_branch
        and getattr(pr, "base_repository_id", None) == expected.repository_id
        and pr.title == expected.title
        and pr.body == expected.body
        and expected.marker in (pr.body or "")
        and pr.html_url == f"https://github.com/{expected.owner}/{expected.name}/pull/{pr.number}"
    )


def _identity_facts(pr: GitHubPullRequest) -> tuple[Any, ...]:
    return (
        pr.number,
        pr.id,
        pr.head_repository_id,
        pr.head_owner,
        pr.head_ref,
        pr.head_sha,
        pr.base_repository_id,
        pr.base_ref,
        pr.title,
        pr.body,
        pr.html_url,
    )


def _is_relevant(
    pr: GitHubPullRequest,
    expected: _ExpectedPR,
    reference: tuple[int, int, str] | None,
) -> bool:
    return (
        expected.marker in (pr.body or "")
        or (reference is not None and pr.number == reference[0])
        or (
            pr.head_ref == expected.head_branch
            and isinstance(pr.head_owner, str)
            and pr.head_owner.casefold() == expected.owner.casefold()
        )
    )


def _observed_status(pr: GitHubPullRequest) -> str:
    if pr.merged:
        return "merged"
    if pr.state == "closed":
        return "closed"
    return "adopted"


def _error_code(error: Exception) -> str:
    code = getattr(error, "code", None)
    return code if isinstance(code, str) else "github_pr_publication_failed"
