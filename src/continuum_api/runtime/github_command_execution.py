"""Execute fresh, owner-authorized GitHub commands through shared transitions."""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast
from uuid import UUID

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.runtime.admission import AdmissionError, AdmissionService
from continuum_api.runtime.approvals import ApprovalError, ApprovalService
from continuum_api.runtime.cancellation import (
    CancellationError,
    CancellationService,
    RuntimeRunNotFound,
)
from continuum_api.runtime.github_commands import (
    GitHubCommandError,
    GitHubCommandReceipt,
    NormalizedGitHubCommand,
)
from continuum_api.runtime.persistence import Principal, RuntimeRepository

_MAX_AGE = timedelta(hours=72)
_PERMANENT_COMMAND_REJECTIONS = frozenset(
    {
        "github_command_kind_unsupported",
        "github_command_target_invalid",
        "github_command_target_not_found",
        "github_command_approval_stale",
        "github_command_run_deadline_elapsed",
        "approval_digest_invalid",
        "approval_not_found",
        "approval_stale",
        "approval_expired",
        "approval_decision_conflict",
        "approval_dispatch_not_authorized",
        "approval_dispatch_deadline_elapsed",
        "approval_manifest_corrupt",
        "approval_manifest_stale_or_expired",
        "approval_manifest_digest_invalid",
        "approval_candidate_stale",
        "approval_candidate_stale_or_expired",
        "approval_candidate_lineage_mismatch",
        "approval_candidate_result_hash_mismatch",
        "approval_feedback_unexpected",
        "approval_idempotency_key_invalid",
        "admission_source_invalid",
        "admission_task_invalid",
        "admission_operator_stop",
        "public_admission_disabled",
        "repository_not_found",
        "repository_not_eligible",
        "repository_provider_default_required",
        "repository_profile_default_required",
        "repository_default_branch_invalid",
        "credential_unavailable",
        "setup_continuation_invalid",
        "setup_continuation_not_found",
        "setup_continuation_stale",
        "setup_continuation_content_expired",
        "setup_continuation_content_corrupt",
    }
)
_RETRYABLE_ERROR_MARKERS = (
    "unavailable",
    "pending",
    "timeout",
    "storage",
    "unverifiable",
    "authorization",
    "provider",
    "rate_limit",
    "request_limit",
    "response_limit",
)


@dataclass(frozen=True, slots=True)
class GitHubCommandResult:
    kind: str
    run_id: UUID | None = None
    status: str | None = None
    revision: int | None = None
    approval_id: UUID | None = None
    approval_status: str | None = None
    duplicate: bool = False

    def json(self) -> dict[str, Any]:
        value: dict[str, Any] = {"kind": self.kind}
        if self.run_id is not None:
            value["run_id"] = str(self.run_id)
        if self.status is not None:
            value["status"] = self.status
        if self.revision is not None:
            value["revision"] = self.revision
        if self.approval_id is not None:
            value["approval_id"] = str(self.approval_id)
        if self.approval_status is not None:
            value["approval_status"] = self.approval_status
        return value


class GitHubCommandProcessor:
    """Process one previously accepted command without public run admission.

    Accepted normalized intent and feedback are immutable. Current actor and
    repository rights are rechecked independently before execution; comment
    edits or deletion after acceptance do not revoke intent.
    """

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        approvals: ApprovalService,
        cancellations: CancellationService,
        admissions: AdmissionService | None = None,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._approvals = approvals
        self._cancellations = cancellations
        self._admissions = admissions

    async def execute(self, receipt: GitHubCommandReceipt) -> GitHubCommandResult:
        """Execute an accepted command once; repeated calls adopt its result."""
        try:
            return await self._execute(receipt)
        except GitHubCommandError as error:
            if not _is_permanent_rejection(error):
                raise
            return await self.record_rejection(receipt)

    async def _execute(self, receipt: GitHubCommandReceipt) -> GitHubCommandResult:
        row = await self._load_bound_command(receipt)
        command = _command_from_row(row)
        try:
            await self._approvals.assert_current_repository_write_access(
                row["repository_id"], row["github_user_id"]
            )
        except ApprovalError as error:
            raise GitHubCommandError(error.code) from None

        prior = await self._stored_result(receipt)
        if prior is not None:
            return _result_from_json(prior, duplicate=True)
        if command.kind == "run":
            if self._admissions is None:
                raise GitHubCommandError("github_command_run_unavailable")
            try:
                admitted = await self._admissions.admit_current_issue_comment(
                    repository_id=receipt.repository_id,
                    issue_number=receipt.issue_number,
                    comment_id=receipt.comment_id,
                )
            except AdmissionError as error:
                raise GitHubCommandError(error.code, retryable=error.retryable) from None
            result = GitHubCommandResult("run", admitted.run_id, admitted.status)
            await self._store_result(receipt, result)
            return result
        if command.kind == "status":
            result = await self._status(receipt, command)
        elif command.kind == "cancel":
            result = await self._cancel(receipt, command, row["accepted_at"])
        elif command.kind in {"approve", "reject", "changes"}:
            result = await self._decide(receipt, command)
        else:  # pragma: no cover - Literal makes this defensive
            raise GitHubCommandError("github_command_kind_unsupported")
        await self._store_result(receipt, result)
        return result

    async def record_rejection(self, receipt: GitHubCommandReceipt) -> GitHubCommandResult:
        """Persist one fixed permanent rejection after rechecking the bound receipt."""
        await self._load_bound_command(receipt)
        prior = await self._stored_result(receipt)
        if prior is not None:
            return _result_from_json(prior, duplicate=True)
        result = GitHubCommandResult("rejected", status="rejected")
        try:
            await self._store_result(receipt, result)
        except GitHubCommandError as error:
            if error.code != "github_command_result_conflict":
                raise
            # A concurrent execution may have durably won after our prior read.
            # Adopt that immutable result instead of inventing a second outcome.
            prior = await self._stored_result(receipt)
            if prior is None:
                raise
            return _result_from_json(prior, duplicate=True)
        return result

    async def _load_bound_command(self, receipt: GitHubCommandReceipt) -> Mapping[Any, Any]:
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                row = (
                    connection.execute(
                        text(
                            "SELECT command.repository_id, command.comment_id, command.issue_id, "
                            "command.issue_number, command.author_id, command.comment_created_at, "
                            "command.accepted_at, command.expires_at, command.command_kind, "
                            "command.command_metadata, "
                            "payload.payload, users.github_user_id, users.status AS user_status, "
                            "clock_timestamp() AS database_now "
                            "FROM runtime_private.github_command_receipts AS command "
                            "JOIN runtime_private.users AS users ON users.user_id=command.owner_id "
                            "JOIN runtime_private.repository_registrations AS registration "
                            "ON registration.owner_id=command.owner_id AND "
                            "registration.github_repository_id=command.repository_id "
                            "LEFT JOIN runtime_private.github_command_payloads AS payload "
                            "ON payload.owner_id=command.owner_id "
                            "AND payload.repository_id=command.repository_id "
                            "AND payload.comment_id=command.comment_id "
                            "WHERE command.owner_id=:owner AND command.repository_id=:repo "
                            "AND command.comment_id=:comment"
                        ),
                        {
                            "owner": self._principal.account_id,
                            "repo": receipt.repository_id,
                            "comment": receipt.comment_id,
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
            if row is None:
                raise GitHubCommandError("github_command_not_found")
            if (
                row["user_status"] != "active"
                or row["github_user_id"] != row["author_id"]
                or row["issue_number"] != receipt.issue_number
                or row["accepted_at"] != receipt.accepted_at
                or row["expires_at"] <= row["database_now"]
                or row["accepted_at"] < row["database_now"] - _MAX_AGE
            ):
                raise GitHubCommandError("github_command_authorization_changed")
            if (
                receipt.command.kind != row["command_kind"]
                or receipt.command.metadata() != row["command_metadata"]
            ):
                raise GitHubCommandError("github_command_identity_conflict")
            if row["command_kind"] == "changes" and (
                row["payload"] is None or row["payload"].get("feedback") is None
            ):
                raise GitHubCommandError("github_command_feedback_expired")
            return row
        except SQLAlchemyError:
            raise GitHubCommandError("github_command_storage_unavailable", retryable=True) from None

    async def _status(
        self, receipt: GitHubCommandReceipt, command: NormalizedGitHubCommand
    ) -> GitHubCommandResult:
        assert command.target_id is not None
        try:
            run_id = UUID(command.target_id)
        except ValueError:
            raise GitHubCommandError("github_command_target_invalid") from None
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                row = (
                    connection.execute(
                        text(
                            "SELECT run.status, run.revision FROM runtime_private.runs AS run "
                            "WHERE run.owner_id=:owner AND run.run_id=:run "
                            "AND run.github_repository_id=:repo AND run.issue_number=:issue"
                        ),
                        {
                            "owner": self._principal.account_id,
                            "run": run_id,
                            "repo": receipt.repository_id,
                            "issue": receipt.issue_number,
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
            if row is None:
                raise GitHubCommandError("github_command_target_not_found")
            return GitHubCommandResult("status", run_id, row["status"], row["revision"])
        except SQLAlchemyError:
            raise GitHubCommandError("github_command_storage_unavailable", retryable=True) from None

    async def _cancel(
        self,
        receipt: GitHubCommandReceipt,
        command: NormalizedGitHubCommand,
        accepted_at: datetime,
    ) -> GitHubCommandResult:
        assert command.target_id is not None
        try:
            run_id = UUID(command.target_id)
            self._assert_run_repository(run_id, receipt.repository_id, receipt.issue_number)
            cancelled = await asyncio.to_thread(
                self._cancellations.cancel_accepted_command, run_id, accepted_at
            )
            return GitHubCommandResult("cancel", run_id, cancelled.run_status.value)
        except RuntimeRunNotFound:
            raise GitHubCommandError("github_command_target_not_found") from None
        except CancellationError as error:
            code = str(error) or "github_command_cancel_failed"
            raise GitHubCommandError(code) from None
        except ValueError:
            raise GitHubCommandError("github_command_target_invalid") from None

    async def _decide(
        self, receipt: GitHubCommandReceipt, command: NormalizedGitHubCommand
    ) -> GitHubCommandResult:
        assert command.target_id is not None
        try:
            approval_id = UUID(command.target_id)
            binding = await asyncio.to_thread(
                self._approval_binding,
                approval_id,
                receipt.repository_id,
                receipt.issue_number,
            )
            if command.kind == "approve":
                if command.action_sha256 != binding["manifest_sha256"]:
                    raise GitHubCommandError("approval_digest_invalid")
            decision = cast(Any, command.kind)
            accepted_feedback = command.feedback if decision == "changes" else None
            approved = await self._approvals.decide(
                approval_id=approval_id,
                manifest_sha256=binding["manifest_sha256"],
                decision=decision,
                feedback=accepted_feedback,
                idempotency_key=f"github:{receipt.repository_id}:{receipt.comment_id}",
            )
            return GitHubCommandResult(
                "decision",
                approved.run_id,
                approval_status=approved.status,
                approval_id=approval_id,
            )
        except ApprovalError as error:
            raise GitHubCommandError(error.code) from None
        except (ValueError, TypeError):
            raise GitHubCommandError("github_command_target_invalid") from None

    def _assert_run_repository(self, run_id: UUID, repository_id: int, issue_number: int) -> None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = connection.execute(
                text(
                    "SELECT run_id FROM runtime_private.runs WHERE owner_id=:owner "
                    "AND run_id=:run AND github_repository_id=:repo AND issue_number=:issue"
                ),
                {
                    "owner": self._principal.account_id,
                    "run": run_id,
                    "repo": repository_id,
                    "issue": issue_number,
                },
            ).one_or_none()
        if row is None:
            raise GitHubCommandError("github_command_target_not_found")

    def _approval_binding(
        self, approval_id: UUID, repository_id: int, issue_number: int
    ) -> Mapping[Any, Any]:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = (
                connection.execute(
                    text(
                        "SELECT approval.run_id, approval.status, "
                        "encode(approval.manifest_sha256, 'hex') AS manifest_sha256, "
                        "run.deadline_at, run.revision, run.status AS run_status, "
                        "run.github_repository_id AS repository_id, run.issue_number, "
                        "clock_timestamp() AS database_now "
                        "FROM runtime_private.approvals AS approval "
                        "JOIN runtime_private.runs AS run "
                        "USING (owner_id, run_id) WHERE approval.owner_id=:owner "
                        "AND approval.approval_id=:approval"
                    ),
                    {"owner": self._principal.account_id, "approval": approval_id},
                )
                .mappings()
                .one_or_none()
            )
        if (
            row is None
            or row["repository_id"] != repository_id
            or row["issue_number"] != issue_number
            or row["status"] != "pending"
            or row["run_status"] != "waiting_approval"
            or row["deadline_at"] is None
            or row["deadline_at"] <= row["database_now"]
        ):
            raise GitHubCommandError("github_command_approval_stale")
        return row

    async def _stored_result(self, receipt: GitHubCommandReceipt) -> Mapping[str, Any] | None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            result = connection.execute(
                text(
                    "SELECT result FROM runtime_private.github_command_results "
                    "WHERE owner_id=:owner AND repository_id=:repo AND comment_id=:comment"
                ),
                {
                    "owner": self._principal.account_id,
                    "repo": receipt.repository_id,
                    "comment": receipt.comment_id,
                },
            ).scalar_one_or_none()
        return cast(Mapping[str, Any] | None, result)

    async def _store_result(
        self, receipt: GitHubCommandReceipt, result: GitHubCommandResult
    ) -> None:
        encoded = json.dumps(result.json(), sort_keys=True, separators=(",", ":"))
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = connection.execute(
                text(
                    "INSERT INTO runtime_private.github_command_results "
                    "(owner_id, repository_id, comment_id, result) "
                    "VALUES (:owner, :repo, :comment, CAST(:result AS jsonb)) "
                    "ON CONFLICT (owner_id, repository_id, comment_id) DO NOTHING "
                    "RETURNING result"
                ),
                {
                    "owner": self._principal.account_id,
                    "repo": receipt.repository_id,
                    "comment": receipt.comment_id,
                    "result": encoded,
                },
            ).scalar_one_or_none()
            if row is None:
                prior = connection.execute(
                    text(
                        "SELECT result FROM runtime_private.github_command_results "
                        "WHERE owner_id=:owner AND repository_id=:repo AND comment_id=:comment"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "repo": receipt.repository_id,
                        "comment": receipt.comment_id,
                    },
                ).scalar_one_or_none()
                if prior != result.json():
                    raise GitHubCommandError("github_command_result_conflict")


def status_comment_body(
    run_id: UUID,
    status: str,
    revision: int,
    console_url: str,
    *,
    event_sequence: int,
    approval_id: UUID | None = None,
    approval_sha256: str | None = None,
    approval_expires_at: datetime | None = None,
) -> str:
    """Build a stable public status comment containing no run-private content."""
    if (
        status
        not in {
            "queued",
            "running",
            "waiting_approval",
            "waiting_credentials",
            "waiting_capacity",
            "needs_attention",
            "succeeded",
            "failed",
            "cancelled",
            "expired",
        }
        or type(revision) is not int
        or revision < 0
        or type(event_sequence) is not int
        or event_sequence < 0
        or not _valid_console_url(console_url)
    ):
        raise ValueError("github_status_comment_input_invalid")
    lines = [
        f"Continuum run `{run_id}` status: **{status}** (revision {revision}).",
        f"[Open run in Continuum]({console_url.rstrip('/')}/runs/{run_id})",
    ]
    if approval_id is not None:
        if (
            approval_sha256 is None
            or re.fullmatch(r"[0-9a-f]{64}", approval_sha256) is None
            or approval_expires_at is None
            or approval_expires_at.tzinfo is None
        ):
            raise ValueError("github_status_comment_approval_invalid")
        lines.append(
            f"Approval `{approval_id}` · SHA-256 `{approval_sha256}` · "
            f"expires `{approval_expires_at.isoformat()}`"
        )
    marker = f"<!-- continuum:status:{run_id}:{event_sequence} -->"
    return "\n\n".join(["\n".join(lines), marker])


def _command_from_row(row: Mapping[str, Any]) -> NormalizedGitHubCommand:
    metadata = row["command_metadata"]
    payload = row["payload"] or {}
    return NormalizedGitHubCommand(
        kind=metadata["kind"],
        target_id=metadata.get("target_id"),
        action_sha256=metadata.get("action_sha256"),
        feedback=payload.get("feedback"),
    )


def _valid_console_url(value: str) -> bool:
    from urllib.parse import urlsplit

    try:
        parsed = urlsplit(value)
        return (
            parsed.scheme == "https"
            and parsed.hostname is not None
            and parsed.username is None
            and parsed.password is None
            and parsed.query == ""
            and parsed.fragment == ""
        )
    except ValueError:
        return False


def _is_permanent_rejection(error: GitHubCommandError) -> bool:
    code = error.code
    return (
        error.retryable is False
        and code in _PERMANENT_COMMAND_REJECTIONS
        and not any(marker in code for marker in _RETRYABLE_ERROR_MARKERS)
    )


def _result_from_json(value: Mapping[str, Any], *, duplicate: bool) -> GitHubCommandResult:
    return GitHubCommandResult(
        kind=value["kind"],
        run_id=UUID(value["run_id"]) if value.get("run_id") else None,
        status=value.get("status"),
        revision=value.get("revision"),
        approval_id=UUID(value["approval_id"]) if value.get("approval_id") else None,
        approval_status=value.get("approval_status"),
        duplicate=duplicate,
    )
