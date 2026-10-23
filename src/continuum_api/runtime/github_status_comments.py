"""Owner-scoped status-comment effects with one fenced dispatch and reconciliation."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

from sqlalchemy import Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.runtime.approvals import ApprovalError, ApprovalService
from continuum_api.runtime.github_command_execution import status_comment_body
from continuum_api.runtime.persistence import Principal, RuntimeRepository


class GitHubStatusCommentError(RuntimeError):
    """Sanitized failure while publishing or reconciling a public run status."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class StatusCommentEffect:
    owner_id: UUID
    run_id: UUID
    event_sequence: int
    repository_id: int
    issue_number: int
    status_snapshot: str
    revision: int
    marker: str
    body: str
    request_sha256: bytes
    effect_status: str
    dispatch_token: UUID | None
    remote_comment_id: str | None
    dispatch_allowed: bool = False


class GitHubStatusCommentTransport(Protocol):
    async def find_status_comment(
        self, repository_id: int, github_user_id: int, issue_number: int, marker: str
    ) -> str | None: ...

    async def create_status_comment(
        self, repository_id: int, github_user_id: int, issue_number: int, body: str
    ) -> str: ...


class GitHubStatusCommentLedger:
    """Persist a narrow public-status effect, independent of run execution leases."""

    def __init__(self, engine: Engine, principal: Principal) -> None:
        self._engine = engine
        self._principal = principal

    def current_snapshot(self, run_id: UUID) -> Mapping[Any, Any]:
        """Read the owner-scoped public status and stable domain-event identity."""
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                row = (
                    connection.execute(
                        text(
                            "SELECT run.status, run.revision, users.github_user_id, "
                            "users.status AS user_status, "
                            "run.github_repository_id, run.issue_number, "
                            "COALESCE((to_jsonb(run)->>'content_expires_at')::timestamptz, "
                            "clock_timestamp()+interval '100 years') AS content_expires_at, "
                            "to_jsonb(run)->>'content_deleted_at' AS content_deleted_at, "
                            "clock_timestamp() AS database_now, "
                            "COALESCE((SELECT max(event.sequence) "
                            "FROM runtime_private.events AS event "
                            "WHERE event.owner_id=run.owner_id AND event.run_id=run.run_id "
                            "AND event.event_type NOT LIKE 'external_operation.%'),0) "
                            "AS event_sequence, approval.approval_id, approval.approval_sha256, "
                            "approval.expires_at AS approval_expires_at "
                            "FROM runtime_private.runs AS run "
                            "JOIN runtime_private.users AS users ON users.user_id=run.owner_id "
                            "JOIN runtime_private.repository_registrations AS registration "
                            "ON registration.owner_id=run.owner_id "
                            "AND registration.github_repository_id=run.github_repository_id "
                            "LEFT JOIN LATERAL (SELECT approval_id, "
                            "manifest_sha256 AS approval_sha256, expires_at "
                            "FROM runtime_private.approvals WHERE owner_id=run.owner_id "
                            "AND run_id=run.run_id AND status='pending' "
                            "ORDER BY revision DESC LIMIT 1) AS approval ON true "
                            "WHERE run.owner_id=:owner AND run.run_id=:run"
                        ),
                        {"owner": self._principal.account_id, "run": run_id},
                    )
                    .mappings()
                    .one_or_none()
                )
            if row is None or (
                row["user_status"] != "active"
                or row["content_deleted_at"] is not None
                or row["content_expires_at"] <= row["database_now"]
            ):
                raise GitHubStatusCommentError("github_status_comment_run_not_found")
            return row
        except GitHubStatusCommentError:
            raise
        except SQLAlchemyError:
            raise GitHubStatusCommentError("github_status_comment_storage_unavailable") from None

    def prepare(
        self,
        *,
        run_id: UUID,
        repository_id: int,
        issue_number: int,
        status: str,
        revision: int,
        event_sequence: int,
        marker: str,
        body: str,
    ) -> StatusCommentEffect:
        request = {
            "body": body,
            "event_sequence": event_sequence,
            "issue_number": issue_number,
            "marker": marker,
            "repository_id": repository_id,
            "revision": revision,
            "run_id": str(run_id),
            "status": status,
        }
        encoded = json.dumps(request, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        request_sha256 = hashlib.sha256(encoded.encode("utf-8")).digest()
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                self._assert_current_snapshot(
                    connection,
                    self._principal.account_id,
                    run_id,
                    repository_id,
                    issue_number,
                    status,
                    revision,
                    event_sequence,
                )
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.github_status_comment_effects "
                        "(owner_id, run_id, event_sequence, repository_id, issue_number, "
                        "status_snapshot, revision, marker, body, request_sha256) "
                        "VALUES (:owner, :run, :sequence, :repo, :issue, :status, :revision, "
                        ":marker, :body, :digest) "
                        "ON CONFLICT (owner_id, run_id, event_sequence) DO NOTHING"
                    ),
                    {
                        "owner": self._principal.account_id,
                        "run": run_id,
                        "sequence": event_sequence,
                        "repo": repository_id,
                        "issue": issue_number,
                        "status": status,
                        "revision": revision,
                        "marker": marker,
                        "body": body,
                        "digest": request_sha256,
                    },
                )
                row = self._read_locked(connection, run_id, event_sequence)
                if row["request_sha256"] != request_sha256:
                    raise GitHubStatusCommentError("github_status_comment_identity_conflict")
                return _receipt(row)
        except GitHubStatusCommentError:
            raise
        except SQLAlchemyError:
            raise GitHubStatusCommentError("github_status_comment_storage_unavailable") from None

    def authorize_dispatch(self, effect: StatusCommentEffect) -> StatusCommentEffect:
        """Grant one send only while this public snapshot remains current."""
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                row = self._read_locked(connection, effect.run_id, effect.event_sequence)
                if bytes(row["request_sha256"]) != effect.request_sha256:
                    raise GitHubStatusCommentError("github_status_comment_identity_conflict")
                if row["effect_status"] != "prepared":
                    return _receipt(row)
                self._assert_current_snapshot(
                    connection,
                    self._principal.account_id,
                    effect.run_id,
                    effect.repository_id,
                    effect.issue_number,
                    effect.status_snapshot,
                    effect.revision,
                    effect.event_sequence,
                )
                token = uuid4()
                updated = (
                    connection.execute(
                        text(
                            "UPDATE runtime_private.github_status_comment_effects SET "
                            "effect_status='dispatching', dispatch_token=:token, "
                            "dispatched_at=clock_timestamp(), updated_at=clock_timestamp() "
                            "WHERE owner_id=:owner AND run_id=:run "
                            "AND event_sequence=:sequence "
                            "AND effect_status='prepared' RETURNING *"
                        ),
                        {
                            "owner": self._principal.account_id,
                            "run": effect.run_id,
                            "sequence": effect.event_sequence,
                            "token": token,
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
                if updated is None:
                    return _receipt(
                        self._read_locked(connection, effect.run_id, effect.event_sequence)
                    )
                receipt = _receipt(updated)
                return replace(receipt, dispatch_allowed=True)
        except GitHubStatusCommentError:
            raise
        except SQLAlchemyError:
            raise GitHubStatusCommentError("github_status_comment_storage_unavailable") from None

    def adopt_existing(
        self, effect: StatusCommentEffect, remote_comment_id: str
    ) -> StatusCommentEffect:
        return self._record(effect, None, remote_comment_id, unknown=False)

    def read_effect(self, run_id: UUID, event_sequence: int) -> StatusCommentEffect:
        """Read one owner-scoped status effect for lookup-only reconciliation."""
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                row = self._read_locked(connection, run_id, event_sequence)
                return _receipt(row)
        except GitHubStatusCommentError:
            raise
        except SQLAlchemyError:
            raise GitHubStatusCommentError("github_status_comment_storage_unavailable") from None

    def observe_created(
        self, effect: StatusCommentEffect, dispatch_token: UUID, remote_comment_id: str
    ) -> StatusCommentEffect:
        return self._record(effect, dispatch_token, remote_comment_id, unknown=False)

    def mark_unknown(self, effect: StatusCommentEffect, dispatch_token: UUID) -> None:
        self._record(effect, dispatch_token, None, unknown=True)

    def _record(
        self,
        effect: StatusCommentEffect,
        dispatch_token: UUID | None,
        remote_comment_id: str | None,
        *,
        unknown: bool,
    ) -> StatusCommentEffect:
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                row = self._read_locked(connection, effect.run_id, effect.event_sequence)
                if bytes(row["request_sha256"]) != effect.request_sha256:
                    raise GitHubStatusCommentError("github_status_comment_identity_conflict")
                if row["effect_status"] == "confirmed":
                    if (
                        remote_comment_id is not None
                        and row["remote_comment_id"] != remote_comment_id
                    ):
                        raise GitHubStatusCommentError("github_status_comment_identity_conflict")
                    return _receipt(row)
                if dispatch_token is None:
                    if (
                        row["effect_status"] not in {"prepared", "dispatching", "unknown"}
                        or remote_comment_id is None
                    ):
                        raise GitHubStatusCommentError("github_status_comment_effect_unresolved")
                elif row["dispatch_token"] != dispatch_token or row["effect_status"] not in {
                    "dispatching",
                    "unknown",
                }:
                    raise GitHubStatusCommentError("github_status_comment_dispatch_conflict")
                if unknown:
                    updated = (
                        connection.execute(
                            text(
                                "UPDATE runtime_private.github_status_comment_effects SET "
                                "effect_status='unknown', updated_at=clock_timestamp() "
                                "WHERE owner_id=:owner AND run_id=:run "
                                "AND event_sequence=:sequence "
                                "AND dispatch_token=:token RETURNING *"
                            ),
                            {
                                "owner": self._principal.account_id,
                                "run": effect.run_id,
                                "sequence": effect.event_sequence,
                                "token": dispatch_token,
                            },
                        )
                        .mappings()
                        .one()
                    )
                else:
                    updated = (
                        connection.execute(
                            text(
                                "UPDATE runtime_private.github_status_comment_effects SET "
                                "effect_status='confirmed', remote_comment_id=:remote, "
                                "updated_at=clock_timestamp() WHERE owner_id=:owner "
                                "AND run_id=:run "
                                "AND event_sequence=:sequence RETURNING *"
                            ),
                            {
                                "owner": self._principal.account_id,
                                "run": effect.run_id,
                                "sequence": effect.event_sequence,
                                "remote": remote_comment_id,
                            },
                        )
                        .mappings()
                        .one()
                    )
                return _receipt(updated)
        except GitHubStatusCommentError:
            raise
        except SQLAlchemyError:
            raise GitHubStatusCommentError("github_status_comment_storage_unavailable") from None

    def _read_locked(self, connection: Any, run_id: UUID, event_sequence: int) -> Mapping[Any, Any]:
        row = (
            connection.execute(
                text(
                    "SELECT owner_id, run_id, event_sequence, repository_id, issue_number, "
                    "status_snapshot, revision, marker, body, request_sha256, effect_status, "
                    "dispatch_token, remote_comment_id FROM "
                    "runtime_private.github_status_comment_effects WHERE owner_id=:owner "
                    "AND run_id=:run AND event_sequence=:sequence FOR UPDATE"
                ),
                {
                    "owner": self._principal.account_id,
                    "run": run_id,
                    "sequence": event_sequence,
                },
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise GitHubStatusCommentError("github_status_comment_effect_not_found")
        return cast(Mapping[Any, Any], row)

    @staticmethod
    def _assert_current_snapshot(
        connection: Any,
        owner_id: UUID,
        run_id: UUID,
        repository_id: int,
        issue_number: int,
        status: str,
        revision: int,
        event_sequence: int,
    ) -> None:
        row = (
            connection.execute(
                text(
                    "SELECT run.status, run.revision, run.github_repository_id, run.issue_number, "
                    "users.status AS user_status, EXISTS (SELECT 1 FROM "
                    "runtime_private.repository_registrations AS registration "
                    "WHERE registration.owner_id=run.owner_id "
                    "AND registration.github_repository_id=run.github_repository_id) "
                    "AS registration_active, "
                    "COALESCE((to_jsonb(run)->>'content_expires_at')::timestamptz, "
                    "clock_timestamp()+interval '100 years') AS content_expires_at, "
                    "to_jsonb(run)->>'content_deleted_at' AS content_deleted_at, "
                    "clock_timestamp() AS database_now, "
                    "COALESCE((SELECT max(event.sequence) FROM runtime_private.events AS event "
                    "WHERE event.owner_id=run.owner_id AND event.run_id=run.run_id "
                    "AND event.event_type NOT LIKE 'external_operation.%'),0) AS event_sequence "
                    "FROM runtime_private.runs AS run JOIN runtime_private.users AS users "
                    "ON users.user_id=run.owner_id WHERE run.owner_id=:owner "
                    "AND run.run_id=:run"
                ),
                {"owner": owner_id, "run": run_id},
            )
            .mappings()
            .one_or_none()
        )
        if row is None or (
            row["user_status"] != "active"
            or not row["registration_active"]
            or row["content_deleted_at"] is not None
            or row["content_expires_at"] <= row["database_now"]
            or row["status"] != status
            or row["revision"] != revision
            or row["github_repository_id"] != repository_id
            or row["issue_number"] != issue_number
            or row["event_sequence"] != event_sequence
        ):
            raise GitHubStatusCommentError("github_status_comment_run_changed")


class GitHubStatusCommentPublisher:
    """Create safe public status comments without an execution lease."""

    def __init__(
        self,
        principal: Principal,
        ledger: GitHubStatusCommentLedger,
        approvals: ApprovalService,
        transport: GitHubStatusCommentTransport,
        console_url: str,
    ) -> None:
        if not _valid_console_url(console_url):
            raise ValueError("github_status_comment_console_url_invalid")
        self._principal = principal
        self._ledger = ledger
        self._approvals = approvals
        self._transport = transport
        self._console_url = console_url.rstrip("/")

    async def publish(
        self,
        *,
        run_id: UUID,
        repository_id: int,
        issue_number: int,
        status: str,
        revision: int,
        expected_event_sequence: int | None = None,
    ) -> str:
        context = self._run_binding(run_id, repository_id, issue_number, status, revision)
        if (
            expected_event_sequence is not None
            and context["event_sequence"] != expected_event_sequence
        ):
            raise GitHubStatusCommentError("github_status_comment_run_changed")
        try:
            await self._approvals.assert_current_repository_write_access(
                repository_id, context["github_user_id"]
            )
        except ApprovalError as error:
            raise GitHubStatusCommentError(error.code) from None
        body = status_comment_body(
            run_id,
            status,
            revision,
            self._console_url,
            event_sequence=context["event_sequence"],
            approval_id=context["approval_id"],
            approval_sha256=context["approval_sha256"],
            approval_expires_at=context["approval_expires_at"],
        )
        marker = body.rsplit("\n\n", 1)[-1]
        effect = self._ledger.prepare(
            run_id=run_id,
            repository_id=repository_id,
            issue_number=issue_number,
            status=status,
            revision=revision,
            event_sequence=context["event_sequence"],
            marker=marker,
            body=body,
        )
        if effect.effect_status == "confirmed":
            assert effect.remote_comment_id is not None
            return effect.remote_comment_id

        existing = await self._transport.find_status_comment(
            repository_id, context["github_user_id"], issue_number, marker
        )
        if existing is not None:
            adopted = self._ledger.adopt_existing(effect, existing)
            assert adopted.remote_comment_id is not None
            return adopted.remote_comment_id
        if effect.effect_status != "prepared":
            raise GitHubStatusCommentError("github_status_comment_effect_unresolved")

        dispatch = self._ledger.authorize_dispatch(effect)
        if not dispatch.dispatch_allowed or dispatch.dispatch_token is None:
            raise GitHubStatusCommentError("github_status_comment_dispatch_unavailable")
        try:
            remote_id = await self._transport.create_status_comment(
                repository_id, context["github_user_id"], issue_number, body
            )
        except Exception:
            self._ledger.mark_unknown(dispatch, dispatch.dispatch_token)
            raise GitHubStatusCommentError("github_status_comment_effect_unknown") from None
        observed = self._ledger.observe_created(dispatch, dispatch.dispatch_token, remote_id)
        assert observed.remote_comment_id is not None
        return observed.remote_comment_id

    async def reconcile(self, run_id: UUID, event_sequence: int) -> str:
        """Look up an earlier send without ever issuing a new remote create."""
        effect = self._ledger.read_effect(run_id, event_sequence)
        # The run must still belong to this active owner, retain its GitHub
        # binding, and remain within retention. Its status/revision/event may
        # have advanced since this effect was prepared, so do not require the
        # old snapshot to remain current.
        context = self._ledger.current_snapshot(run_id)
        if (
            context["github_repository_id"] != effect.repository_id
            or context["issue_number"] != effect.issue_number
        ):
            raise GitHubStatusCommentError("github_status_comment_run_changed")

        if effect.effect_status == "confirmed":
            assert effect.remote_comment_id is not None
            return effect.remote_comment_id
        if effect.effect_status not in {"unknown", "dispatching"}:
            raise GitHubStatusCommentError("github_status_comment_effect_unresolved")
        try:
            await self._approvals.assert_current_repository_write_access(
                effect.repository_id, context["github_user_id"]
            )
        except ApprovalError as error:
            raise GitHubStatusCommentError(error.code) from None

        existing = await self._transport.find_status_comment(
            effect.repository_id,
            context["github_user_id"],
            effect.issue_number,
            effect.marker,
        )
        if existing is None:
            raise GitHubStatusCommentError("github_status_comment_effect_unresolved")
        adopted = self._ledger.adopt_existing(effect, existing)
        assert adopted.remote_comment_id is not None
        return adopted.remote_comment_id

    def _run_binding(
        self, run_id: UUID, repository_id: int, issue_number: int, status: str, revision: int
    ) -> Mapping[Any, Any]:
        row = self._ledger.current_snapshot(run_id)
        if (
            row["status"] != status
            or row["revision"] != revision
            or row["github_repository_id"] != repository_id
            or row["issue_number"] != issue_number
            or (status == "waiting_approval" and row["approval_id"] is None)
        ):
            raise GitHubStatusCommentError("github_status_comment_run_changed")
        return {
            "github_user_id": row["github_user_id"],
            "event_sequence": row["event_sequence"],
            "approval_id": row["approval_id"] if status == "waiting_approval" else None,
            "approval_sha256": None
            if row["approval_sha256"] is None or status != "waiting_approval"
            else bytes(row["approval_sha256"]).hex(),
            "approval_expires_at": (
                row["approval_expires_at"] if status == "waiting_approval" else None
            ),
        }


def _receipt(row: Mapping[Any, Any]) -> StatusCommentEffect:
    return StatusCommentEffect(
        owner_id=row["owner_id"],
        run_id=row["run_id"],
        event_sequence=row["event_sequence"],
        repository_id=row["repository_id"],
        issue_number=row["issue_number"],
        status_snapshot=row["status_snapshot"],
        revision=row["revision"],
        marker=row["marker"],
        body=row["body"],
        request_sha256=bytes(row["request_sha256"]),
        effect_status=row["effect_status"],
        dispatch_token=row["dispatch_token"],
        remote_comment_id=row["remote_comment_id"],
    )


def _valid_console_url(value: str) -> bool:
    from urllib.parse import urlsplit

    try:
        parts = urlsplit(value)
        return (
            parts.scheme == "https"
            and bool(parts.hostname)
            and parts.username is None
            and parts.password is None
            and not parts.query
            and not parts.fragment
        )
    except ValueError:
        return False
