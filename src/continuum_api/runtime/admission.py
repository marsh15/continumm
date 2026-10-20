"""Atomic public run admission over owner-scoped registrations and credentials."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.auth.github import GitHubIssueSnapshot
from continuum_api.credentials import CredentialProvider
from continuum_api.repositories.preflight import (
    EligibilityReceipt,
    RepositoryAccessProof,
    RepositoryPreflightError,
)
from continuum_api.runtime.agent_context import AgentContextError, canonicalize_initial_context
from continuum_api.runtime.approvals import RepositoryPublicationFacts
from continuum_api.runtime.persistence import EventActor, Principal, ReplayPolicy, RuntimeRepository
from continuum_api.runtime.provider_credentials import CredentialService

_SHA = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_OPENAI_MODEL = "gpt-5.4-mini-2026-03-17"
_ANTHROPIC_MODEL = "claude-sonnet-5-5"
_ROUTE = "/api/v1/runs"
_MAX_TASK_BYTES = 16_384
_MAX_IDEMPOTENCY_KEY = 128


class AdmissionError(RuntimeError):
    """Stable safe-to-display admission failure code."""

    def __init__(self, code: str, *, retryable: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class IssueSource(_StrictModel):
    kind: Literal["issue"]
    registration_id: int = Field(gt=0, le=9_223_372_036_854_775_807)
    issue_number: int = Field(gt=0, le=2_147_483_647)
    request_id: UUID
    task: str = Field(min_length=1, max_length=_MAX_TASK_BYTES, repr=False)
    credential_id: UUID | None = None


class FreeTextSource(_StrictModel):
    kind: Literal["free_text"]
    registration_id: int = Field(gt=0, le=9_223_372_036_854_775_807)
    request_id: UUID
    task: str = Field(min_length=1, max_length=_MAX_TASK_BYTES, repr=False)
    credential_id: UUID | None = None


AdmissionSource = IssueSource | FreeTextSource


@dataclass(frozen=True, slots=True)
class _ContinuationSource:
    registration_id: int
    issue_number: int
    task: str = field(repr=False)
    credential_id: UUID | None
    continuation_id: UUID
    repository_id: int
    comment_id: int
    comment_created_at: datetime
    submission_body: str = field(repr=False)
    submission_sha256: bytes


_Source = AdmissionSource | _ContinuationSource
_CONTINUATION_ROUTE = "/api/v1/runs/continuations"


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    run_id: UUID
    status: str
    created_at: datetime
    deadline_at: datetime
    profile_id: str
    profile_version: str
    provider: str
    model_id: str
    source_sha: str
    credential_id: UUID | None = None
    issue_id: int | None = None
    issue_sha: str | None = None
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class _RegistrationSnapshot:
    registration_id: int
    repository_id: int
    github_user_id: int
    defaults_version: int
    profile_id: str
    profile_version: str
    provider: str | None
    credential_id: UUID | None
    eligibility_status: str
    checked_sha: str | None
    eligibility_receipt: dict[str, Any] | None


class AdmissionService:
    """Verify current GitHub access, then commit all admission state atomically."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        facts_provider: Any,
        credentials: CredentialService,
        *,
        enabled: bool = False,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._facts_provider = facts_provider
        self._credentials = credentials
        self._enabled = enabled

    async def admit(self, source: AdmissionSource, idempotency_key: str) -> AdmissionResult:
        if self._enabled is not True:
            raise AdmissionError("public_admission_disabled")
        if not isinstance(source, (IssueSource, FreeTextSource)):
            raise AdmissionError("admission_source_invalid")
        key_hash = _key_hash(idempotency_key)
        try:
            task_bytes = source.task.encode("utf-8")
        except UnicodeEncodeError:
            raise AdmissionError("admission_task_invalid") from None
        if len(task_bytes) > _MAX_TASK_BYTES or not source.task.strip():
            raise AdmissionError("admission_task_invalid")
        try:
            canonicalize_initial_context({"task": source.task})
        except AgentContextError:
            raise AdmissionError("admission_task_invalid") from None
        request_digest = _request_digest(source)
        content_digest = _content_digest(source)

        early = await asyncio.to_thread(
            self._early_replay, source, key_hash, request_digest, _ROUTE
        )
        if early is not None:
            return early
        snapshot = await asyncio.to_thread(self._registration, source.registration_id)
        facts, issue_snapshot = await self._current_source(
            snapshot, source.issue_number if isinstance(source, IssueSource) else None
        )
        receipt = _validate_receipt(snapshot, facts)
        try:
            return await asyncio.to_thread(
                self._commit,
                source=source,
                idempotency_hash=key_hash,
                request_digest=request_digest,
                content_digest=content_digest,
                route=_ROUTE,
                snapshot=snapshot,
                facts=facts,
                receipt=receipt,
                issue_snapshot=issue_snapshot,
                continuation=None,
            )
        except AdmissionError:
            raise
        except SQLAlchemyError as error:
            raise _database_error(error) from None

    async def admit_continuation(
        self,
        continuation_id: UUID,
        *,
        task: str,
        idempotency_key: str,
        credential_id: UUID | None = None,
    ) -> AdmissionResult:
        """Turn a captured, verified comment into one run in a single transaction."""
        if self._enabled is not True:
            raise AdmissionError("public_admission_disabled")
        if not isinstance(continuation_id, UUID):
            raise AdmissionError("setup_continuation_invalid")
        if not isinstance(task, str) or not task.strip():
            raise AdmissionError("admission_task_invalid")
        try:
            task_bytes = task.encode("utf-8")
        except UnicodeEncodeError:
            raise AdmissionError("admission_task_invalid") from None
        if len(task_bytes) > _MAX_TASK_BYTES:
            raise AdmissionError("admission_task_invalid")
        try:
            canonicalize_initial_context({"task": task})
        except AgentContextError:
            raise AdmissionError("admission_task_invalid") from None
        key_hash = _key_hash(idempotency_key)
        route = _CONTINUATION_ROUTE
        source_key = _ContinuationSource(
            registration_id=1,
            issue_number=1,
            task=task,
            credential_id=credential_id,
            continuation_id=continuation_id,
            repository_id=1,
            comment_id=1,
            comment_created_at=datetime(1970, 1, 1, tzinfo=UTC),
            submission_body="",
            submission_sha256=b"",
        )
        request_digest = _continuation_request_digest(continuation_id, task, credential_id)
        early = await asyncio.to_thread(
            self._early_replay, source_key, key_hash, request_digest, route
        )
        if early is not None:
            return early
        source, snapshot = await asyncio.to_thread(
            self._load_continuation, continuation_id, task, credential_id
        )
        facts, issue_snapshot = await self._current_source(snapshot, source.issue_number)
        receipt = _validate_receipt(snapshot, facts)
        try:
            return await asyncio.to_thread(
                self._commit,
                source=source,
                idempotency_hash=key_hash,
                request_digest=request_digest,
                content_digest=_continuation_content_digest(task, credential_id),
                route=route,
                snapshot=snapshot,
                facts=facts,
                receipt=receipt,
                issue_snapshot=issue_snapshot,
                continuation={
                    "continuation_id": source.continuation_id,
                    "repository_id": source.repository_id,
                    "comment_id": source.comment_id,
                    "issue_number": source.issue_number,
                    "comment_created_at": source.comment_created_at,
                    "submission_body": source.submission_body,
                    "submission_sha256": source.submission_sha256,
                },
            )
        except AdmissionError:
            raise
        except SQLAlchemyError as error:
            raise _database_error(error) from None

    def _load_continuation(
        self, continuation_id: UUID, task: str, credential_id: UUID | None
    ) -> tuple[_ContinuationSource, _RegistrationSnapshot]:
        owner = self._principal.account_id
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                row = (
                    connection.execute(
                        text(
                            "SELECT c.continuation_id, c.github_user_id, c.github_repository_id, "
                            "c.github_comment_id, c.github_issue_number, c.comment_created_at, "
                            "c.submission_body, c.submission_sha256, c.expires_at, c.claimed_at, "
                            "u.github_user_id AS owner_github_user_id, u.status AS account_status, "
                            "u.created_at AS account_created_at, r.registration_id, "
                            "r.created_at AS registration_created_at, "
                            "r.defaults_version, r.profile_id, r.profile_version, "
                            "r.provider, r.credential_id, "
                            "r.eligibility_status, r.checked_sha, r.eligibility_receipt, "
                            "clock_timestamp() AS now "
                            "FROM runtime_private.setup_continuations AS c "
                            "JOIN runtime_private.users AS u ON u.user_id=c.owner_id "
                            "JOIN runtime_private.repository_registrations AS r "
                            "ON r.owner_id=c.owner_id AND "
                            "r.github_repository_id=c.github_repository_id "
                            "WHERE c.owner_id=:owner AND c.continuation_id=:id"
                        ),
                        {"owner": owner, "id": continuation_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise AdmissionError("setup_continuation_not_found")
                _validate_continuation_row(row)
                snapshot = _snapshot({**dict(row), "github_user_id": row["owner_github_user_id"]})
                return (
                    _ContinuationSource(
                        registration_id=snapshot.registration_id,
                        issue_number=row["github_issue_number"],
                        task=task,
                        credential_id=credential_id,
                        continuation_id=continuation_id,
                        repository_id=row["github_repository_id"],
                        comment_id=row["github_comment_id"],
                        comment_created_at=row["comment_created_at"],
                        submission_body=row["submission_body"],
                        submission_sha256=bytes(row["submission_sha256"]),
                    ),
                    snapshot,
                )
        except AdmissionError:
            raise
        except SQLAlchemyError:
            raise AdmissionError("admission_storage_unavailable", retryable=True) from None

    def _lock_continuation(self, connection: Connection, expected: Mapping[str, Any]) -> None:
        row = (
            connection.execute(
                text(
                    "SELECT c.continuation_id, c.github_user_id, c.github_repository_id, "
                    "c.github_comment_id, c.github_issue_number, c.comment_created_at, "
                    "c.submission_body, c.submission_sha256, c.expires_at, c.claimed_at, "
                    "u.github_user_id AS owner_github_user_id, u.status AS account_status, "
                    "u.created_at AS account_created_at, r.created_at AS registration_created_at, "
                    "clock_timestamp() AS now "
                    "FROM runtime_private.setup_continuations AS c "
                    "JOIN runtime_private.users AS u ON u.user_id=c.owner_id "
                    "JOIN runtime_private.repository_registrations AS r "
                    "ON r.owner_id=c.owner_id AND r.github_repository_id=c.github_repository_id "
                    "WHERE c.owner_id=:owner AND c.continuation_id=:id FOR UPDATE OF c"
                ),
                {"owner": self._principal.account_id, "id": expected["continuation_id"]},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise AdmissionError("setup_continuation_not_found")
        _validate_continuation_row(row)
        if (
            row["github_repository_id"] != expected["repository_id"]
            or row["github_comment_id"] != expected["comment_id"]
            or row["github_issue_number"] != expected["issue_number"]
            or row["comment_created_at"] != expected["comment_created_at"]
            or row["submission_body"] != expected["submission_body"]
            or bytes(row["submission_sha256"]) != expected["submission_sha256"]
        ):
            raise AdmissionError("setup_continuation_identity_conflict")

    def _claim_continuation(self, connection: Connection, continuation_id: UUID) -> None:
        claimed = connection.execute(
            text(
                "UPDATE runtime_private.setup_continuations SET claimed_at=clock_timestamp() "
                "WHERE owner_id=:owner AND continuation_id=:id AND claimed_at IS NULL "
                "AND expires_at>clock_timestamp() RETURNING continuation_id"
            ),
            {"owner": self._principal.account_id, "id": continuation_id},
        ).scalar_one_or_none()
        if claimed is None:
            raise AdmissionError("setup_continuation_already_claimed")

    def _registration(self, registration_id: int) -> _RegistrationSnapshot:
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                row = (
                    connection.execute(
                        text(
                            "SELECT u.github_user_id, u.status AS account_status, "
                            "r.registration_id, r.github_repository_id, r.defaults_version, "
                            "r.profile_id, r.profile_version, r.provider, r.credential_id, "
                            "r.eligibility_status, r.checked_sha, r.eligibility_receipt "
                            "FROM runtime_private.repository_registrations AS r "
                            "JOIN runtime_private.users AS u ON u.user_id=r.owner_id "
                            "WHERE r.owner_id=:owner AND r.registration_id=:registration"
                        ),
                        {"owner": self._principal.account_id, "registration": registration_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise AdmissionError("repository_not_found")
                if row["account_status"] != "active":
                    raise AdmissionError("account_unavailable")
                return _snapshot(row)
        except AdmissionError:
            raise
        except SQLAlchemyError:
            raise AdmissionError("admission_storage_unavailable", retryable=True) from None

    def _early_replay(
        self, source: _Source, key_hash: bytes, request_digest: bytes, route: str
    ) -> AdmissionResult | None:
        """Recover cached/durable submission identity before a new GitHub check."""
        owner = self._principal.account_id
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
                    {"identity": f"continuum:admission:{owner}:{route}:{key_hash.hex()}"},
                )
                self._sweep_expired_idempotency(connection)
                cached = self._idempotency_result(connection, key_hash, request_digest, route)
                if cached is not None:
                    return cached
                kind, _, request_id = _origin(source)
                if isinstance(source, _ContinuationSource):
                    identity_sql = "submission.continuation_id=:identity"
                    identity_value: UUID | None = source.continuation_id
                else:
                    identity_sql = "submission.request_id=:identity"
                    identity_value = request_id
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
                    {"identity": f"continuum:submission:{owner}:{kind}:{identity_value}"},
                )
                row = (
                    connection.execute(
                        text(
                            "SELECT run.run_id, run.status, run.created_at, run.deadline_at, "
                            "pins.profile_id, pins.profile_version, pins.provider, pins.model_id, "
                            "pins.source_sha, "
                            "(SELECT binding.credential_id FROM "
                            "runtime_private.run_credentials AS binding "
                            "WHERE binding.owner_id=run.owner_id AND binding.run_id=run.run_id) "
                            "AS credential_id, "
                            "pins.issue_snapshot, "
                            "submission.input_digest FROM "
                            "runtime_private.admission_submissions AS submission "
                            "JOIN runtime_private.runs AS run USING(owner_id, run_id) "
                            "JOIN runtime_private.run_admission_pins AS pins "
                            "USING(owner_id, run_id) "
                            "WHERE submission.owner_id=:owner AND submission.origin_kind=:kind AND "
                            + identity_sql
                        ),
                        {"owner": owner, "kind": kind, "identity": identity_value},
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    return None
                if bytes(row["input_digest"]) != request_digest:
                    raise AdmissionError("submission_request_id_conflict")
                result = _admission_result(row, replayed=True)
                self._store_idempotency(connection, key_hash, request_digest, result.run_id, route)
                return result
        except AdmissionError:
            raise
        except SQLAlchemyError as error:
            raise _database_error(error) from None

    async def _current_facts(self, snapshot: _RegistrationSnapshot) -> RepositoryPublicationFacts:
        try:
            facts = await self._facts_provider.current(
                snapshot.repository_id, snapshot.github_user_id
            )
        except Exception as error:
            code = getattr(error, "code", "github_authorization_unavailable")
            raise AdmissionError(_safe_github_code(code), retryable=True) from None
        _validate_fresh_facts(facts, snapshot)
        assert isinstance(facts, RepositoryPublicationFacts)
        return facts

    async def _current_source(
        self, snapshot: _RegistrationSnapshot, issue_number: int | None
    ) -> tuple[RepositoryPublicationFacts, dict[str, Any] | None]:
        if issue_number is None:
            return await self._current_facts(snapshot), None
        current_issue = getattr(self._facts_provider, "current_issue", None)
        if not callable(current_issue):
            raise AdmissionError("github_issue_verification_unavailable")
        try:
            facts, issue = await current_issue(
                snapshot.repository_id, snapshot.github_user_id, issue_number
            )
        except Exception as error:
            code = getattr(error, "code", "github_authorization_unavailable")
            raise AdmissionError(_safe_github_code(code), retryable=True) from None
        _validate_fresh_facts(facts, snapshot)
        assert isinstance(facts, RepositoryPublicationFacts)
        if (
            not isinstance(issue, GitHubIssueSnapshot)
            or issue.repository_id != snapshot.repository_id
            or issue.github_user_id != snapshot.github_user_id
            or issue.issue_number != issue_number
            or type(issue.issue_id) is not int
            or issue.issue_id <= 0
            or issue.state not in {"open", "closed"}
            or not isinstance(issue.sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", issue.sha256) is None
            or not isinstance(issue.title, str)
            or (issue.body is not None and not isinstance(issue.body, str))
        ):
            raise AdmissionError("github_authorization_invalid")
        return facts, {
            "id": issue.issue_id,
            "number": issue.issue_number,
            "title": issue.title,
            "body": issue.body,
            "state": issue.state,
            "updated_at": issue.updated_at,
            "sha256": issue.sha256,
        }

    def _commit(
        self,
        *,
        source: _Source,
        idempotency_hash: bytes,
        request_digest: bytes,
        content_digest: bytes,
        route: str,
        snapshot: _RegistrationSnapshot,
        facts: RepositoryPublicationFacts,
        receipt: EligibilityReceipt,
        issue_snapshot: dict[str, Any] | None,
        continuation: Mapping[str, Any] | None,
    ) -> AdmissionResult:
        owner_id = self._principal.account_id
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                self._credentials.lock_owner_in_transaction(connection, active=True)
                connection.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
                    {
                        "identity": (
                            f"continuum:admission:{owner_id}:{route}:{idempotency_hash.hex()}"
                        )
                    },
                )
                account = (
                    connection.execute(
                        text(
                            "SELECT github_user_id, status FROM runtime_private.users "
                            "WHERE user_id=:owner"
                        ),
                        {"owner": owner_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if account is None or account["status"] != "active":
                    raise AdmissionError("account_unavailable")
                if account["github_user_id"] != snapshot.github_user_id:
                    raise AdmissionError("github_identity_changed")
                if continuation is not None:
                    self._lock_continuation(connection, continuation)
                credential_locks = {
                    credential
                    for credential in (snapshot.credential_id, source.credential_id)
                    if credential is not None
                }
                for locked_credential_id in sorted(credential_locks, key=lambda item: item.int):
                    connection.execute(
                        text(
                            "SELECT credential_id FROM runtime_private.provider_credentials "
                            "WHERE owner_id=:owner AND credential_id=:credential FOR UPDATE"
                        ),
                        {"owner": owner_id, "credential": locked_credential_id},
                    ).one_or_none()
                registration = (
                    connection.execute(
                        text(
                            "SELECT registration_id, github_repository_id, defaults_version, "
                            "profile_id, profile_version, provider, credential_id, "
                            "eligibility_status, checked_sha, eligibility_receipt "
                            "FROM runtime_private.repository_registrations "
                            "WHERE owner_id=:owner AND registration_id=:registration FOR UPDATE"
                        ),
                        {"owner": owner_id, "registration": snapshot.registration_id},
                    )
                    .mappings()
                    .one_or_none()
                )
                if registration is None:
                    raise AdmissionError("repository_not_found")
                current = _snapshot(
                    {**dict(registration), "github_user_id": account["github_user_id"]}
                )
                if current != snapshot:
                    raise AdmissionError("repository_defaults_changed", retryable=True)
                receipt = _validate_receipt(current, facts)
                prior = self._idempotency_result(
                    connection, idempotency_hash, request_digest, route
                )
                if prior is not None:
                    return prior

                existing = self._existing_submission(
                    connection, source, request_digest, current.repository_id
                )
                if existing is not None:
                    result = _admission_result(existing, replayed=True)
                    if continuation is not None:
                        self._claim_continuation(connection, continuation["continuation_id"])
                    self._store_idempotency(
                        connection, idempotency_hash, request_digest, result.run_id, route
                    )
                    return result

                if current.provider not in {"openai", "anthropic"}:
                    raise AdmissionError("repository_provider_default_required")
                provider = CredentialProvider(current.provider)
                requested_credential = source.credential_id
                credential_id = requested_credential or current.credential_id
                credential_ready = self._credential_ready(
                    connection, credential_id, provider, explicit=requested_credential is not None
                )
                model_id = (
                    _OPENAI_MODEL if provider is CredentialProvider.OPENAI else _ANTHROPIC_MODEL
                )
                status = "queued" if credential_ready else "waiting_credentials"
                run_id = uuid4()
                run = (
                    connection.execute(
                        text(
                            "WITH admission_time AS (SELECT clock_timestamp() AS created_at) "
                            "INSERT INTO runtime_private.runs "
                            "(run_id, owner_id, github_repository_id, issue_number, status, "
                            "created_at, deadline_at, model_id, model_task_context, "
                            "model_tool_schema_sha256) "
                            "SELECT :run, :owner, :repo, :issue, :status, "
                            "admission_time.created_at, "
                            "admission_time.created_at+interval '7 days', NULL, NULL, NULL "
                            "FROM admission_time "
                            "RETURNING run_id, status, created_at, deadline_at"
                        ),
                        {
                            "run": run_id,
                            "owner": owner_id,
                            "repo": current.repository_id,
                            "issue": None
                            if isinstance(source, FreeTextSource)
                            else source.issue_number,
                            "status": status,
                        },
                    )
                    .mappings()
                    .one()
                )
                repository = RuntimeRepository(connection, self._principal)
                step_id = repository.create_step(run_id, "admission.source", "source_preparation")
                repository.create_job(
                    run_id, step_id, replay_policy=ReplayPolicy.ATTENTION_ON_UNKNOWN
                )
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.run_admission_pins "
                        "(owner_id, run_id, registration_id, defaults_version, profile_id, "
                        "profile_version, base_branch, source_sha, source_tree_sha, "
                        "source_receipt, issue_snapshot, provider, model_id, task_context, "
                        "submission_digest) VALUES "
                        "(:owner, :run, :registration, :defaults, :profile, :profile_version, "
                        ":branch, :sha, :tree, CAST(:receipt AS jsonb), "
                        "CAST(:issue_snapshot AS jsonb), "
                        ":provider, :model, CAST(:task_context AS jsonb), :digest)"
                    ),
                    {
                        "owner": owner_id,
                        "run": run_id,
                        "registration": current.registration_id,
                        "defaults": current.defaults_version,
                        "profile": current.profile_id,
                        "profile_version": current.profile_version,
                        "branch": facts.default_branch,
                        "sha": receipt.commit_sha,
                        "tree": receipt.tree_sha,
                        "receipt": json.dumps(receipt.to_cache_payload(), separators=(",", ":")),
                        "issue_snapshot": (
                            None
                            if issue_snapshot is None
                            else json.dumps(
                                issue_snapshot, ensure_ascii=False, separators=(",", ":")
                            )
                        ),
                        "provider": provider.value,
                        "model": model_id,
                        "task_context": json.dumps(
                            {"task": source.task},
                            ensure_ascii=False,
                            allow_nan=False,
                            separators=(",", ":"),
                        ),
                        "digest": content_digest,
                    },
                )
                origin_kind, origin_key, request_id = _origin(source)
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.admission_submissions "
                        "(submission_id, owner_id, run_id, origin_kind, origin_key, repository_id, "
                        "issue_number, request_id, continuation_id, input_digest) VALUES "
                        "(:id, :owner, :run, :kind, :key, :repo, :issue, :request_id, "
                        ":continuation_id, :digest)"
                    ),
                    {
                        "id": uuid4(),
                        "owner": owner_id,
                        "run": run_id,
                        "kind": origin_kind,
                        "key": origin_key,
                        "repo": current.repository_id,
                        "issue": None
                        if isinstance(source, FreeTextSource)
                        else source.issue_number,
                        "request_id": request_id,
                        "continuation_id": (
                            source.continuation_id
                            if isinstance(source, _ContinuationSource)
                            else None
                        ),
                        "digest": request_digest,
                    },
                )
                repository.append_event(
                    run_id,
                    1,
                    "run.admitted",
                    EventActor.ACCOUNT,
                    actor_id=str(owner_id),
                    payload={
                        "status": status,
                        "source_kind": origin_kind,
                        "profile_id": current.profile_id,
                        "profile_version": current.profile_version,
                        "provider": provider.value,
                    },
                )
                if credential_ready:
                    assert credential_id is not None
                    self._credentials.bind_run_in_transaction(
                        connection, run_id, provider, credential_id
                    )
                else:
                    repository.append_event(
                        run_id,
                        2,
                        "run.waiting_credentials",
                        EventActor.SYSTEM,
                        payload={"provider": provider.value},
                    )
                if continuation is not None:
                    self._claim_continuation(connection, continuation["continuation_id"])
                result = AdmissionResult(
                    run_id=run["run_id"],
                    status=run["status"],
                    created_at=run["created_at"],
                    deadline_at=run["deadline_at"],
                    profile_id=current.profile_id,
                    profile_version=current.profile_version,
                    provider=provider.value,
                    model_id=model_id,
                    source_sha=receipt.commit_sha,
                    credential_id=credential_id if credential_ready else None,
                    issue_id=None if issue_snapshot is None else issue_snapshot["id"],
                    issue_sha=None if issue_snapshot is None else issue_snapshot["sha256"],
                )
                self._store_idempotency(connection, idempotency_hash, request_digest, run_id, route)
                return result
        except AdmissionError:
            raise
        except SQLAlchemyError as error:
            raise _database_error(error) from None

    def _idempotency_result(
        self, connection: Connection, key_hash: bytes, input_digest: bytes, route: str
    ) -> AdmissionResult | None:
        owner = self._principal.account_id
        row = (
            connection.execute(
                text(
                    "SELECT input_digest, run_id, expires_at "
                    "FROM runtime_private.admission_idempotency "
                    "WHERE owner_id=:owner AND route=:route AND key_hash=:key"
                ),
                {"owner": owner, "route": route, "key": key_hash},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        expired: bool = connection.execute(
            text("SELECT :expiry <= clock_timestamp()"), {"expiry": row["expires_at"]}
        ).scalar_one()
        if expired:
            connection.execute(
                text(
                    "DELETE FROM runtime_private.admission_idempotency "
                    "WHERE owner_id=:owner AND route=:route AND key_hash=:key"
                ),
                {"owner": owner, "route": route, "key": key_hash},
            )
            return None
        if bytes(row["input_digest"]) != input_digest:
            raise AdmissionError("idempotency_key_conflict")
        run = (
            connection.execute(
                text(
                    "SELECT run.run_id, run.status, run.created_at, run.deadline_at, "
                    "pins.profile_id, pins.profile_version, pins.provider, pins.model_id, "
                    "pins.source_sha, "
                    "(SELECT binding.credential_id FROM runtime_private.run_credentials AS binding "
                    "WHERE binding.owner_id=run.owner_id AND binding.run_id=run.run_id) "
                    "AS credential_id, "
                    "pins.issue_snapshot "
                    "FROM runtime_private.runs AS run "
                    "JOIN runtime_private.run_admission_pins AS pins "
                    "USING(owner_id, run_id) WHERE run.owner_id=:owner AND run.run_id=:run"
                ),
                {"owner": owner, "run": row["run_id"]},
            )
            .mappings()
            .one_or_none()
        )
        if run is None:
            raise AdmissionError("idempotency_result_expired")
        return _admission_result(run, replayed=True)

    def _sweep_expired_idempotency(self, connection: Connection, *, limit: int = 100) -> int:
        result = connection.execute(
            text(
                "WITH expired AS (SELECT ctid FROM runtime_private.admission_idempotency "
                "WHERE owner_id=:owner AND expires_at<=clock_timestamp() "
                "ORDER BY expires_at, route, key_hash LIMIT :limit) "
                "DELETE FROM runtime_private.admission_idempotency AS cache "
                "USING expired WHERE cache.ctid=expired.ctid"
            ),
            {"owner": self._principal.account_id, "limit": limit},
        )
        return result.rowcount

    def _store_idempotency(
        self,
        connection: Connection,
        key_hash: bytes,
        input_digest: bytes,
        run_id: UUID,
        route: str,
    ) -> None:
        connection.execute(
            text(
                "WITH now AS (SELECT clock_timestamp() AS value) "
                "INSERT INTO runtime_private.admission_idempotency "
                "(owner_id, route, key_hash, input_digest, run_id, created_at, expires_at) "
                "SELECT :owner, :route, :key, :digest, :run, value, value+interval '24 hours' "
                "FROM now"
            ),
            {
                "owner": self._principal.account_id,
                "route": route,
                "key": key_hash,
                "digest": input_digest,
                "run": run_id,
            },
        )

    def _existing_submission(
        self,
        connection: Connection,
        source: _Source,
        request_digest: bytes,
        repository_id: int,
    ) -> Mapping[Any, Any] | None:
        owner = self._principal.account_id
        origin_kind, _, request_id = _origin(source)
        if isinstance(source, _ContinuationSource):
            identity_sql = "submission.continuation_id=:identity"
            identity_value: UUID | None = source.continuation_id
        else:
            identity_sql = "submission.request_id=:identity"
            identity_value = request_id
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
            {"identity": f"continuum:submission:{owner}:{origin_kind}:{identity_value}"},
        )
        row = (
            connection.execute(
                text(
                    "SELECT run.run_id, run.status, run.created_at, run.deadline_at, "
                    "pins.profile_id, pins.profile_version, pins.provider, pins.model_id, "
                    "pins.source_sha, "
                    "(SELECT binding.credential_id FROM runtime_private.run_credentials AS binding "
                    "WHERE binding.owner_id=run.owner_id AND binding.run_id=run.run_id) "
                    "AS credential_id, "
                    "pins.issue_snapshot, "
                    "submission.input_digest FROM "
                    "runtime_private.admission_submissions AS submission "
                    "JOIN runtime_private.runs AS run USING(owner_id, run_id) "
                    "JOIN runtime_private.run_admission_pins AS pins USING(owner_id, run_id) "
                    "WHERE submission.owner_id=:owner AND submission.origin_kind=:kind "
                    "AND " + identity_sql
                ),
                {"owner": owner, "kind": origin_kind, "identity": identity_value},
            )
            .mappings()
            .one_or_none()
        )
        if row is not None:
            if bytes(row["input_digest"]) != request_digest:
                raise AdmissionError("submission_request_id_conflict")
            return row
        if isinstance(source, FreeTextSource):
            return None
        row = (
            connection.execute(
                text(
                    "SELECT run.run_id, run.status, run.created_at, run.deadline_at, "
                    "pins.profile_id, pins.profile_version, pins.provider, pins.model_id, "
                    "pins.source_sha, "
                    "(SELECT binding.credential_id FROM runtime_private.run_credentials AS binding "
                    "WHERE binding.owner_id=run.owner_id AND binding.run_id=run.run_id) "
                    "AS credential_id, "
                    "pins.issue_snapshot, "
                    "pins.submission_digest AS input_digest FROM runtime_private.runs AS run "
                    "JOIN runtime_private.run_admission_pins AS pins USING(owner_id, run_id) "
                    "WHERE run.owner_id=:owner AND run.github_repository_id=:repo "
                    "AND run.issue_number=:issue AND run.status IN "
                    "('queued','running','waiting_approval','waiting_credentials','waiting_capacity',"
                    "'needs_attention') FOR UPDATE OF run"
                ),
                {"owner": owner, "repo": repository_id, "issue": source.issue_number},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        if isinstance(source, _ContinuationSource):
            connection.execute(
                text(
                    "INSERT INTO runtime_private.admission_submissions "
                    "(submission_id, owner_id, run_id, origin_kind, origin_key, repository_id, "
                    "issue_number, continuation_id, input_digest) VALUES "
                    "(:id, :owner, :run, 'github_comment', :key, :repo, :issue, "
                    ":continuation, :digest)"
                ),
                {
                    "id": uuid4(),
                    "owner": owner,
                    "run": row["run_id"],
                    "key": f"{repository_id}:{source.comment_id}",
                    "repo": repository_id,
                    "issue": source.issue_number,
                    "continuation": source.continuation_id,
                    "digest": request_digest,
                },
            )
            return row
        connection.execute(
            text(
                "INSERT INTO runtime_private.admission_submissions "
                "(submission_id, owner_id, run_id, origin_kind, origin_key, repository_id, "
                "issue_number, request_id, input_digest) VALUES "
                "(:id, :owner, :run, 'web_issue', :key, :repo, :issue, :request_id, :digest)"
            ),
            {
                "id": uuid4(),
                "owner": owner,
                "run": row["run_id"],
                "key": str(source.request_id),
                "repo": repository_id,
                "issue": source.issue_number,
                "request_id": source.request_id,
                "digest": request_digest,
            },
        )
        return row

    def _credential_ready(
        self,
        connection: Connection,
        credential_id: UUID | None,
        provider: CredentialProvider,
        *,
        explicit: bool,
    ) -> bool:
        if credential_id is None:
            return False
        row = (
            connection.execute(
                text(
                    "SELECT scope, status, expires_at FROM runtime_private.provider_credentials "
                    "WHERE owner_id=:owner AND credential_id=:credential AND provider=:provider "
                    "FOR UPDATE"
                ),
                {
                    "owner": self._principal.account_id,
                    "credential": credential_id,
                    "provider": provider.value,
                },
            )
            .mappings()
            .one_or_none()
        )
        now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
        valid = (
            row is not None
            and row["scope"] == "saved"
            and row["status"] == "active"
            and (row["expires_at"] is None or row["expires_at"] > now)
        )
        if not valid and explicit:
            raise AdmissionError("credential_unavailable")
        return bool(valid)


def _snapshot(row: Mapping[Any, Any]) -> _RegistrationSnapshot:
    receipt = row["eligibility_receipt"]
    if receipt is not None and not isinstance(receipt, dict):
        raise AdmissionError("repository_eligibility_corrupt")
    return _RegistrationSnapshot(
        registration_id=row["registration_id"],
        repository_id=row["github_repository_id"],
        github_user_id=row["github_user_id"],
        defaults_version=row["defaults_version"],
        profile_id=row["profile_id"],
        profile_version=row["profile_version"],
        provider=row["provider"],
        credential_id=row["credential_id"],
        eligibility_status=row["eligibility_status"],
        checked_sha=row["checked_sha"],
        eligibility_receipt=receipt,
    )


def _validate_receipt(
    registration: _RegistrationSnapshot, facts: RepositoryPublicationFacts
) -> EligibilityReceipt:
    if registration.provider not in {"openai", "anthropic"}:
        raise AdmissionError("repository_provider_default_required")
    if (
        registration.profile_id not in {"python-pytest", "node-npm"}
        or registration.profile_version != "1"
    ):
        raise AdmissionError("repository_profile_default_required")
    if registration.eligibility_status != "eligible" or registration.eligibility_receipt is None:
        raise AdmissionError("repository_not_eligible")
    try:
        receipt = EligibilityReceipt.from_cache_payload(registration.eligibility_receipt)
    except RepositoryPreflightError:
        raise AdmissionError("repository_eligibility_corrupt") from None
    if (
        receipt.repository_id != registration.repository_id
        or receipt.profile_id != registration.profile_id
        or receipt.profile_version != registration.profile_version
        or registration.checked_sha != receipt.commit_sha
        or receipt.commit_sha != facts.base_commit_sha
    ):
        raise AdmissionError("repository_eligibility_stale", retryable=True)
    branch = facts.default_branch
    if (
        not isinstance(branch, str)
        or not 1 <= len(branch) <= 255
        or branch.startswith("/")
        or branch.endswith("/")
        or ".." in branch
        or "//" in branch
        or any(ord(char) < 32 or char == " " for char in branch)
    ):
        raise AdmissionError("repository_default_branch_invalid")
    return receipt


def _validate_fresh_facts(facts: object, registration: _RegistrationSnapshot) -> None:
    if (
        not isinstance(facts, RepositoryPublicationFacts)
        or not isinstance(facts.access_proof, RepositoryAccessProof)
        or facts.repository_id != registration.repository_id
        or facts.github_user_id != registration.github_user_id
        or facts.access_proof.actor_can_write is not True
        or facts.access_proof.app_can_access is not True
        or facts.access_proof.repository_id != registration.repository_id
        or facts.access_proof.github_user_id != registration.github_user_id
        or not isinstance(facts.base_commit_sha, str)
        or _SHA.fullmatch(facts.base_commit_sha) is None
    ):
        raise AdmissionError("github_authorization_invalid")


def _key_hash(value: str) -> bytes:
    if not isinstance(value, str) or not 16 <= len(value) <= _MAX_IDEMPOTENCY_KEY:
        raise AdmissionError("idempotency_key_invalid")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError:
        raise AdmissionError("idempotency_key_invalid") from None
    if any(byte < 0x21 or byte > 0x7E for byte in encoded):
        raise AdmissionError("idempotency_key_invalid")
    return hashlib.sha256(encoded).digest()


def _request_digest(source: AdmissionSource) -> bytes:
    raw = json.dumps(
        source.model_dump(mode="json"),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(raw.encode("utf-8")).digest()


def _content_digest(source: AdmissionSource) -> bytes:
    values = source.model_dump(mode="json")
    values.pop("request_id", None)
    raw = json.dumps(
        values, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(raw.encode("utf-8")).digest()


def _continuation_request_digest(
    continuation_id: UUID, task: str, credential_id: UUID | None
) -> bytes:
    payload = {
        "continuation_id": str(continuation_id),
        "task": task,
        "credential_id": None if credential_id is None else str(credential_id),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    ).digest()


def _continuation_content_digest(task: str, credential_id: UUID | None) -> bytes:
    payload = {"task": task, "credential_id": None if credential_id is None else str(credential_id)}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    ).digest()


def _origin(source: _Source) -> tuple[str, str, UUID | None]:
    if isinstance(source, _ContinuationSource):
        return "github_comment", f"{source.repository_id}:{source.comment_id}", None
    kind = "web_issue" if isinstance(source, IssueSource) else "free_text"
    return kind, str(source.request_id), source.request_id


def _validate_continuation_row(row: Mapping[Any, Any]) -> None:
    now: datetime | None = row.get("now")
    if now is None:
        raise AdmissionError("setup_continuation_time_unavailable", retryable=True)
    if (
        row["account_status"] != "active"
        or row["github_user_id"] != row["owner_github_user_id"]
        or row["claimed_at"] is not None
        or row["expires_at"] <= now
        or row["comment_created_at"] < row["account_created_at"]
        or row["comment_created_at"] < row["registration_created_at"]
        or row["comment_created_at"] > now + timedelta(minutes=5)
        or row["comment_created_at"] < now - timedelta(hours=72)
    ):
        raise AdmissionError("setup_continuation_stale")
    body = row["submission_body"]
    if not isinstance(body, str) or not 1 <= len(body.encode("utf-8")) <= _MAX_TASK_BYTES:
        raise AdmissionError("setup_continuation_content_expired")
    expected = hashlib.sha256(
        int(row["github_issue_number"]).to_bytes(8, "big", signed=False)
        + b"\0"
        + body.encode("utf-8")
    ).digest()
    if bytes(row["submission_sha256"]) != expected:
        raise AdmissionError("setup_continuation_content_corrupt")


def _admission_result(row: Mapping[Any, Any], *, replayed: bool = False) -> AdmissionResult:
    snapshot = row.get("issue_snapshot")
    return AdmissionResult(
        run_id=row["run_id"],
        status=row["status"],
        created_at=row["created_at"],
        deadline_at=row["deadline_at"],
        profile_id=row["profile_id"],
        profile_version=row["profile_version"],
        provider=row["provider"],
        model_id=row["model_id"],
        source_sha=row["source_sha"],
        credential_id=row["credential_id"],
        issue_id=None if snapshot is None else snapshot.get("id"),
        issue_sha=None if snapshot is None else snapshot.get("sha256"),
        replayed=replayed,
    )


def _safe_github_code(value: object) -> str:
    allowed = {
        "github_user_token_unavailable",
        "github_identity_mismatch",
        "repository_actor_write_access_required",
        "github_installation_user_access_required",
        "github_authorization_timeout",
        "github_api_unavailable",
        "github_api_request_failed",
        "github_installation_unavailable",
        "repository_must_be_public",
        "github_issue_number_invalid",
        "github_issue_identity_invalid",
        "github_issue_snapshot_invalid",
        "github_issue_is_pull_request",
        "github_issue_not_open",
    }
    return (
        value if isinstance(value, str) and value in allowed else "github_authorization_unavailable"
    )


def _database_error(error: SQLAlchemyError) -> AdmissionError:
    original = getattr(error, "orig", None)
    diagnostic = getattr(original, "diag", None)
    message = getattr(diagnostic, "message_primary", None)
    constraint = getattr(diagnostic, "constraint_name", None)
    if message == "admission_global_queue_full":
        return AdmissionError("global_queue_full", retryable=True)
    if message == "admission_principal_mismatch":
        return AdmissionError("admission_storage_principal_invalid")
    if constraint == "uq_runtime_runs_one_active_account":
        return AdmissionError("account_active_run_exists", retryable=True)
    if constraint == "uq_runtime_runs_one_active_issue":
        return AdmissionError("active_issue_exists", retryable=True)
    if constraint == "uq_admission_api_request":
        return AdmissionError("submission_request_id_conflict")
    return AdmissionError("admission_storage_unavailable", retryable=True)
