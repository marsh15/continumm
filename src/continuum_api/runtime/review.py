"""Immutable owner-scoped review snapshots and bounded rework decisions."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any, Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Engine, text

from continuum_api.approvals.manifest import digest_manifest
from continuum_api.artifacts.archives import VerifiedArchive, verify_archive
from continuum_api.runtime.agent_loop import (
    MAX_AGENT_INPUT_TOKENS_PER_RUN,
    MAX_AGENT_OUTPUT_TOKENS_PER_RUN,
    MAX_AGENT_TURNS_PER_RUN,
)
from continuum_api.runtime.approvals import (
    ApprovalError,
    ApprovalNotFound,
    ApprovalObjectStorage,
    ApprovalReceipt,
    ApprovalService,
    PendingPublicationFactsProvider,
    PublicationFactsProvider,
    _canonical_json,
    _validate_publication_facts,
)
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.runtime.transitions import TransitionService
from continuum_api.tools.source_tools import (
    MAX_TOOL_OUTPUT_BYTES,
    DiffResult,
    SourceSnapshot,
    diff_sources,
)
from continuum_api.validation.contracts import ValidationEvidence

MAX_REVIEW_BODY_BYTES = 64 * 1024


class _StrictReviewModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class ReviewDiff(_StrictReviewModel):
    text: str = Field(max_length=MAX_TOOL_OUTPUT_BYTES)
    changed_paths: tuple[str, ...] = Field(max_length=100)
    truncated: bool


class ReviewUsage(_StrictReviewModel):
    used: int = Field(ge=0)
    limit: int = Field(gt=0)


class ReviewLimits(_StrictReviewModel):
    requests: ReviewUsage
    input_tokens: ReviewUsage
    output_tokens: ReviewUsage
    sandbox_seconds: ReviewUsage


class ReviewRevisions(_StrictReviewModel):
    used: int = Field(ge=0, le=2)
    limit: Literal[2] = 2
    remaining: int = Field(ge=0, le=2)


class ReviewApproval(_StrictReviewModel):
    approval_id: UUID
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expires_at: datetime
    status: Literal["pending", "approved", "rejected", "changes", "expired"]


class ReviewResponse(_StrictReviewModel):
    run_id: UUID
    revision: int = Field(ge=0, le=2)
    status: str
    diff: ReviewDiff
    evidence: ValidationEvidence
    manifest: dict[str, Any]
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    approval: ReviewApproval
    limits: ReviewLimits
    revisions: ReviewRevisions
    decision_eligible: bool


class ReviewDecisionReceipt(_StrictReviewModel):
    approval_id: UUID
    run_id: UUID
    revision: int = Field(ge=0, le=2)
    status: Literal["approved", "rejected", "changes"]
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


CsrfGuard = Callable[[Any], bool | Awaitable[bool]]


class ReviewService:
    """Read exact review evidence and apply owner decisions through T27."""

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        storage: ApprovalObjectStorage,
        *,
        publication_facts: PublicationFactsProvider | None = None,
        transitions: TransitionService | None = None,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._storage = storage
        self._publication_facts = publication_facts or PendingPublicationFactsProvider()
        self._approvals = ApprovalService(
            engine,
            principal,
            transitions or TransitionService(engine, principal),
            storage,
            publication_facts=self._publication_facts,
        )

    async def get_review(self, run_id: UUID) -> ReviewResponse:
        """Return the current immutable candidate, typed evidence, and run limits."""
        data = self._load_review_rows(run_id)
        if data is None:
            raise ApprovalNotFound()
        manifest = data["manifest"]
        try:
            encoded, digest = digest_manifest(manifest)
            if hashlib.sha256(_canonical_json(data["evidence_payload"])).digest() != bytes(
                data["evidence_result_sha256"]
            ):
                raise ValueError("review evidence hash mismatch")
            evidence = ValidationEvidence.model_validate_json(
                json.dumps(
                    data["evidence_payload"]["approval_validation"],
                    ensure_ascii=False,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                strict=True,
            )
        except Exception:
            raise ApprovalError("review_snapshot_corrupt") from None
        if digest != bytes(data["manifest_sha256"]):
            raise ApprovalError("review_manifest_digest_mismatch")
        if (
            data["evidence_payload"].get("run_revision") != data["run_revision"]
            or data["evidence_payload"].get("candidate_artifact_id")
            != str(data["candidate_artifact_id"])
            or manifest.get("validation", {}).get("evidence_result_id")
            != str(data["evidence_result_id"])
            or manifest.get("validation", {}).get("evidence_result_sha256")
            != bytes(data["evidence_result_sha256"]).hex()
        ):
            raise ApprovalError("review_evidence_binding_mismatch")
        if len(encoded) > 65_536:
            raise ApprovalError("review_manifest_size_limit")
        now = data["database_now"]
        if (
            data["approval_status"] != "pending"
            or data["run_status"] != "waiting_approval"
            or data["approval_revision"] != data["run_revision"]
            or data["candidate_status"] != "committed"
            or data["baseline_status"] != "committed"
            or data["content_expires_at"] <= now
            or data["approval_expires_at"] <= now
            or data["run_deadline_at"] <= now
        ):
            raise ApprovalError("approval_stale")
        try:
            facts = await self._publication_facts.current(
                data["repository_id"], data["github_user_id"]
            )
        except ApprovalError:
            raise
        except Exception:
            raise ApprovalError("repository_authorization_pending") from None
        _validate_publication_facts(facts, data["repository_id"], data["github_user_id"])
        if (
            manifest.get("repository", {}).get("id") != facts.repository_id
            or manifest.get("base", {}).get("branch") != facts.default_branch
            or manifest.get("base", {}).get("commit_sha") != facts.base_commit_sha
            or evidence.candidate.repository_id != facts.repository_id
            or evidence.candidate.base_commit_sha != facts.base_commit_sha
        ):
            raise ApprovalError("approval_base_branch_moved")
        try:
            await self._storage.check_private_bucket()
            candidate_bytes, baseline_bytes = await asyncio.gather(
                self._storage.download(data["candidate_object_key"]),
                self._storage.download(data["baseline_object_key"]),
            )
            candidate = await asyncio.to_thread(
                verify_archive,
                candidate_bytes,
                expected_archive_sha256=bytes(data["candidate_archive_sha256"]).hex(),
                expected_tree_sha256=bytes(data["candidate_tree_sha256"]).hex(),
            )
            baseline = await asyncio.to_thread(
                verify_archive,
                baseline_bytes,
                expected_archive_sha256=bytes(data["baseline_archive_sha256"]).hex(),
                expected_tree_sha256=bytes(data["baseline_tree_sha256"]).hex(),
            )
            diff = _review_diff(baseline_bytes, baseline, candidate_bytes, candidate)
        except Exception as error:
            if isinstance(error, ApprovalError):
                raise
            raise ApprovalError("review_candidate_restore_failed") from None

        latest = self._load_review_rows(run_id)
        if (
            latest is None
            or latest["approval_id"] != data["approval_id"]
            or latest["run_revision"] != data["run_revision"]
            or latest["manifest_sha256"] != data["manifest_sha256"]
            or latest["approval_status"] != "pending"
            or latest["run_status"] != "waiting_approval"
        ):
            raise ApprovalError("approval_stale")
        try:
            current_facts = await self._publication_facts.current(
                data["repository_id"], data["github_user_id"]
            )
        except ApprovalError:
            raise
        except Exception:
            raise ApprovalError("repository_authorization_pending") from None
        _validate_publication_facts(current_facts, data["repository_id"], data["github_user_id"])
        if (
            current_facts.default_branch != facts.default_branch
            or current_facts.base_commit_sha != facts.base_commit_sha
        ):
            raise ApprovalError("approval_base_branch_moved")
        if (
            latest["approval_expires_at"] <= latest["database_now"]
            or latest["run_deadline_at"] <= latest["database_now"]
            or latest["content_expires_at"] <= latest["database_now"]
        ):
            raise ApprovalError("approval_expired")

        revision = int(data["run_revision"])
        requests_used = int(data["model_requests_reserved"])
        input_used = int(data["model_input_tokens_used"]) + int(data["model_input_tokens_reserved"])
        output_used = int(data["model_output_tokens_used"]) + int(
            data["model_output_tokens_reserved"]
        )
        sandbox_used = int(data["sandbox_seconds_used"] or 0)
        return ReviewResponse(
            run_id=run_id,
            revision=revision,
            status=data["run_status"],
            diff=ReviewDiff(
                text=diff.text,
                changed_paths=diff.changed_paths,
                truncated=diff.truncated,
            ),
            evidence=evidence,
            manifest=manifest,
            manifest_sha256=digest.hex(),
            approval=ReviewApproval(
                approval_id=data["approval_id"],
                manifest_sha256=digest.hex(),
                expires_at=data["approval_expires_at"],
                status=data["approval_status"],
            ),
            limits=ReviewLimits(
                requests=ReviewUsage(used=requests_used, limit=MAX_AGENT_TURNS_PER_RUN),
                input_tokens=ReviewUsage(used=input_used, limit=MAX_AGENT_INPUT_TOKENS_PER_RUN),
                output_tokens=ReviewUsage(used=output_used, limit=MAX_AGENT_OUTPUT_TOKENS_PER_RUN),
                sandbox_seconds=ReviewUsage(used=sandbox_used, limit=900),
            ),
            revisions=ReviewRevisions(used=revision, remaining=2 - revision),
            decision_eligible=True,
        )

    async def decide(
        self,
        approval_id: UUID,
        manifest_sha256: str,
        decision: Literal["approve", "reject", "changes"],
        *,
        feedback: str | None = None,
        idempotency_key: str,
    ) -> ReviewDecisionReceipt:
        """Apply one idempotent owner decision and return its durable receipt."""
        receipt: ApprovalReceipt = await self._approvals.decide(
            approval_id=approval_id,
            manifest_sha256=manifest_sha256,
            decision=decision,
            feedback=feedback,
            idempotency_key=idempotency_key,
        )
        # The receipt's revision identifies the immutable approval it decided.
        return ReviewDecisionReceipt(
            approval_id=receipt.approval_id,
            run_id=receipt.run_id,
            revision=receipt.revision + (1 if receipt.status == "changes" else 0),
            status=cast(Literal["approved", "rejected", "changes"], receipt.status),
            manifest_sha256=receipt.manifest_sha256.hex(),
        )

    def _load_review_rows(self, run_id: UUID) -> dict[str, Any] | None:
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            row = (
                connection.execute(
                    text(
                        "SELECT run.status AS run_status, run.revision AS run_revision, "
                        "run.deadline_at AS run_deadline_at, "
                        "run.github_repository_id AS repository_id, "
                        "run.model_requests_reserved, run.model_input_tokens_used, "
                        "run.model_input_tokens_reserved, run.model_output_tokens_used, "
                        "run.model_output_tokens_reserved, users.github_user_id, "
                        "approval.approval_id, approval.revision AS approval_revision, "
                        "approval.status AS approval_status, approval.manifest, "
                        "approval.manifest_sha256, approval.expires_at AS approval_expires_at, "
                        "approval.candidate_artifact_id, evidence.result_id AS evidence_result_id, "
                        "evidence.payload AS evidence_payload, "
                        "evidence.result_sha256 AS evidence_result_sha256, "
                        "candidate.object_key AS candidate_object_key, "
                        "candidate.archive_sha256 AS candidate_archive_sha256, "
                        "candidate.tree_sha256 AS candidate_tree_sha256, "
                        "candidate.content_expires_at AS candidate_expires_at, "
                        "candidate.status AS candidate_status, "
                        "baseline.object_key AS baseline_object_key, "
                        "baseline.archive_sha256 AS baseline_archive_sha256, "
                        "baseline.tree_sha256 AS baseline_tree_sha256, "
                        "baseline.content_expires_at AS baseline_expires_at, "
                        "baseline.status AS baseline_status, "
                        "LEAST(candidate.content_expires_at, "
                        "baseline.content_expires_at) AS content_expires_at, "
                        "clock_timestamp() AS database_now, "
                        "(SELECT COALESCE(sum(allocation.requested_seconds),0) "
                        "FROM runtime_private.sandbox_allocations AS allocation "
                        "WHERE allocation.owner_id=run.owner_id AND allocation.run_id=run.run_id) "
                        "AS sandbox_seconds_used "
                        "FROM runtime_private.runs AS run "
                        "JOIN runtime_private.users AS users ON users.user_id=run.owner_id "
                        "JOIN runtime_private.approvals AS approval "
                        "ON approval.owner_id=run.owner_id "
                        "AND approval.run_id=run.run_id AND approval.revision=run.revision "
                        "JOIN runtime_private.accepted_results AS evidence "
                        "ON evidence.owner_id=approval.owner_id "
                        "AND evidence.run_id=approval.run_id "
                        "AND evidence.result_id=approval.evidence_result_id "
                        "JOIN runtime_private.artifact_uploads AS candidate "
                        "ON candidate.owner_id=approval.owner_id "
                        "AND candidate.run_id=approval.run_id "
                        "AND candidate.upload_id=approval.candidate_artifact_id "
                        "JOIN runtime_private.artifact_uploads AS baseline "
                        "ON baseline.owner_id=approval.owner_id "
                        "AND baseline.run_id=approval.run_id "
                        "AND baseline.upload_id=CAST(evidence.payload->'approval_validation' "
                        "->>'baseline_artifact_id' AS uuid) "
                        "WHERE run.owner_id=:owner AND run.run_id=:run "
                        "AND (to_jsonb(run)->>'content_deleted_at') IS NULL "
                        "AND COALESCE((to_jsonb(run)->>'content_expires_at')::timestamptz, "
                        "'infinity'::timestamptz)>clock_timestamp() "
                        "AND (run.deadline_at IS NULL OR run.deadline_at>clock_timestamp()) "
                        "AND users.status='active'"
                    ),
                    {"owner": self._principal.account_id, "run": run_id},
                )
                .mappings()
                .one_or_none()
            )
        return None if row is None else dict(row)


def _review_diff(
    baseline_bytes: bytes,
    baseline: VerifiedArchive,
    candidate_bytes: bytes,
    candidate: VerifiedArchive,
) -> DiffResult:
    baseline_snapshot = SourceSnapshot(
        archive=baseline_bytes,
        archive_sha256=baseline.archive_sha256,
        tree_sha256=baseline.manifest.tree_sha256,
        manifest=baseline.manifest,
    )
    candidate_snapshot = SourceSnapshot(
        archive=candidate_bytes,
        archive_sha256=candidate.archive_sha256,
        tree_sha256=candidate.manifest.tree_sha256,
        manifest=candidate.manifest,
    )
    allowed = frozenset(file.path for file in baseline.files + candidate.files)
    return diff_sources(baseline_snapshot, candidate_snapshot, readable_paths=allowed)
