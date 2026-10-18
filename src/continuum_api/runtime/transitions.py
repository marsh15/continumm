"""Atomic, replayable runtime activity transitions."""

from __future__ import annotations

import hashlib
import json
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy import Engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

from continuum_api.runtime.artifact_persistence import ArtifactReference
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, lock_current_execution
from continuum_api.runtime.persistence import (
    EventActor,
    Principal,
    ReplayPolicy,
    RunStatus,
    RuntimeRepository,
)

MAX_RESULT_BYTES = 65_536
MAX_TRANSACTION_ATTEMPTS = 3
RETRYABLE_SQLSTATES = frozenset({"40001", "40P01"})
_WAIT_OR_TERMINAL_RUN_STATES = frozenset(
    {
        RunStatus.WAITING_APPROVAL,
        RunStatus.WAITING_CREDENTIALS,
        RunStatus.WAITING_CAPACITY,
        RunStatus.NEEDS_ATTENTION,
        RunStatus.FAILED,
    }
)


class TransitionError(Exception):
    """Base class for rejected runtime transitions."""


class RuntimeRecordNotFound(TransitionError):
    """The principal cannot access one of the transition records."""


class InvalidTransition(TransitionError):
    """The locked runtime state cannot accept the requested transition."""


class ResultIdentityConflict(TransitionError):
    """A logical step already accepted a different transition intent."""


class ArtifactTransitionStore(Protocol):
    """Attach one verified artifact inside the accepted-result transaction."""

    def attach_verified(
        self,
        connection: Connection,
        principal: Principal,
        lease: ExecutionLease,
        upload_id: UUID,
    ) -> ArtifactReference: ...


@dataclass(frozen=True, slots=True)
class NextWork:
    logical_key: str
    activity: str
    revision: int = 0
    replay_policy: ReplayPolicy = ReplayPolicy.ATTENTION_ON_UNKNOWN


@dataclass(frozen=True, slots=True)
class TransitionReceipt:
    result_id: UUID
    run_id: UUID
    step_id: UUID
    job_id: UUID
    result_sha256: bytes
    result: dict[str, Any]
    run_status: RunStatus
    event_sequence: int
    next_step_id: UUID | None
    next_job_id: UUID | None
    adopted: bool
    artifact_id: UUID | None = None


class TransitionService:
    """Commit one activity result and its state advancement as one transaction.

    The lease identity is mandatory. Exact committed receipt adoption is
    read-only and can succeed after lease expiry; every new durable write
    validates the current generation and database-time lease after row locks.
    No remote operation runs inside this database retry loop.
    """

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        *,
        artifact_persistence: ArtifactTransitionStore | None = None,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._artifact_persistence = artifact_persistence

    def complete_publication(
        self, lease: ExecutionLease, approval_id: UUID, manifest_sha256: str
    ) -> TransitionReceipt:
        """Finish only a fully evidenced publication; generic activities cannot succeed a run."""
        if lease.owner_id != self._principal.account_id:
            raise LeaseLost("runtime_lease_owner_mismatch")
        if not isinstance(approval_id, UUID) or not re.fullmatch(r"[0-9a-f]{64}", manifest_sha256):
            raise InvalidTransition("publication_binding_invalid")
        return self._commit_result(
            lease=lease,
            result={
                "approval_id": str(approval_id),
                "manifest_sha256": manifest_sha256,
                "publication": "confirmed",
            },
            target_run_status=RunStatus.SUCCEEDED,
            publication_binding=(approval_id, bytes.fromhex(manifest_sha256)),
        )

    def commit_result(
        self,
        *,
        lease: ExecutionLease,
        result: dict[str, Any],
        target_run_status: RunStatus,
        next_work: NextWork | None = None,
        artifact_id: UUID | None = None,
    ) -> TransitionReceipt:
        return self._commit_result(
            lease=lease,
            result=result,
            target_run_status=target_run_status,
            next_work=next_work,
            artifact_id=artifact_id,
        )

    def _commit_result(
        self,
        *,
        lease: ExecutionLease,
        result: dict[str, Any],
        target_run_status: RunStatus,
        next_work: NextWork | None = None,
        artifact_id: UUID | None = None,
        publication_binding: tuple[UUID, bytes] | None = None,
    ) -> TransitionReceipt:
        if publication_binding is None:
            self._validate_plan(target_run_status, next_work)
        elif (
            target_run_status is not RunStatus.SUCCEEDED
            or next_work is not None
            or artifact_id is not None
        ):
            raise InvalidTransition("publication_completion_plan_invalid")
        result_json = _canonical_json(result)
        if len(result_json.encode("utf-8")) > MAX_RESULT_BYTES:
            raise InvalidTransition("accepted_result_too_large")
        result_digest = hashlib.sha256(result_json.encode("utf-8")).digest()
        intent_json = _transition_intent_json(
            result,
            target_run_status,
            next_work,
            artifact_id=artifact_id,
        )
        intent_digest = hashlib.sha256(intent_json.encode("utf-8")).digest()

        for attempt in range(MAX_TRANSACTION_ATTEMPTS):
            try:
                return self._commit_once(
                    lease=lease,
                    result_json=result_json,
                    result_digest=result_digest,
                    intent_digest=intent_digest,
                    target_run_status=target_run_status,
                    next_work=next_work,
                    artifact_id=artifact_id,
                    publication_binding=publication_binding,
                )
            except DBAPIError as error:
                if (
                    attempt + 1 == MAX_TRANSACTION_ATTEMPTS
                    or _sqlstate(error) not in RETRYABLE_SQLSTATES
                ):
                    raise
                time.sleep(random.uniform(0, 0.01 * (2**attempt)))
        raise AssertionError("transition_retry_loop_exhausted")

    def get_accepted_result(self, lease: ExecutionLease) -> TransitionReceipt | None:
        """Read the accepted logical result for this step without requiring a live lease.

        Recovery must inspect durable authority before restoring a checkpoint or
        dispatching work. This read is deliberately owner-scoped and read-only;
        the lease is used to select the logical operation, not to authorize a
        new write.
        """
        if self._principal.account_id != lease.owner_id:
            raise LeaseLost("runtime_lease_owner_mismatch")
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            existing = (
                connection.execute(
                    text(
                        """
                        SELECT accepted.result_id, accepted.result_sha256,
                               accepted.intent_sha256, accepted.payload,
                               accepted.run_status, accepted.event_sequence,
                               accepted.next_job_id, accepted.artifact_id,
                               next_step.step_id AS next_step_id
                        FROM runtime_private.accepted_results AS accepted
                        LEFT JOIN runtime_private.jobs AS next_step
                          ON next_step.owner_id=accepted.owner_id
                         AND next_step.run_id=accepted.run_id
                         AND next_step.job_id=accepted.next_job_id
                        WHERE accepted.owner_id=:owner_id
                          AND accepted.run_id=:run_id
                          AND accepted.step_id=:step_id
                          AND accepted.job_id=:job_id
                        """
                    ),
                    {
                        "owner_id": lease.owner_id,
                        "run_id": lease.run_id,
                        "step_id": lease.step_id,
                        "job_id": lease.job_id,
                    },
                )
                .mappings()
                .one_or_none()
            )
        if existing is None:
            return None
        return _receipt_from_row(
            existing,
            run_id=lease.run_id,
            step_id=lease.step_id,
            job_id=lease.job_id,
            adopted=True,
            artifact_id=existing["artifact_id"],
        )

    def _commit_once(
        self,
        *,
        lease: ExecutionLease,
        result_json: str,
        result_digest: bytes,
        intent_digest: bytes,
        target_run_status: RunStatus,
        next_work: NextWork | None,
        artifact_id: UUID | None,
        publication_binding: tuple[UUID, bytes] | None = None,
    ) -> TransitionReceipt:
        with self._engine.begin() as connection:
            repository = RuntimeRepository(connection, self._principal)
            owner_id = self._principal.account_id
            run_id = lease.run_id
            step_id = lease.step_id
            job_id = lease.job_id
            run = connection.execute(
                text(
                    """
                    SELECT status
                    FROM runtime_private.runs
                    WHERE owner_id=:owner_id AND run_id=:run_id
                    FOR UPDATE
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ).one_or_none()
            if run is None:
                raise RuntimeRecordNotFound("runtime_run_not_found")

            step = connection.execute(
                text(
                    """
                    SELECT status
                    FROM runtime_private.steps
                    WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id
                    FOR UPDATE
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id, "step_id": step_id},
            ).one_or_none()
            if step is None:
                raise RuntimeRecordNotFound("runtime_step_not_found")

            job = connection.execute(
                text(
                    """
                    SELECT status, lease_owner
                    FROM runtime_private.jobs
                    WHERE owner_id=:owner_id AND run_id=:run_id
                      AND step_id=:step_id AND job_id=:job_id
                    FOR UPDATE
                    """
                ),
                {
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "step_id": step_id,
                    "job_id": job_id,
                },
            ).one_or_none()
            if job is None:
                raise RuntimeRecordNotFound("runtime_job_not_found")

            existing = (
                connection.execute(
                    text(
                        """
                    SELECT result_id, result_sha256, intent_sha256, payload,
                           run_status, event_sequence, next_job_id,
                           next_step.step_id AS next_step_id
                    FROM runtime_private.accepted_results AS accepted
                    LEFT JOIN runtime_private.jobs AS next_step
                      ON next_step.owner_id=accepted.owner_id
                     AND next_step.run_id=accepted.run_id
                     AND next_step.job_id=accepted.next_job_id
                    WHERE accepted.owner_id=:owner_id AND accepted.run_id=:run_id
                      AND accepted.step_id=:step_id AND accepted.job_id=:job_id
                    """
                    ),
                    {"owner_id": owner_id, "run_id": run_id, "step_id": step_id, "job_id": job_id},
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if (
                    existing["result_sha256"] != result_digest
                    or existing["intent_sha256"] != intent_digest
                ):
                    raise ResultIdentityConflict("accepted_result_intent_conflict")
                return _receipt_from_row(
                    existing,
                    run_id=run_id,
                    step_id=step_id,
                    job_id=job_id,
                    adopted=True,
                    artifact_id=artifact_id,
                )

            if artifact_id is not None and self._artifact_persistence is None:
                raise InvalidTransition("runtime_artifact_persistence_unavailable")

            # Re-locking held rows is safe; this also locks the exact attempt
            # and obtains database time only after run, step, job, and attempt.
            locked = lock_current_execution(connection, self._principal, lease)
            if locked.run_status != RunStatus.RUNNING.value:
                raise InvalidTransition("runtime_run_not_running")

            if publication_binding is not None:
                _assert_publication_complete(connection, lease, *publication_binding)

            artifact_reference: ArtifactReference | None = None
            if artifact_id is not None:
                assert self._artifact_persistence is not None
                artifact_reference = self._artifact_persistence.attach_verified(
                    connection,
                    self._principal,
                    lease,
                    artifact_id,
                )
                if artifact_reference.artifact_id != artifact_id:
                    raise InvalidTransition("runtime_artifact_identity_mismatch")

            connection.execute(
                text(
                    """
                    UPDATE runtime_private.steps SET status='succeeded'
                    WHERE owner_id=:owner_id AND run_id=:run_id AND step_id=:step_id
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id, "step_id": step_id},
            )
            connection.execute(
                text(
                    """
                    UPDATE runtime_private.jobs
                    SET status='succeeded', lease_owner=NULL, lease_expires_at=NULL
                    WHERE owner_id=:owner_id AND run_id=:run_id
                      AND step_id=:step_id AND job_id=:job_id
                    """
                ),
                {
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "step_id": step_id,
                    "job_id": job_id,
                },
            )
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET status=:status "
                    "WHERE owner_id=:owner_id AND run_id=:run_id"
                ),
                {
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "status": target_run_status.value,
                },
            )
            attempt_update = connection.execute(
                text(
                    """
                    UPDATE runtime_private.attempts
                    SET status='succeeded', finished_at=:finished_at
                    WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id
                      AND generation=:generation AND attempt_id=:attempt_id
                      AND lease_owner=:lease_owner AND status='running'
                    """
                ),
                {
                    "finished_at": locked.database_now,
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "job_id": job_id,
                    "generation": lease.generation,
                    "attempt_id": lease.attempt_id,
                    "lease_owner": lease.lease_owner,
                },
            )
            if attempt_update.rowcount != 1:
                raise LeaseLost("runtime_attempt_not_current")

            next_step_id: UUID | None = None
            next_job_id: UUID | None = None
            if next_work is not None:
                next_step_id = repository.create_step(
                    run_id,
                    next_work.logical_key,
                    next_work.activity,
                    revision=next_work.revision,
                )
                next_job_id = repository.create_job(
                    run_id, next_step_id, replay_policy=next_work.replay_policy
                )

            event_sequence: int = connection.execute(
                text(
                    """
                    SELECT coalesce(max(sequence), 0) + 1
                    FROM runtime_private.events
                    WHERE owner_id=:owner_id AND run_id=:run_id
                    """
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ).scalar_one()
            result_id = uuid4()
            event_payload: dict[str, Any] = {
                "next_job_id": None if next_job_id is None else str(next_job_id),
                "result_id": str(result_id),
                "result_sha256": result_digest.hex(),
                "step_id": str(step_id),
                "target_run_status": target_run_status.value,
            }
            if artifact_reference is not None:
                event_payload.update(
                    {
                        "artifact_id": str(artifact_reference.artifact_id),
                        "archive_sha256": artifact_reference.archive_sha256,
                        "tree_sha256": artifact_reference.tree_sha256,
                        "format_version": artifact_reference.format_version,
                        "compressed_bytes": artifact_reference.compressed_bytes,
                        "source_bytes": artifact_reference.source_bytes,
                        "file_count": artifact_reference.file_count,
                    }
                )
                if artifact_reference.parent_artifact_id is not None:
                    event_payload["parent_artifact_id"] = str(artifact_reference.parent_artifact_id)
            repository.append_event(
                run_id,
                event_sequence,
                "activity.result_accepted",
                EventActor.WORKER,
                actor_id=lease.lease_owner,
                payload=event_payload,
            )
            if artifact_id is None:
                insert_sql = """
                    INSERT INTO runtime_private.accepted_results (
                        result_id, owner_id, run_id, step_id, job_id,
                        result_sha256, intent_sha256, run_status, event_sequence,
                        next_job_id, payload, generation, attempt_id, lease_owner
                    ) VALUES (
                        :result_id, :owner_id, :run_id, :step_id, :job_id,
                        :result_sha256, :intent_sha256, :run_status, :event_sequence,
                        :next_job_id, CAST(:payload AS jsonb), :generation, :attempt_id,
                        :lease_owner
                    )
                    """
            else:
                insert_sql = """
                    INSERT INTO runtime_private.accepted_results (
                        result_id, owner_id, run_id, step_id, job_id,
                        result_sha256, intent_sha256, run_status, event_sequence,
                        next_job_id, payload, generation, attempt_id, lease_owner, artifact_id
                    ) VALUES (
                        :result_id, :owner_id, :run_id, :step_id, :job_id,
                        :result_sha256, :intent_sha256, :run_status, :event_sequence,
                        :next_job_id, CAST(:payload AS jsonb), :generation, :attempt_id,
                        :lease_owner, :artifact_id
                    )
                    """
            insert_parameters = {
                "result_id": result_id,
                "owner_id": owner_id,
                "run_id": run_id,
                "step_id": step_id,
                "job_id": job_id,
                "result_sha256": result_digest,
                "intent_sha256": intent_digest,
                "run_status": target_run_status.value,
                "event_sequence": event_sequence,
                "next_job_id": next_job_id,
                "payload": result_json,
                "generation": lease.generation,
                "attempt_id": lease.attempt_id,
                "lease_owner": lease.lease_owner,
            }
            if artifact_id is not None:
                insert_parameters["artifact_id"] = artifact_id
            connection.execute(text(insert_sql), insert_parameters)
            final_database_now: datetime = connection.execute(
                text("SELECT clock_timestamp()")
            ).scalar_one()
            if locked.lease_expires_at is None or locked.lease_expires_at <= final_database_now:
                raise LeaseLost("runtime_lease_expired_before_commit")
            if locked.run_deadline_at is not None and locked.run_deadline_at <= final_database_now:
                raise LeaseLost("runtime_run_expired_before_commit")
            return TransitionReceipt(
                result_id=result_id,
                run_id=run_id,
                step_id=step_id,
                job_id=job_id,
                result_sha256=result_digest,
                result=json.loads(result_json),
                run_status=target_run_status,
                event_sequence=event_sequence,
                next_step_id=next_step_id,
                next_job_id=next_job_id,
                adopted=False,
                artifact_id=artifact_id,
            )

    @staticmethod
    def _validate_plan(target_run_status: RunStatus, next_work: NextWork | None) -> None:
        _validate_transition_plan(target_run_status, next_work)


def _assert_publication_complete(
    connection: Connection, lease: ExecutionLease, approval_id: UUID, digest: bytes
) -> None:
    """Read the completion proof under the same run lock as the accepted result."""
    from continuum_api.approvals.manifest import ApprovalManifestError, digest_manifest

    parameters = {
        "owner": lease.owner_id,
        "run": lease.run_id,
        "step": lease.step_id,
        "job": lease.job_id,
        "approval": approval_id,
    }
    row = (
        connection.execute(
            text(
                "SELECT approval.manifest, approval.manifest_sha256, approval.status, "
                "approval.revision, run.revision AS run_revision, step.revision AS step_revision, "
                "step.activity, step.logical_key FROM runtime_private.approvals AS approval "
                "JOIN runtime_private.runs AS run USING(owner_id, run_id) "
                "JOIN runtime_private.steps AS step ON step.owner_id=run.owner_id "
                "AND step.run_id=run.run_id AND step.step_id=:step "
                "WHERE approval.owner_id=:owner AND approval.run_id=:run "
                "AND approval.approval_id=:approval"
            ),
            parameters,
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise InvalidTransition("publication_completion_binding_missing")
    key = row["logical_key"]
    root = f"publish-approved:{approval_id.hex}"
    if (
        row["status"] != "approved"
        or bytes(row["manifest_sha256"]) != digest
        or row["revision"] != row["run_revision"]
        or row["revision"] != row["step_revision"]
        or row["activity"] != "publish_approved"
        or re.fullmatch(rf"{root}(?::part:(?:0|[1-9][0-9]{{0,5}}))?", key) is None
    ):
        raise InvalidTransition("publication_completion_binding_invalid")
    try:
        manifest = row["manifest"]
        _encoded, actual_digest = digest_manifest(manifest)
        branch_metadata = {
            "operation": "update_managed_branch",
            "branch": manifest["branch"]["name"],
            "desired_sha": manifest["commit"]["sha"],
        }
        pr = manifest["pull_request"]
        pr_metadata = {
            "operation": "create_pull_request",
            "repository_id": manifest["repository"]["id"],
            "owner": manifest["repository"]["owner"],
            "name": manifest["repository"]["name"],
            "title": pr["title"],
            "body": pr["body"],
            "base_branch": pr["base"],
            "base_sha": manifest["base"]["commit_sha"],
            "head_branch": pr["head"],
            "head_sha": manifest["commit"]["sha"],
            "marker": pr["marker"],
        }
    except (KeyError, TypeError, ApprovalManifestError):
        raise InvalidTransition("publication_completion_manifest_invalid") from None
    if actual_digest != digest:
        raise InvalidTransition("publication_completion_manifest_invalid")
    operations = (
        connection.execute(
            text(
                "SELECT operation_id, operation_key, status, approval_id, approval_sha256, "
                "request_metadata FROM runtime_private.external_operations "
                "WHERE owner_id=:owner AND run_id=:run "
                "AND operation_key IN ('github:managed-branch','github:pull-request')"
            ),
            parameters,
        )
        .mappings()
        .all()
    )
    by_key = {item["operation_key"]: item for item in operations}
    for op_key, metadata in (
        ("github:managed-branch", branch_metadata),
        ("github:pull-request", pr_metadata),
    ):
        operation = by_key.get(op_key)
        if (
            operation is None
            or operation["status"] != "confirmed"
            or operation["approval_id"] != approval_id
            or operation["approval_sha256"] is None
            or bytes(operation["approval_sha256"]) != digest
            or operation["request_metadata"] != metadata
        ):
            raise InvalidTransition("publication_completion_unconfirmed")
    reference = connection.execute(
        text(
            "SELECT observation.details FROM runtime_private.external_operation_observations "
            "AS observation JOIN runtime_private.external_operations AS operation "
            "USING(owner_id,run_id,operation_id) WHERE operation.owner_id=:owner "
            "AND operation.run_id=:run AND operation.operation_key='github:pull-request' "
            "AND observation.observation_key='pull-request-reference' "
            "AND observation.outcome='confirmed'"
        ),
        parameters,
    ).scalar_one_or_none()
    if (
        not isinstance(reference, dict)
        or type(reference.get("number")) is not int
        or reference["number"] <= 0
        or type(reference.get("id")) is not int
        or reference["id"] <= 0
        or reference.get("url")
        != f"https://github.com/{pr_metadata['owner']}/{pr_metadata['name']}/pull/{reference['number']}"
    ):
        raise InvalidTransition("publication_completion_reference_invalid")
    checks = (
        ("steps", "step_id<>:step AND status<>'succeeded'"),
        ("jobs", "job_id<>:job AND status<>'succeeded'"),
        ("external_operations", "status<>'confirmed'"),
        ("sandbox_allocations", "status NOT IN ('stopped','destroyed','not_applied','released')"),
        ("resource_reservations", "status<>'released'"),
        (
            "dispatch_intents",
            "observation IS NULL OR "
            "coalesce(observation->>'state','') NOT IN ('completed','confirmed')",
        ),
        ("cleanup_intents", "status='pending'"),
    )
    for table, predicate in checks:
        exists = connection.execute(
            text(
                f"SELECT EXISTS(SELECT 1 FROM runtime_private.{table} "
                f"WHERE owner_id=:owner AND run_id=:run AND ({predicate}))"
            ),
            parameters,
        ).scalar_one()
        if exists:
            raise InvalidTransition("publication_completion_unresolved_work")


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )


def transition_intent_sha256(
    result: dict[str, Any],
    target_run_status: RunStatus,
    next_work: NextWork | None = None,
) -> bytes:
    """Return the canonical transition intent digest before an artifact ID exists."""
    _validate_transition_plan(target_run_status, next_work)
    result_json = _canonical_json(result)
    if len(result_json.encode("utf-8")) > MAX_RESULT_BYTES:
        raise InvalidTransition("accepted_result_too_large")
    intent_json = _transition_intent_json(result, target_run_status, next_work)
    return hashlib.sha256(intent_json.encode("utf-8")).digest()


def _transition_intent_json(
    result: dict[str, Any],
    target_run_status: RunStatus,
    next_work: NextWork | None,
    *,
    artifact_id: UUID | None = None,
) -> str:
    intent: dict[str, Any] = {
        "next_work": None
        if next_work is None
        else {
            "activity": next_work.activity,
            "logical_key": next_work.logical_key,
            "revision": next_work.revision,
            **(
                {"replay_policy": next_work.replay_policy.value}
                if next_work.replay_policy is ReplayPolicy.REPLAY_SAFE
                else {}
            ),
        },
        "result": result,
        "target_run_status": target_run_status.value,
    }
    if artifact_id is not None:
        intent["artifact_id"] = str(artifact_id)
    return _canonical_json(intent)


def _validate_transition_plan(target_run_status: RunStatus, next_work: NextWork | None) -> None:
    if target_run_status in {RunStatus.SUCCEEDED, RunStatus.CANCELLED, RunStatus.EXPIRED}:
        raise InvalidTransition("activity_result_cannot_set_terminal_run_status")
    if next_work is not None:
        if target_run_status is not RunStatus.RUNNING:
            raise InvalidTransition("next_work_requires_running_status")
        return
    if target_run_status not in _WAIT_OR_TERMINAL_RUN_STATES:
        raise InvalidTransition("transition_requires_next_work_or_wait_or_terminal_status")


def _receipt_from_row(
    row: Any,
    *,
    run_id: UUID,
    step_id: UUID,
    job_id: UUID,
    adopted: bool,
    artifact_id: UUID | None = None,
) -> TransitionReceipt:
    next_job_id = row["next_job_id"]
    return TransitionReceipt(
        result_id=row["result_id"],
        run_id=run_id,
        step_id=step_id,
        job_id=job_id,
        result_sha256=row["result_sha256"],
        result=row["payload"],
        run_status=RunStatus(row["run_status"]),
        event_sequence=row["event_sequence"],
        next_step_id=row["next_step_id"],
        next_job_id=next_job_id,
        adopted=adopted,
        artifact_id=artifact_id,
    )


def _sqlstate(error: DBAPIError) -> str | None:
    original = error.orig
    state = getattr(original, "sqlstate", None)
    if state is None:
        state = getattr(original, "pgcode", None)
    return state
