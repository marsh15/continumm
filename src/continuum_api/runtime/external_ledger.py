"""Fenced intent and observation ledger for effects outside PostgreSQL.

This module has no remote client. A caller may make one remote mutation only
when ``authorize_dispatch`` returns ``dispatch_allowed=True``. Replaying a
database transaction adopts its committed DISPATCHING row and never grants a
second send.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Engine, text

from continuum_api.runtime.cancellation import (
    _assert_dispatch_lease_is_current,
    _lock_dispatch_context,
    _next_event_sequence,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, _retry_database_only
from continuum_api.runtime.persistence import EventActor, Principal, RuntimeRepository

MAX_OPERATION_BYTES = 4096
MAX_OBSERVATION_BYTES = 4096
_OUTCOMES = frozenset({"confirmed", "definitely_failed", "unknown", "conflict"})
_ACTOR_TYPES = frozenset(actor.value for actor in EventActor)


class ExternalOperationError(Exception):
    """Base class for external operation ledger failures."""


class ExternalOperationConflict(ExternalOperationError):
    """A stable operation or observation identity was reused inconsistently."""


class ExternalOperationNotFound(ExternalOperationError):
    """The requested owner-scoped operation does not exist."""


class ExternalOperationNotDispatchable(ExternalOperationError):
    """The operation is not in PREPARED state and cannot be dispatched."""


@dataclass(frozen=True, slots=True)
class ExternalOperationReceipt:
    operation_id: UUID
    run_id: UUID
    operation_key: str
    generation: int
    attempt_id: UUID
    request_sha256: bytes
    request_metadata: dict[str, Any]
    status: str
    dispatch_allowed: bool
    adopted: bool
    event_sequence: int | None = None


class ExternalOperationService:
    """Persist external-effect intent, dispatch authorization, and observations."""

    def __init__(self, engine: Engine, principal: Principal) -> None:
        self._engine = engine
        self._principal = principal

    def prepare(
        self,
        lease: ExecutionLease,
        operation_key: str,
        request_metadata: dict[str, Any],
        *,
        approval_id: UUID | None = None,
        approval_sha256: bytes | None = None,
    ) -> ExternalOperationReceipt:
        operation_key = _validate_key(operation_key, "runtime_external_operation_key_invalid")
        request_json = _canonical_json(request_metadata)
        request_bytes = request_json.encode("utf-8")
        if len(request_bytes) > MAX_OPERATION_BYTES:
            raise ValueError("runtime_external_operation_metadata_too_large")
        if (approval_id is None) != (approval_sha256 is None):
            raise ValueError("runtime_external_operation_approval_binding_invalid")
        if approval_sha256 is not None and len(approval_sha256) != 32:
            raise ValueError("runtime_external_operation_approval_binding_invalid")
        request_digest = hashlib.sha256(request_bytes).digest()
        return _retry_database_only(
            lambda: self._prepare_once(
                lease, operation_key, request_json, request_digest, approval_id, approval_sha256
            )
        )

    def authorize_dispatch(
        self, lease: ExecutionLease, operation_key: str
    ) -> ExternalOperationReceipt:
        operation_key = _validate_key(operation_key, "runtime_external_operation_key_invalid")
        return _retry_database_only(lambda: self._authorize_dispatch_once(lease, operation_key))

    def observe(
        self,
        *,
        run_id: UUID,
        operation_key: str,
        dispatch_receipt: ExternalOperationReceipt,
        observation_key: str,
        outcome: str,
        details: dict[str, Any],
        actor_type: EventActor = EventActor.PROVIDER,
        actor_id: str | None = None,
    ) -> ExternalOperationReceipt:
        operation_key = _validate_key(operation_key, "runtime_external_operation_key_invalid")
        observation_key = _validate_key(observation_key, "runtime_external_observation_key_invalid")
        if outcome not in _OUTCOMES:
            raise ValueError("runtime_external_observation_outcome_invalid")
        if actor_type.value not in _ACTOR_TYPES:
            raise ValueError("runtime_external_observation_actor_invalid")
        if actor_id is not None:
            actor_id = actor_id.strip()
            if not actor_id or len(actor_id) > 128:
                raise ValueError("runtime_external_observation_actor_id_invalid")
        details_json = _canonical_json(details)
        observation_json = _canonical_json(
            {
                "actor_id": actor_id,
                "actor_type": actor_type.value,
                "details": details,
                "outcome": outcome,
            }
        )
        if len(observation_json.encode("utf-8")) > MAX_OBSERVATION_BYTES:
            raise ValueError("runtime_external_observation_too_large")
        return _retry_database_only(
            lambda: self._observe_once(
                run_id,
                operation_key,
                dispatch_receipt,
                observation_key,
                outcome,
                details_json,
                actor_type,
                actor_id,
                observation_json,
            )
        )

    def _prepare_once(
        self,
        lease: ExecutionLease,
        operation_key: str,
        request_json: str,
        request_digest: bytes,
        approval_id: UUID | None,
        approval_sha256: bytes | None,
    ) -> ExternalOperationReceipt:
        with self._engine.begin() as connection:
            repository = RuntimeRepository(connection, self._principal)
            locked = _lock_dispatch_context(connection, self._principal, lease)
            owner_id = self._principal.account_id
            existing = (
                connection.execute(
                    text(
                        "SELECT operation_id, step_id, job_id, generation, attempt_id, "
                        "lease_owner, "
                        "request_sha256, request_metadata, approval_id, approval_sha256, status "
                        "FROM runtime_private.external_operations WHERE owner_id=:owner_id "
                        "AND run_id=:run_id AND operation_key=:operation_key FOR UPDATE"
                    ),
                    {"owner_id": owner_id, "run_id": lease.run_id, "operation_key": operation_key},
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if (
                    existing["request_sha256"] != request_digest
                    or existing["approval_id"] != approval_id
                    or existing["approval_sha256"] != approval_sha256
                ):
                    raise ExternalOperationConflict("runtime_external_operation_conflict")
                adopted = True
                receipt_data = dict(existing)
                if existing["status"] == "prepared":
                    _assert_dispatch_lease_is_current(locked, lease)
                    if not _same_lease(existing, lease):
                        # PREPARED proves no remote dispatch was authorized, so a
                        # current generation can safely adopt the durable intent.
                        connection.execute(
                            text(
                                "UPDATE runtime_private.external_operations SET step_id=:step_id, "
                                "job_id=:job_id, generation=:generation, attempt_id=:attempt_id, "
                                "lease_owner=:lease_owner, updated_at=clock_timestamp() "
                                "WHERE operation_id=:operation_id AND status='prepared'"
                            ),
                            _lease_values(lease) | {"operation_id": existing["operation_id"]},
                        )
                        receipt_data.update(_lease_values(lease))
                return _receipt(receipt_data, lease.run_id, operation_key, adopted=adopted)

            _assert_dispatch_lease_is_current(locked, lease)
            operation_id = uuid4()
            connection.execute(
                text(
                    "INSERT INTO runtime_private.external_operations "
                    "(operation_id, owner_id, run_id, step_id, job_id, generation, attempt_id, "
                    "lease_owner, operation_key, request_sha256, request_metadata, approval_id, "
                    "approval_sha256) VALUES (:operation_id, :owner_id, :run_id, "
                    ":step_id, :job_id, "
                    ":generation, :attempt_id, :lease_owner, :operation_key, :request_sha256, "
                    "CAST(:request_metadata AS jsonb), :approval_id, :approval_sha256)"
                ),
                {
                    "operation_id": operation_id,
                    "owner_id": owner_id,
                    "run_id": lease.run_id,
                    **_lease_values(lease),
                    "operation_key": operation_key,
                    "request_sha256": request_digest,
                    "request_metadata": request_json,
                    "approval_id": approval_id,
                    "approval_sha256": approval_sha256,
                },
            )
            event_sequence = _next_event_sequence(connection, owner_id, lease.run_id)
            repository.append_event(
                lease.run_id,
                event_sequence,
                "external_operation.prepared",
                EventActor.WORKER,
                actor_id=lease.lease_owner,
                payload={"operation_id": str(operation_id), "operation_key": operation_key},
            )
            return ExternalOperationReceipt(
                operation_id=operation_id,
                run_id=lease.run_id,
                operation_key=operation_key,
                generation=lease.generation,
                attempt_id=lease.attempt_id,
                request_sha256=request_digest,
                request_metadata=json.loads(request_json),
                status="prepared",
                dispatch_allowed=False,
                adopted=False,
                event_sequence=event_sequence,
            )

    def _authorize_dispatch_once(
        self, lease: ExecutionLease, operation_key: str
    ) -> ExternalOperationReceipt:
        with self._engine.begin() as connection:
            repository = RuntimeRepository(connection, self._principal)
            locked = _lock_dispatch_context(connection, self._principal, lease)
            owner_id = self._principal.account_id
            operation = (
                connection.execute(
                    text(
                        "SELECT operation_id, step_id, job_id, generation, attempt_id, "
                        "lease_owner, "
                        "request_sha256, request_metadata, approval_id, approval_sha256, status "
                        "FROM runtime_private.external_operations WHERE owner_id=:owner_id "
                        "AND run_id=:run_id AND operation_key=:operation_key FOR UPDATE"
                    ),
                    {"owner_id": owner_id, "run_id": lease.run_id, "operation_key": operation_key},
                )
                .mappings()
                .one_or_none()
            )
            if operation is None:
                raise ExternalOperationNotFound("runtime_external_operation_not_found")
            if not _same_lease(operation, lease):
                raise LeaseLost("runtime_external_operation_lease_mismatch")
            if operation["status"] != "prepared":
                return _receipt(operation, lease.run_id, operation_key, adopted=True)
            _assert_dispatch_lease_is_current(locked, lease)
            approval_expires_at: datetime | None = None
            if operation["approval_id"] is not None:
                approval = (
                    connection.execute(
                        text(
                            "SELECT approval.status, approval.manifest_sha256, "
                            "approval.manifest, approval.expires_at, approval.revision, "
                            "run.revision AS run_revision "
                            "FROM runtime_private.approvals AS approval "
                            "JOIN runtime_private.runs AS run "
                            "ON run.owner_id=approval.owner_id AND run.run_id=approval.run_id "
                            "WHERE approval.owner_id=:owner_id AND approval.run_id=:run_id "
                            "AND approval.approval_id=:approval_id FOR UPDATE OF approval"
                        ),
                        {
                            "owner_id": owner_id,
                            "run_id": lease.run_id,
                            "approval_id": operation["approval_id"],
                        },
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    approval is None
                    or approval["status"] != "approved"
                    or approval["manifest_sha256"] != operation["approval_sha256"]
                    or approval["revision"] != approval["run_revision"]
                ):
                    raise ExternalOperationNotDispatchable(
                        "runtime_external_operation_approval_not_current"
                    )
                action = operation["request_metadata"].get("operation")
                if not isinstance(action, str) or action not in approval["manifest"].get(
                    "requested_remote_operations", []
                ):
                    raise ExternalOperationNotDispatchable(
                        "runtime_external_operation_not_in_approved_manifest"
                    )
                approval_expires_at = approval["expires_at"]
            now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if locked["lease_expires_at"] <= now or (
                locked["run_deadline_at"] is not None and locked["run_deadline_at"] <= now
            ):
                raise LeaseLost("runtime_external_operation_lease_lost")
            if approval_expires_at is not None and approval_expires_at <= now:
                raise ExternalOperationNotDispatchable(
                    "runtime_external_operation_approval_expired"
                )
            connection.execute(
                text(
                    "UPDATE runtime_private.external_operations SET status='dispatching', "
                    "dispatch_authorized_at=:now, updated_at=:now WHERE operation_id=:operation_id "
                    "AND status='prepared'"
                ),
                {"now": now, "operation_id": operation["operation_id"]},
            )
            event_sequence = _next_event_sequence(connection, owner_id, lease.run_id)
            repository.append_event(
                lease.run_id,
                event_sequence,
                "external_operation.dispatching",
                EventActor.WORKER,
                actor_id=lease.lease_owner,
                payload={
                    "operation_id": str(operation["operation_id"]),
                    "operation_key": operation_key,
                },
            )
            commit_now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if locked["lease_expires_at"] <= commit_now or (
                locked["run_deadline_at"] is not None and locked["run_deadline_at"] <= commit_now
            ):
                raise LeaseLost("runtime_external_operation_lease_lost_before_commit")
            if approval_expires_at is not None and approval_expires_at <= commit_now:
                raise ExternalOperationNotDispatchable(
                    "runtime_external_operation_approval_expired_before_commit"
                )
            return ExternalOperationReceipt(
                operation_id=operation["operation_id"],
                run_id=lease.run_id,
                operation_key=operation_key,
                generation=lease.generation,
                attempt_id=lease.attempt_id,
                request_sha256=operation["request_sha256"],
                request_metadata=operation["request_metadata"],
                status="dispatching",
                dispatch_allowed=True,
                adopted=False,
                event_sequence=event_sequence,
            )

    def _observe_once(
        self,
        run_id: UUID,
        operation_key: str,
        dispatch_receipt: ExternalOperationReceipt,
        observation_key: str,
        outcome: str,
        details_json: str,
        actor_type: EventActor,
        actor_id: str | None,
        observation_json: str,
    ) -> ExternalOperationReceipt:
        with self._engine.begin() as connection:
            repository = RuntimeRepository(connection, self._principal)
            owner_id = self._principal.account_id
            run = connection.execute(
                text(
                    "SELECT run_id FROM runtime_private.runs WHERE owner_id=:owner_id "
                    "AND run_id=:run_id FOR UPDATE"
                ),
                {"owner_id": owner_id, "run_id": run_id},
            ).one_or_none()
            if run is None:
                raise ExternalOperationNotFound("runtime_external_operation_not_found")
            operation = (
                connection.execute(
                    text(
                        "SELECT operation_id, generation, attempt_id, request_sha256, "
                        "request_metadata, status "
                        "FROM runtime_private.external_operations WHERE owner_id=:owner_id "
                        "AND run_id=:run_id AND operation_key=:operation_key FOR UPDATE"
                    ),
                    {"owner_id": owner_id, "run_id": run_id, "operation_key": operation_key},
                )
                .mappings()
                .one_or_none()
            )
            if operation is None:
                raise ExternalOperationNotFound("runtime_external_operation_not_found")
            if (
                dispatch_receipt.run_id != run_id
                or dispatch_receipt.operation_key != operation_key
                or dispatch_receipt.operation_id != operation["operation_id"]
                or dispatch_receipt.generation != operation["generation"]
                or dispatch_receipt.attempt_id != operation["attempt_id"]
                or dispatch_receipt.request_sha256 != operation["request_sha256"]
            ):
                raise ExternalOperationConflict("runtime_external_dispatch_identity_mismatch")
            previous = (
                connection.execute(
                    text(
                        "SELECT outcome, details, actor_type, actor_id FROM "
                        "runtime_private.external_operation_observations WHERE owner_id=:owner_id "
                        "AND run_id=:run_id AND operation_id=:operation_id "
                        "AND observation_key=:observation_key"
                    ),
                    {
                        "owner_id": owner_id,
                        "run_id": run_id,
                        "operation_id": operation["operation_id"],
                        "observation_key": observation_key,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if previous is not None:
                previous_json = _canonical_json(
                    {
                        "actor_id": previous["actor_id"],
                        "actor_type": previous["actor_type"],
                        "details": previous["details"],
                        "outcome": previous["outcome"],
                    }
                )
                if previous_json != observation_json:
                    raise ExternalOperationConflict("runtime_external_observation_conflict")
                return _receipt(operation, run_id, operation_key, adopted=True)
            if operation["status"] == "prepared":
                raise ExternalOperationNotDispatchable("runtime_external_operation_not_dispatched")

            observation_id = uuid4()
            connection.execute(
                text(
                    "INSERT INTO runtime_private.external_operation_observations "
                    "(observation_id, owner_id, run_id, operation_id, observation_key, outcome, "
                    "details, actor_type, actor_id) VALUES (:observation_id, :owner_id, :run_id, "
                    ":operation_id, :observation_key, :outcome, CAST(:details AS jsonb), "
                    ":actor_type, :actor_id)"
                ),
                {
                    "observation_id": observation_id,
                    "owner_id": owner_id,
                    "run_id": run_id,
                    "operation_id": operation["operation_id"],
                    "observation_key": observation_key,
                    "outcome": outcome,
                    "details": details_json,
                    "actor_type": actor_type.value,
                    "actor_id": actor_id,
                },
            )
            status = _next_status(operation["status"], outcome)
            connection.execute(
                text(
                    "UPDATE runtime_private.external_operations SET status=:status, "
                    "updated_at=clock_timestamp() WHERE operation_id=:operation_id"
                ),
                {"status": status, "operation_id": operation["operation_id"]},
            )
            event_sequence = _next_event_sequence(connection, owner_id, run_id)
            repository.append_event(
                run_id,
                event_sequence,
                "external_operation.observed",
                actor_type,
                actor_id=actor_id,
                payload={
                    "observation_id": str(observation_id),
                    "observation_key": observation_key,
                    "operation_id": str(operation["operation_id"]),
                    "operation_key": operation_key,
                    "outcome": outcome,
                    "status": status,
                },
            )
            return ExternalOperationReceipt(
                operation_id=operation["operation_id"],
                run_id=run_id,
                operation_key=operation_key,
                generation=operation["generation"],
                attempt_id=operation["attempt_id"],
                request_sha256=operation["request_sha256"],
                request_metadata=operation["request_metadata"],
                status=status,
                dispatch_allowed=False,
                adopted=False,
                event_sequence=event_sequence,
            )


def _next_status(current: str, outcome: str) -> str:
    if current == "conflict":
        return "conflict"
    if current in {"confirmed", "definitely_failed"}:
        return current if current == outcome else "conflict"
    if outcome == "conflict":
        return "conflict"
    return outcome


def _same_lease(operation: Any, lease: ExecutionLease) -> bool:
    return bool(
        operation["step_id"] == lease.step_id
        and operation["job_id"] == lease.job_id
        and operation["generation"] == lease.generation
        and operation["attempt_id"] == lease.attempt_id
        and operation["lease_owner"] == lease.lease_owner
    )


def _lease_values(lease: ExecutionLease) -> dict[str, Any]:
    return {
        "step_id": lease.step_id,
        "job_id": lease.job_id,
        "generation": lease.generation,
        "attempt_id": lease.attempt_id,
        "lease_owner": lease.lease_owner,
    }


def _receipt(
    operation: Any, run_id: UUID, operation_key: str, *, adopted: bool
) -> ExternalOperationReceipt:
    return ExternalOperationReceipt(
        operation_id=operation["operation_id"],
        run_id=run_id,
        operation_key=operation_key,
        generation=operation["generation"],
        attempt_id=operation["attempt_id"],
        request_sha256=operation["request_sha256"],
        request_metadata=operation["request_metadata"],
        status=operation["status"],
        dispatch_allowed=False,
        adopted=adopted,
    )


def _validate_key(value: str, error: str) -> str:
    value = value.strip()
    if not value or len(value) > 128:
        raise ValueError(error)
    return value


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )
