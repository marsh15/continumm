"""Owner-scoped encrypted BYOK storage and one-use dispatch authorizations."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import SecretStr
from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import SQLAlchemyError

from continuum_api.credentials import (
    CredentialCipher,
    CredentialEnvelope,
    CredentialError,
    CredentialIdentity,
    CredentialProvider,
    CredentialScope,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, lock_current_execution
from continuum_api.runtime.persistence import EventActor, Principal, RuntimeRepository

_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "expired"})
_PAUSABLE = frozenset({"queued", "running", "waiting_capacity", "waiting_credentials"})


@dataclass(frozen=True, slots=True)
class CredentialMetadata:
    credential_id: UUID
    provider: CredentialProvider
    scope: CredentialScope
    run_id: UUID | None
    key_version: str
    status: str
    created_at: datetime
    expires_at: datetime | None


@dataclass(frozen=True, slots=True)
class CredentialDispatchGrant:
    """Internal adapter value. Never put this object in prompts or tool arguments."""

    grant_id: UUID
    credential_id: UUID
    provider: CredentialProvider
    request_id: UUID
    secret: SecretStr = field(repr=False)


class CredentialService:
    def __init__(
        self, engine: Engine, principal: Principal, cipher: CredentialCipher | None
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._cipher = cipher

    @contextmanager
    def _connection(self) -> Iterator[Connection]:
        try:
            with self._engine.begin() as connection:
                RuntimeRepository(connection, self._principal)
                yield connection
        except SQLAlchemyError:
            raise CredentialError("credential_storage_unavailable") from None

    def save(
        self,
        provider: CredentialProvider,
        secret: SecretStr,
        *,
        scope: CredentialScope,
        run_id: UUID | None = None,
        saved_key_opt_in: bool = False,
    ) -> CredentialMetadata:
        if (
            not isinstance(provider, CredentialProvider)
            or provider not in {CredentialProvider.OPENAI, CredentialProvider.ANTHROPIC}
            or not isinstance(scope, CredentialScope)
        ):
            raise CredentialError("credential_identity_invalid")
        if scope is CredentialScope.SAVED and saved_key_opt_in is not True:
            raise CredentialError("credential_saved_opt_in_required")
        identity = CredentialIdentity(uuid4(), self._principal.account_id, provider, scope, run_id)
        envelope = self._require_cipher().encrypt(identity, secret)
        with self._connection() as connection:
            self._lock_user(connection, active=True)
            expires_at = None
            if scope is CredentialScope.RUN:
                run = self._lock_run(connection, run_id)
                self._require_open_run(connection, run)
                expires_at = run["content_deadline"]
            count: int = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.provider_credentials "
                    "WHERE owner_id=:owner AND status='active'"
                ),
                {"owner": self._principal.account_id},
            ).scalar_one()
            if count >= 50:
                raise CredentialError("credential_count_limit")
            row = (
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.provider_credentials "
                        "(credential_id, owner_id, provider, scope, scope_run_id, key_version, "
                        "nonce, ciphertext, expires_at) VALUES "
                        "(:id, :owner, :provider, :scope, :run, :version, :nonce, "
                        ":ciphertext, :expires) "
                        "RETURNING *"
                    ),
                    {
                        "id": identity.credential_id,
                        "owner": identity.owner_id,
                        "provider": provider.value,
                        "scope": scope.value,
                        "run": run_id,
                        "version": envelope.key_version,
                        "nonce": envelope.nonce,
                        "ciphertext": envelope.ciphertext,
                        "expires": expires_at,
                    },
                )
                .mappings()
                .one()
            )
            return _metadata(dict(row))

    def list_metadata(self) -> tuple[CredentialMetadata, ...]:
        with self._connection() as connection:
            rows = connection.execute(
                text(
                    "SELECT credential_id, provider, scope, scope_run_id, key_version, "
                    "status, created_at, expires_at FROM runtime_private.provider_credentials "
                    "WHERE owner_id=:owner AND status='active' "
                    "ORDER BY created_at DESC, credential_id LIMIT 50"
                ),
                {"owner": self._principal.account_id},
            ).mappings()
            return tuple(_metadata(dict(row)) for row in rows)

    def bind_run(self, run_id: UUID, provider: CredentialProvider, credential_id: UUID) -> None:
        if not isinstance(provider, CredentialProvider) or provider not in {
            CredentialProvider.OPENAI,
            CredentialProvider.ANTHROPIC,
        }:
            raise CredentialError("credential_provider_invalid")
        with self._connection() as connection:
            self.bind_run_in_transaction(connection, run_id, provider, credential_id)

    def bind_run_in_transaction(
        self,
        connection: Connection,
        run_id: UUID,
        provider: CredentialProvider,
        credential_id: UUID,
    ) -> None:
        """Bind an owned saved key inside a caller's transaction.

        Admission uses this to commit the new run, credential binding and the
        established credential event together. The same checks as ``bind_run``
        apply; no credential bytes enter this method.
        """
        if not connection.in_transaction():
            raise CredentialError("credential_transaction_required")
        if not isinstance(provider, CredentialProvider) or provider not in {
            CredentialProvider.OPENAI,
            CredentialProvider.ANTHROPIC,
        }:
            raise CredentialError("credential_provider_invalid")
        RuntimeRepository(connection, self._principal)
        self.lock_owner_in_transaction(connection, active=True)
        run = self._lock_run(connection, run_id)
        self._require_open_run(connection, run)
        credential = self._lock_credential(connection, credential_id)
        self._require_usable(connection, credential, provider, run_id)
        existing = connection.execute(
            text(
                "SELECT provider, credential_id FROM runtime_private.run_credentials "
                "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
            ),
            {"owner": self._principal.account_id, "run": run_id},
        ).one_or_none()
        if existing is not None and existing.provider != provider.value:
            raise CredentialError("credential_provider_pinned")
        if (
            existing is not None
            and existing.credential_id == credential_id
            and run["status"] != "waiting_credentials"
        ):
            return
        connection.execute(
            text(
                "INSERT INTO runtime_private.run_credentials "
                "(owner_id, run_id, provider, credential_id) VALUES "
                "(:owner, :run, :provider, :credential) ON CONFLICT (owner_id, run_id) "
                "DO UPDATE SET credential_id=EXCLUDED.credential_id, "
                "revision=run_credentials.revision + 1 "
                "WHERE run_credentials.credential_id<>EXCLUDED.credential_id"
            ),
            {
                "owner": self._principal.account_id,
                "run": run_id,
                "provider": provider.value,
                "credential": credential_id,
            },
        )
        if run["status"] == "waiting_credentials":
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET status='queued' "
                    "WHERE owner_id=:owner AND run_id=:run AND status='waiting_credentials'"
                ),
                {"owner": self._principal.account_id, "run": run_id},
            )
        self._event(connection, run_id, "credential.bound", credential_id)

    def lock_owner_in_transaction(self, connection: Connection, *, active: bool) -> None:
        """Take the shared owner credential lock before credential rows are locked."""
        if not connection.in_transaction():
            raise CredentialError("credential_transaction_required")
        RuntimeRepository(connection, self._principal)
        self._lock_user(connection, active=active)

    def revoke(self, credential_id: UUID) -> bool:
        """Protective owner action; no GitHub permission or decryption is required."""
        with self._connection() as connection:
            self._lock_user(connection, active=False)
            run_ids: list[UUID] = list(
                connection.execute(
                    text(
                        "SELECT binding.run_id FROM runtime_private.run_credentials AS binding "
                        "JOIN runtime_private.runs AS run ON run.owner_id=binding.owner_id "
                        "AND run.run_id=binding.run_id WHERE binding.owner_id=:owner "
                        "AND binding.credential_id=:credential "
                        "AND run.status IN ('queued','running','waiting_capacity', "
                        "'waiting_credentials') "
                        "ORDER BY binding.run_id"
                    ),
                    {"owner": self._principal.account_id, "credential": credential_id},
                )
                .scalars()
                .all()
            )
            runs = [(run_id, self._lock_run(connection, run_id)) for run_id in run_ids]
            credential = self._lock_credential(connection, credential_id)
            if credential["status"] != "active":
                return False
            connection.execute(
                text(
                    "UPDATE runtime_private.provider_credentials SET status='revoked', "
                    "ciphertext=NULL, nonce=NULL, ended_at=clock_timestamp() "
                    "WHERE owner_id=:owner AND credential_id=:credential AND status='active'"
                ),
                {"owner": self._principal.account_id, "credential": credential_id},
            )
            for run_id, run in runs:
                if run["status"] in _PAUSABLE:
                    self._pause_run(connection, run_id)
                    self._event(connection, run_id, "credential.revoked", credential_id)
            return True

    def authorize_dispatch(
        self, lease: ExecutionLease, provider: CredentialProvider, request_id: UUID
    ) -> CredentialDispatchGrant:
        """Authorize once under DB locks; provider I/O occurs after this returns.

        Revocation cannot recall an authorization already committed. A repeated
        request ID never returns a second secret/permission to send.
        """
        if lease.owner_id != self._principal.account_id:
            raise LeaseLost("runtime_lease_owner_mismatch")
        if (
            not isinstance(provider, CredentialProvider)
            or provider not in {CredentialProvider.OPENAI, CredentialProvider.ANTHROPIC}
            or not isinstance(request_id, UUID)
        ):
            raise CredentialError("credential_dispatch_identity_invalid")
        failure: str | None = None
        grant: CredentialDispatchGrant | None = None
        with self._connection() as connection:
            locked = lock_current_execution(connection, self._principal, lease)
            active_user = connection.execute(
                text("SELECT status FROM runtime_private.users WHERE user_id=:owner"),
                {"owner": self._principal.account_id},
            ).scalar_one_or_none()
            if active_user != "active":
                raise CredentialError("credential_account_unavailable")
            prior = connection.execute(
                text(
                    "SELECT grant_id FROM runtime_private.credential_dispatches "
                    "WHERE owner_id=:owner AND run_id=:run AND request_id=:request"
                ),
                {"owner": lease.owner_id, "run": lease.run_id, "request": request_id},
            ).scalar_one_or_none()
            if prior is not None:
                raise CredentialError("credential_dispatch_already_authorized")
            binding = (
                connection.execute(
                    text(
                        "SELECT credential_id, provider, revision "
                        "FROM runtime_private.run_credentials "
                        "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id},
                )
                .mappings()
                .one_or_none()
            )
            try:
                if binding is None or binding["provider"] != provider.value:
                    raise CredentialError("credential_unavailable")
                credential = self._lock_credential(connection, binding["credential_id"])
                self._require_usable(connection, credential, provider, lease.run_id)
                secret = self._require_cipher().decrypt(
                    _identity(credential), _envelope(credential)
                )
                grant = CredentialDispatchGrant(
                    uuid4(), credential["credential_id"], provider, request_id, secret
                )
            except CredentialError as error:
                failure = error.code
                self._pause_run(connection, lease.run_id)
                self._event(connection, lease.run_id, "run.waiting_credentials", None)
            if grant is not None and binding is not None:
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.credential_dispatches "
                        "(grant_id, owner_id, run_id, request_id, provider, credential_id, "
                        "binding_revision, job_id, generation, attempt_id, lease_owner) VALUES "
                        "(:grant, :owner, :run, :request, :provider, :credential, :revision, "
                        ":job, :generation, :attempt, :worker)"
                    ),
                    {
                        "grant": grant.grant_id,
                        "owner": lease.owner_id,
                        "run": lease.run_id,
                        "request": request_id,
                        "provider": provider.value,
                        "credential": grant.credential_id,
                        "revision": binding["revision"],
                        "job": lease.job_id,
                        "generation": lease.generation,
                        "attempt": lease.attempt_id,
                        "worker": lease.lease_owner,
                    },
                )
            now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if (
                locked.lease_expires_at is None
                or locked.lease_expires_at <= now
                or (locked.run_deadline_at is not None and locked.run_deadline_at <= now)
            ):
                raise LeaseLost("credential_dispatch_lease_lost")
        if failure is not None:
            raise CredentialError(failure)
        assert grant is not None
        return grant

    def rotate(self, credential_id: UUID) -> CredentialMetadata:
        with self._connection() as connection:
            self._lock_user(connection, active=True)
            scope_run_id = connection.execute(
                text(
                    "SELECT scope_run_id FROM runtime_private.provider_credentials "
                    "WHERE owner_id=:owner AND credential_id=:credential"
                ),
                {"owner": self._principal.account_id, "credential": credential_id},
            ).scalar_one_or_none()
            if scope_run_id is not None:
                run = self._lock_run(connection, scope_run_id)
                self._require_open_run(connection, run)
            row = self._lock_credential(connection, credential_id)
            now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            if row["status"] != "active" or (
                row["expires_at"] is not None and row["expires_at"] <= now
            ):
                raise CredentialError("credential_unavailable")
            envelope = self._require_cipher().rotate(_identity(row), _envelope(row))
            updated = (
                connection.execute(
                    text(
                        "UPDATE runtime_private.provider_credentials SET key_version=:version, "
                        "nonce=:nonce, ciphertext=:ciphertext WHERE owner_id=:owner "
                        "AND credential_id=:credential AND status='active' RETURNING *"
                    ),
                    {
                        "version": envelope.key_version,
                        "nonce": envelope.nonce,
                        "ciphertext": envelope.ciphertext,
                        "owner": self._principal.account_id,
                        "credential": credential_id,
                    },
                )
                .mappings()
                .one()
            )
            return _metadata(dict(updated))

    def sweep_terminal_material(self, *, limit: int = 50) -> int:
        """Idempotent repair for expired or terminal per-run material, under owner RLS."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise CredentialError("credential_cleanup_limit_invalid")
        with self._connection() as connection:
            rows: list[UUID] = list(
                connection.execute(
                    text(
                        "SELECT run.run_id FROM runtime_private.runs AS run "
                        "WHERE run.owner_id=:owner AND EXISTS (SELECT 1 "
                        "FROM runtime_private.provider_credentials AS credential "
                        "WHERE credential.owner_id=run.owner_id "
                        "AND credential.scope_run_id=run.run_id "
                        "AND credential.scope='run' AND credential.status='active' "
                        "AND (run.status IN ('succeeded','failed','cancelled','expired') "
                        "OR COALESCE(run.deadline_at,run.created_at+interval '7 days') "
                        "<=clock_timestamp() "
                        "OR credential.expires_at<=clock_timestamp())) "
                        "ORDER BY run.run_id LIMIT :limit FOR UPDATE OF run SKIP LOCKED"
                    ),
                    {"owner": self._principal.account_id, "limit": limit},
                )
                .scalars()
                .all()
            )
            count = 0
            for run_id in rows:
                result = connection.execute(
                    text(
                        "UPDATE runtime_private.provider_credentials SET status='deleted', "
                        "ciphertext=NULL, nonce=NULL, ended_at=clock_timestamp() "
                        "WHERE owner_id=:owner AND scope_run_id=:run AND scope='run' "
                        "AND status='active' AND (expires_at<=clock_timestamp() OR EXISTS "
                        "(SELECT 1 FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run "
                        "AND (status IN ('succeeded','failed','cancelled','expired') "
                        "OR COALESCE(deadline_at,created_at+interval '7 days') "
                        "<=clock_timestamp())))"
                    ),
                    {"owner": self._principal.account_id, "run": run_id},
                )
                count += result.rowcount
            return count

    def _require_cipher(self) -> CredentialCipher:
        if self._cipher is None:
            raise CredentialError("credential_encryption_unconfigured")
        return self._cipher

    def _lock_user(self, connection: Connection, *, active: bool) -> None:
        # Account credential mutations share a transaction-scoped lock without
        # granting the runtime permission to modify the user table.
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
            {"identity": f"continuum:credentials:{self._principal.account_id}"},
        )
        status = connection.execute(
            text("SELECT status FROM runtime_private.users WHERE user_id=:owner"),
            {"owner": self._principal.account_id},
        ).scalar_one_or_none()
        if status is None or (active and status != "active"):
            raise CredentialError("credential_account_unavailable")

    def _lock_run(self, connection: Connection, run_id: UUID | None) -> Mapping[str, Any]:
        row = (
            connection.execute(
                text(
                    "SELECT status, COALESCE(deadline_at, created_at+interval '7 days') "
                    "AS content_deadline FROM runtime_private.runs "
                    "WHERE owner_id=:owner AND run_id=:run FOR UPDATE"
                ),
                {"owner": self._principal.account_id, "run": run_id},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise CredentialError("credential_run_unavailable")
        return dict(row)

    def _lock_credential(self, connection: Connection, credential_id: UUID) -> Mapping[str, Any]:
        row = (
            connection.execute(
                text(
                    "SELECT * FROM runtime_private.provider_credentials "
                    "WHERE owner_id=:owner AND credential_id=:credential FOR UPDATE"
                ),
                {"owner": self._principal.account_id, "credential": credential_id},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise CredentialError("credential_not_found")
        return dict(row)

    @staticmethod
    def _require_open_run(connection: Connection, run: Mapping[str, Any]) -> None:
        now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
        if run["status"] in _TERMINAL or run["content_deadline"] <= now:
            raise CredentialError("credential_run_unavailable")

    @staticmethod
    def _require_usable(
        connection: Connection, row: Mapping[str, Any], provider: CredentialProvider, run_id: UUID
    ) -> None:
        now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
        if (
            row["provider"] != provider.value
            or row["status"] != "active"
            or (row["scope"] == "run" and row["scope_run_id"] != run_id)
            or (row["expires_at"] is not None and row["expires_at"] <= now)
        ):
            raise CredentialError("credential_unavailable")

    def _pause_run(self, connection: Connection, run_id: UUID) -> None:
        parameters = {"owner": self._principal.account_id, "run": run_id}
        connection.execute(
            text(
                "UPDATE runtime_private.attempts SET status='unknown', "
                "finished_at=clock_timestamp(), failure_class='missing_credential' "
                "WHERE owner_id=:owner AND run_id=:run AND status='running'"
            ),
            parameters,
        )
        connection.execute(
            text(
                "UPDATE runtime_private.steps SET status='queued' "
                "WHERE owner_id=:owner AND run_id=:run AND status='running'"
            ),
            parameters,
        )
        connection.execute(
            text(
                "UPDATE runtime_private.jobs SET status='queued', generation=generation+1, "
                "lease_owner=NULL, lease_expires_at=NULL, capacity_resume_at=NULL, "
                "available_at=clock_timestamp() WHERE owner_id=:owner AND run_id=:run "
                "AND status IN ('queued','leased')"
            ),
            parameters,
        )
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET status='waiting_credentials' "
                "WHERE owner_id=:owner AND run_id=:run "
                "AND status IN ('queued','running','waiting_capacity','waiting_credentials')"
            ),
            parameters,
        )

    def _event(
        self, connection: Connection, run_id: UUID, event_type: str, credential_id: UUID | None
    ) -> None:
        sequence: int = connection.execute(
            text(
                "SELECT coalesce(max(sequence),0)+1 FROM runtime_private.events "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": self._principal.account_id, "run": run_id},
        ).scalar_one()
        RuntimeRepository(connection, self._principal).append_event(
            run_id,
            sequence,
            event_type,
            EventActor.ACCOUNT,
            actor_id=str(self._principal.account_id),
            payload={"credential_id": None if credential_id is None else str(credential_id)},
        )


def _identity(row: Mapping[str, Any]) -> CredentialIdentity:
    return CredentialIdentity(
        row["credential_id"],
        row["owner_id"],
        CredentialProvider(row["provider"]),
        CredentialScope(row["scope"]),
        row["scope_run_id"],
    )


def _envelope(row: Mapping[str, Any]) -> CredentialEnvelope:
    return CredentialEnvelope(row["key_version"], bytes(row["nonce"]), bytes(row["ciphertext"]))


def _metadata(row: Mapping[str, Any]) -> CredentialMetadata:
    return CredentialMetadata(
        row["credential_id"],
        CredentialProvider(row["provider"]),
        CredentialScope(row["scope"]),
        row["scope_run_id"],
        row["key_version"],
        row["status"],
        row["created_at"],
        row["expires_at"],
    )
