"""Synthetic-only credential encryption and lifecycle boundary tests."""

from __future__ import annotations

import base64
import json
import os
import secrets
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from continuum_api.credentials import (
    CredentialCipher,
    CredentialError,
    CredentialIdentity,
    CredentialProvider,
    CredentialScope,
)
from continuum_api.logging_config import redact_secrets
from continuum_api.runtime.cancellation import CancellationService
from continuum_api.runtime.leases import LeaseLost, QueueService
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RuntimeRepository
from continuum_api.runtime.provider_credentials import CredentialService
from continuum_api.runtime.scheduler import FailureClass, RuntimeRecoveryService
from continuum_api.settings import Settings


def _identity() -> CredentialIdentity:
    return CredentialIdentity(
        uuid4(), uuid4(), CredentialProvider.OPENAI, CredentialScope.RUN, uuid4()
    )


def test_authenticated_encryption_binds_identity_and_rejects_tampering() -> None:
    cipher = CredentialCipher("v1", {"v1": os.urandom(32)})
    identity = _identity()
    secret = SecretStr("synthetic-provider-secret-not-a-real-key")
    envelope = cipher.encrypt(identity, secret)
    second = cipher.encrypt(identity, secret)
    assert envelope.nonce != second.nonce
    assert secret.get_secret_value().encode() not in envelope.ciphertext
    assert cipher.decrypt(identity, envelope).get_secret_value() == secret.get_secret_value()
    for changed in (
        replace(identity, owner_id=uuid4()),
        replace(identity, credential_id=uuid4()),
        replace(identity, provider=CredentialProvider.ANTHROPIC),
        replace(identity, run_id=uuid4()),
        replace(identity, scope=CredentialScope.SAVED, run_id=None),
    ):
        with pytest.raises(CredentialError, match="credential_authentication_failed"):
            cipher.decrypt(changed, envelope)
    for changed in (
        replace(envelope, nonce=os.urandom(12)),
        replace(
            envelope, ciphertext=envelope.ciphertext[:-1] + bytes([envelope.ciphertext[-1] ^ 1])
        ),
    ):
        with pytest.raises(CredentialError, match="credential_authentication_failed"):
            cipher.decrypt(identity, changed)
    assert secret.get_secret_value() not in repr(cipher) + repr(envelope) + repr(secret)


def test_rotation_reencrypts_with_a_new_version_and_fails_without_old_key() -> None:
    original_key, replacement_key = os.urandom(32), os.urandom(32)
    identity = _identity()
    original = CredentialCipher("v1", {"v1": original_key})
    envelope = original.encrypt(identity, SecretStr("synthetic-rotation-key"))
    rotating = CredentialCipher("v2", {"v1": original_key, "v2": replacement_key})
    rotated = rotating.rotate(identity, envelope)
    assert rotated.key_version == "v2" and rotated.nonce != envelope.nonce
    current = CredentialCipher("v2", {"v2": replacement_key})
    assert current.decrypt(identity, rotated).get_secret_value() == "synthetic-rotation-key"
    with pytest.raises(CredentialError, match="credential_key_version_unavailable"):
        current.decrypt(identity, envelope)


def test_settings_keyring_is_strict_versioned_and_redacted() -> None:
    original_key, replacement_key = os.urandom(32), os.urandom(32)
    settings = Settings(
        _env_file=None,
        encryption_key=SecretStr(base64.urlsafe_b64encode(replacement_key).decode()),
        encryption_key_version="v2",
        encryption_previous_keys=SecretStr(
            json.dumps({"v1": base64.urlsafe_b64encode(original_key).decode()})
        ),
    )
    assert CredentialCipher.from_settings(settings).active_version == "v2"
    assert base64.urlsafe_b64encode(original_key).decode() not in repr(settings)
    with pytest.raises(CredentialError, match="credential_keyring_invalid"):
        CredentialCipher.from_settings(
            Settings(_env_file=None, encryption_key=SecretStr("invalid"))
        )


@pytest.mark.parametrize("value", ["", "has space", "has\nnewline", "\0", "x" * 4097, "é"])
def test_invalid_secret_values_fail_without_echoing_submitted_material(value: str) -> None:
    cipher = CredentialCipher("v1", {"v1": os.urandom(32)})
    with pytest.raises(CredentialError) as failure:
        cipher.encrypt(_identity(), SecretStr(value))
    assert str(failure.value) == "credential_secret_invalid"


@contextmanager
def _credential_database() -> Any:
    database_url = os.environ.get("CONTINUUM_T23_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip(
            "set CONTINUUM_T23_TEST_DATABASE_URL to a dedicated loopback PostgreSQL 17 instance"
        )
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != "continuum_t23_test":
        pytest.fail("T23 requires the disposable loopback database continuum_t23_test")
    admin = create_engine(url.set(drivername="postgresql+psycopg"), hide_parameters=True)
    runtime_password = secrets.token_urlsafe(32)
    runtime = None
    roles = (
        "continuum_runtime",
        "continuum_wake_receiver",
        "continuum_wake",
        "continuum_wake_issuer",
        "continuum_ingress",
    )
    schemas = ("runtime_private", "wake_private", "webhook_private", "continuum_migrations")

    def clean() -> None:
        with admin.begin() as connection:
            for schema in schemas:
                connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
            for role in roles:
                if connection.execute(
                    text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role)"),
                    {"role": role},
                ).scalar_one():
                    connection.execute(
                        text(f"REVOKE CONNECT ON DATABASE continuum_t23_test FROM {role}")
                    )
                    connection.execute(text(f"DROP ROLE {role}"))

    def migrate(action: str, revision: str) -> None:
        environment = {**os.environ, "CONTINUUM_MIGRATION_DATABASE_URL": database_url}
        result = subprocess.run(
            [sys.executable, "-m", "continuum_api.migrations", action, revision],
            cwd=Path(__file__).resolve().parents[2],
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode:
            pytest.fail(f"T23 migration {action} failed: {redact_secrets(result.stderr[-1800:])}")

    try:
        with admin.connect() as connection:
            version = connection.execute(
                text("SELECT current_setting('server_version_num')::integer")
            ).scalar_one()
            assert 170000 <= version < 180000
        clean()
        with admin.begin() as connection:
            for role in reversed(roles):
                password = (
                    runtime_password if role == "continuum_runtime" else secrets.token_urlsafe(32)
                )
                login = "NOLOGIN" if role == "continuum_wake_issuer" else "LOGIN"
                password_sql = "" if login == "NOLOGIN" else f"PASSWORD '{password}'"
                connection.exec_driver_sql(
                    f"CREATE ROLE {role} {login} NOSUPERUSER NOCREATEDB "
                    f"NOCREATEROLE NOINHERIT NOBYPASSRLS {password_sql}"
                )
        migrate("upgrade", "20261015_12")
        runtime = create_engine(
            url.set(
                drivername="postgresql+psycopg",
                username="continuum_runtime",
                password=runtime_password,
                query={"sslmode": "disable"},
            ),
            hide_parameters=True,
            connect_args={
                "connect_timeout": 5,
                "options": "-c statement_timeout=8000 -c lock_timeout=5000",
            },
        )
        yield admin, runtime
        migrate("downgrade", "20261013_11")
        with admin.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT to_regclass('runtime_private.provider_credentials')")
                ).scalar_one()
                is None
            )
        migrate("downgrade", "base")
    finally:
        if runtime is not None:
            runtime.dispose()
        clean()
        admin.dispose()


def _seed_credential_run(
    admin: Any, runtime: Any, owner: Principal | None = None
) -> tuple[Principal, Any]:
    principal = owner or Principal(uuid4())
    if owner is None:
        with admin.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO runtime_private.users (user_id, github_user_id) "
                    "VALUES (:owner,:github)"
                ),
                {"owner": principal.account_id, "github": principal.account_id.int % (2**62) + 1},
            )
    with runtime.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run()
        step_id = repository.create_step(run_id, f"credential:{run_id}", "agent_turn")
        repository.create_job(run_id, step_id, replay_policy=ReplayPolicy.REPLAY_SAFE)
    lease = QueueService(runtime, principal).claim_one(f"credential-worker-{run_id}")
    assert lease is not None and lease.run_id == run_id
    return principal, lease


def test_postgres_credential_lifecycle_is_owner_bound_revocable_and_erased(caplog: Any) -> None:
    with _credential_database() as (admin, runtime):
        principal, lease = _seed_credential_run(admin, runtime)
        provider = CredentialProvider.OPENAI
        v1, v2 = os.urandom(32), os.urandom(32)
        cipher = CredentialCipher("v1", {"v1": v1})
        service = CredentialService(runtime, principal, cipher)
        plaintext = "synthetic-only-provider-key-never-logged"
        with pytest.raises(CredentialError, match="credential_saved_opt_in_required"):
            service.save(provider, SecretStr(plaintext), scope=CredentialScope.SAVED)
        saved = service.save(
            provider, SecretStr(plaintext), scope=CredentialScope.SAVED, saved_key_opt_in=True
        )
        service.bind_run(lease.run_id, provider, saved.credential_id)
        service.bind_run(lease.run_id, provider, saved.credential_id)
        request_id = uuid4()
        grant = service.authorize_dispatch(lease, provider, request_id)
        assert grant.secret.get_secret_value() == plaintext
        assert plaintext not in repr(grant) + repr(service.list_metadata())
        with pytest.raises(TypeError):
            json.dumps({"model_context": grant})
        with pytest.raises(CredentialError, match="credential_dispatch_already_authorized"):
            service.authorize_dispatch(lease, provider, request_id)
        other, other_lease = _seed_credential_run(admin, runtime)
        foreign = CredentialService(runtime, other, cipher)
        assert foreign.list_metadata() == ()
        with pytest.raises(CredentialError, match="credential_not_found"):
            foreign.revoke(saved.credential_id)
        with pytest.raises(CredentialError, match="credential_not_found"):
            foreign.bind_run(other_lease.run_id, provider, saved.credential_id)
        with runtime.begin() as connection:
            RuntimeRepository(connection, other)
            assert (
                connection.execute(
                    text("SELECT count(*) FROM runtime_private.provider_credentials")
                ).scalar_one()
                == 0
            )
        with pytest.raises(DBAPIError) as denied:
            with runtime.begin() as connection:
                RuntimeRepository(connection, other)
                connection.execute(
                    text(
                        "UPDATE runtime_private.provider_credentials SET status='revoked' "
                        "WHERE owner_id=:owner"
                    ),
                    {"owner": principal.account_id},
                )
                # SELECT/UPDATE isolation hides rows; explicitly test INSERT WITH CHECK too.
                connection.execute(
                    text(
                        "INSERT INTO runtime_private.run_credentials "
                        "(owner_id,run_id,provider,credential_id) "
                        "VALUES (:owner,:run,'openai',:credential)"
                    ),
                    {
                        "owner": principal.account_id,
                        "run": lease.run_id,
                        "credential": saved.credential_id,
                    },
                )
        assert getattr(denied.value.orig, "sqlstate", None) == "42501"
        rotating = CredentialService(
            runtime, principal, CredentialCipher("v2", {"v1": v1, "v2": v2})
        )
        assert rotating.rotate(saved.credential_id).key_version == "v2"
        current = CredentialService(runtime, principal, CredentialCipher("v2", {"v2": v2}))
        assert (
            current.authorize_dispatch(lease, provider, uuid4()).secret.get_secret_value()
            == plaintext
        )
        assert CredentialService(runtime, principal, None).revoke(saved.credential_id)
        assert not service.revoke(saved.credential_id)
        with pytest.raises(LeaseLost):
            current.authorize_dispatch(lease, provider, uuid4())
        with runtime.begin() as connection:
            RuntimeRepository(connection, principal)
            state = connection.execute(
                text("SELECT status FROM runtime_private.runs WHERE run_id=:run"),
                {"run": lease.run_id},
            ).scalar_one()
            assert state == "waiting_credentials"
            row = connection.execute(
                text(
                    "SELECT ciphertext,nonce,status FROM runtime_private.provider_credentials "
                    "WHERE credential_id=:id"
                ),
                {"id": saved.credential_id},
            ).one()
            assert row.ciphertext is None and row.nonce is None and row.status == "revoked"
        run_secret = current.save(
            provider,
            SecretStr("synthetic-run-replacement"),
            scope=CredentialScope.RUN,
            run_id=lease.run_id,
        )
        current.bind_run(lease.run_id, provider, run_secret.credential_id)
        resumed = QueueService(runtime, principal).claim_one("credential-replacement-worker")
        assert resumed is not None and resumed.generation > lease.generation
        assert (
            current.authorize_dispatch(resumed, provider, uuid4()).secret.get_secret_value()
            == "synthetic-run-replacement"
        )
        with admin.connect() as connection:
            backup_material = connection.execute(
                text(
                    "SELECT key_version, nonce, ciphertext "
                    "FROM runtime_private.provider_credentials "
                    "WHERE credential_id=:id"
                ),
                {"id": run_secret.credential_id},
            ).one()
        with pytest.raises(CredentialError, match="credential_provider_pinned"):
            anthropic = current.save(
                CredentialProvider.ANTHROPIC,
                SecretStr("synthetic-other-provider"),
                scope=CredentialScope.SAVED,
                saved_key_opt_in=True,
            )
            current.bind_run(resumed.run_id, CredentialProvider.ANTHROPIC, anthropic.credential_id)
        CancellationService(runtime, principal).cancel(resumed.run_id)
        with runtime.begin() as connection:
            RuntimeRepository(connection, principal)
            erased = connection.execute(
                text(
                    "SELECT status,ciphertext,nonce FROM runtime_private.provider_credentials "
                    "WHERE credential_id=:id"
                ),
                {"id": run_secret.credential_id},
            ).one()
            assert erased.status == "deleted" and erased.ciphertext is None and erased.nonce is None
            events = (
                connection.execute(
                    text("SELECT payload::text FROM runtime_private.events WHERE run_id=:run"),
                    {"run": lease.run_id},
                )
                .scalars()
                .all()
            )
            assert all(
                plaintext not in event and "synthetic-run-replacement" not in event
                for event in events
            )
        assert current.sweep_terminal_material() == 0
        # A legacy restored ciphertext must neither bind to a different run nor
        # survive the idempotent terminal-material repair sweep.
        with admin.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.provider_credentials SET status='active', "
                    "key_version=:version,nonce=:nonce,ciphertext=:ciphertext,ended_at=NULL "
                    "WHERE credential_id=:id"
                ),
                {
                    "id": run_secret.credential_id,
                    "version": backup_material.key_version,
                    "nonce": backup_material.nonce,
                    "ciphertext": backup_material.ciphertext,
                },
            )
        _owner, later = _seed_credential_run(admin, runtime, principal)
        with pytest.raises(CredentialError, match="credential_unavailable"):
            current.bind_run(later.run_id, provider, run_secret.credential_id)
        assert CredentialService(runtime, principal, None).sweep_terminal_material() == 1
        assert current.sweep_terminal_material() == 0

        # Expiry is a dispatch denial even when a repair sweep has not run yet.
        expiring = current.save(
            provider,
            SecretStr("synthetic-expiring-key"),
            scope=CredentialScope.RUN,
            run_id=later.run_id,
        )
        current.bind_run(later.run_id, provider, expiring.credential_id)
        with admin.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.provider_credentials "
                    "SET expires_at=clock_timestamp()-interval '1 second' "
                    "WHERE credential_id=:id"
                ),
                {"id": expiring.credential_id},
            )
        with pytest.raises(CredentialError, match="credential_unavailable"):
            current.authorize_dispatch(later, provider, uuid4())
        assert current.sweep_terminal_material() == 1

        for terminal in ("succeeded", "failed", "cancelled", "expired"):
            terminal_owner, terminal_lease = _seed_credential_run(admin, runtime)
            terminal_service = CredentialService(runtime, terminal_owner, cipher)
            terminal_key = terminal_service.save(
                provider,
                SecretStr("synthetic-terminal-key"),
                scope=CredentialScope.RUN,
                run_id=terminal_lease.run_id,
            )
            if terminal == "failed":
                RuntimeRecoveryService(runtime, terminal_owner).fail_attempt(
                    lease=terminal_lease, failure_class=FailureClass.INVALID_INPUT
                )
            elif terminal == "cancelled":
                CancellationService(runtime, terminal_owner).cancel(terminal_lease.run_id)
            elif terminal == "expired":
                with admin.begin() as connection:
                    connection.execute(
                        text(
                            "UPDATE runtime_private.runs "
                            "SET created_at=clock_timestamp()-interval '2 seconds', "
                            "deadline_at=clock_timestamp()-interval '1 second' "
                            "WHERE run_id=:run"
                        ),
                        {"run": terminal_lease.run_id},
                    )
                assert CancellationService(runtime, terminal_owner).expire_due(limit=1) == 1
            else:
                # Publication success is a later workflow; exercise the DB hook
                # directly without bypassing its future application approval gate.
                with admin.begin() as connection:
                    connection.execute(
                        text(
                            "UPDATE runtime_private.runs SET status='succeeded' WHERE run_id=:run"
                        ),
                        {"run": terminal_lease.run_id},
                    )
            with runtime.begin() as connection:
                RuntimeRepository(connection, terminal_owner)
                terminal_row = connection.execute(
                    text(
                        "SELECT status,ciphertext,nonce FROM runtime_private.provider_credentials "
                        "WHERE credential_id=:id"
                    ),
                    {"id": terminal_key.credential_id},
                ).one()
                assert terminal_row.status == "deleted"
                assert terminal_row.ciphertext is None and terminal_row.nonce is None

        # Time is checked after a credential-row lock wait, not only when the
        # activity originally acquired its execution lease.
        expiry_owner, expiry_lease = _seed_credential_run(admin, runtime)
        expiry_service = CredentialService(runtime, expiry_owner, cipher)
        expiry_key = expiry_service.save(
            provider,
            SecretStr("synthetic-lease-key"),
            scope=CredentialScope.RUN,
            run_id=expiry_lease.run_id,
        )
        expiry_service.bind_run(expiry_lease.run_id, provider, expiry_key.credential_id)
        with admin.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.jobs "
                    "SET lease_expires_at=clock_timestamp()+interval '2 seconds' "
                    "WHERE job_id=:job"
                ),
                {"job": expiry_lease.job_id},
            )
        request_id = uuid4()
        with ThreadPoolExecutor(max_workers=1) as executor:
            with admin.begin() as blocker:
                blocker.execute(
                    text(
                        "SELECT credential_id FROM runtime_private.provider_credentials "
                        "WHERE credential_id=:id FOR UPDATE"
                    ),
                    {"id": expiry_key.credential_id},
                )
                future = executor.submit(
                    expiry_service.authorize_dispatch, expiry_lease, provider, request_id
                )
                deadline = time.monotonic() + 5
                while True:
                    with admin.connect() as observer:
                        waiting = observer.execute(
                            text(
                                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                                "WHERE usename='continuum_runtime' "
                                "AND wait_event_type='Lock')"
                            )
                        ).scalar_one()
                    if waiting:
                        break
                    assert time.monotonic() < deadline
                    time.sleep(0.01)
                with admin.connect() as observer:
                    observer.execute(text("SELECT pg_sleep(2.1)"))
            with pytest.raises(LeaseLost):
                future.result(timeout=5)
        with admin.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.credential_dispatches "
                        "WHERE request_id=:request"
                    ),
                    {"request": request_id},
                ).scalar_one()
                == 0
            )
        assert plaintext not in caplog.text and "synthetic-run-replacement" not in caplog.text
