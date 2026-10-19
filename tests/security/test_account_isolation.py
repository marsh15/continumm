"""T35 PostgreSQL proofs for owner-scoped repository onboarding and continuations."""

from __future__ import annotations

import asyncio
import os
import secrets
import subprocess
import sys
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url

from continuum_api.app import create_app
from continuum_api.credentials import CredentialCipher, CredentialProvider, CredentialScope
from continuum_api.runtime.approvals import ApprovalError, RepositoryPublicationFacts
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.provider_credentials import CredentialService
from continuum_api.runtime.repository_routes import RepositoryOnboardingService
from continuum_api.runtime.setup_continuations import (
    SetupContinuationError,
    SetupContinuationService,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t22_test"
RUNTIME_ROLE = "continuum_runtime"
ROLE_NAMES = (
    RUNTIME_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
)
SCHEMAS = ("runtime_private", "wake_private", "webhook_private", "continuum_migrations")


@contextmanager
def _database() -> Any:
    database_url = os.environ.get("CONTINUUM_T35_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("set CONTINUUM_T35_TEST_DATABASE_URL to the dedicated loopback PostgreSQL 17")
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail(f"T35 requires the dedicated loopback database {DATABASE_NAME}")
    admin = create_engine(url.set(drivername="postgresql+psycopg"), hide_parameters=True)
    runtime_password = secrets.token_urlsafe(32)
    runtime: Engine | None = None

    def clean() -> None:
        with admin.begin() as connection:
            for schema in SCHEMAS:
                connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
            for role in ROLE_NAMES:
                if connection.execute(
                    text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:role)"),
                    {"role": role},
                ).scalar_one():
                    connection.execute(
                        text(f"REVOKE CONNECT ON DATABASE {DATABASE_NAME} FROM {role}")
                    )
                    connection.execute(text(f"DROP ROLE {role}"))

    def migrate(action: str, revision: str) -> None:
        environment = {**os.environ, "CONTINUUM_MIGRATION_DATABASE_URL": database_url}
        result = subprocess.run(
            [sys.executable, "-m", "continuum_api.migrations", action, revision],
            cwd=PROJECT_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=90,
        )
        if result.returncode:
            pytest.fail(f"T35 migration {action} failed: {result.stderr[-2000:]}")

    try:
        clean()
        with admin.begin() as connection:
            version = connection.execute(
                text("SELECT current_setting('server_version_num')::integer")
            ).scalar_one()
            assert 170000 <= version < 180000
            for role in reversed(ROLE_NAMES):
                password = runtime_password if role == RUNTIME_ROLE else secrets.token_urlsafe(32)
                login = "NOLOGIN" if role == "continuum_wake_issuer" else "LOGIN"
                password_sql = "" if login == "NOLOGIN" else f"PASSWORD '{password}'"
                connection.exec_driver_sql(
                    f"CREATE ROLE {role} {login} NOSUPERUSER NOCREATEDB "
                    f"NOCREATEROLE NOINHERIT NOBYPASSRLS {password_sql}"
                )
        migrate("upgrade", "20261019_22")
        runtime = create_engine(
            url.set(
                drivername="postgresql+psycopg", username=RUNTIME_ROLE, password=runtime_password
            ),
            hide_parameters=True,
        )
        yield admin, runtime
        runtime.dispose()
        runtime = None
        migrate("downgrade", "20261019_21")
    finally:
        if runtime is not None:
            runtime.dispose()
        clean()
        admin.dispose()


def _seed_users(admin: Engine) -> tuple[Principal, Principal]:
    owners = Principal(uuid4()), Principal(uuid4())
    with admin.begin() as connection:
        for index, principal in enumerate(owners, start=1):
            connection.execute(
                text(
                    "INSERT INTO runtime_private.users (user_id, github_user_id) "
                    "VALUES (:owner, :github_id)"
                ),
                {"owner": principal.account_id, "github_id": 8_700_000 + index},
            )
    return owners


class _FakeAuthorization:
    def __init__(self) -> None:
        self.denied = False
        self.calls = 0

    async def current(self, repository_id: int, github_user_id: int) -> RepositoryPublicationFacts:
        self.calls += 1
        if self.denied:
            raise ApprovalError("repository_actor_write_access_required")
        from continuum_api.repositories.preflight import RepositoryAccessProof

        return RepositoryPublicationFacts(
            repository_id=repository_id,
            github_user_id=github_user_id,
            owner="owner",
            name=f"repo-{repository_id}",
            default_branch="main",
            base_commit_sha="a" * 40,
            access_proof=RepositoryAccessProof(repository_id, github_user_id, True, True),
        )


class _FakePreflight:
    def __init__(self) -> None:
        self.calls = 0
        self.before_return: Any = None

    async def check(self, principal: Principal, **kwargs: Any) -> Any:
        del principal
        self.calls += 1
        if self.before_return is not None:
            self.before_return()
        from continuum_api.repositories.preflight import EligibilityReceipt

        return EligibilityReceipt(
            repository_id=kwargs["repository_id"],
            commit_sha=kwargs["commit_sha"],
            tree_sha="b" * 40,
            profile_id=kwargs["profile_id"],
            profile_version=kwargs["profile_version"],
            file_count=2,
            source_bytes=64,
            manifest_blobs=(("pyproject.toml", "c" * 40),),
            package_count=1,
        )


def _service(
    runtime: Engine,
    principal: Principal,
    authorization: _FakeAuthorization | None = None,
    preflight: _FakePreflight | None = None,
) -> tuple[RepositoryOnboardingService, _FakeAuthorization, _FakePreflight]:
    auth = authorization or _FakeAuthorization()
    check = preflight or _FakePreflight()
    return (
        RepositoryOnboardingService(
            runtime,
            principal,
            auth,
            check,
            CredentialService(runtime, principal, CredentialCipher("v1", {"v1": b"k" * 32})),
        ),
        auth,
        check,
    )


def test_registration_defaults_are_owner_scoped_and_freshly_authorized() -> None:
    with _database() as (admin, runtime):
        owner, other = _seed_users(admin)
        credential = CredentialService(
            runtime, owner, CredentialCipher("v1", {"v1": b"k" * 32})
        ).save(
            CredentialProvider.OPENAI,
            SecretStr("synthetic-key-for-owner-one"),
            scope=CredentialScope.SAVED,
            saved_key_opt_in=True,
        )
        service, auth, preflight = _service(runtime, owner)
        first = asyncio.run(
            service.register(
                repository_id=77_101,
                profile_id="python-pytest",
                provider="openai",
                credential_id=credential.credential_id,
            )
        )
        second = asyncio.run(
            service.register(
                repository_id=77_102,
                profile_id="node-npm",
                provider="openai",
                credential_id=credential.credential_id,
            )
        )
        assert first["id"] != second["id"]
        assert first["credential_id"] == second["credential_id"] == credential.credential_id
        assert auth.calls == 2
        application = create_app(
            principal_resolver=lambda _request: owner,
            repository_onboarding_service_factory=lambda _request, _principal: service,
            csrf_guard=lambda _request: True,
        )
        with TestClient(application) as client:
            listed = client.get("/api/v1/repositories")
            assert listed.status_code == 200
            assert listed.json()["repositories"][0]["credential_id"] == str(
                credential.credential_id
            )
            metadata = client.get("/api/v1/credentials")
            assert metadata.status_code == 200
            assert metadata.json()["credentials"][0]["credential_id"] == str(
                credential.credential_id
            )
            assert "synthetic-key-for-owner-one" not in metadata.text
        checked = asyncio.run(service.check(first["id"], "a" * 40))
        assert checked["status"] == "eligible"
        assert checked["checked_sha"] == "a" * 40
        assert preflight.calls == 1

        racing_preflight = _FakePreflight()

        def change_defaults_during_check() -> None:
            from continuum_api.runtime.persistence import RuntimeRepository

            with runtime.begin() as connection:
                RuntimeRepository(connection, owner)
                connection.execute(
                    text(
                        "UPDATE runtime_private.repository_registrations SET "
                        "profile_id='node-npm', defaults_version=defaults_version+1, "
                        "eligibility_status='unchecked', checked_sha=NULL, "
                        "eligibility_checked_at=NULL, eligibility_error=NULL, "
                        "eligibility_receipt=NULL WHERE owner_id=:owner AND registration_id=:id"
                    ),
                    {"owner": owner.account_id, "id": first["id"]},
                )

        racing_preflight.before_return = change_defaults_during_check
        racing_service, _, _ = _service(runtime, owner, preflight=racing_preflight)
        with pytest.raises(Exception, match="repository_defaults_changed_during_check"):
            asyncio.run(racing_service.check(first["id"], "d" * 40))
        assert racing_preflight.calls == 1
        first_after_race = next(item for item in racing_service.list() if item["id"] == first["id"])
        assert first_after_race["eligibility"]["status"] == "unchecked"

        other_service, _, _ = _service(runtime, other)
        assert other_service.list() == []
        assert other_service.list_credentials() == []
        with pytest.raises(Exception, match="credential_default_invalid"):
            asyncio.run(
                other_service.register(
                    repository_id=77_101,
                    profile_id="python-pytest",
                    provider="openai",
                    credential_id=credential.credential_id,
                )
            )
        with pytest.raises(Exception, match="not_found"):
            asyncio.run(other_service.update(first["id"], _patch(provider=None)))

        owner_service, denied_auth, denied_preflight = _service(runtime, owner)
        denied_auth.denied = True
        with pytest.raises(Exception, match="repository_actor_write_access_required"):
            asyncio.run(owner_service.check(first["id"], "d" * 40))
        assert denied_preflight.calls == 0

        with runtime.begin() as connection:
            from continuum_api.runtime.persistence import RuntimeRepository

            RuntimeRepository(connection, owner)
            count = connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.repository_registrations "
                    "WHERE credential_id=:credential"
                ),
                {"credential": credential.credential_id},
            ).scalar_one()
            assert count == 2
        with runtime.begin() as connection:
            from continuum_api.runtime.persistence import RuntimeRepository

            RuntimeRepository(connection, other)
            assert (
                connection.execute(
                    text("SELECT count(*) FROM runtime_private.repository_registrations")
                ).scalar_one()
                == 0
            )


def _patch(**values: Any) -> Any:
    from continuum_api.runtime.repository_routes import _PatchBody

    return _PatchBody(**values)


def test_setup_continuation_is_fresh_owner_bound_and_claimed_once() -> None:
    with _database() as (admin, runtime):
        owner, other = _seed_users(admin)
        service, _, _ = _service(runtime, owner)
        registration = asyncio.run(
            service.register(
                repository_id=77_201,
                profile_id="python-pytest",
                provider=None,
                credential_id=None,
            )
        )
        other_service = SetupContinuationService(runtime, other)
        comment_created_at = datetime.now(UTC) + timedelta(seconds=1)
        with pytest.raises(SetupContinuationError, match="repository_unregistered"):
            other_service.capture(
                github_repository_id=77_201,
                github_comment_id=900_001,
                github_issue_number=1,
                comment_author_github_id=8_700_001,
                comment_created_at=comment_created_at,
                submission_body="/continumm run\nFix the original issue.",
            )

        continuation = SetupContinuationService(runtime, owner)
        with pytest.raises(SetupContinuationError, match="author_mismatch"):
            continuation.capture(
                github_repository_id=77_201,
                github_comment_id=900_002,
                github_issue_number=1,
                comment_author_github_id=99_999,
                comment_created_at=comment_created_at,
                submission_body="/continumm run\nFix the original issue.",
            )
        with admin.begin() as connection:
            registered_at = connection.execute(
                text(
                    "SELECT created_at FROM runtime_private.repository_registrations "
                    "WHERE registration_id=:registration AND owner_id=:owner"
                ),
                {"registration": registration["id"], "owner": owner.account_id},
            ).scalar_one()
        with pytest.raises(SetupContinuationError, match="stale"):
            continuation.capture(
                github_repository_id=77_201,
                github_comment_id=900_003,
                github_issue_number=1,
                comment_author_github_id=8_700_001,
                comment_created_at=registered_at - timedelta(seconds=1),
                submission_body="/continumm run\nFix the original issue.",
            )

        captured = continuation.capture(
            github_repository_id=77_201,
            github_comment_id=900_004,
            github_issue_number=57,
            comment_author_github_id=8_700_001,
            comment_created_at=comment_created_at,
            submission_body="/continumm run\nFix the original issue.",
        )
        with pytest.raises(SetupContinuationError, match="setup_incomplete"):
            continuation.claim(captured.continuation_id)
        asyncio.run(service.update(registration["id"], _patch(provider="openai")))
        claimed = continuation.claim(captured.continuation_id)
        assert claimed.github_comment_id == captured.github_comment_id
        assert claimed.github_issue_number == 57
        assert claimed.submission_body == captured.submission_body
        assert claimed.submission_sha256 == captured.submission_sha256
        with pytest.raises(SetupContinuationError, match="already_claimed"):
            continuation.claim(captured.continuation_id)

        expiring = continuation.capture(
            github_repository_id=77_201,
            github_comment_id=900_005,
            github_issue_number=58,
            comment_author_github_id=8_700_001,
            comment_created_at=datetime.now(UTC) + timedelta(seconds=1),
            submission_body="/continumm run\nAnother original submission.",
        )
        with admin.begin() as connection:
            connection.exec_driver_sql(
                "ALTER TABLE runtime_private.setup_continuations "
                "DISABLE TRIGGER setup_continuation_update_guard"
            )
            connection.exec_driver_sql(
                "ALTER TABLE runtime_private.setup_continuations "
                "DROP CONSTRAINT ck_setup_continuation_expiry"
            )
            connection.execute(
                text(
                    "WITH clock AS (SELECT clock_timestamp() AS value) "
                    "UPDATE runtime_private.setup_continuations AS c SET "
                    "comment_created_at=clock.value-interval '73 hours', "
                    "expires_at=clock.value-interval '1 hour' FROM clock "
                    "WHERE c.continuation_id=:id AND c.owner_id=:owner"
                ),
                {"id": expiring.continuation_id, "owner": owner.account_id},
            )
            connection.exec_driver_sql(
                "ALTER TABLE runtime_private.setup_continuations "
                "ADD CONSTRAINT ck_setup_continuation_expiry "
                "CHECK (expires_at = comment_created_at + interval '72 hours')"
            )
            connection.exec_driver_sql(
                "ALTER TABLE runtime_private.setup_continuations "
                "ENABLE TRIGGER setup_continuation_update_guard"
            )
        assert service.list()
        assert continuation.sweep_expired(limit=1) == 0
        with admin.connect() as connection:
            body = connection.execute(
                text(
                    "SELECT submission_body FROM runtime_private.setup_continuations "
                    "WHERE owner_id=:owner AND continuation_id=:id"
                ),
                {"owner": owner.account_id, "id": expiring.continuation_id},
            ).scalar_one()
            assert body is None


def test_repository_http_routes_enforce_csrf_and_bounded_strict_json() -> None:
    principal = Principal(uuid4())

    class EmptyService:
        def list(self) -> list[dict[str, Any]]:
            return []

        def list_credentials(self) -> list[dict[str, Any]]:
            return []

    fake_service = EmptyService()
    application = create_app(
        principal_resolver=lambda _request: principal,
        repository_onboarding_service_factory=lambda _request, _principal: fake_service,
        csrf_guard=lambda _request: True,
    )
    with TestClient(application) as client:
        assert client.get("/api/v1/repositories").json() == {"repositories": []}
        assert client.get("/api/v1/credentials").json() == {"credentials": []}
        no_csrf = client.post(
            "/api/v1/repositories",
            json={"github_repository_id": 123, "profile_id": "python-pytest"},
            headers={"origin": "https://app.example"},
        )
        assert no_csrf.status_code == 403
        oversized = client.post(
            "/api/v1/repositories",
            content=b"{" + b"x" * 8192,
            headers={
                "origin": "https://app.example",
                "x-csrf-token": "csrf",
                "content-type": "application/json",
            },
        )
        assert oversized.status_code == 413
        invalid = client.post(
            "/api/v1/repositories",
            json={
                "github_repository_id": 123,
                "profile_id": "python-pytest",
                "unsafe_url": "https://evil",
            },
            headers={"origin": "https://app.example", "x-csrf-token": "csrf"},
        )
        assert invalid.status_code == 422
