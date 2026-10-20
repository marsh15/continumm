"""PostgreSQL proof for global public admission, idempotency, and safe replay."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError

from continuum_api.auth.github import GitHubIssueSnapshot
from continuum_api.runtime.admission import (
    AdmissionError,
    AdmissionService,
    FreeTextSource,
    IssueSource,
)
from continuum_api.runtime.approvals import RepositoryPublicationFacts
from continuum_api.runtime.cancellation import CancellationService
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.provider_credentials import CredentialService

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_NAME = "continuum_t36_test"
RUNTIME_ROLE = "continuum_runtime"
ADMISSION_ROLE = "continuum_admission"
ROLE_NAMES = (
    RUNTIME_ROLE,
    ADMISSION_ROLE,
    "continuum_wake_receiver",
    "continuum_wake",
    "continuum_wake_issuer",
    "continuum_ingress",
    "continuum_recovery",
    "continuum_operator",
)


class SyntheticGitHub:
    def __init__(self) -> None:
        self.calls = 0

    async def current(self, repository_id: int, github_user_id: int) -> RepositoryPublicationFacts:
        self.calls += 1
        return self._facts(repository_id, github_user_id)

    async def current_issue(
        self, repository_id: int, github_user_id: int, issue_number: int
    ) -> tuple[RepositoryPublicationFacts, GitHubIssueSnapshot]:
        self.calls += 1
        facts = self._facts(repository_id, github_user_id)
        title = f"Issue {issue_number}"
        body = "A bounded synthetic issue body."
        digest = hashlib.sha256(
            json.dumps(
                {
                    "id": repository_id * 10 + issue_number,
                    "number": issue_number,
                    "title": title,
                    "body": body,
                    "state": "open",
                    "updated_at": "2026-10-19T12:00:00+00:00",
                },
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return facts, GitHubIssueSnapshot(
            repository_id=repository_id,
            github_user_id=github_user_id,
            issue_id=repository_id * 10 + issue_number,
            issue_number=issue_number,
            title=title,
            body=body,
            state="open",
            updated_at="2026-10-19T12:00:00+00:00",
            sha256=digest,
        )

    @staticmethod
    def _facts(repository_id: int, github_user_id: int) -> RepositoryPublicationFacts:
        from continuum_api.repositories.preflight import RepositoryAccessProof

        return RepositoryPublicationFacts(
            repository_id=repository_id,
            github_user_id=github_user_id,
            owner="owner",
            name=f"repo-{repository_id}",
            default_branch="main",
            base_commit_sha="a" * 40,
            access_proof=RepositoryAccessProof(
                repository_id=repository_id,
                github_user_id=github_user_id,
                actor_can_write=True,
                app_can_access=True,
            ),
        )


def test_admission_queue_replay_continuation_and_rls() -> None:
    database_url = os.environ.get("CONTINUUM_T36_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip(
            "set CONTINUUM_T36_TEST_DATABASE_URL to a disposable loopback PostgreSQL 17 database"
        )
    url = make_url(database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or url.database != DATABASE_NAME:
        pytest.fail("T36 integration requires loopback PostgreSQL named continuum_t36_test")

    admin_engine = create_engine(
        url.set(drivername="postgresql+psycopg"), hide_parameters=True, pool_size=8, max_overflow=0
    )
    runtime_password = secrets.token_urlsafe(32)
    runtime_engine: Engine | None = None
    migration_applied = False
    try:
        _reset_database(admin_engine, runtime_password)
        _assert_postgres_17(admin_engine)
        _run_migration("upgrade", "head", database_url)
        migration_applied = True
        runtime_engine = create_engine(
            _role_url(url, RUNTIME_ROLE, runtime_password),
            hide_parameters=True,
            pool_size=8,
            max_overflow=0,
        )
        with admin_engine.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT has_table_privilege('continuum_runtime', "
                    "'runtime_private.admission_idempotency', 'SELECT')"
                )
            ).scalar_one()
        owners = _seed_owners(admin_engine, 11)
        github = SyntheticGitHub()
        services = {
            item["owner"]: AdmissionService(
                runtime_engine,
                Principal(item["owner"]),
                github,
                CredentialService(runtime_engine, Principal(item["owner"]), None),
                enabled=True,
            )
            for item in owners
        }

        def admit(item: dict[str, object], request_id: UUID | None = None):
            source = IssueSource(
                kind="issue",
                registration_id=item["registration_id"],
                issue_number=7,
                request_id=request_id or uuid4(),
                task="Fix the bounded synthetic issue.",
            )
            return asyncio.run(services[item["owner"]].admit(source, secrets.token_urlsafe(24)))

        with ThreadPoolExecutor(max_workers=11) as pool:
            futures = [(item, pool.submit(admit, item)) for item in owners]
            admitted: list[tuple[dict[str, object], object]] = []
            rejected: list[tuple[dict[str, object], AdmissionError]] = []
            for item, future in futures:
                try:
                    admitted.append((item, future.result(timeout=30)))
                except AdmissionError as error:
                    rejected.append((item, error))
        assert len(admitted) == 10, [error.code for _, error in rejected]
        assert len(rejected) == 1 and rejected[0][1].code == "global_queue_full"
        assert all(result.status == "waiting_credentials" for _, result in admitted)
        assert all(
            result.issue_id is not None and result.issue_sha is not None for _, result in admitted
        )
        with admin_engine.connect() as connection:
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.admission_queue_seats "
                        "WHERE owner_id IS NOT NULL"
                    )
                ).scalar_one()
                == 10
            )
            assert (
                connection.execute(
                    text("SELECT count(*) FROM runtime_private.sandbox_allocations")
                ).scalar_one()
                == 0
            )
            assert (
                connection.execute(text("SELECT count(*) FROM runtime_private.runs")).scalar_one()
                == 10
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM runtime_private.runs WHERE model_id IS NOT NULL "
                        "OR model_task_context IS NOT NULL "
                        "OR model_tool_schema_sha256 IS NOT NULL"
                    )
                ).scalar_one()
                == 0
            )
            assert (
                connection.execute(
                    text(
                        "SELECT bool_and(deadline_at=created_at+interval '7 days') "
                        "FROM runtime_private.runs"
                    )
                ).scalar_one()
                is True
            )

        _assert_no_direct_queue_access(runtime_engine, owners[0]["owner"])
        _assert_active_issue_adoption(services, admitted[0][0], admitted[0][1], github)
        _assert_one_active_run_per_account(services, admin_engine, admitted[0][0])
        _assert_continuation_is_atomic(
            admin_engine, runtime_engine, services, owners, admitted, rejected[0][0]
        )
        _assert_idempotency_and_durable_request_replay(admin_engine, services, rejected[0][0])
        _assert_free_text_uses_request_identity(admin_engine, services, rejected[0][0])

        runtime_engine.dispose()
        runtime_engine = None
        _run_migration("downgrade", "base", database_url)
        migration_applied = False
    finally:
        if runtime_engine is not None:
            runtime_engine.dispose()
        try:
            if migration_applied:
                _run_migration("downgrade", "base", database_url)
        finally:
            _cleanup_database(admin_engine)
            admin_engine.dispose()


def _assert_active_issue_adoption(services, owner, original, github: SyntheticGitHub) -> None:
    before = github.calls
    source = IssueSource(
        kind="issue",
        registration_id=owner["registration_id"],
        issue_number=7,
        request_id=uuid4(),
        task="A changed follow-up task cannot rewrite the admitted snapshot.",
    )
    result = asyncio.run(services[owner["owner"]].admit(source, secrets.token_urlsafe(24)))
    assert result.replayed is True
    assert result.run_id == original.run_id
    assert result.source_sha == original.source_sha
    assert result.model_id == original.model_id
    assert result.credential_id == original.credential_id
    assert result.issue_id is not None
    assert github.calls == before + 1


def _assert_one_active_run_per_account(services, admin_engine, owner) -> None:
    source = IssueSource(
        kind="issue",
        registration_id=owner["registration_id"],
        issue_number=8,
        request_id=uuid4(),
        task="A second active issue must be rejected for this account.",
    )
    with pytest.raises(AdmissionError, match="account_active_run_exists"):
        asyncio.run(services[owner["owner"]].admit(source, secrets.token_urlsafe(24)))
    with admin_engine.connect() as connection:
        active_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.admission_queue_seats WHERE owner_id=:owner"
            ),
            {"owner": owner["owner"]},
        ).scalar_one()
    assert active_count == 1


def _assert_idempotency_and_durable_request_replay(admin_engine, services, item) -> None:
    request_id = uuid4()
    key = secrets.token_urlsafe(24)
    source = IssueSource(
        kind="issue",
        registration_id=item["registration_id"],
        issue_number=19,
        request_id=request_id,
        task="Keep this exact request identity.",
    )
    first = asyncio.run(services[item["owner"]].admit(source, key))
    calls_before = services[item["owner"]]._facts_provider.calls
    replay = asyncio.run(services[item["owner"]].admit(source, key))
    assert replay.run_id == first.run_id and replay.replayed is True
    assert services[item["owner"]]._facts_provider.calls == calls_before
    with pytest.raises(AdmissionError, match="idempotency_key_conflict"):
        asyncio.run(
            services[item["owner"]].admit(
                IssueSource(
                    kind="issue",
                    registration_id=item["registration_id"],
                    issue_number=19,
                    request_id=request_id,
                    task="Changed input under the same key.",
                ),
                key,
            )
        )
    CancellationService(services[item["owner"]]._engine, Principal(item["owner"])).cancel(
        first.run_id
    )
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "WITH now AS (SELECT clock_timestamp() AS t) "
                "UPDATE runtime_private.admission_idempotency "
                "SET created_at=now.t-interval '25 hours', expires_at=now.t-interval '1 hour' "
                "FROM now WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": item["owner"], "run": first.run_id},
        )
    durable = asyncio.run(services[item["owner"]].admit(source, secrets.token_urlsafe(24)))
    assert durable.run_id == first.run_id and durable.status == "cancelled"
    assert durable.replayed is True


def _assert_continuation_is_atomic(
    admin_engine, runtime_engine, services, owners, admitted, rejected_owner
) -> None:
    comment_id = 991_001
    body = "/continumm run\n\nPlease work on the issue carefully."
    comment_created_at = datetime.now(UTC)
    continuation_id = uuid4()
    body_digest = hashlib.sha256((7).to_bytes(8, "big") + b"\0" + body.encode("utf-8")).digest()
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.setup_continuations "
                "(continuation_id, owner_id, github_user_id, github_repository_id, "
                "github_comment_id, "
                "github_issue_number, comment_created_at, submission_body, "
                "submission_sha256, expires_at) "
                "VALUES (:id, :owner, :user, :repo, :comment, 7, :created, :body, :digest, "
                ":created+interval '72 hours')"
            ),
            {
                "id": continuation_id,
                "owner": rejected_owner["owner"],
                "user": rejected_owner["github_user_id"],
                "repo": rejected_owner["repository_id"],
                "comment": comment_id,
                "created": comment_created_at,
                "body": body,
                "digest": body_digest,
            },
        )
    with pytest.raises(AdmissionError, match="global_queue_full"):
        asyncio.run(
            services[rejected_owner["owner"]].admit_continuation(
                continuation_id,
                task="Please work on the issue carefully.",
                idempotency_key=secrets.token_urlsafe(24),
            )
        )
    with admin_engine.connect() as connection:
        assert (
            connection.execute(
                text(
                    "SELECT claimed_at IS NULL FROM runtime_private.setup_continuations "
                    "WHERE owner_id=:owner AND continuation_id=:id"
                ),
                {"owner": rejected_owner["owner"], "id": continuation_id},
            ).scalar_one()
            is True
        )
    first_admission = admitted[0][1]
    first_owner = next(
        item for item in owners if _has_run(runtime_engine, item["owner"], first_admission.run_id)
    )
    CancellationService(runtime_engine, Principal(first_owner["owner"])).cancel(
        first_admission.run_id
    )
    resumed = asyncio.run(
        services[rejected_owner["owner"]].admit_continuation(
            continuation_id,
            task="Please work on the issue carefully.",
            idempotency_key=secrets.token_urlsafe(24),
        )
    )
    assert resumed.status == "waiting_credentials"
    with admin_engine.connect() as connection:
        assert (
            connection.execute(
                text(
                    "SELECT claimed_at IS NOT NULL FROM runtime_private.setup_continuations "
                    "WHERE owner_id=:owner AND continuation_id=:id"
                ),
                {"owner": rejected_owner["owner"], "id": continuation_id},
            ).scalar_one()
            is True
        )
        assert (
            connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.admission_submissions "
                    "WHERE owner_id=:owner AND run_id=:run AND continuation_id=:continuation"
                ),
                {
                    "owner": rejected_owner["owner"],
                    "run": resumed.run_id,
                    "continuation": continuation_id,
                },
            ).scalar_one()
            == 1
        )
    replay = asyncio.run(
        services[rejected_owner["owner"]].admit_continuation(
            continuation_id,
            task="Please work on the issue carefully.",
            idempotency_key=secrets.token_urlsafe(24),
        )
    )
    assert replay.run_id == resumed.run_id and replay.replayed is True
    CancellationService(runtime_engine, Principal(rejected_owner["owner"])).cancel(resumed.run_id)
    terminal_replay = asyncio.run(
        services[rejected_owner["owner"]].admit_continuation(
            continuation_id,
            task="Please work on the issue carefully.",
            idempotency_key=secrets.token_urlsafe(24),
        )
    )
    assert terminal_replay.run_id == resumed.run_id
    assert terminal_replay.status == "cancelled" and terminal_replay.replayed is True


def _assert_free_text_uses_request_identity(admin_engine, services, owner) -> None:
    source_a = FreeTextSource(
        kind="free_text",
        registration_id=owner["registration_id"],
        request_id=uuid4(),
        task="The same wording may represent a new request.",
    )
    first = asyncio.run(services[owner["owner"]].admit(source_a, secrets.token_urlsafe(24)))
    CancellationService(services[owner["owner"]]._engine, Principal(owner["owner"])).cancel(
        first.run_id
    )
    source_b = FreeTextSource(
        kind="free_text",
        registration_id=owner["registration_id"],
        request_id=uuid4(),
        task=source_a.task,
    )
    second = asyncio.run(services[owner["owner"]].admit(source_b, secrets.token_urlsafe(24)))
    assert second.run_id != first.run_id
    with admin_engine.connect() as connection:
        repo_id, issue_number = connection.execute(
            text(
                "SELECT github_repository_id, issue_number FROM runtime_private.runs "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner["owner"], "run": second.run_id},
        ).one()
    assert repo_id == owner["repository_id"] and issue_number is None
    CancellationService(services[owner["owner"]]._engine, Principal(owner["owner"])).cancel(
        second.run_id
    )

    invalid_source = FreeTextSource(
        kind="free_text",
        registration_id=owner["registration_id"],
        request_id=uuid4(),
        task="An unowned or missing selected key must reject before admission.",
        credential_id=uuid4(),
    )
    with pytest.raises(AdmissionError, match="credential_unavailable"):
        asyncio.run(services[owner["owner"]].admit(invalid_source, secrets.token_urlsafe(24)))

    credential_id = uuid4()
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.provider_credentials "
                "(credential_id, owner_id, provider, scope, key_version, nonce, ciphertext) "
                "VALUES (:credential, :owner, 'openai', 'saved', 'v1', :nonce, :ciphertext)"
            ),
            {
                "credential": credential_id,
                "owner": owner["owner"],
                "nonce": os.urandom(12),
                "ciphertext": os.urandom(32),
            },
        )
    selected_source = FreeTextSource(
        kind="free_text",
        registration_id=owner["registration_id"],
        request_id=uuid4(),
        task="An explicitly selected saved owner key can bind atomically.",
        credential_id=credential_id,
    )
    selected = asyncio.run(
        services[owner["owner"]].admit(selected_source, secrets.token_urlsafe(24))
    )
    assert selected.status == "queued"
    assert selected.credential_id == credential_id
    with admin_engine.connect() as connection:
        binding = connection.execute(
            text(
                "SELECT provider, credential_id FROM runtime_private.run_credentials "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner["owner"], "run": selected.run_id},
        ).one()
    assert binding.provider == "openai" and binding.credential_id == credential_id
    CancellationService(services[owner["owner"]]._engine, Principal(owner["owner"])).cancel(
        selected.run_id
    )
    _assert_revocation_and_admission_share_lock_order(
        services[owner["owner"]]._engine, services[owner["owner"]], owner, credential_id
    )


def _assert_revocation_and_admission_share_lock_order(
    runtime_engine: Engine, service: AdmissionService, owner, credential_id: UUID
) -> None:
    revocation_ready = Event()
    admission_ready = Event()
    principal = Principal(owner["owner"])

    class ObservedCredentials(CredentialService):
        def lock_owner_in_transaction(self, connection, *, active):
            admission_ready.set()
            return super().lock_owner_in_transaction(connection, active=active)

    class PausedRevocation(CredentialService):
        def _lock_credential(self, connection, selected_id):
            revocation_ready.set()
            assert admission_ready.wait(10), "admission never reached the owner lock"
            return super()._lock_credential(connection, selected_id)

    admitting = AdmissionService(
        runtime_engine,
        principal,
        service._facts_provider,
        ObservedCredentials(runtime_engine, principal, None),
        enabled=True,
    )
    revoking = PausedRevocation(runtime_engine, principal, None)
    source = FreeTextSource(
        kind="free_text",
        registration_id=owner["registration_id"],
        request_id=uuid4(),
        task="A revoked key must not admit or deadlock a new run.",
        credential_id=credential_id,
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        revoked = pool.submit(revoking.revoke, credential_id)
        assert revocation_ready.wait(10), "revocation never acquired the owner lock"
        admitted = pool.submit(
            lambda: asyncio.run(admitting.admit(source, secrets.token_urlsafe(24)))
        )
        assert revoked.result(timeout=15) is True
        with pytest.raises(AdmissionError, match="credential_unavailable"):
            admitted.result(timeout=15)


def _assert_no_direct_queue_access(runtime_engine: Engine, owner_id: UUID) -> None:
    with pytest.raises(DBAPIError):
        with runtime_engine.begin() as connection:
            connection.execute(text("SELECT * FROM runtime_private.admission_queue_seats")).all()
    with pytest.raises(DBAPIError):
        with runtime_engine.begin() as connection:
            connection.execute(
                text("SELECT runtime_private.reserve_admission_queue_seat(:owner,:run)"),
                {"owner": owner_id, "run": uuid4()},
            )


def _active_run_count(runtime_engine: Engine, owner_id: UUID) -> int:
    with runtime_engine.begin() as connection:
        connection.execute(
            text("SELECT set_config('continuum.principal_id', :owner, true)"),
            {"owner": str(owner_id)},
        )
        return connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.runs WHERE owner_id=:owner "
                "AND status IN ('queued','running','waiting_credentials','waiting_approval',"
                "'waiting_capacity','needs_attention')"
            ),
            {"owner": owner_id},
        ).scalar_one()


def _has_run(runtime_engine: Engine, owner_id: UUID, run_id: UUID) -> bool:
    with runtime_engine.begin() as connection:
        connection.execute(
            text("SELECT set_config('continuum.principal_id', :owner, true)"),
            {"owner": str(owner_id)},
        )
        return bool(
            connection.execute(
                text(
                    "SELECT EXISTS(SELECT 1 FROM runtime_private.runs "
                    "WHERE owner_id=:owner AND run_id=:run)"
                ),
                {"owner": owner_id, "run": run_id},
            ).scalar_one()
        )


def _reset_database(engine: Engine, runtime_password: str) -> None:
    with engine.begin() as connection:
        for schema in (
            "runtime_private",
            "wake_private",
            "webhook_private",
            "recovery_private",
            "operator_private",
            "continuum_migrations",
        ):
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        for role_name in ROLE_NAMES:
            if connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)"),
                {"name": role_name},
            ).scalar_one():
                connection.execute(text(f"DROP ROLE {role_name}"))
        for role_name, login, password in (
            ("continuum_ingress", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_issuer", "NOLOGIN", None),
            (ADMISSION_ROLE, "NOLOGIN", None),
            ("continuum_wake", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_wake_receiver", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_recovery", "LOGIN", secrets.token_urlsafe(32)),
            ("continuum_operator", "LOGIN", secrets.token_urlsafe(32)),
            (RUNTIME_ROLE, "LOGIN", runtime_password),
        ):
            secret = "" if password is None else f"PASSWORD '{password}'"
            connection.exec_driver_sql(
                f"CREATE ROLE {role_name} {login} NOSUPERUSER NOCREATEDB NOCREATEROLE "
                f"NOINHERIT NOBYPASSRLS {secret}"
            )


def _seed_owners(engine: Engine, count: int) -> list[dict[str, object]]:
    owners: list[dict[str, object]] = []
    with engine.begin() as connection:
        for index in range(count):
            owner_id = uuid4()
            github_user_id = 930_000 + index
            repository_id = 830_000 + index
            connection.execute(
                text(
                    "INSERT INTO runtime_private.users (user_id, github_user_id) "
                    "VALUES (:owner, :github_user)"
                ),
                {"owner": owner_id, "github_user": github_user_id},
            )
            payload = {
                "repository_id": repository_id,
                "commit_sha": "a" * 40,
                "tree_sha": "b" * 40,
                "profile_id": "python-pytest",
                "profile_version": "1",
                "file_count": 4,
                "source_bytes": 256,
                "manifest_blobs": [["pyproject.toml", "c" * 40]],
                "package_count": 1,
            }
            registration_id = connection.execute(
                text(
                    "INSERT INTO runtime_private.repository_registrations "
                    "(owner_id, github_repository_id, repository_owner, repository_name, "
                    "profile_id, profile_version, provider, eligibility_status, checked_sha, "
                    "eligibility_checked_at, eligibility_receipt) VALUES "
                    "(:owner, :repo, :repo_owner, :repo_name, 'python-pytest', '1', 'openai', "
                    "'eligible', :sha, clock_timestamp(), CAST(:receipt AS jsonb)) "
                    "RETURNING registration_id"
                ),
                {
                    "owner": owner_id,
                    "repo": repository_id,
                    "repo_owner": f"owner{index}",
                    "repo_name": f"repo-{index}",
                    "sha": "a" * 40,
                    "receipt": json.dumps(payload, separators=(",", ":")),
                },
            ).scalar_one()
            owners.append(
                {
                    "owner": owner_id,
                    "github_user_id": github_user_id,
                    "repository_id": repository_id,
                    "registration_id": registration_id,
                }
            )
    return owners


def _role_url(url: URL, role: str, password: str) -> URL:
    return url.set(drivername="postgresql+psycopg", username=role, password=password)


def _assert_postgres_17(engine: Engine) -> None:
    with engine.connect() as connection:
        version = connection.execute(
            text("SELECT current_setting('server_version_num')::integer")
        ).scalar_one()
    if not 170000 <= version < 180000:
        pytest.fail("T36 integration requires PostgreSQL 17")


def _run_migration(operation: str, revision: str, database_url: str) -> None:
    environment = os.environ.copy()
    environment["CONTINUUM_MIGRATION_DATABASE_URL"] = database_url
    result = subprocess.run(
        [sys.executable, "-m", "continuum_api.migrations", operation, revision],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        pytest.fail(f"T36 migration {operation} {revision} failed")


def _cleanup_database(engine: Engine) -> None:
    with engine.begin() as connection:
        for schema in (
            "runtime_private",
            "wake_private",
            "webhook_private",
            "recovery_private",
            "operator_private",
            "continuum_migrations",
        ):
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        connection.execute(text("DROP TABLE IF EXISTS public.alembic_version"))
        for role_name in ROLE_NAMES:
            if connection.execute(
                text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=:name)"),
                {"name": role_name},
            ).scalar_one():
                connection.execute(text(f"DROP ROLE {role_name}"))
