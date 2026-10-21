"""PostgreSQL proof for owner-scoped workflow admission and billing pins."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, text
from test_agent_loop import t22_database as shared_t22_database

from continuum_api.runtime.agent_loop import OPENAI_MODEL_ID
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.runtime.workflow_resources import (
    WorkflowConfigurationError,
    _read_admission_pins,
    _require_current_provider_period,
)


@pytest.fixture(scope="module")
def workflow_database():
    yield from shared_t22_database.__wrapped__()


def test_admission_reads_require_principal_and_allow_missing_credential(workflow_database) -> None:
    owner, other = uuid4(), uuid4()
    run_id = _seed_admitted_run(workflow_database, owner)
    with workflow_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET status='waiting_credentials' "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        )

    pins = _read_admission_pins(workflow_database.runtime_engine, owner, run_id)
    assert pins.provider.value == "openai"
    assert pins.profile_id == "python-pytest"
    with pytest.raises(WorkflowConfigurationError, match="workflow_run_not_found"):
        _read_admission_pins(workflow_database.runtime_engine, other, run_id)


def test_admission_rejects_expired_content_and_checks_current_provider_period(
    workflow_database,
) -> None:
    owner = uuid4()
    run_id = _seed_admitted_run(workflow_database, owner)
    with workflow_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs "
                "SET created_at=clock_timestamp()-interval '8 days', "
                "content_expires_at=clock_timestamp()-interval '1 second' "
                "WHERE owner_id=:owner AND run_id=:run"
            ),
            {"owner": owner, "run": run_id},
        )
    with pytest.raises(WorkflowConfigurationError, match="workflow_run_inactive"):
        _read_admission_pins(workflow_database.runtime_engine, owner, run_id)

    _insert_period(workflow_database.admin_engine, "workflow-current-period")
    _require_current_provider_period(workflow_database.runtime_engine, "workflow-current-period")
    with pytest.raises(WorkflowConfigurationError, match="workflow_provider_period_mismatch"):
        _require_current_provider_period(workflow_database.runtime_engine, "workflow-other-period")


def _seed_admitted_run(database, owner_id: UUID) -> UUID:
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:owner, :github_user)"
            ),
            {"owner": owner_id, "github_user": 900_000_000 + owner_id.int % 1_000_000},
        )
        registration_id = connection.execute(
            text(
                "INSERT INTO runtime_private.repository_registrations "
                "(owner_id, github_repository_id, repository_owner, repository_name, "
                "profile_id, profile_version, provider) "
                "VALUES (:owner, :repo, 'owner', 'repo', 'python-pytest', '1', 'openai') "
                "RETURNING registration_id"
            ),
            {"owner": owner_id, "repo": 1_000_000 + owner_id.int % 1_000_000},
        ).scalar_one()
    with database.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, Principal(owner_id))
        run_id = repository.create_run(deadline_at=datetime.now(UTC) + timedelta(days=1))
        connection.execute(
            text(
                "INSERT INTO runtime_private.run_admission_pins "
                "(owner_id, run_id, registration_id, defaults_version, profile_id, "
                "profile_version, base_branch, source_sha, source_tree_sha, source_receipt, "
                "provider, model_id, task_context, submission_digest) "
                "VALUES (:owner, :run, :registration, 1, 'python-pytest', '1', 'main', "
                ":sha, :tree, '{}'::jsonb, 'openai', :model, '{\"task\":\"test\"}'::jsonb, :digest)"
            ),
            {
                "owner": owner_id,
                "run": run_id,
                "registration": registration_id,
                "sha": "a" * 40,
                "tree": "b" * 40,
                "model": OPENAI_MODEL_ID,
                "digest": b"x" * 32,
            },
        )
    return run_id


def _insert_period(engine: Engine, period_id: str) -> None:
    now = datetime.now(UTC)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.sandbox_provider_periods "
                "(provider_period_id, starts_at, ends_at, public_cpu_seconds_limit, "
                "operator_cpu_seconds_limit) VALUES (:id, :start, :end, 14400, 3600)"
            ),
            {"id": period_id, "start": now - timedelta(hours=1), "end": now + timedelta(hours=1)},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.sandbox_period_usage "
                "(provider_period_id, allocation_class) VALUES (:id, 'public'), (:id, 'operator')"
            ),
            {"id": period_id},
        )
