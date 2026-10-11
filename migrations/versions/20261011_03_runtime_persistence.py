"""Add private run, step, job, attempt, and event persistence.

Revision ID: 20261011_03
Revises: 20261011_02
Create Date: 2026-10-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "20261011_03"
down_revision: str | None = "20261011_02"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

RUNTIME_ROLE = "continuum_runtime"
SCHEMA = "runtime_private"

ACTIVE_RUN_STATES = (
    "queued",
    "running",
    "waiting_approval",
    "waiting_credentials",
    "waiting_capacity",
    "needs_attention",
)


def _validate_runtime_role() -> None:
    bind = op.get_bind()
    row = bind.execute(
        sa.text(
            """
            SELECT
                r.rolcanlogin,
                r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR
                    r.rolreplication OR r.rolbypassrls AS elevated,
                r.rolinherit,
                EXISTS (
                    SELECT 1 FROM pg_auth_members m WHERE m.member = r.oid
                ) AS has_parent_membership,
                EXISTS (
                    SELECT 1 FROM pg_auth_members m WHERE m.roleid = r.oid
                ) AS has_members
            FROM pg_roles r
            WHERE r.rolname = :role_name
            """
        ),
        {"role_name": RUNTIME_ROLE},
    ).one_or_none()
    if row is None:
        raise RuntimeError("create_continuum_runtime_role_before_migration")
    if not row.rolcanlogin or row.elevated or row.rolinherit:
        raise RuntimeError("continuum_runtime_role_privileges_invalid")
    if row.has_parent_membership or row.has_members:
        raise RuntimeError("continuum_runtime_role_membership_invalid")


def _grant_connect() -> None:
    bind = op.get_bind()
    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"GRANT CONNECT ON DATABASE {quoted_database} TO {RUNTIME_ROLE}")


def upgrade() -> None:
    _validate_runtime_role()
    _grant_connect()

    op.execute(f"CREATE SCHEMA {SCHEMA}")
    op.execute(f"REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {RUNTIME_ROLE}")

    op.create_table(
        "users",
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("github_user_id", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="active"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("user_id", name="pk_runtime_users"),
        sa.UniqueConstraint("github_user_id", name="uq_runtime_users_github_user_id"),
        sa.CheckConstraint("github_user_id > 0", name="ck_runtime_users_github_user_id"),
        sa.CheckConstraint(
            "status IN ('active', 'suspended', 'deleting', 'deleted')",
            name="ck_runtime_users_status",
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "runs",
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("github_repository_id", sa.BigInteger(), nullable=True),
        sa.Column("issue_number", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="queued"),
        sa.Column("revision", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("run_id", name="pk_runtime_runs"),
        sa.UniqueConstraint("owner_id", "run_id", name="uq_runtime_runs_owner_run"),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            [f"{SCHEMA}.users.user_id"],
            name="fk_runtime_runs_owner",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'waiting_approval', 'waiting_credentials', "
            "'waiting_capacity', 'needs_attention', 'succeeded', 'failed', 'cancelled', 'expired')",
            name="ck_runtime_runs_status",
        ),
        sa.CheckConstraint("revision >= 0", name="ck_runtime_runs_revision_nonnegative"),
        sa.CheckConstraint(
            "(github_repository_id IS NULL AND issue_number IS NULL) OR "
            "(github_repository_id IS NOT NULL AND issue_number IS NOT NULL AND "
            "github_repository_id > 0 AND issue_number > 0)",
            name="ck_runtime_runs_repository_issue_pair",
        ),
        sa.CheckConstraint(
            "deadline_at IS NULL OR deadline_at > created_at",
            name="ck_runtime_runs_deadline_after_creation",
        ),
        schema=SCHEMA,
    )
    active_states_sql = ", ".join(f"'{state}'" for state in ACTIVE_RUN_STATES)
    op.create_index(
        "uq_runtime_runs_one_active_account",
        "runs",
        ["owner_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text(f"status IN ({active_states_sql})"),
    )
    op.create_index(
        "uq_runtime_runs_one_active_issue",
        "runs",
        ["owner_id", "github_repository_id", "issue_number"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text(f"status IN ({active_states_sql}) AND issue_number IS NOT NULL"),
    )
    op.create_index(
        "ix_runtime_runs_owner_created",
        "runs",
        ["owner_id", "created_at"],
        schema=SCHEMA,
    )

    op.create_table(
        "steps",
        sa.Column("step_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("logical_key", sa.String(length=128), nullable=False),
        sa.Column("activity", sa.String(length=64), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="queued"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("step_id", name="pk_runtime_steps"),
        sa.UniqueConstraint(
            "owner_id",
            "run_id",
            "logical_key",
            "revision",
            name="uq_runtime_steps_logical_key",
        ),
        sa.UniqueConstraint(
            "owner_id", "run_id", "step_id", name="uq_runtime_steps_owner_run_step"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            name="fk_runtime_steps_owner_run",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "length(btrim(logical_key)) BETWEEN 1 AND 128",
            name="ck_runtime_steps_logical_key",
        ),
        sa.CheckConstraint(
            "length(btrim(activity)) BETWEEN 1 AND 64", name="ck_runtime_steps_activity"
        ),
        sa.CheckConstraint("revision >= 0", name="ck_runtime_steps_revision_nonnegative"),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'skipped')",
            name="ck_runtime_steps_status",
        ),
        schema=SCHEMA,
    )
    op.create_index("ix_runtime_steps_owner_run", "steps", ["owner_id", "run_id"], schema=SCHEMA)

    op.create_table(
        "jobs",
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("step_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="queued"),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("lease_owner", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("generation", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("job_id", name="pk_runtime_jobs"),
        sa.UniqueConstraint("owner_id", "step_id", name="uq_runtime_jobs_step"),
        sa.UniqueConstraint("owner_id", "run_id", "job_id", name="uq_runtime_jobs_owner_run_job"),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "step_id"],
            [f"{SCHEMA}.steps.owner_id", f"{SCHEMA}.steps.run_id", f"{SCHEMA}.steps.step_id"],
            name="fk_runtime_jobs_owner_run_step",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'leased', 'succeeded', 'failed', 'cancelled')",
            name="ck_runtime_jobs_status",
        ),
        sa.CheckConstraint("generation >= 0", name="ck_runtime_jobs_generation_nonnegative"),
        sa.CheckConstraint(
            "(status = 'leased' AND lease_owner IS NOT NULL AND "
            "length(btrim(lease_owner)) BETWEEN 1 AND 128 AND lease_expires_at IS NOT NULL) OR "
            "(status <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)",
            name="ck_runtime_jobs_lease_fields",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_runtime_jobs_owner_available",
        "jobs",
        ["owner_id", "available_at"],
        schema=SCHEMA,
    )

    op.create_table(
        "attempts",
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="running"),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("attempt_id", name="pk_runtime_attempts"),
        sa.UniqueConstraint("job_id", "generation", name="uq_runtime_attempts_job_generation"),
        sa.UniqueConstraint(
            "owner_id", "run_id", "attempt_id", name="uq_runtime_attempts_owner_run_attempt"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "job_id"],
            [f"{SCHEMA}.jobs.owner_id", f"{SCHEMA}.jobs.run_id", f"{SCHEMA}.jobs.job_id"],
            name="fk_runtime_attempts_owner_run_job",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("generation > 0", name="ck_runtime_attempts_generation_positive"),
        sa.CheckConstraint(
            "status IN ('running', 'succeeded', 'retryable_failure', 'definitive_failure', "
            "'unknown', 'protocol_failure')",
            name="ck_runtime_attempts_status",
        ),
        sa.CheckConstraint(
            "(status = 'running' AND finished_at IS NULL) OR "
            "(status <> 'running' AND finished_at IS NOT NULL)",
            name="ck_runtime_attempts_finished_at",
        ),
        sa.CheckConstraint(
            "finished_at IS NULL OR finished_at >= started_at",
            name="ck_runtime_attempts_time_order",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_runtime_attempts_owner_job",
        "attempts",
        ["owner_id", "job_id", "generation"],
        schema=SCHEMA,
    )

    op.create_table(
        "events",
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("actor_type", sa.String(length=16), nullable=False),
        sa.Column("actor_id", sa.String(length=128), nullable=True),
        sa.Column("payload_version", sa.SmallInteger(), nullable=False, server_default="1"),
        sa.Column(
            "payload",
            JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("event_id", name="pk_runtime_events"),
        sa.UniqueConstraint(
            "owner_id", "run_id", "sequence", name="uq_runtime_events_run_sequence"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            name="fk_runtime_events_owner_run",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("sequence > 0", name="ck_runtime_events_sequence_positive"),
        sa.CheckConstraint(
            "length(btrim(event_type)) BETWEEN 1 AND 64", name="ck_runtime_events_type"
        ),
        sa.CheckConstraint(
            "actor_type IN ('account', 'system', 'worker', 'provider')",
            name="ck_runtime_events_actor_type",
        ),
        sa.CheckConstraint(
            "actor_id IS NULL OR length(btrim(actor_id)) BETWEEN 1 AND 128",
            name="ck_runtime_events_actor_id",
        ),
        sa.CheckConstraint(
            "payload_version > 0", name="ck_runtime_events_payload_version_positive"
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 8192",
            name="ck_runtime_events_payload_limit",
        ),
        schema=SCHEMA,
    )
    for table_name, owner_column in (
        ("users", "user_id"),
        ("runs", "owner_id"),
        ("steps", "owner_id"),
        ("jobs", "owner_id"),
        ("attempts", "owner_id"),
        ("events", "owner_id"),
    ):
        op.execute(f"ALTER TABLE {SCHEMA}.{table_name} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {SCHEMA}.{table_name} FORCE ROW LEVEL SECURITY")
        predicate = (
            f"{owner_column} = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
        )
        op.execute(
            f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.{table_name} "
            f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
        )

    op.execute(f"REVOKE ALL ON ALL TABLES IN SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"GRANT SELECT ON {SCHEMA}.users TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE ON {SCHEMA}.runs, {SCHEMA}.steps, {SCHEMA}.jobs, "
        f"{SCHEMA}.attempts TO {RUNTIME_ROLE}"
    )
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.events TO {RUNTIME_ROLE}")


def downgrade() -> None:
    op.execute(f"DROP SCHEMA {SCHEMA} CASCADE")
    bind = op.get_bind()
    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"REVOKE CONNECT ON DATABASE {quoted_database} FROM {RUNTIME_ROLE}")
