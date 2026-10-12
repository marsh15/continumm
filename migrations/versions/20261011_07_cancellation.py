"""Add cancellation terminality, dispatch observations, and cleanup intents.

Revision ID: 20261011_07
Revises: 20261011_06
Create Date: 2026-10-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "20261011_07"
down_revision: str | None = "20261011_06"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

RUNTIME_ROLE = "continuum_runtime"
SCHEMA = "runtime_private"


def upgrade() -> None:
    op.drop_constraint("ck_runtime_steps_status", "steps", schema=SCHEMA, type_="check")
    op.create_check_constraint(
        "ck_runtime_steps_status",
        "steps",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')",
        schema=SCHEMA,
    )
    op.drop_constraint(
        "ck_runtime_attempts_failure_class", "attempts", schema=SCHEMA, type_="check"
    )
    op.create_check_constraint(
        "ck_runtime_attempts_failure_class",
        "attempts",
        "failure_class IS NULL OR failure_class IN "
        "('invalid_input', 'missing_credential', 'transient_read', "
        "'unsupported_environment', 'resource_exhaustion', 'model_response_lost', "
        "'unknown_effect', 'remote_conflict', 'corrupt_artifact', 'worker_lost', 'cancelled')",
        schema=SCHEMA,
    )

    op.create_table(
        "resource_reservations",
        sa.Column("reservation_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("logical_key", sa.String(length=128), nullable=False),
        sa.Column("resource_kind", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("metadata", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("reservation_id", name="pk_runtime_resource_reservations"),
        sa.UniqueConstraint(
            "owner_id", "run_id", "logical_key", name="uq_runtime_resource_reservations_key"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            name="fk_runtime_resource_reservations_owner_run",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "length(btrim(logical_key)) BETWEEN 1 AND 128",
            name="ck_runtime_resource_reservations_key",
        ),
        sa.CheckConstraint(
            "length(btrim(resource_kind)) BETWEEN 1 AND 32",
            name="ck_runtime_resource_reservations_kind",
        ),
        sa.CheckConstraint(
            "status IN ('reserved', 'unknown', 'released')",
            name="ck_runtime_resource_reservations_status",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(metadata) = 'object' AND octet_length(metadata::text) <= 4096",
            name="ck_runtime_resource_reservations_metadata_limit",
        ),
        sa.CheckConstraint(
            "(status = 'released' AND observed_at IS NOT NULL) OR "
            "(status <> 'released' AND observed_at IS NULL)",
            name="ck_runtime_resource_reservations_observed_at",
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "dispatch_intents",
        sa.Column("dispatch_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("step_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=False),
        sa.Column("operation_key", sa.String(length=128), nullable=False),
        sa.Column("request_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("request_metadata", JSONB(), nullable=False),
        sa.Column("observation", JSONB(), nullable=True),
        sa.Column(
            "authorized_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("dispatch_id", name="pk_runtime_dispatch_intents"),
        sa.UniqueConstraint(
            "owner_id", "run_id", "operation_key", name="uq_runtime_dispatch_intents_key"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "step_id", "job_id"],
            [
                f"{SCHEMA}.jobs.owner_id",
                f"{SCHEMA}.jobs.run_id",
                f"{SCHEMA}.jobs.step_id",
                f"{SCHEMA}.jobs.job_id",
            ],
            name="fk_runtime_dispatch_intents_owner_job",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "job_id", "generation", "attempt_id", "lease_owner"],
            [
                f"{SCHEMA}.attempts.owner_id",
                f"{SCHEMA}.attempts.run_id",
                f"{SCHEMA}.attempts.job_id",
                f"{SCHEMA}.attempts.generation",
                f"{SCHEMA}.attempts.attempt_id",
                f"{SCHEMA}.attempts.lease_owner",
            ],
            name="fk_runtime_dispatch_intents_attempt",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("generation > 0", name="ck_runtime_dispatch_intents_generation"),
        sa.CheckConstraint(
            "length(btrim(lease_owner)) BETWEEN 1 AND 128",
            name="ck_runtime_dispatch_intents_lease_owner",
        ),
        sa.CheckConstraint(
            "length(btrim(operation_key)) BETWEEN 1 AND 128",
            name="ck_runtime_dispatch_intents_key",
        ),
        sa.CheckConstraint(
            "octet_length(request_sha256) = 32",
            name="ck_runtime_dispatch_intents_digest_length",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(request_metadata) = 'object' AND "
            "octet_length(request_metadata::text) <= 4096",
            name="ck_runtime_dispatch_intents_request_limit",
        ),
        sa.CheckConstraint(
            "(observation IS NULL AND observed_at IS NULL) OR "
            "(observation IS NOT NULL AND observed_at IS NOT NULL "
            "AND jsonb_typeof(observation) = 'object' AND "
            "octet_length(observation::text) <= 4096)",
            name="ck_runtime_dispatch_intents_observation_limit",
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "cleanup_intents",
        sa.Column("cleanup_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("cleanup_kind", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="pending"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("cleanup_id", name="pk_runtime_cleanup_intents"),
        sa.UniqueConstraint(
            "owner_id", "run_id", "cleanup_kind", name="uq_runtime_cleanup_intents_run_kind"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            name="fk_runtime_cleanup_intents_owner_run",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "cleanup_kind IN ('stop_execution', 'observe_effects')",
            name="ck_runtime_cleanup_intents_kind",
        ),
        sa.CheckConstraint("status = 'pending'", name="ck_runtime_cleanup_intents_pending"),
        schema=SCHEMA,
    )

    predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    for table_name in ("resource_reservations", "dispatch_intents", "cleanup_intents"):
        op.execute(f"ALTER TABLE {SCHEMA}.{table_name} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {SCHEMA}.{table_name} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.{table_name} "
            f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
        )
    op.execute(f"REVOKE ALL ON {SCHEMA}.resource_reservations FROM PUBLIC")
    op.execute(f"REVOKE ALL ON {SCHEMA}.dispatch_intents FROM PUBLIC")
    op.execute(f"REVOKE ALL ON {SCHEMA}.cleanup_intents FROM PUBLIC")
    # Allocation is owned by the later sandbox-capacity slice. Cancellation
    # reads reservation state and must leave it untouched.
    op.execute(f"GRANT SELECT ON {SCHEMA}.resource_reservations TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE (observation, observed_at) "
        f"ON {SCHEMA}.dispatch_intents TO {RUNTIME_ROLE}"
    )
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.cleanup_intents TO {RUNTIME_ROLE}")


def downgrade() -> None:
    # The pre-T11 schema has no cancelled step/failure labels. The terminal
    # run state and its immutable cancellation event retain the audit reason.
    op.execute(f"UPDATE {SCHEMA}.steps SET status='skipped' WHERE status='cancelled'")
    op.execute(f"UPDATE {SCHEMA}.attempts SET failure_class=NULL WHERE failure_class='cancelled'")
    op.drop_table("cleanup_intents", schema=SCHEMA)
    op.drop_table("dispatch_intents", schema=SCHEMA)
    op.drop_table("resource_reservations", schema=SCHEMA)
    op.drop_constraint(
        "ck_runtime_attempts_failure_class", "attempts", schema=SCHEMA, type_="check"
    )
    op.create_check_constraint(
        "ck_runtime_attempts_failure_class",
        "attempts",
        "failure_class IS NULL OR failure_class IN "
        "('invalid_input', 'missing_credential', 'transient_read', "
        "'unsupported_environment', 'resource_exhaustion', 'model_response_lost', "
        "'unknown_effect', 'remote_conflict', 'corrupt_artifact', 'worker_lost')",
        schema=SCHEMA,
    )
    op.drop_constraint("ck_runtime_steps_status", "steps", schema=SCHEMA, type_="check")
    op.create_check_constraint(
        "ck_runtime_steps_status",
        "steps",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'skipped')",
        schema=SCHEMA,
    )
