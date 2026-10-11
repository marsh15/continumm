"""Add execution lease identity and bounded per-attempt checkpoints.

Revision ID: 20261011_05
Revises: 20261011_04
Create Date: 2026-10-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "20261011_05"
down_revision: str | None = "20261011_04"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

RUNTIME_ROLE = "continuum_runtime"
SCHEMA = "runtime_private"


def upgrade() -> None:
    op.add_column(
        "attempts", sa.Column("lease_owner", sa.String(length=128), nullable=True), schema=SCHEMA
    )
    op.create_check_constraint(
        "ck_runtime_attempts_lease_owner",
        "attempts",
        "lease_owner IS NULL OR length(btrim(lease_owner)) BETWEEN 1 AND 128",
        schema=SCHEMA,
    )
    op.create_unique_constraint(
        "uq_runtime_attempts_execution_identity",
        "attempts",
        ["owner_id", "run_id", "job_id", "generation", "attempt_id", "lease_owner"],
        schema=SCHEMA,
    )

    op.add_column(
        "accepted_results", sa.Column("generation", sa.BigInteger(), nullable=True), schema=SCHEMA
    )
    op.add_column(
        "accepted_results", sa.Column("attempt_id", sa.Uuid(), nullable=True), schema=SCHEMA
    )
    op.add_column(
        "accepted_results",
        sa.Column("lease_owner", sa.String(length=128), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_accepted_results_execution_identity",
        "accepted_results",
        "(generation IS NULL AND attempt_id IS NULL AND lease_owner IS NULL) OR "
        "(generation IS NOT NULL AND generation > 0 AND attempt_id IS NOT NULL "
        "AND lease_owner IS NOT NULL AND "
        "length(btrim(lease_owner)) BETWEEN 1 AND 128)",
        schema=SCHEMA,
    )
    op.create_foreign_key(
        "fk_runtime_accepted_results_attempt",
        "accepted_results",
        "attempts",
        ["owner_id", "run_id", "job_id", "generation", "attempt_id", "lease_owner"],
        ["owner_id", "run_id", "job_id", "generation", "attempt_id", "lease_owner"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="RESTRICT",
    )

    op.create_table(
        "attempt_checkpoints",
        sa.Column("checkpoint_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("step_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=False),
        sa.Column("checkpoint_key", sa.String(length=128), nullable=False),
        sa.Column("payload_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("checkpoint_id", name="pk_runtime_attempt_checkpoints"),
        sa.UniqueConstraint(
            "owner_id",
            "run_id",
            "attempt_id",
            "checkpoint_key",
            name="uq_runtime_attempt_checkpoints_attempt_key",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "step_id", "job_id"],
            [
                f"{SCHEMA}.jobs.owner_id",
                f"{SCHEMA}.jobs.run_id",
                f"{SCHEMA}.jobs.step_id",
                f"{SCHEMA}.jobs.job_id",
            ],
            name="fk_runtime_attempt_checkpoints_owner_job",
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
            name="fk_runtime_attempt_checkpoints_attempt",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "generation > 0", name="ck_runtime_attempt_checkpoints_generation_positive"
        ),
        sa.CheckConstraint(
            "length(btrim(lease_owner)) BETWEEN 1 AND 128",
            name="ck_runtime_attempt_checkpoints_lease_owner",
        ),
        sa.CheckConstraint(
            "length(btrim(checkpoint_key)) BETWEEN 1 AND 128",
            name="ck_runtime_attempt_checkpoints_key",
        ),
        sa.CheckConstraint(
            "octet_length(payload_sha256) = 32",
            name="ck_runtime_attempt_checkpoints_digest_length",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 32768",
            name="ck_runtime_attempt_checkpoints_payload_limit",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_runtime_attempt_checkpoints_attempt",
        "attempt_checkpoints",
        ["owner_id", "run_id", "attempt_id"],
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.attempt_checkpoints ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.attempt_checkpoints FORCE ROW LEVEL SECURITY")
    predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.attempt_checkpoints "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.attempt_checkpoints FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.attempt_checkpoints TO {RUNTIME_ROLE}")

    op.execute(f"REVOKE UPDATE ON {SCHEMA}.attempts FROM {RUNTIME_ROLE}")
    op.execute(f"GRANT UPDATE (status, finished_at) ON {SCHEMA}.attempts TO {RUNTIME_ROLE}")


def downgrade() -> None:
    op.drop_table("attempt_checkpoints", schema=SCHEMA)
    op.drop_constraint(
        "fk_runtime_accepted_results_attempt",
        "accepted_results",
        schema=SCHEMA,
        type_="foreignkey",
    )
    op.drop_constraint(
        "ck_runtime_accepted_results_execution_identity",
        "accepted_results",
        schema=SCHEMA,
        type_="check",
    )
    op.drop_column("accepted_results", "lease_owner", schema=SCHEMA)
    op.drop_column("accepted_results", "attempt_id", schema=SCHEMA)
    op.drop_column("accepted_results", "generation", schema=SCHEMA)
    op.execute(f"GRANT UPDATE ON {SCHEMA}.attempts TO {RUNTIME_ROLE}")
    op.drop_constraint(
        "uq_runtime_attempts_execution_identity",
        "attempts",
        schema=SCHEMA,
        type_="unique",
    )
    op.drop_constraint("ck_runtime_attempts_lease_owner", "attempts", schema=SCHEMA, type_="check")
    op.drop_column("attempts", "lease_owner", schema=SCHEMA)
