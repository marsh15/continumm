"""Persist accepted activity results with atomic runtime transitions.

Revision ID: 20261011_04
Revises: 20261011_03
Create Date: 2026-10-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "20261011_04"
down_revision: str | None = "20261011_03"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

RUNTIME_ROLE = "continuum_runtime"
SCHEMA = "runtime_private"


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_runtime_jobs_owner_run_step_job",
        "jobs",
        ["owner_id", "run_id", "step_id", "job_id"],
        schema=SCHEMA,
    )
    op.create_table(
        "accepted_results",
        sa.Column("result_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("step_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("result_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("intent_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("run_status", sa.String(length=32), nullable=False),
        sa.Column("event_sequence", sa.BigInteger(), nullable=False),
        sa.Column("next_job_id", sa.Uuid(), nullable=True),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column(
            "accepted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("result_id", name="pk_runtime_accepted_results"),
        sa.UniqueConstraint(
            "owner_id", "run_id", "step_id", name="uq_runtime_accepted_results_step"
        ),
        sa.UniqueConstraint("owner_id", "run_id", "job_id", name="uq_runtime_accepted_results_job"),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "step_id", "job_id"],
            [
                f"{SCHEMA}.jobs.owner_id",
                f"{SCHEMA}.jobs.run_id",
                f"{SCHEMA}.jobs.step_id",
                f"{SCHEMA}.jobs.job_id",
            ],
            name="fk_runtime_accepted_results_owner_run_step_job",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "event_sequence"],
            [
                f"{SCHEMA}.events.owner_id",
                f"{SCHEMA}.events.run_id",
                f"{SCHEMA}.events.sequence",
            ],
            name="fk_runtime_accepted_results_event",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "next_job_id"],
            [f"{SCHEMA}.jobs.owner_id", f"{SCHEMA}.jobs.run_id", f"{SCHEMA}.jobs.job_id"],
            name="fk_runtime_accepted_results_next_job",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "octet_length(result_sha256) = 32", name="ck_runtime_accepted_results_digest_length"
        ),
        sa.CheckConstraint(
            "octet_length(intent_sha256) = 32", name="ck_runtime_accepted_results_intent_length"
        ),
        sa.CheckConstraint("event_sequence > 0", name="ck_runtime_accepted_results_sequence"),
        sa.CheckConstraint(
            "run_status IN ('running', 'waiting_approval', 'waiting_credentials', "
            "'waiting_capacity', 'needs_attention', 'failed', 'cancelled', 'expired')",
            name="ck_runtime_accepted_results_run_status",
        ),
        sa.CheckConstraint(
            "next_job_id IS NULL OR next_job_id <> job_id",
            name="ck_runtime_accepted_results_next_job_distinct",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 65536",
            name="ck_runtime_accepted_results_payload_limit",
        ),
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.accepted_results ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.accepted_results FORCE ROW LEVEL SECURITY")
    predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.accepted_results "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.accepted_results FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.accepted_results TO {RUNTIME_ROLE}")


def downgrade() -> None:
    op.drop_table("accepted_results", schema=SCHEMA)
    op.drop_constraint("uq_runtime_jobs_owner_run_step_job", "jobs", schema=SCHEMA, type_="unique")
