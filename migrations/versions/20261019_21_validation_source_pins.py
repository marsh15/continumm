"""Persist immutable eligibility and baseline pins for candidate validation."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261019_21"
down_revision = "20261018_20"
branch_labels = None
depends_on = None

SCHEMA = "runtime_private"
RUNTIME_ROLE = "continuum_runtime"


def upgrade() -> None:
    op.create_table(
        "validation_source_pins",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("baseline_artifact_id", sa.Uuid(), nullable=False),
        sa.Column("eligibility_receipt", JSONB(), nullable=False),
        sa.Column("eligibility_sha256", sa.LargeBinary(32), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("step_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("lease_owner", sa.String(128), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("owner_id", "run_id", name="pk_runtime_validation_source_pins"),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            name="fk_runtime_validation_source_pins_run",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "baseline_artifact_id"],
            [
                f"{SCHEMA}.artifact_uploads.owner_id",
                f"{SCHEMA}.artifact_uploads.run_id",
                f"{SCHEMA}.artifact_uploads.upload_id",
            ],
            name="fk_runtime_validation_source_pins_baseline",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "step_id", "job_id"],
            [
                f"{SCHEMA}.jobs.owner_id",
                f"{SCHEMA}.jobs.run_id",
                f"{SCHEMA}.jobs.step_id",
                f"{SCHEMA}.jobs.job_id",
            ],
            name="fk_runtime_validation_source_pins_job",
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
            name="fk_runtime_validation_source_pins_attempt",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("revision >= 0", name="ck_runtime_validation_source_pins_revision"),
        sa.CheckConstraint("generation > 0", name="ck_runtime_validation_source_pins_generation"),
        sa.CheckConstraint(
            "length(btrim(lease_owner)) BETWEEN 1 AND 128",
            name="ck_runtime_validation_source_pins_lease_owner",
        ),
        sa.CheckConstraint(
            "octet_length(eligibility_sha256) = 32",
            name="ck_runtime_validation_source_pins_digest",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(eligibility_receipt) = 'object' AND "
            "octet_length(eligibility_receipt::text) <= 4096",
            name="ck_runtime_validation_source_pins_receipt_size",
        ),
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.validation_source_pins ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.validation_source_pins FORCE ROW LEVEL SECURITY")
    predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.validation_source_pins "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.validation_source_pins FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.validation_source_pins TO {RUNTIME_ROLE}")


def downgrade() -> None:
    op.drop_table("validation_source_pins", schema=SCHEMA)
