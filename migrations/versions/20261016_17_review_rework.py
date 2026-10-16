"""Persist owner review commands and bounded revision feedback.

Revision ID: 20261016_17
Revises: 20261015_16
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261016_17"
down_revision = "20261015_16"
branch_labels = None
depends_on = None
SCHEMA = "runtime_private"
RUNTIME_ROLE = "continuum_runtime"


def _owner_policy(table: str) -> None:
    predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(f"ALTER TABLE {SCHEMA}.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.{table} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.{table} FOR ALL TO {RUNTIME_ROLE} "
        f"USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.{table} FROM PUBLIC")


def upgrade() -> None:
    op.create_table(
        "revision_feedback",
        sa.Column("feedback_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("approval_id", sa.Uuid(), nullable=False),
        sa.Column("feedback", sa.Text(), nullable=False),
        sa.Column("feedback_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("actor_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("feedback_id", name="pk_runtime_revision_feedback"),
        sa.UniqueConstraint(
            "owner_id", "run_id", "revision", name="uq_runtime_revision_feedback_revision"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            name="fk_runtime_revision_feedback_run",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "approval_id"],
            [
                f"{SCHEMA}.approvals.owner_id",
                f"{SCHEMA}.approvals.run_id",
                f"{SCHEMA}.approvals.approval_id",
            ],
            name="fk_runtime_revision_feedback_approval",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "revision BETWEEN 1 AND 2", name="ck_runtime_revision_feedback_revision"
        ),
        sa.CheckConstraint(
            "octet_length(feedback_sha256)=32", name="ck_runtime_revision_feedback_digest"
        ),
        sa.CheckConstraint(
            "octet_length(feedback)<=16384", name="ck_runtime_revision_feedback_size"
        ),
        sa.CheckConstraint("actor_id=owner_id", name="ck_runtime_revision_feedback_owner_actor"),
        schema=SCHEMA,
    )
    _owner_policy("revision_feedback")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.revision_feedback TO {RUNTIME_ROLE}")

    op.create_table(
        "approval_commands",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("request_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("receipt", JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("owner_id", "idempotency_key", name="pk_runtime_approval_commands"),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            [f"{SCHEMA}.users.user_id"],
            name="fk_runtime_approval_commands_owner",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$'", name="ck_runtime_approval_commands_key"
        ),
        sa.CheckConstraint(
            "octet_length(request_sha256)=32", name="ck_runtime_approval_commands_digest"
        ),
        sa.CheckConstraint(
            "jsonb_typeof(receipt)='object'", name="ck_runtime_approval_commands_receipt"
        ),
        schema=SCHEMA,
    )
    _owner_policy("approval_commands")
    op.execute(f"GRANT SELECT, INSERT, DELETE ON {SCHEMA}.approval_commands TO {RUNTIME_ROLE}")


def downgrade() -> None:
    op.drop_table("approval_commands", schema=SCHEMA)
    op.drop_table("revision_feedback", schema=SCHEMA)
