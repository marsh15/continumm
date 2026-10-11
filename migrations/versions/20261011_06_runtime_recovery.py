"""Add durable retry accounting and conservative replay policy.

Revision ID: 20261011_06
Revises: 20261011_05
Create Date: 2026-10-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "20261011_06"
down_revision: str | None = "20261011_05"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

RUNTIME_ROLE = "continuum_runtime"
SCHEMA = "runtime_private"


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default="0"),
        schema=SCHEMA,
    )
    op.add_column(
        "jobs",
        sa.Column(
            "replay_policy",
            sa.String(length=32),
            nullable=False,
            server_default="attention_on_unknown",
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "jobs",
        sa.Column("capacity_resume_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_jobs_failure_count",
        "jobs",
        "failure_count >= 0",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_jobs_replay_policy",
        "jobs",
        "replay_policy IN ('replay_safe', 'attention_on_unknown')",
        schema=SCHEMA,
    )

    op.add_column(
        "attempts", sa.Column("failure_class", sa.String(length=32), nullable=True), schema=SCHEMA
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

    op.execute(f"REVOKE UPDATE ON {SCHEMA}.attempts FROM {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE (status, finished_at, failure_class) ON {SCHEMA}.attempts TO {RUNTIME_ROLE}"
    )


def downgrade() -> None:
    op.execute(f"REVOKE UPDATE (failure_class) ON {SCHEMA}.attempts FROM {RUNTIME_ROLE}")
    op.execute(f"GRANT UPDATE (status, finished_at) ON {SCHEMA}.attempts TO {RUNTIME_ROLE}")
    op.drop_constraint(
        "ck_runtime_attempts_failure_class", "attempts", schema=SCHEMA, type_="check"
    )
    op.drop_column("attempts", "failure_class", schema=SCHEMA)
    op.drop_constraint("ck_runtime_jobs_replay_policy", "jobs", schema=SCHEMA, type_="check")
    op.drop_constraint("ck_runtime_jobs_failure_count", "jobs", schema=SCHEMA, type_="check")
    op.drop_column("jobs", "capacity_resume_at", schema=SCHEMA)
    op.drop_column("jobs", "replay_policy", schema=SCHEMA)
    op.drop_column("jobs", "failure_count", schema=SCHEMA)
