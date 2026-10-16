"""Persist immutable exact-publication manifests and owner decisions.

Revision ID: 20261015_16
Revises: 20261015_15
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261015_16"
down_revision = "20261015_15"
branch_labels = None
depends_on = None
SCHEMA = "runtime_private"
RUNTIME_ROLE = "continuum_runtime"


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_runtime_accepted_results_owner_run_result",
        "accepted_results",
        ["owner_id", "run_id", "result_id"],
        schema=SCHEMA,
    )
    op.create_table(
        "approvals",
        sa.Column("approval_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("candidate_artifact_id", sa.Uuid(), nullable=False),
        sa.Column("evidence_result_id", sa.Uuid(), nullable=False),
        sa.Column("manifest", JSONB(), nullable=False),
        sa.Column("manifest_sha256", sa.LargeBinary(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("decision_actor_id", sa.Uuid(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("approval_id", name="pk_runtime_approvals"),
        sa.UniqueConstraint(
            "owner_id", "run_id", "approval_id", name="uq_runtime_approvals_owner_run_id"
        ),
        sa.UniqueConstraint("owner_id", "run_id", "revision", name="uq_runtime_approvals_revision"),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            name="fk_runtime_approvals_owner_run",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "candidate_artifact_id"],
            [
                f"{SCHEMA}.artifact_uploads.owner_id",
                f"{SCHEMA}.artifact_uploads.run_id",
                f"{SCHEMA}.artifact_uploads.upload_id",
            ],
            name="fk_runtime_approvals_candidate_artifact",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "evidence_result_id"],
            [
                f"{SCHEMA}.accepted_results.owner_id",
                f"{SCHEMA}.accepted_results.run_id",
                f"{SCHEMA}.accepted_results.result_id",
            ],
            name="fk_runtime_approvals_evidence_result",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("revision >= 0", name="ck_runtime_approvals_revision"),
        sa.CheckConstraint("octet_length(manifest_sha256)=32", name="ck_runtime_approvals_digest"),
        sa.CheckConstraint(
            "jsonb_typeof(manifest)='object' AND octet_length(manifest::text)<=65536",
            name="ck_runtime_approvals_manifest_size",
        ),
        sa.CheckConstraint(
            "status IN ('pending','approved','rejected','changes','expired')",
            name="ck_runtime_approvals_status",
        ),
        sa.CheckConstraint(
            "(status='pending' AND decision_actor_id IS NULL AND decided_at IS NULL) OR "
            "(status IN ('approved','rejected','changes') AND "
            "decision_actor_id IS NOT NULL AND decided_at IS NOT NULL) OR "
            "(status='expired' AND decision_actor_id IS NULL AND decided_at IS NOT NULL)",
            name="ck_runtime_approvals_decision",
        ),
        sa.CheckConstraint(
            "decision_actor_id IS NULL OR decision_actor_id=owner_id",
            name="ck_runtime_approvals_owner_decision",
        ),
        sa.CheckConstraint(
            "expires_at > created_at AND expires_at <= created_at + interval '48 hours'",
            name="ck_runtime_approvals_expiry",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "uq_runtime_approvals_one_pending",
        "approvals",
        ["owner_id", "run_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("status='pending'"),
    )
    op.create_index("ix_runtime_approvals_expiry", "approvals", ["expires_at"], schema=SCHEMA)
    op.execute(f"ALTER TABLE {SCHEMA}.approvals ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.approvals FORCE ROW LEVEL SECURITY")
    predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.approvals FOR ALL TO {RUNTIME_ROLE} "
        f"USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.approvals FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.approvals TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE (status, decision_actor_id, decided_at) "
        f"ON {SCHEMA}.approvals TO {RUNTIME_ROLE}"
    )


def downgrade() -> None:
    op.drop_table("approvals", schema=SCHEMA)
    op.drop_constraint(
        "uq_runtime_accepted_results_owner_run_result",
        "accepted_results",
        schema=SCHEMA,
        type_="unique",
    )
