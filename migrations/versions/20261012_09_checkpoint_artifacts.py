"""Persist source checkpoint upload reservations and global storage capacity.

Revision ID: 20261012_09
Revises: 20261012_08
Create Date: 2026-10-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "20261012_09"
down_revision: str | None = "20261012_08"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

RUNTIME_ROLE = "continuum_runtime"
SCHEMA = "runtime_private"
CAPACITY_BYTES = 629_145_600
MAX_COMPRESSED_BYTES = 10_485_760


def upgrade() -> None:
    op.create_table(
        "artifact_capacity",
        sa.Column("capacity_id", sa.SmallInteger(), nullable=False),
        sa.Column("capacity_limit_bytes", sa.BigInteger(), nullable=False),
        sa.Column("charged_bytes", sa.BigInteger(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("capacity_id", name="pk_runtime_artifact_capacity"),
        sa.CheckConstraint("capacity_id = 1", name="ck_runtime_artifact_capacity_singleton"),
        sa.CheckConstraint(
            f"capacity_limit_bytes = {CAPACITY_BYTES}",
            name="ck_runtime_artifact_capacity_limit",
        ),
        sa.CheckConstraint(
            "charged_bytes >= 0 AND charged_bytes <= capacity_limit_bytes",
            name="ck_runtime_artifact_capacity_charged",
        ),
        schema=SCHEMA,
    )
    op.bulk_insert(
        sa.table(
            "artifact_capacity",
            sa.column("capacity_id", sa.SmallInteger()),
            sa.column("capacity_limit_bytes", sa.BigInteger()),
            sa.column("charged_bytes", sa.BigInteger()),
            schema=SCHEMA,
        ),
        [{"capacity_id": 1, "capacity_limit_bytes": CAPACITY_BYTES, "charged_bytes": 0}],
    )

    op.create_table(
        "artifact_uploads",
        sa.Column("upload_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("parent_upload_id", sa.Uuid(), nullable=True),
        sa.Column("step_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=False),
        sa.Column("github_repository_id", sa.BigInteger(), nullable=False),
        sa.Column("checkpoint_key", sa.String(length=128), nullable=False),
        sa.Column("intent_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("object_key", sa.String(length=96), nullable=False),
        sa.Column("archive_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("tree_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("format_version", sa.SmallInteger(), nullable=False),
        sa.Column("compressed_bytes", sa.BigInteger(), nullable=False),
        sa.Column("source_bytes", sa.BigInteger(), nullable=False),
        sa.Column("file_count", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="reserved"),
        sa.Column("object_bytes", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column(
            "orphan_grace_until",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp() + interval '1 hour'"),
        ),
        sa.Column("content_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("committed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("orphaned_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("upload_id", name="pk_runtime_artifact_uploads"),
        sa.UniqueConstraint("object_key", name="uq_runtime_artifact_uploads_object_key"),
        sa.UniqueConstraint(
            "owner_id",
            "run_id",
            "upload_id",
            name="uq_runtime_artifact_uploads_owner_run_id",
        ),
        sa.UniqueConstraint(
            "owner_id",
            "run_id",
            "attempt_id",
            "checkpoint_key",
            name="uq_runtime_artifact_uploads_attempt_key",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            name="fk_runtime_artifact_uploads_owner_run",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "parent_upload_id"],
            [
                f"{SCHEMA}.artifact_uploads.owner_id",
                f"{SCHEMA}.artifact_uploads.run_id",
                f"{SCHEMA}.artifact_uploads.upload_id",
            ],
            name="fk_runtime_artifact_uploads_parent",
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
            name="fk_runtime_artifact_uploads_owner_job",
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
            name="fk_runtime_artifact_uploads_execution",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("github_repository_id > 0", name="ck_runtime_artifact_repository_id"),
        sa.CheckConstraint(
            "parent_upload_id IS NULL OR parent_upload_id <> upload_id",
            name="ck_runtime_artifact_parent_distinct",
        ),
        sa.CheckConstraint("generation > 0", name="ck_runtime_artifact_generation"),
        sa.CheckConstraint(
            "length(btrim(lease_owner)) BETWEEN 1 AND 128",
            name="ck_runtime_artifact_lease_owner",
        ),
        sa.CheckConstraint(
            "length(btrim(checkpoint_key)) BETWEEN 1 AND 128",
            name="ck_runtime_artifact_checkpoint_key",
        ),
        sa.CheckConstraint("octet_length(intent_sha256) = 32", name="ck_runtime_artifact_intent"),
        sa.CheckConstraint(
            "object_key ~ '^checkpoints/[0-9a-f-]{36}\\.tar\\.gz$'",
            name="ck_runtime_artifact_object_key",
        ),
        sa.CheckConstraint(
            "octet_length(archive_sha256) = 32 AND octet_length(tree_sha256) = 32",
            name="ck_runtime_artifact_digest_lengths",
        ),
        sa.CheckConstraint("format_version = 1", name="ck_runtime_artifact_format_version"),
        sa.CheckConstraint(
            f"compressed_bytes BETWEEN 1 AND {MAX_COMPRESSED_BYTES}",
            name="ck_runtime_artifact_compressed_bytes",
        ),
        sa.CheckConstraint(
            "source_bytes BETWEEN 0 AND 52428800", name="ck_runtime_artifact_source_bytes"
        ),
        sa.CheckConstraint("file_count BETWEEN 0 AND 5000", name="ck_runtime_artifact_file_count"),
        sa.CheckConstraint(
            "status IN ('reserved', 'uploading', 'unknown', 'verified', 'orphan', "
            "'committed', 'released', 'deleting')",
            name="ck_runtime_artifact_status",
        ),
        sa.CheckConstraint(
            "(status IN ('verified', 'committed') AND verified_at IS NOT NULL "
            "AND object_bytes = compressed_bytes) OR "
            "(status NOT IN ('verified', 'committed'))",
            name="ck_runtime_artifact_verified_metadata",
        ),
        sa.CheckConstraint(
            "(status = 'committed' AND committed_at IS NOT NULL) OR "
            "(status <> 'committed' AND committed_at IS NULL)",
            name="ck_runtime_artifact_committed_at",
        ),
        sa.CheckConstraint(
            "(status = 'released' AND released_at IS NOT NULL) OR "
            "(status <> 'released' AND released_at IS NULL)",
            name="ck_runtime_artifact_released_at",
        ),
        sa.CheckConstraint(
            "(orphaned_at IS NULL AND status <> 'orphan' AND "
            "orphan_grace_until > created_at AND "
            "orphan_grace_until <= created_at + interval '1 hour 1 second') OR "
            "(orphaned_at IS NOT NULL AND status IN ('orphan', 'deleting', 'released') AND "
            "orphan_grace_until > orphaned_at AND "
            "orphan_grace_until <= orphaned_at + interval '1 hour 1 second')",
            name="ck_runtime_artifact_orphan_grace",
        ),
        sa.CheckConstraint(
            "content_expires_at > created_at AND "
            "content_expires_at <= created_at + interval '14 days 1 second'",
            name="ck_runtime_artifact_content_deadline",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_runtime_artifact_uploads_owner_run_status",
        "artifact_uploads",
        ["owner_id", "run_id", "status", "created_at"],
        schema=SCHEMA,
    )
    op.create_index(
        "ix_runtime_artifact_uploads_expiry",
        "artifact_uploads",
        ["content_expires_at"],
        schema=SCHEMA,
    )

    op.add_column(
        "accepted_results", sa.Column("artifact_id", sa.Uuid(), nullable=True), schema=SCHEMA
    )
    op.create_foreign_key(
        "fk_runtime_accepted_results_artifact",
        "accepted_results",
        "artifact_uploads",
        ["owner_id", "run_id", "artifact_id"],
        ["owner_id", "run_id", "upload_id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="RESTRICT",
    )

    op.execute(f"ALTER TABLE {SCHEMA}.artifact_capacity ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.artifact_capacity FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY runtime_artifact_capacity_singleton ON {SCHEMA}.artifact_capacity "
        f"FOR ALL TO {RUNTIME_ROLE} USING (capacity_id = 1) WITH CHECK (capacity_id = 1)"
    )
    op.execute(f"ALTER TABLE {SCHEMA}.artifact_uploads ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.artifact_uploads FORCE ROW LEVEL SECURITY")
    predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.artifact_uploads "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
    )

    op.execute(f"REVOKE ALL ON {SCHEMA}.artifact_capacity FROM PUBLIC")
    op.execute(f"REVOKE ALL ON {SCHEMA}.artifact_uploads FROM PUBLIC")
    op.execute(f"GRANT SELECT ON {SCHEMA}.artifact_capacity TO {RUNTIME_ROLE}")
    op.execute(f"GRANT UPDATE (charged_bytes) ON {SCHEMA}.artifact_capacity TO {RUNTIME_ROLE}")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.artifact_uploads TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE (status, object_bytes, updated_at, verified_at, committed_at, released_at, "
        f"orphan_grace_until, orphaned_at) ON {SCHEMA}.artifact_uploads TO {RUNTIME_ROLE}"
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_runtime_accepted_results_artifact",
        "accepted_results",
        schema=SCHEMA,
        type_="foreignkey",
    )
    op.drop_column("accepted_results", "artifact_id", schema=SCHEMA)
    op.drop_table("artifact_uploads", schema=SCHEMA)
    op.drop_table("artifact_capacity", schema=SCHEMA)
