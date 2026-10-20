"""Persist short-lived normalized GitHub commands and dedup tombstones.

Revision ID: 20261020_24
Revises: 20261019_23
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261020_24"
down_revision = "20261019_23"
branch_labels = None
depends_on = None

SCHEMA = "runtime_private"
RUNTIME_ROLE = "continuum_runtime"


def upgrade() -> None:
    op.create_table(
        "github_command_receipts",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("repository_id", sa.BigInteger(), nullable=False),
        sa.Column("comment_id", sa.BigInteger(), nullable=False),
        sa.Column("issue_id", sa.BigInteger(), nullable=False),
        sa.Column("issue_number", sa.BigInteger(), nullable=False),
        sa.Column("author_id", sa.BigInteger(), nullable=False),
        sa.Column("comment_created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("command_kind", sa.String(16), nullable=False),
        sa.Column("command_metadata", JSONB(), nullable=False),
        sa.Column("command_sha256", sa.LargeBinary(32), nullable=False),
        sa.Column(
            "accepted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "owner_id", "repository_id", "comment_id", name="pk_github_command_receipts"
        ),
        sa.UniqueConstraint("repository_id", "comment_id", name="uq_github_command_comment"),
        sa.UniqueConstraint(
            "owner_id",
            "repository_id",
            "comment_id",
            "accepted_at",
            name="uq_github_command_comment_acceptance",
        ),
        sa.ForeignKeyConstraint(["owner_id"], [f"{SCHEMA}.users.user_id"], ondelete="CASCADE"),
        sa.CheckConstraint("repository_id > 0", name="ck_github_command_repository"),
        sa.CheckConstraint("comment_id > 0", name="ck_github_command_comment"),
        sa.CheckConstraint("issue_id > 0", name="ck_github_command_issue_id"),
        sa.CheckConstraint("issue_number > 0", name="ck_github_command_issue_number"),
        sa.CheckConstraint("author_id > 0", name="ck_github_command_author"),
        sa.CheckConstraint(
            "command_kind IN ('run','status','cancel','approve','reject','changes')",
            name="ck_github_command_kind",
        ),
        sa.CheckConstraint("octet_length(command_sha256)=32", name="ck_github_command_digest"),
        sa.CheckConstraint(
            "jsonb_typeof(command_metadata)='object' AND "
            "command_metadata->>'kind'=command_kind AND "
            "octet_length(command_metadata::text)<=1024",
            name="ck_github_command_metadata",
        ),
        sa.CheckConstraint(
            "expires_at = comment_created_at + interval '90 days'",
            name="ck_github_command_expiry",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_github_command_receipts_owner_expiry",
        "github_command_receipts",
        ["owner_id", "expires_at"],
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.github_command_receipts ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.github_command_receipts FORCE ROW LEVEL SECURITY")
    owner = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.github_command_receipts "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({owner}) WITH CHECK ({owner})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.github_command_receipts FROM PUBLIC")
    op.execute(
        f"GRANT SELECT, INSERT, DELETE ON {SCHEMA}.github_command_receipts TO {RUNTIME_ROLE}"
    )

    op.create_table(
        "github_command_payloads",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("repository_id", sa.BigInteger(), nullable=False),
        sa.Column("comment_id", sa.BigInteger(), nullable=False),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "owner_id", "repository_id", "comment_id", name="pk_github_command_payloads"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "repository_id", "comment_id", "accepted_at"],
            [
                f"{SCHEMA}.github_command_receipts.owner_id",
                f"{SCHEMA}.github_command_receipts.repository_id",
                f"{SCHEMA}.github_command_receipts.comment_id",
                f"{SCHEMA}.github_command_receipts.accepted_at",
            ],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload)='object' AND payload ? 'feedback' AND "
            # JSON escaping can expand a bounded 16 KiB body by up to six times.
            "octet_length(payload::text)<=98432",
            name="ck_github_command_payload",
        ),
        sa.CheckConstraint(
            "expires_at = accepted_at + interval '72 hours'",
            name="ck_github_command_payload_expiry",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_github_command_payloads_expiry",
        "github_command_payloads",
        ["owner_id", "expires_at"],
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.github_command_payloads ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.github_command_payloads FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.github_command_payloads "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({owner}) WITH CHECK ({owner})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.github_command_payloads FROM PUBLIC")
    op.execute(
        f"GRANT SELECT, INSERT, DELETE ON {SCHEMA}.github_command_payloads TO {RUNTIME_ROLE}"
    )


def downgrade() -> None:
    op.drop_table("github_command_payloads", schema=SCHEMA)
    op.drop_table("github_command_receipts", schema=SCHEMA)
