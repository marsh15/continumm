"""Persist repository preflight reservations and bounded eligibility receipts.

Revision ID: 20261012_08
Revises: 20261011_07
Create Date: 2026-10-12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "20261012_08"
down_revision: str | None = "20261011_07"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

RUNTIME_ROLE = "continuum_runtime"
SCHEMA = "runtime_private"


def upgrade() -> None:
    op.create_table(
        "repository_preflight_checks",
        sa.Column("check_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("github_repository_id", sa.BigInteger(), nullable=False),
        sa.Column("commit_sha", sa.String(length=64), nullable=False),
        sa.Column("profile_id", sa.String(length=64), nullable=False),
        sa.Column("profile_version", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="active"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("check_id", name="pk_runtime_repository_preflight_checks"),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            [f"{SCHEMA}.users.user_id"],
            name="fk_runtime_repository_preflight_checks_owner",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("github_repository_id > 0", name="ck_runtime_preflight_repo_id"),
        sa.CheckConstraint(
            "commit_sha ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'",
            name="ck_runtime_preflight_commit_sha",
        ),
        sa.CheckConstraint(
            "length(btrim(profile_id)) BETWEEN 1 AND 64", name="ck_runtime_preflight_profile_id"
        ),
        sa.CheckConstraint(
            "length(btrim(profile_version)) BETWEEN 1 AND 64",
            name="ck_runtime_preflight_profile_version",
        ),
        sa.CheckConstraint(
            "status IN ('active', 'completed', 'failed', 'expired')",
            name="ck_runtime_preflight_status",
        ),
        sa.CheckConstraint(
            "deadline_at > created_at", name="ck_runtime_preflight_deadline_after_creation"
        ),
        sa.CheckConstraint(
            "(status = 'active' AND finished_at IS NULL) OR "
            "(status <> 'active' AND finished_at IS NOT NULL)",
            name="ck_runtime_preflight_finished_at",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "uq_runtime_preflight_one_active",
        "repository_preflight_checks",
        ["owner_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("status = 'active'"),
    )
    op.create_index(
        "ix_runtime_preflight_owner_created",
        "repository_preflight_checks",
        ["owner_id", "created_at"],
        schema=SCHEMA,
    )

    op.create_table(
        "repository_eligibility_cache",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("github_repository_id", sa.BigInteger(), nullable=False),
        sa.Column("commit_sha", sa.String(length=64), nullable=False),
        sa.Column("profile_id", sa.String(length=64), nullable=False),
        sa.Column("profile_version", sa.String(length=64), nullable=False),
        sa.Column("receipt", JSONB(), nullable=False),
        sa.Column(
            "cached_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "owner_id",
            "github_repository_id",
            "commit_sha",
            "profile_id",
            "profile_version",
            name="pk_runtime_repository_eligibility_cache",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id"],
            [f"{SCHEMA}.users.user_id"],
            name="fk_runtime_repository_eligibility_cache_owner",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("github_repository_id > 0", name="ck_runtime_eligibility_cache_repo_id"),
        sa.CheckConstraint(
            "commit_sha ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'",
            name="ck_runtime_eligibility_cache_commit_sha",
        ),
        sa.CheckConstraint(
            "length(btrim(profile_id)) BETWEEN 1 AND 64",
            name="ck_runtime_eligibility_cache_profile_id",
        ),
        sa.CheckConstraint(
            "length(btrim(profile_version)) BETWEEN 1 AND 64",
            name="ck_runtime_eligibility_cache_profile_version",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(receipt) = 'object' AND octet_length(receipt::text) <= 4096",
            name="ck_runtime_eligibility_cache_receipt_limit",
        ),
        sa.CheckConstraint(
            "expires_at > cached_at AND expires_at <= cached_at + interval '10 minutes 1 second'",
            name="ck_runtime_eligibility_cache_expiry",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_runtime_repository_eligibility_cache_expiry",
        "repository_eligibility_cache",
        ["expires_at"],
        schema=SCHEMA,
    )

    predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    for table_name in ("repository_preflight_checks", "repository_eligibility_cache"):
        op.execute(f"ALTER TABLE {SCHEMA}.{table_name} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {SCHEMA}.{table_name} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.{table_name} "
            f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
        )

    op.execute(f"REVOKE ALL ON {SCHEMA}.repository_preflight_checks FROM PUBLIC")
    op.execute(f"REVOKE ALL ON {SCHEMA}.repository_eligibility_cache FROM PUBLIC")
    op.execute(
        f"GRANT SELECT, INSERT, DELETE ON {SCHEMA}.repository_preflight_checks TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT UPDATE (status, finished_at) "
        f"ON {SCHEMA}.repository_preflight_checks TO {RUNTIME_ROLE}"
    )
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.repository_eligibility_cache TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE (receipt, cached_at, expires_at) "
        f"ON {SCHEMA}.repository_eligibility_cache TO {RUNTIME_ROLE}"
    )
    op.execute(f"GRANT DELETE ON {SCHEMA}.repository_eligibility_cache TO {RUNTIME_ROLE}")


def downgrade() -> None:
    op.drop_table("repository_eligibility_cache", schema=SCHEMA)
    op.drop_index(
        "ix_runtime_preflight_owner_created",
        table_name="repository_preflight_checks",
        schema=SCHEMA,
    )
    op.drop_index(
        "uq_runtime_preflight_one_active",
        table_name="repository_preflight_checks",
        schema=SCHEMA,
    )
    op.drop_table("repository_preflight_checks", schema=SCHEMA)
