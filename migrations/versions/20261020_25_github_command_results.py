"""Persist sanitized results for accepted owner GitHub commands.

Revision ID: 20261020_25
Revises: 20261020_24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261020_25"
down_revision = "20261020_24"
branch_labels = None
depends_on = None

SCHEMA = "runtime_private"
RUNTIME_ROLE = "continuum_runtime"


def upgrade() -> None:
    op.create_table(
        "github_status_comment_effects",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("event_sequence", sa.BigInteger(), nullable=False),
        sa.Column("repository_id", sa.BigInteger(), nullable=False),
        sa.Column("issue_number", sa.BigInteger(), nullable=False),
        sa.Column("status_snapshot", sa.String(32), nullable=False),
        sa.Column("revision", sa.BigInteger(), nullable=False),
        sa.Column("marker", sa.String(128), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("request_sha256", sa.LargeBinary(32), nullable=False),
        sa.Column("effect_status", sa.String(16), nullable=False, server_default="prepared"),
        sa.Column("dispatch_token", sa.Uuid()),
        sa.Column("dispatched_at", sa.DateTime(timezone=True)),
        sa.Column("remote_comment_id", sa.String(32)),
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
        sa.PrimaryKeyConstraint(
            "owner_id", "run_id", "event_sequence", name="pk_github_status_comment_effects"
        ),
        sa.UniqueConstraint(
            "owner_id",
            "repository_id",
            "issue_number",
            "marker",
            name="uq_github_status_comment_marker",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("event_sequence >= 0", name="ck_github_status_event_sequence"),
        sa.CheckConstraint("repository_id > 0", name="ck_github_status_repository"),
        sa.CheckConstraint("issue_number > 0", name="ck_github_status_issue"),
        sa.CheckConstraint("revision >= 0", name="ck_github_status_revision"),
        sa.CheckConstraint(
            "status_snapshot IN ('queued','running','waiting_approval','waiting_credentials',"
            "'waiting_capacity','needs_attention','succeeded','failed','cancelled','expired')",
            name="ck_github_status_run_state",
        ),
        sa.CheckConstraint(
            "effect_status IN ('prepared','dispatching','confirmed','unknown','conflict')",
            name="ck_github_status_effect_state",
        ),
        sa.CheckConstraint("octet_length(request_sha256)=32", name="ck_github_status_digest"),
        sa.CheckConstraint(
            "octet_length(marker)<=128 AND length(btrim(marker))>0 AND octet_length(body)<=2048",
            name="ck_github_status_content",
        ),
        sa.CheckConstraint(
            "(effect_status='prepared' AND dispatch_token IS NULL AND dispatched_at IS NULL "
            "AND remote_comment_id IS NULL) OR "
            "(effect_status='dispatching' AND dispatch_token IS NOT NULL "
            "AND dispatched_at IS NOT NULL AND remote_comment_id IS NULL) OR "
            "(effect_status='unknown' AND dispatch_token IS NOT NULL "
            "AND dispatched_at IS NOT NULL AND remote_comment_id IS NULL) OR "
            "(effect_status='confirmed' AND remote_comment_id IS NOT NULL) OR "
            "(effect_status='conflict' AND remote_comment_id IS NULL)",
            name="ck_github_status_effect_identity",
        ),
        sa.CheckConstraint(
            "remote_comment_id IS NULL OR remote_comment_id ~ '^[0-9]{1,20}$'",
            name="ck_github_status_remote_comment_id",
        ),
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.github_status_comment_effects ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.github_status_comment_effects FORCE ROW LEVEL SECURITY")
    owner = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.github_status_comment_effects "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({owner}) WITH CHECK ({owner})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.github_status_comment_effects FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.github_status_comment_effects TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE (effect_status, dispatch_token, dispatched_at, remote_comment_id, "
        f"updated_at) ON {SCHEMA}.github_status_comment_effects TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"CREATE FUNCTION {SCHEMA}.guard_github_status_comment_effect() RETURNS trigger "
        "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$ BEGIN "
        "IF ROW(NEW.owner_id, NEW.run_id, NEW.event_sequence, NEW.repository_id, "
        "NEW.issue_number, NEW.status_snapshot, NEW.revision, NEW.marker, NEW.body, "
        "NEW.request_sha256) IS DISTINCT FROM ROW(OLD.owner_id, OLD.run_id, "
        "OLD.event_sequence, OLD.repository_id, OLD.issue_number, OLD.status_snapshot, "
        "OLD.revision, OLD.marker, OLD.body, OLD.request_sha256) THEN "
        "RAISE EXCEPTION 'github_status_comment_intent_immutable'; END IF; "
        "IF NOT ((OLD.effect_status='prepared' AND "
        "NEW.effect_status IN ('dispatching','confirmed')) "
        "OR (OLD.effect_status='dispatching' AND NEW.effect_status IN ('unknown','confirmed')) "
        "OR (OLD.effect_status='unknown' AND NEW.effect_status IN ('confirmed','conflict'))) THEN "
        "RAISE EXCEPTION 'github_status_comment_state_transition_invalid'; END IF; "
        "RETURN NEW; END $$"
    )
    op.execute(
        f"CREATE TRIGGER github_status_comment_effect_guard BEFORE UPDATE ON "
        f"{SCHEMA}.github_status_comment_effects FOR EACH ROW EXECUTE FUNCTION "
        f"{SCHEMA}.guard_github_status_comment_effect()"
    )

    op.create_table(
        "github_command_admissions",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("repository_id", sa.BigInteger(), nullable=False),
        sa.Column("comment_id", sa.BigInteger(), nullable=False),
        sa.Column("issue_number", sa.BigInteger(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "owner_id",
            "repository_id",
            "comment_id",
            name="pk_github_command_admissions",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "repository_id", "comment_id"],
            [
                f"{SCHEMA}.github_command_receipts.owner_id",
                f"{SCHEMA}.github_command_receipts.repository_id",
                f"{SCHEMA}.github_command_receipts.comment_id",
            ],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("repository_id > 0", name="ck_github_command_admission_repo"),
        sa.CheckConstraint("comment_id > 0", name="ck_github_command_admission_comment"),
        sa.CheckConstraint("issue_number > 0", name="ck_github_command_admission_issue"),
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.github_command_admissions ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.github_command_admissions FORCE ROW LEVEL SECURITY")
    owner = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.github_command_admissions "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({owner}) WITH CHECK ({owner})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.github_command_admissions FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.github_command_admissions TO {RUNTIME_ROLE}")

    op.create_table(
        "github_command_results",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("repository_id", sa.BigInteger(), nullable=False),
        sa.Column("comment_id", sa.BigInteger(), nullable=False),
        sa.Column("result", JSONB(), nullable=False),
        sa.Column(
            "completed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "owner_id", "repository_id", "comment_id", name="pk_github_command_results"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "repository_id", "comment_id"],
            [
                f"{SCHEMA}.github_command_receipts.owner_id",
                f"{SCHEMA}.github_command_receipts.repository_id",
                f"{SCHEMA}.github_command_receipts.comment_id",
            ],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(result)='object' AND result ? 'kind' "
            "AND octet_length(result::text)<=2048",
            name="ck_github_command_result",
        ),
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.github_command_results ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.github_command_results FORCE ROW LEVEL SECURITY")
    owner = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.github_command_results "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({owner}) WITH CHECK ({owner})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.github_command_results FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.github_command_results TO {RUNTIME_ROLE}")


def downgrade() -> None:
    op.drop_table("github_command_results", schema=SCHEMA)
    op.drop_table("github_command_admissions", schema=SCHEMA)
    op.execute(
        f"DROP TRIGGER github_status_comment_effect_guard ON {SCHEMA}.github_status_comment_effects"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.guard_github_status_comment_effect()")
    op.drop_table("github_status_comment_effects", schema=SCHEMA)
