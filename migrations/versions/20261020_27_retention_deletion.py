"""Add content deadlines and durable deletion/cleanup journals.

Revision ID: 20261020_27
Revises: 20261020_26
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261020_27"
down_revision = "20261020_26"
branch_labels = None
depends_on = None

SCHEMA = "runtime_private"
RUNTIME_ROLE = "continuum_runtime"


def _owner_policy(table: str) -> None:
    owner = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(f"ALTER TABLE {SCHEMA}.{table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.{table} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.{table} "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({owner}) WITH CHECK ({owner})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.{table} FROM PUBLIC")


def upgrade() -> None:
    op.add_column(
        "runs",
        sa.Column(
            "content_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
            server_default=sa.text("clock_timestamp() + interval '14 days'"),
        ),
        schema=SCHEMA,
    )
    op.add_column(
        "runs",
        sa.Column("content_deleted_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "runs",
        sa.Column("content_scrubbed_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.execute(
        f"""
        UPDATE {SCHEMA}.runs AS r SET content_expires_at = CASE
          WHEN r.status IN ('succeeded','failed','cancelled','expired') THEN
            COALESCE((SELECT max(e.created_at) FROM {SCHEMA}.events AS e
                      WHERE e.owner_id=r.owner_id AND e.run_id=r.run_id), r.created_at)
            + interval '7 days'
          ELSE r.created_at + interval '14 days'
        END
        """
    )
    op.alter_column("runs", "content_expires_at", nullable=False, schema=SCHEMA)
    op.create_check_constraint(
        "ck_runtime_runs_content_expiry",
        "runs",
        "content_expires_at > created_at",
        schema=SCHEMA,
    )
    op.create_index(
        "ix_runtime_runs_content_expiry",
        "runs",
        ["content_expires_at", "run_id"],
        schema=SCHEMA,
    )

    # Terminal time is authoritative for seven-day retention. The first
    # transition to a terminal state starts the deadline exactly once.
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.set_run_content_expiry()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
        BEGIN
          IF OLD.status NOT IN ('succeeded','failed','cancelled','expired')
             AND NEW.status IN ('succeeded','failed','cancelled','expired')
             AND NEW.content_deleted_at IS NULL THEN
            NEW.content_expires_at := clock_timestamp() + interval '7 days';
          END IF;
          RETURN NEW;
        END $$
        """
    )
    op.execute(
        f"CREATE TRIGGER trg_runtime_run_content_expiry BEFORE UPDATE OF status "
        f"ON {SCHEMA}.runs FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.set_run_content_expiry()"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.set_run_content_expiry() FROM PUBLIC")
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.delete_terminal_run_credentials()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          IF OLD.status NOT IN ('succeeded','failed','cancelled','expired')
             AND NEW.status IN ('succeeded','failed','cancelled','expired') THEN
            UPDATE runtime_private.provider_credentials SET status='deleted', nonce=NULL,
              ciphertext=NULL, ended_at=COALESCE(ended_at, clock_timestamp())
            WHERE owner_id=NEW.owner_id AND scope='run' AND scope_run_id=NEW.run_id
              AND status='active';
          END IF;
          RETURN NEW;
        END $$
        """
    )
    op.execute(
        f"CREATE TRIGGER trg_runtime_delete_terminal_run_credentials "
        f"AFTER UPDATE OF status ON {SCHEMA}.runs FOR EACH ROW "
        f"EXECUTE FUNCTION {SCHEMA}.delete_terminal_run_credentials()"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.delete_terminal_run_credentials() FROM PUBLIC")

    op.create_table(
        "retention_receipts",
        sa.Column("receipt_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("scope", sa.String(8), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("opaque_record_ids", JSONB(), nullable=False, server_default="[]"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column(
            "expires_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp() + interval '90 days'"),
        ),
        sa.PrimaryKeyConstraint("receipt_id", name="pk_runtime_retention_receipts"),
        sa.UniqueConstraint(
            "scope",
            "owner_id",
            "subject_id",
            "action",
            name="uq_runtime_retention_receipt_subject_action",
        ),
        sa.CheckConstraint("scope IN ('run','account')", name="ck_retention_receipt_scope"),
        sa.CheckConstraint("action IN ('delete','expire')", name="ck_retention_receipt_action"),
        sa.CheckConstraint(
            "jsonb_typeof(opaque_record_ids)='array' AND "
            "octet_length(opaque_record_ids::text)<=4096",
            name="ck_retention_receipt_ids",
        ),
        sa.CheckConstraint(
            "expires_at >= created_at + interval '90 days' AND "
            "expires_at < created_at + interval '90 days' + interval '1 second'",
            name="ck_retention_receipt_expiry",
        ),
        schema=SCHEMA,
    )
    _owner_policy("retention_receipts")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.retention_receipts TO {RUNTIME_ROLE}")
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.reject_retention_receipt_mutation()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
        BEGIN
          RAISE EXCEPTION 'retention_receipts_are_append_only';
        END $$
    """)
    op.execute(
        f"CREATE TRIGGER retention_receipts_append_only BEFORE UPDATE OR DELETE "
        f"ON {SCHEMA}.retention_receipts FOR EACH ROW "
        f"EXECUTE FUNCTION {SCHEMA}.reject_retention_receipt_mutation()"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.reject_retention_receipt_mutation() FROM PUBLIC")

    # T40's bounded scrubber removes content in place while preserving the
    # opaque identities and digests needed for audit and replay prevention.
    for table, columns in {
        "run_admission_pins": "task_context, issue_snapshot, source_receipt",
        "agent_turns": "context, tool_schemas, response",
        "agent_invocations": "arguments",
        "events": "payload",
        "accepted_results": "payload",
        "approvals": "manifest",
        "revision_feedback": "feedback",
        "github_command_payloads": "payload",
    }.items():
        op.execute(f"GRANT UPDATE ({columns}) ON {SCHEMA}.{table} TO {RUNTIME_ROLE}")

    op.create_table(
        "retention_cleanup_items",
        sa.Column("cleanup_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("scope", sa.String(8), nullable=False),
        sa.Column("sink", sa.String(16), nullable=False),
        sa.Column("provider", sa.String(16), nullable=True),
        sa.Column("reference_id", sa.String(256), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error_code", sa.String(64), nullable=True),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("cleanup_id", name="pk_runtime_retention_cleanup_items"),
        sa.UniqueConstraint(
            "owner_id",
            "scope",
            "subject_id",
            "sink",
            "reference_id",
            name="uq_retention_cleanup_identity",
        ),
        sa.CheckConstraint("scope IN ('run','account')", name="ck_retention_cleanup_scope"),
        sa.CheckConstraint(
            "sink IN ('object','provider','sandbox')", name="ck_retention_cleanup_sink"
        ),
        sa.CheckConstraint(
            "provider IS NULL OR provider IN ('openai','anthropic')",
            name="ck_retention_cleanup_provider",
        ),
        sa.CheckConstraint(
            "status IN ('pending','deleted','unsupported','unknown')",
            name="ck_retention_cleanup_status",
        ),
        sa.CheckConstraint("attempts BETWEEN 0 AND 1000000", name="ck_retention_cleanup_attempts"),
        sa.CheckConstraint(
            "length(btrim(reference_id)) BETWEEN 1 AND 256", name="ck_retention_cleanup_reference"
        ),
        schema=SCHEMA,
    )
    _owner_policy("retention_cleanup_items")
    op.create_index(
        "ix_retention_cleanup_due",
        "retention_cleanup_items",
        ["status", "next_attempt_at", "cleanup_id"],
        schema=SCHEMA,
        postgresql_where=sa.text("status IN ('pending','unknown')"),
    )
    op.execute(
        f"GRANT SELECT, INSERT, UPDATE ON {SCHEMA}.retention_cleanup_items TO {RUNTIME_ROLE}"
    )

    # Keep account status updates behind a fixed, non-overridable procedure;
    # auth tables remain inaccessible to the runtime role.
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.retention_begin_account_deletion(p_owner uuid)
        RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          IF p_owner IS NULL THEN RETURN false; END IF;
          UPDATE runtime_private.users SET status='deleting' WHERE user_id=p_owner
            AND status IN ('active','suspended','deleting');
          IF NOT FOUND THEN
            RETURN EXISTS (SELECT 1 FROM runtime_private.users
                           WHERE user_id=p_owner AND status='deleted');
          END IF;
          PERFORM runtime_private.auth_revoke_account(p_owner);
          RETURN true;
        END $$
    """)
    op.execute(
        f"REVOKE ALL ON FUNCTION {SCHEMA}.retention_begin_account_deletion(uuid) FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.retention_begin_account_deletion(uuid) "
        f"TO {RUNTIME_ROLE}"
    )

    # Raw deliveries have no account ownership link before normalization.
    # Purge globally at the documented body deadlines without widening direct
    # access to the ingress schema.
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.retention_scrub_webhook_bodies(p_limit integer)
        RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE v_count integer;
        BEGIN
          IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
            RAISE EXCEPTION 'retention_batch_limit_invalid' USING ERRCODE='22023';
          END IF;
          WITH due AS (
            SELECT github_app_id, delivery_id
            FROM webhook_private.github_delivery_inbox
            WHERE octet_length(raw_body)>0
              AND ((consumed_at IS NOT NULL AND
                    consumed_at <= clock_timestamp()-interval '24 hours')
                   OR received_at <= clock_timestamp()-interval '72 hours')
            ORDER BY received_at, github_app_id, delivery_id
            FOR UPDATE SKIP LOCKED LIMIT p_limit
          )
          UPDATE webhook_private.github_delivery_inbox AS inbox SET raw_body=''::bytea
          FROM due WHERE inbox.github_app_id=due.github_app_id
            AND inbox.delivery_id=due.delivery_id;
          GET DIAGNOSTICS v_count = ROW_COUNT;
          RETURN v_count;
        END $$
    """)
    op.execute(
        f"REVOKE ALL ON FUNCTION {SCHEMA}.retention_scrub_webhook_bodies(integer) FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.retention_scrub_webhook_bodies(integer) "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.retention_due_owners(p_limit integer)
        RETURNS TABLE(owner_id uuid)
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
            RAISE EXCEPTION 'retention_batch_limit_invalid' USING ERRCODE='22023';
          END IF;
          RETURN QUERY
          SELECT due.owner_id FROM (
            SELECT u.user_id AS owner_id FROM runtime_private.users AS u
              WHERE u.status='deleting'
            UNION
            SELECT r.owner_id FROM runtime_private.runs AS r
              WHERE r.content_deleted_at IS NULL AND (
                r.content_expires_at<=clock_timestamp() OR (
                  r.status IN ('queued','running','waiting_approval','waiting_credentials',
                               'waiting_capacity','needs_attention')
                  AND r.deadline_at<=clock_timestamp()
                )
              )
            UNION
            SELECT i.owner_id FROM runtime_private.retention_cleanup_items AS i
              WHERE i.status IN ('pending','unknown') AND i.next_attempt_at<=clock_timestamp()
          ) AS due ORDER BY due.owner_id LIMIT p_limit;
        END $$
    """)
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.retention_due_owners(integer) FROM PUBLIC")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.retention_due_owners(integer) TO {RUNTIME_ROLE}"
    )
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.retention_finalize_account_deletion(p_owner uuid)
        RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          IF p_owner IS NULL OR EXISTS (
            SELECT 1 FROM runtime_private.runs WHERE owner_id=p_owner
              AND content_scrubbed_at IS NULL
          ) OR EXISTS (
            SELECT 1 FROM runtime_private.retention_cleanup_items WHERE owner_id=p_owner
              AND status IN ('pending','unknown')
          ) THEN
            RETURN false;
          END IF;
          UPDATE runtime_private.users SET status='deleted'
            WHERE user_id=p_owner AND status='deleting';
          RETURN FOUND OR EXISTS (
            SELECT 1 FROM runtime_private.users WHERE user_id=p_owner AND status='deleted'
          );
        END $$
    """)
    op.execute(
        f"REVOKE ALL ON FUNCTION {SCHEMA}.retention_finalize_account_deletion(uuid) FROM PUBLIC"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.retention_finalize_account_deletion(uuid) "
        f"TO {RUNTIME_ROLE}"
    )


def downgrade() -> None:
    op.execute(f"DROP FUNCTION {SCHEMA}.retention_finalize_account_deletion(uuid)")
    op.execute(f"DROP FUNCTION {SCHEMA}.retention_due_owners(integer)")
    op.execute(f"DROP FUNCTION {SCHEMA}.retention_begin_account_deletion(uuid)")
    op.execute(f"DROP FUNCTION {SCHEMA}.retention_scrub_webhook_bodies(integer)")
    op.execute(f"DROP TRIGGER retention_receipts_append_only ON {SCHEMA}.retention_receipts")
    op.execute(f"DROP FUNCTION {SCHEMA}.reject_retention_receipt_mutation()")
    op.execute(f"DROP TRIGGER trg_runtime_delete_terminal_run_credentials ON {SCHEMA}.runs")
    op.execute(f"DROP FUNCTION {SCHEMA}.delete_terminal_run_credentials()")
    op.execute(f"DROP TRIGGER trg_runtime_run_content_expiry ON {SCHEMA}.runs")
    op.execute(f"DROP FUNCTION {SCHEMA}.set_run_content_expiry()")
    op.drop_table("retention_cleanup_items", schema=SCHEMA)
    op.drop_table("retention_receipts", schema=SCHEMA)
    op.drop_index("ix_runtime_runs_content_expiry", table_name="runs", schema=SCHEMA)
    op.drop_constraint("ck_runtime_runs_content_expiry", "runs", schema=SCHEMA, type_="check")
    op.drop_column("runs", "content_deleted_at", schema=SCHEMA)
    op.drop_column("runs", "content_scrubbed_at", schema=SCHEMA)
    op.drop_column("runs", "content_expires_at", schema=SCHEMA)
