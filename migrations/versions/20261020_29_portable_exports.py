"""Journal encrypted portable exports and deletion invalidations.

Revision ID: 20261020_29
Revises: 20261020_28
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261020_29"
down_revision = "20261020_28"
branch_labels = None
depends_on = None

SCHEMA = "operator_private"
RUNTIME_ROLE = "continuum_runtime"
OPERATOR_ROLE = "continuum_operator"


def upgrade() -> None:
    op.create_table(
        "portable_exports",
        sa.Column("export_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("included_run_ids", JSONB(), nullable=False),
        sa.Column("archive_name", sa.String(64), nullable=False),
        sa.Column("manifest_sha256", sa.LargeBinary(32), nullable=False),
        sa.Column("archive_sha256", sa.LargeBinary(32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("export_id", name="pk_operator_portable_exports"),
        sa.CheckConstraint(
            "jsonb_typeof(included_run_ids)='array' AND jsonb_array_length(included_run_ids)<=100",
            name="ck_operator_portable_export_runs",
        ),
        sa.CheckConstraint(
            "archive_name ~ '^[0-9a-f-]{36}\\.cpx$'",
            name="ck_operator_portable_export_name",
        ),
        sa.CheckConstraint(
            "octet_length(manifest_sha256)=32", name="ck_portable_export_manifest_hash"
        ),
        sa.CheckConstraint(
            "octet_length(archive_sha256)=32", name="ck_portable_export_archive_hash"
        ),
        sa.CheckConstraint(
            "expires_at>created_at AND expires_at<=created_at+interval '7 days'",
            name="ck_operator_portable_export_expiry",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_operator_portable_exports_owner_expiry",
        "portable_exports",
        ["owner_id", "expires_at"],
        schema=SCHEMA,
    )
    op.create_table(
        "portable_export_invalidations",
        sa.Column("invalidation_id", sa.Uuid(), nullable=False),
        sa.Column("export_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=True),
        sa.Column("reason", sa.String(20), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("invalidation_id", name="pk_operator_export_invalidations"),
        sa.UniqueConstraint(
            "export_id", "run_id", "reason", name="uq_portable_export_invalidation"
        ),
        sa.ForeignKeyConstraint(
            ["export_id"], [f"{SCHEMA}.portable_exports.export_id"], ondelete="RESTRICT"
        ),
        sa.CheckConstraint(
            "reason IN ('account_deleted','run_deleted','content_expired')",
            name="ck_portable_export_invalidation_reason",
        ),
        sa.CheckConstraint(
            "(reason='account_deleted' AND run_id IS NULL) OR "
            "(reason IN ('run_deleted','content_expired') AND run_id IS NOT NULL)",
            name="ck_portable_export_invalidation_scope",
        ),
        schema=SCHEMA,
    )
    op.execute(
        f"REVOKE ALL ON {SCHEMA}.portable_exports, "
        f"{SCHEMA}.portable_export_invalidations FROM PUBLIC"
    )
    op.execute(
        f"GRANT SELECT ON {SCHEMA}.portable_exports, {SCHEMA}.portable_export_invalidations "
        f"TO {OPERATOR_ROLE}"
    )

    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.register_portable_export(
          p_export_id uuid, p_owner_id uuid, p_run_ids jsonb, p_archive_name text,
          p_manifest_sha256 bytea, p_archive_sha256 bytea, p_expires_at timestamptz
        ) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE v_now timestamptz; v_earliest timestamptz; v_user uuid; v_run record;
        BEGIN
          PERFORM pg_advisory_xact_lock(72414020291029);
          v_now := clock_timestamp();
          IF p_export_id IS NULL OR p_owner_id IS NULL OR
             NULLIF(current_setting('continuum.principal_id', true), '')::uuid
               IS DISTINCT FROM p_owner_id OR
             p_run_ids IS NULL OR jsonb_typeof(p_run_ids)<>'array' OR
             jsonb_array_length(p_run_ids)>100 OR
             p_archive_name !~ '^[0-9a-f-]{{36}}[.]cpx$' OR
             p_manifest_sha256 IS NULL OR octet_length(p_manifest_sha256)<>32 OR
             p_archive_sha256 IS NULL OR octet_length(p_archive_sha256)<>32 OR
             p_expires_at<=v_now OR p_expires_at>v_now+interval '7 days' THEN
            RAISE EXCEPTION 'portable_export_input_invalid' USING ERRCODE='22023';
          END IF;
          SELECT user_id INTO v_user FROM runtime_private.users
            WHERE user_id=p_owner_id AND status='active' FOR SHARE;
          IF v_user IS NULL THEN
            RAISE EXCEPTION 'portable_export_owner_unavailable' USING ERRCODE='23514';
          END IF;
          IF EXISTS (
            SELECT 1 FROM {SCHEMA}.portable_exports AS export
            WHERE export.expires_at>v_now AND NOT EXISTS (
              SELECT 1 FROM {SCHEMA}.portable_export_invalidations AS invalidation
              WHERE invalidation.export_id=export.export_id
            )
          ) AND (SELECT count(*) FROM {SCHEMA}.portable_exports AS export
            WHERE export.expires_at>v_now AND NOT EXISTS (
              SELECT 1 FROM {SCHEMA}.portable_export_invalidations AS invalidation
              WHERE invalidation.export_id=export.export_id
            ))>=2 THEN
            RAISE EXCEPTION 'portable_export_count_limit' USING ERRCODE='23514';
          END IF;
          IF jsonb_array_length(p_run_ids)>0 THEN
            FOR v_run IN SELECT value::uuid AS run_id
              FROM jsonb_array_elements_text(p_run_ids) AS selected(value)
            LOOP
              SELECT run_id,content_expires_at,deadline_at,content_deleted_at
                INTO v_run FROM runtime_private.runs
                WHERE run_id=v_run.run_id AND owner_id=p_owner_id FOR SHARE;
              IF NOT FOUND OR v_run.content_deleted_at IS NOT NULL OR
                 v_run.content_expires_at<=v_now OR
                 (v_run.deadline_at IS NOT NULL AND v_run.deadline_at<=v_now) THEN
                RAISE EXCEPTION 'portable_export_content_unavailable' USING ERRCODE='23514';
              END IF;
              v_earliest := LEAST(COALESCE(v_earliest,v_run.content_expires_at),
                                  v_run.content_expires_at);
            END LOOP;
            IF p_expires_at>v_earliest THEN
              RAISE EXCEPTION 'portable_export_expiry_too_late' USING ERRCODE='23514';
            END IF;
          END IF;
          INSERT INTO {SCHEMA}.portable_exports(export_id,owner_id,included_run_ids,archive_name,
            manifest_sha256,archive_sha256,created_at,expires_at)
          VALUES (p_export_id,p_owner_id,p_run_ids,p_archive_name,p_manifest_sha256,
            p_archive_sha256,v_now,p_expires_at);
        END $$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.portable_export_status(p_export_id uuid, p_manifest_sha256 bytea)
        RETURNS TABLE(export_id uuid, owner_id uuid, included_run_ids jsonb, expires_at timestamptz,
                       invalidated_run_ids jsonb, invalidated boolean)
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          RETURN QUERY
          SELECT export.export_id, export.owner_id, export.included_run_ids, export.expires_at,
            COALESCE((SELECT jsonb_agg(DISTINCT invalidation.run_id::text)
              FROM {SCHEMA}.portable_export_invalidations AS invalidation
              WHERE invalidation.export_id=export.export_id AND invalidation.run_id IS NOT NULL),
              '[]'::jsonb),
            EXISTS(SELECT 1 FROM {SCHEMA}.portable_export_invalidations AS invalidation
              WHERE invalidation.export_id=export.export_id)
          FROM {SCHEMA}.portable_exports AS export
          WHERE export.export_id=p_export_id AND export.manifest_sha256=p_manifest_sha256
            AND export.owner_id=NULLIF(current_setting('continuum.principal_id', true), '')::uuid;
        END $$
        """
    )

    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.record_retention_export_invalidation()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          IF NEW.action='delete' AND NEW.scope='account' THEN
            INSERT INTO {SCHEMA}.portable_export_invalidations
              (invalidation_id,export_id,owner_id,run_id,reason)
            SELECT gen_random_uuid(),export.export_id,NEW.owner_id,NULL,'account_deleted'
            FROM {SCHEMA}.portable_exports AS export
            WHERE export.owner_id=NEW.owner_id
              AND NOT EXISTS(
                SELECT 1 FROM {SCHEMA}.portable_export_invalidations AS prior_invalidation
                WHERE prior_invalidation.export_id=export.export_id
                  AND prior_invalidation.run_id IS NULL);
          ELSIF NEW.scope='run' THEN
            INSERT INTO {SCHEMA}.portable_export_invalidations
              (invalidation_id,export_id,owner_id,run_id,reason)
            SELECT gen_random_uuid(),export.export_id,NEW.owner_id,NEW.subject_id,
              CASE WHEN NEW.action='expire' THEN 'content_expired' ELSE 'run_deleted' END
            FROM {SCHEMA}.portable_exports AS export
            WHERE export.owner_id=NEW.owner_id
              AND export.included_run_ids ? (NEW.subject_id::text)
              AND NOT EXISTS(
                SELECT 1 FROM {SCHEMA}.portable_export_invalidations AS prior_invalidation
                WHERE prior_invalidation.export_id=export.export_id
                  AND prior_invalidation.run_id=NEW.subject_id);
          END IF;
          RETURN NEW;
        END $$
        """
    )
    op.execute(
        f"CREATE TRIGGER record_portable_export_invalidation AFTER INSERT "
        f"ON runtime_private.retention_receipts FOR EACH ROW "
        f"EXECUTE FUNCTION {SCHEMA}.record_retention_export_invalidation()"
    )
    for signature in (
        "register_portable_export(uuid,uuid,jsonb,text,bytea,bytea,timestamptz)",
        "portable_export_status(uuid,bytea)",
    ):
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC")
        op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{signature} TO {RUNTIME_ROLE}")
    op.execute(
        f"REVOKE ALL ON FUNCTION {SCHEMA}.record_retention_export_invalidation() FROM PUBLIC"
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER record_portable_export_invalidation ON runtime_private.retention_receipts"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.record_retention_export_invalidation()")
    op.execute(f"DROP FUNCTION {SCHEMA}.portable_export_status(uuid,bytea)")
    op.execute(
        f"DROP FUNCTION {SCHEMA}.register_portable_export("
        "uuid,uuid,jsonb,text,bytea,bytea,timestamptz)"
    )
    op.drop_table("portable_export_invalidations", schema=SCHEMA)
    op.drop_table("portable_exports", schema=SCHEMA)
