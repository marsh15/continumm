"""Add bounded GitHub App failed-delivery recovery state.

Revision ID: 20261020_26
Revises: 20261020_25
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20261020_26"
down_revision = "20261020_25"
branch_labels = None
depends_on = None

SCHEMA = "recovery_private"
EDGE_ROLE = "continuum_recovery"
CRON_ROLE = "continuum_wake_issuer"


def upgrade() -> None:
    bind = op.get_bind()
    recovery_role = bind.execute(
        sa.text(
            """
            SELECT r.rolcanlogin,
                   r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR
                     r.rolreplication OR r.rolbypassrls AS elevated,
                   r.rolinherit,
                   EXISTS (SELECT 1 FROM pg_auth_members m
                           WHERE m.member = r.oid OR m.roleid = r.oid) AS has_membership
            FROM pg_roles r WHERE r.rolname = :role_name
            """
        ),
        {"role_name": EDGE_ROLE},
    ).one_or_none()
    if recovery_role is None:
        raise RuntimeError("create_continuum_recovery_role_before_migration")
    if (
        not recovery_role.rolcanlogin
        or recovery_role.elevated
        or recovery_role.rolinherit
        or recovery_role.has_membership
    ):
        raise RuntimeError("continuum_recovery_role_privileges_invalid")
    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"GRANT CONNECT ON DATABASE {quoted_database} TO {EDGE_ROLE}")
    op.execute(f"CREATE SCHEMA {SCHEMA}")
    op.execute(f"REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {EDGE_ROLE}, {CRON_ROLE}")
    op.create_table(
        "scan_state",
        sa.Column("singleton", sa.Boolean(), primary_key=True, server_default=sa.true()),
        sa.Column("watermark_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("scan_cursor", sa.Text(), nullable=True),
        sa.Column("pending_watermark_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_scan_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("history_gap_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("singleton", name="ck_recovery_scan_singleton"),
        schema=SCHEMA,
    )
    op.create_table(
        "scan_ticket",
        sa.Column("ticket_id", sa.Uuid(), primary_key=True),
        sa.Column("ticket_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("octet_length(ticket_sha256)=32", name="ck_recovery_ticket_digest"),
        schema=SCHEMA,
    )
    op.create_index("ix_recovery_ticket_expiry", "scan_ticket", ["expires_at"], schema=SCHEMA)
    op.create_table(
        "delivery_redelivery",
        sa.Column("delivery_guid", sa.Uuid(), primary_key=True),
        sa.Column("delivery_id", sa.BigInteger(), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("outcome", sa.String(length=20), nullable=False, server_default="claimed"),
        sa.Column("http_status", sa.SmallInteger(), nullable=True),
        sa.Column("retry_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "claimed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.CheckConstraint("delivery_id > 0", name="ck_recovery_delivery_id"),
        sa.CheckConstraint(
            "outcome IN ('claimed','accepted','unknown','rate_limited','rejected')",
            name="ck_recovery_delivery_outcome",
        ),
        sa.CheckConstraint(
            "http_status IS NULL OR http_status BETWEEN 100 AND 599",
            name="ck_recovery_delivery_http_status",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_recovery_delivery_retry",
        "delivery_redelivery",
        ["retry_after"],
        schema=SCHEMA,
        postgresql_where=sa.text("outcome = 'rate_limited'"),
    )
    for table in ("scan_state", "scan_ticket", "delivery_redelivery"):
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {SCHEMA}.{table} FORCE ROW LEVEL SECURITY")
        op.execute(f"REVOKE ALL ON {SCHEMA}.{table} FROM PUBLIC")
    op.execute(
        f"CREATE POLICY recovery_edge_all_state ON {SCHEMA}.scan_state "
        f"FOR ALL TO PUBLIC USING (true) WITH CHECK (true)"
    )
    op.execute(
        f"CREATE POLICY recovery_edge_all_ticket ON {SCHEMA}.scan_ticket "
        f"FOR ALL TO PUBLIC USING (true) WITH CHECK (true)"
    )
    op.execute(
        f"CREATE POLICY recovery_edge_all_delivery ON {SCHEMA}.delivery_redelivery "
        f"FOR ALL TO PUBLIC USING (true) WITH CHECK (true)"
    )
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON {SCHEMA}.scan_state TO {EDGE_ROLE}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON {SCHEMA}.scan_ticket TO {EDGE_ROLE}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON {SCHEMA}.delivery_redelivery TO {EDGE_ROLE}")
    op.execute(f"INSERT INTO {SCHEMA}.scan_state (singleton) VALUES (true)")
    _create_functions()


def _create_functions() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.issue_scan_ticket()
        RETURNS TABLE(ticket_id uuid, ticket_value text)
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE v_id uuid; v_value text;
        BEGIN
            DELETE FROM {SCHEMA}.scan_ticket WHERE expires_at < pg_catalog.clock_timestamp();
            v_id := pg_catalog.gen_random_uuid();
            v_value := pg_catalog.gen_random_uuid()::text;
            INSERT INTO {SCHEMA}.scan_ticket(ticket_id, ticket_sha256, expires_at)
            VALUES (v_id, pg_catalog.sha256(pg_catalog.convert_to(v_value, 'UTF8')),
                    pg_catalog.clock_timestamp() + interval '4 minutes');
            RETURN QUERY SELECT v_id, v_value;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.consume_scan_ticket(p_id uuid, p_value text)
        RETURNS boolean
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE v_hash bytea;
        BEGIN
            UPDATE {SCHEMA}.scan_ticket
            SET consumed_at = pg_catalog.clock_timestamp()
            WHERE ticket_id = p_id AND consumed_at IS NULL
              AND expires_at > pg_catalog.clock_timestamp()
              AND ticket_sha256 = pg_catalog.sha256(pg_catalog.convert_to(p_value, 'UTF8'))
            RETURNING ticket_sha256 INTO v_hash;
            RETURN FOUND;
        END;
        $function$
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.issue_scan_ticket() FROM PUBLIC")
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.consume_scan_ticket(uuid,text) FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.issue_scan_ticket() TO {CRON_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.consume_scan_ticket(uuid,text) TO {EDGE_ROLE}")


def downgrade() -> None:
    op.execute(f"DROP FUNCTION {SCHEMA}.consume_scan_ticket(uuid,text)")
    op.execute(f"DROP FUNCTION {SCHEMA}.issue_scan_ticket()")
    op.drop_table("delivery_redelivery", schema=SCHEMA)
    op.drop_table("scan_ticket", schema=SCHEMA)
    op.drop_table("scan_state", schema=SCHEMA)
    op.execute(f"DROP SCHEMA {SCHEMA}")
    bind = op.get_bind()
    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"REVOKE CONNECT ON DATABASE {quoted_database} FROM {EDGE_ROLE}")
