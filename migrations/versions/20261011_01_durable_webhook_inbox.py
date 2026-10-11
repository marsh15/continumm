"""Add the private GitHub webhook inbox.

Revision ID: 20261011_01
Revises:
Create Date: 2026-10-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "20261011_01"
down_revision: str | None = None
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

INGRESS_ROLE = "continuum_ingress"
SCHEMA = "webhook_private"
TABLE = "github_delivery_inbox"


def upgrade() -> None:
    bind = op.get_bind()
    role = bind.execute(
        sa.text(
            """
            SELECT
                r.rolcanlogin,
                r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR
                    r.rolreplication OR r.rolbypassrls AS elevated,
                r.rolinherit,
                EXISTS (
                    SELECT 1 FROM pg_auth_members m
                    WHERE m.member = r.oid OR m.roleid = r.oid
                ) AS has_membership
            FROM pg_roles r
            WHERE r.rolname = :role_name
            """
        ),
        {"role_name": INGRESS_ROLE},
    ).one_or_none()
    if role is None:
        raise RuntimeError("create_continuum_ingress_role_before_migration")
    if not role.rolcanlogin or role.elevated or role.rolinherit or role.has_membership:
        raise RuntimeError("continuum_ingress_role_privileges_invalid")

    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"GRANT CONNECT ON DATABASE {quoted_database} TO {INGRESS_ROLE}")

    op.execute(f"CREATE SCHEMA {SCHEMA}")
    op.execute(f"REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {INGRESS_ROLE}")
    op.create_table(
        TABLE,
        sa.Column("github_app_id", sa.BigInteger(), nullable=False),
        sa.Column("delivery_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("action", sa.String(length=128), nullable=True),
        sa.Column("payload_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("raw_body", sa.LargeBinary(), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("github_app_id", "delivery_id", name="pk_github_delivery_inbox"),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_github_delivery_inbox_app_id_positive",
        TABLE,
        "github_app_id > 0",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_github_delivery_inbox_digest_length",
        TABLE,
        "octet_length(payload_sha256) = 32",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_github_delivery_inbox_body_limit",
        TABLE,
        "octet_length(raw_body) <= 1048576",
        schema=SCHEMA,
    )

    op.execute(f"ALTER TABLE {SCHEMA}.{TABLE} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.{TABLE} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY ingress_select_delivery ON {SCHEMA}.{TABLE} "
        f"FOR SELECT TO {INGRESS_ROLE} USING (true)"
    )
    op.execute(
        f"CREATE POLICY ingress_insert_delivery ON {SCHEMA}.{TABLE} "
        f"FOR INSERT TO {INGRESS_ROLE} WITH CHECK (true)"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.{TABLE} FROM PUBLIC")
    op.execute(
        f"GRANT INSERT (github_app_id, delivery_id, event_type, action, payload_sha256, raw_body) "
        f"ON {SCHEMA}.{TABLE} TO {INGRESS_ROLE}"
    )
    op.execute(
        f"GRANT SELECT (github_app_id, delivery_id, payload_sha256) "
        f"ON {SCHEMA}.{TABLE} TO {INGRESS_ROLE}"
    )


def downgrade() -> None:
    op.drop_table(TABLE, schema=SCHEMA)
    op.execute(f"DROP SCHEMA {SCHEMA}")
    bind = op.get_bind()
    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"REVOKE CONNECT ON DATABASE {quoted_database} FROM {INGRESS_ROLE}")
