"""Owner-isolated encrypted BYOK credentials, bindings and dispatch grants.

Revision ID: 20261015_12
Revises: 20261013_11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20261015_12"
down_revision = "20261013_11"
branch_labels = None
depends_on = None
SCHEMA = "runtime_private"


def upgrade() -> None:
    op.create_table(
        "provider_credentials",
        sa.Column("credential_id", sa.Uuid(), primary_key=True),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(16), nullable=False),
        sa.Column("scope", sa.String(8), nullable=False),
        sa.Column("scope_run_id", sa.Uuid()),
        sa.Column("key_version", sa.String(32), nullable=False),
        sa.Column("nonce", sa.LargeBinary()),
        sa.Column("ciphertext", sa.LargeBinary()),
        sa.Column("status", sa.String(16), nullable=False, server_default="active"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint(
            "owner_id", "provider", "credential_id", name="uq_runtime_credentials_owner_provider_id"
        ),
        sa.UniqueConstraint("key_version", "nonce", name="uq_runtime_credentials_nonce"),
        sa.ForeignKeyConstraint(["owner_id"], [f"{SCHEMA}.users.user_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["owner_id", "scope_run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("provider IN ('openai', 'anthropic')", name="ck_credentials_provider"),
        sa.CheckConstraint("key_version ~ '^[A-Za-z0-9_-]{1,32}$'", name="ck_credentials_version"),
        sa.CheckConstraint(
            "(scope='saved' AND scope_run_id IS NULL AND expires_at IS NULL) OR "
            "(scope='run' AND scope_run_id IS NOT NULL AND expires_at IS NOT NULL)",
            name="ck_credentials_scope",
        ),
        sa.CheckConstraint(
            "(status='active' AND nonce IS NOT NULL AND octet_length(nonce)=12 "
            "AND ciphertext IS NOT NULL AND octet_length(ciphertext) BETWEEN 17 AND 4112 "
            "AND ended_at IS NULL) OR "
            "(status IN ('revoked', 'deleted') AND nonce IS NULL AND ciphertext IS NULL "
            "AND ended_at IS NOT NULL)",
            name="ck_credentials_material",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_credentials_owner_created",
        "provider_credentials",
        ["owner_id", "created_at"],
        schema=SCHEMA,
    )
    op.create_table(
        "run_credentials",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(16), nullable=False),
        sa.Column("credential_id", sa.Uuid(), nullable=False),
        sa.Column("revision", sa.BigInteger(), nullable=False, server_default="1"),
        sa.PrimaryKeyConstraint("owner_id", "run_id"),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "provider", "credential_id"],
            [
                f"{SCHEMA}.provider_credentials.owner_id",
                f"{SCHEMA}.provider_credentials.provider",
                f"{SCHEMA}.provider_credentials.credential_id",
            ],
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("revision > 0", name="ck_run_credentials_revision"),
        schema=SCHEMA,
    )
    op.create_table(
        "credential_dispatches",
        sa.Column("grant_id", sa.Uuid(), primary_key=True),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("request_id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(16), nullable=False),
        sa.Column("credential_id", sa.Uuid(), nullable=False),
        sa.Column("binding_revision", sa.BigInteger(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("lease_owner", sa.String(128), nullable=False),
        sa.Column(
            "authorized_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.UniqueConstraint(
            "owner_id", "run_id", "request_id", name="uq_runtime_credential_dispatch_request"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "provider", "credential_id"],
            [
                f"{SCHEMA}.provider_credentials.owner_id",
                f"{SCHEMA}.provider_credentials.provider",
                f"{SCHEMA}.provider_credentials.credential_id",
            ],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "job_id", "generation", "attempt_id", "lease_owner"],
            [
                f"{SCHEMA}.attempts.{column}"
                for column in (
                    "owner_id",
                    "run_id",
                    "job_id",
                    "generation",
                    "attempt_id",
                    "lease_owner",
                )
            ],
            ondelete="RESTRICT",
        ),
        schema=SCHEMA,
    )
    for table in ("provider_credentials", "run_credentials", "credential_dispatches"):
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {SCHEMA}.{table} FORCE ROW LEVEL SECURITY")
        predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
        op.execute(
            f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.{table} "
            f"FOR ALL TO continuum_runtime USING ({predicate}) WITH CHECK ({predicate})"
        )
        op.execute(f"REVOKE ALL ON {SCHEMA}.{table} FROM PUBLIC")
        op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.{table} TO continuum_runtime")
    op.execute(
        f"GRANT UPDATE (key_version, nonce, ciphertext, status, ended_at) "
        f"ON {SCHEMA}.provider_credentials TO continuum_runtime"
    )
    op.execute(
        f"GRANT UPDATE (credential_id, revision) ON {SCHEMA}.run_credentials TO continuum_runtime"
    )
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.erase_terminal_run_credentials() RETURNS trigger
        LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog AS $$
        BEGIN
          IF NEW.status IN ('succeeded','failed','cancelled','expired') THEN
            UPDATE runtime_private.provider_credentials
            SET status='deleted', nonce=NULL, ciphertext=NULL, ended_at=clock_timestamp()
            WHERE owner_id=NEW.owner_id AND scope_run_id=NEW.run_id AND scope='run'
              AND status='active';
          END IF;
          RETURN NEW;
        END $$
    """)
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.erase_terminal_run_credentials() FROM PUBLIC")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.erase_terminal_run_credentials() TO continuum_runtime"
    )
    op.execute(
        f"CREATE TRIGGER erase_terminal_run_credentials AFTER UPDATE OF status "
        f"ON {SCHEMA}.runs FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.erase_terminal_run_credentials()"
    )


def downgrade() -> None:
    op.execute(f"DROP TRIGGER erase_terminal_run_credentials ON {SCHEMA}.runs")
    op.execute(f"DROP FUNCTION {SCHEMA}.erase_terminal_run_credentials()")
    for table in ("credential_dispatches", "run_credentials", "provider_credentials"):
        op.drop_table(table, schema=SCHEMA)
