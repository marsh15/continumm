"""Persist stable external mutation intents and append-only observations.

Revision ID: 20261017_19
Revises: 20261017_18
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261017_19"
down_revision = "20261017_18"
branch_labels = None
depends_on = None

SCHEMA = "runtime_private"
RUNTIME_ROLE = "continuum_runtime"


def upgrade() -> None:
    op.create_table(
        "external_operations",
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("step_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("lease_owner", sa.String(128), nullable=False),
        sa.Column("operation_key", sa.String(128), nullable=False),
        sa.Column("request_sha256", sa.LargeBinary(32), nullable=False),
        sa.Column("request_metadata", JSONB(), nullable=False),
        sa.Column("approval_id", sa.Uuid(), nullable=True),
        sa.Column("approval_sha256", sa.LargeBinary(32), nullable=True),
        sa.Column("status", sa.String(24), nullable=False, server_default="prepared"),
        sa.Column("dispatch_authorized_at", sa.DateTime(timezone=True)),
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
        sa.PrimaryKeyConstraint("operation_id", name="pk_runtime_external_operations"),
        sa.UniqueConstraint(
            "owner_id", "run_id", "operation_key", name="uq_runtime_external_operations_key"
        ),
        sa.UniqueConstraint(
            "owner_id", "run_id", "operation_id", name="uq_runtime_external_operations_owner_id"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            name="fk_runtime_external_operations_run",
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
            name="fk_runtime_external_operations_job",
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
            name="fk_runtime_external_operations_attempt",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "approval_id"],
            [
                f"{SCHEMA}.approvals.owner_id",
                f"{SCHEMA}.approvals.run_id",
                f"{SCHEMA}.approvals.approval_id",
            ],
            name="fk_runtime_external_operations_approval",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("generation > 0", name="ck_runtime_external_operations_generation"),
        sa.CheckConstraint(
            "length(btrim(lease_owner)) BETWEEN 1 AND 128",
            name="ck_runtime_external_operations_lease_owner",
        ),
        sa.CheckConstraint(
            "length(btrim(operation_key)) BETWEEN 1 AND 128",
            name="ck_runtime_external_operations_key",
        ),
        sa.CheckConstraint(
            "octet_length(request_sha256)=32", name="ck_runtime_external_operations_digest"
        ),
        sa.CheckConstraint(
            "jsonb_typeof(request_metadata)='object' AND "
            "octet_length(request_metadata::text)<=4096",
            name="ck_runtime_external_operations_metadata",
        ),
        sa.CheckConstraint(
            "(approval_id IS NULL AND approval_sha256 IS NULL) OR "
            "(approval_id IS NOT NULL AND approval_sha256 IS NOT NULL "
            "AND octet_length(approval_sha256)=32)",
            name="ck_runtime_external_operations_approval_binding",
        ),
        sa.CheckConstraint(
            "status IN ('prepared','dispatching','confirmed','definitely_failed',"
            "'unknown','conflict')",
            name="ck_runtime_external_operations_status",
        ),
        sa.CheckConstraint(
            "(status='prepared' AND dispatch_authorized_at IS NULL) OR "
            "(status<>'prepared' AND dispatch_authorized_at IS NOT NULL)",
            name="ck_runtime_external_operations_dispatch_time",
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "external_operation_observations",
        sa.Column("observation_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("observation_key", sa.String(128), nullable=False),
        sa.Column("outcome", sa.String(24), nullable=False),
        sa.Column("details", JSONB(), nullable=False),
        sa.Column("actor_type", sa.String(16), nullable=False),
        sa.Column("actor_id", sa.String(128)),
        sa.Column(
            "observed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "observation_id", name="pk_runtime_external_operation_observations"
        ),
        sa.UniqueConstraint(
            "owner_id",
            "run_id",
            "operation_id",
            "observation_key",
            name="uq_runtime_external_operation_observation_key",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "operation_id"],
            [
                f"{SCHEMA}.external_operations.owner_id",
                f"{SCHEMA}.external_operations.run_id",
                f"{SCHEMA}.external_operations.operation_id",
            ],
            name="fk_runtime_external_operation_observation_operation",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "length(btrim(observation_key)) BETWEEN 1 AND 128",
            name="ck_runtime_external_observation_key",
        ),
        sa.CheckConstraint(
            "outcome IN ('confirmed','definitely_failed','unknown','conflict')",
            name="ck_runtime_external_observation_outcome",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(details)='object' AND octet_length(details::text)<=4096",
            name="ck_runtime_external_observation_details",
        ),
        sa.CheckConstraint(
            "actor_type IN ('account','system','worker','provider')",
            name="ck_runtime_external_observation_actor",
        ),
        sa.CheckConstraint(
            "actor_id IS NULL OR length(btrim(actor_id)) BETWEEN 1 AND 128",
            name="ck_runtime_external_observation_actor_id",
        ),
        schema=SCHEMA,
    )
    for table in ("external_operations", "external_operation_observations"):
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {SCHEMA}.{table} FORCE ROW LEVEL SECURITY")
        predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
        op.execute(
            f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.{table} "
            f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
        )
        op.execute(f"REVOKE ALL ON {SCHEMA}.{table} FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.external_operations TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE (status, dispatch_authorized_at, updated_at) "
        f"ON {SCHEMA}.external_operations TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT UPDATE (step_id, job_id, generation, attempt_id, lease_owner) "
        f"ON {SCHEMA}.external_operations TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT SELECT, INSERT ON {SCHEMA}.external_operation_observations TO {RUNTIME_ROLE}"
    )
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.reject_external_observation_mutation()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
        BEGIN
          RAISE EXCEPTION 'external_operation_observations_are_append_only';
        END $$
    """)
    op.execute(f"""
        CREATE TRIGGER external_operation_observations_append_only
        BEFORE UPDATE OR DELETE ON {SCHEMA}.external_operation_observations
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.reject_external_observation_mutation()
    """)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.guard_external_operation_identity()
        RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
        BEGIN
          IF (OLD.status <> 'prepared' OR NEW.status <> 'prepared') AND (
            NEW.owner_id IS DISTINCT FROM OLD.owner_id OR
            NEW.run_id IS DISTINCT FROM OLD.run_id OR
            NEW.step_id IS DISTINCT FROM OLD.step_id OR
            NEW.job_id IS DISTINCT FROM OLD.job_id OR
            NEW.generation IS DISTINCT FROM OLD.generation OR
            NEW.attempt_id IS DISTINCT FROM OLD.attempt_id OR
            NEW.lease_owner IS DISTINCT FROM OLD.lease_owner OR
            NEW.operation_key IS DISTINCT FROM OLD.operation_key OR
            NEW.request_sha256 IS DISTINCT FROM OLD.request_sha256 OR
            NEW.request_metadata IS DISTINCT FROM OLD.request_metadata OR
            NEW.approval_id IS DISTINCT FROM OLD.approval_id OR
            NEW.approval_sha256 IS DISTINCT FROM OLD.approval_sha256
          ) THEN
            RAISE EXCEPTION 'external_operation_identity_is_immutable';
          END IF;
          IF OLD.status <> 'prepared' AND NEW.status = 'prepared' THEN
            RAISE EXCEPTION 'external_operation_dispatch_cannot_be_rearmed';
          END IF;
          IF OLD.status <> 'prepared' AND
             NEW.dispatch_authorized_at IS DISTINCT FROM OLD.dispatch_authorized_at THEN
            RAISE EXCEPTION 'external_operation_dispatch_time_is_immutable';
          END IF;
          IF NOT (
            (OLD.status = 'prepared' AND NEW.status IN ('prepared','dispatching')) OR
            (OLD.status = 'dispatching' AND NEW.status IN
              ('dispatching','confirmed','definitely_failed','unknown','conflict')) OR
            (OLD.status = 'unknown' AND NEW.status IN
              ('unknown','confirmed','definitely_failed','conflict')) OR
            (OLD.status = 'confirmed' AND NEW.status IN ('confirmed','conflict')) OR
            (OLD.status = 'definitely_failed' AND NEW.status IN
              ('definitely_failed','conflict')) OR
            (OLD.status = 'conflict' AND NEW.status = 'conflict')
          ) THEN
            RAISE EXCEPTION 'external_operation_status_transition_invalid';
          END IF;
          RETURN NEW;
        END $$
    """)
    op.execute(f"""
        CREATE TRIGGER external_operation_identity_guard
        BEFORE UPDATE ON {SCHEMA}.external_operations
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.guard_external_operation_identity()
    """)


def downgrade() -> None:
    op.execute(f"DROP TRIGGER external_operation_identity_guard ON {SCHEMA}.external_operations")
    op.execute(f"DROP FUNCTION {SCHEMA}.guard_external_operation_identity()")
    op.execute(
        f"DROP TRIGGER external_operation_observations_append_only "
        f"ON {SCHEMA}.external_operation_observations"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.reject_external_observation_mutation()")
    op.drop_table("external_operation_observations", schema=SCHEMA)
    op.drop_table("external_operations", schema=SCHEMA)
