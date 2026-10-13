"""Persist sandbox reservations, provider-period budgets, and one global slot.

Revision ID: 20261012_10
Revises: 20261012_09
Create Date: 2026-10-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "20261012_10"
down_revision: str | None = "20261012_09"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

RUNTIME_ROLE = "continuum_runtime"
SCHEMA = "runtime_private"
PUBLIC_CPU_SECONDS = 14_400
OPERATOR_CPU_SECONDS = 3_600


def upgrade() -> None:
    op.create_table(
        "sandbox_provider_periods",
        sa.Column("provider_period_id", sa.String(length=64), nullable=False),
        sa.Column("starts_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ends_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("public_cpu_seconds_limit", sa.BigInteger(), nullable=False),
        sa.Column("operator_cpu_seconds_limit", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("provider_period_id", name="pk_runtime_sandbox_provider_periods"),
        sa.CheckConstraint(
            "length(btrim(provider_period_id)) BETWEEN 1 AND 64",
            name="ck_runtime_sandbox_period_id",
        ),
        sa.CheckConstraint("starts_at < ends_at", name="ck_runtime_sandbox_period_bounds"),
        sa.CheckConstraint(
            f"public_cpu_seconds_limit = {PUBLIC_CPU_SECONDS}",
            name="ck_runtime_sandbox_period_public_limit",
        ),
        sa.CheckConstraint(
            f"operator_cpu_seconds_limit = {OPERATOR_CPU_SECONDS}",
            name="ck_runtime_sandbox_period_operator_limit",
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "sandbox_period_usage",
        sa.Column("provider_period_id", sa.String(length=64), nullable=False),
        sa.Column("allocation_class", sa.String(length=16), nullable=False),
        sa.Column("charged_cpu_seconds", sa.BigInteger(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint(
            "provider_period_id",
            "allocation_class",
            name="pk_runtime_sandbox_period_usage",
        ),
        sa.ForeignKeyConstraint(
            ["provider_period_id"],
            [f"{SCHEMA}.sandbox_provider_periods.provider_period_id"],
            name="fk_runtime_sandbox_period_usage_period",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "allocation_class IN ('public', 'operator')",
            name="ck_runtime_sandbox_period_usage_class",
        ),
        sa.CheckConstraint(
            "charged_cpu_seconds >= 0", name="ck_runtime_sandbox_period_usage_nonnegative"
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "sandbox_allocations",
        sa.Column("allocation_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("step_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=False),
        sa.Column("logical_key", sa.String(length=128), nullable=False),
        sa.Column("request_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("provider_name", sa.String(length=64), nullable=False),
        sa.Column("provider_session_id", sa.String(length=128), nullable=True),
        sa.Column("provider_period_id", sa.String(length=64), nullable=False),
        sa.Column("allocation_class", sa.String(length=16), nullable=False),
        sa.Column("requested_seconds", sa.Integer(), nullable=False),
        sa.Column("vcpus", sa.SmallInteger(), nullable=False),
        sa.Column("memory_mb", sa.Integer(), nullable=False),
        sa.Column("reserved_cpu_seconds", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False, server_default="reserved"),
        sa.Column("create_authorized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("create_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stop_authorized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("destroy_authorized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_sandbox_status", sa.String(length=32), nullable=True),
        sa.Column("last_session_status", sa.String(length=32), nullable=True),
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
        sa.PrimaryKeyConstraint("allocation_id", name="pk_runtime_sandbox_allocations"),
        sa.UniqueConstraint("provider_name", name="uq_runtime_sandbox_allocations_provider_name"),
        sa.UniqueConstraint(
            "allocation_id",
            "provider_name",
            "allocation_class",
            "provider_period_id",
            "reserved_cpu_seconds",
            name="uq_runtime_sandbox_allocations_global_identity",
        ),
        sa.UniqueConstraint(
            "owner_id",
            "run_id",
            "allocation_id",
            name="uq_runtime_sandbox_allocations_owner_run_id",
        ),
        sa.UniqueConstraint(
            "owner_id",
            "run_id",
            "attempt_id",
            "logical_key",
            name="uq_runtime_sandbox_allocations_attempt_key",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            name="fk_runtime_sandbox_allocations_owner_run",
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
            name="fk_runtime_sandbox_allocations_owner_job",
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
            name="fk_runtime_sandbox_allocations_execution",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["provider_period_id"],
            [f"{SCHEMA}.sandbox_provider_periods.provider_period_id"],
            name="fk_runtime_sandbox_allocations_period",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("generation > 0", name="ck_runtime_sandbox_allocations_generation"),
        sa.CheckConstraint(
            "length(btrim(lease_owner)) BETWEEN 1 AND 128",
            name="ck_runtime_sandbox_allocations_lease_owner",
        ),
        sa.CheckConstraint(
            "length(btrim(logical_key)) BETWEEN 1 AND 128",
            name="ck_runtime_sandbox_allocations_logical_key",
        ),
        sa.CheckConstraint(
            "octet_length(request_sha256) = 32", name="ck_runtime_sandbox_allocations_request_hash"
        ),
        sa.CheckConstraint(
            "provider_name ~ '^continuum-[0-9a-f]{32}$'",
            name="ck_runtime_sandbox_allocations_provider_name",
        ),
        sa.CheckConstraint(
            "provider_session_id IS NULL OR length(btrim(provider_session_id)) BETWEEN 1 AND 128",
            name="ck_runtime_sandbox_allocations_session_id",
        ),
        sa.CheckConstraint(
            "allocation_class IN ('public', 'operator')",
            name="ck_runtime_sandbox_allocations_class",
        ),
        sa.CheckConstraint(
            "requested_seconds BETWEEN 1 AND 900",
            name="ck_runtime_sandbox_allocations_requested_seconds",
        ),
        sa.CheckConstraint("vcpus = 2", name="ck_runtime_sandbox_allocations_vcpus"),
        sa.CheckConstraint("memory_mb = 4096", name="ck_runtime_sandbox_allocations_memory"),
        sa.CheckConstraint(
            "reserved_cpu_seconds = requested_seconds * vcpus",
            name="ck_runtime_sandbox_allocations_cpu_reservation",
        ),
        sa.CheckConstraint(
            "status IN ('reserved', 'creating', 'unknown', 'running', 'stop_unknown', "
            "'stopped', 'destroy_unknown', 'destroyed', 'not_applied', 'released')",
            name="ck_runtime_sandbox_allocations_status",
        ),
        sa.CheckConstraint(
            "(status = 'reserved' AND create_authorized_at IS NULL) OR "
            "(status = 'released' AND create_authorized_at IS NULL) OR "
            "(status = 'unknown' AND "
            "(create_authorized_at IS NOT NULL OR last_observed_at IS NOT NULL)) OR "
            "(status NOT IN ('reserved', 'released', 'unknown') "
            "AND create_authorized_at IS NOT NULL)",
            name="ck_runtime_sandbox_allocations_create_authorization",
        ),
        sa.CheckConstraint(
            "status NOT IN ('running', 'stop_unknown', 'destroy_unknown') "
            "OR provider_session_id IS NOT NULL",
            name="ck_runtime_sandbox_allocations_session_required",
        ),
        sa.CheckConstraint(
            "status <> 'stopped' OR provider_session_id IS NOT NULL OR "
            "(last_sandbox_status='stopped' AND last_session_status IS NULL)",
            name="ck_runtime_sandbox_allocations_sessionless_stopped",
        ),
        sa.CheckConstraint(
            "last_sandbox_status IS NULL OR length(btrim(last_sandbox_status)) BETWEEN 1 AND 32",
            name="ck_runtime_sandbox_allocations_observed_sandbox_status",
        ),
        sa.CheckConstraint(
            "last_session_status IS NULL OR length(btrim(last_session_status)) BETWEEN 1 AND 32",
            name="ck_runtime_sandbox_allocations_observed_session_status",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_runtime_sandbox_allocations_owner_run",
        "sandbox_allocations",
        ["owner_id", "run_id", "created_at"],
        schema=SCHEMA,
    )
    op.create_index(
        "ix_runtime_sandbox_allocations_status",
        "sandbox_allocations",
        ["status", "created_at"],
        schema=SCHEMA,
    )

    op.create_table(
        "sandbox_allocation_period_charges",
        sa.Column("allocation_id", sa.Uuid(), nullable=False),
        sa.Column("provider_period_id", sa.String(length=64), nullable=False),
        sa.Column("allocation_class", sa.String(length=16), nullable=False),
        sa.Column("cpu_seconds", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint(
            "allocation_id",
            "provider_period_id",
            name="pk_runtime_sandbox_allocation_period_charges",
        ),
        sa.ForeignKeyConstraint(
            ["allocation_id"],
            [f"{SCHEMA}.sandbox_allocations.allocation_id"],
            name="fk_runtime_sandbox_allocation_period_charges_allocation",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["provider_period_id", "allocation_class"],
            [
                f"{SCHEMA}.sandbox_period_usage.provider_period_id",
                f"{SCHEMA}.sandbox_period_usage.allocation_class",
            ],
            name="fk_runtime_sandbox_allocation_period_charges_usage",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "allocation_class IN ('public', 'operator')",
            name="ck_runtime_sandbox_allocation_period_charges_class",
        ),
        sa.CheckConstraint(
            "cpu_seconds > 0", name="ck_runtime_sandbox_allocation_period_charges_cpu_seconds"
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "sandbox_global_capacity",
        sa.Column("capacity_id", sa.SmallInteger(), nullable=False),
        sa.Column("active_allocation_id", sa.Uuid(), nullable=True),
        sa.Column("active_provider_name", sa.String(length=64), nullable=True),
        sa.Column("active_allocation_class", sa.String(length=16), nullable=True),
        sa.Column("active_origin_period_id", sa.String(length=64), nullable=True),
        sa.Column("active_cpu_seconds", sa.BigInteger(), nullable=True),
        sa.Column("period_blocked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("period_blocked_period_id", sa.String(length=64), nullable=True),
        sa.Column("period_blocked_reason", sa.String(length=64), nullable=True),
        sa.PrimaryKeyConstraint("capacity_id", name="pk_runtime_sandbox_global_capacity"),
        sa.ForeignKeyConstraint(
            [
                "active_allocation_id",
                "active_provider_name",
                "active_allocation_class",
                "active_origin_period_id",
                "active_cpu_seconds",
            ],
            [
                f"{SCHEMA}.sandbox_allocations.allocation_id",
                f"{SCHEMA}.sandbox_allocations.provider_name",
                f"{SCHEMA}.sandbox_allocations.allocation_class",
                f"{SCHEMA}.sandbox_allocations.provider_period_id",
                f"{SCHEMA}.sandbox_allocations.reserved_cpu_seconds",
            ],
            name="fk_runtime_sandbox_global_capacity_allocation",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["active_origin_period_id"],
            [f"{SCHEMA}.sandbox_provider_periods.provider_period_id"],
            name="fk_runtime_sandbox_global_capacity_period",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["period_blocked_period_id"],
            [f"{SCHEMA}.sandbox_provider_periods.provider_period_id"],
            name="fk_runtime_sandbox_global_capacity_blocked_period",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("capacity_id = 1", name="ck_runtime_sandbox_global_capacity_singleton"),
        sa.CheckConstraint(
            "(active_allocation_id IS NULL AND active_provider_name IS NULL AND "
            "active_allocation_class IS NULL AND active_origin_period_id IS NULL AND "
            "active_cpu_seconds IS NULL) OR "
            "(active_allocation_id IS NOT NULL AND active_provider_name IS NOT NULL AND "
            "active_allocation_class IN ('public', 'operator') AND "
            "active_origin_period_id IS NOT NULL AND active_cpu_seconds > 0)",
            name="ck_runtime_sandbox_global_capacity_active_fields",
        ),
        sa.CheckConstraint(
            "(period_blocked_at IS NULL AND period_blocked_period_id IS NULL AND "
            "period_blocked_reason IS NULL) OR "
            "(period_blocked_at IS NOT NULL AND period_blocked_period_id IS NOT NULL AND "
            "length(btrim(period_blocked_reason)) BETWEEN 1 AND 64)",
            name="ck_runtime_sandbox_global_capacity_period_block",
        ),
        schema=SCHEMA,
    )
    op.bulk_insert(
        sa.table(
            "sandbox_global_capacity",
            sa.column("capacity_id", sa.SmallInteger()),
            schema=SCHEMA,
        ),
        [{"capacity_id": 1}],
    )

    op.execute(f"ALTER TABLE {SCHEMA}.sandbox_provider_periods ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.sandbox_provider_periods FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY runtime_sandbox_period_read ON {SCHEMA}.sandbox_provider_periods "
        f"FOR SELECT TO {RUNTIME_ROLE} USING (true)"
    )
    op.execute(f"ALTER TABLE {SCHEMA}.sandbox_period_usage ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.sandbox_period_usage FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY runtime_sandbox_period_usage ON {SCHEMA}.sandbox_period_usage "
        f"FOR ALL TO {RUNTIME_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute(f"ALTER TABLE {SCHEMA}.sandbox_allocations ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.sandbox_allocations FORCE ROW LEVEL SECURITY")
    predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.sandbox_allocations "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(f"ALTER TABLE {SCHEMA}.sandbox_allocation_period_charges ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.sandbox_allocation_period_charges FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY runtime_sandbox_allocation_period_charges "
        f"ON {SCHEMA}.sandbox_allocation_period_charges FOR ALL TO {RUNTIME_ROLE} "
        f"USING (true) WITH CHECK (true)"
    )
    op.execute(f"ALTER TABLE {SCHEMA}.sandbox_global_capacity ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.sandbox_global_capacity FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY runtime_sandbox_global_capacity ON {SCHEMA}.sandbox_global_capacity "
        f"FOR ALL TO {RUNTIME_ROLE} USING (capacity_id = 1) WITH CHECK (capacity_id = 1)"
    )

    for table_name in (
        "sandbox_provider_periods",
        "sandbox_period_usage",
        "sandbox_allocations",
        "sandbox_allocation_period_charges",
        "sandbox_global_capacity",
    ):
        op.execute(f"REVOKE ALL ON {SCHEMA}.{table_name} FROM PUBLIC")
    op.execute(f"GRANT SELECT ON {SCHEMA}.sandbox_provider_periods TO {RUNTIME_ROLE}")
    op.execute(f"GRANT SELECT ON {SCHEMA}.sandbox_period_usage TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE (charged_cpu_seconds) ON {SCHEMA}.sandbox_period_usage TO {RUNTIME_ROLE}"
    )
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.sandbox_allocations TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE (provider_period_id, requested_seconds, vcpus, memory_mb, "
        f"reserved_cpu_seconds, status, provider_session_id, "
        f"create_authorized_at, create_observed_at, "
        f"stop_authorized_at, destroy_authorized_at, last_observed_at, last_sandbox_status, "
        f"last_session_status, updated_at) ON {SCHEMA}.sandbox_allocations TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT SELECT, INSERT, DELETE ON {SCHEMA}.sandbox_allocation_period_charges "
        f"TO {RUNTIME_ROLE}"
    )
    op.execute(f"GRANT SELECT ON {SCHEMA}.sandbox_global_capacity TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE (active_allocation_id, active_provider_name, active_allocation_class, "
        f"active_origin_period_id, active_cpu_seconds, period_blocked_at, "
        f"period_blocked_period_id, period_blocked_reason) "
        f"ON {SCHEMA}.sandbox_global_capacity TO {RUNTIME_ROLE}"
    )


def downgrade() -> None:
    op.drop_table("sandbox_global_capacity", schema=SCHEMA)
    op.drop_table("sandbox_allocation_period_charges", schema=SCHEMA)
    op.drop_index(
        "ix_runtime_sandbox_allocations_status",
        table_name="sandbox_allocations",
        schema=SCHEMA,
    )
    op.drop_index(
        "ix_runtime_sandbox_allocations_owner_run",
        table_name="sandbox_allocations",
        schema=SCHEMA,
    )
    op.drop_table("sandbox_allocations", schema=SCHEMA)
    op.drop_table("sandbox_period_usage", schema=SCHEMA)
    op.drop_table("sandbox_provider_periods", schema=SCHEMA)
