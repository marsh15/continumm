"""Persist scripted agent turn intents, responses, and stable tool calls.

Revision ID: 20261013_11
Revises: 20261012_10
Create Date: 2026-10-15
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "20261013_11"
down_revision: str | None = "20261012_10"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

RUNTIME_ROLE = "continuum_runtime"
SCHEMA = "runtime_private"
MAX_TURN_REQUESTS = 30
MAX_AGENT_JSON_BYTES = 65_536
MAX_AGENT_JSONB_TEXT_BYTES = 98_304
MAX_TOOL_CALLS = 10


def upgrade() -> None:
    op.create_table(
        "agent_turns",
        sa.Column("turn_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("step_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("logical_key", sa.String(length=128), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=False),
        sa.Column("baseline_artifact_id", sa.Uuid(), nullable=False),
        sa.Column("model_id", sa.String(length=64), nullable=False),
        sa.Column("request_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("context", JSONB(), nullable=False),
        sa.Column("tool_schemas", JSONB(), nullable=False),
        sa.Column("input_bytes_reserved", sa.Integer(), nullable=False),
        sa.Column("output_tokens_reserved", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False, server_default="prepared"),
        sa.Column("dispatch_authorized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dispatch_generation", sa.BigInteger(), nullable=True),
        sa.Column("dispatch_attempt_id", sa.Uuid(), nullable=True),
        sa.Column("dispatch_lease_owner", sa.String(length=128), nullable=True),
        sa.Column("response", JSONB(), nullable=True),
        sa.Column("response_sha256", sa.LargeBinary(length=32), nullable=True),
        sa.Column("response_received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("response_error_code", sa.String(length=96), nullable=True),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
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
        sa.PrimaryKeyConstraint("turn_id", name="pk_runtime_agent_turns"),
        sa.UniqueConstraint(
            "owner_id", "run_id", "turn_id", name="uq_runtime_agent_turns_owner_run_id"
        ),
        sa.UniqueConstraint(
            "owner_id",
            "run_id",
            "job_id",
            "logical_key",
            name="uq_runtime_agent_turns_job_key",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "step_id", "job_id"],
            [
                f"{SCHEMA}.jobs.owner_id",
                f"{SCHEMA}.jobs.run_id",
                f"{SCHEMA}.jobs.step_id",
                f"{SCHEMA}.jobs.job_id",
            ],
            name="fk_runtime_agent_turns_owner_job",
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
            name="fk_runtime_agent_turns_creator_attempt",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "baseline_artifact_id"],
            [
                f"{SCHEMA}.artifact_uploads.owner_id",
                f"{SCHEMA}.artifact_uploads.run_id",
                f"{SCHEMA}.artifact_uploads.upload_id",
            ],
            name="fk_runtime_agent_turns_baseline_artifact",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            [
                "owner_id",
                "run_id",
                "job_id",
                "dispatch_generation",
                "dispatch_attempt_id",
                "dispatch_lease_owner",
            ],
            [
                f"{SCHEMA}.attempts.owner_id",
                f"{SCHEMA}.attempts.run_id",
                f"{SCHEMA}.attempts.job_id",
                f"{SCHEMA}.attempts.generation",
                f"{SCHEMA}.attempts.attempt_id",
                f"{SCHEMA}.attempts.lease_owner",
            ],
            name="fk_runtime_agent_turns_dispatch_attempt",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("generation > 0", name="ck_runtime_agent_turns_generation"),
        sa.CheckConstraint(
            "length(btrim(logical_key)) BETWEEN 1 AND 128",
            name="ck_runtime_agent_turns_logical_key",
        ),
        sa.CheckConstraint("model_id = 'scripted-v1'", name="ck_runtime_agent_turns_model"),
        sa.CheckConstraint(
            "octet_length(request_sha256) = 32", name="ck_runtime_agent_turns_request_hash"
        ),
        sa.CheckConstraint(
            f"octet_length(context::text) <= {MAX_AGENT_JSONB_TEXT_BYTES}",
            name="ck_runtime_agent_turns_context_size",
        ),
        sa.CheckConstraint(
            f"octet_length(tool_schemas::text) <= {MAX_AGENT_JSONB_TEXT_BYTES}",
            name="ck_runtime_agent_turns_tools_size",
        ),
        sa.CheckConstraint(
            "input_bytes_reserved BETWEEN 0 AND 65536",
            name="ck_runtime_agent_turns_input_reservation",
        ),
        sa.CheckConstraint(
            "output_tokens_reserved BETWEEN 1 AND 4096",
            name="ck_runtime_agent_turns_output_reservation",
        ),
        sa.CheckConstraint(
            "status IN ('prepared', 'dispatching', 'unknown', 'response_accepted', "
            "'invalid_response')",
            name="ck_runtime_agent_turns_status",
        ),
        sa.CheckConstraint(
            "(dispatch_authorized_at IS NULL AND dispatch_generation IS NULL AND "
            "dispatch_attempt_id IS NULL AND dispatch_lease_owner IS NULL) OR "
            "(dispatch_authorized_at IS NOT NULL AND dispatch_generation IS NOT NULL AND "
            "dispatch_attempt_id IS NOT NULL AND dispatch_lease_owner IS NOT NULL)",
            name="ck_runtime_agent_turns_dispatch_identity",
        ),
        sa.CheckConstraint(
            "(status = 'prepared' AND dispatch_authorized_at IS NULL AND response IS NULL) OR "
            "(status = 'dispatching' AND dispatch_authorized_at IS NOT NULL AND response IS NULL) "
            "OR (status = 'unknown' AND dispatch_authorized_at IS NOT NULL AND response IS NULL) "
            "OR (status IN ('response_accepted', 'invalid_response') AND "
            "dispatch_authorized_at IS NOT NULL AND response IS NOT NULL "
            "AND response_sha256 IS NOT NULL AND response_received_at IS NOT NULL)",
            name="ck_runtime_agent_turns_state_shape",
        ),
        sa.CheckConstraint(
            "response_sha256 IS NULL OR octet_length(response_sha256) = 32",
            name="ck_runtime_agent_turns_response_hash",
        ),
        sa.CheckConstraint(
            "(status = 'response_accepted' AND response_error_code IS NULL "
            "AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL) OR "
            "(status = 'invalid_response' AND response_error_code IS NOT NULL) OR "
            "status NOT IN ('response_accepted', 'invalid_response')",
            name="ck_runtime_agent_turns_response_outcome",
        ),
        sa.CheckConstraint(
            f"response IS NULL OR octet_length(response::text) <= {MAX_AGENT_JSONB_TEXT_BYTES}",
            name="ck_runtime_agent_turns_response_size",
        ),
        sa.CheckConstraint(
            "(input_tokens IS NULL AND output_tokens IS NULL) OR "
            "(input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND "
            "input_tokens >= 0 AND output_tokens >= 0 AND output_tokens <= 4096)",
            name="ck_runtime_agent_turns_usage",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_runtime_agent_turns_owner_run_created",
        "agent_turns",
        ["owner_id", "run_id", "created_at"],
        schema=SCHEMA,
    )

    op.create_table(
        "agent_invocations",
        sa.Column("invocation_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("turn_id", sa.Uuid(), nullable=False),
        sa.Column("ordinal", sa.SmallInteger(), nullable=False),
        sa.Column("provider_call_id", sa.String(length=128), nullable=False),
        sa.Column("tool_name", sa.String(length=32), nullable=False),
        sa.Column("logical_key", sa.String(length=128), nullable=False),
        sa.Column("arguments", JSONB(), nullable=False),
        sa.Column("arguments_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("invocation_id", name="pk_runtime_agent_invocations"),
        sa.UniqueConstraint(
            "owner_id",
            "run_id",
            "invocation_id",
            name="uq_runtime_agent_invocations_owner_run_id",
        ),
        sa.UniqueConstraint(
            "owner_id",
            "run_id",
            "turn_id",
            "ordinal",
            name="uq_runtime_agent_invocations_turn_ordinal",
        ),
        sa.UniqueConstraint(
            "owner_id",
            "run_id",
            "turn_id",
            "provider_call_id",
            name="uq_runtime_agent_invocations_provider_call",
        ),
        sa.UniqueConstraint(
            "owner_id", "run_id", "logical_key", name="uq_runtime_agent_invocations_logical_key"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id", "turn_id"],
            [
                f"{SCHEMA}.agent_turns.owner_id",
                f"{SCHEMA}.agent_turns.run_id",
                f"{SCHEMA}.agent_turns.turn_id",
            ],
            name="fk_runtime_agent_invocations_turn",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            f"ordinal BETWEEN 0 AND {MAX_TOOL_CALLS - 1}",
            name="ck_runtime_agent_invocations_ordinal",
        ),
        sa.CheckConstraint(
            "length(btrim(provider_call_id)) BETWEEN 1 AND 128",
            name="ck_runtime_agent_invocations_provider_call_id",
        ),
        sa.CheckConstraint(
            "tool_name IN ('list_files', 'read_file', 'search_text', 'apply_patch', 'diff')",
            name="ck_runtime_agent_invocations_tool_name",
        ),
        sa.CheckConstraint(
            "length(btrim(logical_key)) BETWEEN 1 AND 128",
            name="ck_runtime_agent_invocations_logical_key",
        ),
        sa.CheckConstraint(
            "octet_length(arguments_sha256) = 32",
            name="ck_runtime_agent_invocations_arguments_hash",
        ),
        sa.CheckConstraint(
            f"octet_length(arguments::text) <= {MAX_AGENT_JSONB_TEXT_BYTES}",
            name="ck_runtime_agent_invocations_arguments_size",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_runtime_agent_invocations_turn_order",
        "agent_invocations",
        ["owner_id", "run_id", "turn_id", "ordinal"],
        schema=SCHEMA,
    )

    for table in ("agent_turns", "agent_invocations"):
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {SCHEMA}.{table} FORCE ROW LEVEL SECURITY")
        predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
        op.execute(
            f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.{table} "
            f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
        )
        op.execute(f"REVOKE ALL ON {SCHEMA}.{table} FROM PUBLIC")

    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.agent_turns TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE (status, dispatch_authorized_at, dispatch_generation, "
        f"dispatch_attempt_id, dispatch_lease_owner, response, response_sha256, "
        f"response_received_at, response_error_code, input_tokens, output_tokens, updated_at) "
        f"ON {SCHEMA}.agent_turns TO {RUNTIME_ROLE}"
    )
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.agent_invocations TO {RUNTIME_ROLE}")


def downgrade() -> None:
    op.drop_index(
        "ix_runtime_agent_invocations_turn_order",
        table_name="agent_invocations",
        schema=SCHEMA,
    )
    op.drop_table("agent_invocations", schema=SCHEMA)
    op.drop_index(
        "ix_runtime_agent_turns_owner_run_created", table_name="agent_turns", schema=SCHEMA
    )
    op.drop_table("agent_turns", schema=SCHEMA)
