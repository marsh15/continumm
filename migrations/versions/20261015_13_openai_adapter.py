"""Extend durable model turns for the pinned OpenAI adapter.

Revision ID: 20261015_13
Revises: 20261015_12
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20261015_13"
down_revision = "20261015_12"
branch_labels = None
depends_on = None
SCHEMA = "runtime_private"
TABLE = f"{SCHEMA}.agent_turns"


def upgrade() -> None:
    op.drop_constraint("ck_runtime_agent_turns_model", "agent_turns", schema=SCHEMA)
    op.create_check_constraint(
        "ck_runtime_agent_turns_model",
        "agent_turns",
        "model_id IN ('scripted-v1', 'gpt-5.4-mini-2026-03-17')",
        schema=SCHEMA,
    )
    op.drop_constraint("ck_runtime_agent_turns_response_outcome", "agent_turns", schema=SCHEMA)
    op.create_check_constraint(
        "ck_runtime_agent_turns_response_outcome",
        "agent_turns",
        "(status = 'response_accepted' AND response_error_code IS NULL) OR "
        "(status = 'invalid_response' AND response_error_code IS NOT NULL) OR "
        "status NOT IN ('response_accepted', 'invalid_response')",
        schema=SCHEMA,
    )
    op.add_column(
        "agent_turns",
        sa.Column("cached_input_tokens", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "agent_turns",
        sa.Column("reasoning_output_tokens", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "agent_turns",
        sa.Column("provider_request_id", sa.String(128), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "agent_turns",
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_agent_turns_cached_usage",
        "agent_turns",
        "cached_input_tokens IS NULL OR (input_tokens IS NOT NULL AND "
        "cached_input_tokens BETWEEN 0 AND input_tokens)",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_agent_turns_reasoning_usage",
        "agent_turns",
        "reasoning_output_tokens IS NULL OR (output_tokens IS NOT NULL AND "
        "reasoning_output_tokens BETWEEN 0 AND output_tokens)",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_agent_turns_latency",
        "agent_turns",
        "latency_ms IS NULL OR latency_ms BETWEEN 0 AND 600000",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_agent_turns_provider_request_id",
        "agent_turns",
        "provider_request_id IS NULL OR provider_request_id ~ '^[A-Za-z0-9_.:-]{1,128}$'",
        schema=SCHEMA,
    )
    op.execute(
        f"GRANT UPDATE (cached_input_tokens, reasoning_output_tokens, provider_request_id, "
        f"latency_ms) ON {TABLE} TO continuum_runtime"
    )


def downgrade() -> None:
    bind = op.get_bind()
    openai_rows = bind.exec_driver_sql(
        "SELECT count(*) FROM runtime_private.agent_turns WHERE model_id='gpt-5.4-mini-2026-03-17'"
    ).scalar_one()
    unknown_usage_rows = bind.exec_driver_sql(
        "SELECT count(*) FROM runtime_private.agent_turns "
        "WHERE status='response_accepted' AND (input_tokens IS NULL OR output_tokens IS NULL)"
    ).scalar_one()
    if openai_rows or unknown_usage_rows:
        raise RuntimeError(
            "cannot downgrade OpenAI turn metadata while OpenAI or unknown-usage turns exist"
        )
    op.drop_constraint("ck_runtime_agent_turns_provider_request_id", "agent_turns", schema=SCHEMA)
    op.drop_constraint("ck_runtime_agent_turns_latency", "agent_turns", schema=SCHEMA)
    op.drop_constraint("ck_runtime_agent_turns_reasoning_usage", "agent_turns", schema=SCHEMA)
    op.drop_constraint("ck_runtime_agent_turns_cached_usage", "agent_turns", schema=SCHEMA)
    op.drop_column("agent_turns", "latency_ms", schema=SCHEMA)
    op.drop_column("agent_turns", "provider_request_id", schema=SCHEMA)
    op.drop_column("agent_turns", "reasoning_output_tokens", schema=SCHEMA)
    op.drop_column("agent_turns", "cached_input_tokens", schema=SCHEMA)
    op.drop_constraint("ck_runtime_agent_turns_response_outcome", "agent_turns", schema=SCHEMA)
    op.create_check_constraint(
        "ck_runtime_agent_turns_response_outcome",
        "agent_turns",
        "(status = 'response_accepted' AND response_error_code IS NULL "
        "AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL) OR "
        "(status = 'invalid_response' AND response_error_code IS NOT NULL) OR "
        "status NOT IN ('response_accepted', 'invalid_response')",
        schema=SCHEMA,
    )
    op.drop_constraint("ck_runtime_agent_turns_model", "agent_turns", schema=SCHEMA)
    op.create_check_constraint(
        "ck_runtime_agent_turns_model",
        "agent_turns",
        "model_id = 'scripted-v1'",
        schema=SCHEMA,
    )
