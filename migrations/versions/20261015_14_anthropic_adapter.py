"""Allow the pinned Anthropic turn and persist cache-creation usage.

Revision ID: 20261015_14
Revises: 20261015_13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20261015_14"
down_revision = "20261015_13"
branch_labels = None
depends_on = None
SCHEMA = "runtime_private"


def upgrade() -> None:
    op.drop_constraint("ck_runtime_agent_turns_model", "agent_turns", schema=SCHEMA)
    op.create_check_constraint(
        "ck_runtime_agent_turns_model",
        "agent_turns",
        "model_id IN ('scripted-v1', 'gpt-5.4-mini-2026-03-17', 'claude-sonnet-5-5')",
        schema=SCHEMA,
    )
    op.add_column(
        "agent_turns",
        sa.Column("cache_creation_input_tokens", sa.Integer(), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_agent_turns_cache_creation_usage",
        "agent_turns",
        "cache_creation_input_tokens IS NULL OR (input_tokens IS NOT NULL AND "
        "cache_creation_input_tokens BETWEEN 0 AND input_tokens)",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_agent_turns_cache_total_usage",
        "agent_turns",
        "cached_input_tokens IS NULL OR cache_creation_input_tokens IS NULL OR "
        "input_tokens IS NULL OR cached_input_tokens + cache_creation_input_tokens <= input_tokens",
        schema=SCHEMA,
    )
    op.execute(
        f"GRANT UPDATE (cache_creation_input_tokens) ON {SCHEMA}.agent_turns TO continuum_runtime"
    )


def downgrade() -> None:
    bind = op.get_bind()
    anthro_rows = bind.exec_driver_sql(
        "SELECT count(*) FROM runtime_private.agent_turns WHERE model_id='claude-sonnet-5-5'"
    ).scalar_one()
    if anthro_rows:
        raise RuntimeError("cannot downgrade while Anthropic model turns exist")
    op.drop_constraint("ck_runtime_agent_turns_cache_total_usage", "agent_turns", schema=SCHEMA)
    op.drop_constraint("ck_runtime_agent_turns_cache_creation_usage", "agent_turns", schema=SCHEMA)
    op.drop_column("agent_turns", "cache_creation_input_tokens", schema=SCHEMA)
    op.drop_constraint("ck_runtime_agent_turns_model", "agent_turns", schema=SCHEMA)
    op.create_check_constraint(
        "ck_runtime_agent_turns_model",
        "agent_turns",
        "model_id IN ('scripted-v1', 'gpt-5.4-mini-2026-03-17')",
        schema=SCHEMA,
    )
