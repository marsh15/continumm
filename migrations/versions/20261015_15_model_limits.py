"""Pin a run's model and persist cumulative model budgets.

Revision ID: 20261015_15
Revises: 20261015_14
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261015_15"
down_revision = "20261015_14"
branch_labels = None
depends_on = None
SCHEMA = "runtime_private"


def upgrade() -> None:
    op.add_column("runs", sa.Column("model_id", sa.String(64), nullable=True), schema=SCHEMA)
    op.add_column("runs", sa.Column("model_task_context", JSONB(), nullable=True), schema=SCHEMA)
    op.add_column(
        "runs",
        sa.Column("model_tool_schema_sha256", sa.LargeBinary(32), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "runs",
        sa.Column("model_requests_reserved", sa.Integer(), nullable=False, server_default="0"),
        schema=SCHEMA,
    )
    for column in (
        "model_requests_authorized",
        "model_requests_received",
        "model_requests_ambiguous",
        "model_usage_unknown_requests",
    ):
        op.add_column(
            "runs",
            sa.Column(column, sa.Integer(), nullable=False, server_default="0"),
            schema=SCHEMA,
        )
    op.add_column(
        "runs",
        sa.Column("model_input_tokens_used", sa.Integer(), nullable=False, server_default="0"),
        schema=SCHEMA,
    )
    op.add_column(
        "runs",
        sa.Column("model_input_tokens_reserved", sa.Integer(), nullable=False, server_default="0"),
        schema=SCHEMA,
    )
    op.add_column(
        "runs",
        sa.Column("model_output_tokens_used", sa.Integer(), nullable=False, server_default="0"),
        schema=SCHEMA,
    )
    op.add_column(
        "runs",
        sa.Column("model_output_tokens_reserved", sa.Integer(), nullable=False, server_default="0"),
        schema=SCHEMA,
    )
    op.add_column(
        "runs",
        sa.Column("model_budget_exceeded", sa.Boolean(), nullable=False, server_default="false"),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_runs_model_id",
        "runs",
        "model_id IS NULL OR model_id IN "
        "('scripted-v1', 'gpt-5.4-mini-2026-03-17', 'claude-sonnet-5-5')",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_runs_model_task_context",
        "runs",
        "model_task_context IS NULL OR (jsonb_typeof(model_task_context)='object' AND "
        "octet_length(model_task_context::text)<=65536)",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_runs_model_tool_schema_hash",
        "runs",
        "model_tool_schema_sha256 IS NULL OR octet_length(model_tool_schema_sha256)=32",
        schema=SCHEMA,
    )
    for name, column in (
        ("ck_runtime_runs_model_requests", "model_requests_reserved"),
        ("ck_runtime_runs_model_authorized", "model_requests_authorized"),
        ("ck_runtime_runs_model_received", "model_requests_received"),
        ("ck_runtime_runs_model_ambiguous", "model_requests_ambiguous"),
        ("ck_runtime_runs_model_usage_unknown", "model_usage_unknown_requests"),
        ("ck_runtime_runs_model_input_used", "model_input_tokens_used"),
        ("ck_runtime_runs_model_input_reserved", "model_input_tokens_reserved"),
        ("ck_runtime_runs_model_output_used", "model_output_tokens_used"),
        ("ck_runtime_runs_model_output_reserved", "model_output_tokens_reserved"),
    ):
        op.create_check_constraint(name, "runs", f"{column} >= 0", schema=SCHEMA)

    op.add_column(
        "agent_turns",
        sa.Column("source_sha256", sa.LargeBinary(32), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "agent_turns",
        sa.Column("source_request_sha256", sa.LargeBinary(32), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "agent_turns",
        sa.Column("input_tokens_reserved", sa.Integer(), nullable=False, server_default="0"),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_agent_turns_source_hash",
        "agent_turns",
        "source_sha256 IS NULL OR octet_length(source_sha256)=32",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_agent_turns_source_request_hash",
        "agent_turns",
        "source_request_sha256 IS NULL OR octet_length(source_request_sha256)=32",
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_agent_turns_input_token_reservation",
        "agent_turns",
        "input_tokens_reserved BETWEEN 0 AND 120000",
        schema=SCHEMA,
    )
    # Older turns do not carry the context-builder source manifest. Keep that
    # provenance unknown; request_sha256 has a different meaning.
    op.execute(
        f"UPDATE {SCHEMA}.agent_turns SET "
        "input_tokens_reserved=LEAST(120000,input_bytes_reserved+1024)"
    )
    op.execute(
        f"WITH totals AS (SELECT owner_id, run_id, count(*)::integer AS requests, "
        "count(*) FILTER (WHERE dispatch_authorized_at IS NOT NULL)::integer AS authorized, "
        "count(*) FILTER (WHERE response_received_at IS NOT NULL)::integer AS received, "
        "count(*) FILTER (WHERE status='unknown')::integer AS ambiguous, "
        "count(DISTINCT model_id)::integer AS model_count, min(model_id) AS model_id, "
        "COALESCE(sum(CASE WHEN status='response_accepted' AND input_tokens IS NOT NULL "
        "THEN input_tokens ELSE 0 END),0)::integer AS input_used, "
        "COALESCE(sum(CASE WHEN status='response_accepted' AND input_tokens IS NOT NULL "
        "THEN 0 ELSE input_tokens_reserved END),0)::integer AS input_reserved, "
        "COALESCE(sum(CASE WHEN status='response_accepted' AND output_tokens IS NOT NULL "
        "THEN output_tokens ELSE 0 END),0)::integer AS output_used, "
        "COALESCE(sum(CASE WHEN status='response_accepted' AND output_tokens IS NOT NULL "
        "THEN 0 ELSE output_tokens_reserved END),0)::integer AS output_reserved, "
        "count(*) FILTER (WHERE status IN ('dispatching','unknown','invalid_response') OR "
        "(status='response_accepted' AND "
        "(input_tokens IS NULL OR output_tokens IS NULL)))::integer "
        "AS unknown_requests "
        f"FROM {SCHEMA}.agent_turns GROUP BY owner_id,run_id) "
        f"UPDATE {SCHEMA}.runs AS run SET model_id=CASE WHEN totals.model_count=1 "
        "THEN totals.model_id ELSE NULL END, "
        "model_requests_reserved=totals.requests, "
        "model_requests_authorized=totals.authorized, "
        "model_requests_received=totals.received, "
        "model_requests_ambiguous=totals.ambiguous, "
        "model_usage_unknown_requests=totals.unknown_requests, "
        "model_input_tokens_used=totals.input_used, "
        "model_input_tokens_reserved=totals.input_reserved, "
        "model_output_tokens_used=totals.output_used, "
        "model_output_tokens_reserved=totals.output_reserved, "
        "model_budget_exceeded=(totals.model_count<>1 OR totals.requests>30 OR "
        "totals.input_used+totals.input_reserved>120000 OR "
        "totals.output_used+totals.output_reserved>30000) "
        "FROM totals WHERE run.owner_id=totals.owner_id AND run.run_id=totals.run_id"
    )
    op.execute(
        f"WITH first_turn AS (SELECT DISTINCT ON (owner_id,run_id) owner_id,run_id,context "
        f"FROM {SCHEMA}.agent_turns ORDER BY owner_id,run_id,created_at,turn_id), "
        "pinned AS (SELECT owner_id,run_id,jsonb_strip_nulls(jsonb_build_object("
        "'task',context->'task','objective',context->'objective',"
        "'constraints',context->'constraints','policy',context->'policy')) AS task_context "
        "FROM first_turn) "
        f"UPDATE {SCHEMA}.runs AS run SET model_task_context=pinned.task_context, "
        "model_budget_exceeded=(run.model_budget_exceeded OR "
        "NOT (pinned.task_context ? 'task' OR pinned.task_context ? 'objective')) "
        "FROM pinned WHERE run.owner_id=pinned.owner_id AND run.run_id=pinned.run_id"
    )
    op.execute(
        f"GRANT UPDATE (model_requests_reserved, model_requests_authorized, "
        f"model_requests_received, model_requests_ambiguous, model_usage_unknown_requests, "
        f"model_input_tokens_used, "
        f"model_input_tokens_reserved, model_output_tokens_used, model_output_tokens_reserved, "
        f"model_budget_exceeded, model_task_context, model_tool_schema_sha256) "
        f"ON {SCHEMA}.runs TO continuum_runtime"
    )
    op.execute(
        f"GRANT UPDATE (source_request_sha256, input_tokens_reserved) "
        f"ON {SCHEMA}.agent_turns TO continuum_runtime"
    )


def downgrade() -> None:
    bind = op.get_bind()
    provider_rows = bind.exec_driver_sql(
        "SELECT count(*) FROM runtime_private.agent_turns WHERE model_id IN "
        "('gpt-5.4-mini-2026-03-17', 'claude-sonnet-5-5')"
    ).scalar_one()
    if provider_rows:
        raise RuntimeError("cannot downgrade while real-provider turn records exist")
    op.drop_constraint(
        "ck_runtime_agent_turns_input_token_reservation", "agent_turns", schema=SCHEMA
    )
    op.drop_constraint("ck_runtime_agent_turns_source_request_hash", "agent_turns", schema=SCHEMA)
    op.drop_constraint("ck_runtime_agent_turns_source_hash", "agent_turns", schema=SCHEMA)
    op.drop_column("agent_turns", "input_tokens_reserved", schema=SCHEMA)
    op.drop_column("agent_turns", "source_request_sha256", schema=SCHEMA)
    op.drop_column("agent_turns", "source_sha256", schema=SCHEMA)
    for name in (
        "ck_runtime_runs_model_tool_schema_hash",
        "ck_runtime_runs_model_task_context",
        "ck_runtime_runs_model_authorized",
        "ck_runtime_runs_model_received",
        "ck_runtime_runs_model_ambiguous",
        "ck_runtime_runs_model_usage_unknown",
        "ck_runtime_runs_model_output_reserved",
        "ck_runtime_runs_model_output_used",
        "ck_runtime_runs_model_input_reserved",
        "ck_runtime_runs_model_input_used",
        "ck_runtime_runs_model_requests",
        "ck_runtime_runs_model_id",
    ):
        op.drop_constraint(name, "runs", schema=SCHEMA)
    for column in (
        "model_budget_exceeded",
        "model_output_tokens_reserved",
        "model_output_tokens_used",
        "model_input_tokens_reserved",
        "model_input_tokens_used",
        "model_requests_reserved",
        "model_usage_unknown_requests",
        "model_requests_ambiguous",
        "model_requests_received",
        "model_requests_authorized",
        "model_id",
        "model_tool_schema_sha256",
        "model_task_context",
    ):
        op.drop_column("runs", column, schema=SCHEMA)
