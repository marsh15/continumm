"""Persist opaque login sessions, one-use OAuth state and encrypted GitHub tokens.

Revision ID: 20261017_18
Revises: 20261016_17

The migration connection must be an operator-controlled PostgreSQL owner/bypass-RLS
role. Runtime gets only EXECUTE on fixed-search-path SECURITY DEFINER entry points;
it receives no direct access to auth tables or INSERT on runtime_private.users.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20261017_18"
down_revision = "20261016_17"
branch_labels = None
depends_on = None

SCHEMA = "runtime_private"
RUNTIME_ROLE = "continuum_runtime"


def _function(name: str, signature: str) -> None:
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{name}({signature}) FROM PUBLIC")
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.{name}({signature}) TO {RUNTIME_ROLE}")


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("github_login", sa.String(length=39), nullable=True),
        schema=SCHEMA,
    )
    op.create_check_constraint(
        "ck_runtime_users_github_login",
        "users",
        "github_login IS NULL OR github_login ~ '^[A-Za-z0-9-]{1,39}$'",
        schema=SCHEMA,
    )
    op.create_unique_constraint(
        "uq_runtime_users_id_github_id",
        "users",
        ["user_id", "github_user_id"],
        schema=SCHEMA,
    )

    op.create_table(
        "github_user_tokens",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("github_user_id", sa.BigInteger(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("key_version", sa.String(length=32), nullable=False),
        sa.Column("nonce", sa.LargeBinary()),
        sa.Column("ciphertext", sa.LargeBinary()),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="active"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("owner_id", name="pk_runtime_github_user_tokens"),
        sa.UniqueConstraint("github_user_id", name="uq_runtime_github_user_tokens_github_id"),
        sa.UniqueConstraint("token_id", name="uq_runtime_github_user_tokens_token_id"),
        sa.UniqueConstraint("key_version", "nonce", name="uq_runtime_github_user_tokens_nonce"),
        sa.ForeignKeyConstraint(
            ["owner_id", "github_user_id"],
            [f"{SCHEMA}.users.user_id", f"{SCHEMA}.users.github_user_id"],
            name="fk_runtime_github_user_tokens_identity",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("github_user_id > 0", name="ck_runtime_github_user_tokens_id"),
        sa.CheckConstraint(
            "key_version ~ '^[A-Za-z0-9_-]{1,32}$'",
            name="ck_runtime_github_user_tokens_key_version",
        ),
        sa.CheckConstraint(
            "(status='active' AND nonce IS NOT NULL AND octet_length(nonce)=12 "
            "AND ciphertext IS NOT NULL AND octet_length(ciphertext) BETWEEN 17 AND 4112) OR "
            "(status='revoked' AND nonce IS NULL AND ciphertext IS NULL)",
            name="ck_runtime_github_user_tokens_material",
        ),
        sa.CheckConstraint(
            "status IN ('active', 'revoked')", name="ck_runtime_github_user_tokens_status"
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "browser_sessions",
        sa.Column("session_hash", sa.LargeBinary(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("csrf_hash", sa.LargeBinary(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.PrimaryKeyConstraint("session_hash", name="pk_runtime_browser_sessions"),
        sa.ForeignKeyConstraint(["user_id"], [f"{SCHEMA}.users.user_id"], ondelete="CASCADE"),
        sa.CheckConstraint("octet_length(session_hash)=32", name="ck_browser_sessions_hash"),
        sa.CheckConstraint("octet_length(csrf_hash)=32", name="ck_browser_sessions_csrf_hash"),
        sa.CheckConstraint("expires_at > created_at", name="ck_browser_sessions_expiry"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_browser_sessions_user_expiry",
        "browser_sessions",
        ["user_id", "expires_at"],
        schema=SCHEMA,
    )
    op.create_index(
        "ix_browser_sessions_expiry",
        "browser_sessions",
        ["expires_at"],
        schema=SCHEMA,
    )
    op.create_table(
        "github_oauth_states",
        sa.Column("state_hash", sa.LargeBinary(), nullable=False),
        sa.Column("browser_hash", sa.LargeBinary(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True)),
        sa.PrimaryKeyConstraint("state_hash", name="pk_runtime_github_oauth_states"),
        sa.CheckConstraint("octet_length(state_hash)=32", name="ck_github_oauth_states_hash"),
        sa.CheckConstraint("octet_length(browser_hash)=32", name="ck_github_oauth_states_browser"),
        sa.CheckConstraint("expires_at > created_at", name="ck_github_oauth_states_expiry"),
        schema=SCHEMA,
    )
    op.create_table(
        "github_oauth_rate_limits",
        sa.Column("address_hash", sa.LargeBinary(), nullable=False),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("address_hash", name="pk_runtime_github_oauth_rate_limits"),
        sa.CheckConstraint(
            "octet_length(address_hash)=32", name="ck_github_oauth_rate_limits_hash"
        ),
        sa.CheckConstraint(
            "attempts BETWEEN 1 AND 101", name="ck_github_oauth_rate_limits_attempts"
        ),
        schema=SCHEMA,
    )

    for table in (
        "github_user_tokens",
        "browser_sessions",
        "github_oauth_states",
        "github_oauth_rate_limits",
    ):
        op.execute(f"ALTER TABLE {SCHEMA}.{table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {SCHEMA}.{table} FORCE ROW LEVEL SECURITY")
        op.execute(f"REVOKE ALL ON {SCHEMA}.{table} FROM PUBLIC, {RUNTIME_ROLE}")

    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.auth_upsert_github_user(
          p_new_owner uuid, p_github_user_id bigint, p_login text
        ) RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE v_user_id uuid;
        BEGIN
          IF p_new_owner IS NULL OR p_github_user_id IS NULL OR p_github_user_id <= 0
             OR p_login IS NULL
             OR p_login !~ '^[A-Za-z0-9-]{{1,39}}$' THEN
            RAISE EXCEPTION 'invalid_auth_input' USING ERRCODE='22023';
          END IF;
          INSERT INTO runtime_private.users(user_id, github_user_id, github_login, status)
          VALUES (p_new_owner, p_github_user_id, p_login, 'active')
          ON CONFLICT (github_user_id) DO UPDATE SET github_login=EXCLUDED.github_login
          WHERE runtime_private.users.status='active'
          RETURNING runtime_private.users.user_id INTO v_user_id;
          RETURN v_user_id;
        END $$
    """)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.auth_issue_oauth_state(p_state_hash bytea, p_browser_hash bytea)
        RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          IF p_state_hash IS NULL OR p_browser_hash IS NULL
             OR octet_length(p_state_hash) <> 32 OR octet_length(p_browser_hash) <> 32 THEN
            RAISE EXCEPTION 'invalid_auth_input' USING ERRCODE='22023';
          END IF;
          UPDATE runtime_private.github_user_tokens
          SET status='revoked', nonce=NULL, ciphertext=NULL, updated_at=clock_timestamp()
          WHERE status='active' AND expires_at <= clock_timestamp();
          DELETE FROM runtime_private.github_oauth_states
          WHERE expires_at < clock_timestamp() - interval '1 day';
          DELETE FROM runtime_private.browser_sessions
          WHERE expires_at < clock_timestamp() - interval '1 day'
             OR revoked_at < clock_timestamp() - interval '1 day';
          INSERT INTO runtime_private.github_oauth_states
            (state_hash, browser_hash, created_at, expires_at)
          VALUES (p_state_hash, p_browser_hash, clock_timestamp(),
                  clock_timestamp() + interval '10 minutes');
        END $$
    """)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.auth_consume_oauth_state(p_state_hash bytea, p_browser_hash bytea)
        RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE saved_browser_hash bytea; state_expires_at timestamptz;
        BEGIN
          IF p_state_hash IS NULL OR p_browser_hash IS NULL
             OR octet_length(p_state_hash) <> 32 OR octet_length(p_browser_hash) <> 32 THEN
            RETURN false;
          END IF;
          SELECT browser_hash, expires_at
          INTO saved_browser_hash, state_expires_at
          FROM runtime_private.github_oauth_states
          WHERE state_hash=p_state_hash AND consumed_at IS NULL
          FOR UPDATE;
          IF NOT FOUND OR saved_browser_hash <> p_browser_hash
             OR state_expires_at <= clock_timestamp() THEN
            RETURN false;
          END IF;
          UPDATE runtime_private.github_oauth_states
          SET consumed_at=clock_timestamp()
          WHERE state_hash=p_state_hash AND consumed_at IS NULL;
          RETURN true;
        END $$
    """)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.auth_allow_oauth_attempt(
          p_address_hash bytea, p_attempt_limit integer, p_window_seconds integer
        ) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE attempts integer;
        BEGIN
          IF p_address_hash IS NULL OR octet_length(p_address_hash) <> 32
             OR p_attempt_limit NOT BETWEEN 1 AND 100
             OR p_window_seconds NOT BETWEEN 1 AND 86400 THEN
            RETURN false;
          END IF;
          DELETE FROM runtime_private.github_oauth_rate_limits
          WHERE window_started_at < clock_timestamp() - interval '1 day';
          INSERT INTO runtime_private.github_oauth_rate_limits
            (address_hash, window_started_at, attempts)
          VALUES (p_address_hash, clock_timestamp(), 1)
          ON CONFLICT (address_hash) DO UPDATE SET
            window_started_at=CASE
              WHEN runtime_private.github_oauth_rate_limits.window_started_at
                   + make_interval(secs => p_window_seconds) <= clock_timestamp()
              THEN clock_timestamp()
              ELSE runtime_private.github_oauth_rate_limits.window_started_at END,
            attempts=CASE
              WHEN runtime_private.github_oauth_rate_limits.window_started_at
                   + make_interval(secs => p_window_seconds) <= clock_timestamp()
              THEN 1
              ELSE least(runtime_private.github_oauth_rate_limits.attempts + 1,
                         p_attempt_limit + 1) END
          RETURNING runtime_private.github_oauth_rate_limits.attempts INTO attempts;
          RETURN attempts <= p_attempt_limit;
        END $$
    """)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.auth_complete_github_login(
          p_owner_id uuid, p_github_user_id bigint, p_login text, p_token_id uuid,
          p_key_version text, p_nonce bytea, p_ciphertext bytea, p_token_expires_in integer,
          p_session_hash bytea, p_csrf_hash bytea, p_prior_session_hash bytea
        ) RETURNS TABLE(user_id uuid, github_user_id bigint, login text, expires_at timestamptz)
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE v_user_id uuid; v_expires_at timestamptz; v_created_at timestamptz;
        BEGIN
          IF p_owner_id IS NULL OR p_github_user_id IS NULL OR p_github_user_id <= 0
             OR p_login IS NULL
             OR p_login !~ '^[A-Za-z0-9-]{{1,39}}$'
             OR p_token_expires_in NOT BETWEEN 1 AND 28800
             OR p_key_version !~ '^[A-Za-z0-9_-]{{1,32}}$'
             OR octet_length(p_nonce) <> 12
             OR octet_length(p_ciphertext) NOT BETWEEN 17 AND 4112
             OR octet_length(p_session_hash) <> 32 OR octet_length(p_csrf_hash) <> 32
             OR (p_prior_session_hash IS NOT NULL AND octet_length(p_prior_session_hash) <> 32) THEN
            RAISE EXCEPTION 'invalid_auth_input' USING ERRCODE='22023';
          END IF;
          SELECT u.user_id INTO v_user_id FROM runtime_private.users AS u
          WHERE u.user_id=p_owner_id AND u.github_user_id=p_github_user_id
            AND u.status='active';
          IF v_user_id IS NULL THEN RETURN; END IF;

          IF p_prior_session_hash IS NOT NULL THEN
            UPDATE runtime_private.browser_sessions SET revoked_at=clock_timestamp()
            WHERE session_hash=p_prior_session_hash AND revoked_at IS NULL;
          END IF;
          INSERT INTO runtime_private.github_user_tokens(
            owner_id, github_user_id, token_id, key_version, nonce,
            ciphertext, expires_at, updated_at
          ) VALUES (
            v_user_id, p_github_user_id, p_token_id, p_key_version, p_nonce, p_ciphertext,
            clock_timestamp() + make_interval(secs => p_token_expires_in), clock_timestamp()
          ) ON CONFLICT (owner_id) DO UPDATE SET
            token_id=EXCLUDED.token_id, key_version=EXCLUDED.key_version, nonce=EXCLUDED.nonce,
            ciphertext=EXCLUDED.ciphertext, expires_at=EXCLUDED.expires_at,
            status='active', updated_at=clock_timestamp();

          v_created_at := clock_timestamp();
          v_expires_at := v_created_at + interval '7 days';
          INSERT INTO runtime_private.browser_sessions(
            session_hash, user_id, csrf_hash, created_at, expires_at
          ) VALUES (p_session_hash, v_user_id, p_csrf_hash, v_created_at, v_expires_at);
          RETURN QUERY SELECT v_user_id, p_github_user_id, p_login, v_expires_at;
        END $$
    """)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.auth_resolve_session(p_session_hash bytea, p_csrf_hash bytea)
        RETURNS TABLE(user_id uuid, github_user_id bigint, login text,
                      expires_at timestamptz, csrf_valid boolean)
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          IF octet_length(p_session_hash) <> 32
             OR (p_csrf_hash IS NOT NULL AND octet_length(p_csrf_hash) <> 32) THEN
            RETURN;
          END IF;
          RETURN QUERY
          SELECT s.user_id, u.github_user_id, u.github_login::text, s.expires_at,
                 p_csrf_hash IS NOT NULL AND s.csrf_hash=p_csrf_hash
          FROM runtime_private.browser_sessions AS s
          JOIN runtime_private.users AS u ON u.user_id=s.user_id
          WHERE s.session_hash=p_session_hash AND s.revoked_at IS NULL
            AND s.expires_at > clock_timestamp() AND u.status='active';
        END $$
    """)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.auth_revoke_session(p_session_hash bytea)
        RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          IF octet_length(p_session_hash)=32 THEN
            UPDATE runtime_private.browser_sessions SET revoked_at=clock_timestamp()
            WHERE session_hash=p_session_hash AND revoked_at IS NULL;
          END IF;
        END $$
    """)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.auth_revoke_account(p_owner_id uuid)
        RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          UPDATE runtime_private.browser_sessions SET revoked_at=clock_timestamp()
          WHERE user_id=p_owner_id AND revoked_at IS NULL;
          UPDATE runtime_private.github_user_tokens SET status='revoked', nonce=NULL,
            ciphertext=NULL, updated_at=clock_timestamp() WHERE owner_id=p_owner_id;
        END $$
    """)
    op.execute(f"""
        CREATE FUNCTION {SCHEMA}.auth_get_github_token(p_owner_id uuid, p_github_user_id bigint)
        RETURNS TABLE(token_id uuid, key_version text, nonce bytea, ciphertext bytea)
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          IF p_owner_id IS NULL OR p_github_user_id IS NULL OR p_github_user_id <= 0 THEN
            RETURN;
          END IF;
          RETURN QUERY
          SELECT t.token_id, t.key_version::text, t.nonce, t.ciphertext
          FROM runtime_private.github_user_tokens AS t
          JOIN runtime_private.users AS u
            ON u.user_id=t.owner_id AND u.github_user_id=t.github_user_id
          WHERE t.owner_id=p_owner_id AND t.github_user_id=p_github_user_id
            AND t.expires_at > clock_timestamp() AND u.status='active'
            AND t.status='active'
            AND t.nonce IS NOT NULL AND t.ciphertext IS NOT NULL;
        END $$
    """)

    for name, signature in (
        ("auth_issue_oauth_state", "bytea, bytea"),
        ("auth_upsert_github_user", "uuid, bigint, text"),
        ("auth_consume_oauth_state", "bytea, bytea"),
        ("auth_allow_oauth_attempt", "bytea, integer, integer"),
        (
            "auth_complete_github_login",
            "uuid, bigint, text, uuid, text, bytea, bytea, integer, bytea, bytea, bytea",
        ),
        ("auth_resolve_session", "bytea, bytea"),
        ("auth_revoke_session", "bytea"),
        ("auth_revoke_account", "uuid"),
        ("auth_get_github_token", "uuid, bigint"),
    ):
        _function(name, signature)


def downgrade() -> None:
    for name, signature in (
        ("auth_get_github_token", "uuid, bigint"),
        ("auth_revoke_account", "uuid"),
        ("auth_revoke_session", "bytea"),
        ("auth_resolve_session", "bytea, bytea"),
        (
            "auth_complete_github_login",
            "uuid, bigint, text, uuid, text, bytea, bytea, integer, bytea, bytea, bytea",
        ),
        ("auth_allow_oauth_attempt", "bytea, integer, integer"),
        ("auth_consume_oauth_state", "bytea, bytea"),
        ("auth_issue_oauth_state", "bytea, bytea"),
        ("auth_upsert_github_user", "uuid, bigint, text"),
    ):
        op.execute(f"DROP FUNCTION {SCHEMA}.{name}({signature})")
    for table in (
        "github_oauth_rate_limits",
        "github_oauth_states",
        "browser_sessions",
        "github_user_tokens",
    ):
        op.drop_table(table, schema=SCHEMA)
    op.drop_constraint("ck_runtime_users_github_login", "users", schema=SCHEMA)
    op.drop_constraint("uq_runtime_users_id_github_id", "users", schema=SCHEMA)
    op.drop_column("users", "github_login", schema=SCHEMA)
