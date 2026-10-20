"""Add restricted operator controls, audit records, and safe metadata readers.

Revision ID: 20261020_28
Revises: 20261020_27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20261020_28"
down_revision = "20261020_27"
branch_labels = None
depends_on = None

SCHEMA = "operator_private"
OPERATOR_ROLE = "continuum_operator"
RUNTIME_ROLE = "continuum_runtime"

_REASONS = (
    "quota_exhausted",
    "database_pause",
    "failed_delivery",
    "lost_allocation",
    "uncertain_publication",
    "security",
    "storage_pressure",
    "other",
)


def upgrade() -> None:
    _validate_operator_role()
    _grant_connect()
    op.execute(f"CREATE SCHEMA {SCHEMA}")
    op.execute(f"REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC")
    op.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {OPERATOR_ROLE}, {RUNTIME_ROLE}")

    op.create_table(
        "global_control",
        sa.Column("singleton", sa.Boolean(), primary_key=True, server_default=sa.true()),
        sa.Column("admissions_stopped", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.CheckConstraint("singleton", name="ck_operator_control_singleton"),
        schema=SCHEMA,
    )
    op.execute(f"INSERT INTO {SCHEMA}.global_control (singleton) VALUES (true)")
    op.create_table(
        "audit_events",
        sa.Column(
            "audit_id",
            sa.Uuid(),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("actor_github_user_id", sa.BigInteger(), nullable=False),
        sa.Column("action", sa.String(32), nullable=False),
        sa.Column("target_user_id", sa.Uuid(), nullable=True),
        sa.Column("reason_code", sa.String(32), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.CheckConstraint("actor_github_user_id > 0", name="ck_operator_audit_actor_positive"),
        sa.CheckConstraint(
            "action IN ('admission_stop','admission_resume','account_suspend',"
            "'account_reactivate','credential_revoke')",
            name="ck_operator_audit_action",
        ),
        sa.CheckConstraint(
            "reason_code IS NULL OR reason_code IN "
            "('quota_exhausted','database_pause','failed_delivery','lost_allocation',"
            "'uncertain_publication','security','storage_pressure','other')",
            name="ck_operator_audit_reason",
        ),
        sa.CheckConstraint(
            "(action IN ('admission_stop','admission_resume') AND target_user_id IS NULL) OR "
            "(action IN ('account_suspend','account_reactivate','credential_revoke') "
            "AND target_user_id IS NOT NULL)",
            name="ck_operator_audit_target",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_operator_audit_created",
        "audit_events",
        ["created_at", "audit_id"],
        schema=SCHEMA,
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.global_control, {SCHEMA}.audit_events FROM PUBLIC")
    op.execute(f"GRANT SELECT ON {SCHEMA}.global_control, {SCHEMA}.audit_events TO {OPERATOR_ROLE}")

    _create_functions()
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.read_overview() TO {OPERATOR_ROLE}")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.read_unknown_effects(integer) TO {OPERATOR_ROLE}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.read_unknown_allocations(integer) TO {OPERATOR_ROLE}"
    )
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.read_pending_inbox(integer) TO {OPERATOR_ROLE}")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.read_suspended_accounts(integer) TO {OPERATOR_ROLE}"
    )
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.read_audit_events(integer) TO {OPERATOR_ROLE}")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.set_admission_stopped(boolean,bigint,text) "
        f"TO {OPERATOR_ROLE}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.set_account_suspended(bigint,boolean,bigint,text) "
        f"TO {OPERATOR_ROLE}"
    )
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {SCHEMA}.revoke_provider_credential(uuid,bigint,text) "
        f"TO {OPERATOR_ROLE}"
    )
    op.execute(f"GRANT EXECUTE ON FUNCTION {SCHEMA}.admission_allowed(uuid) TO {RUNTIME_ROLE}")


def _validate_operator_role() -> None:
    bind = op.get_bind()
    role = bind.execute(
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
        {"role_name": OPERATOR_ROLE},
    ).one_or_none()
    if role is None:
        raise RuntimeError("create_continuum_operator_role_before_migration")
    if not role.rolcanlogin or role.elevated or role.rolinherit or role.has_membership:
        raise RuntimeError("continuum_operator_role_privileges_invalid")


def _grant_connect() -> None:
    bind = op.get_bind()
    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"GRANT CONNECT ON DATABASE {quoted_database} TO {OPERATOR_ROLE}")


def _create_functions() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.admission_allowed(p_user_id uuid)
        RETURNS boolean
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE v_stopped boolean; v_status text;
        BEGIN
            SELECT admissions_stopped INTO v_stopped
            FROM {SCHEMA}.global_control WHERE singleton = true FOR SHARE;
            SELECT status INTO v_status
            FROM runtime_private.users WHERE user_id = p_user_id FOR SHARE;
            RETURN FOUND AND NOT COALESCE(v_stopped, true) AND v_status = 'active';
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.set_admission_stopped(
            p_stopped boolean, p_actor bigint, p_reason text
        ) RETURNS boolean
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE v_old boolean; v_action text;
        BEGIN
            IF p_stopped IS NULL OR p_actor IS NULL OR p_actor <= 0 OR p_reason IS NULL
               OR p_reason NOT IN {_sql_values(_REASONS)} THEN
                RAISE EXCEPTION 'operator_input_invalid' USING ERRCODE='22023';
            END IF;
            SELECT admissions_stopped INTO v_old FROM {SCHEMA}.global_control
            WHERE singleton = true FOR UPDATE;
            IF NOT FOUND THEN RAISE EXCEPTION 'operator_control_missing'; END IF;
            IF v_old = p_stopped THEN RETURN false; END IF;
            UPDATE {SCHEMA}.global_control
            SET admissions_stopped=p_stopped, updated_at=clock_timestamp()
            WHERE singleton = true;
            v_action := CASE WHEN p_stopped THEN 'admission_stop' ELSE 'admission_resume' END;
            INSERT INTO {SCHEMA}.audit_events
                (actor_github_user_id, action, reason_code)
            VALUES (p_actor, v_action, p_reason);
            RETURN true;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.set_account_suspended(
            p_github_user_id bigint, p_suspended boolean, p_actor bigint, p_reason text
        ) RETURNS boolean
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE v_user runtime_private.users%ROWTYPE; v_action text;
        BEGIN
            IF p_github_user_id IS NULL OR p_github_user_id <= 0
               OR p_actor IS NULL OR p_actor <= 0 OR p_reason IS NULL
               OR p_reason NOT IN {_sql_values(_REASONS)} THEN
                RAISE EXCEPTION 'operator_input_invalid' USING ERRCODE='22023';
            END IF;
            SELECT * INTO v_user FROM runtime_private.users
            WHERE github_user_id=p_github_user_id FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'operator_account_not_found' USING ERRCODE='P0002';
            END IF;
            IF p_suspended AND v_user.status = 'suspended' THEN RETURN false; END IF;
            IF NOT p_suspended AND v_user.status = 'active' THEN RETURN false; END IF;
            IF v_user.status NOT IN ('active','suspended') THEN
                RAISE EXCEPTION 'operator_account_not_reactivatable' USING ERRCODE='55000';
            END IF;
            UPDATE runtime_private.users SET status = CASE WHEN p_suspended THEN 'suspended'
                                                            ELSE 'active' END
            WHERE user_id=v_user.user_id;
            IF p_suspended THEN
                UPDATE runtime_private.github_user_tokens
                SET status='revoked', nonce=NULL, ciphertext=NULL, updated_at=clock_timestamp()
                WHERE owner_id=v_user.user_id AND status='active';
                UPDATE runtime_private.browser_sessions SET revoked_at=clock_timestamp()
                WHERE user_id=v_user.user_id AND revoked_at IS NULL;
            END IF;
            v_action := CASE WHEN p_suspended THEN 'account_suspend' ELSE 'account_reactivate' END;
            INSERT INTO {SCHEMA}.audit_events
                (actor_github_user_id, action, target_user_id, reason_code)
            VALUES (p_actor, v_action, v_user.user_id, p_reason);
            RETURN true;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.read_overview()
        RETURNS jsonb
        LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        SELECT jsonb_build_object(
            'admissions_stopped', control.admissions_stopped,
            'active_runs', (SELECT count(*) FROM runtime_private.runs
                WHERE status IN ('queued','running','waiting_approval','waiting_credentials',
                                 'waiting_capacity','needs_attention')),
            'unknown_effects', (SELECT count(*) FROM runtime_private.external_operations
                WHERE status IN ('unknown','conflict')),
            'uncertain_allocations', (SELECT count(*) FROM runtime_private.sandbox_allocations
                WHERE status IN ('unknown','stop_unknown','destroy_unknown')),
            'pending_inbox_deliveries', (SELECT count(*) FROM webhook_private.github_delivery_inbox
                WHERE consumed_at IS NULL),
            'oldest_pending_inbox_at', (SELECT min(received_at)
                FROM webhook_private.github_delivery_inbox WHERE consumed_at IS NULL),
            'artifact_charged_bytes', capacity.charged_bytes,
            'artifact_capacity_limit_bytes', capacity.capacity_limit_bytes,
            'suspended_accounts', (SELECT count(*) FROM runtime_private.users
                WHERE status='suspended')
        )
        FROM {SCHEMA}.global_control AS control
        JOIN runtime_private.artifact_capacity AS capacity ON capacity.capacity_id=1
        WHERE control.singleton = true
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.revoke_provider_credential(
            p_credential_id uuid, p_actor bigint, p_reason text
        ) RETURNS boolean
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE v_owner uuid;
        BEGIN
            IF p_credential_id IS NULL OR p_actor IS NULL OR p_actor <= 0 OR p_reason IS NULL
               OR p_reason NOT IN {_sql_values(_REASONS)} THEN
                RAISE EXCEPTION 'operator_input_invalid' USING ERRCODE='22023';
            END IF;
            SELECT owner_id INTO v_owner FROM runtime_private.provider_credentials
            WHERE credential_id=p_credential_id FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'operator_credential_not_found' USING ERRCODE='P0002';
            END IF;
            UPDATE runtime_private.provider_credentials
            SET status='revoked', nonce=NULL, ciphertext=NULL,
                ended_at=COALESCE(ended_at, clock_timestamp())
            WHERE credential_id=p_credential_id AND status='active';
            IF NOT FOUND THEN RETURN false; END IF;
            INSERT INTO {SCHEMA}.audit_events
                (actor_github_user_id, action, target_user_id, reason_code)
            VALUES (p_actor, 'credential_revoke', v_owner, p_reason);
            RETURN true;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.read_unknown_effects(p_limit integer)
        RETURNS TABLE(effect_id uuid, owner_id uuid, run_id uuid, status text,
                      created_at timestamptz, updated_at timestamptz)
        LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        BEGIN
            IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
                RAISE EXCEPTION 'operator_limit_invalid' USING ERRCODE='22023';
            END IF;
            RETURN QUERY SELECT op.operation_id, op.owner_id, op.run_id, op.status,
                                op.created_at, op.updated_at
                FROM runtime_private.external_operations AS op
                WHERE op.status IN ('unknown','conflict')
                ORDER BY op.updated_at, op.operation_id LIMIT p_limit;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.read_unknown_allocations(p_limit integer)
        RETURNS TABLE(allocation_id uuid, owner_id uuid, run_id uuid, status text,
                      provider_name text, created_at timestamptz, last_observed_at timestamptz)
        LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        BEGIN
            IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
                RAISE EXCEPTION 'operator_limit_invalid' USING ERRCODE='22023';
            END IF;
            RETURN QUERY SELECT alloc.allocation_id, alloc.owner_id, alloc.run_id, alloc.status,
                                alloc.provider_name, alloc.created_at, alloc.last_observed_at
                FROM runtime_private.sandbox_allocations AS alloc
                WHERE alloc.status IN ('unknown','stop_unknown','destroy_unknown')
                ORDER BY alloc.updated_at, alloc.allocation_id LIMIT p_limit;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.read_pending_inbox(p_limit integer)
        RETURNS TABLE(delivery_id uuid, event_type text, received_at timestamptz)
        LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        BEGIN
            IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
                RAISE EXCEPTION 'operator_limit_invalid' USING ERRCODE='22023';
            END IF;
            RETURN QUERY SELECT inbox.delivery_id, inbox.event_type::text, inbox.received_at
                FROM webhook_private.github_delivery_inbox AS inbox
                WHERE inbox.consumed_at IS NULL
                ORDER BY inbox.received_at, inbox.delivery_id LIMIT p_limit;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.read_suspended_accounts(p_limit integer)
        RETURNS TABLE(user_id uuid, github_user_id bigint, created_at timestamptz)
        LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        BEGIN
            IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
                RAISE EXCEPTION 'operator_limit_invalid' USING ERRCODE='22023';
            END IF;
            RETURN QUERY SELECT u.user_id, u.github_user_id, u.created_at
                FROM runtime_private.users AS u WHERE u.status='suspended'
                ORDER BY u.created_at, u.user_id LIMIT p_limit;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.read_audit_events(p_limit integer)
        RETURNS TABLE(audit_id uuid, actor_github_user_id bigint, action text,
                      target_user_id uuid, reason_code text, created_at timestamptz)
        LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        BEGIN
            IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
                RAISE EXCEPTION 'operator_limit_invalid' USING ERRCODE='22023';
            END IF;
            RETURN QUERY SELECT event.audit_id, event.actor_github_user_id, event.action::text,
                                event.target_user_id, event.reason_code::text, event.created_at
                FROM {SCHEMA}.audit_events AS event
                ORDER BY event.created_at DESC, event.audit_id DESC LIMIT p_limit;
        END;
        $function$
        """
    )
    for signature in (
        "admission_allowed(uuid)",
        "set_admission_stopped(boolean,bigint,text)",
        "set_account_suspended(bigint,boolean,bigint,text)",
        "revoke_provider_credential(uuid,bigint,text)",
        "read_overview()",
        "read_unknown_effects(integer)",
        "read_unknown_allocations(integer)",
        "read_pending_inbox(integer)",
        "read_suspended_accounts(integer)",
        "read_audit_events(integer)",
    ):
        op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.{signature} FROM PUBLIC")


def _sql_values(values: tuple[str, ...]) -> str:
    return "(" + ",".join("'" + value + "'" for value in values) + ")"


def downgrade() -> None:
    for signature in (
        "admission_allowed(uuid)",
        "set_admission_stopped(boolean,bigint,text)",
        "set_account_suspended(bigint,boolean,bigint,text)",
        "revoke_provider_credential(uuid,bigint,text)",
        "read_overview()",
        "read_unknown_effects(integer)",
        "read_unknown_allocations(integer)",
        "read_pending_inbox(integer)",
        "read_suspended_accounts(integer)",
        "read_audit_events(integer)",
    ):
        op.execute(f"DROP FUNCTION {SCHEMA}.{signature}")
    op.drop_table("audit_events", schema=SCHEMA)
    op.drop_table("global_control", schema=SCHEMA)
    op.execute(f"DROP SCHEMA {SCHEMA}")
    bind = op.get_bind()
    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"REVOKE CONNECT ON DATABASE {quoted_database} FROM {OPERATOR_ROLE}")
