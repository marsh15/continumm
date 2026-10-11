"""Add fenced work-triggered wake dispatch state.

Revision ID: 20261011_02
Revises: 20261011_01
Create Date: 2026-10-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "20261011_02"
down_revision: str | None = "20261011_01"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None

INGRESS_SCHEMA = "webhook_private"
INBOX_TABLE = "github_delivery_inbox"
WAKE_SCHEMA = "wake_private"
ISSUER_ROLE = "continuum_wake_issuer"
EDGE_ROLE = "continuum_wake"
RECEIVER_ROLE = "continuum_wake_receiver"


def _validate_role(role_name: str, *, login: bool, issuer_owner: bool = False) -> None:
    bind = op.get_bind()
    migration_role = bind.execute(sa.text("SELECT current_user")).scalar_one()
    row = bind.execute(
        sa.text(
            """
            SELECT
                r.rolcanlogin,
                r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR
                    r.rolreplication OR r.rolbypassrls AS elevated,
                r.rolinherit,
                EXISTS (
                    SELECT 1 FROM pg_auth_members m
                    WHERE m.member = r.oid
                ) AS has_parent_membership,
                EXISTS (
                    SELECT 1 FROM pg_auth_members m
                    WHERE m.roleid = r.oid
                      AND pg_get_userbyid(m.member) <> :migration_role
                ) AS has_untrusted_member,
                EXISTS (
                    SELECT 1 FROM pg_roles mr
                    WHERE mr.rolname = :migration_role AND mr.rolsuper
                ) AS migration_is_superuser,
                EXISTS (
                    SELECT 1 FROM pg_auth_members mm
                    WHERE mm.member = (
                        SELECT mr.oid FROM pg_roles mr WHERE mr.rolname = :migration_role
                    )
                    AND mm.roleid = r.oid
                ) AS migration_is_member
            FROM pg_roles r
            WHERE r.rolname = :role_name
            """
        ),
        {"role_name": role_name, "migration_role": migration_role},
    ).one_or_none()
    if row is None:
        raise RuntimeError(f"create_{role_name}_role_before_migration")
    invalid_membership = row.has_parent_membership or row.has_untrusted_member
    if not issuer_owner:
        invalid_membership = invalid_membership or row.migration_is_member
    elif not row.migration_is_superuser and not row.migration_is_member:
        raise RuntimeError("migration_role_must_be_issuer_member_for_owner_transfer")
    if row.rolcanlogin is not login or row.elevated or row.rolinherit or invalid_membership:
        raise RuntimeError(f"{role_name}_role_privileges_invalid")


def _grant_connect(role_name: str) -> None:
    bind = op.get_bind()
    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"GRANT CONNECT ON DATABASE {quoted_database} TO {role_name}")


def upgrade() -> None:
    _validate_role(ISSUER_ROLE, login=False, issuer_owner=True)
    _validate_role(EDGE_ROLE, login=True)
    _validate_role(RECEIVER_ROLE, login=True)
    _grant_connect(EDGE_ROLE)
    _grant_connect(RECEIVER_ROLE)

    op.add_column(
        INBOX_TABLE,
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        schema=INGRESS_SCHEMA,
    )
    op.create_index(
        "ix_github_delivery_inbox_unconsumed",
        INBOX_TABLE,
        ["received_at"],
        schema=INGRESS_SCHEMA,
        postgresql_where=sa.text("consumed_at IS NULL"),
    )
    op.execute(
        f"CREATE POLICY wake_issuer_read_pending ON {INGRESS_SCHEMA}.{INBOX_TABLE} "
        f"FOR SELECT TO {ISSUER_ROLE} USING (consumed_at IS NULL)"
    )
    op.execute(f"GRANT SELECT (consumed_at) ON {INGRESS_SCHEMA}.{INBOX_TABLE} TO {ISSUER_ROLE}")

    op.execute(f"CREATE SCHEMA {WAKE_SCHEMA}")
    op.execute(f"REVOKE ALL ON SCHEMA {WAKE_SCHEMA} FROM PUBLIC")
    op.execute(
        f"GRANT USAGE ON SCHEMA {WAKE_SCHEMA} TO {ISSUER_ROLE}, {EDGE_ROLE}, {RECEIVER_ROLE}"
    )
    op.create_table(
        "dispatch_control",
        sa.Column("singleton", sa.Boolean(), primary_key=True, server_default=sa.true()),
        sa.Column("generation", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("active_attempt_id", sa.Uuid(), nullable=True),
        sa.Column("active_wake_id", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("known_capacity_hold_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_dispatch_outcome", sa.String(length=24), nullable=True),
        sa.Column("last_http_status", sa.SmallInteger(), nullable=True),
        sa.Column("last_receipt_accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.CheckConstraint("singleton", name="ck_wake_control_singleton"),
        sa.CheckConstraint("generation >= 0", name="ck_wake_control_generation_nonnegative"),
        sa.CheckConstraint("failure_count >= 0", name="ck_wake_control_failures_nonnegative"),
        sa.CheckConstraint(
            "last_http_status IS NULL OR last_http_status BETWEEN 100 AND 599",
            name="ck_wake_control_http_status",
        ),
        sa.CheckConstraint(
            "last_dispatch_outcome IS NULL OR last_dispatch_outcome IN "
            "('claimed', 'accepted', 'retryable_failure', 'unknown', 'protocol_failure', 'idle')",
            name="ck_wake_control_outcome",
        ),
        schema=WAKE_SCHEMA,
    )
    op.create_table(
        "dispatch_ticket",
        sa.Column("ticket_id", sa.Uuid(), primary_key=True),
        sa.Column("ticket_sha256", sa.LargeBinary(length=32), nullable=False),
        sa.Column("attempt_id", sa.Uuid(), nullable=False, unique=True),
        sa.Column("wake_id", sa.Uuid(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.CheckConstraint("octet_length(ticket_sha256) = 32", name="ck_wake_ticket_digest_length"),
        sa.CheckConstraint("generation > 0", name="ck_wake_ticket_generation_positive"),
        schema=WAKE_SCHEMA,
    )
    op.create_index(
        "ix_wake_dispatch_ticket_expiry",
        "dispatch_ticket",
        ["expires_at"],
        schema=WAKE_SCHEMA,
    )
    op.create_table(
        "wake_receipt",
        sa.Column("wake_id", sa.Uuid(), primary_key=True),
        sa.Column("reason", sa.String(length=32), nullable=False),
        sa.Column("accepted_attempt_id", sa.Uuid(), nullable=False),
        sa.Column("accepted_generation", sa.BigInteger(), nullable=False),
        sa.Column(
            "accepted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column(
            "drain_status",
            sa.String(length=32),
            nullable=False,
            server_default="worker_not_started",
        ),
        sa.CheckConstraint("reason = 'pending_inbox'", name="ck_wake_receipt_reason"),
        sa.CheckConstraint(
            "drain_status IN ('worker_not_started', 'drained')",
            name="ck_wake_receipt_drain_status",
        ),
        sa.CheckConstraint("accepted_generation > 0", name="ck_wake_receipt_generation_positive"),
        schema=WAKE_SCHEMA,
    )
    op.create_table(
        "drain_watermark",
        sa.Column("singleton", sa.Boolean(), primary_key=True, server_default=sa.true()),
        sa.Column("last_successful_drain_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("singleton", name="ck_wake_watermark_singleton"),
        schema=WAKE_SCHEMA,
    )
    op.execute(f"INSERT INTO {WAKE_SCHEMA}.dispatch_control (singleton) VALUES (true)")
    op.execute(f"INSERT INTO {WAKE_SCHEMA}.drain_watermark (singleton) VALUES (true)")

    op.execute(f"GRANT USAGE ON SCHEMA {INGRESS_SCHEMA} TO {ISSUER_ROLE}")
    for table_name in (
        "dispatch_control",
        "dispatch_ticket",
        "wake_receipt",
        "drain_watermark",
    ):
        op.execute(f"ALTER TABLE {WAKE_SCHEMA}.{table_name} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {WAKE_SCHEMA}.{table_name} FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY issuer_all_control ON {WAKE_SCHEMA}.dispatch_control "
        f"FOR ALL TO {ISSUER_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute(
        f"CREATE POLICY issuer_all_ticket ON {WAKE_SCHEMA}.dispatch_ticket "
        f"FOR ALL TO {ISSUER_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute(
        f"CREATE POLICY issuer_select_receipt ON {WAKE_SCHEMA}.wake_receipt "
        f"FOR SELECT TO {ISSUER_ROLE} USING (true)"
    )
    op.execute(
        f"CREATE POLICY issuer_insert_receipt ON {WAKE_SCHEMA}.wake_receipt "
        f"FOR INSERT TO {ISSUER_ROLE} WITH CHECK (true)"
    )
    op.execute(
        f"CREATE POLICY issuer_select_watermark ON {WAKE_SCHEMA}.drain_watermark "
        f"FOR SELECT TO {ISSUER_ROLE} USING (true)"
    )
    op.execute(f"REVOKE ALL ON ALL TABLES IN SCHEMA {WAKE_SCHEMA} FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON {WAKE_SCHEMA}.dispatch_control TO {ISSUER_ROLE}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON {WAKE_SCHEMA}.dispatch_ticket TO {ISSUER_ROLE}")
    op.execute(f"GRANT SELECT, INSERT ON {WAKE_SCHEMA}.wake_receipt TO {ISSUER_ROLE}")
    op.execute(f"GRANT SELECT ON {WAKE_SCHEMA}.drain_watermark TO {ISSUER_ROLE}")
    op.execute(f"GRANT CREATE ON SCHEMA {WAKE_SCHEMA} TO {ISSUER_ROLE}")
    _create_functions()


def _create_functions() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {WAKE_SCHEMA}.issue_dispatch_ticket()
        RETURNS TABLE (
            ticket_id uuid,
            ticket_value text,
            attempt_id uuid,
            wake_id uuid,
            generation bigint,
            reason text,
            expires_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            v_control {WAKE_SCHEMA}.dispatch_control%ROWTYPE;
            v_now timestamptz;
            v_ticket_id uuid;
            v_ticket_value text;
            v_attempt_id uuid;
            v_wake_id uuid;
            v_generation bigint;
            v_expires_at timestamptz;
        BEGIN
            SELECT c.* INTO v_control
            FROM {WAKE_SCHEMA}.dispatch_control AS c
            WHERE c.singleton = true
            FOR UPDATE;
            IF NOT FOUND THEN
                RETURN;
            END IF;
            v_now := pg_catalog.clock_timestamp();
            IF v_control.known_capacity_hold_until > v_now
                OR v_control.next_attempt_at > v_now
                OR v_control.lease_expires_at > v_now
                OR NOT EXISTS (
                    SELECT 1 FROM {INGRESS_SCHEMA}.{INBOX_TABLE} AS i
                    WHERE i.consumed_at IS NULL
                ) THEN
                RETURN;
            END IF;

            v_ticket_id := pg_catalog.gen_random_uuid();
            v_ticket_value := pg_catalog.gen_random_uuid()::text;
            v_attempt_id := pg_catalog.gen_random_uuid();
            v_wake_id := COALESCE(v_control.active_wake_id, pg_catalog.gen_random_uuid());
            v_generation := v_control.generation + 1;
            v_expires_at := v_now + interval '120 seconds';

            INSERT INTO {WAKE_SCHEMA}.dispatch_ticket (
                ticket_id, ticket_sha256, attempt_id, wake_id, generation, expires_at
            ) VALUES (
                v_ticket_id,
                pg_catalog.sha256(pg_catalog.convert_to(v_ticket_value, 'UTF8')),
                v_attempt_id,
                v_wake_id,
                v_generation,
                v_expires_at
            );
            UPDATE {WAKE_SCHEMA}.dispatch_control AS c
            SET generation = v_generation,
                active_attempt_id = v_attempt_id,
                active_wake_id = v_wake_id,
                lease_expires_at = v_expires_at,
                last_dispatch_outcome = 'claimed',
                last_http_status = NULL,
                updated_at = v_now
            WHERE c.singleton = true;

            RETURN QUERY SELECT
                v_ticket_id,
                v_ticket_value,
                v_attempt_id,
                v_wake_id,
                v_generation,
                'pending_inbox'::text,
                v_expires_at;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {WAKE_SCHEMA}.consume_dispatch_ticket(
            p_ticket_id uuid,
            p_ticket_value text
        )
        RETURNS TABLE (
            authorized boolean,
            attempt_id uuid,
            wake_id uuid,
            generation bigint,
            reason text
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            v_ticket {WAKE_SCHEMA}.dispatch_ticket%ROWTYPE;
            v_control {WAKE_SCHEMA}.dispatch_control%ROWTYPE;
            v_now timestamptz;
        BEGIN
            SELECT t.* INTO v_ticket
            FROM {WAKE_SCHEMA}.dispatch_ticket AS t
            WHERE t.ticket_id = p_ticket_id;
            IF NOT FOUND THEN
                RETURN QUERY SELECT false, NULL::uuid, NULL::uuid, NULL::bigint, NULL::text;
                RETURN;
            END IF;

            SELECT c.* INTO v_control
            FROM {WAKE_SCHEMA}.dispatch_control AS c
            WHERE c.singleton = true
            FOR UPDATE;
            IF NOT FOUND THEN
                RETURN QUERY SELECT false, NULL::uuid, NULL::uuid, NULL::bigint, NULL::text;
                RETURN;
            END IF;
            SELECT t.* INTO v_ticket
            FROM {WAKE_SCHEMA}.dispatch_ticket AS t
            WHERE t.ticket_id = p_ticket_id
            FOR UPDATE;
            v_now := pg_catalog.clock_timestamp();
            IF NOT FOUND
                OR p_ticket_value IS NULL
                OR v_ticket.ticket_sha256 <> pg_catalog.sha256(
                    pg_catalog.convert_to(p_ticket_value, 'UTF8')
                )
                OR v_ticket.consumed_at IS NOT NULL
                OR v_ticket.expires_at <= v_now
                OR v_control.generation <> v_ticket.generation
                OR v_control.active_attempt_id IS DISTINCT FROM v_ticket.attempt_id
                OR v_control.active_wake_id IS DISTINCT FROM v_ticket.wake_id
                OR v_control.lease_expires_at IS NULL
                OR v_control.lease_expires_at <= v_now THEN
                RETURN QUERY SELECT false, NULL::uuid, NULL::uuid, NULL::bigint, NULL::text;
                RETURN;
            END IF;

            IF v_control.known_capacity_hold_until > v_now
                OR v_control.next_attempt_at > v_now
                OR NOT EXISTS (
                    SELECT 1 FROM {INGRESS_SCHEMA}.{INBOX_TABLE} AS i
                    WHERE i.consumed_at IS NULL
                ) THEN
                UPDATE {WAKE_SCHEMA}.dispatch_ticket AS t
                SET consumed_at = v_now
                WHERE t.ticket_id = p_ticket_id;
                UPDATE {WAKE_SCHEMA}.dispatch_control AS c
                SET active_attempt_id = NULL,
                    lease_expires_at = NULL,
                    last_dispatch_outcome = 'idle',
                    next_attempt_at = CASE
                        WHEN v_control.known_capacity_hold_until > v_now
                            THEN v_control.known_capacity_hold_until
                        ELSE c.next_attempt_at
                    END,
                    updated_at = v_now
                WHERE c.singleton = true
                    AND c.generation = v_ticket.generation
                    AND c.active_attempt_id = v_ticket.attempt_id;
                RETURN QUERY SELECT false, NULL::uuid, NULL::uuid, NULL::bigint, NULL::text;
                RETURN;
            END IF;

            UPDATE {WAKE_SCHEMA}.dispatch_ticket AS t
            SET consumed_at = v_now
            WHERE t.ticket_id = p_ticket_id;
            RETURN QUERY SELECT
                true,
                v_ticket.attempt_id,
                v_ticket.wake_id,
                v_ticket.generation,
                'pending_inbox'::text;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {WAKE_SCHEMA}.record_dispatch_result(
            p_attempt_id uuid,
            p_generation bigint,
            p_outcome text,
            p_http_status smallint
        )
        RETURNS TABLE (recorded boolean)
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            v_control {WAKE_SCHEMA}.dispatch_control%ROWTYPE;
            v_now timestamptz;
            v_failures integer;
            v_delay_seconds double precision;
        BEGIN
            IF p_outcome IS NULL OR p_outcome NOT IN (
                'accepted', 'retryable_failure', 'unknown', 'protocol_failure'
            ) OR (p_outcome = 'accepted' AND p_http_status IS DISTINCT FROM 202)
                OR (p_http_status IS NOT NULL AND p_http_status NOT BETWEEN 100 AND 599) THEN
                RAISE EXCEPTION 'wake_outcome_invalid' USING ERRCODE = '22023';
            END IF;
            SELECT c.* INTO v_control
            FROM {WAKE_SCHEMA}.dispatch_control AS c
            WHERE c.singleton = true
            FOR UPDATE;
            v_now := pg_catalog.clock_timestamp();
            IF NOT FOUND
                OR v_control.generation <> p_generation
                OR v_control.active_attempt_id IS DISTINCT FROM p_attempt_id
                OR v_control.lease_expires_at IS NULL
                OR v_control.lease_expires_at <= v_now THEN
                RETURN QUERY SELECT false;
                RETURN;
            END IF;

            IF p_outcome = 'accepted' THEN
                UPDATE {WAKE_SCHEMA}.dispatch_control AS c
                SET active_attempt_id = NULL,
                    lease_expires_at = NULL,
                    failure_count = 0,
                    next_attempt_at = v_now + interval '15 minutes',
                    last_dispatch_outcome = 'accepted',
                    last_http_status = p_http_status,
                    last_receipt_accepted_at = v_now,
                    updated_at = v_now
                WHERE c.singleton = true
                    AND c.generation = p_generation
                    AND c.active_attempt_id = p_attempt_id
                    AND c.lease_expires_at > v_now;
            ELSE
                v_failures := LEAST(v_control.failure_count + 1, 30);
                v_delay_seconds := LEAST(
                    900.0,
                    60.0 * pg_catalog.power(2.0, LEAST(v_failures - 1, 4))
                );
                v_delay_seconds := GREATEST(
                    30.0,
                    LEAST(900.0, v_delay_seconds * (0.5 + pg_catalog.random()))
                );
                UPDATE {WAKE_SCHEMA}.dispatch_control AS c
                SET active_attempt_id = NULL,
                    lease_expires_at = NULL,
                    failure_count = v_failures,
                    next_attempt_at = v_now + pg_catalog.make_interval(secs => v_delay_seconds),
                    last_dispatch_outcome = p_outcome,
                    last_http_status = p_http_status,
                    updated_at = v_now
                WHERE c.singleton = true
                    AND c.generation = p_generation
                    AND c.active_attempt_id = p_attempt_id
                    AND c.lease_expires_at > v_now;
            END IF;
            RETURN QUERY SELECT true;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {WAKE_SCHEMA}.accept_wake_receipt(
            p_attempt_id uuid,
            p_generation bigint,
            p_wake_id uuid,
            p_reason text
        )
        RETURNS TABLE (
            outcome text,
            receipt_id uuid,
            accepted_at timestamptz,
            drain_status text,
            last_successful_drain_at timestamptz
        )
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        DECLARE
            v_control {WAKE_SCHEMA}.dispatch_control%ROWTYPE;
            v_receipt {WAKE_SCHEMA}.wake_receipt%ROWTYPE;
            v_now timestamptz;
            v_last_successful_drain_at timestamptz;
            v_control_found boolean;
        BEGIN
            SELECT c.* INTO v_control
            FROM {WAKE_SCHEMA}.dispatch_control AS c
            WHERE c.singleton = true
            FOR UPDATE;
            v_control_found := FOUND;
            v_now := pg_catalog.clock_timestamp();
            SELECT w.last_successful_drain_at INTO v_last_successful_drain_at
            FROM {WAKE_SCHEMA}.drain_watermark AS w
            WHERE w.singleton = true;
            IF NOT v_control_found
                OR v_control.generation <> p_generation
                OR v_control.active_attempt_id IS DISTINCT FROM p_attempt_id
                OR v_control.active_wake_id IS DISTINCT FROM p_wake_id
                OR v_control.lease_expires_at IS NULL
                OR v_control.lease_expires_at <= v_now THEN
                RETURN QUERY SELECT
                    'stale'::text, NULL::uuid, NULL::timestamptz, NULL::text,
                    v_last_successful_drain_at;
                RETURN;
            END IF;
            IF p_reason IS DISTINCT FROM 'pending_inbox'
                OR NOT EXISTS (
                    SELECT 1 FROM {INGRESS_SCHEMA}.{INBOX_TABLE} AS i
                    WHERE i.consumed_at IS NULL
                ) THEN
                RETURN QUERY SELECT
                    'not_actionable'::text, NULL::uuid, NULL::timestamptz, NULL::text,
                    v_last_successful_drain_at;
                RETURN;
            END IF;

            SELECT r.* INTO v_receipt
            FROM {WAKE_SCHEMA}.wake_receipt AS r
            WHERE r.wake_id = p_wake_id;
            IF FOUND THEN
                IF v_receipt.reason <> p_reason THEN
                    RETURN QUERY SELECT
                        'conflict'::text, NULL::uuid, NULL::timestamptz, NULL::text,
                        v_last_successful_drain_at;
                    RETURN;
                END IF;
                RETURN QUERY SELECT
                    'duplicate'::text,
                    v_receipt.wake_id,
                    v_receipt.accepted_at,
                    v_receipt.drain_status::text,
                    v_last_successful_drain_at;
                RETURN;
            END IF;

            INSERT INTO {WAKE_SCHEMA}.wake_receipt (
                wake_id, reason, accepted_attempt_id, accepted_generation
            ) VALUES (
                p_wake_id, p_reason, p_attempt_id, p_generation
            ) RETURNING * INTO v_receipt;
            RETURN QUERY SELECT
                'accepted'::text,
                v_receipt.wake_id,
                v_receipt.accepted_at,
                v_receipt.drain_status::text,
                v_last_successful_drain_at;
        END;
        $function$
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {WAKE_SCHEMA}.read_wake_status()
        RETURNS TABLE (
            last_successful_drain_at timestamptz,
            receipt_state text,
            receipt_accepted_at timestamptz
        )
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
            SELECT
                w.last_successful_drain_at,
                r.drain_status::text,
                r.accepted_at
            FROM {WAKE_SCHEMA}.drain_watermark AS w
            LEFT JOIN {WAKE_SCHEMA}.dispatch_control AS c ON c.singleton = true
            LEFT JOIN {WAKE_SCHEMA}.wake_receipt AS r ON r.wake_id = c.active_wake_id
            WHERE w.singleton = true
        $function$
        """
    )

    functions = (
        ("issue_dispatch_ticket()", ""),
        ("consume_dispatch_ticket(uuid, text)", EDGE_ROLE),
        ("record_dispatch_result(uuid, bigint, text, smallint)", EDGE_ROLE),
        ("accept_wake_receipt(uuid, bigint, uuid, text)", RECEIVER_ROLE),
        ("read_wake_status()", RECEIVER_ROLE),
    )
    for signature, _grantee in functions:
        op.execute(f"ALTER FUNCTION {WAKE_SCHEMA}.{signature} OWNER TO {ISSUER_ROLE}")

    migration_role = op.get_bind().execute(sa.text("SELECT current_user")).scalar_one()
    quoted_migration_role = op.get_bind().dialect.identifier_preparer.quote(migration_role)
    op.execute(f"SET ROLE {ISSUER_ROLE}")
    for signature, grantee in functions:
        op.execute(f"REVOKE ALL ON FUNCTION {WAKE_SCHEMA}.{signature} FROM PUBLIC")
        if grantee:
            op.execute(f"GRANT EXECUTE ON FUNCTION {WAKE_SCHEMA}.{signature} TO {grantee}")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {WAKE_SCHEMA}.issue_dispatch_ticket() "
        f"TO {quoted_migration_role}"
    )
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA {WAKE_SCHEMA} FROM {ISSUER_ROLE}")


def downgrade() -> None:
    op.execute(f"DROP FUNCTION {WAKE_SCHEMA}.read_wake_status()")
    op.execute(f"DROP FUNCTION {WAKE_SCHEMA}.accept_wake_receipt(uuid, bigint, uuid, text)")
    op.execute(f"DROP FUNCTION {WAKE_SCHEMA}.record_dispatch_result(uuid, bigint, text, smallint)")
    op.execute(f"DROP FUNCTION {WAKE_SCHEMA}.consume_dispatch_ticket(uuid, text)")
    op.execute(f"DROP FUNCTION {WAKE_SCHEMA}.issue_dispatch_ticket()")
    op.drop_table("drain_watermark", schema=WAKE_SCHEMA)
    op.drop_table("wake_receipt", schema=WAKE_SCHEMA)
    op.drop_index(
        "ix_wake_dispatch_ticket_expiry", table_name="dispatch_ticket", schema=WAKE_SCHEMA
    )
    op.drop_table("dispatch_ticket", schema=WAKE_SCHEMA)
    op.drop_table("dispatch_control", schema=WAKE_SCHEMA)
    op.execute(f"DROP SCHEMA {WAKE_SCHEMA}")

    op.execute(f"DROP POLICY wake_issuer_read_pending ON {INGRESS_SCHEMA}.{INBOX_TABLE}")
    op.execute(f"REVOKE SELECT (consumed_at) ON {INGRESS_SCHEMA}.{INBOX_TABLE} FROM {ISSUER_ROLE}")
    op.execute(f"REVOKE USAGE ON SCHEMA {INGRESS_SCHEMA} FROM {ISSUER_ROLE}")
    op.drop_index(
        "ix_github_delivery_inbox_unconsumed",
        table_name=INBOX_TABLE,
        schema=INGRESS_SCHEMA,
    )
    op.drop_column(INBOX_TABLE, "consumed_at", schema=INGRESS_SCHEMA)

    bind = op.get_bind()
    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"REVOKE CONNECT ON DATABASE {quoted_database} FROM {EDGE_ROLE}")
    op.execute(f"REVOKE CONNECT ON DATABASE {quoted_database} FROM {RECEIVER_ROLE}")
