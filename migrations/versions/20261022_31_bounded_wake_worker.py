"""Add a least-privilege, fenced bounded wake worker store.

Revision ID: 20261022_31
Revises: 20261020_30
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# SQL bodies are kept readable as SQL, with exact text retained for the audited function patches.
# ruff: noqa: E501

revision = "20261022_31"
down_revision = "20261020_30"
branch_labels = None
depends_on = None

WAKE = "wake_private"
INBOX = "webhook_private.github_delivery_inbox"
RUNTIME = "runtime_private"
LOGIN_ROLE = "continuum_wake_worker"
OWNER_ROLE = "continuum_wake_worker_owner"
ISSUER_ROLE = "continuum_wake_issuer"


def _validate_role(name: str, *, login: bool, migration_role: str) -> None:
    row = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT rolcanlogin, rolsuper OR rolcreatedb OR rolcreaterole OR "
                "rolreplication OR rolbypassrls AS elevated, rolinherit, "
                "EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.member=r.oid) AS has_parent, "
                "EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.roleid=r.oid "
                "AND pg_get_userbyid(m.member)<>:migration_role) AS has_untrusted_member, "
                "EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.roleid=r.oid "
                "AND pg_get_userbyid(m.member)=:migration_role) AS migration_is_member "
                "FROM pg_roles r WHERE rolname=:name"
            ),
            {"name": name, "migration_role": migration_role},
        )
        .one_or_none()
    )
    if row is None:
        raise RuntimeError(f"create_{name}_role_before_migration")
    if (
        row.rolcanlogin is not login
        or row.elevated
        or row.rolinherit
        or row.has_parent
        or row.has_untrusted_member
        or (login and row.migration_is_member)
    ):
        raise RuntimeError(f"{name}_role_privileges_invalid")


def upgrade() -> None:
    bind = op.get_bind()
    migration_role = bind.execute(sa.text("SELECT current_user")).scalar_one()
    _validate_role(LOGIN_ROLE, login=True, migration_role=migration_role)
    _validate_role(OWNER_ROLE, login=False, migration_role=migration_role)
    migration_is_superuser = bind.execute(
        sa.text("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")
    ).scalar_one()
    migration_is_owner_member = bind.execute(
        sa.text(
            "SELECT EXISTS(SELECT 1 FROM pg_auth_members WHERE roleid="
            "(SELECT oid FROM pg_roles WHERE rolname=:owner) AND member="
            "(SELECT oid FROM pg_roles WHERE rolname=:migration))"
        ),
        {"owner": OWNER_ROLE, "migration": migration_role},
    ).scalar_one()
    if not migration_is_superuser and not migration_is_owner_member:
        raise RuntimeError("migration_role_must_be_worker_owner_member")

    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"GRANT CONNECT ON DATABASE {quoted_database} TO {LOGIN_ROLE}")
    op.execute(f"GRANT USAGE ON SCHEMA {WAKE} TO {LOGIN_ROLE}, {OWNER_ROLE}")
    op.execute(f"GRANT USAGE ON SCHEMA webhook_private, runtime_private TO {OWNER_ROLE}")
    for schema_table in (
        "webhook_private.github_delivery_inbox",
        "runtime_private.users",
        "runtime_private.repository_registrations",
        "runtime_private.runs",
        "runtime_private.jobs",
        "runtime_private.github_command_receipts",
        "runtime_private.github_command_results",
        "wake_private.dispatch_control",
        "wake_private.wake_receipt",
        "wake_private.drain_watermark",
    ):
        op.execute(f"REVOKE ALL ON {schema_table} FROM {LOGIN_ROLE}")
    op.execute("ALTER TABLE wake_private.wake_receipt DROP CONSTRAINT ck_wake_receipt_drain_status")
    op.execute(
        "ALTER TABLE wake_private.wake_receipt ADD CONSTRAINT ck_wake_receipt_drain_status "
        "CHECK (drain_status IN ('worker_not_started','running','partial','drained'))"
    )
    op.add_column(
        "wake_receipt",
        sa.Column("last_accepted_attempt_id", sa.Uuid(), nullable=True),
        schema=WAKE,
    )
    op.add_column(
        "wake_receipt",
        sa.Column("last_accepted_generation", sa.BigInteger(), nullable=True),
        schema=WAKE,
    )
    op.execute(
        "UPDATE wake_private.wake_receipt SET last_accepted_attempt_id=accepted_attempt_id, "
        "last_accepted_generation=accepted_generation"
    )
    op.alter_column("wake_receipt", "last_accepted_attempt_id", nullable=False, schema=WAKE)
    op.alter_column("wake_receipt", "last_accepted_generation", nullable=False, schema=WAKE)
    op.create_check_constraint(
        "ck_wake_receipt_latest_generation",
        "wake_receipt",
        "last_accepted_generation > 0",
        schema=WAKE,
    )

    op.create_table(
        "worker_lease",
        sa.Column("singleton", sa.Boolean(), primary_key=True, server_default=sa.true()),
        sa.Column("wake_id", sa.Uuid()),
        sa.Column("github_app_id", sa.BigInteger()),
        sa.Column("token", sa.Uuid()),
        sa.Column("generation", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint("singleton", name="ck_wake_worker_lease_singleton"),
        sa.CheckConstraint("generation >= 0", name="ck_wake_worker_lease_generation"),
        sa.CheckConstraint(
            "(wake_id IS NULL AND github_app_id IS NULL AND token IS NULL) OR "
            "(wake_id IS NOT NULL AND github_app_id > 0 AND token IS NOT NULL AND expires_at IS NOT NULL)",
            name="ck_wake_worker_lease_identity",
        ),
        schema=WAKE,
    )
    op.execute(f"INSERT INTO {WAKE}.worker_lease(singleton) VALUES (true)")
    op.create_table(
        "worker_inbox_claim",
        sa.Column("github_app_id", sa.BigInteger(), nullable=False),
        sa.Column("delivery_id", sa.Uuid(), nullable=False),
        sa.Column("wake_id", sa.Uuid(), nullable=False),
        sa.Column("lease_token", sa.Uuid(), nullable=False),
        sa.Column("lease_generation", sa.BigInteger(), nullable=False),
        sa.Column("owner_id", sa.Uuid()),
        sa.Column("offered_at", sa.DateTime(timezone=True)),
        sa.PrimaryKeyConstraint("github_app_id", "delivery_id", name="pk_wake_worker_inbox_claim"),
        sa.CheckConstraint("github_app_id > 0", name="ck_wake_worker_claim_app"),
        sa.CheckConstraint("lease_generation > 0", name="ck_wake_worker_claim_generation"),
        schema=WAKE,
    )
    op.create_index(
        "ix_wake_worker_inbox_claim_lease",
        "worker_inbox_claim",
        ["wake_id", "lease_generation", "offered_at"],
        schema=WAKE,
    )
    op.create_table(
        "worker_run_seen",
        sa.Column("wake_id", sa.Uuid(), nullable=False),
        sa.Column("lease_generation", sa.BigInteger(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.PrimaryKeyConstraint(
            "wake_id", "lease_generation", "owner_id", "run_id", name="pk_wake_worker_run_seen"
        ),
        schema=WAKE,
    )

    # Forced RLS applies even inside SECURITY DEFINER functions. The NOLOGIN
    # owner has access only to the rows and columns needed by these functions.
    tables = ("worker_lease", "worker_inbox_claim", "worker_run_seen")
    for table in tables:
        op.execute(f"ALTER TABLE {WAKE}.{table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {WAKE}.{table} FORCE ROW LEVEL SECURITY")
        op.execute(f"REVOKE ALL ON {WAKE}.{table} FROM PUBLIC, {LOGIN_ROLE}, {ISSUER_ROLE}")
        op.execute(
            f"CREATE POLICY worker_owner_all_{table} ON {WAKE}.{table} "
            f"FOR ALL TO {OWNER_ROLE} USING (true) WITH CHECK (true)"
        )
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {WAKE}.{table} TO {OWNER_ROLE}")

    op.execute(
        f"CREATE POLICY wake_worker_pending_inbox_select ON {INBOX} "
        f"FOR SELECT TO {OWNER_ROLE} USING (true)"
    )
    op.execute(
        f"CREATE POLICY wake_worker_pending_inbox_update ON {INBOX} "
        f"FOR UPDATE TO {OWNER_ROLE} USING (consumed_at IS NULL) WITH CHECK (true)"
    )
    op.execute(
        f"GRANT SELECT (github_app_id, delivery_id, event_type, action, payload_sha256, "
        f"raw_body, received_at, consumed_at) "
        f"ON {INBOX} TO {OWNER_ROLE}"
    )
    op.execute(f"GRANT UPDATE (consumed_at) ON {INBOX} TO {OWNER_ROLE}")

    for table in ("users", "repository_registrations", "runs", "jobs"):
        op.execute(
            f"CREATE POLICY wake_worker_select_{table} ON {RUNTIME}.{table} "
            f"FOR SELECT TO {OWNER_ROLE} USING (true)"
        )
    op.execute(f"GRANT SELECT (user_id, github_user_id, status) ON {RUNTIME}.users TO {OWNER_ROLE}")
    op.execute(
        f"GRANT SELECT (owner_id, github_repository_id) ON {RUNTIME}.repository_registrations "
        f"TO {OWNER_ROLE}"
    )
    op.execute(f"GRANT SELECT (owner_id, run_id, status) ON {RUNTIME}.runs TO {OWNER_ROLE}")
    op.execute(
        f"GRANT SELECT (owner_id, run_id, job_id, status, available_at, lease_expires_at, created_at) "
        f"ON {RUNTIME}.jobs TO {OWNER_ROLE}"
    )
    op.execute(
        "CREATE POLICY wake_worker_receipt_select ON wake_private.wake_receipt "
        f"FOR SELECT TO {OWNER_ROLE} USING (true)"
    )
    op.execute(
        "CREATE POLICY wake_worker_receipt_update ON wake_private.wake_receipt "
        f"FOR UPDATE TO {OWNER_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute(
        "CREATE POLICY wake_issuer_update_latest_receipt ON wake_private.wake_receipt "
        f"FOR UPDATE TO {ISSUER_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute(
        "CREATE POLICY wake_worker_control_select ON wake_private.dispatch_control "
        f"FOR SELECT TO {OWNER_ROLE} USING (true)"
    )
    op.execute(
        "CREATE POLICY wake_worker_control_lock ON wake_private.dispatch_control "
        f"FOR UPDATE TO {OWNER_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute(
        "CREATE POLICY wake_worker_watermark_select ON wake_private.drain_watermark "
        f"FOR SELECT TO {OWNER_ROLE} USING (true)"
    )
    op.execute(
        "CREATE POLICY wake_worker_watermark_update ON wake_private.drain_watermark "
        f"FOR UPDATE TO {OWNER_ROLE} USING (true) WITH CHECK (true)"
    )
    op.execute(
        "GRANT SELECT (singleton, generation, active_wake_id, active_attempt_id) "
        "ON wake_private.dispatch_control TO continuum_wake_worker_owner"
    )
    op.execute(
        "GRANT UPDATE (singleton) ON wake_private.dispatch_control TO continuum_wake_worker_owner"
    )
    op.execute("GRANT SELECT ON wake_private.wake_receipt TO continuum_wake_worker_owner")
    op.execute(
        "GRANT UPDATE (drain_status) ON wake_private.wake_receipt TO continuum_wake_worker_owner"
    )
    op.execute(
        "GRANT UPDATE (last_accepted_attempt_id,last_accepted_generation) "
        "ON wake_private.wake_receipt TO continuum_wake_issuer"
    )
    op.execute(
        "GRANT SELECT (singleton, last_successful_drain_at) "
        "ON wake_private.drain_watermark TO continuum_wake_worker_owner"
    )
    op.execute(
        "GRANT UPDATE (last_successful_drain_at) ON wake_private.drain_watermark "
        "TO continuum_wake_worker_owner"
    )

    op.execute(
        "CREATE POLICY wake_worker_command_receipt_select ON runtime_private.github_command_receipts "
        f"FOR SELECT TO {OWNER_ROLE} USING (true)"
    )
    op.execute(
        "CREATE POLICY wake_worker_command_result_select ON runtime_private.github_command_results "
        f"FOR SELECT TO {OWNER_ROLE} USING (true)"
    )
    op.execute(
        "GRANT SELECT (owner_id, repository_id, comment_id, author_id, expires_at) "
        "ON runtime_private.github_command_receipts TO continuum_wake_worker_owner"
    )
    op.execute(
        "GRANT SELECT (owner_id, repository_id, comment_id) "
        "ON runtime_private.github_command_results TO continuum_wake_worker_owner"
    )

    _create_functions()
    op.execute(f"GRANT CREATE ON SCHEMA {WAKE} TO {ISSUER_ROLE}")
    op.execute(f"SET ROLE {ISSUER_ROLE}")
    _patch_actionable_predicates()
    _patch_accept_latest_attempt(restore=False)
    _patch_active_attempt_retention(restore=False)
    _patch_read_wake_status()
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA {WAKE} FROM {ISSUER_ROLE}")

    op.execute(f"GRANT CREATE ON SCHEMA {WAKE} TO {OWNER_ROLE}")
    for signature in (
        "has_actionable_work()",
        "worker_status(uuid,text)",
        "resolve_inbox_owner(jsonb)",
        "resolve_inbox_command_state(jsonb)",
        "worker_claim(uuid,uuid,bigint,bigint)",
        "worker_heartbeat(uuid,uuid,bigint)",
        "worker_next_inbox(uuid,uuid,bigint)",
        "worker_complete_inbox(uuid,uuid,bigint,uuid,text)",
        "worker_next_run(uuid,uuid,bigint)",
        "worker_finish(uuid,uuid,bigint,boolean)",
    ):
        op.execute(f"ALTER FUNCTION {WAKE}.{signature} OWNER TO {OWNER_ROLE}")
    op.execute(f"SET ROLE {OWNER_ROLE}")
    for signature in (
        "has_actionable_work()",
        "worker_status(uuid,text)",
        "resolve_inbox_owner(jsonb)",
        "resolve_inbox_command_state(jsonb)",
        "worker_claim(uuid,uuid,bigint,bigint)",
        "worker_heartbeat(uuid,uuid,bigint)",
        "worker_next_inbox(uuid,uuid,bigint)",
        "worker_complete_inbox(uuid,uuid,bigint,uuid,text)",
        "worker_next_run(uuid,uuid,bigint)",
        "worker_finish(uuid,uuid,bigint,boolean)",
    ):
        op.execute(f"REVOKE ALL ON FUNCTION {WAKE}.{signature} FROM PUBLIC")
        op.execute(f"REVOKE ALL ON FUNCTION {WAKE}.{signature} FROM {LOGIN_ROLE}")
    for signature in (
        "worker_claim(uuid,uuid,bigint,bigint)",
        "worker_heartbeat(uuid,uuid,bigint)",
        "worker_next_inbox(uuid,uuid,bigint)",
        "worker_complete_inbox(uuid,uuid,bigint,uuid,text)",
        "worker_next_run(uuid,uuid,bigint)",
        "worker_finish(uuid,uuid,bigint,boolean)",
    ):
        op.execute(f"GRANT EXECUTE ON FUNCTION {WAKE}.{signature} TO {LOGIN_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {WAKE}.has_actionable_work() TO {ISSUER_ROLE}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {WAKE}.worker_status(uuid,text) TO {ISSUER_ROLE}")
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA {WAKE} FROM {OWNER_ROLE}")


def _create_functions() -> None:
    op.execute(
        """
        CREATE FUNCTION wake_private.has_actionable_work() RETURNS boolean
        LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
          SELECT EXISTS(SELECT 1 FROM webhook_private.github_delivery_inbox i
                        WHERE i.consumed_at IS NULL)
             OR EXISTS(SELECT 1 FROM runtime_private.jobs j
               JOIN runtime_private.runs r ON r.owner_id=j.owner_id AND r.run_id=j.run_id
               JOIN runtime_private.users u ON u.user_id=r.owner_id
               WHERE u.status='active' AND r.status IN ('queued','running')
                 AND ((j.status='queued' AND j.available_at<=clock_timestamp())
                   OR (j.status='leased' AND j.lease_expires_at<=clock_timestamp())))
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION wake_private.worker_status(p_wake_id uuid,p_state text) RETURNS text
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE v_expiry timestamptz;
        BEGIN
          IF p_wake_id IS NULL THEN RETURN NULL; END IF;
          IF p_state='drained' THEN RETURN 'drained'; END IF;
          SELECT expires_at INTO v_expiry FROM wake_private.worker_lease
            WHERE singleton AND wake_id=p_wake_id;
          IF v_expiry>clock_timestamp() THEN RETURN 'running'; END IF;
          IF p_state IN ('running','partial') THEN RETURN 'partial'; END IF;
          RETURN 'worker_not_started';
        END $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION wake_private.resolve_inbox_owner(p_payload jsonb) RETURNS uuid
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE v_repo_text text; v_author_text text; v_comment_text text; v_issue_text text;
          v_number_text text; v_repo bigint; v_author bigint; v_owner uuid;
        BEGIN
          IF p_payload IS NULL OR jsonb_typeof(p_payload)<>'object'
             OR jsonb_typeof(p_payload->'repository')<>'object'
             OR jsonb_typeof(p_payload->'comment')<>'object'
             OR jsonb_typeof(p_payload->'comment'->'user')<>'object'
             OR jsonb_typeof(p_payload->'issue')<>'object' THEN
            RETURN NULL;
          END IF;
          v_repo_text := p_payload->'repository'->>'id';
          v_author_text := p_payload->'comment'->'user'->>'id';
          v_comment_text := p_payload->'comment'->>'id';
          v_issue_text := p_payload->'issue'->>'id';
          v_number_text := p_payload->'issue'->>'number';
          IF v_repo_text IS NULL OR v_author_text IS NULL
             OR v_comment_text IS NULL OR v_issue_text IS NULL OR v_number_text IS NULL
             OR length(v_repo_text)>19 OR length(v_author_text)>19 OR length(v_comment_text)>19
             OR length(v_issue_text)>19 OR length(v_number_text)>19
             OR v_repo_text !~ '^[0-9]{1,19}$' OR v_author_text !~ '^[0-9]{1,19}$'
             OR v_comment_text !~ '^[0-9]{1,19}$' OR v_issue_text !~ '^[0-9]{1,19}$'
             OR v_number_text !~ '^[0-9]{1,19}$' THEN
            RETURN NULL;
          END IF;
          BEGIN
            v_repo := v_repo_text::bigint;
            v_author := v_author_text::bigint;
            PERFORM v_comment_text::bigint, v_issue_text::bigint, v_number_text::bigint;
          EXCEPTION WHEN numeric_value_out_of_range OR invalid_text_representation THEN
            RETURN NULL;
          END;
          IF v_repo<=0 OR v_author<=0 OR v_comment_text::numeric<=0
             OR v_issue_text::numeric<=0 OR v_number_text::numeric<=0 THEN RETURN NULL; END IF;
          SELECT u.user_id INTO v_owner
          FROM runtime_private.repository_registrations rr
          JOIN runtime_private.users u ON u.user_id=rr.owner_id
          WHERE rr.github_repository_id=v_repo AND u.github_user_id=v_author
            AND u.status='active'
          LIMIT 1;
          RETURN v_owner;
        END $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION wake_private.resolve_inbox_command_state(p_payload jsonb) RETURNS text
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE repo_text text; comment_text text; author_text text;
          repo bigint; comment bigint; author bigint; v_owner uuid; v_expiry timestamptz;
        BEGIN
          IF p_payload IS NULL OR jsonb_typeof(p_payload)<>'object'
             OR jsonb_typeof(p_payload->'repository')<>'object'
             OR jsonb_typeof(p_payload->'comment')<>'object'
             OR jsonb_typeof(p_payload->'comment'->'user')<>'object' THEN
            RETURN 'none';
          END IF;
          repo_text:=p_payload->'repository'->>'id';
          comment_text:=p_payload->'comment'->>'id';
          author_text:=p_payload->'comment'->'user'->>'id';
          IF repo_text IS NULL OR comment_text IS NULL OR author_text IS NULL
             OR length(repo_text)>19 OR length(comment_text)>19 OR length(author_text)>19
             OR repo_text !~ '^[0-9]{1,19}$' OR comment_text !~ '^[0-9]{1,19}$'
             OR author_text !~ '^[0-9]{1,19}$' THEN RETURN 'none'; END IF;
          BEGIN
            repo:=repo_text::bigint;
            comment:=comment_text::bigint;
            author:=author_text::bigint;
          EXCEPTION WHEN numeric_value_out_of_range OR invalid_text_representation THEN
            RETURN 'none';
          END;
          IF repo<=0 OR comment<=0 OR author<=0 THEN RETURN 'none'; END IF;
          SELECT receipt.owner_id,receipt.expires_at INTO v_owner,v_expiry
          FROM runtime_private.github_command_receipts AS receipt
          WHERE receipt.repository_id=repo AND receipt.comment_id=comment
            AND receipt.author_id=author;
          IF NOT FOUND THEN RETURN 'none'; END IF;
          IF EXISTS(SELECT 1 FROM runtime_private.github_command_results AS result
              WHERE result.owner_id=v_owner AND result.repository_id=repo
                AND result.comment_id=comment) THEN RETURN 'completed'; END IF;
          IF v_expiry<=clock_timestamp() THEN RETURN 'expired'; END IF;
          RETURN 'accepted_unresolved';
        END $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION wake_private.worker_claim(p_wake_id uuid,p_attempt_id uuid,
          p_dispatch_generation bigint,p_github_app_id bigint)
        RETURNS TABLE(wake_id uuid,token uuid,generation bigint)
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE c record; r wake_private.wake_receipt%ROWTYPE;
          l wake_private.worker_lease%ROWTYPE; n timestamptz; t uuid;
        BEGIN
          SELECT control.singleton,control.generation,control.active_wake_id,
            control.active_attempt_id INTO c
            FROM wake_private.dispatch_control AS control
            WHERE control.singleton FOR UPDATE;
          n:=clock_timestamp();
          IF NOT FOUND OR p_wake_id IS NULL OR p_attempt_id IS NULL OR p_dispatch_generation<=0
             OR p_github_app_id IS NULL OR p_github_app_id<=0
             OR c.active_attempt_id IS DISTINCT FROM p_attempt_id
             OR c.active_wake_id IS DISTINCT FROM p_wake_id
             OR c.generation IS DISTINCT FROM p_dispatch_generation THEN
            RETURN;
          END IF;
          SELECT receipt.* INTO r FROM wake_private.wake_receipt AS receipt
            WHERE receipt.wake_id=p_wake_id;
          IF NOT FOUND OR r.drain_status='drained'
             OR r.last_accepted_attempt_id IS DISTINCT FROM p_attempt_id
             OR r.last_accepted_generation IS DISTINCT FROM p_dispatch_generation THEN RETURN; END IF;
          SELECT * INTO l FROM wake_private.worker_lease WHERE singleton FOR UPDATE;
          IF l.expires_at>n THEN RETURN; END IF;
          IF l.generation=9223372036854775807 THEN RETURN; END IF;
          t:=gen_random_uuid();
          UPDATE wake_private.worker_lease SET wake_id=p_wake_id,github_app_id=p_github_app_id,token=t,
            generation=l.generation+1,expires_at=n+interval '120 seconds'
            WHERE singleton RETURNING * INTO l;
          UPDATE wake_private.wake_receipt AS receipt SET drain_status='running'
            WHERE receipt.wake_id=p_wake_id;
          IF NOT FOUND THEN RETURN; END IF;
          DELETE FROM wake_private.worker_run_seen AS seen WHERE seen.wake_id<>p_wake_id
             OR seen.lease_generation<>l.generation;
          RETURN QUERY SELECT p_wake_id,t,l.generation;
        END $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION wake_private.worker_heartbeat(p_wake_id uuid,p_token uuid,p_generation bigint)
        RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          UPDATE wake_private.worker_lease SET expires_at=clock_timestamp()+interval '120 seconds'
          WHERE singleton AND wake_id=p_wake_id AND token=p_token AND generation=p_generation
            AND expires_at>clock_timestamp();
          RETURN FOUND;
        END $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION wake_private.worker_next_inbox(p_wake_id uuid,p_token uuid,p_generation bigint)
        RETURNS TABLE(delivery_id uuid,event_name text,payload jsonb,owner_id uuid,
          github_app_id bigint,command_state text)
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE l wake_private.worker_lease%ROWTYPE; i record; p jsonb; o uuid; s text;
        BEGIN
          SELECT * INTO l FROM wake_private.worker_lease WHERE singleton FOR UPDATE;
          IF NOT FOUND OR l.wake_id IS DISTINCT FROM p_wake_id OR l.token IS DISTINCT FROM p_token
             OR l.generation IS DISTINCT FROM p_generation OR l.expires_at<=clock_timestamp() THEN RETURN; END IF;
          SELECT inbox.github_app_id,inbox.delivery_id,inbox.event_type,inbox.action,
            inbox.payload_sha256,inbox.raw_body,inbox.received_at,inbox.consumed_at INTO i
            FROM webhook_private.github_delivery_inbox inbox
          LEFT JOIN wake_private.worker_inbox_claim claim
            ON claim.github_app_id=inbox.github_app_id AND claim.delivery_id=inbox.delivery_id
          WHERE inbox.github_app_id=l.github_app_id AND inbox.consumed_at IS NULL AND
            (claim.delivery_id IS NULL OR claim.wake_id IS DISTINCT FROM p_wake_id
              OR claim.lease_generation IS DISTINCT FROM p_generation OR claim.offered_at IS NULL)
          ORDER BY inbox.received_at,inbox.github_app_id,inbox.delivery_id
          LIMIT 1 FOR UPDATE OF inbox SKIP LOCKED;
          IF NOT FOUND THEN RETURN; END IF;
          IF i.payload_sha256 IS DISTINCT FROM pg_catalog.sha256(i.raw_body) THEN RETURN; END IF;
          BEGIN p:=convert_from(i.raw_body,'UTF8')::jsonb;
          EXCEPTION WHEN others THEN p:='{}'::jsonb; END;
          IF jsonb_typeof(p)<>'object' THEN p:='{}'::jsonb; END IF;
          o:=CASE WHEN i.event_type='issue_comment' THEN wake_private.resolve_inbox_owner(p) ELSE NULL END;
          s:=CASE WHEN i.event_type='issue_comment'
            THEN wake_private.resolve_inbox_command_state(p) ELSE 'none' END;
          INSERT INTO wake_private.worker_inbox_claim(github_app_id,delivery_id,wake_id,
            lease_token,lease_generation,owner_id,offered_at)
          VALUES(i.github_app_id,i.delivery_id,p_wake_id,p_token,p_generation,o,clock_timestamp())
          ON CONFLICT ON CONSTRAINT pk_wake_worker_inbox_claim DO UPDATE SET wake_id=EXCLUDED.wake_id,
            lease_token=EXCLUDED.lease_token,lease_generation=EXCLUDED.lease_generation,
            owner_id=EXCLUDED.owner_id,offered_at=EXCLUDED.offered_at;
          RETURN QUERY SELECT i.delivery_id,i.event_type::text,p,o,i.github_app_id,s;
        END $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION wake_private.worker_complete_inbox(
          p_wake_id uuid,p_token uuid,p_generation bigint,p_delivery_id uuid,p_outcome text)
        RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE l wake_private.worker_lease%ROWTYPE; c wake_private.worker_inbox_claim%ROWTYPE;
          i record; p jsonb; repo_text text; comment_text text; author_text text;
          repo bigint; comment bigint; author bigint; v_receipt boolean; v_result boolean;
          v_expired boolean; v_command_owner uuid;
        BEGIN
          IF p_outcome NOT IN ('processed','inert','rejected') THEN RETURN false; END IF;
          SELECT * INTO l FROM wake_private.worker_lease WHERE singleton FOR UPDATE;
          IF NOT FOUND OR l.wake_id IS DISTINCT FROM p_wake_id OR l.token IS DISTINCT FROM p_token
             OR l.generation IS DISTINCT FROM p_generation OR l.expires_at<=clock_timestamp() THEN RETURN false; END IF;
          SELECT * INTO c FROM wake_private.worker_inbox_claim
          WHERE delivery_id=p_delivery_id AND github_app_id=l.github_app_id
            AND wake_id=p_wake_id AND lease_token=p_token
            AND lease_generation=p_generation FOR UPDATE;
          IF NOT FOUND THEN RETURN false; END IF;
          SELECT inbox.github_app_id,inbox.delivery_id,inbox.event_type,inbox.payload_sha256,
            inbox.raw_body,
            inbox.consumed_at INTO i FROM webhook_private.github_delivery_inbox inbox
          WHERE inbox.github_app_id=c.github_app_id AND inbox.delivery_id=c.delivery_id
            AND inbox.consumed_at IS NULL FOR UPDATE;
          IF NOT FOUND THEN RETURN false; END IF;
          IF i.payload_sha256 IS DISTINCT FROM pg_catalog.sha256(i.raw_body) THEN RETURN false; END IF;
          BEGIN p:=convert_from(i.raw_body,'UTF8')::jsonb;
          EXCEPTION WHEN others THEN
            IF p_outcome NOT IN ('inert','rejected') THEN RETURN false; END IF;
            UPDATE webhook_private.github_delivery_inbox SET consumed_at=clock_timestamp()
              WHERE github_app_id=c.github_app_id AND delivery_id=c.delivery_id
                AND consumed_at IS NULL;
            IF NOT FOUND THEN RETURN false; END IF;
            DELETE FROM wake_private.worker_inbox_claim
              WHERE github_app_id=c.github_app_id AND delivery_id=c.delivery_id;
            RETURN true;
          END;
          IF jsonb_typeof(p)<>'object' THEN
            IF p_outcome NOT IN ('inert','rejected') THEN RETURN false; END IF;
            UPDATE webhook_private.github_delivery_inbox SET consumed_at=clock_timestamp()
              WHERE github_app_id=c.github_app_id AND delivery_id=c.delivery_id
                AND consumed_at IS NULL;
            IF NOT FOUND THEN RETURN false; END IF;
            DELETE FROM wake_private.worker_inbox_claim
              WHERE github_app_id=c.github_app_id AND delivery_id=c.delivery_id;
            RETURN true;
          END IF;
          repo_text:=p->'repository'->>'id'; comment_text:=p->'comment'->>'id';
          author_text:=p->'comment'->'user'->>'id';
          IF repo_text ~ '^[0-9]{1,19}$' AND length(repo_text)<=19
             AND comment_text ~ '^[0-9]{1,19}$' AND length(comment_text)<=19
             AND author_text ~ '^[0-9]{1,19}$' AND length(author_text)<=19 THEN
            BEGIN repo:=repo_text::bigint; comment:=comment_text::bigint; author:=author_text::bigint;
            EXCEPTION WHEN others THEN repo:=NULL; comment:=NULL; author:=NULL; END;
          END IF;
          v_receipt:=false; v_result:=false; v_expired:=false;
          IF repo>0 AND comment>0 AND author>0 THEN
            SELECT x.owner_id,(x.expires_at<=clock_timestamp())
              INTO v_command_owner,v_expired
              FROM runtime_private.github_command_receipts x
              WHERE x.repository_id=repo AND x.comment_id=comment AND x.author_id=author
                AND (c.owner_id IS NULL OR x.owner_id=c.owner_id);
            v_receipt:=FOUND;
            IF v_receipt THEN
            SELECT EXISTS(SELECT 1 FROM runtime_private.github_command_results x
              WHERE x.owner_id=v_command_owner AND x.repository_id=repo AND x.comment_id=comment)
              INTO v_result;
            END IF;
          END IF;
          IF p_outcome='processed' AND NOT (v_receipt AND v_result) THEN RETURN false; END IF;
          IF v_receipt AND NOT v_result
             AND NOT (p_outcome='rejected' AND v_expired) THEN RETURN false; END IF;
          IF p_outcome='inert' AND v_receipt THEN RETURN false; END IF;
          IF p_outcome='rejected' AND v_receipt AND v_result THEN RETURN false; END IF;
          UPDATE webhook_private.github_delivery_inbox SET consumed_at=clock_timestamp()
            WHERE github_app_id=c.github_app_id AND delivery_id=c.delivery_id AND consumed_at IS NULL;
          DELETE FROM wake_private.worker_inbox_claim
            WHERE github_app_id=c.github_app_id AND delivery_id=c.delivery_id;
          RETURN FOUND;
        END $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION wake_private.worker_next_run(p_wake_id uuid,p_token uuid,p_generation bigint)
        RETURNS TABLE(owner_id uuid,run_id uuid)
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE l wake_private.worker_lease%ROWTYPE; r record;
        BEGIN
          SELECT * INTO l FROM wake_private.worker_lease WHERE singleton FOR UPDATE;
          IF NOT FOUND OR l.wake_id IS DISTINCT FROM p_wake_id OR l.token IS DISTINCT FROM p_token
             OR l.generation IS DISTINCT FROM p_generation OR l.expires_at<=clock_timestamp() THEN RETURN; END IF;
          SELECT j.owner_id,j.run_id INTO r FROM runtime_private.jobs j
          JOIN runtime_private.runs run ON run.owner_id=j.owner_id AND run.run_id=j.run_id
          JOIN runtime_private.users u ON u.user_id=run.owner_id
          WHERE u.status='active' AND run.status IN ('queued','running')
            AND ((j.status='queued' AND j.available_at<=clock_timestamp())
              OR (j.status='leased' AND j.lease_expires_at<=clock_timestamp()))
            AND NOT EXISTS(SELECT 1 FROM wake_private.worker_run_seen s WHERE s.wake_id=p_wake_id
              AND s.lease_generation=p_generation AND s.owner_id=j.owner_id AND s.run_id=j.run_id)
          ORDER BY j.available_at,j.created_at,j.job_id LIMIT 1;
          IF NOT FOUND THEN RETURN; END IF;
          INSERT INTO wake_private.worker_run_seen(wake_id,lease_generation,owner_id,run_id)
            VALUES(p_wake_id,p_generation,r.owner_id,r.run_id) ON CONFLICT DO NOTHING;
          IF NOT FOUND THEN RETURN; END IF;
          RETURN QUERY SELECT r.owner_id,r.run_id;
        END $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION wake_private.worker_finish(p_wake_id uuid,p_token uuid,p_generation bigint,
          p_complete boolean) RETURNS boolean
        LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        DECLARE l wake_private.worker_lease%ROWTYPE; n timestamptz; v_drained boolean:=false;
        BEGIN
          SELECT * INTO l FROM wake_private.worker_lease WHERE singleton FOR UPDATE;
          n:=clock_timestamp();
          IF NOT FOUND OR l.wake_id IS DISTINCT FROM p_wake_id OR l.token IS DISTINCT FROM p_token
             OR l.generation IS DISTINCT FROM p_generation OR l.expires_at<=n THEN RETURN false; END IF;
          IF p_complete AND NOT EXISTS(SELECT 1 FROM webhook_private.github_delivery_inbox i
                WHERE i.consumed_at IS NULL)
             AND NOT wake_private.has_actionable_work() THEN
            UPDATE wake_private.wake_receipt SET drain_status='drained' WHERE wake_id=p_wake_id;
            IF NOT FOUND THEN RETURN false; END IF;
            UPDATE wake_private.drain_watermark SET last_successful_drain_at=n WHERE singleton;
            IF NOT FOUND THEN RETURN false; END IF;
            v_drained:=true;
          ELSE
            UPDATE wake_private.wake_receipt SET drain_status='partial'
              WHERE wake_id=p_wake_id AND drain_status<>'drained';
            IF NOT FOUND THEN RETURN false; END IF;
          END IF;
          UPDATE wake_private.worker_lease SET wake_id=NULL,github_app_id=NULL,token=NULL,expires_at=NULL
            WHERE singleton AND wake_id=p_wake_id AND token=p_token AND generation=p_generation;
          IF NOT FOUND THEN RETURN false; END IF;
          RETURN v_drained;
        END $$
        """
    )


def _patch_actionable_predicates() -> None:
    # Preserve the existing SECURITY DEFINER bodies while replacing their exact
    # pending-inbox checks with the broader, bounded actionable-work predicate.
    op.execute(
        """
        DO $patch$
        DECLARE signature text; definition text; original text; replacement text; hits integer;
        BEGIN
          SELECT pg_get_functiondef('wake_private.issue_dispatch_ticket()'::regprocedure)
            INTO definition;
          original := 'v_wake_id := COALESCE(v_control.active_wake_id, pg_catalog.gen_random_uuid());';
          replacement := 'v_wake_id := CASE WHEN v_control.active_wake_id IS NOT NULL AND NOT EXISTS (' ||
            'SELECT 1 FROM wake_private.wake_receipt AS r WHERE r.wake_id = v_control.active_wake_id ' ||
            'AND r.drain_status = ' || quote_literal('drained') ||
            ') THEN v_control.active_wake_id ELSE pg_catalog.gen_random_uuid() END;';
          hits := (length(definition)-length(replace(definition,original,''))) / length(original);
          IF hits <> 1 THEN RAISE EXCEPTION 'wake_id_rotation_patch_mismatch:%',hits; END IF;
          EXECUTE replace(definition,original,replacement);
          original := 'NOT EXISTS (' || E'\n                    SELECT 1 FROM webhook_private.github_delivery_inbox AS i' ||
            E'\n                    WHERE i.consumed_at IS NULL' || E'\n                )';
          replacement := 'NOT wake_private.has_actionable_work()';
          FOREACH signature IN ARRAY ARRAY[
            'wake_private.issue_dispatch_ticket()',
            'wake_private.consume_dispatch_ticket(uuid, text)',
            'wake_private.accept_wake_receipt(uuid, bigint, uuid, text)']
          LOOP
            SELECT pg_get_functiondef(signature::regprocedure) INTO definition;
            hits := (length(definition)-length(replace(definition,original,''))) / length(original);
            IF hits <> 1 THEN RAISE EXCEPTION 'wake_actionable_patch_mismatch:%:%',signature,hits; END IF;
            EXECUTE replace(definition,original,replacement);
          END LOOP;
        END $patch$
        """
    )


def _patch_read_wake_status() -> None:
    op.execute(
        """
        DO $patch$
        DECLARE definition text; original text; replacement text; hits integer;
        BEGIN
          original := 'r.drain_status::text';
          replacement := 'wake_private.worker_status(r.wake_id,r.drain_status::text)';
          SELECT pg_get_functiondef('wake_private.read_wake_status()'::regprocedure)
            INTO definition;
          hits := (length(definition)-length(replace(definition,original,''))) / length(original);
          IF hits <> 1 THEN RAISE EXCEPTION 'wake_status_patch_mismatch:%',hits; END IF;
          EXECUTE replace(definition,original,replacement);
        END $patch$
        """
    )


def _patch_accept_latest_attempt(*, restore: bool) -> None:
    if restore:
        duplicate_original = (
            "                END IF;\n                UPDATE wake_private.wake_receipt SET "
            "last_accepted_attempt_id=p_attempt_id,last_accepted_generation=p_generation "
            "WHERE wake_id=p_wake_id;\n                RETURN QUERY SELECT\n                    'duplicate'::text,"
        )
        duplicate_replacement = "                END IF;\n                RETURN QUERY SELECT\n                    'duplicate'::text,"
        insert_original = (
            "wake_id, reason, accepted_attempt_id, accepted_generation, "
            "last_accepted_attempt_id, last_accepted_generation"
        )
        insert_replacement = "wake_id, reason, accepted_attempt_id, accepted_generation"
        values_original = (
            "p_wake_id, p_reason, p_attempt_id, p_generation, p_attempt_id, p_generation"
        )
        values_replacement = "p_wake_id, p_reason, p_attempt_id, p_generation"
        error = "wake_receipt_latest_attempt_restore_mismatch"
    else:
        duplicate_original = (
            "                RETURN QUERY SELECT\n                    'duplicate'::text,"
        )
        duplicate_replacement = (
            "                UPDATE wake_private.wake_receipt SET "
            "last_accepted_attempt_id=p_attempt_id,last_accepted_generation=p_generation "
            "WHERE wake_id=p_wake_id;\n                RETURN QUERY SELECT\n                    'duplicate'::text,"
        )
        insert_original = "wake_id, reason, accepted_attempt_id, accepted_generation"
        insert_replacement = (
            "wake_id, reason, accepted_attempt_id, accepted_generation, "
            "last_accepted_attempt_id, last_accepted_generation"
        )
        values_original = "p_wake_id, p_reason, p_attempt_id, p_generation"
        values_replacement = (
            "p_wake_id, p_reason, p_attempt_id, p_generation, p_attempt_id, p_generation"
        )
        error = "wake_receipt_latest_attempt_patch_mismatch"
    duplicate_original_sql = "'" + duplicate_original.replace("'", "''") + "'"
    duplicate_replacement_sql = "'" + duplicate_replacement.replace("'", "''") + "'"
    insert_original_sql = "'" + insert_original.replace("'", "''") + "'"
    insert_replacement_sql = "'" + insert_replacement.replace("'", "''") + "'"
    values_original_sql = "'" + values_original.replace("'", "''") + "'"
    values_replacement_sql = "'" + values_replacement.replace("'", "''") + "'"
    op.execute(
        f"""
        DO $patch$
        DECLARE definition text; original text; replacement text; hits integer;
        BEGIN
          original := {duplicate_original_sql};
          replacement := {duplicate_replacement_sql};
          SELECT pg_get_functiondef(
            'wake_private.accept_wake_receipt(uuid,bigint,uuid,text)'::regprocedure
          ) INTO definition;
          hits := (length(definition)-length(replace(definition,original,''))) / length(original);
          IF hits <> 1 THEN RAISE EXCEPTION '{error}:duplicate:%',hits; END IF;
          definition := replace(definition,original,replacement);
          original := {insert_original_sql};
          replacement := {insert_replacement_sql};
          hits := (length(definition)-length(replace(definition,original,''))) / length(original);
          IF hits <> 1 THEN RAISE EXCEPTION '{error}:insert:%',hits; END IF;
          definition := replace(definition,original,replacement);
          original := {values_original_sql}; replacement := {values_replacement_sql};
          hits := (length(definition)-length(replace(definition,original,''))) / length(original);
          IF hits <> 1 THEN RAISE EXCEPTION '{error}:values:%',hits; END IF;
          EXECUTE replace(definition,original,replacement);
        END $patch$
        """
    )


def _patch_active_attempt_retention(*, restore: bool) -> None:
    if restore:
        original = (
            "SET active_attempt_id = p_attempt_id,\n                    lease_expires_at = NULL,"
            "\n                    failure_count = 0,"
        )
        replacement = (
            "SET active_attempt_id = NULL,\n                    lease_expires_at = NULL,"
            "\n                    failure_count = 0,"
        )
        error = "wake_active_attempt_restore_mismatch"
    else:
        original = (
            "SET active_attempt_id = NULL,\n                    lease_expires_at = NULL,"
            "\n                    failure_count = 0,"
        )
        replacement = (
            "SET active_attempt_id = p_attempt_id,\n                    lease_expires_at = NULL,"
            "\n                    failure_count = 0,"
        )
        error = "wake_active_attempt_patch_mismatch"
    original_sql = "'" + original.replace("'", "''") + "'"
    replacement_sql = "'" + replacement.replace("'", "''") + "'"
    op.execute(
        f"""
        DO $patch$
        DECLARE definition text; original text; replacement text; hits integer;
        BEGIN
          original := {original_sql}; replacement := {replacement_sql};
          SELECT pg_get_functiondef(
            'wake_private.record_dispatch_result(uuid,bigint,text,smallint)'::regprocedure
          ) INTO definition;
          hits := (length(definition)-length(replace(definition,original,''))) / length(original);
          IF hits <> 1 THEN RAISE EXCEPTION '{error}:%',hits; END IF;
          EXECUTE replace(definition,original,replacement);
        END $patch$
        """
    )


def downgrade() -> None:
    # Restore the original function bodies by swapping the helper predicate back.
    op.execute(f"GRANT CREATE ON SCHEMA {WAKE} TO {ISSUER_ROLE}")
    op.execute(f"SET ROLE {ISSUER_ROLE}")
    op.execute(
        """
        DO $patch$
        DECLARE signature text; definition text; original text; replacement text; hits integer;
        BEGIN
          original := 'NOT wake_private.has_actionable_work()';
          replacement := 'NOT EXISTS (' || E'\n                    SELECT 1 FROM webhook_private.github_delivery_inbox AS i' ||
            E'\n                    WHERE i.consumed_at IS NULL' || E'\n                )';
          FOREACH signature IN ARRAY ARRAY[
            'wake_private.issue_dispatch_ticket()',
            'wake_private.consume_dispatch_ticket(uuid, text)',
            'wake_private.accept_wake_receipt(uuid, bigint, uuid, text)']
          LOOP
            SELECT pg_get_functiondef(signature::regprocedure) INTO definition;
            hits := (length(definition)-length(replace(definition,original,''))) / length(original);
            IF hits <> 1 THEN RAISE EXCEPTION 'wake_actionable_restore_mismatch:%:%',signature,hits; END IF;
            EXECUTE replace(definition,original,replacement);
          END LOOP;
          SELECT pg_get_functiondef('wake_private.issue_dispatch_ticket()'::regprocedure)
            INTO definition;
          original := 'v_wake_id := CASE WHEN v_control.active_wake_id IS NOT NULL AND NOT EXISTS (' ||
            'SELECT 1 FROM wake_private.wake_receipt AS r WHERE r.wake_id = v_control.active_wake_id ' ||
            'AND r.drain_status = ' || quote_literal('drained') ||
            ') THEN v_control.active_wake_id ELSE pg_catalog.gen_random_uuid() END;';
          replacement := 'v_wake_id := COALESCE(v_control.active_wake_id, pg_catalog.gen_random_uuid());';
          hits := (length(definition)-length(replace(definition,original,''))) / length(original);
          IF hits <> 1 THEN RAISE EXCEPTION 'wake_id_rotation_restore_mismatch:%',hits; END IF;
          EXECUTE replace(definition,original,replacement);
        END $patch$
        """
    )
    _patch_accept_latest_attempt(restore=True)
    _patch_active_attempt_retention(restore=True)
    op.execute(
        """
        DO $patch$
        DECLARE definition text; original text; replacement text; hits integer;
        BEGIN
          original := 'wake_private.worker_status(r.wake_id,r.drain_status::text)';
          replacement := 'r.drain_status::text';
          SELECT pg_get_functiondef('wake_private.read_wake_status()'::regprocedure)
            INTO definition;
          hits := (length(definition)-length(replace(definition,original,''))) / length(original);
          IF hits <> 1 THEN RAISE EXCEPTION 'wake_status_restore_mismatch:%',hits; END IF;
          EXECUTE replace(definition,original,replacement);
        END $patch$
        """
    )
    op.execute("RESET ROLE")
    op.execute(f"REVOKE CREATE ON SCHEMA {WAKE} FROM {ISSUER_ROLE}")
    for policy_table, policy_name in (
        ("webhook_private.github_delivery_inbox", "wake_worker_pending_inbox_select"),
        ("webhook_private.github_delivery_inbox", "wake_worker_pending_inbox_update"),
        ("runtime_private.users", "wake_worker_select_users"),
        ("runtime_private.repository_registrations", "wake_worker_select_repository_registrations"),
        ("runtime_private.runs", "wake_worker_select_runs"),
        ("runtime_private.jobs", "wake_worker_select_jobs"),
        ("wake_private.wake_receipt", "wake_worker_receipt_select"),
        ("wake_private.wake_receipt", "wake_worker_receipt_update"),
        ("wake_private.wake_receipt", "wake_issuer_update_latest_receipt"),
        ("wake_private.dispatch_control", "wake_worker_control_select"),
        ("wake_private.dispatch_control", "wake_worker_control_lock"),
        ("wake_private.drain_watermark", "wake_worker_watermark_select"),
        ("wake_private.drain_watermark", "wake_worker_watermark_update"),
        ("runtime_private.github_command_receipts", "wake_worker_command_receipt_select"),
        ("runtime_private.github_command_results", "wake_worker_command_result_select"),
    ):
        op.execute(f"DROP POLICY {policy_name} ON {policy_table}")
    op.execute(f"REVOKE SELECT, UPDATE ON {INBOX} FROM {OWNER_ROLE}")
    for schema_table in (
        "runtime_private.users",
        "runtime_private.repository_registrations",
        "runtime_private.runs",
        "runtime_private.jobs",
        "runtime_private.github_command_receipts",
        "runtime_private.github_command_results",
        "wake_private.wake_receipt",
        "wake_private.dispatch_control",
        "wake_private.drain_watermark",
    ):
        op.execute(f"REVOKE ALL ON {schema_table} FROM {OWNER_ROLE}")
    op.execute(
        f"REVOKE SELECT (github_app_id,delivery_id,event_type,action,payload_sha256,raw_body,received_at,consumed_at) "
        f"ON {INBOX} FROM {OWNER_ROLE}"
    )
    op.execute(f"REVOKE UPDATE (consumed_at) ON {INBOX} FROM {OWNER_ROLE}")
    for table, columns in (
        ("runtime_private.users", "user_id,github_user_id,status"),
        ("runtime_private.repository_registrations", "owner_id,github_repository_id"),
        ("runtime_private.runs", "owner_id,run_id,status"),
        (
            "runtime_private.jobs",
            "owner_id,run_id,job_id,status,available_at,lease_expires_at,created_at",
        ),
        (
            "runtime_private.github_command_receipts",
            "owner_id,repository_id,comment_id,author_id,expires_at",
        ),
        ("runtime_private.github_command_results", "owner_id,repository_id,comment_id"),
        (
            "wake_private.dispatch_control",
            "singleton,generation,active_wake_id,active_attempt_id",
        ),
        (
            "wake_private.wake_receipt",
            "wake_id,reason,accepted_attempt_id,accepted_generation,accepted_at,drain_status,"
            "last_accepted_attempt_id,last_accepted_generation",
        ),
        ("wake_private.drain_watermark", "singleton,last_successful_drain_at"),
    ):
        op.execute(f"REVOKE SELECT ({columns}) ON {table} FROM {OWNER_ROLE}")
    op.execute("REVOKE UPDATE (drain_status) ON wake_private.wake_receipt FROM " + OWNER_ROLE)
    op.execute(
        "REVOKE UPDATE (last_accepted_attempt_id,last_accepted_generation) "
        "ON wake_private.wake_receipt FROM " + ISSUER_ROLE
    )
    op.execute("REVOKE UPDATE (singleton) ON wake_private.dispatch_control FROM " + OWNER_ROLE)
    op.execute(
        "REVOKE UPDATE (last_successful_drain_at) ON wake_private.drain_watermark FROM "
        + OWNER_ROLE
    )
    op.execute(f"SET ROLE {OWNER_ROLE}")
    for signature in (
        "worker_finish(uuid,uuid,bigint,boolean)",
        "worker_next_run(uuid,uuid,bigint)",
        "worker_complete_inbox(uuid,uuid,bigint,uuid,text)",
        "worker_next_inbox(uuid,uuid,bigint)",
        "worker_heartbeat(uuid,uuid,bigint)",
        "worker_claim(uuid,uuid,bigint,bigint)",
        "worker_status(uuid,text)",
        "resolve_inbox_owner(jsonb)",
        "resolve_inbox_command_state(jsonb)",
        "has_actionable_work()",
    ):
        op.execute(f"DROP FUNCTION {WAKE}.{signature}")
    op.execute("RESET ROLE")
    op.drop_table("worker_run_seen", schema=WAKE)
    op.drop_index("ix_wake_worker_inbox_claim_lease", table_name="worker_inbox_claim", schema=WAKE)
    op.drop_table("worker_inbox_claim", schema=WAKE)
    op.drop_table("worker_lease", schema=WAKE)
    op.execute(
        "UPDATE wake_private.wake_receipt SET drain_status='worker_not_started' "
        "WHERE drain_status IN ('running','partial')"
    )
    op.execute("ALTER TABLE wake_private.wake_receipt DROP CONSTRAINT ck_wake_receipt_drain_status")
    op.execute(
        "ALTER TABLE wake_private.wake_receipt ADD CONSTRAINT ck_wake_receipt_drain_status "
        "CHECK (drain_status IN ('worker_not_started','drained'))"
    )
    op.drop_constraint("ck_wake_receipt_latest_generation", "wake_receipt", schema=WAKE)
    op.drop_column("wake_receipt", "last_accepted_generation", schema=WAKE)
    op.drop_column("wake_receipt", "last_accepted_attempt_id", schema=WAKE)
    op.execute(f"REVOKE USAGE ON SCHEMA webhook_private, runtime_private, {WAKE} FROM {OWNER_ROLE}")
    op.execute(f"REVOKE USAGE ON SCHEMA {WAKE} FROM {LOGIN_ROLE}")
    bind = op.get_bind()
    database_name = bind.execute(sa.text("SELECT current_database()")).scalar_one()
    quoted_database = bind.dialect.identifier_preparer.quote(database_name)
    op.execute(f"REVOKE CONNECT ON DATABASE {quoted_database} FROM {LOGIN_ROLE}")
