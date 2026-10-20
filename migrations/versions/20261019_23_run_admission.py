"""Persist owner-scoped run admissions and a bounded global public queue.

Revision ID: 20261019_23
Revises: 20261019_22
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261019_23"
down_revision = "20261019_22"
branch_labels = None
depends_on = None

SCHEMA = "runtime_private"
RUNTIME_ROLE = "continuum_runtime"
ADMISSION_ROLE = "continuum_admission"


def upgrade() -> None:
    _validate_admission_role()
    op.execute(f"ALTER TABLE {SCHEMA}.runs DROP CONSTRAINT ck_runtime_runs_repository_issue_pair")
    op.execute(
        f"ALTER TABLE {SCHEMA}.runs ADD CONSTRAINT ck_runtime_runs_repository_issue_pair "
        "CHECK ((github_repository_id IS NULL AND issue_number IS NULL) OR "
        "(github_repository_id IS NOT NULL AND github_repository_id > 0 AND "
        "(issue_number IS NULL OR issue_number > 0))) NOT VALID"
    )
    op.execute(
        f"ALTER TABLE {SCHEMA}.runs VALIDATE CONSTRAINT ck_runtime_runs_repository_issue_pair"
    )

    op.create_table(
        "run_admission_pins",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("registration_id", sa.BigInteger(), nullable=False),
        sa.Column("defaults_version", sa.BigInteger(), nullable=False),
        sa.Column("profile_id", sa.String(32), nullable=False),
        sa.Column("profile_version", sa.String(16), nullable=False),
        sa.Column("base_branch", sa.String(255), nullable=False),
        sa.Column("source_sha", sa.String(64), nullable=False),
        sa.Column("source_tree_sha", sa.String(64), nullable=False),
        sa.Column("source_receipt", JSONB(), nullable=False),
        sa.Column("issue_snapshot", JSONB()),
        sa.Column("provider", sa.String(16), nullable=False),
        sa.Column("model_id", sa.String(64), nullable=False),
        sa.Column("task_context", JSONB(), nullable=False),
        sa.Column("submission_digest", sa.LargeBinary(32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("owner_id", "run_id", name="pk_run_admission_pins"),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "registration_id"],
            [
                f"{SCHEMA}.repository_registrations.owner_id",
                f"{SCHEMA}.repository_registrations.registration_id",
            ],
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("defaults_version > 0", name="ck_run_admission_defaults_version"),
        sa.CheckConstraint(
            "profile_id IN ('python-pytest','node-npm') AND profile_version='1'",
            name="ck_run_admission_profile",
        ),
        sa.CheckConstraint("provider IN ('openai','anthropic')", name="ck_run_admission_provider"),
        sa.CheckConstraint(
            "length(btrim(base_branch)) BETWEEN 1 AND 255", name="ck_run_admission_base_branch"
        ),
        sa.CheckConstraint(
            "source_sha ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'", name="ck_run_admission_source_sha"
        ),
        sa.CheckConstraint(
            "source_tree_sha ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'", name="ck_run_admission_tree_sha"
        ),
        sa.CheckConstraint("octet_length(submission_digest)=32", name="ck_run_admission_digest"),
        sa.CheckConstraint(
            "jsonb_typeof(task_context)='object' AND "
            "task_context ? 'task' AND octet_length(task_context::text)<=16384",
            name="ck_run_admission_task_context",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(source_receipt)='object' AND octet_length(source_receipt::text)<=16384",
            name="ck_run_admission_receipt",
        ),
        sa.CheckConstraint(
            "issue_snapshot IS NULL OR (jsonb_typeof(issue_snapshot)='object' "
            "AND octet_length(issue_snapshot::text)<=32768)",
            name="ck_run_admission_issue_snapshot",
        ),
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.run_admission_pins ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.run_admission_pins FORCE ROW LEVEL SECURITY")
    predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.run_admission_pins "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.run_admission_pins FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.run_admission_pins TO {RUNTIME_ROLE}")

    op.create_table(
        "admission_submissions",
        sa.Column("submission_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("origin_kind", sa.String(16), nullable=False),
        sa.Column("origin_key", sa.String(128), nullable=False),
        sa.Column("repository_id", sa.BigInteger(), nullable=False),
        sa.Column("issue_number", sa.BigInteger()),
        sa.Column("request_id", sa.Uuid()),
        sa.Column("continuation_id", sa.Uuid()),
        sa.Column("input_digest", sa.LargeBinary(32), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("submission_id", name="pk_admission_submissions"),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "continuation_id"],
            [
                f"{SCHEMA}.setup_continuations.owner_id",
                f"{SCHEMA}.setup_continuations.continuation_id",
            ],
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "origin_kind IN ('web_issue','free_text','github_comment')",
            name="ck_admission_submission_kind",
        ),
        sa.CheckConstraint("repository_id > 0", name="ck_admission_submission_repo"),
        sa.CheckConstraint(
            "issue_number IS NULL OR issue_number > 0", name="ck_admission_submission_issue"
        ),
        sa.CheckConstraint(
            "request_id IS NULL OR origin_kind IN ('web_issue','free_text')",
            name="ck_admission_submission_request_id",
        ),
        sa.CheckConstraint(
            "continuation_id IS NULL OR origin_kind='github_comment'",
            name="ck_admission_submission_continuation",
        ),
        sa.CheckConstraint("octet_length(input_digest)=32", name="ck_admission_submission_digest"),
        sa.CheckConstraint(
            "(origin_kind='web_issue' AND issue_number IS NOT NULL AND request_id IS NOT NULL "
            "AND continuation_id IS NULL) OR "
            "(origin_kind='free_text' AND issue_number IS NULL AND request_id IS NOT NULL "
            "AND continuation_id IS NULL) OR "
            "(origin_kind='github_comment' AND issue_number IS NOT NULL AND request_id IS NULL "
            "AND continuation_id IS NOT NULL)",
            name="ck_admission_submission_identity",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_admission_submissions_owner_run",
        "admission_submissions",
        ["owner_id", "run_id"],
        schema=SCHEMA,
    )
    op.create_index(
        "uq_admission_api_request",
        "admission_submissions",
        ["owner_id", "origin_kind", "request_id"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("request_id IS NOT NULL"),
    )
    op.create_index(
        "uq_admission_comment_origin",
        "admission_submissions",
        ["owner_id", "repository_id", "origin_key"],
        unique=True,
        schema=SCHEMA,
        postgresql_where=sa.text("origin_kind='github_comment'"),
    )
    op.execute(f"ALTER TABLE {SCHEMA}.admission_submissions ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.admission_submissions FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.admission_submissions "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.admission_submissions FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.admission_submissions TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT SELECT (owner_id, run_id, active), UPDATE (active) "
        f"ON {SCHEMA}.admission_submissions TO {ADMISSION_ROLE}"
    )

    op.create_table(
        "admission_idempotency",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("route", sa.String(64), nullable=False),
        sa.Column("key_hash", sa.LargeBinary(32), nullable=False),
        sa.Column("input_digest", sa.LargeBinary(32), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("response_status", sa.SmallInteger(), nullable=False, server_default="202"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("owner_id", "route", "key_hash", name="pk_admission_idempotency"),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("length(btrim(route)) BETWEEN 1 AND 64", name="ck_idem_route"),
        sa.CheckConstraint("octet_length(key_hash)=32", name="ck_idem_key_hash"),
        sa.CheckConstraint("octet_length(input_digest)=32", name="ck_idem_input_digest"),
        sa.CheckConstraint("response_status=202", name="ck_idem_response_status"),
        sa.CheckConstraint("expires_at=created_at + interval '24 hours'", name="ck_idem_expiry"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_admission_idempotency_owner_expiry",
        "admission_idempotency",
        ["owner_id", "expires_at"],
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.admission_idempotency ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.admission_idempotency FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.admission_idempotency "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.admission_idempotency FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT, DELETE ON {SCHEMA}.admission_idempotency TO {RUNTIME_ROLE}")

    op.create_table(
        "admission_queue_seats",
        sa.Column("seat_number", sa.SmallInteger(), nullable=False),
        sa.Column("owner_id", sa.Uuid()),
        sa.Column("run_id", sa.Uuid()),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.PrimaryKeyConstraint("seat_number", name="pk_admission_queue_seats"),
        sa.UniqueConstraint("owner_id", "run_id", name="uq_admission_queue_run"),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            [f"{SCHEMA}.runs.owner_id", f"{SCHEMA}.runs.run_id"],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("seat_number BETWEEN 1 AND 10", name="ck_admission_queue_seat"),
        sa.CheckConstraint(
            "(owner_id IS NULL AND run_id IS NULL AND claimed_at IS NULL) OR "
            "(owner_id IS NOT NULL AND run_id IS NOT NULL AND claimed_at IS NOT NULL)",
            name="ck_admission_queue_occupant",
        ),
        schema=SCHEMA,
    )
    op.bulk_insert(
        sa.table(
            "admission_queue_seats", sa.column("seat_number", sa.SmallInteger()), schema=SCHEMA
        ),
        [{"seat_number": index} for index in range(1, 11)],
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.admission_queue_seats FROM PUBLIC, {RUNTIME_ROLE}")
    op.execute(f"GRANT SELECT, UPDATE ON {SCHEMA}.admission_queue_seats TO {ADMISSION_ROLE}")

    # The no-login role can inspect only own runs under an additional principal-bound RLS policy.
    op.execute(f"GRANT USAGE ON SCHEMA {SCHEMA} TO {ADMISSION_ROLE}")
    op.execute(f"GRANT SELECT (owner_id, run_id, status) ON {SCHEMA}.runs TO {ADMISSION_ROLE}")
    op.execute(f"GRANT CREATE ON SCHEMA {SCHEMA} TO {ADMISSION_ROLE}")
    op.execute(f"ALTER TABLE {SCHEMA}.runs ENABLE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY admission_role_reads_principal_run ON {SCHEMA}.runs "
        f"FOR SELECT TO {ADMISSION_ROLE} USING ({predicate})"
    )
    op.execute(
        f"CREATE POLICY admission_role_updates_principal_submission "
        f"ON {SCHEMA}.admission_submissions FOR UPDATE TO {ADMISSION_ROLE} "
        f"USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(
        f"CREATE POLICY admission_role_reads_principal_submission "
        f"ON {SCHEMA}.admission_submissions FOR SELECT TO {ADMISSION_ROLE} "
        f"USING ({predicate})"
    )
    op.execute(
        f"CREATE FUNCTION {SCHEMA}.reserve_admission_queue_seat(p_owner uuid, p_run uuid) "
        "RETURNS smallint LANGUAGE plpgsql SECURITY DEFINER "
        f"SET search_path=pg_catalog,{SCHEMA} AS $$ DECLARE seat smallint; BEGIN "
        "IF p_owner IS NULL OR p_run IS NULL OR "
        "NULLIF(current_setting('continuum.principal_id', true), '')::uuid "
        "IS DISTINCT FROM p_owner THEN "
        "RAISE EXCEPTION 'admission_principal_mismatch' USING ERRCODE='42501'; END IF; "
        f"IF NOT EXISTS (SELECT 1 FROM {SCHEMA}.runs WHERE owner_id=p_owner AND run_id=p_run "
        "AND status IN ('queued','running','waiting_approval','waiting_credentials',"
        "'waiting_capacity','needs_attention')) THEN "
        "RAISE EXCEPTION 'admission_run_not_active' USING ERRCODE='23514'; END IF; "
        f"UPDATE {SCHEMA}.admission_queue_seats SET owner_id=p_owner, run_id=p_run, "
        "claimed_at=clock_timestamp() WHERE seat_number=(SELECT seat_number "
        f"FROM {SCHEMA}.admission_queue_seats WHERE owner_id IS NULL "
        "ORDER BY seat_number FOR UPDATE SKIP LOCKED LIMIT 1) "
        "AND owner_id IS NULL RETURNING seat_number INTO seat; "
        "IF seat IS NULL THEN RAISE EXCEPTION 'admission_global_queue_full' "
        "USING ERRCODE='53300'; END IF; RETURN seat; END $$"
    )
    op.execute(
        f"ALTER FUNCTION {SCHEMA}.reserve_admission_queue_seat(uuid,uuid) OWNER TO {ADMISSION_ROLE}"
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {SCHEMA}.reserve_admission_queue_seat(uuid,uuid) FROM PUBLIC"
    )
    op.execute(
        f"CREATE FUNCTION {SCHEMA}.admission_submission_claim_seat() RETURNS trigger "
        f"LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,{SCHEMA} AS $$ BEGIN "
        f"PERFORM {SCHEMA}.reserve_admission_queue_seat(NEW.owner_id, NEW.run_id); "
        "RETURN NEW; END $$"
    )
    op.execute(
        f"ALTER FUNCTION {SCHEMA}.admission_submission_claim_seat() OWNER TO {ADMISSION_ROLE}"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.admission_submission_claim_seat() FROM PUBLIC")
    op.execute(
        f"CREATE FUNCTION {SCHEMA}.release_admission_queue_seat() RETURNS trigger "
        f"LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,{SCHEMA} AS $$ BEGIN "
        "IF NEW.status IN ('succeeded','failed','cancelled','expired') AND "
        "OLD.status NOT IN ('succeeded','failed','cancelled','expired') THEN "
        f"UPDATE {SCHEMA}.admission_queue_seats SET owner_id=NULL, run_id=NULL, claimed_at=NULL "
        "WHERE owner_id=NEW.owner_id AND run_id=NEW.run_id; "
        f"UPDATE {SCHEMA}.admission_submissions SET active=false "
        "WHERE owner_id=NEW.owner_id AND run_id=NEW.run_id AND active; "
        "END IF; RETURN NEW; END $$"
    )
    op.execute(f"ALTER FUNCTION {SCHEMA}.release_admission_queue_seat() OWNER TO {ADMISSION_ROLE}")
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.release_admission_queue_seat() FROM PUBLIC")
    op.execute(
        f"CREATE TRIGGER release_admission_queue_seat AFTER UPDATE OF status "
        f"ON {SCHEMA}.runs FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.release_admission_queue_seat()"
    )
    op.execute(
        f"CREATE TRIGGER admission_submission_claim_seat "
        f"AFTER INSERT ON {SCHEMA}.run_admission_pins FOR EACH ROW "
        f"EXECUTE FUNCTION {SCHEMA}.admission_submission_claim_seat()"
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION {SCHEMA}.admission_submission_claim_seat() FROM {RUNTIME_ROLE}"
    )
    op.execute(f"REVOKE CREATE ON SCHEMA {SCHEMA} FROM {ADMISSION_ROLE}")


def downgrade() -> None:
    _validate_admission_role()
    op.execute(f"DROP TRIGGER IF EXISTS release_admission_queue_seat ON {SCHEMA}.runs")
    op.execute(
        f"DROP TRIGGER IF EXISTS admission_submission_claim_seat ON {SCHEMA}.run_admission_pins"
    )
    for function, signature in (
        ("release_admission_queue_seat", ""),
        ("admission_submission_claim_seat", ""),
        ("reserve_admission_queue_seat", "uuid,uuid"),
    ):
        op.execute(f"DROP FUNCTION IF EXISTS {SCHEMA}.{function}({signature})")
    op.execute(f"DROP POLICY IF EXISTS admission_role_reads_principal_run ON {SCHEMA}.runs")
    op.execute(
        f"DROP POLICY IF EXISTS admission_role_updates_principal_submission "
        f"ON {SCHEMA}.admission_submissions"
    )
    op.execute(
        f"DROP POLICY IF EXISTS admission_role_reads_principal_submission "
        f"ON {SCHEMA}.admission_submissions"
    )
    for table in (
        "admission_idempotency",
        "admission_submissions",
        "run_admission_pins",
        "admission_queue_seats",
    ):
        op.drop_table(table, schema=SCHEMA)
    op.execute(f"REVOKE SELECT (owner_id, run_id, status) ON {SCHEMA}.runs FROM {ADMISSION_ROLE}")
    op.execute(f"REVOKE USAGE ON SCHEMA {SCHEMA} FROM {ADMISSION_ROLE}")
    op.execute(f"ALTER TABLE {SCHEMA}.runs DROP CONSTRAINT ck_runtime_runs_repository_issue_pair")
    op.execute(
        f"ALTER TABLE {SCHEMA}.runs ADD CONSTRAINT ck_runtime_runs_repository_issue_pair "
        "CHECK ((github_repository_id IS NULL AND issue_number IS NULL) OR "
        "(github_repository_id IS NOT NULL AND issue_number IS NOT NULL AND "
        "github_repository_id > 0 AND issue_number > 0)) NOT VALID"
    )


def _validate_admission_role() -> None:
    bind = op.get_bind()
    migration_role = bind.execute(sa.text("SELECT current_user")).scalar_one()
    row = bind.execute(
        sa.text(
            "SELECT r.rolcanlogin, r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR "
            "r.rolreplication OR r.rolbypassrls AS elevated, r.rolinherit, "
            "has_schema_privilege(r.oid, 'runtime_private', 'CREATE') AS has_schema_create, "
            "EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member=r.oid) AS has_parent, "
            "EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.roleid=r.oid "
            "AND pg_get_userbyid(m.member)<>:migration_role) AS has_untrusted_member, "
            "EXISTS (SELECT 1 FROM pg_roles mr WHERE mr.rolname=:migration_role "
            "AND mr.rolsuper) AS migration_is_superuser, "
            "EXISTS (SELECT 1 FROM pg_auth_members m JOIN pg_roles mr "
            "ON mr.oid=m.member WHERE mr.rolname=:migration_role AND m.roleid=r.oid) "
            "AS migration_is_member FROM pg_roles r WHERE r.rolname=:role_name"
        ),
        {"migration_role": migration_role, "role_name": ADMISSION_ROLE},
    ).one_or_none()
    if row is None:
        raise RuntimeError("create_continuum_admission_role_before_migration")
    if (
        row.rolcanlogin
        or row.elevated
        or row.rolinherit
        or row.has_schema_create
        or row.has_parent
        or row.has_untrusted_member
        or (not row.migration_is_superuser and not row.migration_is_member)
    ):
        raise RuntimeError("continuum_admission_role_privileges_invalid")
