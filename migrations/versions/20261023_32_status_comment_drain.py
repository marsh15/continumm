"""Add a bounded, lease-fenced queue for GitHub run status comments.

Revision ID: 20261023_32
Revises: 20261022_31
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20261023_32"
down_revision = "20261022_31"
branch_labels = None
depends_on = None

OWNER = "continuum_wake_worker_owner"
LOGIN = "continuum_wake_worker"


_HAS_ACTIONABLE_OLD = """
CREATE OR REPLACE FUNCTION wake_private.has_actionable_work() RETURNS boolean
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


def upgrade() -> None:
    op.create_table(
        "status_comment_attempt",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot_key", sa.LargeBinary(32), nullable=False),
        sa.Column("event_sequence", sa.BigInteger(), nullable=False),
        sa.Column("attempts", sa.SmallInteger(), nullable=False, server_default="0"),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("completed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("lease_token", sa.Uuid()),
        sa.Column("lease_generation", sa.BigInteger()),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint(
            "owner_id", "run_id", "snapshot_key", name="pk_wake_status_comment_attempt"
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            ["runtime_private.runs.owner_id", "runtime_private.runs.run_id"],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("octet_length(snapshot_key)=32", name="ck_wake_status_snapshot_key"),
        sa.CheckConstraint("event_sequence >= 0", name="ck_wake_status_event_sequence"),
        sa.CheckConstraint("attempts BETWEEN 0 AND 3", name="ck_wake_status_attempt_count"),
        sa.CheckConstraint(
            "lease_generation IS NULL OR lease_generation > 0",
            name="ck_wake_status_lease_generation",
        ),
        sa.CheckConstraint(
            "(lease_token IS NULL) = (lease_generation IS NULL)", name="ck_wake_status_lease_pair"
        ),
        schema="wake_private",
    )
    op.create_index(
        "ix_wake_status_comment_due",
        "status_comment_attempt",
        ["next_attempt_at", "completed", "attempts"],
        schema="wake_private",
    )
    op.create_table(
        "status_comment_version",
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("owner_id", "run_id", name="pk_wake_status_comment_version"),
        sa.ForeignKeyConstraint(
            ["owner_id", "run_id"],
            ["runtime_private.runs.owner_id", "runtime_private.runs.run_id"],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("version >= 0", name="ck_wake_status_comment_version"),
        schema="wake_private",
    )
    op.execute("ALTER TABLE wake_private.status_comment_version ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE wake_private.status_comment_version FORCE ROW LEVEL SECURITY")
    op.execute(
        "REVOKE ALL ON wake_private.status_comment_version FROM PUBLIC, continuum_wake_worker"
    )
    op.execute(
        "CREATE POLICY wake_status_version_owner_all ON wake_private.status_comment_version "
        f"FOR ALL TO {OWNER} USING (true) WITH CHECK (true)"
    )
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON wake_private.status_comment_version TO {OWNER}")
    op.execute("ALTER TABLE wake_private.status_comment_attempt ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE wake_private.status_comment_attempt FORCE ROW LEVEL SECURITY")
    op.execute(
        "REVOKE ALL ON wake_private.status_comment_attempt FROM PUBLIC, continuum_wake_worker"
    )
    op.execute(
        "CREATE POLICY wake_status_attempt_owner_all ON wake_private.status_comment_attempt "
        f"FOR ALL TO {OWNER} USING (true) WITH CHECK (true)"
    )
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON wake_private.status_comment_attempt TO {OWNER}")

    # The function owner receives only columns used by the candidate query.
    for table in (
        "runtime_private.runs",
        "runtime_private.events",
        "runtime_private.approvals",
        "runtime_private.github_status_comment_effects",
    ):
        op.execute(
            f"CREATE POLICY wake_worker_status_select_{table.rsplit('.', 1)[1]} ON {table} "
            f"FOR SELECT TO {OWNER} USING (true)"
        )
    op.execute(
        "GRANT SELECT (owner_id,run_id,status,revision,github_repository_id,issue_number,"
        "content_expires_at,content_deleted_at) ON runtime_private.runs TO " + OWNER
    )
    op.execute(
        "GRANT SELECT (owner_id,run_id,sequence,event_type) ON runtime_private.events TO " + OWNER
    )
    op.execute(
        "GRANT SELECT (owner_id,run_id,approval_id,manifest_sha256,expires_at,status,revision) "
        "ON runtime_private.approvals TO " + OWNER
    )
    op.execute(
        "GRANT SELECT (owner_id,run_id,event_sequence,effect_status) "
        "ON runtime_private.github_status_comment_effects TO " + OWNER
    )
    op.execute(
        "GRANT SELECT (owner_id,github_repository_id) "
        "ON runtime_private.repository_registrations TO " + OWNER
    )
    op.execute("GRANT SELECT (user_id,status) ON runtime_private.users TO " + OWNER)
    op.execute(
        "GRANT SELECT (owner_id,run_id,version) ON wake_private.status_comment_version TO " + OWNER
    )

    op.execute("GRANT CREATE ON SCHEMA wake_private TO " + OWNER)
    op.execute("SET ROLE " + OWNER)
    op.execute("""
      CREATE FUNCTION wake_private.bump_status_comment_version()
      RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
      BEGIN
        IF ROW(NEW.status,NEW.revision) IS DISTINCT FROM ROW(OLD.status,OLD.revision) THEN
          INSERT INTO wake_private.status_comment_version(owner_id,run_id,version)
          VALUES(NEW.owner_id,NEW.run_id,1)
          ON CONFLICT ON CONSTRAINT pk_wake_status_comment_version
          DO UPDATE SET version=wake_private.status_comment_version.version+1;
        END IF;
        RETURN NEW;
      END $$
    """)
    op.execute("RESET ROLE")
    op.execute(
        "CREATE TRIGGER bump_status_comment_version AFTER UPDATE OF status,revision "
        "ON runtime_private.runs FOR EACH ROW EXECUTE FUNCTION "
        "wake_private.bump_status_comment_version()"
    )
    op.execute("SET ROLE " + OWNER)
    op.execute("""
      CREATE FUNCTION wake_private.status_comment_candidates()
      RETURNS TABLE(owner_id uuid,run_id uuid,event_sequence bigint,snapshot_key bytea,
                    reconcile_only boolean)
      LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$
        WITH current_targets AS (
          SELECT r.owner_id,r.run_id,
            COALESCE((SELECT max(e.sequence) FROM runtime_private.events e
              WHERE e.owner_id=r.owner_id AND e.run_id=r.run_id
                AND e.event_type NOT LIKE 'external_operation.%'),0)::bigint AS event_sequence,
            pg_catalog.sha256(convert_to(jsonb_build_array(r.status,r.revision,
              COALESCE(sv.version,0),
              CASE WHEN r.status='waiting_approval' THEN a.approval_id::text END,
              CASE WHEN r.status='waiting_approval' THEN encode(a.manifest_sha256,'hex') END,
              CASE WHEN r.status='waiting_approval' THEN extract(epoch FROM a.expires_at)::text END
            )::text,'UTF8')) AS snapshot_key
          FROM runtime_private.runs r
          JOIN runtime_private.users u ON u.user_id=r.owner_id
          JOIN runtime_private.repository_registrations rr
            ON rr.owner_id=r.owner_id AND rr.github_repository_id=r.github_repository_id
          LEFT JOIN wake_private.status_comment_version sv
            ON sv.owner_id=r.owner_id AND sv.run_id=r.run_id
          LEFT JOIN LATERAL (SELECT ap.approval_id,ap.manifest_sha256,ap.expires_at
            FROM runtime_private.approvals ap WHERE ap.owner_id=r.owner_id AND ap.run_id=r.run_id
              AND ap.status='pending' ORDER BY ap.revision DESC LIMIT 1) a
            ON r.status='waiting_approval'
          WHERE u.status='active'
            AND r.status IN ('queued','running','waiting_approval','waiting_credentials',
                             'waiting_capacity','needs_attention','succeeded','failed',
                             'cancelled','expired')
            AND r.github_repository_id IS NOT NULL AND r.issue_number IS NOT NULL
            AND r.content_expires_at>clock_timestamp() AND r.content_deleted_at IS NULL
        ), due_current AS (
          SELECT t.owner_id,t.run_id,t.event_sequence,t.snapshot_key,false AS reconcile_only
          FROM current_targets t
          LEFT JOIN wake_private.status_comment_attempt q
            ON q.owner_id=t.owner_id AND q.run_id=t.run_id AND q.snapshot_key=t.snapshot_key
          WHERE (q.snapshot_key IS NULL OR (NOT q.completed AND q.attempts<3
                    AND q.next_attempt_at<=clock_timestamp()))
            AND NOT EXISTS (SELECT 1 FROM runtime_private.github_status_comment_effects ef
              WHERE ef.owner_id=q.owner_id AND ef.run_id=q.run_id
                AND ef.event_sequence=q.event_sequence
                AND ef.effect_status IN ('dispatching','unknown','confirmed'))
        ), due_reconcile AS (
          SELECT q.owner_id,q.run_id,q.event_sequence,q.snapshot_key,true AS reconcile_only
          FROM wake_private.status_comment_attempt q
          JOIN runtime_private.github_status_comment_effects ef
            ON ef.owner_id=q.owner_id AND ef.run_id=q.run_id
              AND ef.event_sequence=q.event_sequence
          JOIN runtime_private.runs r ON r.owner_id=q.owner_id AND r.run_id=q.run_id
          JOIN runtime_private.users u ON u.user_id=q.owner_id
          JOIN runtime_private.repository_registrations rr
            ON rr.owner_id=r.owner_id AND rr.github_repository_id=r.github_repository_id
          WHERE ef.effect_status IN ('dispatching','unknown','confirmed')
            AND NOT q.completed AND q.attempts<3 AND q.next_attempt_at<=clock_timestamp()
            AND u.status='active'
            AND r.status IN ('queued','running','waiting_approval','waiting_credentials',
                             'waiting_capacity','needs_attention','succeeded','failed',
                             'cancelled','expired')
            AND r.github_repository_id IS NOT NULL AND r.issue_number IS NOT NULL
            AND r.content_expires_at>clock_timestamp() AND r.content_deleted_at IS NULL
        )
        SELECT * FROM due_reconcile
        UNION ALL
        SELECT c.* FROM due_current c
        WHERE NOT EXISTS (SELECT 1 FROM due_reconcile d
          WHERE d.owner_id=c.owner_id AND d.run_id=c.run_id)
        ORDER BY reconcile_only DESC,event_sequence,owner_id,run_id
        LIMIT 64
      $$
    """)

    op.execute("""
      CREATE FUNCTION wake_private.worker_next_status(p_wake_id uuid,p_token uuid,
        p_generation bigint)
      RETURNS TABLE(owner_id uuid,run_id uuid,event_sequence bigint,snapshot_key bytea,
                    reconcile_only boolean)
      LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
      DECLARE l wake_private.worker_lease%ROWTYPE; c record; n timestamptz;
      BEGIN
        SELECT * INTO l FROM wake_private.worker_lease WHERE singleton FOR UPDATE;
        n:=clock_timestamp();
        IF NOT FOUND OR l.wake_id IS DISTINCT FROM p_wake_id OR l.token IS DISTINCT FROM p_token
          OR l.generation IS DISTINCT FROM p_generation OR l.expires_at<=n THEN RETURN; END IF;
        SELECT * INTO c FROM wake_private.status_comment_candidates() LIMIT 1;
        IF NOT FOUND THEN RETURN; END IF;
        INSERT INTO wake_private.status_comment_attempt(owner_id,run_id,snapshot_key,event_sequence,
          attempts,next_attempt_at,completed,lease_token,lease_generation,updated_at)
        VALUES(c.owner_id,c.run_id,c.snapshot_key,c.event_sequence,1,n+interval '15 minutes',false,
          p_token,p_generation,n)
        ON CONFLICT ON CONSTRAINT pk_wake_status_comment_attempt DO UPDATE SET
          event_sequence=CASE WHEN c.reconcile_only THEN
            wake_private.status_comment_attempt.event_sequence ELSE EXCLUDED.event_sequence END,
          attempts=wake_private.status_comment_attempt.attempts+1,
          next_attempt_at=n+interval '15 minutes',lease_token=p_token,
          lease_generation=p_generation,updated_at=n
        WHERE NOT wake_private.status_comment_attempt.completed
          AND wake_private.status_comment_attempt.attempts<3
          AND wake_private.status_comment_attempt.next_attempt_at<=n;
        IF NOT FOUND THEN RETURN; END IF;
        RETURN QUERY SELECT c.owner_id,c.run_id,
          CASE WHEN c.reconcile_only THEN c.event_sequence ELSE
            (SELECT q.event_sequence FROM wake_private.status_comment_attempt q
             WHERE q.owner_id=c.owner_id AND q.run_id=c.run_id AND q.snapshot_key=c.snapshot_key)
          END,c.snapshot_key,c.reconcile_only;
      END $$
    """)

    op.execute("""
      CREATE FUNCTION wake_private.worker_complete_status(p_wake_id uuid,p_token uuid,
        p_generation bigint,p_owner_id uuid,p_run_id uuid,p_event_sequence bigint,
        p_snapshot_key bytea)
      RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
      DECLARE l wake_private.worker_lease%ROWTYPE;
      BEGIN
        SELECT * INTO l FROM wake_private.worker_lease WHERE singleton FOR UPDATE;
        IF NOT FOUND OR l.wake_id IS DISTINCT FROM p_wake_id OR l.token IS DISTINCT FROM p_token
          OR l.generation IS DISTINCT FROM p_generation OR l.expires_at<=clock_timestamp() THEN
          RETURN false;
        END IF;
        UPDATE wake_private.status_comment_attempt q SET completed=true,updated_at=clock_timestamp()
        WHERE q.owner_id=p_owner_id AND q.run_id=p_run_id AND q.snapshot_key=p_snapshot_key
          AND q.event_sequence=p_event_sequence AND q.lease_token=p_token
          AND q.lease_generation=p_generation AND NOT q.completed
          AND EXISTS (SELECT 1 FROM runtime_private.github_status_comment_effects ef
            WHERE ef.owner_id=q.owner_id AND ef.run_id=q.run_id
              AND ef.event_sequence=q.event_sequence AND ef.effect_status='confirmed');
        RETURN FOUND;
      END $$
    """)

    op.execute(
        "CREATE OR REPLACE FUNCTION wake_private.has_actionable_work() RETURNS boolean "
        "LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog AS $$ "
        "SELECT EXISTS(SELECT 1 FROM webhook_private.github_delivery_inbox i "
        "WHERE i.consumed_at IS NULL) OR EXISTS(SELECT 1 FROM runtime_private.jobs j "
        "JOIN runtime_private.runs r ON r.owner_id=j.owner_id AND r.run_id=j.run_id "
        "JOIN runtime_private.users u ON u.user_id=r.owner_id "
        "WHERE u.status='active' AND r.status IN ('queued','running') "
        "AND ((j.status='queued' AND j.available_at<=clock_timestamp()) OR "
        "(j.status='leased' AND j.lease_expires_at<=clock_timestamp()))) "
        "OR EXISTS(SELECT 1 FROM wake_private.status_comment_candidates()) $$"
    )

    for signature in (
        "bump_status_comment_version()",
        "status_comment_candidates()",
        "worker_next_status(uuid,uuid,bigint)",
        "worker_complete_status(uuid,uuid,bigint,uuid,uuid,bigint,bytea)",
    ):
        op.execute(f"REVOKE ALL ON FUNCTION wake_private.{signature} FROM PUBLIC, {LOGIN}")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION wake_private.worker_next_status(uuid,uuid,bigint) TO {LOGIN}"
    )
    op.execute(
        "GRANT EXECUTE ON FUNCTION wake_private.worker_complete_status("
        "uuid,uuid,bigint,uuid,uuid,bigint,bytea) "
        f"TO {LOGIN}"
    )
    op.execute("RESET ROLE")
    op.execute("REVOKE CREATE ON SCHEMA wake_private FROM " + OWNER)


def downgrade() -> None:
    op.execute("DROP TRIGGER bump_status_comment_version ON runtime_private.runs")
    op.execute("GRANT CREATE ON SCHEMA wake_private TO " + OWNER)
    op.execute("SET ROLE " + OWNER)
    op.execute("DROP FUNCTION wake_private.bump_status_comment_version()")
    op.execute(_HAS_ACTIONABLE_OLD)
    for signature in (
        "worker_complete_status(uuid,uuid,bigint,uuid,uuid,bigint,bytea)",
        "worker_next_status(uuid,uuid,bigint)",
        "status_comment_candidates()",
    ):
        op.execute(f"DROP FUNCTION wake_private.{signature}")
    op.execute("RESET ROLE")
    op.execute("REVOKE CREATE ON SCHEMA wake_private FROM " + OWNER)
    for table in ("runs", "events", "approvals", "github_status_comment_effects"):
        op.execute(f"DROP POLICY wake_worker_status_select_{table} ON runtime_private.{table}")
    op.execute(
        "REVOKE SELECT (revision,github_repository_id,issue_number,"
        "content_expires_at,content_deleted_at) ON runtime_private.runs FROM " + OWNER
    )
    op.execute(
        "REVOKE SELECT (owner_id,run_id,sequence,event_type) "
        "ON runtime_private.events FROM " + OWNER
    )
    op.execute(
        "REVOKE SELECT (owner_id,run_id,approval_id,manifest_sha256,expires_at,status,revision) "
        "ON runtime_private.approvals FROM " + OWNER
    )
    op.execute(
        "REVOKE SELECT (owner_id,run_id,event_sequence,effect_status) "
        "ON runtime_private.github_status_comment_effects FROM " + OWNER
    )
    op.execute("DROP POLICY wake_status_attempt_owner_all ON wake_private.status_comment_attempt")
    op.execute("REVOKE ALL ON wake_private.status_comment_attempt FROM " + OWNER)
    op.drop_index(
        "ix_wake_status_comment_due", table_name="status_comment_attempt", schema="wake_private"
    )
    op.drop_table("status_comment_attempt", schema="wake_private")
    op.execute("DROP POLICY wake_status_version_owner_all ON wake_private.status_comment_version")
    op.execute("REVOKE ALL ON wake_private.status_comment_version FROM " + OWNER)
    op.drop_table("status_comment_version", schema="wake_private")
