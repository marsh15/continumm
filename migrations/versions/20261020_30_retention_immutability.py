"""Restrict retention-only updates to inaccessible content and exact redactions.

Revision ID: 20261020_30
Revises: 20261020_29
"""

from __future__ import annotations

import json

from alembic import op

revision = "20261020_30"
down_revision = "20261020_29"
branch_labels = None
depends_on = None

REDACTIONS = {
    "run_admission_pins": {
        "task_context": {"task": "[deleted]"},
        "issue_snapshot": None,
        "source_receipt": {},
    },
    "agent_turns": {"context": {}, "tool_schemas": []},
    "agent_invocations": {"arguments": {}},
    "events": {"payload": {}},
    "accepted_results": {"payload": {}},
    "approvals": {"manifest": {}},
    "revision_feedback": {"feedback": "[deleted]"},
    "github_command_payloads": {"payload": {"feedback": ""}},
}


_EXTERNAL_GUARD_SQL = """
CREATE FUNCTION runtime_private.guard_external_operation_identity()
RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
  IF (OLD.status <> 'prepared' OR NEW.status <> 'prepared') AND (
    NEW.owner_id IS DISTINCT FROM OLD.owner_id OR
    NEW.run_id IS DISTINCT FROM OLD.run_id OR
    NEW.step_id IS DISTINCT FROM OLD.step_id OR
    NEW.job_id IS DISTINCT FROM OLD.job_id OR
    NEW.generation IS DISTINCT FROM OLD.generation OR
    NEW.attempt_id IS DISTINCT FROM OLD.attempt_id OR
    NEW.lease_owner IS DISTINCT FROM OLD.lease_owner OR
    NEW.operation_key IS DISTINCT FROM OLD.operation_key OR
    NEW.request_sha256 IS DISTINCT FROM OLD.request_sha256 OR
    NEW.request_metadata IS DISTINCT FROM OLD.request_metadata OR
    NEW.approval_id IS DISTINCT FROM OLD.approval_id OR
    NEW.approval_sha256 IS DISTINCT FROM OLD.approval_sha256
  ) THEN
    RAISE EXCEPTION 'external_operation_identity_is_immutable';
  END IF;
  IF OLD.status <> 'prepared' AND NEW.status = 'prepared' THEN
    RAISE EXCEPTION 'external_operation_dispatch_cannot_be_rearmed';
  END IF;
  IF OLD.status <> 'prepared' AND
     NEW.dispatch_authorized_at IS DISTINCT FROM OLD.dispatch_authorized_at THEN
    RAISE EXCEPTION 'external_operation_dispatch_time_is_immutable';
  END IF;
  IF NOT (
    (OLD.status = 'prepared' AND NEW.status IN ('prepared','dispatching')) OR
    (OLD.status = 'dispatching' AND NEW.status IN
      ('dispatching','confirmed','definitely_failed','unknown','conflict')) OR
    (OLD.status = 'unknown' AND NEW.status IN
      ('unknown','confirmed','definitely_failed','conflict')) OR
    (OLD.status = 'confirmed' AND NEW.status IN ('confirmed','conflict')) OR
    (OLD.status = 'definitely_failed' AND NEW.status IN
      ('definitely_failed','conflict')) OR
    (OLD.status = 'conflict' AND NEW.status = 'conflict')
  ) THEN
    RAISE EXCEPTION 'external_operation_status_transition_invalid';
  END IF;
  RETURN NEW;
END $$
"""
_SETUP_GUARD_SQL = """
CREATE FUNCTION runtime_private.guard_setup_continuation_update() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, runtime_private AS $$ BEGIN IF
(NEW.owner_id, NEW.continuation_id, NEW.github_user_id, NEW.github_repository_id,
NEW.github_comment_id, NEW.github_issue_number, NEW.comment_created_at,
NEW.submission_sha256, NEW.captured_at, NEW.expires_at) IS DISTINCT FROM (OLD.owner_id,
OLD.continuation_id, OLD.github_user_id, OLD.github_repository_id,
OLD.github_comment_id, OLD.github_issue_number, OLD.comment_created_at,
OLD.submission_sha256, OLD.captured_at, OLD.expires_at) THEN RAISE EXCEPTION
'setup_continuation_identity_immutable' USING ERRCODE='23514'; END IF; IF NEW.claimed_at
IS DISTINCT FROM OLD.claimed_at AND (OLD.claimed_at IS NOT NULL OR NEW.claimed_at IS
NULL) THEN RAISE EXCEPTION 'setup_continuation_claim_immutable' USING ERRCODE='23514';
END IF; IF NEW.submission_body IS DISTINCT FROM OLD.submission_body AND
(OLD.submission_body IS NULL OR NEW.submission_body IS NOT NULL OR OLD.expires_at >
clock_timestamp()) THEN RAISE EXCEPTION 'setup_continuation_body_immutable' USING
ERRCODE='23514'; END IF; RETURN NEW; END $$
"""
TOMBSTONE_KEYS = (
    "method",
    "repository_id",
    "github_user_id",
    "owner",
    "name",
    "branch",
    "base_branch",
    "base_sha",
    "head_sha",
    "approval_id",
    "manifest_sha256",
    "marker",
    "comment_id",
    "pr_number",
    "pr_id",
    "url",
)


_EXPORT_GUARD_SQL = """
CREATE FUNCTION operator_private.register_portable_export(
  p_export_id uuid, p_owner_id uuid, p_run_ids jsonb, p_archive_name text,
  p_manifest_sha256 bytea, p_archive_sha256 bytea, p_expires_at timestamptz
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
DECLARE v_now timestamptz; v_earliest timestamptz; v_user uuid; v_run record;
BEGIN
  PERFORM pg_advisory_xact_lock(72414020291029);
  v_now := clock_timestamp();
  IF p_export_id IS NULL OR p_owner_id IS NULL OR
     NULLIF(current_setting('continuum.principal_id', true), '')::uuid
       IS DISTINCT FROM p_owner_id OR
     p_run_ids IS NULL OR jsonb_typeof(p_run_ids)<>'array' OR
     jsonb_array_length(p_run_ids)>100 OR
     p_archive_name !~ '^[0-9a-f-]{36}[.]cpx$' OR
     p_manifest_sha256 IS NULL OR octet_length(p_manifest_sha256)<>32 OR
     p_archive_sha256 IS NULL OR octet_length(p_archive_sha256)<>32 OR
     p_expires_at<=v_now OR p_expires_at>v_now+interval '7 days' THEN
    RAISE EXCEPTION 'portable_export_input_invalid' USING ERRCODE='22023';
  END IF;
  SELECT user_id INTO v_user FROM runtime_private.users
    WHERE user_id=p_owner_id AND status='active' FOR SHARE;
  IF v_user IS NULL THEN
    RAISE EXCEPTION 'portable_export_owner_unavailable' USING ERRCODE='23514';
  END IF;
  IF EXISTS (
    SELECT 1 FROM operator_private.portable_exports AS export
    WHERE export.expires_at>v_now AND NOT EXISTS (
      SELECT 1 FROM operator_private.portable_export_invalidations AS invalidation
      WHERE invalidation.export_id=export.export_id
    )
  ) AND (SELECT count(*) FROM operator_private.portable_exports AS export
    WHERE export.expires_at>v_now AND NOT EXISTS (
      SELECT 1 FROM operator_private.portable_export_invalidations AS invalidation
      WHERE invalidation.export_id=export.export_id
    ))>=2 THEN
    RAISE EXCEPTION 'portable_export_count_limit' USING ERRCODE='23514';
  END IF;
  IF jsonb_array_length(p_run_ids)>0 THEN
    FOR v_run IN SELECT value::uuid AS run_id
      FROM jsonb_array_elements_text(p_run_ids) AS selected(value)
    LOOP
      SELECT run_id,content_expires_at,deadline_at,content_deleted_at
        INTO v_run FROM runtime_private.runs
        WHERE run_id=v_run.run_id AND owner_id=p_owner_id FOR SHARE;
      IF NOT FOUND OR v_run.content_deleted_at IS NOT NULL OR
         v_run.content_expires_at<=v_now OR
         (v_run.deadline_at IS NOT NULL AND v_run.deadline_at<=v_now) THEN
        RAISE EXCEPTION 'portable_export_content_unavailable' USING ERRCODE='23514';
      END IF;
      v_earliest := LEAST(COALESCE(v_earliest,v_run.content_expires_at),
                          v_run.content_expires_at);
    END LOOP;
    IF p_expires_at>v_earliest THEN
      RAISE EXCEPTION 'portable_export_expiry_too_late' USING ERRCODE='23514';
    END IF;
  END IF;
  INSERT INTO operator_private.portable_exports(export_id,owner_id,included_run_ids,archive_name,
    manifest_sha256,archive_sha256,created_at,expires_at)
  VALUES (p_export_id,p_owner_id,p_run_ids,p_archive_name,p_manifest_sha256,
    p_archive_sha256,v_now,p_expires_at);
END $$
"""


def upgrade() -> None:
    export_sql = _EXPORT_GUARD_SQL.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)
    export_sql = export_sql.replace(
        "v_run.content_expires_at<=v_now OR\n         "
        "(v_run.deadline_at IS NOT NULL AND v_run.deadline_at<=v_now)",
        "v_run.content_expires_at<=v_now",
    )
    if "v_run.deadline_at IS NOT NULL AND v_run.deadline_at<=v_now" in export_sql:
        raise RuntimeError("portable_export_deadline_patch_not_applied")
    op.execute(export_sql)
    # Permit only irreversible privacy redaction; active effect intent remains immutable.
    keys = ",".join("'" + key + "'" for key in TOMBSTONE_KEYS)
    external_prefix = f"""
      IF NEW.request_metadata IS DISTINCT FROM OLD.request_metadata THEN
        IF EXISTS(SELECT 1 FROM runtime_private.runs AS run
             WHERE run.owner_id=OLD.owner_id AND run.run_id=OLD.run_id
               AND (run.content_deleted_at IS NOT NULL
                    OR run.content_expires_at<=clock_timestamp()))
           AND (to_jsonb(NEW)-'request_metadata')=(to_jsonb(OLD)-'request_metadata')
           AND NEW.request_metadata=(SELECT COALESCE(jsonb_object_agg(key,value),'{{}}'::jsonb)
                FROM jsonb_each(OLD.request_metadata) WHERE key=ANY(ARRAY[{keys}])) THEN
          RETURN NEW;
        END IF;
        RAISE EXCEPTION 'external_request_metadata_immutable' USING ERRCODE='42501';
      END IF;
    """
    op.execute(
        _EXTERNAL_GUARD_SQL.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1).replace(
            "BEGIN", "BEGIN " + external_prefix, 1
        )
    )
    op.execute(
        "GRANT UPDATE (request_metadata) ON runtime_private.external_operations "
        "TO continuum_runtime"
    )
    setup_prefix = """
      IF NEW.submission_body IS NULL AND OLD.submission_body IS NOT NULL
         AND (to_jsonb(NEW)-'submission_body')=(to_jsonb(OLD)-'submission_body')
         AND (EXISTS(SELECT 1 FROM runtime_private.users AS account
              WHERE account.user_id=OLD.owner_id AND account.status IN ('deleting','deleted'))
           OR EXISTS(SELECT 1 FROM runtime_private.admission_submissions AS submission
              JOIN runtime_private.runs AS run USING(owner_id,run_id)
              WHERE submission.owner_id=OLD.owner_id
                AND submission.continuation_id=OLD.continuation_id
                AND (run.content_deleted_at IS NOT NULL
                     OR run.content_expires_at<=clock_timestamp()))) THEN
        RETURN NEW;
      END IF;
    """
    op.execute(
        _SETUP_GUARD_SQL.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1).replace(
            "BEGIN", "BEGIN " + setup_prefix, 1
        )
    )
    op.execute("""
    CREATE FUNCTION runtime_private.guard_retention_redaction()
    RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
    DECLARE v_unavailable boolean; v_field record;
    BEGIN
      SELECT EXISTS(SELECT 1 FROM runtime_private.users AS account
        WHERE account.user_id=OLD.owner_id AND account.status IN ('deleting','deleted'))
      INTO v_unavailable;
      IF TG_TABLE_NAME='github_command_payloads' THEN
        v_unavailable := v_unavailable OR OLD.expires_at<=clock_timestamp() OR EXISTS(
          SELECT 1 FROM runtime_private.github_command_receipts AS receipt
          JOIN runtime_private.approvals AS approval ON approval.owner_id=receipt.owner_id
            AND approval.approval_id::text=receipt.command_metadata->>'target_id'
          JOIN runtime_private.runs AS run ON run.owner_id=approval.owner_id
            AND run.run_id=approval.run_id
          WHERE receipt.owner_id=OLD.owner_id AND receipt.repository_id=OLD.repository_id
            AND receipt.comment_id=OLD.comment_id AND receipt.command_kind='changes'
            AND (run.content_deleted_at IS NOT NULL OR run.content_expires_at<=clock_timestamp()));
      ELSE
        v_unavailable := v_unavailable OR EXISTS(SELECT 1 FROM runtime_private.runs AS run
          WHERE run.owner_id=OLD.owner_id AND run.run_id=OLD.run_id
            AND (run.content_deleted_at IS NOT NULL OR run.content_expires_at<=clock_timestamp()
));
      END IF;
      IF NOT v_unavailable THEN
        RAISE EXCEPTION 'active_content_is_immutable' USING ERRCODE='42501';
      END IF;
      FOR v_field IN SELECT key,value FROM jsonb_each(TG_ARGV[0]::jsonb) LOOP
        IF to_jsonb(NEW)->v_field.key IS DISTINCT FROM v_field.value THEN
          RAISE EXCEPTION 'retention_requires_exact_redaction' USING ERRCODE='42501';
        END IF;
      END LOOP;
      RETURN NEW;
    END $$
    """)
    op.execute("REVOKE ALL ON FUNCTION runtime_private.guard_retention_redaction() FROM PUBLIC")
    for table, fields in REDACTIONS.items():
        expected = json.dumps(fields, separators=(",", ":")).replace("'", "''")
        columns = ",".join(fields)
        op.get_bind().exec_driver_sql(
            f"CREATE TRIGGER retention_only_redaction BEFORE UPDATE OF {columns} "
            f"ON runtime_private.{table} FOR EACH ROW EXECUTE FUNCTION "
            f"runtime_private.guard_retention_redaction('{expected}')"
        )


def downgrade() -> None:
    op.execute(_EXPORT_GUARD_SQL.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1))
    op.execute(
        "REVOKE UPDATE (request_metadata) ON runtime_private.external_operations "
        "FROM continuum_runtime"
    )
    op.execute(_EXTERNAL_GUARD_SQL.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1))
    op.execute(_SETUP_GUARD_SQL.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1))
    for table in reversed(REDACTIONS):
        op.execute(f"DROP TRIGGER retention_only_redaction ON runtime_private.{table}")
    op.execute("DROP FUNCTION runtime_private.guard_retention_redaction()")
