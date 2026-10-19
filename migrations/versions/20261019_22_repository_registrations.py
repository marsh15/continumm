"""Owner-scoped repository registrations and future-run defaults."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "20261019_22"
down_revision = "20261019_21"
branch_labels = None
depends_on = None

SCHEMA = "runtime_private"
RUNTIME_ROLE = "continuum_runtime"


def upgrade() -> None:
    op.create_table(
        "repository_registrations",
        sa.Column("registration_id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("github_repository_id", sa.BigInteger(), nullable=False),
        sa.Column("repository_owner", sa.String(39), nullable=False),
        sa.Column("repository_name", sa.String(100), nullable=False),
        sa.Column("profile_id", sa.String(32), nullable=False),
        sa.Column("profile_version", sa.String(16), nullable=False, server_default="1"),
        sa.Column("defaults_version", sa.BigInteger(), nullable=False, server_default="1"),
        sa.Column("provider", sa.String(16)),
        sa.Column("credential_id", sa.Uuid()),
        sa.Column("eligibility_status", sa.String(16), nullable=False, server_default="unchecked"),
        sa.Column("checked_sha", sa.String(64)),
        sa.Column("eligibility_checked_at", sa.DateTime(timezone=True)),
        sa.Column("eligibility_error", sa.String(64)),
        sa.Column("eligibility_receipt", JSONB()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.PrimaryKeyConstraint("owner_id", "registration_id", name="pk_repository_registrations"),
        sa.UniqueConstraint(
            "owner_id", "github_repository_id", name="uq_repository_registrations_owner_repo"
        ),
        sa.ForeignKeyConstraint(["owner_id"], [f"{SCHEMA}.users.user_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["owner_id", "provider", "credential_id"],
            [
                f"{SCHEMA}.provider_credentials.owner_id",
                f"{SCHEMA}.provider_credentials.provider",
                f"{SCHEMA}.provider_credentials.credential_id",
            ],
            name="fk_repository_registration_credential",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("registration_id > 0", name="ck_repository_registration_id"),
        sa.CheckConstraint("github_repository_id > 0", name="ck_repository_registration_repo_id"),
        sa.CheckConstraint(
            "repository_owner ~ '^[A-Za-z0-9-]{1,39}$'",
            name="ck_repository_registration_owner",
        ),
        sa.CheckConstraint(
            "repository_name ~ '^[A-Za-z0-9_.-]{1,100}$'",
            name="ck_repository_registration_name",
        ),
        sa.CheckConstraint(
            "profile_id IN ('python-pytest', 'node-npm') AND profile_version = '1'",
            name="ck_repository_registration_profile",
        ),
        sa.CheckConstraint(
            "defaults_version > 0", name="ck_repository_registration_defaults_version"
        ),
        sa.CheckConstraint(
            "provider IS NULL OR provider IN ('openai', 'anthropic')",
            name="ck_repository_registration_provider",
        ),
        sa.CheckConstraint(
            "credential_id IS NULL OR provider IS NOT NULL",
            name="ck_repository_registration_credential_provider",
        ),
        sa.CheckConstraint(
            "eligibility_status IN ('unchecked', 'eligible', 'ineligible', 'error')",
            name="ck_repository_registration_eligibility_status",
        ),
        sa.CheckConstraint(
            "checked_sha IS NULL OR checked_sha ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'",
            name="ck_repository_registration_checked_sha",
        ),
        sa.CheckConstraint(
            "(eligibility_status = 'unchecked' AND eligibility_checked_at IS NULL "
            "AND checked_sha IS NULL AND eligibility_error IS NULL "
            "AND eligibility_receipt IS NULL) "
            "OR (eligibility_status = 'eligible' AND eligibility_checked_at IS NOT NULL "
            "AND checked_sha IS NOT NULL AND eligibility_error IS NULL "
            "AND jsonb_typeof(eligibility_receipt) = 'object') "
            "OR (eligibility_status IN ('ineligible', 'error') "
            "AND eligibility_checked_at IS NOT NULL "
            "AND checked_sha IS NOT NULL AND eligibility_error IS NOT NULL "
            "AND eligibility_receipt IS NULL)",
            name="ck_repository_registration_eligibility_result",
        ),
        sa.CheckConstraint(
            "eligibility_error IS NULL OR eligibility_error ~ '^[a-z][a-z0-9_]{0,63}$'",
            name="ck_repository_registration_eligibility_error",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_repository_registrations_owner_updated",
        "repository_registrations",
        ["owner_id", "updated_at", "registration_id"],
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.repository_registrations ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.repository_registrations FORCE ROW LEVEL SECURITY")
    predicate = "owner_id = NULLIF(current_setting('continuum.principal_id', true), '')::uuid"
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.repository_registrations "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.repository_registrations FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.repository_registrations TO {RUNTIME_ROLE}")
    op.execute(
        f"GRANT UPDATE (repository_owner, repository_name, profile_id, profile_version, "
        f"defaults_version, "
        f"provider, credential_id, eligibility_status, checked_sha, eligibility_checked_at, "
        f"eligibility_error, eligibility_receipt, updated_at) "
        f"ON {SCHEMA}.repository_registrations TO {RUNTIME_ROLE}"
    )
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE {SCHEMA}.repository_registrations_registration_id_seq "
        f"TO {RUNTIME_ROLE}"
    )

    op.create_table(
        "setup_continuations",
        sa.Column("continuation_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.Uuid(), nullable=False),
        sa.Column("github_user_id", sa.BigInteger(), nullable=False),
        sa.Column("github_repository_id", sa.BigInteger(), nullable=False),
        sa.Column("github_comment_id", sa.BigInteger(), nullable=False),
        sa.Column("github_issue_number", sa.BigInteger(), nullable=False),
        sa.Column("comment_created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submission_body", sa.Text()),
        sa.Column("submission_sha256", sa.LargeBinary(32), nullable=False),
        sa.Column(
            "captured_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("clock_timestamp()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.PrimaryKeyConstraint("owner_id", "continuation_id", name="pk_setup_continuations"),
        sa.UniqueConstraint(
            "owner_id",
            "github_repository_id",
            "github_comment_id",
            name="uq_setup_continuation_comment",
        ),
        sa.ForeignKeyConstraint(
            ["owner_id", "github_user_id"],
            [f"{SCHEMA}.users.user_id", f"{SCHEMA}.users.github_user_id"],
            name="fk_setup_continuation_author",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("github_repository_id > 0", name="ck_setup_continuation_repo"),
        sa.CheckConstraint("github_comment_id > 0", name="ck_setup_continuation_comment"),
        sa.CheckConstraint("github_issue_number > 0", name="ck_setup_continuation_issue"),
        sa.CheckConstraint(
            "submission_body IS NULL OR octet_length(submission_body) BETWEEN 1 AND 16384",
            name="ck_setup_continuation_body_size",
        ),
        sa.CheckConstraint(
            "octet_length(submission_sha256) = 32",
            name="ck_setup_continuation_digest",
        ),
        sa.CheckConstraint(
            "expires_at = comment_created_at + interval '72 hours'",
            name="ck_setup_continuation_expiry",
        ),
        sa.CheckConstraint(
            "claimed_at IS NULL OR claimed_at >= captured_at",
            name="ck_setup_continuation_claim_time",
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_setup_continuations_owner_expiry",
        "setup_continuations",
        ["owner_id", "expires_at"],
        schema=SCHEMA,
    )
    op.execute(f"ALTER TABLE {SCHEMA}.setup_continuations ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {SCHEMA}.setup_continuations FORCE ROW LEVEL SECURITY")
    op.execute(
        f"CREATE POLICY runtime_owner_isolation ON {SCHEMA}.setup_continuations "
        f"FOR ALL TO {RUNTIME_ROLE} USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(f"REVOKE ALL ON {SCHEMA}.setup_continuations FROM PUBLIC")
    op.execute(f"GRANT SELECT, INSERT ON {SCHEMA}.setup_continuations TO {RUNTIME_ROLE}")
    op.execute(f"GRANT UPDATE (claimed_at) ON {SCHEMA}.setup_continuations TO {RUNTIME_ROLE}")
    op.execute(f"GRANT UPDATE (submission_body) ON {SCHEMA}.setup_continuations TO {RUNTIME_ROLE}")
    op.execute(
        f"CREATE FUNCTION {SCHEMA}.guard_setup_continuation_update() RETURNS trigger "
        "LANGUAGE plpgsql SET search_path = pg_catalog, runtime_private AS $$ "
        "BEGIN "
        "IF (NEW.owner_id, NEW.continuation_id, NEW.github_user_id, "
        "NEW.github_repository_id, NEW.github_comment_id, NEW.github_issue_number, "
        "NEW.comment_created_at, "
        "NEW.submission_sha256, NEW.captured_at, NEW.expires_at) IS DISTINCT FROM "
        "(OLD.owner_id, OLD.continuation_id, OLD.github_user_id, "
        "OLD.github_repository_id, OLD.github_comment_id, OLD.github_issue_number, "
        "OLD.comment_created_at, "
        "OLD.submission_sha256, OLD.captured_at, OLD.expires_at) THEN "
        "RAISE EXCEPTION 'setup_continuation_identity_immutable' USING ERRCODE='23514'; "
        "END IF; "
        "IF NEW.claimed_at IS DISTINCT FROM OLD.claimed_at AND "
        "(OLD.claimed_at IS NOT NULL OR NEW.claimed_at IS NULL) THEN "
        "RAISE EXCEPTION 'setup_continuation_claim_immutable' USING ERRCODE='23514'; "
        "END IF; "
        "IF NEW.submission_body IS DISTINCT FROM OLD.submission_body AND "
        "(OLD.submission_body IS NULL OR NEW.submission_body IS NOT NULL "
        "OR OLD.expires_at > clock_timestamp()) THEN "
        "RAISE EXCEPTION 'setup_continuation_body_immutable' USING ERRCODE='23514'; "
        "END IF; "
        "RETURN NEW; END $$"
    )
    op.execute(f"REVOKE ALL ON FUNCTION {SCHEMA}.guard_setup_continuation_update() FROM PUBLIC")
    op.execute(
        f"CREATE TRIGGER setup_continuation_update_guard "
        f"BEFORE UPDATE ON {SCHEMA}.setup_continuations "
        f"FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.guard_setup_continuation_update()"
    )


def downgrade() -> None:
    op.drop_table("setup_continuations", schema=SCHEMA)
    op.execute(f"DROP FUNCTION {SCHEMA}.guard_setup_continuation_update()")
    op.drop_table("repository_registrations", schema=SCHEMA)
