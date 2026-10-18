"""Permit fenced publication completion receipts without rewriting history."""

from alembic import op

revision = "20261018_20"
down_revision = "20261017_19"
branch_labels = None
depends_on = None

SCHEMA = "runtime_private"
CONSTRAINT = "ck_runtime_accepted_results_run_status"
_STATUSES = (
    "'running', 'waiting_approval', 'waiting_credentials', 'waiting_capacity', "
    "'needs_attention', 'failed', 'cancelled', 'expired'"
)


def upgrade() -> None:
    op.drop_constraint(CONSTRAINT, "accepted_results", schema=SCHEMA, type_="check")
    op.create_check_constraint(
        CONSTRAINT, "accepted_results", f"run_status IN ({_STATUSES}, 'succeeded')", schema=SCHEMA
    )


def downgrade() -> None:
    # Preserve immutable successful receipts. The older check still rejects all
    # new success rows; NOT VALID only exempts already persisted evidence.
    op.drop_constraint(CONSTRAINT, "accepted_results", schema=SCHEMA, type_="check")
    op.execute(
        f"ALTER TABLE {SCHEMA}.accepted_results ADD CONSTRAINT {CONSTRAINT} "
        f"CHECK (run_status IN ({_STATUSES})) NOT VALID"
    )
