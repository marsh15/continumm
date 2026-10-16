"""A lock wait must not let an approval accepted after its deadline succeed."""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

from sqlalchemy import text
from test_agent_loop import T22Database
from test_agent_loop import t22_database as t22_database
from test_approvals import create_frozen_approval

from continuum_api.runtime.approvals import ApprovalError


def test_approval_expiring_while_waiting_for_run_lock_is_rejected_and_terminalized(
    t22_database: T22Database, tmp_path: Path
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path, approval_ttl=timedelta(seconds=1))
    approval = scenario.frozen_receipt
    assert approval is not None

    def approve() -> str:
        try:
            asyncio.run(
                scenario.service.decide(
                    approval_id=approval.approval_id,
                    manifest_sha256=approval.manifest_sha256.hex(),
                    decision="approve",
                )
            )
        except ApprovalError as error:
            return error.code
        return "accepted"

    with ThreadPoolExecutor(max_workers=1) as workers:
        with t22_database.admin_engine.begin() as blocker:
            blocker.execute(
                text("SELECT run_id FROM runtime_private.runs WHERE run_id=:run FOR UPDATE"),
                {"run": scenario.run.run_id},
            ).scalar_one()
            future = workers.submit(approve)
            observed_wait = False
            until = time.monotonic() + 0.7
            while time.monotonic() < until and not future.done():
                with t22_database.admin_engine.connect() as observer:
                    observed_wait = observer.execute(
                        text(
                            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                            "WHERE datname=current_database() AND usename='continuum_runtime' "
                            "AND wait_event_type='Lock' AND query LIKE '%runtime_private.runs%')"
                        )
                    ).scalar_one()
                if observed_wait:
                    break
                time.sleep(0.01)
            assert observed_wait, "approval decision never reached the blocked run lock"
            blocker.execute(
                text(
                    "SELECT pg_sleep(GREATEST(0.0, EXTRACT(EPOCH FROM "
                    "CAST(:expiry AS timestamptz) - clock_timestamp())) + 0.05)"
                ),
                {"expiry": approval.expires_at},
            )
        outcome = future.result(timeout=5)
    assert outcome in {"approval_expired", "approval_run_expired"}
    with t22_database.admin_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT status FROM runtime_private.runs WHERE run_id=:run"),
                {"run": scenario.run.run_id},
            ).scalar_one()
            == "expired"
        )
        assert (
            connection.execute(
                text("SELECT status FROM runtime_private.approvals WHERE approval_id=:approval"),
                {"approval": approval.approval_id},
            ).scalar_one()
            == "expired"
        )
        assert (
            connection.execute(
                text(
                    "SELECT count(*) FROM runtime_private.steps WHERE run_id=:run "
                    "AND activity='publish_approved'"
                ),
                {"run": scenario.run.run_id},
            ).scalar_one()
            == 0
        )
