"""PostgreSQL proof for run-scoped activity dispatch and durable transitions."""

from __future__ import annotations

import asyncio
from uuid import uuid4

from sqlalchemy import text
from test_agent_loop import T22Database
from test_agent_loop import t22_database as t22_database

from continuum_api.runtime.activity_runner import (
    ActivityRunner,
    AdmittedRunNotFound,
    RunnerStopReason,
)
from continuum_api.runtime.leases import ExecutionLease, QueueService
from continuum_api.runtime.persistence import Principal, RunStatus, RuntimeRepository
from continuum_api.runtime.transitions import TransitionService


def test_runner_claims_admitted_run_and_handler_persists_result(
    t22_database: T22Database,
) -> None:
    database = t22_database
    principal = Principal(uuid4())
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:owner_id, :github_user_id)"
            ),
            {
                "owner_id": principal.account_id,
                "github_user_id": principal.account_id.int % 900_000_000 + 1,
            },
        )
    with database.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        admitted_run = repository.create_run()
        admitted_step = repository.create_step(admitted_run, "activity:admitted", "known_activity")
        admitted_job = repository.create_job(admitted_run, admitted_step)
    hidden_principal = Principal(uuid4())
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:owner_id, :github_user_id)"
            ),
            {
                "owner_id": hidden_principal.account_id,
                "github_user_id": hidden_principal.account_id.int % 900_000_000 + 1,
            },
        )
    with database.runtime_engine.begin() as connection:
        hidden_repository = RuntimeRepository(connection, hidden_principal)
        hidden_run = hidden_repository.create_run()
        other_step = hidden_repository.create_step(hidden_run, "activity:other", "known_activity")
        other_job = hidden_repository.create_job(hidden_run, other_step)

    async def handle(lease: ExecutionLease) -> None:
        TransitionService(database.runtime_engine, principal).commit_result(
            lease=lease,
            result={"handled_run_id": str(lease.run_id)},
            target_run_status=RunStatus.NEEDS_ATTENTION,
        )

    runner = ActivityRunner(
        database.runtime_engine,
        principal,
        {"known_activity": handle},
        lease_owner="t34-integration-worker",
    )
    for unavailable_run in (uuid4(), hidden_run):
        try:
            asyncio.run(runner.run_admitted(unavailable_run, max_jobs=1, max_seconds=5))
        except AdmittedRunNotFound as error:
            assert error.code == "runtime_run_not_found"
        else:
            raise AssertionError("runner accepted a missing or cross-principal run")

    assert (
        QueueService(database.runtime_engine, principal).claim_one(
            "t34-wrong-run", run_id=hidden_run
        )
        is None
    )
    assert (
        QueueService(database.runtime_engine, principal).claim_one(
            "t34-missing-run", run_id=uuid4()
        )
        is None
    )
    receipt = asyncio.run(runner.run_admitted(admitted_run, max_jobs=1, max_seconds=5))

    assert receipt.run_id == admitted_run
    assert receipt.jobs_started == 1
    assert receipt.stop_reason is RunnerStopReason.JOB_LIMIT
    with database.admin_engine.connect() as connection:
        states = connection.execute(
            text(
                "SELECT job_id, status FROM runtime_private.jobs "
                "WHERE job_id IN (:admitted_job, :other_job)"
            ),
            {"admitted_job": admitted_job, "other_job": other_job},
        ).all()
        accepted = connection.execute(
            text(
                "SELECT payload, run_status FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner_id AND run_id=:run_id AND job_id=:job_id"
            ),
            {
                "owner_id": principal.account_id,
                "run_id": admitted_run,
                "job_id": admitted_job,
            },
        ).one()
    assert {row.job_id: row.status for row in states} == {
        admitted_job: "succeeded",
        other_job: "queued",
    }
    assert accepted.payload == {"handled_run_id": str(admitted_run)}
    assert accepted.run_status == RunStatus.NEEDS_ATTENTION.value

    # The optional run filter leaves normal, unfiltered queue claiming intact.
    default_lease = QueueService(database.runtime_engine, hidden_principal).claim_one(
        "t34-default-claim-worker"
    )
    assert default_lease is not None
    assert default_lease.run_id == hidden_run
    TransitionService(database.runtime_engine, hidden_principal).commit_result(
        lease=default_lease,
        result={"handled_run_id": str(hidden_run)},
        target_run_status=RunStatus.NEEDS_ATTENTION,
    )
