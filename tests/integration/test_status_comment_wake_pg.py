"""PostgreSQL proof for the bounded, fenced run status-comment queue."""

from __future__ import annotations

import os
import secrets
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from test_agent_loop import T22Database, _run_migration, t22_database  # noqa: F401

from continuum_api.runtime.wake_worker_store import (
    DrainLease,
    PostgresWakeWorkerStore,
)


def test_status_comment_queue_is_fenced_bounded_and_reconcilable(
    t22_database: T22Database,  # noqa: F811
) -> None:
    database = t22_database
    owner, run, wake, token = uuid4(), uuid4(), uuid4(), uuid4()
    generation = 4001
    password = secrets.token_urlsafe(32)
    with database.admin_engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER ROLE continuum_wake_worker PASSWORD '{password}'")
        connection.execute(
            text(
                "INSERT INTO runtime_private.users(user_id,github_user_id) "
                "VALUES (:owner,991234567)"
            ),
            {"owner": owner},
        )
        connection.execute(
            text("""
              INSERT INTO runtime_private.runs(
                run_id,owner_id,github_repository_id,issue_number,status)
              VALUES (:run,:owner,991234567,41,'running')
            """),
            {"run": run, "owner": owner},
        )
        connection.execute(
            text("""
          INSERT INTO runtime_private.repository_registrations
            (owner_id,github_repository_id,repository_owner,repository_name,profile_id)
          VALUES (:owner,991234567,'fixture','status-queue','python-pytest')
        """),
            {"owner": owner},
        )
        connection.execute(
            text("""
          INSERT INTO runtime_private.events(
            event_id,owner_id,run_id,sequence,event_type,actor_type)
          VALUES (:event,:owner,:run,1,'run.started','system')
        """),
            {"event": uuid4(), "owner": owner, "run": run},
        )
        connection.execute(
            text("""
          UPDATE wake_private.worker_lease SET wake_id=:wake,github_app_id=88001,token=:token,
            generation=:generation,expires_at=clock_timestamp()+interval '120 seconds'
          WHERE singleton
        """),
            {"wake": wake, "token": token, "generation": generation},
        )

    worker_engine = create_engine(
        database.runtime_url.set(username="continuum_wake_worker", password=password),
        hide_parameters=True,
        pool_size=1,
        max_overflow=0,
    )
    store = PostgresWakeWorkerStore(worker_engine, github_app_id=88001)
    lease = DrainLease(wake, token, generation)
    try:
        with database.admin_engine.connect() as connection:
            for role in ("continuum_wake_worker", "continuum_runtime", "continuum_wake_receiver"):
                assert (
                    connection.execute(
                        text(
                            "SELECT has_function_privilege(:role, "
                            "'wake_private.bump_status_comment_version()', 'EXECUTE')"
                        ),
                        {"role": role},
                    ).scalar_one()
                    is False
                )
        with pytest.raises(DBAPIError):
            with worker_engine.connect() as connection:
                connection.execute(text("SELECT * FROM wake_private.status_comment_attempt")).all()
        with pytest.raises(DBAPIError):
            with database.runtime_engine.begin() as connection:
                connection.execute(
                    text("UPDATE wake_private.status_comment_attempt SET completed=true")
                )
        work = store.next_status(lease)
        assert work is not None
        assert work.owner_id == owner and work.run_id == run
        assert work.event_sequence == 1 and len(work.snapshot_key) == 32
        assert work.reconcile_only is False
        stale_lease = DrainLease(wake, uuid4(), generation)
        assert store.next_status(stale_lease) is None
        assert store.complete_status(stale_lease, work) is False
        assert store.next_status(lease) is None  # 15 minute retry cooldown
        assert store.complete_status(lease, work) is False

        # A new external-operation event does not become the public status sequence.
        with database.admin_engine.begin() as connection:
            connection.execute(
                text("""
              INSERT INTO runtime_private.events(
                event_id,owner_id,run_id,sequence,event_type,actor_type)
              VALUES (:event,:owner,:run,2,'external_operation.prepared','worker')
            """),
                {"event": uuid4(), "owner": owner, "run": run},
            )
            connection.execute(
                text("""
              INSERT INTO runtime_private.events(
                event_id,owner_id,run_id,sequence,event_type,actor_type)
              VALUES (:event,:owner,:run,3,'run.progress','worker')
            """),
                {"event": uuid4(), "owner": owner, "run": run},
            )
            connection.execute(
                text("""
              UPDATE wake_private.status_comment_attempt SET next_attempt_at=clock_timestamp()
              WHERE owner_id=:owner AND run_id=:run AND snapshot_key=:key
            """),
                {"owner": owner, "run": run, "key": work.snapshot_key},
            )
        # A due retry adopts the latest public sequence after unrelated domain events.
        retry = store.next_status(lease)
        assert retry is not None and retry.event_sequence == 3
        assert retry.snapshot_key == work.snapshot_key and retry.reconcile_only is False

        with database.admin_engine.begin() as connection:
            connection.execute(
                text("""
              INSERT INTO runtime_private.github_status_comment_effects
                (owner_id,run_id,event_sequence,repository_id,issue_number,status_snapshot,
                 revision,marker,body,request_sha256,effect_status,dispatch_token,dispatched_at,
                 remote_comment_id)
              VALUES (:owner,:run,3,991234567,41,'running',0,'continuum-status-fixture',
                'status fixture',decode(repeat('00',32),'hex'),'confirmed',:dispatch,
                clock_timestamp(),'88441')
            """),
                {"owner": owner, "run": run, "dispatch": uuid4()},
            )
        assert store.complete_status(lease, work) is False
        assert store.complete_status(lease, retry) is True
        assert store.complete_status(lease, retry) is False
        with database.admin_engine.connect() as connection:
            attempts = connection.execute(
                text("""
              SELECT attempts,completed,event_sequence FROM wake_private.status_comment_attempt
              WHERE owner_id=:owner AND run_id=:run AND snapshot_key=:key
            """),
                {"owner": owner, "run": run, "key": work.snapshot_key},
            ).one()
            assert attempts == (2, True, 3)

        # A changed status snapshot is independently eligible, and unresolved effects
        # from older snapshots are returned only as reconciliation work.
        with database.admin_engine.begin() as connection:
            connection.execute(
                text("""
                  INSERT INTO runtime_private.events(
                    event_id,owner_id,run_id,sequence,event_type,actor_type)
                  VALUES (:event,:owner,:run,4,'run.status_changed','system')
                """),
                {"event": uuid4(), "owner": owner, "run": run},
            )
            connection.execute(
                text("""
              UPDATE runtime_private.runs SET status='waiting_credentials',revision=1
              WHERE owner_id=:owner AND run_id=:run
            """),
                {"owner": owner, "run": run},
            )
        next_work = store.next_status(lease)
        assert next_work is not None and next_work.snapshot_key != work.snapshot_key
        assert next_work.reconcile_only is False
        with database.admin_engine.begin() as connection:
            connection.execute(
                text("""
                  INSERT INTO runtime_private.github_status_comment_effects
                    (owner_id,run_id,event_sequence,repository_id,issue_number,status_snapshot,
                     revision,marker,body,request_sha256,effect_status,dispatch_token,dispatched_at)
                  VALUES (:owner,:run,:seq,991234567,41,'waiting_credentials',1,
                    'continuum-status-unresolved','status fixture',decode(repeat('01',32),'hex'),
                    'dispatching',:dispatch,clock_timestamp())
                """),
                {"owner": owner, "run": run, "seq": next_work.event_sequence, "dispatch": uuid4()},
            )
            connection.execute(
                text("""
                  UPDATE runtime_private.runs SET revision=2
                  WHERE owner_id=:owner AND run_id=:run
                """),
                {"owner": owner, "run": run},
            )
            connection.execute(
                text("""
                  UPDATE wake_private.status_comment_attempt SET next_attempt_at=clock_timestamp()
                  WHERE owner_id=:owner AND run_id=:run AND snapshot_key=:key
                """),
                {"owner": owner, "run": run, "key": next_work.snapshot_key},
            )
        reconcile = store.next_status(lease)
        assert reconcile is not None and reconcile.reconcile_only is True
        assert reconcile.event_sequence == next_work.event_sequence
        with database.admin_engine.begin() as connection:
            connection.execute(
                text("""
                  UPDATE runtime_private.github_status_comment_effects SET
                    effect_status='confirmed',remote_comment_id='88442'
                  WHERE owner_id=:owner AND run_id=:run AND event_sequence=:seq
                """),
                {"owner": owner, "run": run, "seq": reconcile.event_sequence},
            )
        assert store.complete_status(lease, reconcile) is True

        capped = store.next_status(lease)
        assert capped is not None and capped.reconcile_only is False
        for expected_attempts in (2, 3):
            with database.admin_engine.begin() as connection:
                connection.execute(
                    text("""
                      UPDATE wake_private.status_comment_attempt SET
                        next_attempt_at=clock_timestamp()
                      WHERE owner_id=:owner AND run_id=:run AND snapshot_key=:key
                    """),
                    {"owner": owner, "run": run, "key": capped.snapshot_key},
                )
            assert store.next_status(lease) is not None
            with database.admin_engine.connect() as connection:
                actual = connection.execute(
                    text("""
                      SELECT attempts FROM wake_private.status_comment_attempt
                      WHERE owner_id=:owner AND run_id=:run AND snapshot_key=:key
                    """),
                    {"owner": owner, "run": run, "key": capped.snapshot_key},
                ).scalar_one()
                assert actual == expected_attempts
        with database.admin_engine.begin() as connection:
            connection.execute(
                text("""
                  UPDATE wake_private.status_comment_attempt SET next_attempt_at=clock_timestamp()
                  WHERE owner_id=:owner AND run_id=:run AND snapshot_key=:key
                """),
                {"owner": owner, "run": run, "key": capped.snapshot_key},
            )
        assert store.next_status(lease) is None

        with database.admin_engine.begin() as connection:
            connection.execute(
                text("""
                  UPDATE runtime_private.runs SET status='failed'
                  WHERE owner_id=:owner AND run_id=:run
                """),
                {"owner": owner, "run": run},
            )
        terminal_work = store.next_status(lease)
        assert terminal_work is not None and terminal_work.run_id == run
        with database.admin_engine.begin() as connection:
            connection.execute(
                text("""
                  UPDATE runtime_private.runs SET status='running'
                  WHERE owner_id=:owner AND run_id=:run
                """),
                {"owner": owner, "run": run},
            )
        initial_roundtrip = store.next_status(lease)
        assert initial_roundtrip is not None and initial_roundtrip.run_id == run
        with database.admin_engine.begin() as connection:
            connection.execute(
                text("""
                  UPDATE runtime_private.runs SET status='waiting_capacity'
                  WHERE owner_id=:owner AND run_id=:run
                """),
                {"owner": owner, "run": run},
            )
            connection.execute(
                text("""
                  UPDATE runtime_private.runs SET status='running'
                  WHERE owner_id=:owner AND run_id=:run
                """),
                {"owner": owner, "run": run},
            )
        resumed_roundtrip = store.next_status(lease)
        assert resumed_roundtrip is not None
        assert resumed_roundtrip.run_id == run
        assert resumed_roundtrip.snapshot_key != initial_roundtrip.snapshot_key
        with database.admin_engine.begin() as connection:
            connection.execute(
                text("""
                  UPDATE runtime_private.runs SET status='running'
                  WHERE owner_id=:owner AND run_id=:run
                """),
                {"owner": owner, "run": run},
            )
            assert (
                connection.execute(
                    text("""
                  SELECT version FROM wake_private.status_comment_version
                  WHERE owner_id=:owner AND run_id=:run
                """),
                    {"owner": owner, "run": run},
                ).scalar_one()
                == 6
            )
        assert store.next_status(lease) is None
        assert store.finish(lease, complete=False) is False
    finally:
        store.close()


def test_status_comment_migration_downgrade_restores_bounded_worker_predicate(
    t22_database: T22Database,  # noqa: F811
) -> None:
    database = t22_database
    database_url = os.environ["CONTINUUM_T22_TEST_DATABASE_URL"]
    _run_migration("downgrade", "20261022_31", database_url)
    with database.admin_engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT to_regclass('wake_private.status_comment_attempt')")
            ).scalar_one()
            is None
        )
        assert (
            connection.execute(
                text("SELECT to_regclass('wake_private.status_comment_version')")
            ).scalar_one()
            is None
        )
        definition = connection.execute(
            text("SELECT pg_get_functiondef('wake_private.has_actionable_work()'::regprocedure)")
        ).scalar_one()
        assert "status_comment_candidates" not in definition
        assert connection.execute(
            text("""
              SELECT has_column_privilege(:role,'runtime_private.runs','status','SELECT')
                AND NOT has_column_privilege(:role,'runtime_private.runs','revision','SELECT')
                AND NOT has_column_privilege(:role,'runtime_private.events','sequence','SELECT')
                AND NOT has_column_privilege(
                  :role,'runtime_private.github_status_comment_effects','effect_status','SELECT')
            """),
            {"role": "continuum_wake_worker_owner"},
        ).scalar_one()
    _run_migration("upgrade", "head", database_url)
