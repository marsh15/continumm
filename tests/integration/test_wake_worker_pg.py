"""PostgreSQL proof for the bounded wake-worker store and its role boundary."""

from __future__ import annotations

import hashlib
import json
import secrets
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import DBAPIError
from test_agent_loop import T22Database, t22_database  # noqa: F401

from continuum_api.runtime.wake_worker_store import PostgresWakeWorkerStore

APP_ID = 7_777_777_777
WORKER_ROLE = "continuum_wake_worker"
OWNER_ROLE = "continuum_wake_worker_owner"


def test_bounded_worker_store_is_fenced_and_only_uses_definer_functions(
    t22_database: T22Database,  # noqa: F811
) -> None:
    database = t22_database
    password = secrets.token_urlsafe(32)
    receiver_password = secrets.token_urlsafe(32)
    with database.admin_engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER ROLE {WORKER_ROLE} PASSWORD '{password}'")
        connection.exec_driver_sql(
            f"ALTER ROLE continuum_wake_receiver PASSWORD '{receiver_password}'"
        )
        grants = connection.execute(
            text(
                "SELECT has_table_privilege(:role, 'wake_private.worker_lease', 'SELECT') "
                "OR has_table_privilege(:role, 'wake_private.worker_lease', 'INSERT') "
                "OR has_table_privilege(:role, 'wake_private.worker_lease', 'UPDATE') "
                "OR has_table_privilege(:role, 'wake_private.worker_lease', 'DELETE') "
                "OR has_table_privilege(:role, 'webhook_private.github_delivery_inbox', 'UPDATE')"
            ),
            {"role": WORKER_ROLE},
        ).scalar_one()
        assert grants is False
        for role in ("continuum_ingress", "continuum_wake", "continuum_wake_receiver"):
            assert connection.execute(
                text(
                    "SELECT NOT has_column_privilege(:role, "
                    "'webhook_private.github_delivery_inbox', 'consumed_at', 'UPDATE') "
                    "AND NOT has_table_privilege(:role, 'wake_private.drain_watermark', 'UPDATE')"
                ),
                {"role": role},
            ).scalar_one()
        owner_flags = connection.execute(
            text(
                "SELECT rolcanlogin, rolsuper OR rolbypassrls AS elevated, rolinherit "
                "FROM pg_roles WHERE rolname=:role"
            ),
            {"role": OWNER_ROLE},
        ).one()
        assert owner_flags == (False, False, False)

    worker_engine = create_engine(
        database.runtime_url.set(username=WORKER_ROLE, password=password),
        hide_parameters=True,
        pool_size=2,
        max_overflow=0,
    )
    receiver_engine = create_engine(
        database.runtime_url.set(username="continuum_wake_receiver", password=receiver_password),
        hide_parameters=True,
        pool_size=1,
        max_overflow=0,
    )
    store = PostgresWakeWorkerStore(worker_engine, github_app_id=APP_ID)
    owner_id, wake_id, attempt_id, retry_attempt = uuid4(), uuid4(), uuid4(), uuid4()
    first_delivery, second_delivery, expired_delivery = uuid4(), uuid4(), uuid4()
    unresolved_delivery, malformed_delivery, edited_delivery = uuid4(), uuid4(), uuid4()
    first_generation, second_generation = 91, 92
    claim_generation = first_generation + 1
    payload = {
        "action": "created",
        "repository": {"id": 99001},
        "issue": {"id": 501, "number": 5},
        "comment": {"id": 7001, "user": {"id": 39001}},
    }
    expired_payload = {
        **payload,
        "comment": {"id": 7002, "user": {"id": 39001}},
    }
    unresolved_payload = {
        **payload,
        "comment": {"id": 7003, "user": {"id": 39001}},
    }
    edited_payload = {**payload, "action": "edited"}
    now = datetime.now(UTC)
    try:
        with pytest.raises(DBAPIError):
            with worker_engine.connect() as connection:
                connection.execute(text("SELECT * FROM wake_private.worker_lease")).all()
        with pytest.raises(DBAPIError):
            with worker_engine.begin() as connection:
                connection.execute(
                    text("UPDATE webhook_private.github_delivery_inbox SET consumed_at=now()")
                )
        with pytest.raises(DBAPIError):
            with worker_engine.begin() as connection:
                connection.execute(
                    text("UPDATE wake_private.drain_watermark SET last_successful_drain_at=now()")
                )

        _seed_wake(database, owner_id, wake_id, attempt_id, first_generation)
        _seed_delivery(database, first_delivery, "issue_comment", payload)
        _seed_delivery(database, edited_delivery, "issue_comment", edited_payload)
        _seed_delivery(database, expired_delivery, "issue_comment", expired_payload)
        _seed_delivery(database, unresolved_delivery, "issue_comment", unresolved_payload)
        _seed_raw_delivery(database, malformed_delivery, "push", b"{broken-json")
        _seed_command_receipt(database, owner_id, 7001, now)
        _seed_command_receipt(database, owner_id, 7002, now - timedelta(days=91))
        _seed_command_receipt(database, owner_id, 7003, now)
        assert _read_wake_state(receiver_engine) == "worker_not_started"

        with database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE wake_private.dispatch_control SET generation=:generation,"
                    "active_attempt_id=:attempt,"
                    "lease_expires_at=clock_timestamp()+interval '120 seconds' "
                    "WHERE singleton"
                ),
                {"generation": claim_generation, "attempt": retry_attempt},
            )
            duplicate = (
                connection.execute(
                    text(
                        "SELECT * FROM wake_private.accept_wake_receipt("
                        "CAST(:attempt AS uuid),CAST(:generation AS bigint),"
                        "CAST(:wake AS uuid),'pending_inbox')"
                    ),
                    {"attempt": retry_attempt, "generation": claim_generation, "wake": wake_id},
                )
                .mappings()
                .one()
            )
            assert duplicate["outcome"] == "duplicate"

        lease = store.claim(wake_id, retry_attempt, claim_generation)
        assert lease is not None
        assert _read_wake_state(receiver_engine) == "running"
        assert store.claim(wake_id, retry_attempt, claim_generation) is None
        assert store.heartbeat(lease) is True
        with database.admin_engine.begin() as connection:
            recorded = connection.execute(
                text(
                    "SELECT recorded FROM wake_private.record_dispatch_result("
                    "CAST(:attempt AS uuid),CAST(:generation AS bigint),"
                    "CAST('accepted' AS text),CAST(202 AS smallint))"
                ),
                {"attempt": retry_attempt, "generation": claim_generation},
            ).scalar_one()
            assert recorded is True
            assert (
                connection.execute(
                    text(
                        "SELECT active_attempt_id FROM wake_private.dispatch_control "
                        "WHERE singleton"
                    )
                ).scalar_one()
                == retry_attempt
            )
            connection.execute(
                text(
                    "UPDATE wake_private.dispatch_control SET generation=:generation,"
                    "active_attempt_id=:attempt WHERE singleton"
                ),
                {"generation": claim_generation + 1, "attempt": uuid4()},
            )
        assert store.heartbeat(lease) is True

        work = store.next_inbox(lease)
        assert work is not None
        assert work.delivery_id == first_delivery
        assert work.github_app_id == APP_ID
        assert work.owner_id == owner_id
        assert work.event_name == "issue_comment"
        assert work.command_state == "accepted_unresolved"
        edited_work = store.next_inbox(lease)
        assert edited_work is not None and edited_work.delivery_id == edited_delivery
        assert edited_work.owner_id == owner_id
        assert edited_work.command_state == "accepted_unresolved"
        with database.admin_engine.begin() as connection:
            connection.execute(
                text("UPDATE runtime_private.users SET status='suspended' WHERE user_id=:owner"),
                {"owner": owner_id},
            )
        expired_work = store.next_inbox(lease)
        assert expired_work is not None and expired_work.delivery_id == expired_delivery
        assert expired_work.owner_id is None
        assert expired_work.command_state == "expired"
        unresolved_work = store.next_inbox(lease)
        assert unresolved_work is not None and unresolved_work.delivery_id == unresolved_delivery
        assert unresolved_work.owner_id is None
        assert unresolved_work.command_state == "accepted_unresolved"
        malformed_work = store.next_inbox(lease)
        assert malformed_work is not None and malformed_work.delivery_id == malformed_delivery
        assert malformed_work.command_state == "none"
        assert store.complete_inbox(lease, first_delivery, "processed") is False
        assert _delivery_is_pending(database, first_delivery)
        assert store.complete_inbox(lease, first_delivery, "inert") is False
        assert store.complete_inbox(lease, first_delivery, "rejected") is False
        assert store.complete_inbox(lease, unresolved_delivery, "inert") is False
        assert store.complete_inbox(lease, unresolved_delivery, "rejected") is False
        assert store.complete_inbox(lease, unresolved_delivery, "processed") is False

        with database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO runtime_private.github_command_results "
                    "(owner_id,repository_id,comment_id,result) "
                    "VALUES (:owner,99001,7001,CAST(:result AS jsonb))"
                ),
                {"owner": owner_id, "result": json.dumps({"kind": "status"})},
            )
            connection.execute(
                text(
                    "INSERT INTO runtime_private.github_command_results "
                    "(owner_id,repository_id,comment_id,result) "
                    "VALUES (:owner,99001,7003,CAST(:result AS jsonb))"
                ),
                {"owner": owner_id, "result": json.dumps({"kind": "status"})},
            )
        assert store.complete_inbox(lease, first_delivery, "processed") is True
        assert store.complete_inbox(lease, edited_delivery, "processed") is True
        assert store.complete_inbox(lease, unresolved_delivery, "processed") is True
        assert _delivery_is_pending(database, first_delivery) is False
        assert store.complete_inbox(lease, expired_delivery, "inert") is False
        assert store.complete_inbox(lease, expired_delivery, "rejected") is True
        assert store.complete_inbox(lease, malformed_delivery, "inert") is True
        assert store.finish(lease, complete=True) is True
        assert _read_wake_state(receiver_engine) == "drained"
        first_watermark = _watermark(database)
        assert first_watermark is not None

        second_wake, second_attempt = uuid4(), uuid4()
        second_generation = claim_generation + 1
        _seed_wake(database, owner_id, second_wake, second_attempt, second_generation)
        _seed_delivery(database, second_delivery, "push", {})
        partial_lease = store.claim(second_wake, second_attempt, second_generation)
        assert partial_lease is not None
        inert_work = store.next_inbox(partial_lease)
        assert inert_work is not None and inert_work.owner_id is None
        assert store.finish(partial_lease, complete=True) is False
        assert _watermark(database) == first_watermark
        assert _read_wake_state(receiver_engine) == "partial"
        with database.admin_engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT drain_status FROM wake_private.wake_receipt WHERE wake_id=:wake"),
                    {"wake": second_wake},
                ).scalar_one()
                == "partial"
            )
        expired_lease = store.claim(second_wake, second_attempt, second_generation)
        assert expired_lease is not None
        assert store.next_inbox(expired_lease) is not None
        third_wake, third_attempt = uuid4(), uuid4()
        with database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE wake_private.worker_lease "
                    "SET expires_at=clock_timestamp()-interval '1 second' WHERE singleton"
                )
            )
        _seed_wake(database, owner_id, third_wake, third_attempt, second_generation + 1)
        assert store.heartbeat(expired_lease) is False
        assert store.complete_inbox(expired_lease, second_delivery, "inert") is False
        assert store.finish(expired_lease, complete=True) is False
        current_lease = store.claim(third_wake, third_attempt, second_generation + 1)
        assert current_lease is not None
        reclaimed_work = store.next_inbox(current_lease)
        assert reclaimed_work is not None and reclaimed_work.delivery_id == second_delivery
        assert store.complete_inbox(current_lease, second_delivery, "inert") is True

        run_id, step_id, job_id = uuid4(), uuid4(), uuid4()
        with database.admin_engine.begin() as connection:
            connection.execute(
                text("UPDATE runtime_private.users SET status='active' WHERE user_id=:owner"),
                {"owner": owner_id},
            )
            connection.execute(
                text(
                    "INSERT INTO runtime_private.runs(run_id,owner_id,github_repository_id,"
                    "issue_number,status) VALUES (:run,:owner,99001,6,'queued')"
                ),
                {"run": run_id, "owner": owner_id},
            )
            connection.execute(
                text(
                    "INSERT INTO runtime_private.steps "
                    "(step_id,owner_id,run_id,logical_key,activity) "
                    "VALUES (:step,:owner,:run,'wake-test','known_activity')"
                ),
                {"step": step_id, "owner": owner_id, "run": run_id},
            )
            connection.execute(
                text(
                    "INSERT INTO runtime_private.jobs(job_id,owner_id,run_id,step_id) "
                    "VALUES (:job,:owner,:run,:step)"
                ),
                {"job": job_id, "owner": owner_id, "run": run_id, "step": step_id},
            )
        run_work = store.next_run(current_lease)
        assert run_work is not None
        assert (run_work.owner_id, run_work.run_id) == (owner_id, run_id)
        assert store.next_run(current_lease) is None
        assert store.finish(current_lease, complete=True) is False
        assert _watermark(database) == first_watermark
        with database.admin_engine.connect() as connection:
            drain_status = connection.execute(
                text("SELECT drain_status FROM wake_private.wake_receipt WHERE wake_id=:wake"),
                {"wake": third_wake},
            ).scalar_one()
        assert drain_status == "partial"
    finally:
        store.close()
        receiver_engine.dispose()


def _seed_wake(
    database: T22Database,
    owner_id: UUID,
    wake_id: UUID,
    attempt_id: UUID,
    generation: int,
) -> None:
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users(user_id,github_user_id) VALUES (:owner,39001) "
                "ON CONFLICT (user_id) DO NOTHING"
            ),
            {"owner": owner_id},
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.repository_registrations "
                "(owner_id,github_repository_id,repository_owner,repository_name,profile_id) "
                "VALUES (:owner,99001,'owner','repo','python-pytest') "
                "ON CONFLICT (owner_id,github_repository_id) DO NOTHING"
            ),
            {"owner": owner_id},
        )
        connection.execute(
            text(
                "UPDATE wake_private.dispatch_control SET generation=:generation,"
                "active_wake_id=:wake,active_attempt_id=:attempt,lease_expires_at=NULL,"
                "last_dispatch_outcome='claimed' WHERE singleton"
            ),
            {"generation": generation, "wake": wake_id, "attempt": attempt_id},
        )
        connection.execute(
            text(
                "INSERT INTO wake_private.wake_receipt "
                "(wake_id,reason,accepted_attempt_id,accepted_generation,"
                "last_accepted_attempt_id,last_accepted_generation) "
                "VALUES (:wake,'pending_inbox',:attempt,:generation,:attempt,:generation)"
            ),
            {"wake": wake_id, "attempt": attempt_id, "generation": generation},
        )


def _seed_delivery(
    database: T22Database, delivery_id: UUID, event_type: str, payload: dict[str, object]
) -> None:
    raw_body = json.dumps(payload).encode()
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO webhook_private.github_delivery_inbox "
                "(github_app_id,delivery_id,event_type,action,payload_sha256,raw_body) "
                "VALUES (:app,:delivery,:event,:action,:digest,:body)"
            ),
            {
                "app": APP_ID,
                "delivery": delivery_id,
                "event": event_type,
                "action": payload.get("action"),
                "digest": hashlib.sha256(raw_body).digest(),
                "body": raw_body,
            },
        )


def _seed_raw_delivery(
    database: T22Database, delivery_id: UUID, event_type: str, raw_body: bytes
) -> None:
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO webhook_private.github_delivery_inbox "
                "(github_app_id,delivery_id,event_type,payload_sha256,raw_body) "
                "VALUES (:app,:delivery,:event,:digest,:body)"
            ),
            {
                "app": APP_ID,
                "delivery": delivery_id,
                "event": event_type,
                "digest": hashlib.sha256(raw_body).digest(),
                "body": raw_body,
            },
        )


def _seed_command_receipt(
    database: T22Database, owner_id: UUID, comment_id: int, created_at: datetime
) -> None:
    with database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.github_command_receipts "
                "(owner_id,repository_id,comment_id,issue_id,issue_number,author_id,"
                "comment_created_at,command_kind,command_metadata,command_sha256,expires_at) "
                "VALUES (:owner,99001,:comment,501,5,39001,:created,'status',"
                "CAST(:metadata AS jsonb),:digest,:expires)"
            ),
            {
                "owner": owner_id,
                "comment": comment_id,
                "created": created_at,
                "metadata": json.dumps({"kind": "status"}),
                "digest": b"c" * 32,
                "expires": created_at + timedelta(days=90),
            },
        )


def _delivery_is_pending(database: T22Database, delivery_id: UUID) -> bool:
    with database.admin_engine.connect() as connection:
        return bool(
            connection.execute(
                text(
                    "SELECT consumed_at IS NULL FROM webhook_private.github_delivery_inbox "
                    "WHERE github_app_id=:app AND delivery_id=:delivery"
                ),
                {"app": APP_ID, "delivery": delivery_id},
            ).scalar_one()
        )


def _watermark(database: T22Database) -> datetime | None:
    with database.admin_engine.connect() as connection:
        return connection.execute(
            text(
                "SELECT last_successful_drain_at FROM wake_private.drain_watermark WHERE singleton"
            )
        ).scalar_one()


def _read_wake_state(engine: Engine) -> str | None:
    with engine.connect() as connection:
        return connection.execute(
            text("SELECT receipt_state FROM wake_private.read_wake_status()")
        ).scalar_one()
