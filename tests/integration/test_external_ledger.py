"""PostgreSQL proof for fenced external-effect dispatch and reconciliation."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from test_agent_loop import T22Database
from test_agent_loop import t22_database as t22_database
from test_approvals import _close_scenario, _decide, create_frozen_approval

import continuum_api.runtime.external_ledger as external_ledger_module
from continuum_api.runtime.cancellation import CancellationService
from continuum_api.runtime.external_ledger import (
    ExternalOperationConflict,
    ExternalOperationNotDispatchable,
    ExternalOperationNotFound,
    ExternalOperationService,
)
from continuum_api.runtime.leases import ExecutionLease, LeaseLost, QueueService
from continuum_api.runtime.leases import _retry_database_only as retry_database_only
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RuntimeRepository


def test_external_operation_dispatch_and_reconciliation_are_durable(
    t22_database: T22Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = t22_database
    owner_id, run_id, lease = _seed_claimed_run(store, "t30-ledger-owner")
    principal = Principal(owner_id)
    ledger = ExternalOperationService(store.runtime_engine, principal)

    request = {"method": "create_pull_request", "head": "continuum/run-1", "base": "main"}
    prepared = ledger.prepare(lease, "publication:pull-request", request)
    replayed_prepare = ledger.prepare(lease, "publication:pull-request", request)
    assert replayed_prepare.operation_id == prepared.operation_id
    assert replayed_prepare.adopted
    with pytest.raises(ExternalOperationConflict):
        ledger.prepare(
            lease,
            "publication:pull-request",
            {"method": "create_pull_request", "head": "continuum/other", "base": "main"},
        )
    with pytest.raises(ValueError, match="metadata_too_large"):
        ledger.prepare(lease, "publication:oversized", {"payload": "x" * 5000})

    # Concurrent retries of the database authorization boundary have one winner.
    # A fake remote receives a request only for that winning receipt.
    with ThreadPoolExecutor(max_workers=4) as pool:
        dispatches = list(
            pool.map(
                lambda _: ledger.authorize_dispatch(lease, "publication:pull-request"), range(4)
            )
        )
    assert sum(receipt.dispatch_allowed for receipt in dispatches) == 1
    dispatch_receipt = next(receipt for receipt in dispatches if receipt.dispatch_allowed)
    remote_effects = sum(receipt.dispatch_allowed for receipt in dispatches)
    assert remote_effects == 1

    # Inject a serialization error after the authorization transaction commits.
    # The retry sees DISPATCHING and cannot authorize another remote request.
    retry_request = {"method": "create_release", "tag": "continuum-1"}
    ledger.prepare(lease, "publication:release", retry_request)
    injected = False

    def retry_after_committed_authorization(operation):
        def commit_then_inject_once():
            nonlocal injected
            receipt = operation()
            if not injected:
                injected = True
                error = Exception("injected serialization failure after commit")
                error.pgcode = "40001"
                raise OperationalError("injected", {}, error)
            return receipt

        return retry_database_only(commit_then_inject_once)

    monkeypatch.setattr(
        external_ledger_module,
        "_retry_database_only",
        retry_after_committed_authorization,
    )
    retried_dispatch = ledger.authorize_dispatch(lease, "publication:release")
    retry_remote_effects = int(retried_dispatch.dispatch_allowed)
    assert injected
    assert retry_remote_effects == 0
    monkeypatch.undo()

    confirmed = ledger.observe(
        run_id=run_id,
        operation_key="publication:pull-request",
        observation_key="lookup:pull-request-42",
        outcome="confirmed",
        details={"remote_id": "pull-request-42", "head": "continuum/run-1", "base": "main"},
        dispatch_receipt=dispatch_receipt,
    )
    assert confirmed.status == "confirmed"
    assert confirmed.dispatch_allowed is False
    adopted_observation = ledger.observe(
        run_id=run_id,
        operation_key="publication:pull-request",
        observation_key="lookup:pull-request-42",
        outcome="confirmed",
        details={"remote_id": "pull-request-42", "head": "continuum/run-1", "base": "main"},
        dispatch_receipt=dispatch_receipt,
    )
    assert adopted_observation.adopted
    assert ledger.authorize_dispatch(lease, "publication:pull-request").dispatch_allowed is False
    with pytest.raises(DBAPIError):
        with store.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            connection.execute(
                text(
                    "UPDATE runtime_private.external_operations SET request_metadata='{}' "
                    "WHERE owner_id=:owner AND run_id=:run AND operation_key=:key"
                ),
                {
                    "owner": owner_id,
                    "run": run_id,
                    "key": "publication:pull-request",
                },
            )
    with pytest.raises(DBAPIError):
        with store.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            connection.execute(
                text(
                    "UPDATE runtime_private.external_operations "
                    "SET status='prepared', dispatch_authorized_at=NULL "
                    "WHERE owner_id=:owner AND run_id=:run AND operation_key=:key"
                ),
                {
                    "owner": owner_id,
                    "run": run_id,
                    "key": "publication:pull-request",
                },
            )
    with pytest.raises(DBAPIError):
        with store.runtime_engine.begin() as connection:
            RuntimeRepository(connection, principal)
            connection.execute(
                text(
                    "UPDATE runtime_private.external_operation_observations SET details='{}' "
                    "WHERE owner_id=:owner AND run_id=:run AND observation_key=:key"
                ),
                {
                    "owner": owner_id,
                    "run": run_id,
                    "key": "lookup:pull-request-42",
                },
            )

    # Reusing an observation identity for different evidence is rejected. A later
    # contradictory terminal observation remains visible as a conflict.
    with pytest.raises(ExternalOperationConflict):
        ledger.observe(
            run_id=run_id,
            operation_key="publication:pull-request",
            observation_key="lookup:pull-request-42",
            outcome="confirmed",
            details={"remote_id": "some-other-result"},
            dispatch_receipt=dispatch_receipt,
        )
    conflicted = ledger.observe(
        run_id=run_id,
        operation_key="publication:pull-request",
        observation_key="provider:error-after-read",
        outcome="definitely_failed",
        details={"reason": "remote reported a terminal rejection"},
        dispatch_receipt=dispatch_receipt,
    )
    assert conflicted.status == "conflict"

    # A lost response stays unknown after a negative read. Reconciliation cannot
    # authorize a second create while the first request could still appear.
    unknown_request = {"method": "create_status_comment", "marker": "continuum-run-1"}
    unknown = ledger.prepare(lease, "publication:status-comment", unknown_request)
    comment_dispatch = ledger.authorize_dispatch(lease, "publication:status-comment")
    assert comment_dispatch.dispatch_allowed
    unknown_observation = ledger.observe(
        run_id=run_id,
        operation_key="publication:status-comment",
        observation_key="lookup:not-visible-yet",
        outcome="unknown",
        details={"lookup": "not_found", "note": "provider response was lost"},
        dispatch_receipt=comment_dispatch,
    )
    assert unknown_observation.status == "unknown"
    assert not ledger.authorize_dispatch(lease, "publication:status-comment").dispatch_allowed
    adopted_remote = ledger.observe(
        run_id=run_id,
        operation_key="publication:status-comment",
        observation_key="lookup:comment-77",
        outcome="confirmed",
        details={"remote_id": "comment-77", "marker": "continuum-run-1"},
        dispatch_receipt=comment_dispatch,
    )
    assert adopted_remote.status == "confirmed"
    assert adopted_remote.operation_id == unknown.operation_id

    # Cancellation prevents new authorization but preserves read-only late
    # observations for a request whose intent was committed before cancellation.
    cancel_request = {"method": "create_branch", "name": "continuum/run-1", "sha": "a" * 40}
    ledger.prepare(lease, "publication:branch", cancel_request)
    branch_dispatch = ledger.authorize_dispatch(lease, "publication:branch")
    assert branch_dispatch.dispatch_allowed
    ledger.prepare(
        lease,
        "publication:queued-before-cancel",
        {"method": "create_status_comment", "marker": "pending"},
    )
    with pytest.raises(ExternalOperationConflict):
        ledger.observe(
            run_id=run_id,
            operation_key="publication:branch",
            observation_key="wrong-dispatch-receipt",
            outcome="confirmed",
            details={"remote_id": "unrelated"},
            dispatch_receipt=prepared,
        )
    CancellationService(store.runtime_engine, principal).cancel(run_id)
    with pytest.raises(LeaseLost):
        ledger.prepare(
            lease, "publication:after-cancel", {"method": "create_branch", "sha": "b" * 40}
        )
    with pytest.raises(LeaseLost):
        ledger.authorize_dispatch(lease, "publication:queued-before-cancel")
    late = ledger.observe(
        run_id=run_id,
        operation_key="publication:branch",
        observation_key="late:branch-created",
        outcome="confirmed",
        details={"remote_id": "branch:continuum/run-1", "sha": "a" * 40},
        dispatch_receipt=branch_dispatch,
    )
    assert late.status == "confirmed"
    with store.admin_engine.connect() as connection:
        status = connection.execute(
            text("SELECT status FROM runtime_private.runs WHERE owner_id=:owner AND run_id=:run"),
            {"owner": owner_id, "run": run_id},
        ).scalar_one()
    assert status == "cancelled"

    # Ownership is part of the operation lookup. A different account cannot
    # read or mutate the first owner's ledger rows.
    other_owner, other_run, other_lease = _seed_claimed_run(store, "t30-ledger-other")
    with pytest.raises(ExternalOperationNotFound):
        ExternalOperationService(store.runtime_engine, Principal(other_owner)).observe(
            run_id=run_id,
            operation_key="publication:branch",
            observation_key="other-owner-probe",
            outcome="confirmed",
            details={"remote_id": "private"},
            dispatch_receipt=branch_dispatch,
        )
    other_ledger = ExternalOperationService(store.runtime_engine, Principal(other_owner))
    other_ledger.prepare(other_lease, "publication:branch", cancel_request)

    # An expired generation cannot gain dispatch authority, even while its run
    # and operation row still exist.
    stale_owner, stale_run, stale_lease = _seed_claimed_run(store, "t30-ledger-stale")
    stale_ledger = ExternalOperationService(store.runtime_engine, Principal(stale_owner))
    stale_ledger.prepare(stale_lease, "publication:stale", {"method": "create", "id": "x"})
    _expire_lease(store, stale_owner, stale_run, stale_lease)
    with pytest.raises(LeaseLost):
        stale_ledger.authorize_dispatch(stale_lease, "publication:stale")


def test_worker_loss_reconciles_adopted_dispatch_without_second_create(
    t22_database: T22Database,
) -> None:
    store = t22_database
    owner_id, run_id, first_lease = _seed_claimed_run(store, "t30-ledger-worker-one")
    principal = Principal(owner_id)
    ledger = ExternalOperationService(store.runtime_engine, principal)
    operation_key = "publication:lost-response"
    request = {"operation": "create_pull_request", "head": "continuum/run-recover", "base": "main"}
    remote = _FakeRemote({"remote_id": "pull-request-99", **request}, visibility_delay=2)

    prepared = ledger.prepare(first_lease, operation_key, request)
    first_dispatch = ledger.authorize_dispatch(first_lease, operation_key)
    assert first_dispatch.dispatch_allowed
    assert prepared.operation_id == first_dispatch.operation_id
    with pytest.raises(TimeoutError, match="response_lost"):
        remote.create(first_dispatch)
    first_negative_read = remote.read()
    assert first_negative_read is None
    ledger.observe(
        run_id=run_id,
        operation_key=operation_key,
        dispatch_receipt=first_dispatch,
        observation_key="lookup:worker-one-not-visible",
        outcome="unknown",
        details={"lookup": "not_found", "worker": "lost_response"},
    )

    _expire_lease(store, owner_id, run_id, first_lease)
    second_lease = QueueService(store.runtime_engine, principal).claim_one("t30-ledger-worker-two")
    assert second_lease is not None and second_lease.job_id == first_lease.job_id
    assert second_lease.generation > first_lease.generation
    adopted = ledger.prepare(second_lease, operation_key, request)
    assert adopted.operation_id == first_dispatch.operation_id
    assert adopted.status == "unknown"
    assert adopted.dispatch_allowed is False

    remote_result = remote.read()
    assert remote_result == {"remote_id": "pull-request-99", **request}
    reconciled = ledger.observe(
        run_id=run_id,
        operation_key=operation_key,
        dispatch_receipt=adopted,
        observation_key="lookup:pull-request-99",
        outcome="confirmed",
        details=remote_result,
        actor_id="reconciler-worker-two",
    )
    assert reconciled.status == "confirmed"
    assert remote.create_calls == 1
    assert remote.read_calls == 2

    # A PREPARED intent has no dispatch to reconcile, so a new lease can safely
    # adopt it and make the single authorization decision.
    rebind_owner, rebind_run, old_lease = _seed_claimed_run(store, "t30-ledger-prepared-old")
    rebind_principal = Principal(rebind_owner)
    rebind_ledger = ExternalOperationService(store.runtime_engine, rebind_principal)
    rebind_request = {
        "operation": "create_pull_request",
        "head": "continuum/prepared",
        "base": "main",
    }
    original = rebind_ledger.prepare(old_lease, "publication:prepared-rebind", rebind_request)
    _expire_lease(store, rebind_owner, rebind_run, old_lease)
    new_lease = QueueService(store.runtime_engine, rebind_principal).claim_one(
        "t30-ledger-prepared-new"
    )
    assert new_lease is not None and new_lease.job_id == old_lease.job_id
    rebound = rebind_ledger.prepare(new_lease, "publication:prepared-rebind", rebind_request)
    assert rebound.operation_id == original.operation_id
    assert rebound.generation == new_lease.generation
    assert rebound.status == "prepared"
    assert rebind_ledger.authorize_dispatch(
        new_lease, "publication:prepared-rebind"
    ).dispatch_allowed


def test_approval_binding_rejects_expiry_and_revision_changes(
    t22_database: T22Database,
    tmp_path,
) -> None:
    scenario = create_frozen_approval(t22_database, tmp_path)
    try:
        approved = _decide(scenario)
        publication_lease = scenario.queue.claim_one("t30-approved-publication")
        assert publication_lease is not None
        ledger = ExternalOperationService(t22_database.runtime_engine, scenario.principal)
        request = {"operation": "create_pull_request", "title": "Reviewed change"}
        ledger.prepare(
            publication_lease,
            "publication:approved-pr",
            request,
            approval_id=approved.approval_id,
            approval_sha256=approved.manifest_sha256,
        )
        _install_approval_dispatch_delay(t22_database)
        try:
            with t22_database.admin_engine.begin() as connection:
                connection.execute(
                    text(
                        "UPDATE runtime_private.approvals "
                        "SET expires_at=clock_timestamp()+interval '1 second' "
                        "WHERE owner_id=:owner AND approval_id=:approval"
                    ),
                    {"owner": scenario.run.owner_id, "approval": approved.approval_id},
                )
            with pytest.raises(ExternalOperationNotDispatchable):
                ledger.authorize_dispatch(publication_lease, "publication:approved-pr")
        finally:
            _remove_approval_dispatch_delay(t22_database)
        with t22_database.admin_engine.connect() as connection:
            status = connection.execute(
                text(
                    "SELECT status FROM runtime_private.external_operations "
                    "WHERE owner_id=:owner AND run_id=:run "
                    "AND operation_key='publication:approved-pr'"
                ),
                {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
            ).scalar_one()
        assert status == "prepared"
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.approvals "
                    "SET created_at=clock_timestamp()-interval '2 seconds', "
                    "expires_at=clock_timestamp()-interval '1 second' "
                    "WHERE owner_id=:owner AND approval_id=:approval"
                ),
                {"owner": scenario.run.owner_id, "approval": approved.approval_id},
            )
        with pytest.raises(ExternalOperationNotDispatchable):
            ledger.authorize_dispatch(publication_lease, "publication:approved-pr")

        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.approvals "
                    "SET expires_at=clock_timestamp()+interval '1 hour' "
                    "WHERE owner_id=:owner AND approval_id=:approval"
                ),
                {"owner": scenario.run.owner_id, "approval": approved.approval_id},
            )
            connection.execute(
                text(
                    "UPDATE runtime_private.runs SET revision=revision+1 "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
            )
        with pytest.raises(ExternalOperationNotDispatchable):
            ledger.authorize_dispatch(publication_lease, "publication:approved-pr")
    finally:
        _close_scenario(scenario)


class _FakeRemote:
    def __init__(self, result: dict[str, str], *, visibility_delay: int) -> None:
        self._result = result
        self._visibility_delay = visibility_delay
        self.create_calls = 0
        self.read_calls = 0

    def create(self, receipt) -> None:
        assert receipt.dispatch_allowed
        self.create_calls += 1
        raise TimeoutError("response_lost_after_remote_create")

    def read(self) -> dict[str, str] | None:
        self.read_calls += 1
        return self._result if self.read_calls >= self._visibility_delay else None


def _install_approval_dispatch_delay(store: T22Database) -> None:
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "CREATE FUNCTION runtime_private.t30_delay_dispatch_event() RETURNS trigger "
                "LANGUAGE plpgsql AS $$ BEGIN "
                "IF NEW.event_type='external_operation.dispatching' THEN PERFORM pg_sleep(1.5); "
                "END IF; RETURN NEW; END $$"
            )
        )
        connection.execute(
            text(
                "CREATE TRIGGER t30_delay_dispatch_event "
                "BEFORE INSERT ON runtime_private.events FOR EACH ROW "
                "EXECUTE FUNCTION runtime_private.t30_delay_dispatch_event()"
            )
        )


def _remove_approval_dispatch_delay(store: T22Database) -> None:
    with store.admin_engine.begin() as connection:
        connection.execute(
            text("DROP TRIGGER IF EXISTS t30_delay_dispatch_event ON runtime_private.events")
        )
        connection.execute(
            text("DROP FUNCTION IF EXISTS runtime_private.t30_delay_dispatch_event()")
        )


def _expire_lease(store: T22Database, owner_id: UUID, run_id: UUID, lease: ExecutionLease) -> None:
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.jobs "
                "SET lease_expires_at=clock_timestamp()-interval '1 second' "
                "WHERE owner_id=:owner AND run_id=:run AND job_id=:job"
            ),
            {
                "owner": owner_id,
                "run": run_id,
                "job": lease.job_id,
            },
        )


def _seed_claimed_run(store: T22Database, lease_owner: str) -> tuple[UUID, UUID, ExecutionLease]:
    owner_id = uuid4()
    principal = Principal(owner_id)
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id, github_user_id) "
                "VALUES (:owner, :github_user_id)"
            ),
            {"owner": owner_id, "github_user_id": 700_000_000 + owner_id.int % 100_000_000},
        )
    with store.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run(github_repository_id=714_030, issue_number=1)
        step_id = repository.create_step(run_id, f"external:{lease_owner}", "test")
        repository.create_job(run_id, step_id, replay_policy=ReplayPolicy.REPLAY_SAFE)
    lease = QueueService(store.runtime_engine, principal).claim_one(lease_owner)
    assert lease is not None and lease.run_id == run_id
    return owner_id, run_id, lease
