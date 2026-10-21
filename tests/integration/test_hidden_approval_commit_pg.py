"""PostgreSQL checks for operator validation allocation policy in approvals."""

from __future__ import annotations

import asyncio
import hashlib
from typing import Any, cast
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_agent_loop import T22Database
from test_agent_loop import t22_database as t22_database
from test_approvals import _seed_allocations, create_approval_scenario

from continuum_api.runtime.approvals import ApprovalConflict, ApprovalService
from continuum_api.validation.contracts import CommandEvidence, PhaseOutcome
from continuum_api.validation.hidden_checks import HiddenCheckResult
from continuum_api.validation.managed_hidden_checks import (
    ManagedHiddenCheckEvidence,
    _invocation_digest,
)


def test_public_approval_service_rejects_hidden_evidence(
    t22_database: T22Database, tmp_path: Any
) -> None:
    scenario = create_approval_scenario(t22_database, tmp_path, prepare_only=True)
    with pytest.raises(ApprovalConflict, match="approval_hidden_evidence_not_allowed"):
        scenario.service.record_validation(
            scenario.validation_lease,
            scenario.candidate_artifact_id,
            scenario.evidence,
            hidden_check_evidence=cast(ManagedHiddenCheckEvidence, object()),
        )


def test_operator_approval_service_accepts_operator_normal_validation_allocations(
    t22_database: T22Database, tmp_path: Any
) -> None:
    scenario = create_approval_scenario(t22_database, tmp_path, prepare_only=True)
    _seed_allocations(
        t22_database,
        scenario.run,
        scenario.validation_lease,
        scenario.allocation_ids,
        scenario.evidence,
    )
    with t22_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_allocations SET allocation_class='operator' "
                "WHERE owner_id=:owner AND run_id=:run AND allocation_id = ANY(:ids)"
            ),
            {
                "owner": scenario.run.owner_id,
                "run": scenario.run.run_id,
                "ids": list(scenario.allocation_ids),
            },
        )
    service = ApprovalService(
        t22_database.runtime_engine,
        scenario.principal,
        scenario.transitions,
        scenario.storage,
        publication_facts=scenario.facts_provider,
        validation_allocation_class="operator",
    )
    receipt = service.record_validation(
        scenario.validation_lease,
        scenario.candidate_artifact_id,
        scenario.evidence,
    )
    assert receipt.result_id is not None


def test_operator_approval_result_is_fenced_by_current_lease(
    t22_database: T22Database, tmp_path: Any
) -> None:
    scenario = create_approval_scenario(t22_database, tmp_path, prepare_only=True)
    _seed_allocations(
        t22_database,
        scenario.run,
        scenario.validation_lease,
        scenario.allocation_ids,
        scenario.evidence,
    )
    with t22_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_allocations SET allocation_class='operator' "
                "WHERE owner_id=:owner AND run_id=:run AND allocation_id = ANY(:ids)"
            ),
            {
                "owner": scenario.run.owner_id,
                "run": scenario.run.run_id,
                "ids": list(scenario.allocation_ids),
            },
        )
        connection.execute(
            text(
                "UPDATE runtime_private.jobs SET lease_owner='superseded' "
                "WHERE owner_id=:owner AND run_id=:run AND job_id=:job"
            ),
            {
                "owner": scenario.run.owner_id,
                "run": scenario.run.run_id,
                "job": scenario.validation_lease.job_id,
            },
        )
    service = ApprovalService(
        t22_database.runtime_engine,
        scenario.principal,
        scenario.transitions,
        scenario.storage,
        publication_facts=scenario.facts_provider,
        validation_allocation_class="operator",
    )
    with pytest.raises(ApprovalConflict, match="approval_execution_stale"):
        service.record_validation(
            scenario.validation_lease,
            scenario.candidate_artifact_id,
            scenario.evidence,
        )
    with t22_database.admin_engine.begin() as connection:
        accepted_count = connection.execute(
            text(
                "SELECT count(*) FROM runtime_private.accepted_results "
                "WHERE owner_id=:owner AND run_id=:run AND payload ? 'approval_validation'"
            ),
            {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
        ).scalar_one()
    assert accepted_count == 0


@pytest.mark.parametrize("allocation_stale", (False, True), ids=("freeze", "stale-allocation"))
def test_hidden_evidence_is_committed_with_validation_and_rechecked_at_freeze(
    t22_database: T22Database, tmp_path: Any, allocation_stale: bool
) -> None:
    scenario = create_approval_scenario(t22_database, tmp_path, prepare_only=True)
    _seed_allocations(
        t22_database,
        scenario.run,
        scenario.validation_lease,
        scenario.allocation_ids,
        scenario.evidence,
    )
    hidden_allocation_id = uuid4()
    now = scenario.validation_lease.lease_expires_at
    period = "t27-approval-fixture"
    suite_sha256 = hashlib.sha256(b"fixture-suite").hexdigest()
    stdout_sha256 = hashlib.sha256(b"passed\n").hexdigest()
    stderr_sha256 = hashlib.sha256(b"").hexdigest()
    command_id = "python-hidden-probe-v1"
    entry_path = "tests/test_hidden.py"
    invocation_sha256 = _invocation_digest(suite_sha256, entry_path, command_id)
    phase = scenario.evidence.candidate.model_copy(
        update={
            "allocation_id": hidden_allocation_id,
            "allocation_status": "stopped",
            "outcome": PhaseOutcome.PASSED,
            "test_exit_code": 0,
            "command_id": command_id,
            "commands": (
                CommandEvidence(
                    phase="hidden_probe",
                    exit_code=0,
                    stdout_bytes=8,
                    stderr_bytes=0,
                    stdout_sha256=stdout_sha256,
                    stderr_sha256=stderr_sha256,
                ),
            ),
        }
    )
    hidden = ManagedHiddenCheckEvidence(
        run_id=scenario.run.run_id,
        step_id=scenario.validation_lease.step_id,
        attempt_id=scenario.validation_lease.attempt_id,
        generation=scenario.validation_lease.generation,
        suite_id="fixture-suite",
        suite_sha256=suite_sha256,
        entry_path=entry_path,
        invocation_sha256=invocation_sha256,
        execution=phase,
        comparison=HiddenCheckResult(
            outcome="passed",
            passed_count=1,
            total_count=1,
            suite_sha256=suite_sha256,
            candidate_archive_sha256=phase.source_archive_sha256,
            candidate_tree_sha256=phase.source_tree_sha256,
            stdout_sha256=stdout_sha256,
            stderr_sha256=stderr_sha256,
            code="passed",
        ),
    )
    with t22_database.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.sandbox_allocations SET allocation_class='operator' "
                "WHERE owner_id=:owner AND run_id=:run AND allocation_id = ANY(:ids)"
            ),
            {
                "owner": scenario.run.owner_id,
                "run": scenario.run.run_id,
                "ids": list(scenario.allocation_ids),
            },
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.sandbox_allocations "
                "(allocation_id, owner_id, run_id, step_id, job_id, generation, attempt_id, "
                "lease_owner, logical_key, request_sha256, provider_name, provider_period_id, "
                "allocation_class, requested_seconds, vcpus, memory_mb, reserved_cpu_seconds, "
                "status, create_authorized_at, last_observed_at, last_sandbox_status) VALUES "
                "(:id, :owner, :run, :step, :job, :generation, :attempt, :lease_owner, "
                ":logical_key, :request_sha, :provider, :period, 'operator', 120, 2, 4096, 240, "
                "'stopped', :now, :now, 'stopped')"
            ),
            {
                "id": hidden_allocation_id,
                "owner": scenario.run.owner_id,
                "run": scenario.run.run_id,
                "step": scenario.validation_lease.step_id,
                "job": scenario.validation_lease.job_id,
                "generation": scenario.validation_lease.generation,
                "attempt": scenario.validation_lease.attempt_id,
                "lease_owner": scenario.validation_lease.lease_owner,
                "logical_key": f"t48:hidden:v1:{invocation_sha256}",
                "request_sha": bytes.fromhex(hashlib.sha256(b"hidden-allocation").hexdigest()),
                "provider": f"continuum-{hidden_allocation_id.hex}",
                "period": period,
                "now": now,
            },
        )
    service = ApprovalService(
        t22_database.runtime_engine,
        scenario.principal,
        scenario.transitions,
        scenario.storage,
        publication_facts=scenario.facts_provider,
        validation_allocation_class="operator",
    )
    receipt = service.record_validation(
        scenario.validation_lease,
        scenario.candidate_artifact_id,
        scenario.evidence,
        hidden_check_evidence=hidden,
    )
    assert receipt.result["hidden_check_evidence"]["invocation_sha256"] == invocation_sha256
    if allocation_stale:
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.sandbox_allocations SET status='unknown' "
                    "WHERE allocation_id=:allocation"
                ),
                {"allocation": hidden_allocation_id},
            )
        stale_freeze_lease = scenario.queue.claim_one("hidden-approval-freeze")
        assert stale_freeze_lease is not None
        with pytest.raises(ApprovalConflict, match="approval_hidden_allocation_mismatch"):
            asyncio.run(service.freeze(stale_freeze_lease))
        return

    freeze_lease = scenario.queue.claim_one("hidden-approval-freeze")
    assert freeze_lease is not None
    frozen = asyncio.run(service.freeze(freeze_lease))
    assert frozen.status == "pending"
    with t22_database.admin_engine.begin() as connection:
        manifest_evidence_digest = connection.execute(
            text(
                "SELECT manifest->'validation'->>'evidence_result_sha256' "
                "FROM runtime_private.approvals WHERE owner_id=:owner AND approval_id=:approval"
            ),
            {"owner": scenario.run.owner_id, "approval": frozen.approval_id},
        ).scalar_one()
    assert manifest_evidence_digest == receipt.result_sha256.hex()
    assert receipt.result["hidden_check_evidence"]["execution"]["allocation_id"] == str(
        hidden_allocation_id
    )
