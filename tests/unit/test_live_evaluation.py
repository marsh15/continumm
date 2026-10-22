from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from continuum_api.credentials import CredentialProvider
from continuum_api.repositories.preflight import EligibilityReceipt
from continuum_api.runtime import live_evaluation as live
from continuum_api.runtime.leases import ExecutionLease
from continuum_api.runtime.persistence import Principal
from continuum_api.runtime.workflow_resources import WorkflowConfigurationError
from continuum_api.validation.contracts import (
    CommandEvidence,
    PhaseEvidence,
    PhaseOutcome,
    ValidationClassification,
    ValidationEvidence,
    ValidationPhase,
)
from continuum_api.validation.service import ValidationService


class _Engine:
    def __init__(self) -> None:
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


def _pin(profile: str, provider: str = "openai") -> SimpleNamespace:
    return SimpleNamespace(profile_id=profile, profile_version="1", provider=provider)


def test_observer_preflights_both_then_reads_without_factory(monkeypatch) -> None:
    account_id, python_id, node_id = uuid4(), uuid4(), uuid4()
    engine = _Engine()
    order: list[str] = []
    monkeypatch.setattr(live, "_preflight_engine", lambda _settings: engine)

    def admission(_engine, _account, run_id):
        order.append("preflight")
        return _pin("python-pytest" if run_id == python_id else "node-npm")

    def factory(*_args, **_kwargs):
        pytest.fail("observer mode must not construct workflow resources")

    def reader(_engine, _account, run_id):
        order.append("snapshot")
        return {
            "run_id": str(run_id),
            "status": "running",
            "secret": "filtered",
            "independent_hidden_checks": "not_run",
        }

    report = asyncio.run(
        live.run_live_evaluation(
            object(),
            account_id,
            "openai",
            python_id,
            node_id,
            execute=False,
            resource_factory=factory,
            snapshot_reader=reader,
            admission_reader=admission,
        )
    )

    assert order == ["preflight", "preflight", "snapshot", "snapshot"]
    assert report["outcome"] == "observed"
    assert report["protocol_smoke_ready"] is False
    assert report["cases"]["python"] == {
        "run_id": str(python_id),
        "status": "running",
        "independent_hidden_checks": "not_run",
    }
    assert report["independent_hidden_checks"] == "not_run"
    assert report["task_correctness"] == "not_assessed"
    assert engine.disposed


def test_hidden_selector_is_execute_only_and_forwarded_to_factory(monkeypatch) -> None:
    account_id, python_id, node_id = uuid4(), uuid4(), uuid4()
    engine = _Engine()
    monkeypatch.setattr(live, "_preflight_engine", lambda _settings: engine)

    def selector(*_args):
        return None

    with pytest.raises(live.LiveEvaluationError, match="live_hidden_selector_invalid"):
        asyncio.run(
            live.run_live_evaluation(
                object(),
                account_id,
                "openai",
                python_id,
                node_id,
                execute=False,
                hidden_check_selector=selector,
                admission_reader=lambda _e, _a, run: _pin(
                    "python-pytest" if run == python_id else "node-npm"
                ),
            )
        )
    assert engine.disposed is False

    seen = []

    async def factory(*args, **kwargs):
        seen.append((args, kwargs))
        return "resources"

    assert (
        asyncio.run(live._call_factory(factory, "settings", account_id, python_id, selector))
        == "resources"
    )
    assert seen == [
        (
            ("settings", account_id, python_id),
            {"evaluation": True, "hidden_check_selector": selector},
        )
    ]


def test_hidden_status_aggregate_is_conservative() -> None:
    assert (
        live._aggregate_hidden_checks(
            {
                "python": {"snapshot": {"independent_hidden_checks": "passed"}},
                "node": {"snapshot": {"independent_hidden_checks": "passed"}},
            }
        )
        == "passed"
    )
    assert (
        live._aggregate_hidden_checks(
            {
                "python": {"snapshot": {"independent_hidden_checks": "passed"}},
                "node": {"snapshot": {"independent_hidden_checks": "not_run"}},
            }
        )
        == "partial"
    )
    assert (
        live._aggregate_hidden_checks(
            {
                "python": {"snapshot": {"independent_hidden_checks": "failed"}},
                "node": {"snapshot": {"independent_hidden_checks": "not_run"}},
            }
        )
        == "failed"
    )


def test_accepted_hidden_payload_digest_must_match_canonical_json() -> None:
    payload = {"candidate_artifact_id": str(uuid4()), "run_revision": 3}
    digest = hashlib.sha256(live._canonical_json(payload).encode()).digest()
    row = {"payload": payload, "result_sha256": digest}
    assert live._accepted_payload_hash_matches(row)
    assert not live._accepted_payload_hash_matches({**row, "result_sha256": b"x" * 32})


class _HiddenSnapshotConnection:
    def __init__(
        self,
        *,
        candidate_id,
        baseline_id,
        candidate_hash,
        candidate_tree,
        baseline_hash,
        baseline_tree,
        hidden,
        lease,
        status="stopped",
    ):
        self.candidate_id = candidate_id
        self.baseline_id = baseline_id
        self.candidate_hash = candidate_hash
        self.candidate_tree = candidate_tree
        self.baseline_hash = baseline_hash
        self.baseline_tree = baseline_tree
        self.hidden = hidden
        self.lease = lease
        self.status = status

    def execute(self, statement, params):
        sql = str(statement)
        if "SELECT activity,revision FROM runtime_private.steps" in sql:
            return SimpleNamespace(
                mappings=lambda: SimpleNamespace(
                    one_or_none=lambda: {"activity": "candidate_validation", "revision": 4}
                )
            )
        if "SELECT artifact_id FROM runtime_private.accepted_results" in sql:
            return SimpleNamespace(scalar_one_or_none=lambda: self.candidate_id)
        if "FROM runtime_private.artifact_uploads" in sql:
            artifact_id = params["artifact"]
            archive, tree = (
                (self.candidate_hash, self.candidate_tree)
                if artifact_id == self.candidate_id
                else (self.baseline_hash, self.baseline_tree)
            )
            row = {
                "status": "committed",
                "archive_sha256": bytes.fromhex(archive),
                "tree_sha256": bytes.fromhex(tree),
                "github_repository_id": 101,
                "content_expires_at": datetime.now(UTC) + timedelta(days=1),
            }
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))
        if "FROM runtime_private.sandbox_allocations" in sql:
            row = {
                "allocation_id": self.hidden.execution.allocation_id,
                "owner_id": self.lease.owner_id,
                "run_id": self.lease.run_id,
                "step_id": self.lease.step_id,
                "job_id": self.lease.job_id,
                "generation": self.lease.generation,
                "attempt_id": self.lease.attempt_id,
                "lease_owner": self.lease.lease_owner,
                "allocation_class": "operator",
                "status": self.status,
            }
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))
        raise AssertionError(f"unexpected hidden snapshot SQL: {sql}")


def _hidden_snapshot_row(payload, lease):
    digest = hashlib.sha256(live._canonical_json(payload).encode()).digest()
    return {
        "payload": payload,
        "result_sha256": digest,
        "owner_id": lease.owner_id,
        "run_id": lease.run_id,
        "step_id": lease.step_id,
        "job_id": lease.job_id,
        "generation": lease.generation,
        "attempt_id": lease.attempt_id,
        "lease_owner": lease.lease_owner,
        "event_sequence": 10,
    }


def test_hidden_snapshot_requires_current_hashed_fenced_evidence() -> None:
    from test_hidden_validation_activity import _fixture

    lease, inputs, _suite, hidden, normal = _fixture()
    payload = {
        "candidate_artifact_id": str(inputs.candidate_artifact_id),
        "run_revision": 4,
        "approval_validation": normal.model_dump(mode="json"),
        "hidden_check_evidence": hidden.model_dump(mode="json"),
    }
    row = _hidden_snapshot_row(payload, lease)
    connection = _HiddenSnapshotConnection(
        candidate_id=inputs.candidate_artifact_id,
        baseline_id=inputs.baseline_artifact_id,
        candidate_hash=normal.candidate.source_archive_sha256,
        candidate_tree=normal.candidate.source_tree_sha256,
        baseline_hash=normal.baseline.source_archive_sha256,
        baseline_tree=normal.baseline.source_tree_sha256,
        hidden=hidden,
        lease=lease,
    )
    args = dict(
        revision=4,
        repository_id=101,
        profile_id="python-pytest",
        profile_version="1",
        observed_at=datetime.now(UTC),
    )
    assert (
        live._hidden_check_snapshot(
            connection, lease.owner_id, lease.run_id, results=[row], **args
        )[0]
        == "passed"
    )

    _status, metadata = live._hidden_check_snapshot(
        connection, lease.owner_id, lease.run_id, results=[row], **args
    )
    assert (
        metadata["hidden_check_candidate_archive_sha256"] == normal.candidate.source_archive_sha256
    )
    assert metadata["hidden_check_candidate_tree_sha256"] == normal.candidate.source_tree_sha256
    assert metadata["hidden_check_repository_id"] == normal.candidate.repository_id
    assert metadata["hidden_check_base_commit_sha"] == normal.candidate.base_commit_sha
    assert metadata["hidden_check_invocation_sha256"] == hidden.invocation_sha256
    assert live._safe_snapshot(metadata) == metadata

    tampered = {**row, "result_sha256": b"x" * 32}
    assert (
        live._hidden_check_snapshot(
            connection, lease.owner_id, lease.run_id, results=[tampered], **args
        )[0]
        == "unknown"
    )
    stale = _hidden_snapshot_row({**payload, "run_revision": 3}, lease)
    assert (
        live._hidden_check_snapshot(
            connection, lease.owner_id, lease.run_id, results=[stale], **args
        )[0]
        == "not_run"
    )
    connection.status = "running"
    assert (
        live._hidden_check_snapshot(
            connection, lease.owner_id, lease.run_id, results=[row], **args
        )[0]
        == "invalid"
    )


def test_normal_validation_payload_without_hidden_evidence_cannot_pass() -> None:
    from test_hidden_validation_activity import _fixture

    lease, inputs, _suite, _hidden, normal = _fixture()
    payload = {
        "candidate_artifact_id": str(inputs.candidate_artifact_id),
        "run_revision": 4,
        "approval_validation": normal.model_dump(mode="json"),
    }
    assert (
        live._hidden_check_snapshot(
            object(),
            lease.owner_id,
            lease.run_id,
            revision=4,
            repository_id=101,
            profile_id="python-pytest",
            profile_version="1",
            results=[_hidden_snapshot_row(payload, lease)],
            observed_at=datetime.now(UTC),
        )[0]
        == "not_run"
    )


def test_execute_preflights_both_before_factory_and_closes_each(monkeypatch) -> None:
    account_id, python_id, node_id = uuid4(), uuid4(), uuid4()
    engine = _Engine()
    order: list[str] = []
    monkeypatch.setattr(live, "_preflight_engine", lambda _settings: engine)
    monkeypatch.setattr(live, "_reject_public_allocations", lambda *_args: None)

    def admission(_engine, _account, run_id):
        order.append(f"preflight:{run_id}")
        return _pin("python-pytest" if run_id == python_id else "node-npm")

    class Resources:
        def __init__(self, run_id):
            self.engine = engine
            self.run_id = run_id
            self.runner = self
            self.closed = False

        async def run_admitted(self, run_id, *, max_jobs, max_seconds):
            assert run_id == self.run_id
            assert max_jobs == 2 and max_seconds == 10
            order.append(f"drain:{run_id}")
            return SimpleNamespace(run_id=run_id, jobs_started=1, stop_reason="waiting")

        async def aclose(self):
            self.closed = True
            order.append(f"close:{self.run_id}")
            if self.run_id == python_id:
                raise RuntimeError("close failed")

    resources = []

    async def factory(_settings, _account, run_id, *, evaluation):
        assert evaluation is True
        order.append(f"factory:{run_id}")
        item = Resources(run_id)
        resources.append(item)
        return item

    report = asyncio.run(
        live.run_live_evaluation(
            object(),
            account_id,
            CredentialProvider.OPENAI,
            python_id,
            node_id,
            execute=True,
            max_jobs=2,
            max_seconds=10,
            resource_factory=factory,
            snapshot_reader=lambda _engine, _account, run_id: {
                "run_id": str(run_id),
                "protocol_smoke_ready": False,
            },
            admission_reader=admission,
        )
    )

    assert order[:2] == [f"preflight:{python_id}", f"preflight:{node_id}"]
    assert order.index(f"factory:{python_id}") > order.index(f"preflight:{node_id}")
    assert order.index(f"factory:{node_id}") > order.index(f"close:{python_id}")
    assert all(item.closed for item in resources)
    assert report["protocol_smoke_ready"] is False
    assert report["outcome"] == "failed"
    assert set(report["cases"]) == {"python", "node"}
    assert report["cases"]["python"]["cleanup_error"] == "live_run_failed"
    assert "snapshot" in report["cases"]["python"]
    assert "drain" in report["cases"]["node"]
    assert engine.disposed


def test_preflight_mismatch_prevents_both_factory_and_drain(monkeypatch) -> None:
    account_id, python_id, node_id = uuid4(), uuid4(), uuid4()
    engine = _Engine()
    monkeypatch.setattr(live, "_preflight_engine", lambda _settings: engine)
    calls = []

    def admission(_engine, _account, run_id):
        return _pin("python-pytest" if run_id == python_id else "node-npm", "anthropic")

    with pytest.raises(live.LiveEvaluationError, match="live_admission_pin_mismatch"):
        asyncio.run(
            live.run_live_evaluation(
                object(),
                account_id,
                "openai",
                python_id,
                node_id,
                execute=True,
                resource_factory=lambda *_args, **_kwargs: calls.append("factory"),
                admission_reader=admission,
            )
        )
    assert calls == []
    assert engine.disposed


def test_admission_configuration_error_is_sanitized_and_before_factory(monkeypatch) -> None:
    account_id, python_id, node_id = uuid4(), uuid4(), uuid4()
    engine = _Engine()
    monkeypatch.setattr(live, "_preflight_engine", lambda _settings: engine)
    calls = []

    def admission(*_args):
        raise WorkflowConfigurationError("workflow_run_inactive")

    with pytest.raises(live.LiveEvaluationError, match="workflow_run_inactive"):
        asyncio.run(
            live.run_live_evaluation(
                object(),
                account_id,
                "openai",
                python_id,
                node_id,
                execute=False,
                resource_factory=lambda *_args, **_kwargs: calls.append("factory"),
                admission_reader=admission,
            )
        )
    assert calls == []
    assert engine.disposed


def test_validation_uses_operator_allocation_class_when_selected() -> None:
    allocation_classes = []

    class Allocator:
        async def create(self, _principal, _lease, _key, _seconds, *, allocation_class="public"):
            allocation_classes.append(allocation_class)
            raise RuntimeError("stop after allocation authorization")

    class Profile:
        configured_image = "registry.example/test@sha256:" + "a" * 64

    service = ValidationService(Profile(), Profile(), Allocator(), allocation_class="operator")
    eligibility = EligibilityReceipt(
        repository_id=1,
        commit_sha="a" * 40,
        tree_sha="b" * 40,
        profile_id="python-pytest",
        profile_version="1",
        file_count=1,
        source_bytes=1,
        manifest_blobs=(),
        package_count=0,
    )

    asyncio.run(
        service._run_phase(
            Profile(),
            Principal(uuid4()),
            ExecutionLease(uuid4(), uuid4(), uuid4(), uuid4(), uuid4(), 1, "worker", None),
            phase=ValidationPhase.BASELINE,
            logical_key="test-operator-allocation",
            baseline_artifact_id=uuid4(),
            eligibility=eligibility,
            source_archive_sha256="c" * 64,
            source_tree_sha256="d" * 64,
            baseline_source_tree_sha256="b" * 64,
            baseline_archive_sha256="a" * 64,
            candidate_archive_sha256=None,
            candidate_source_tree_sha256=None,
            expected_profile=("python-pytest", "1", "python-pytest-v1"),
            validation_source=None,
        )
    )

    assert allocation_classes == ["operator"]


def test_validation_evidence_json_round_trip_and_digests_are_verified() -> None:
    from continuum_api.approvals.manifest import digest_manifest

    run_id, result_id, candidate_id = uuid4(), uuid4(), uuid4()
    evidence = _passed_evidence(candidate_id)
    payload = {
        "candidate_artifact_id": str(candidate_id),
        "validation_evidence": evidence.model_dump(mode="json"),
    }
    payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    result_digest = hashlib.sha256(payload_json.encode()).digest()
    validation = {
        "evidence_result_id": str(result_id),
        "evidence_result_sha256": result_digest.hex(),
        "profile_id": "python-pytest",
        "profile_version": "1",
        "classification": ValidationClassification.PASSED.value,
    }
    manifest = {
        "manifest_version": 1,
        "run_id": str(run_id),
        "revision": 5,
        "repository": {"id": 1},
        "candidate": {
            "checkpoint_id": str(candidate_id),
            "archive_sha256": evidence.candidate_archive_sha256,
            "source_tree_sha256": evidence.candidate_source_tree_sha256,
            "changed_paths": [],
        },
        "validation": validation,
    }
    _encoded, manifest_digest = digest_manifest(manifest)
    approval = {
        "status": "pending",
        "revision": 5,
        "manifest": manifest,
        "manifest_sha256": manifest_digest,
        "candidate_artifact_id": candidate_id,
        "evidence_result_id": result_id,
    }
    result_rows = [{"result_id": result_id, "result_sha256": result_digest, "payload": payload}]

    classification, _commands = live._validation_summary(result_rows, approval)
    assert classification == "passed"
    assert live._approval_identity_matches(
        approval,
        result_rows,
        metadata_run_id=run_id,
        revision=5,
        repository_id=1,
        profile_id="python-pytest",
        profile_version="1",
        validation_classification="passed",
    )

    changed_manifest = {**manifest, "revision": 6}
    tampered_approval = {**approval, "manifest": changed_manifest}
    assert not live._approval_identity_matches(
        tampered_approval,
        result_rows,
        metadata_run_id=run_id,
        revision=5,
        repository_id=1,
        profile_id="python-pytest",
        profile_version="1",
        validation_classification="passed",
    )
    assert live._candidate_patch(tampered_approval) == (None, None)
    tampered_rows = [{**result_rows[0], "payload": {**payload, "extra": "tampered"}}]
    assert live._validation_summary(tampered_rows, approval) == ("unknown", [])
    assert not live._approval_identity_matches(
        approval,
        tampered_rows,
        metadata_run_id=run_id,
        revision=5,
        repository_id=1,
        profile_id="python-pytest",
        profile_version="1",
        validation_classification="passed",
    )


def _passed_evidence(candidate_id) -> ValidationEvidence:
    from continuum_api.repositories.preflight import PYTHON_TARGET_VERSION

    archive = "a" * 64
    tree = "b" * 64
    candidate_archive = "c" * 64
    candidate_tree = "d" * 64
    output_hash = "e" * 64
    baseline_allocation, candidate_allocation = uuid4(), uuid4()
    baseline = PhaseEvidence(
        phase=ValidationPhase.BASELINE,
        outcome=PhaseOutcome.PASSED,
        allocation_id=baseline_allocation,
        artifact_id=candidate_id,
        repository_id=1,
        base_commit_sha="f" * 40,
        baseline_git_tree_sha256="b" * 40,
        profile_id="python-pytest",
        profile_version="1",
        image="registry.example/test@sha256:" + "1" * 64,
        runtime_version=PYTHON_TARGET_VERSION,
        command_id="python-pytest-v1",
        baseline_archive_sha256=archive,
        baseline_archive_tree_sha256=tree,
        source_archive_sha256=archive,
        source_tree_sha256=tree,
        test_exit_code=0,
        package_count=0,
        allocation_status="stopped",
        commands=(
            CommandEvidence(
                phase="pytest",
                exit_code=0,
                stdout_bytes=0,
                stderr_bytes=0,
                stdout_sha256=output_hash,
                stderr_sha256=output_hash,
            ),
        ),
    )
    candidate = PhaseEvidence(
        phase=ValidationPhase.CANDIDATE,
        outcome=PhaseOutcome.PASSED,
        allocation_id=candidate_allocation,
        artifact_id=candidate_id,
        repository_id=1,
        base_commit_sha="f" * 40,
        baseline_git_tree_sha256="b" * 40,
        profile_id="python-pytest",
        profile_version="1",
        image="registry.example/test@sha256:" + "1" * 64,
        runtime_version=PYTHON_TARGET_VERSION,
        command_id="python-pytest-v1",
        baseline_archive_sha256=archive,
        baseline_archive_tree_sha256=tree,
        source_archive_sha256=candidate_archive,
        source_tree_sha256=candidate_tree,
        test_exit_code=0,
        package_count=0,
        allocation_status="stopped",
        commands=(),
    )
    return ValidationEvidence(
        classification=ValidationClassification.PASSED,
        baseline_artifact_id=candidate_id,
        candidate_archive_sha256=candidate_archive,
        candidate_source_tree_sha256=candidate_tree,
        candidate_lineage_sha256="9" * 64,
        changed_path_count=0,
        baseline=baseline,
        candidate=candidate,
    )


@pytest.mark.parametrize("status", ["invalid", "unknown"])
def test_rejected_hidden_observation_is_reported_without_fabricated_evidence(status) -> None:
    from test_hidden_validation_activity import _fixture

    lease, _inputs, _suite, _hidden, normal = _fixture()
    payload = {
        "run_revision": 4,
        "validation_evidence": normal.model_dump(mode="json"),
        "hidden_check_status": status,
    }
    row = _hidden_snapshot_row(payload, lease)
    options = dict(
        revision=4,
        repository_id=101,
        profile_id="python-pytest",
        profile_version="1",
        results=[row],
        observed_at=datetime.now(UTC),
    )
    assert live._hidden_check_snapshot(None, lease.owner_id, lease.run_id, **options) == (
        status,
        {},
    )
    row["result_sha256"] = b"x" * 32
    assert live._hidden_check_snapshot(None, lease.owner_id, lease.run_id, **options) == (
        "unknown",
        {},
    )
