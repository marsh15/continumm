"""PostgreSQL proof for persisted validation source pins and resolution."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from test_agent_loop import T22Database, _checkpoint_service
from test_agent_loop import t22_database as t22_database
from test_approvals import (
    _BASE_SHA,
    _REPOSITORY_ID,
    _close_scenario,
    _seed_allocations,
    _validation_evidence,
    create_approval_scenario,
)
from test_candidate_validation import _LOCK

from continuum_api.artifacts.archives import ArchiveFile
from continuum_api.repositories.preflight import EligibilityReceipt
from continuum_api.runtime.artifact_persistence import ArtifactPersistence
from continuum_api.runtime.leases import ExecutionLease, LeaseLost
from continuum_api.runtime.persistence import Principal, RuntimeRepository
from continuum_api.runtime.validation_activity import (
    CandidateValidationActivityHandler,
    ValidationActivityError,
)
from continuum_api.runtime.validation_source import (
    PersistedCandidateValidationInputResolver,
    ValidationSourcePinService,
)
from continuum_api.tools.contracts import ToolCapability, ToolStage
from continuum_api.tools.policy import ToolPolicyContext
from continuum_api.tools.source_tools import (
    discover_baseline_test_paths,
    snapshot_from_verified_archive,
    source_paths,
)
from continuum_api.validation.service import ValidationService

_BASELINE = (
    ArchiveFile("pyproject.toml", b"[tool.pytest.ini_options]\ntestpaths = ['tests']\n"),
    ArchiveFile("requirements.lock", _LOCK),
    ArchiveFile("src/app.py", b"def answer():\n    return 'base'\n"),
    ArchiveFile(
        "tests/test_app.py",
        b"from src.app import answer\n\ndef test_answer():\n    assert answer() == 'base'\n",
    ),
)


def _eligibility(baseline) -> EligibilityReceipt:
    from continuum_api.sandbox.python_profile import _git_blob_sha, _git_tree_sha

    return EligibilityReceipt(
        repository_id=_REPOSITORY_ID,
        commit_sha=_BASE_SHA,
        tree_sha=_git_tree_sha(_BASELINE, "sha1"),
        profile_id="python-pytest",
        profile_version="1",
        file_count=len(baseline.manifest.entries),
        source_bytes=sum(entry.size for entry in baseline.manifest.entries),
        manifest_blobs=(
            (
                "pyproject.toml",
                _git_blob_sha(_BASELINE[0].content, "sha1"),
            ),
            ("requirements.lock", _git_blob_sha(_LOCK, "sha1")),
        ),
        package_count=1,
    )


def _policy_context(lease, eligibility: EligibilityReceipt, baseline):
    del lease
    snapshot = snapshot_from_verified_archive(
        baseline,
        expected_archive_sha256=baseline.archive_sha256,
        expected_tree_sha256=baseline.manifest.tree_sha256,
    )
    return ToolPolicyContext(
        capabilities=frozenset(ToolCapability),
        profile_id=eligibility.profile_id,
        profile_version=eligibility.profile_version,
        actor_can_write_repository=True,
        app_can_access_repository=True,
        stage=ToolStage.AGENT_WORK,
        run_budget_available=True,
        baseline_tree_sha256=snapshot.tree_sha256,
        baseline_source_paths=frozenset(source_paths(snapshot)),
        baseline_test_paths=frozenset(
            discover_baseline_test_paths(
                snapshot,
                eligibility.profile_id,
                expected_tree_sha256=snapshot.tree_sha256,
            )
        ),
        baseline_paths_verified=True,
        protected_paths=frozenset(),
    )


class _FixedAllocator:
    def __init__(self, allocation_ids) -> None:
        self._allocation_ids = iter(allocation_ids)

    async def create(self, *_args, **_kwargs):
        return SimpleNamespace(allocation_id=next(self._allocation_ids))


def test_persisted_resolver_feeds_candidate_validation_and_survives_rework(
    t22_database: T22Database,
    tmp_path: Path,
) -> None:
    from test_candidate_validation import _Profile
    from test_review_rework import _decision, _validate_reworked_candidate

    scenario = create_approval_scenario(
        t22_database,
        tmp_path,
        prepare_only=True,
        baseline_files=_BASELINE,
    )
    try:
        persistence = ArtifactPersistence(t22_database.runtime_engine)
        checkpoints = _checkpoint_service(
            t22_database.runtime_engine,
            scenario.principal,
            persistence,
            scenario.storage,
        )
        eligibility = _eligibility(scenario.baseline_verified)
        resolver = PersistedCandidateValidationInputResolver(
            t22_database.runtime_engine,
            scenario.principal,
            checkpoints,
            _policy_context,
        )
        with pytest.raises(ValidationActivityError, match="candidate_validation_inputs_unpinned"):
            asyncio.run(resolver(scenario.validation_lease))
        other_owner = PersistedCandidateValidationInputResolver(
            t22_database.runtime_engine,
            Principal(uuid4()),
            checkpoints,
            _policy_context,
        )
        with pytest.raises(ValidationActivityError, match="candidate_validation_owner_mismatch"):
            asyncio.run(other_owner(scenario.validation_lease))
        with pytest.raises(LeaseLost):
            asyncio.run(resolver(replace(scenario.validation_lease, generation=999)))

        pin_service = ValidationSourcePinService(
            t22_database.runtime_engine,
            scenario.principal,
            checkpoints,
        )
        pinned = asyncio.run(
            pin_service.pin(
                scenario.validation_lease,
                eligibility,
                scenario.baseline_artifact_id,
            )
        )
        assert pinned.baseline_artifact_id == scenario.baseline_artifact_id
        assert (
            asyncio.run(
                pin_service.pin(
                    scenario.validation_lease,
                    eligibility,
                    scenario.baseline_artifact_id,
                )
            )
            == pinned
        )
        with pytest.raises(ValidationActivityError, match="validation_source_pin_conflict"):
            asyncio.run(
                pin_service.pin(
                    scenario.validation_lease,
                    replace(eligibility, package_count=eligibility.package_count + 1),
                    scenario.baseline_artifact_id,
                )
            )
        with pytest.raises(DBAPIError):
            with t22_database.runtime_engine.begin() as connection:
                RuntimeRepository(connection, scenario.principal)
                connection.execute(
                    text(
                        "UPDATE runtime_private.validation_source_pins "
                        "SET eligibility_sha256=eligibility_sha256 "
                        "WHERE owner_id=:owner AND run_id=:run"
                    ),
                    {"owner": scenario.run.owner_id, "run": scenario.run.run_id},
                )
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.validation_source_pins SET eligibility_sha256=:digest "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {
                    "digest": b"x" * 32,
                    "owner": scenario.run.owner_id,
                    "run": scenario.run.run_id,
                },
            )
        with pytest.raises(ValidationActivityError, match="candidate_validation_pin_corrupt"):
            asyncio.run(resolver(scenario.validation_lease))
        with t22_database.admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE runtime_private.validation_source_pins SET eligibility_sha256=:digest "
                    "WHERE owner_id=:owner AND run_id=:run"
                ),
                {
                    "digest": pinned.eligibility_sha256,
                    "owner": scenario.run.owner_id,
                    "run": scenario.run.run_id,
                },
            )
        inputs = asyncio.run(resolver(scenario.validation_lease))
        assert inputs.candidate_artifact_id == scenario.candidate_artifact_id
        assert (
            inputs.source.candidate_archive_sha256 == scenario.candidate_patch.source.archive_sha256
        )

        allocation_ids = (uuid4(), uuid4())
        profile = _Profile(inputs.source, [])
        validation = ValidationService(profile, profile, _FixedAllocator(allocation_ids))
        expected_evidence = _validation_evidence(
            scenario.run,
            scenario.baseline_artifact_id,
            inputs.source.baseline_archive_sha256,
            inputs.source.baseline_source_tree_sha256,
            inputs.source.candidate_archive_sha256,
            inputs.source.candidate_source_tree_sha256,
            allocation_ids,
        )
        _seed_allocations(
            t22_database,
            scenario.run,
            scenario.validation_lease,
            allocation_ids,
            expected_evidence,
            lineage_sha256=inputs.source.lineage_sha256,
        )
        handler = CandidateValidationActivityHandler(
            t22_database.runtime_engine,
            scenario.principal,
            resolver,
            validation,
            scenario.service,
            scenario.transitions,
        )
        receipt = asyncio.run(handler(scenario.validation_lease))
        assert receipt.run_id == scenario.run.run_id
        scenario.validation_receipt = receipt
        scenario.freeze_lease = scenario.queue.claim_one("t34-source-pin-freeze")
        assert scenario.freeze_lease is not None
        scenario.frozen_receipt = asyncio.run(scenario.service.freeze(scenario.freeze_lease))

        _decision(scenario, key="t34-source-pin-rework", feedback="Add one boundary check")
        resolved_rework = []

        async def resolve_rework(lease: ExecutionLease) -> None:
            resolved_rework.append(await resolver(lease))

        _validate_reworked_candidate(scenario, before_validation=resolve_rework)
        assert len(resolved_rework) == 1
        rework = resolved_rework[0]
        assert rework.baseline_artifact_id == scenario.baseline_artifact_id
        assert rework.candidate_artifact_id == scenario.candidate_artifact_id
        assert rework.source.eligibility.to_cache_payload() == eligibility.to_cache_payload()
    finally:
        _close_scenario(scenario)
