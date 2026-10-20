"""Lease-bound preparation of an admitted run's immutable baseline checkpoint."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Engine, text

from continuum_api.artifacts.archives import (
    ArchiveError,
    ArchiveFile,
    VerifiedArchive,
    create_archive,
    verify_archive,
)
from continuum_api.artifacts.checkpoints import CheckpointStorageService
from continuum_api.repositories.preflight import (
    BUILD_PROFILES,
    EligibilityReceipt,
    RepositoryPreflightError,
)
from continuum_api.runtime.leases import ExecutionLease, lock_current_execution
from continuum_api.runtime.persistence import Principal, ReplayPolicy, RunStatus, RuntimeRepository
from continuum_api.runtime.transitions import NextWork
from continuum_api.sandbox.node_profile import NodeProfileError, build_node_profile_plan
from continuum_api.sandbox.python_profile import PythonProfileError, build_python_profile_plan


class SourcePreparationError(RuntimeError):
    """Sanitized source preparation failure with a stable code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class AdmittedSource:
    repository_id: int
    receipt: EligibilityReceipt


SourceFetcher = Callable[
    [ExecutionLease, int, EligibilityReceipt],
    Iterable[ArchiveFile] | Awaitable[Iterable[ArchiveFile]],
]


class SourcePreparationActivityHandler:
    """Fetch the exact admitted commit, validate its profile pins, and accept it.

    The injected fetcher is a trusted server adapter. It must retrieve the
    requested repository and commit and return their source files; the handler
    independently checks the Git tree, manifest blobs, profile contract,
    admission identity, and lease before checkpoint acceptance.
    """

    def __init__(
        self,
        engine: Engine,
        principal: Principal,
        checkpoints: CheckpointStorageService,
        fetch_source: SourceFetcher,
    ) -> None:
        self._engine = engine
        self._principal = principal
        self._checkpoints = checkpoints
        self._fetch_source = fetch_source

    async def __call__(self, lease: ExecutionLease) -> Any:
        admitted = await asyncio.to_thread(self._read_admitted_source, lease)
        try:
            fetch_arguments = (lease, admitted.repository_id, admitted.receipt)
            if inspect.iscoroutinefunction(self._fetch_source):
                source = await self._fetch_source(*fetch_arguments)
            else:
                source = await asyncio.to_thread(self._fetch_source, *fetch_arguments)
                if inspect.isawaitable(source):
                    source = await source
            bundle = await asyncio.to_thread(create_archive, source)
            verified = await asyncio.to_thread(
                verify_archive,
                bundle.archive,
                expected_archive_sha256=bundle.archive_sha256,
                expected_tree_sha256=bundle.manifest.tree_sha256,
                expected_manifest=bundle.manifest,
            )
            await asyncio.to_thread(_verify_profile_source, admitted.receipt, verified)
        except SourcePreparationError:
            raise
        except (
            ArchiveError,
            RepositoryPreflightError,
            PythonProfileError,
            NodeProfileError,
        ) as error:
            code = _safe_code(getattr(error, "code", "source_invalid"))
            raise SourcePreparationError(code) from None
        except Exception:
            raise SourcePreparationError("source_fetch_unavailable") from None

        return await self._checkpoints.commit_checkpoint(
            self._principal,
            lease,
            "baseline:source-preparation:v1",
            verified.files,
            result={
                "activity": "source_preparation",
                "repository_id": admitted.repository_id,
                "commit_sha": admitted.receipt.commit_sha,
                "git_tree_sha": admitted.receipt.tree_sha,
                "profile_id": admitted.receipt.profile_id,
                "profile_version": admitted.receipt.profile_version,
            },
            target_run_status=RunStatus.RUNNING,
            next_work=NextWork(
                logical_key="agent-turn:0",
                activity="agent_turn",
                revision=0,
                replay_policy=ReplayPolicy.REPLAY_SAFE,
            ),
        )

    def _read_admitted_source(self, lease: ExecutionLease) -> AdmittedSource:
        if lease.owner_id != self._principal.account_id:
            raise SourcePreparationError("source_preparation_owner_mismatch")
        with self._engine.begin() as connection:
            RuntimeRepository(connection, self._principal)
            lock_current_execution(connection, self._principal, lease)
            row = (
                connection.execute(
                    text(
                        "SELECT run.status AS run_status, run.revision, step.activity, "
                        "step.revision AS step_revision, run.github_repository_id, "
                        "pins.profile_id, pins.profile_version, pins.source_sha, "
                        "pins.source_tree_sha, pins.source_receipt "
                        "FROM runtime_private.runs AS run "
                        "JOIN runtime_private.steps AS step ON step.owner_id=run.owner_id "
                        "AND step.run_id=run.run_id "
                        "JOIN runtime_private.run_admission_pins AS pins "
                        "ON pins.owner_id=run.owner_id AND pins.run_id=run.run_id "
                        "WHERE run.owner_id=:owner AND run.run_id=:run AND step.step_id=:step"
                    ),
                    {"owner": lease.owner_id, "run": lease.run_id, "step": lease.step_id},
                )
                .mappings()
                .one_or_none()
            )
            prior_artifact = connection.execute(
                text(
                    "SELECT 1 FROM runtime_private.accepted_results "
                    "WHERE owner_id=:owner AND run_id=:run AND artifact_id IS NOT NULL LIMIT 1"
                ),
                {"owner": lease.owner_id, "run": lease.run_id},
            ).scalar_one_or_none()
        if (
            row is None
            or row["activity"] != "source_preparation"
            or row["run_status"] != RunStatus.RUNNING.value
            or row["revision"] != 0
            or row["revision"] != row["step_revision"]
            or prior_artifact is not None
        ):
            raise SourcePreparationError("source_preparation_step_stale")
        try:
            receipt = EligibilityReceipt.from_cache_payload(row["source_receipt"])
        except (RepositoryPreflightError, TypeError, ValueError):
            raise SourcePreparationError("source_preparation_receipt_invalid") from None
        profile = BUILD_PROFILES.get(row["profile_id"])
        if (
            type(row["github_repository_id"]) is not int
            or row["github_repository_id"] <= 0
            or profile is None
            or row["profile_version"] != profile.version
            or receipt.repository_id != row["github_repository_id"]
            or receipt.profile_id != profile.profile_id
            or receipt.profile_version != profile.version
            or receipt.commit_sha != row["source_sha"]
            or receipt.tree_sha != row["source_tree_sha"]
        ):
            raise SourcePreparationError("source_preparation_admission_pin_invalid")
        return AdmittedSource(row["github_repository_id"], receipt)


def _verify_profile_source(receipt: EligibilityReceipt, verified: VerifiedArchive) -> None:
    if receipt.profile_id == "python-pytest":
        build_python_profile_plan(receipt, verified)
    elif receipt.profile_id == "node-npm":
        build_node_profile_plan(receipt, verified)
    else:
        raise SourcePreparationError("source_preparation_profile_unsupported")


def _safe_code(value: object) -> str:
    code = str(value)
    return (
        code
        if code.isascii() and code.replace("_", "").isalnum() and len(code) <= 96
        else "source_preparation_failed"
    )
