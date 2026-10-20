"""PostgreSQL proof for encrypted allowlisted exports and quarantined restore checks."""

from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

from sqlalchemy import text

from continuum_api.artifacts.archives import ArchiveFile, create_archive
from continuum_api.portable_exports import (
    EXPORT_MAGIC,
    PortableExportError,
    _open_sealed,
    _read_plaintext_package,
    export_owner,
    restore_check,
)
from continuum_api.runtime.leases import QueueService
from continuum_api.runtime.persistence import EventActor, Principal, RuntimeRepository
from continuum_api.runtime.retention import RetentionService
from tests.integration.test_retention import T40Database
from tests.integration.test_retention import t40_database as t40_database


class MemoryObjectReader:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.reads: list[str] = []

    async def download(self, object_key: str) -> bytes:
        self.reads.append(object_key)
        return self.content


def test_portable_export_is_encrypted_allowlisted_and_quarantined(
    t40_database: T40Database, tmp_path: Path
) -> None:
    store = t40_database
    owner = uuid4()
    github_user_id = 940_000_000 + owner.int % 10_000_000
    principal = Principal(owner)
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.users (user_id,github_user_id,github_login) "
                "VALUES (:owner,:github,'export-test')"
            ),
            {"owner": owner, "github": github_user_id},
        )
    with store.runtime_engine.begin() as connection:
        repository = RuntimeRepository(connection, principal)
        run_id = repository.create_run(github_repository_id=410_000_001, issue_number=21)
        step_id = repository.create_step(run_id, "source", "prepare")
        repository.create_job(run_id, step_id)
        repository.append_event(
            run_id,
            1,
            "task.accepted",
            EventActor.ACCOUNT,
            actor_id=str(owner),
            payload={"summary": "synthetic export fixture"},
        )
    lease = QueueService(store.runtime_engine, principal).claim_one("export-source-fixture")
    assert lease is not None

    source = create_archive([ArchiveFile("src/example.py", b"print('portable')\n")])
    object_key = f"checkpoints/{uuid4()}.tar.gz"
    artifact_id = uuid4()
    with store.admin_engine.connect() as connection:
        run_times = connection.execute(
            text(
                "SELECT created_at,content_expires_at FROM runtime_private.runs WHERE run_id=:run"
            ),
            {"run": run_id},
        ).one()
        now: datetime = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.artifact_uploads "
                "(upload_id,owner_id,run_id,step_id,job_id,generation,attempt_id,lease_owner,"
                "github_repository_id,checkpoint_key,intent_sha256,object_key,archive_sha256,"
                "tree_sha256,format_version,compressed_bytes,source_bytes,file_count,status,"
                "object_bytes,created_at,updated_at,orphan_grace_until,content_expires_at,"
                "verified_at,committed_at) VALUES (:id,:owner,:run,:step,:job,:generation,:attempt,"
                ":lease_owner,410000001,'baseline',:intent,:object_key,:archive,:tree,1,:bytes,"
                ":source_bytes,1,'committed',:bytes,:created,:created,:grace,:expires,:verified,:committed)"
            ),
            {
                "id": artifact_id,
                "owner": owner,
                "run": run_id,
                "step": lease.step_id,
                "job": lease.job_id,
                "generation": lease.generation,
                "attempt": lease.attempt_id,
                "lease_owner": lease.lease_owner,
                "intent": hashlib.sha256(b"export-fixture").digest(),
                "object_key": object_key,
                "archive": bytes.fromhex(source.archive_sha256),
                "tree": bytes.fromhex(source.manifest.tree_sha256),
                "bytes": len(source.archive),
                "source_bytes": len(b"print('portable')\n"),
                "created": run_times.created_at,
                "grace": run_times.created_at + timedelta(hours=1),
                "expires": run_times.content_expires_at,
                "verified": now,
                "committed": now,
            },
        )
        event_sequence: int = connection.execute(
            text("SELECT max(sequence) FROM runtime_private.events WHERE run_id=:run"),
            {"run": run_id},
        ).scalar_one()
        connection.execute(
            text(
                "INSERT INTO runtime_private.accepted_results "
                "(result_id,owner_id,run_id,step_id,job_id,result_sha256,intent_sha256,"
                "run_status,event_sequence,payload,accepted_at,artifact_id) "
                "VALUES (:result,:owner,:run,:step,:job,:result_hash,:intent_hash,'running',"
                ":event,'{}'::jsonb,:accepted,:artifact)"
            ),
            {
                "result": uuid4(),
                "owner": owner,
                "run": run_id,
                "step": lease.step_id,
                "job": lease.job_id,
                "result_hash": hashlib.sha256(b"result").digest(),
                "intent_hash": hashlib.sha256(b"intent").digest(),
                "event": event_sequence,
                "accepted": now,
                "artifact": artifact_id,
            },
        )

    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO runtime_private.provider_credentials "
                "(credential_id,owner_id,provider,scope,key_version,nonce,ciphertext) "
                "VALUES (:id,:owner,'openai','saved','test-key-version',:nonce,:ciphertext)"
            ),
            {
                "id": uuid4(),
                "owner": owner,
                "nonce": secrets.token_bytes(12),
                "ciphertext": secrets.token_bytes(32),
            },
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.github_user_tokens "
                "(owner_id,github_user_id,token_id,key_version,nonce,ciphertext,status,expires_at) "
                "VALUES (:owner,:github,:id,'test-key-version',:nonce,:ciphertext,'active',"
                "clock_timestamp()+interval '1 hour')"
            ),
            {
                "owner": owner,
                "github": github_user_id,
                "id": uuid4(),
                "nonce": secrets.token_bytes(12),
                "ciphertext": secrets.token_bytes(32),
            },
        )
        connection.execute(
            text(
                "INSERT INTO runtime_private.browser_sessions "
                "(session_hash,user_id,csrf_hash,created_at,expires_at) "
                "VALUES (:session,:owner,:csrf,clock_timestamp(),"
                "clock_timestamp()+interval '1 hour')"
            ),
            {"session": secrets.token_bytes(32), "owner": owner, "csrf": secrets.token_bytes(32)},
        )

    # Work expiry must not erase the seven-day terminal-content export window.
    with store.admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE runtime_private.runs SET created_at=clock_timestamp()-interval '2 days', "
                "deadline_at=clock_timestamp()-interval '1 second',status='failed' "
                "WHERE run_id=:run"
            ),
            {"run": run_id},
        )
    key = secrets.token_bytes(32)
    storage = MemoryObjectReader(source.archive)
    output_dir = tmp_path / "encrypted-exports"
    export = export_owner(store.runtime_engine, principal, storage, output_dir, key)
    package_path = Path(export["path"])
    encrypted = package_path.read_bytes()
    assert encrypted.startswith(EXPORT_MAGIC)
    assert b"test-key-version" not in encrypted
    manifest, state, objects = _read_plaintext_package(_open_sealed(encrypted, key))
    assert manifest["secrets_included"] is False
    assert "provider_credentials" not in state["tables"]
    assert "github_user_tokens" not in state["tables"]
    assert "browser_sessions" not in state["tables"]
    assert state["tables"]["artifact_uploads"][0]["archive_sha256"] == source.archive_sha256
    assert objects[f"objects/{source.archive_sha256}.tar.gz"] == source.archive
    assert len(storage.reads) == 1
    assert package_path.stat().st_mode & 0o077 == 0

    quarantine_dir = tmp_path / "quarantine"
    checked = restore_check(package_path, store.runtime_engine, quarantine_dir, key)
    assert checked["state"] == "quarantined_validated"
    assert checked["mutations_enabled"] is False
    assert checked["quarantine"] == "private_temporary_stage"
    assert list(quarantine_dir.iterdir()) == []

    # Account deletion is recorded in the current journal and destroys the old export.
    RetentionService(store.runtime_engine, principal).delete_account()
    try:
        restore_check(package_path, store.runtime_engine, quarantine_dir, key)
    except PortableExportError as error:
        assert error.code == "export_invalidated_by_deletion"
    else:
        raise AssertionError("deleted account export was accepted")
    assert not package_path.exists()
