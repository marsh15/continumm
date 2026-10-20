# Retention and deletion

Continumm gates retained run content with PostgreSQL time. A run is readable only while its account is active, its owner has not deleted it, its active-run deadline has not elapsed, and its content deadline is still in the future. The API exposes the active-run deadline and the final content expiry on each run snapshot.

An admitted run has at most seven days to reach a terminal state. Its initial content expiry is fourteen days after admission, covering the longest permitted active period plus seven days of terminal retention. The first transition to a terminal state resets the content expiry to seven days after that transition. Access closes at the active deadline or content expiry even when the cleanup worker has not run yet. A bounded drain then expires due runs and scrubs their retained database content.

Run deletion records an opaque receipt, blocks future work, terminalizes an active run, scrubs content, and queues external copies in one database transaction. Account deletion first marks the account deleting, revokes browser sessions and encrypted GitHub credentials, removes provider keys, and immediately blocks all owned run reads and work. A bounded worker scrubs account runs and marks the account deleted when local cleanup is finished. The receipt retains opaque IDs and a timestamp for 90 days; source, prompts, logs, and keys are excluded.

The run-content inventory is scrubbed across:

- Repository source archives, candidate patches, and checkpoint objects in private object storage.
- Pinned task context, issue snapshots, source receipts, model inputs, continuation context, model responses, and tool arguments.
- Validation results, test output, accepted activity payloads, timeline event payloads, approval manifests, and revision feedback.
- Accepted GitHub command payloads and provider or sandbox copies when a stable external reference is available.
- Raw webhook bodies under their independent deadline: 24 hours after processing, and never later than 72 hours after receipt.

Database rows keep only the run status, record identities, provenance, and existing digests needed for history and replay prevention. Reference pins never extend a mandatory deletion deadline. Committed object references remain charged until the configured deletion adapter confirms removal; cleanup outcome is idempotent and records `deleted`, `unknown`, or `unsupported`.

Remote deletion is deliberately reported from adapter evidence. The current application has no provider API for deleting stored model requests or provider backups, and no sandbox cleanup adapter is configured by default. Those items remain pending or become `unsupported`; Continumm does not describe them as erased. The same distinction applies to copies in provider backups and logs. Application cleanup can confirm deletion from the configured active object store, but it cannot promise immediate erasure from provider backups or logs.

`python -m continuum_api.worker --retention --max-jobs 25` runs one bounded local cleanup drain. It processes due database expiry, account deletion, webhook-body cleanup, and any cleanup adapter configured by the caller. Without a remote adapter, it leaves object, sandbox, and provider references pending for a later configured drain.

Private object deletion is explicitly enabled with `worker --retention --delete-objects`
and configured Supabase access. The production adapter checks the private bucket,
observes absence before sending an idempotent delete, and verifies authenticated
404 afterward. A lost response is reconciled by observation; unresolved outcomes
remain charged and queued. Without this option the worker performs DB-only
redaction and records pending cleanup. Provider-copy deletion remains unsupported
when no stable application-controlled remote object exists; backup erasure is
never inferred from an active-store receipt.
