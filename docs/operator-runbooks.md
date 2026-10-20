# Operator diagnostics and controls

T42 provides an allowlisted, read-only `/operations` view and a local
administrative CLI. The view shows admission state, active-run counts,
unresolved external effects, uncertain sandbox allocations, pending webhook
delivery age, artifact-capacity accounting, suspended accounts, and recent
control audit records. It never returns prompts, issue/comment text, event
payloads, provider credentials, GitHub tokens, session hashes, or artifact
content.

The runtime database login can only execute the admission predicate. A separate
`continuum_operator` login reads the safe metadata functions and audit records
and executes the audited stop, suspension, reactivation, and credential-revocation
functions. It has no direct access to credential/session tables, webhook bodies,
run content, or mutable runtime state. The browser receives only its ordinary
same-origin session cookie; the operator database URL and credentials stay in
the server environment.

## Provisioning

Before migration `20261020_28`, create `continuum_operator` through the database
operator channel. It must be a `LOGIN NOINHERIT` role with no elevated flags or
role memberships. Set its password interactively; never place it in SQL files,
shell history, browser configuration, or source control. Apply the migration
using the separate migration connection.

Configure these application environment variables in the server secret store:

- `CONTINUUM_OPERATOR_DATABASE_URL`: TLS connection string for the dedicated
  `continuum_operator` role.
- `CONTINUUM_OPERATOR_GITHUB_USER_IDS`: comma-separated immutable numeric
  GitHub user IDs, such as `12345,67890`. Leave it unset or empty to disable
  operator access.

The web view also requires the normal authenticated GitHub session. It takes the
actor ID from that session and applies the deployment allowlist. It does not
accept an actor ID from request parameters or headers. Open `/operations`
directly after signing in with an allowlisted account. No operator link is
shown to ordinary users.

For local administrative work, run the CLI from a trusted workstation with the
same two operator settings. Its actor ID is required and must be on the same
allowlist:

```text
python -m continuum_api.runtime.operator_actions --actor-github-user-id 12345 inspect overview
python -m continuum_api.runtime.operator_actions --actor-github-user-id 12345 inspect unknown-effects
python -m continuum_api.runtime.operator_actions --actor-github-user-id 12345 inspect uncertain-allocations
python -m continuum_api.runtime.operator_actions --actor-github-user-id 12345 inspect inbox-lag
python -m continuum_api.runtime.operator_actions --actor-github-user-id 12345 inspect audit
```

## Emergency controls

Stop new admission when provider quota, database availability, storage pressure,
or a security issue makes new work unsafe:

```text
python -m continuum_api.runtime.operator_actions --actor-github-user-id 12345 admission stop --reason database_pause
```

Admission checks the global flag and account status inside the same PostgreSQL
transaction that inserts a new run and reserves its quota. The shared transaction
also serializes a concurrent stop or suspension change. Existing idempotent
replays remain readable; the stop does not force-complete or retry a run.

After inspecting the cause and confirming capacity is safe, reopen admission:

```text
python -m continuum_api.runtime.operator_actions --actor-github-user-id 12345 admission resume --reason other
```

To respond to a compromised account, suspend it. This revokes browser sessions
and its stored GitHub user token; BYOK provider credentials remain individually
revocable through the credential command below. Reactivation sets the account
back to active but does not restore revoked sessions or tokens, so the user must
sign in again.

```text
python -m continuum_api.runtime.operator_actions --actor-github-user-id 12345 account suspend 24680 --reason security
python -m continuum_api.runtime.operator_actions --actor-github-user-id 12345 account reactivate 24680 --reason security
```

To revoke a specific saved or per-run provider key after a provider-key
compromise, use its credential UUID from trusted account metadata. The command
clears encrypted bytes, marks the credential revoked, and records the actor and
reason. It does not expose the key.

```text
python -m continuum_api.runtime.operator_actions --actor-github-user-id 12345 credential revoke 8f1150a0-5ea5-4d13-8f6b-8c76e2e87dc8 --reason security
```

Every successful state change writes one immutable audit event in the same
transaction. Reasons come from a fixed code list; the CLI accepts no free-text
notes that could accidentally contain private content. Repeated requests that
already match the stored state make no change and create no duplicate event.

## Incident runbooks

| Situation | Inspect and respond |
|---|---|
| Provider quota exhaustion | Stop admission, inspect active runs and uncertain effects, check the provider account through its owner, then resume only after limits are available. Do not dispatch jobs to bypass the pause. |
| Database pause or outage | If the database is unavailable, do not treat the stop command's failure as success. Use the provider's documented recovery path, then set the stop after the database returns if admissions should remain paused. Readiness is not a claim that an unavailable database is healthy. |
| Failed webhook delivery | Inspect pending inbox age and delivery IDs. Follow [failed-delivery recovery](github-delivery-recovery.md); a history gap means users must submit a new command. Do not infer approval from an old comment. |
| Lost sandbox allocation | Inspect uncertain allocation IDs and their last observation time. Check the provider using its operator console and record the external evidence through the supported reconciliation path. Unknown allocations retain their quota hold until termination is observed. |
| Uncertain publication | Inspect the effect ID and run ID, then use the authenticated reconciliation flow in [external-effect attention](external-attention.md). An unknown POST is not proof that GitHub did nothing; do not retry it from the operator tools. |
| GitHub or provider key compromise | Suspend the account to revoke sessions and GitHub access. Revoke the affected BYOK credential by UUID. Ask the owner to issue a replacement through the normal credential flow. |
| Corrupted checkpoint artifact | Stop admission if the corruption affects shared storage. Follow [checkpoint storage recovery](checkpoint-storage.md) to validate the object and attach a verified checkpoint; do not force-complete a run. |
| Storage pressure | Inspect charged and configured artifact bytes. Use the bounded retention cleanup procedure in [retention](retention.md). Do not delete objects directly while references remain. |
| Account deletion | Use the account's authenticated deletion request and follow [retention](retention.md). Suspension is not a substitute for content deletion. |
| Migration failure | Keep admissions stopped, preserve the redacted migration error code, and follow the migration rollback procedure for the exact revision. Never edit the migration version row by hand. |
| Restore | Restore into quarantine, apply deletion and expiry journals, revoke restored sessions, invalidate old execution ownership, and inspect external effects before enabling mutations. See the restore requirements in [the implementation plan](../tasks/plan.md#143-backup-and-restore). |

The operator API has no run-completion, remote-mutation retry, approval, or
publication endpoint. Observation and explicit revocation are the only supported
recovery actions in this surface.
