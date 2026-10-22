# Bounded wake worker

The API can now start a bounded background drain after its authenticated wake
receipt commits. `CONTINUUM_ENABLE_WAKE_WORKER=0` remains the default in both
`.env.example` and the Render Blueprint. No hosted execution is claimed.

Apply migration `20261022_31` with the separate migration identity. Provision
`continuum_wake_worker` as a LOGIN NOINHERIT with its own password and no role memberships;
its connection goes only in `CONTINUUM_WAKE_WORKER_DATABASE_URL`. The migration
uses the NOLOGIN NOINHERIT `continuum_wake_worker_owner` for narrow SECURITY DEFINER
functions with forced row security. Neither the ingress, Edge dispatcher nor
wake receiver can consume an inbox row or write the completion watermark.
Remote worker connections require verified TLS. Never give this role raw table
write access or reuse a receiver/runtime connection. The migration identity must
be a superuser or a trusted member of the NOLOGIN owner role; no other inbound
memberships are accepted, and the owner must not inherit any parent role.

Enable the worker only after configuring the normal runtime connection, GitHub
App identity/private key, encrypted owner OAuth tokens, encryption keyring,
private object storage, sandbox accounting period and immutable profile images.
Model keys remain owner-scoped encrypted credentials. Missing configuration
leaves committed work pending; it cannot manufacture completion.

The host retains at most one asynchronous drain task per API process. PostgreSQL
provides a separate global, renewable 120-second lease across processes. Each
claim is bound to the latest accepted wake attempt; after claiming, worker
heartbeats use their own token and generation. A later Edge dispatch cannot
invalidate a still-current worker merely by advancing its dispatch generation.
A replacement worker can adopt an expired lease; the old token cannot consume
or finish work.

One default drain processes at most five inbox deliveries and two runs, with
at most two jobs per run and a 60-second outer deadline. It performs no idle
keepalive loop. Inbox parsing accepts only strict public issue-comment events;
normal owner, repository, command, credential, quota and approval controls still
apply. A persisted command result precedes consumption. Transient or unknown
outcomes remain pending. Expired job leases go through normal recovery before
workflow execution. Cancellation and cleanup failures cannot claim success.

The wake API can report `scheduled/not_started`, `running/running`,
`partial/partial`, or `drained/completed`. Scheduling and HTTP 202 are receipts,
not completion. Only the fenced SQL finish transaction can update
`last_successful_drain_at`, after checking for remaining inbox deliveries and
due workflow jobs. A completed watermark means that actionable queue snapshot
was drained; it does not mean every run finished, publication succeeded, or all
external effects are reconciled. Future-dated jobs wait for a later due tick.

The existing five-minute database dispatcher now considers due workflow jobs
and expired leases as well as inbox deliveries. Capacity holds and persistent
backoff still apply. New work after a completed wake receives a new wake ID.
Shutdown cancels retained tasks and awaits bounded cleanup; abrupt process death
relies on persisted leases and the next due dispatch.

Automatic public status-comment delivery still needs a separate bounded effect
drain. The existing marker-based publisher must reconcile unknown creates
before retrying. Publication remains subject to its explicit execution controls.
Do not use successful inbox/job draining as evidence that comments or PRs were
published. The normal admission flag also remains closed: use an isolated,
network-restricted qualification deployment for synthetic admission exercises.

Local checks include worker orchestration and host lifecycle unit tests, actual
PostgreSQL role/fence/consumption tests, and an owned subprocess death/reclaim
test. Hosted cold starts, quota holds, provider execution and deployed recovery
remain qualification work in the [completion guide](../tasks/completion-guide.md).
