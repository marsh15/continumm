# External-operation ledger

T30 adds an internal, owner-scoped ledger for logical remote mutations. It does
not send GitHub requests or expose a browser endpoint. Publication adapters are
implemented in later tasks.

## Dispatch and recovery

A worker prepares a stable operation key and bounded desired-state metadata
under a current execution lease. Reusing the key with different intent conflicts.
The ledger commits dispatch authorization before a caller sends a remote mutation.
Only the caller receiving a new dispatch authorization may send; adopting an
existing authorization never grants permission to send again. A prepared intent
can move to a new current attempt because no send was authorized; a dispatched
operation retains its original attempt identity.

Database deadlock/serialization retries contain database work only. Remote I/O
must occur outside that retry boundary, with generic mutation retries disabled.
A crash after authorization but before a known response leaves an uncertain
effect; it cannot justify a replacement non-idempotent mutation.

The states are `prepared`, `dispatching`, `confirmed`, `definitely_failed`,
`unknown`, and `conflict`. Read-only reconciliation may append observations and
adopt an exact matching remote result. Observations must supply the persisted
operation ID, attempt, generation and request digest via a matching receipt. An
adopted receipt permits observation while keeping `dispatch_allowed=False`. A
negative lookup does not prove an
outstanding request cannot still appear. Conflicting remote state requires
attention instead of a force overwrite or another creation.

Cancellation and dispatch serialize on the run lock. A cancelled run cannot
receive a new dispatch authorization. Previously authorized requests may still
finish; matching observations remain recordable without reopening the run.

## Storage and trust

Migration `20261017_19` adds private operation and append-only observation tables.
Logical identity is scoped to owner, run and operation key. Forced row-level
security isolates owners; the runtime role cannot change intent or delete ledger
records. Observation metadata carries actor provenance. Metadata is bounded and
must contain identities and evidence references, never credentials or full source
archives.

Approval-bound operations recheck their stored approval identity, manifest digest,
current revision, requested operation membership and deadline at authorization,
including a final clock check after authorization writes. The ledger is an internal
persistence boundary: trusted publication adapters must also obtain fresh GitHub
rights and validate the exact approved remote action before sending.

## Verification and limits

Run `make test-target TARGET=tests/integration/test_external_ledger.py` with
`CONTINUUM_T22_TEST_DATABASE_URL` pointing to disposable loopback PostgreSQL 17
named `continuum_t22_test`. The fixture resets its private schemas and roles;
never point it at an application database.

Controlled fake-remote tests prove the local dispatch and reconciliation policy.
They do not establish exactly-once behavior for arbitrary remote systems. Live
GitHub publication and hosted verification remain later work. This ledger does
not provide a force-retry action for unknown creates.
