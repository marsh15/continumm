# Runtime persistence

Migration `20261011_03` adds the first runtime tables in the private
`runtime_private` schema: users, runs, steps, jobs, attempts, and events. The
`users` row is the minimal account identity referenced by runtime ownership
foreign keys. It stores the internal UUID and immutable GitHub numeric ID; it
does not implement login sessions or credential storage.

Create the dedicated `continuum_runtime` login before applying the migration.
It must have `LOGIN`, `NOINHERIT`, and no elevated attributes or role
memberships. The migration grants it schema usage, account-scoped table access,
and database connect. T05 and T06 role prerequisites also apply when upgrading
from the current base revision. Keep the runtime login separate from the
migration and webhook ingress credentials.

Every runtime table has forced row-level security. The application constructs
`RuntimeRepository` inside a SQLAlchemy transaction and passes an authenticated
`Principal(account_id=...)`. The repository binds that account for the whole
transaction, includes it in owner queries and inserts, and rejects switching
principals before the transaction ends. Use an account identity obtained from
the server-side authentication boundary; never take the principal ID from a
browser request body.

PostgreSQL owns duplicate-work rejection: an account cannot have two active
runs, a run cannot repeat a step logical key within one revision, a step has one
job, a job has at most one attempt per generation, and each run event sequence
is unique. A later run revision may reuse a step key. Event rows are append-only
for the runtime role, and payloads must be JSON objects no larger than 8 KiB as
rendered by PostgreSQL. The sequence column enforces uniqueness and positivity;
T08 allocates each sequence while holding the run row lock.

## Atomic activity results (T08)

`TransitionService` is an internal transaction boundary. It locks the run, then
the step, then the job, and commits the accepted result, run/step/job statuses,
the next event sequence, and an optional next step/job in one PostgreSQL
transaction. It performs only database work inside that transaction. The
runtime API does not expose this as a generic execution endpoint; worker
execution must wait for T09 claim and fencing checks.

Accepted results are immutable, scoped to one owner/run/step/job, and limited
to 64 KiB of compact UTF-8 JSON input. PostgreSQL checks the rendered JSONB
value again. The intent digest includes the result, requested run status, and
optional next step key/activity/revision. A retry with the same intent returns
the stored result ID, event sequence, and next job; a changed result or
transition plan for that logical step raises a conflict. Run, step, job, event,
and result changes roll back together on any database constraint failure.
An activity result may leave the run running or move it to a wait/failure state;
only the later publication-observation boundary may mark a run `succeeded`.

The transition service makes at most three attempts at the full database-only
transaction for PostgreSQL deadlock or serialization errors, with short random
backoff. It does not retry connection loss or uncertain commit acknowledgments;
the caller retries the same intent to adopt an already committed result. T09
adds generation fencing and attempt finalization.

## Claims and fencing (T09)

`QueueService` requires an explicit `Principal` and claims one eligible job per
call. Claim locks the run, then its step, job, and prior attempt by stable ID,
then reads PostgreSQL `clock_timestamp()` for due-work and lease checks. A
successful claim changes a queued run to running, increments the job generation,
creates one attempt with an immutable UUID and lease owner, and grants a
60-second lease. Workers heartbeat every 15 seconds. An expired job can be
reclaimed under the same lock order; its old running attempt becomes `unknown`
before the next generation is issued.

Heartbeats, bounded attempt checkpoints, and final accepted results require the
`ExecutionLease` returned by a claim. They lock run → step → job → attempt and
check generation, owner, permitted state, run deadline, and lease expiry using
database time after locks. Heartbeat expiry cannot be revived. Final writes
recheck the original lease after their last potentially blocking write and
before commit. An exact accepted-result replay returns the stored receipt
without changing state, even if the supplied lease is now stale. T09 does not
expose worker execution through an HTTP route; activity dispatch must remain
behind these checks.

`attempt_checkpoints` stores only small JSON metadata or intermediate results,
up to 32 KiB. Full workspace archives remain in the later artifact/checkpoint
slice. Legacy attempts and T08 accepted results retain null lease identity on
migration; T09 does not invent historical owners or generations.

The T07 integration fixture requires a disposable loopback PostgreSQL 17
database named `continuum_t07_test`; the T08 fixture uses
`continuum_t08_test`. The URL must be administrative enough to create and drop
roles and schemas. Each fixture creates and removes its test roles and private
schemas. For example:

```sh
CONTINUUM_T07_TEST_DATABASE_URL='postgresql://postgres:<password>@127.0.0.1:55479/continuum_t07_test' \
  make test-target TARGET=tests/integration/test_runtime_schema.py
```

```sh
CONTINUUM_T08_TEST_DATABASE_URL='postgresql://postgres:<password>@127.0.0.1:55480/continuum_t08_test' \
  make test-target TARGET=tests/integration/test_transitions.py
```

The T09 integration fixture requires an administrative URL for a disposable
loopback PostgreSQL 17 database named `continuum_t09_test`. It creates and
removes its test roles, private schemas, and trigger fixtures.

```sh
CONTINUUM_T09_TEST_DATABASE_URL='postgresql://postgres:<password>@127.0.0.1:55481/continuum_t09_test' \
  make test-target TARGET=tests/integration/test_leases.py
```

Checkpoint C still needs a real two-transaction deadlock experiment. The T09
fixture verifies run-first skip-locked selection and injects PostgreSQL SQLSTATE
`40P01` to prove transaction rollback and bounded retry; that injection does not
create a deadlock between two transactions.
