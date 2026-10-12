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

## Recovery and durable waits (T10)

Migration `20261011_06` separates each job's `failure_count` from its claim
generation and stores an explicit `replay_policy`, optional capacity resume
time, and the terminal failure class on each attempt. Existing jobs default to
`attention_on_unknown`; work may opt into `replay_safe` only when repeating the
activity after a lost worker cannot duplicate an external effect. Claim-time
recovery and `RuntimeRecoveryService.recover_due()` both record a crashed
attempt as `unknown` with `worker_lost`. The expired attempt consumes a new
generation, not the failure retry budget.

`recover_due()` uses PostgreSQL time and row locks to recover expired replay-safe
leases and timed capacity waits. It is safe to run repeatedly and concurrently;
run → step → job → attempt lock order fences each change, and recovery checks the
lease expiry and run deadline after obtaining the attempt lock and again before
commit. A replay-safe expired lease returns to the queue immediately. An
expired lease with `attention_on_unknown` ends in `needs_attention` without
replay. Due retry jobs remain queued with their persisted `available_at`; a
worker can claim them after that time, including after the API process restarts.

Transient read failures receive at most three total attempts. Their delay uses
full jitter with a one-second initial cap and a 60-second maximum; a later
provider `retry_after` takes precedence when it remains before the run deadline.
If the next retry would reach or pass the deadline, the job and run fail.
Missing credentials persist `waiting_credentials` without advancing the
failure counter. Resource exhaustion persists `waiting_capacity`; an explicit
provider resume time allows automatic recovery, while an unknown capacity
window waits for an operator-triggered later transition. Invalid input and
unsupported environments fail without retry. Unknown effects, conflicts,
corrupt artifacts, and lost model responses stop in `needs_attention`.

## Cancellation and dispatch boundary (T11)

Migration `20261011_07` adds owner-scoped resource reservation metadata,
dispatch intents with optional observations, and pending cleanup intents. It
also permits cancelled step and attempt labels. The cancellation service
requires the authenticated `Principal`; it does not check GitHub access.
Cancellation locks run → sorted steps → sorted jobs → attempts → dispatch
intents; it reads reservation metadata under the run lock, then reads database
time and marks the run terminal. It cancels queued/leased work, advances active
job generations, clears leases, and marks running attempts `unknown`. It
commits a `run.cancelled` event and pending
`stop_execution` and `observe_effects` cleanup intents in that same transaction.
The account's active-run uniqueness slot is released by the terminal run state.

`CancellationService.expire_due(limit=...)` applies the same terminalization
boundary to overdue runs, processing at most 100 owner-scoped rows per call.
It selects due runs with `SKIP LOCKED`, takes the same child and ledger locks,
then rechecks the deadline using database time. It records `run.expired` as a
system event, fences work, and releases the active-run slot. A cancel/expiry
race preserves whichever terminal commit obtains the run lock first.

An intent committed by `DispatchIntentService.authorize()` is the dispatch
authorization boundary. A worker may send that already-authorized request after
cancellation if it was paused between the intent commit and its remote call.
The T11 service performs no remote call and does not claim an immediate remote
stop. Cleanup intents are durable pending work for a later dispatcher; T11
does not claim that either cleanup action has completed. Cleanup handling is
deferred to T16 and T28.

Cancellation leaves resource reservation status and dispatch intent
observations unchanged. In particular, an `unknown` reservation remains
unknown. The reservation table stores metadata only; T11 does not allocate
resources or account capacity, which belongs to T16. A later internal
observation can update an existing dispatch intent and append an audit event
after cancellation, but cannot reopen the run or authorize another dispatch.
Replaying the same committed intent or observation is read-only and idempotent.
Adopting a committed dispatch receipt does not
authorize a second send; a later controller must treat it as the same
pre-cancellation authorization. The observation API accepts recorded metadata;
it does not contact a provider. Dispatch sending and observation reconciliation
remain later controller work.

Downgrading `20261011_07` drops the T11 resource, dispatch, and cleanup tables.
For compatibility with the earlier schema it maps cancelled steps to skipped
and clears the cancelled attempt failure label. The run remains cancelled and
its immutable `run.cancelled` event remains available to explain that downgrade.

The T10 integration fixture requires an administrative URL for a disposable
loopback PostgreSQL 17 database named `continuum_t10_test`. It creates and
removes the runtime roles and private schemas while proving restart-safe retry
eligibility, expired-lease recovery, ownership isolation, deadlines, concurrent
recovery, and explicit capacity waits.

```sh
CONTINUUM_T10_TEST_DATABASE_URL='postgresql://postgres:<password>@127.0.0.1:55440/continuum_t10_test' \
  make test-target TARGET=tests/integration/test_scheduler.py
```

The T11 fixture requires its own disposable PostgreSQL 17 instance, named
`continuum_t11_test`. It proves cancellation, expiry, dispatch/result races,
ownership isolation, late observations, and migration downgrade/re-upgrade.

```sh
CONTINUUM_T11_TEST_DATABASE_URL='postgresql://postgres:<password>@127.0.0.1:<port>/continuum_t11_test' \
  make test-target TARGET=tests/integration/test_cancellation.py
```

Run each integration target on its own disposable PostgreSQL 17 instance.
Although the fixtures use different database names, they create and remove the
same cluster-wide runtime roles, so separate databases on one server can still
conflict during setup or cleanup. Do not run these targets concurrently against
one server.

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

Checkpoint C is verified locally. The T09 fixture forms a real two-transaction
lock cycle using separate connections, observes both blocking edges through
`pg_blocking_pids`, captures PostgreSQL SQLSTATE `40P01`, and verifies rollback
before the transition retry commits exactly one result/event/next job. Separate
processes exercise competing claims and result submissions.
