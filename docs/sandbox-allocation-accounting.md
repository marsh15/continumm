# Sandbox allocation accounting

T16 reserves managed sandbox capacity before creation. The reservation records
the owner, run, job, attempt, generation, stable provider name and maximum
lifetime. A one-time dispatch claim precedes the provider request; retrying an
unknown creation inspects that identity instead of creating another sandbox.

One allocation may hold the global slot, including an unknown create or stop.
Neither an elapsed local timeout nor a provider 404 proves that a delayed create
will never arrive. Only a positive terminal observation for the recorded
allocation can free concurrency. Session identities are checked before cleanup;
the service never resumes a sandbox or uses get-or-create.

Wall-time reservations across allocations are capped at 900 seconds per run,
including retries and revisions. CPU accounting conservatively charges the full
maximum at two vCPUs. Stopping a dispatched allocation can free concurrency but
does not refund that reserved compute. A proven undispatched operation can be
refunded under the current fence.
There is also a protected reconciliation path for a recorded reservation with
no create authorization. It can refund that prior intent after worker loss or
cancellation; its row lock and compare-and-set prevent a racing create from
dispatching afterward. It never refunds a claimed or uncertain create.

Provider billing periods must be explicitly configured with their real bounds.
Public and operator pools have separate application ceilings of four and one
CPU-hours respectively; they cannot borrow from each other. Unknown allocations
carry a conservative charge into a new configured period before new admission.
No configured current period means no new allocation. The period is accounting
metadata, not provider request identity: an unchanged replay after rollover
observes the original allocation and books the current charge once, preserving
the original period for audit.

The runtime envelope is separate from T04's spike settings. Legacy unaccounted
CLI mutations are disabled before provider construction. Passive observation is
retained; operators can use provider controls for protective cleanup while an
accounted operator path is being integrated.

These application limits do not establish hosted provider availability. Local
tests use fake provider operations and a dedicated disposable PostgreSQL 17
instance. No real allocation, image publication or hosted smoke is performed by
this implementation task.

Source: [Vercel Python SDK creation, session and lifecycle contracts](https://vercel.com/docs/sandbox/python-sdk-reference).

## Operator preparation

Apply migration `20261012_10` through the migration account. Configure a provider
period with real UTC start/end bounds, then create its zeroed `public` and
`operator` rows in `runtime_private.sandbox_period_usage`. Period configuration
uses the migration/operator account; the runtime role cannot insert or change
period definitions. Pass that period ID to `SandboxAllocationService`. Multiple
overlapping current periods, an ended period, or missing pool rows fail closed.
Use a pinned image digest and the separate `SandboxAllocationLimits` envelope;
the service consumes the duration actually reserved by the database.

## Local verification

```sh
make test-target TARGET=tests/unit/test_sandbox_allocation_service.py
CONTINUUM_T16_TEST_DATABASE_URL='postgresql://<local-admin>@127.0.0.1:<port>/continuum_t16_test' \
  make test-target TARGET=tests/integration/test_sandbox_allocations.py
```

The integration fixture requires a dedicated disposable PostgreSQL 17 instance,
not just a database in a shared cluster: it resets cluster-wide runtime roles.
Its combined proof passes with unknown-create replay, exact terminal evidence,
one global slot, owner RLS, concurrent admission, period carry and exhaustion,
lease expiry while blocked on a row, ended-period dispatch refusal, identical
undispatched rearming, and new-supervisor cleanup without a provider call.

The focused sandbox service, SDK and legacy CLI targets pass 41 tests. The full
Python suite passes 267 tests with 15 optional integration environment skips;
T16's PostgreSQL proof was run separately. Formatting, lint, typing and package
build pass. Operator run counts and broader admission rules remain T30 work;
typed command/file tools remain later tasks. Actual hosted allocations remain
unverified.
