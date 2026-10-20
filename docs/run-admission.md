# Atomic run admission (T36)

Public admission stays disabled by default with
`CONTINUUM_ENABLE_PUBLIC_ADMISSION=0`. Both the HTTP boundary and the admission
service enforce the gate. Enabling it is not deployment acceptance: T34's
source-preparation/worker composition and live demonstration remain deferred.
An admitted run has a durable initial `source_preparation` job; this change does
not register that job in the publication-only worker or make provider calls.

## API and identities

`POST /api/v1/runs` requires an authenticated owner, valid Origin/session CSRF,
and a 16–128 character printable ASCII `Idempotency-Key`. The strict JSON body is
bounded to 64 KiB; task text is bounded to 16 KiB of valid UTF-8. For example:

```json
{
  "source": {
    "kind": "free_text",
    "registration_id": 1,
    "request_id": "00000000-0000-4000-8000-000000000001",
    "task": "Add the missing boundary check."
  }
}
```

An issue source uses `kind: "issue"` and also supplies `issue_number`.
An optional `credential_id` selects an owned active saved key matching the
registered provider; otherwise repository defaults apply. The service freshly
checks the numeric GitHub actor's write rights and App access. Issue sources
also fetch and verify an ordinary issue, rejecting pull-request payloads.
The stored eligibility receipt must match the registered profile and the fresh
base commit. Network reads happen outside the short admission transaction.

The transaction rechecks account status, defaults version and selected key, then
commits the run, immutable source/profile/provider/model pins, submission identity,
initial work/event, HTTP idempotency entry and queue seat together. Missing usable
default keys create `waiting_credentials` on the same seven-day run. Explicitly
selected unavailable keys reject. Existing credential binding resumes a waiting
run without creating another admission or consuming a second queue seat.

HTTP keys expire after 24 hours. The source `request_id` has a separate durable
identity, so retrying it after cache expiry recovers the original run. A changed
body under the same key/request identity conflicts. A new request for an already
active admitted issue returns the existing run with its pinned settings. Free
text uses request identity, not text similarity. An old controlled experiment
without admission pins cannot be adopted; its owner must finish/cancel it before
new public admission. Existing account/issue uniqueness still prevents duplicates.

The trusted continuation method locks and validates T35's original comment
snapshot and claims it in this same transaction. A full queue or another failed
check rolls back both run creation and claim. Public comment verification and
command handling remain T37/T38 work; no new public continuation endpoint exists.

## Queue and dispatch limits

Ten seats bound admitted nonterminal runs globally, including those waiting for
credentials. One active run per account remains enforced by the existing database
index. A terminal transition releases the admission seat in its transaction.
Unknown sandbox/model outcomes retain their existing charges and resource holds;
releasing a run's queue seat does not release those resources.

This admission queue applies to the admission service and its shared channel
paths. The older trusted experiment helper creates no public admission record
and is not an HTTP creation path. Sandbox allowance, billing-period execution
limits, operator/public pools and model budgets continue to reserve at their
existing T16/T26 dispatch boundaries. Admission itself creates no sandbox
allocation and sends no model request.

Runtime model fields remain uninitialized until trusted first-turn composition;
admission selections/task context live in separate immutable pins. This avoids
partially initializing the model/schema pins required by T34. Task and issue
snapshots must join the T40 content-expiry/deletion inventory; they are not
included in ordinary admission responses or events.

## Migration 23 operator bootstrap

Provision the function-owner role through the privileged migration/admin channel
before upgrading from revision 22:

```sql
CREATE ROLE continuum_admission NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB
  NOCREATEROLE NOREPLICATION NOBYPASSRLS;
-- For a non-superuser operator, substitute its actual migration role name:
GRANT continuum_admission TO your_migration_role;
```

Use the same designated operator for upgrade and downgrade. A superuser migration
channel needs no membership grant. The role must have no parent memberships or
members other than that operator, no login/elevated attributes, and no permanent
schema CREATE privilege. Never grant it to runtime, webhook, wake or browser
identities. The migration validates these prerequisites before changing schema;
it briefly grants schema CREATE for function ownership transfer and revokes it
within the migration transaction.

The role owns fixed-search-path `SECURITY DEFINER` queue functions and receives
only queue-table privileges plus narrow principal-bound run/submission access.
Runtime cannot read or modify the global seats directly. The global queue never
requires unrestricted runtime reads of other accounts' runs.

Before rollback, disable admission and preserve needed admission/input metadata.
Downgrade removes the new admission tables/functions and revokes their grants,
while leaving the preprovisioned role in place. It restores the earlier
repository/issue check as `NOT VALID`, preserving existing repo-only free-text
run history while rejecting new rows unsupported by the older schema. It does
not rewrite existing runs or synthesize missing task inputs after re-upgrade.
Hosted migration/bootstrap has not been demonstrated.

## Verification

Final checks passed 617 Python tests (154 optional database skips), formatting,
lint, strict typing across 89 source files and package build. OpenAPI generation
and frontend checks passed, including 27 component tests, 11 browser tests and
the production frontend build.

Nineteen PostgreSQL cases passed across admission, credential lifecycle, model
limits, review/rework and publication. The admission case exercises eleven
concurrent owners against ten seats, exact seven-day deadlines, waiting-key
capacity, zero sandbox allocation at admission, private seat access, active-issue
adoption, durable replay after HTTP-cache expiry, atomic continuation rollback,
free-text identity and selected-key binding. A coordinated revocation/admission
race verifies the shared advisory-before-credential lock order.

All external adapters in these checks are controlled fixtures. No live provider
requests, public deployment or hosted migration were performed. T34 remains
deferred. T37 and later tasks were not started when the owner requested a stop.
