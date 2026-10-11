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
T08 adds the locked per-run allocator.

The integration fixture requires a disposable loopback PostgreSQL 17 database
named `continuum_t07_test` and an administrative URL that can create and drop
roles and schemas. It creates and removes its test roles and private schemas.
For example:

```sh
CONTINUUM_T07_TEST_DATABASE_URL='postgresql://postgres:<password>@127.0.0.1:55479/continuum_t07_test' \
  make test-target TARGET=tests/integration/test_runtime_schema.py
```
