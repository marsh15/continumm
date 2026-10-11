# Implementation progress

The canonical scope is [plan.md](plan.md); acceptance criteria remain in
[todo.md](todo.md). The older blueprint is historical.

## 11 October 2026

- T01: complete. `make check` passed formatting, lint, strict typing and 10 tests;
  the settings target passed 7 tests; Python package and pinned Docker image built.
- T02: complete. Generated OpenAPI schema/types, React shell and secure static serving
  verified by `make check` (15 backend tests, 3 component tests), `make build`,
  a production-container HTTP probe and a rendered desktop browser check.
- T03: local preparation complete; hosted acceptance remains pending. The guarded
  startup probe, Render Free descriptor, PostgreSQL adapter and private Storage
  adapter are implemented. `make check` passed with 49 backend tests, 2 optional
  local integration skips and 3 frontend tests; explicit `make test-integration`
  passed both tests against disposable PostgreSQL 17.11. `make build` passed,
  and `make smoke-hosted` correctly refused this non-Render workstation.
  The temporary PostgreSQL container was removed. The operator confirmed Render
  and Supabase projects are not set up; [hosted evidence](../docs/evidence/t03-hosted.md)
  remains **NOT RUN**. Follow [hosting setup](../docs/hosting-setup.md) next.
- T04: local preparation complete; hosted proof remains pending. The pinned
  `vercel==0.11.6` adapter uses exact session handles, bounded output/file streams,
  immutable image configuration and explicit smoke IDs. `make check` passed with
  66 backend tests, 2 optional PostgreSQL skips and 3 frontend tests; `make build`
  passed. The disabled smoke target refused before SDK calls. No Vercel calls
  were made. [Hosted evidence](../docs/evidence/t04-hosted.md) remains **NOT RUN**.
- T05: local preparation complete; hosted acceptance remains pending. The Deno
  receiver verifies bounded raw bodies before parsing and acknowledges only a
  committed inbox result. Private migration metadata, column-limited ingress
  privileges and unsafe-role rejection are verified. The PostgreSQL 17.11
  integration passed, including duplicate/conflict/lost-ack and permission cases.
  `make check` passed with 68 backend tests, 3 optional integration skips,
  3 frontend tests and 14 Deno tests; `make build` passed. The disposable database
  container was removed. [Hosted evidence](../docs/evidence/t05-hosted.md) is
  **NOT RUN**.
- T06: local preparation complete; hosted acceptance remains pending. The fenced
  DB-ticket dispatcher checks pending work, backoff and capacity holds. The
  authenticated backend commits coalesced receipts without claiming a drain.
  Real PostgreSQL 17 tests cover concurrency, expiry during lock waits, role
  privileges, rollback/re-upgrade and the actual Edge-to-FastAPI path. `make check`
  passed with 83 Python tests, 4 expected environment skips, 3 frontend tests and
  21 Edge tests; `make build` passed. The separate T06 PostgreSQL integration
  passed, and its database/API fixtures were removed. The cron SQL remains
  unexecuted locally and hosted. [Hosted evidence](../docs/evidence/t06-hosted.md)
  is **NOT RUN**; `last_successful_drain_at` remains null until a real consumer
  commits a drain.
- T07: local persistence complete. Private runtime tables enforce explicit states,
  composite owner foreign keys, logical work uniqueness, attempt generations,
  and bounded append-only events. Principal-scoped repositories and forced RLS
  passed the dedicated PostgreSQL 17 integration, including cross-account denial
  and downgrade/re-upgrade. `make check build-package` passed with 83 Python
  tests, 5 expected integration environment skips, 3 frontend tests and 21 Edge
  tests. The separate T07 integration passed and its container was removed.
  See [runtime setup](../docs/runtime-persistence.md). Hosted gates remain open.
- T08 onward: pending their declared dependencies.

Implementation proceeds in backlog order. A local test or mocked adapter does not
prove a hosted gate. Checkpoints A and B remain open until the required account
configuration and real hosted evidence exist.

The requested implementation workers use `gpt-6-luna` with `xhigh` reasoning.

The foundation was additionally built from a fresh clone of committed revision
`0134299` using `make bootstrap build-package build-frontend`.
[Build evidence](../docs/evidence/foundation-build.md) closes the first item in
Checkpoint A; its hosted items remain open.

T03 review corrections include request-scoped credentials, private-origin and
object-key checks, end-to-end HTTP deadlines and byte caps, conservative unknown
mutation outcomes, verified cleanup, and preservation of reports after teardown
failure. The one existing Starlette TestClient deprecation warning does not fail
the suite. A transient Docker Hub 502 required a build retry.
