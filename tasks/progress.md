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
- T05: ingress implementation in progress after the verified T04 local slice.
- T06 onward: pending their declared dependencies.

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
