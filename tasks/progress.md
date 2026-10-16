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
- T08: local atomic activity transitions complete. Accepted result, state, event,
  and next job commit together in run/step/job lock order. Exact replay adopts
  a receipt; changed intent conflicts. Dedicated PostgreSQL 17 tests passed late
  failure rollback, terminal rejection, T07 regression, and migration rollback/
  re-upgrade. Unit checks cover bounded deadlock/serialization retries and no
  connection-loss retry. `make check build-package` passed with 87 Python tests,
  6 expected integration environment skips, 3 frontend tests and 21 Edge tests.
  Both owned database fixtures were removed. The internal service remains
  unconnected to execution until T09 fencing; hosted gates remain open.
- T09: local claims, heartbeats and fencing complete. Principal-scoped batch-one
  claims issue monotonically increasing generations and immutable attempt
  identities. Intermediate checkpoints and final results require a current
  execution lease; database time is checked after locks and before commit.
  Dedicated PostgreSQL 17 tests passed independent-process competitors, stale
  writes, expiry during lock waits/heartbeat/checkpoint/result writes, and legacy
  migration preservation/rollback. The adapted T08 PostgreSQL integration also
  passed. `make check build-package` passed with 90 Python tests, 7 expected
  integration environment skips, 3 frontend tests and 21 Edge tests. The final
  checkpoint regression passed separately; its fixture container was removed.
  Checkpoint C is complete locally. A follow-up PostgreSQL integration formed
  an actual two-transaction lock cycle, captured `40P01`, proved rollback before
  retry, and verified one committed result/event/next job plus receipt adoption.
  The focused integration, formatting, lint and typing passed; its owned
  fixture was removed.
  Hosted gates remain open.
- T10: local recovery, retries and durable waits complete. Failure counts are
  distinct from attempt generations; retries persist `available_at`, honor
  retry-after/deadlines, and stop after three classified transient failures.
  Recovery requeues only explicitly replay-safe work; unknown effects move to
  needs attention. Capacity waits without a known resume time remain manual.
  Separate isolated PostgreSQL 17 targets passed T10 recovery, T08 transitions
  and T09 leases/deadlock regression. Each target uses its own disposable server
  because fixture role names are cluster-wide. Final formatting, lint, typing,
  101 Python tests (8 expected integration environment skips), and package build
  passed; unchanged frontend/Edge checks passed with 3/21 tests. The owned
  fixture was removed and unrelated containers preserved. Hosted gates remain
  open. See [runtime setup](../docs/runtime-persistence.md).
- T13 onward: pending their declared dependencies.

## 12 October 2026

- T11: local cancellation and deadline terminality complete. Owner cancellation
  requires no GitHub reauthorization; cancellation/expiry commit lease and
  generation revocation, terminal state, event and pending cleanup intents
  together. The account slot is released while unresolved reservations and
  dispatch observations remain independent. Late observations and exact receipt
  adoption cannot reopen execution. Bounded database-time expiry uses the same
  terminalization path; competing cancellation/expiry preserves the first
  terminal outcome. The isolated PostgreSQL 17 target passed ownership, stale
  writes, dispatch/result/cancellation races, before/due deadline checks, and
  populated migration downgrade/re-upgrade. `make check build-package` passed
  with 101 Python tests, 9 expected integration environment skips, 3 frontend
  tests and 21 Edge tests. Cleanup intents remain pending for later adapter
  dispatch; no remote stop or mutation was performed. Hosted gates remain open.

- T12: local run timeline complete. Owner-scoped snapshots and event pages use
  consistent database snapshots; decimal-string SSE cursors replay without JS
  precision loss. Typed privacy projections, access rechecks, bounded streams,
  disconnect cleanup, and gap/reset recovery are verified. The default identity
  boundary remains closed until the identity slice. `make check build-frontend
  build-package` passed with 119 Python tests, 10 expected integration environment
  skips, 16 frontend tests and 21 Edge tests; the isolated PostgreSQL 17 event
  target passed. A loopback-only app with a trusted test identity rendered a real
  PostgreSQL-backed waiting-credentials snapshot and two committed runtime
  events; status refresh and connected timeline were checked in the browser.
  No public auth, model or GitHub execution was enabled. Checkpoint D's process
  loss and paused-worker demonstrations are next; hosted gates remain open.

- Checkpoint D: complete locally. Separate real SIGKILL and SIGSTOP/SIGCONT
  worker tests passed against PostgreSQL 17. Generation 2 reloads and validates
  generation 1's committed checkpoint before deriving its accepted result;
  a resumed stale worker receives `LeaseLost` for intermediate and final writes.
  Controlled database lease expiry is documented separately from the production
  60-second lease. The rendered seven-event timeline shows recovery, rejection
  and accepted progress. The process target passed 2 tests and the focused event
  target passed; formatting, lint, typing, 119 Python tests (12 expected
  integration environment skips) and package build passed. The idle-SSE
  connection acknowledgment fix was separately committed and pushed. All owned
  apps/clusters and private retained credentials were removed.
  [Local evidence](../docs/evidence/checkpoint-d-local.md).

- T13: local repository preflight implemented. Numeric repository/commit/profile
  identities, fail-closed trusted authorization, fixed-origin bounded metadata
  reads, strict Python/npm profiles, and Python dependency closure validation
  precede any allocation. Migration 08 adds durable five-check/hour admission,
  one active reservation, ten-minute technical caching, DB-clock stale-check
  fencing, and owner RLS. Dedicated PostgreSQL 17 verification passed, including
  concurrent reservations and migration rollback. Full local checks and package
  build passed; final focused regression totals are in the preflight evidence.
  Live GitHub App verification remains pending T33.
  [Contract and local verification](../docs/repository-preflight.md).

- T14: canonical source archives complete locally. Deterministic strict USTAR
  inside gzip preserves permitted executable modes; separate archive and
  versioned tree SHA-256 values are verified without filesystem extraction.
  Bounded single-member inflation precedes raw-header checks; links, sparse and
  extension records, unsafe/colliding paths, cache directories, corrupt gzip,
  noncanonical metadata and malformed endings fail closed. The archive target
  passed 51 tests; Python formatting, lint, typing, regression tests and package
  build passed. [Format contract](../docs/canonical-archives.md).

## 13 October 2026

- T15: checkpoint storage complete locally. Immutable upload intents reserve
  bytes before dispatch, fresh access checks precede every source read, and
  verified objects attach atomically with accepted results/events under fencing.
  Unknown and orphaned uploads remain charged; stale workers cannot attach them.
  A dedicated PostgreSQL 17 proof covers parent chains, lost acknowledgments,
  rollback, quota contention, owner isolation, corruption attention and revision
  07 compatibility. The focused targets passed 63 tests, the complete Python
  suite passed 243 tests (14 optional integration environment skips), and
  formatting, lint, typing and package build passed. Storage HTTP clients now
  use transport-only test injection without ambient credentials or proxies.
  [Local evidence](../docs/checkpoint-storage.md). Hosted Storage remains pending.

- Checkpoint E: complete locally through the T15 PostgreSQL/HTTP fixture proof.
  Exact reconstruction, rejected uncommitted uploads and corrupt/missing-object
  attention are covered without substituting local files for committed state.
  [Evidence](../docs/evidence/checkpoint-e-local.md). Hosted restoration is pending.

- T16: managed allocation accounting complete locally. Migration 10 records
  stable attempts, explicit provider-period pools, full compute reservations and
  one global slot. Unknown creates/stops retain capacity; exact terminal evidence
  frees concurrency without refunding dispatched compute. Protected cleanup
  recovers never-authorized reservations after worker loss. Period rollover
  preserves resource identity and carries charge once. A dedicated PostgreSQL
  proof passed; 41 focused tests and 267 Python regression tests passed (15
  optional integration environment skips), along with formatting, lint, typing
  and package build. Legacy unaccounted CLI mutations now fail closed.
  [Local contract and verification](../docs/sandbox-allocation-accounting.md).
  Hosted allocation is still pending.

- T17: Python profile complete locally. The pinned CPython 3.13.12 image recipe
  contains isolated trusted setup and bounded wheel validation. The driver binds
  committed source to eligibility, fences each remote mutation, verifies active
  session deny-all egress before local wheel installation and pytest, and uses
  protected allocation cleanup. Sixteen profile tests, fourteen bootstrap tests,
  eleven SDK tests and the dedicated PostgreSQL allocation proof passed. The full
  Python suite passed 300 tests (15 optional integration environment skips), with
  formatting, lint, typing and package build passing. Image publication and the
  actual managed fixture are pending; cumulative run output accounting belongs
  to T30. [Contract and verification](../docs/python-execution-profile.md).

## 14 October 2026

- T18: Node profile complete locally. Node 24.21.0/npm 11.19.0 run through
  trusted script-disabled setup, source/manifest binding, per-action allocation
  fencing, verified deny-all test egress and protected cleanup. Installed content
  checks include bounded scans, links, hooks and detected native/browser inputs.
  Thirty-two profile tests and twenty-six bootstrap tests passed. The full Python
  suite passed 359 tests (15 optional integration environment skips), along with
  formatting, lint, typing and package build. Independent review found no local
  blocker. Docker was unavailable; image build/publication and managed fixture
  acceptance remain pending. [Contract](../docs/node-execution-profile.md).

- T19: typed source tools and policy complete locally. Strict requests and trusted
  permission intersections govern verified in-memory archives; patches create
  atomic candidates, while baseline tests, harnesses, manifests and CI stay
  protected. Known credential paths are filtered from inspection. Harness
  discovery is bounded and fails closed on unsupported forms. Twenty-eight
  policy tests and nine source tests passed, together with 400 Python regression
  tests (15 optional integration environment skips), formatting, lint, typing
  and package build. [Contract](../docs/source-file-tools.md).

## 15 October 2026

- T20: immutable candidate validation complete locally. Preparation preserves the
  original Git eligibility receipt and rechecks candidate lineage and protected
  tests. Baseline and candidate execute in sequential fresh allocations, with
  verified baseline cleanup required before candidate creation. Bounded evidence
  separates original failures, final failures and inconclusive outcomes; test
  filesystem writes cannot replace the candidate. Twenty-five validation integration
  tests, two preparation tests, nineteen Python adapter tests and thirty-four
  Node adapter tests passed. The full Python suite passed 434 tests (15 optional
  database environment skips), with formatting, lint, typing and package build.
  [Contract and code review guide](../docs/candidate-validation.md). Hosted
  execution remains pending. Evidence acceptance uses the existing fenced T08
  caller; T21 worker orchestration has not started.

- T21: worker checkpoint recovery complete locally. The service adopts accepted
  receipts before restoring, reconciles prior allocations, restores only the
  exact latest accepted artifact and rechecks candidate bytes against a fresh
  restore before committing. Ten cases passed on a dedicated PostgreSQL 17
  instance, including real SIGKILL windows around upload/commit, stale output,
  forged snapshots, owner isolation, retained unknown-allocation capacity and
  corrupt/expired latest artifacts. Three allocation-identity unit tests and the
  existing T15 PostgreSQL checkpoint proof passed. The full Python suite passed
  438 tests with 24 optional database cases skipped. Formatting, lint, typing and
  package build passed. The dedicated database instance was stopped and removed.
  [Recovery contract and evidence](../docs/workspace-recovery.md).

- Checkpoint G: complete locally through T21 process recovery, T20 immutable
  validation and the existing archive/source-tool security fixtures. Hosted
  managed execution and publication remain pending.

- T22: scripted durable agent loop complete locally. Migration 11 stores fenced
  intent, complete responses and immutable stable call identities with owner
  RLS. Per-call activities execute persisted arguments through T19 and accept
  results/checkpoints through T08/T21/T15. Process loss reuses complete responses;
  incomplete or malformed responses never dispatch, and unknown dispatched
  requests stop in attention. Ten dedicated PostgreSQL cases passed, including
  SIGKILL recovery, cross-account denial, sequential multi-turn source edits,
  protected-test denial, retained reservations and downgrade compatibility.
  The full Python suite passed 439 tests with 33 optional database cases skipped;
  formatting, lint, typing and package build passed. Live adapters and full
  cumulative model token limits remain later tasks.
  [Contract and evidence](../docs/scripted-agent-loop.md).

- T23: credential encryption and BYOK lifecycle complete locally. AES-256-GCM
  binds owner/provider/scope/run/credential/version; saved keys require explicit
  opt-in, and per-run material is erased transactionally at terminal transitions.
  The owner-RLS store supports provider-pinned bindings, one-use dispatch
  authorizations, revocation, replacement, rotation and bounded repair. The
  focused target passed ten tests with the dedicated PostgreSQL 17 proof enabled,
  including expiry during a row-lock wait and all four terminal states. The full
  Python suite passed 448 tests with 34 optional database cases skipped;
  formatting, lint, typing and package build passed. Runtime dependency audit
  found no advisories. A dev-only pytest advisory and its private-directory
  mitigation are recorded in the [credential contract](../docs/credential-lifecycle.md).
  No live keys, provider calls or public credential routes were used.

- T24: OpenAI adapter complete locally. The pinned official Responses SDK uses
  disabled retries, strict sequential function calling, store=false and bounded
  request deadlines. Durable turns preserve continuation, provider IDs, latency
  and nullable usage; encrypted one-use credential grants precede dispatch.
  Nine offline protocol tests and thirteen dedicated PostgreSQL agent-loop tests
  passed, including missing-key resume, known/unknown usage and response reuse.
  The full Python suite passed 457 tests (37 optional database cases skipped),
  with formatting, lint, typing and package build passing. The live task gate
  remains pending: eval-live fails closed until the controlled admitted-run
  harness exists. [Contract](../docs/openai-adapter.md).

- T25: Anthropic adapter complete locally. The official Messages SDK uses the
  pinned Sonnet model, disabled retries, strict sequential tools and one-use
  encrypted credential dispatch. Full ordered content (including thinking
  signatures) survives persistence; tool IDs, stop reasons, cached input,
  cache-creation input, latency and unknown usage follow the shared contract.
  Sixteen Anthropic protocol tests passed, as did all six shared provider
  PostgreSQL cases. The full Python suite passed 473 tests (40 optional database
  cases skipped), with formatting, lint, typing and package build passing.
  Live Anthropic execution remains pending; eval-live fails closed without the
  controlled admitted-run harness. [Contract](../docs/anthropic-adapter.md).

- T26: cumulative model limits and durable context complete locally. A run pins
  task inputs, tool schemas and exact model at its first turn preparation.
  Row-locked reservations enforce 30 requests, 120,000 input and 30,000 output
  tokens, with 4,096 output per request. Known usage settles reservations;
  unknown outcomes retain them, and reported overruns stop before tools.
  Context comes from verified accepted responses/results with exact provider
  continuation and bounded visible summaries; caller history, corrupt evidence
  and legacy prepared turns without provenance fail closed. Migration backfills
  existing usage rather than resetting allowance. The final `make check` passed
  498 Python tests (55 optional database cases skipped), 16 frontend tests and
  21 edge tests, plus formatting, lint, Python/TypeScript typing and API checks.
  Dedicated PostgreSQL 17 verification passed 31 cases, including concurrent
  reservations, migration backfill, process loss, corruption rejection and both
  providers' two-turn tool/result flows. Package and frontend production builds
  passed. Docker verification remains pending because its daemon is unavailable.
  No live or paid provider call was made. The disposable database was removed.
  [Limits and context contract](../docs/model-limits.md).

## 16 October 2026

- T27: exact publication approvals complete locally. Migration 16 adds
  owner-isolated immutable manifests, checkpoint/evidence foreign keys and
  one decision per run revision. The T20 bridge records strict passing evidence
  through T08; freezing restores verified archives and checks the hash-verified
  apply-patch checkpoint chain to the original validation baseline, including
  multiple accepted edits. Canonical manifest hashing binds deterministic Git
  objects/commit bytes, cumulative patch digest, exact branch/PR fields,
  validation result, policy and capped deadlines. Independent Git commands
  confirm tree/commit identity for nested directories, Unicode, binary and
  executable files. Decisions serialize against cancellation/revision/expiry;
  approval queues one publication job, rejection uses terminal cleanup, and
  changes awaits T28. Each future remote mutation requires a fresh grant bound
  to current execution, operation, manifest digest and PostgreSQL time.
  Prepared manifests recover after transition loss; an obsolete approval's
  expiry cannot terminalize a newer revision. `make check` passed 508 Python
  tests (optional database cases skipped), 16 frontend tests and 21 edge tests,
  with formatting, lint, typing and API checks. Package/frontend builds passed.
  Dedicated PostgreSQL 17 verification passed 44 approval, agent-loop, provider
  and model-limit/context cases. The disposable database was removed after
  verification. Repository facts/access default to fail closed until the T29
  live verifier; T27 adds no public route or GitHub writes. Docker, real-provider
  approval flows and hosted release evidence remain pending. No paid provider
  call was made. [Approval contract](../docs/approvals.md).

- T28: candidate review interface and bounded rework complete locally. The
  authenticated review query verifies manifest/evidence hashes and pinned source
  archives, checks current repository access and rechecks state/deadlines before
  returning. The responsive diff screen shows file navigation, both validation
  phases, exact manifest/digest, expiry and cumulative allowance. Approve/reject
  and required-feedback changes use durable owner-scoped 24-hour idempotency;
  actual streamed bodies are capped before JSON parsing. Shared approval logic
  supports only revision 0→1→2, creates a new fenced agent turn for each rework,
  binds database feedback into context provenance, and preserves the pinned
  task/model/tool schemas, resource usage and original deadline. Original
  revision-0 context hashes stay compatible. PostgreSQL 17 passed 50 review,
  approval, agent-loop, provider and cumulative-budget/context tests, including
  two new checkpoints/evidence/manifests, old approval rejection, competing
  decisions, cross-owner/access denial and actual HTTP decision/replay. The
  temporary database was removed. `make check`, package/frontend builds and
  the Chromium browser suite passed: 519 Python tests (74 optional database cases skipped), 22
  frontend tests, 21 edge tests and six Chromium browser tests, with formatting,
  lint, Python/TypeScript typing, API generation checks and builds passing.
  Browser tests exercise the built UI with explicit API fixtures, including
  keyboard/mobile layout, hostile text, expiry/stale state and retry identity;
  desktop/mobile renders were inspected. Playwright is pinned as a dev-only
  browser-test dependency. The default API still fails closed without injected
  identity, service and CSRF/origin boundaries; T29 must provide live sessions
  and GitHub authorization. Docker, hosted integration and real-provider review
  remain pending; no paid provider or GitHub mutation was made.
  [Review contract and verification](../docs/review-interface.md).

Implementation resumed through T28 following the user's continuation request.
T29 and later tasks have not started. A local test or mocked adapter does not
prove a hosted gate. Checkpoints A and B remain open until the required account
configuration and real hosted evidence exist.

The requested implementation workers use `gpt-6-luna` with `high` reasoning.

The foundation was additionally built from a fresh clone of committed revision
`0134299` using `make bootstrap build-package build-frontend`.
[Build evidence](../docs/evidence/foundation-build.md) closes the first item in
Checkpoint A; its hosted items remain open.

T03 review corrections include request-scoped credentials, private-origin and
object-key checks, end-to-end HTTP deadlines and byte caps, conservative unknown
mutation outcomes, verified cleanup, and preservation of reports after teardown
failure. The one existing Starlette TestClient deprecation warning does not fail
the suite. A transient Docker Hub 502 required a build retry.
