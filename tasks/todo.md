# Continumm — Implementation Backlog

**Status:** Implementation started on 11 October 2026; see [progress](progress.md) for verification and pending gates.  
**Specification:** [Canonical plan](plan.md).  
**Prepared:** 10 October 2026.

## How to execute this backlog

Work in dependency order. Each task is a focused implementation slice, normally touching one module and its tests. Split a task before implementation if it requires unrelated changes or more than one focused session. Do not replace a failed test with a weaker assertion to pass a checkpoint.

The commands below are the backlog command contract. T01–T02 establish bootstrap, development, checks, targeted tests, UI tests and builds; later commands are added with their owning tasks. Authentication and workers are still pending.

| Command | Contract |
|---|---|
| make bootstrap | Install locked backend and frontend development dependencies |
| make dev | Start the local authenticated development app and worker |
| make check | Formatting checks, Python/TypeScript typing, and deterministic tests |
| make test-target TARGET=tests/path.py | Run the named pytest file using uv |
| make test-integration | PostgreSQL-backed integration suite |
| make test-faults | Deterministic fault and process-recovery suite |
| make test-ui | Frontend unit/component tests |
| make test-e2e | Playwright against local fixtures |
| make build | Produce Python/container and static frontend artifacts |
| make smoke-hosted | Explicitly enabled tests against the owner's controlled deployed fixture |
| make eval-live PROVIDER=openai or anthropic | Explicit paid evaluation using the owner's key |
| make benchmark | Local reproducible queue and durability comparisons |
| make export | Encrypted operator export excluding reusable credentials |
| make restore-check | Quarantined restoration and integrity checks |

All tasks additionally require relevant documentation updates, no secret exposure, and a clean focused verification result. Paid/cloud operations are never part of an untrusted pull-request test job.

## Phase 0 — Prove the foundation and hosting assumptions

### T01 — Backend project skeleton and development commands

- **Dependencies:** None.
- **Scope:** Medium; Python manifest, application entrypoint, Makefile, basic test and configuration.
- [x] Establish Python 3.13/uv, typed settings, FastAPI health endpoints, structured logging, and the backend command contract.
- [x] Missing secrets/configuration fail clearly without printing values; local-only development defaults cannot become public bypasses.
- **Verify:** make test-target TARGET=tests/unit/test_settings.py; make build.

### T02 — Frontend skeleton and shared API contract

- **Dependencies:** T01.
- **Scope:** Medium; frontend manifest, entrypoint, router, API client and one component test.
- [x] Build a TypeScript/React/Vite shell and serve its production assets from the Python application.
- [x] Add generated OpenAPI types and a uniform typed error display, with no handwritten parallel API schema.
- **Verify:** make test-ui; make build; exercise API failure display.

### T03 — Hosted database and artifact connectivity spike

- **Dependencies:** T01.
- **Scope:** Medium; deployment descriptor, database adapter, storage adapter, smoke test.
- **Progress:** Local preparation and verification complete; hosted acceptance is pending provider setup. See [progress](progress.md) and [hosting instructions](../docs/hosting-setup.md).
- [ ] Prove Render Free can connect to Supabase through the selected pooler, commit a row, and upload/download a private object.
- [ ] Record free-plan eligibility, payment/overage settings, memory baseline, and account-specific limits without enabling paid fallback.
- **Verify:** make smoke-hosted with a controlled fixture; retain a redacted evidence report.

### Checkpoint A — Basic feasibility

- [x] Backend and static UI build from a fresh checkout. See [build evidence](../docs/evidence/foundation-build.md).
- [ ] Hosted database and private object access are proven.
- [ ] No provider account setting violates the operator's spending constraint.

### T04 — Managed sandbox lifecycle spike

- **Dependencies:** T03.
- **Scope:** Medium; sandbox adapter skeleton and hosted smoke tests.
- **Progress:** Local adapter and opt-in fixture preparation verified; hosted lifecycle and killed-client timeout proof remain pending. See [sandbox setup](../docs/sandbox-setup.md).
- [ ] Create a named session with limits, run a harmless fixture, stream files, and stop/destroy it.
- [ ] Demonstrate a server-side timeout after the client/API process is killed and verify no credentials are available inside.
- **Verify:** make smoke-hosted; inspect remote allocation state and redacted evidence.

### T05 — Durable webhook ingress spike

- **Dependencies:** T03.
- **Scope:** Medium; tiny edge handler, inbox migration, signature tests, deployment configuration.
- **Progress:** Local receiver, private migration and real PostgreSQL integration verified; deployed ingress and sleeping-Render proof remain pending. See [ingress setup](../docs/ingress-setup.md).
- [ ] Verify HMAC over raw bytes and commit an inbox entry before acknowledging; reject oversized or invalid payloads.
- [ ] Show that a duplicate delivery and lost acknowledgment do not duplicate the stored event, including when Render is asleep.
- **Verify:** make test-target TARGET=tests/integration/test_webhook_inbox.py; make smoke-hosted.

### T06 — Work-triggered wake spike

- **Dependencies:** T05.
- **Scope:** Medium; wake function, schedule, authenticated drain endpoint and tests.
- **Progress:** Local fenced dispatcher and authenticated receipt/status boundary verified. Receipts explicitly remain undrained; hosted cron, cold-start and suspension evidence are pending. See [wake setup](../docs/wake-setup.md).
- [ ] Wake the backend only for actionable work, with backoff and no idle keepalive.
- [ ] Simulate quota suspension and database unavailability without reporting successful processing.
- **Verify:** make test-target TARGET=tests/integration/test_wake_dispatch.py; make smoke-hosted.

### Checkpoint B — Deployment viability

- [ ] Actual isolation, remote timeout, artifact transfer, inbox commit, and wake behavior are recorded.
- [ ] Failed feasibility checks block public rollout instead of triggering an unapproved provider substitution.
- [ ] The supported deployment is reproducible from configuration and documented manual account setup.

## Phase 1 — Durable runtime

### T07 — Run, step, job, and event persistence

- **Dependencies:** T01, T03.
- **Scope:** Medium; first runtime migration, persistence module and integration tests.
- [x] Create explicit states, foreign keys, logical keys, event sequence constraints, and owner fields.
- [x] Duplicate logical work is rejected or adopted by a database constraint, not an in-memory check.
- **Verify:** make test-target TARGET=tests/integration/test_runtime_schema.py.

### T08 — Atomic transition service

- **Dependencies:** T07.
- **Scope:** Medium; transition service and integration tests.
- [x] Commit accepted result, state, event, and next job atomically using the documented lock order.
- [x] Rollback leaves no partial advancement; lost acknowledgment recovery finds the committed result.
- **Verify:** make test-target TARGET=tests/integration/test_transitions.py.

### T09 — Claim, heartbeat, and fencing

- **Dependencies:** T08.
- **Scope:** Medium; queue/lease functions and concurrent-worker tests.
- [x] Competing workers claim with a monotonic generation; expired heartbeats cannot revive ownership.
- [x] Fenced writes reject the old generation at intermediate and final boundaries.
- **Verify:** make test-target TARGET=tests/integration/test_leases.py.

### Checkpoint C — Ownership proof

- [x] Two independent processes cannot commit competing results.
- [x] Database-time boundary races are deterministic in tests.
- [x] Lock-order and bounded deadlock-retry tests pass.
  Run-first skip-locked selection, bounded retry, and an actual two-transaction
  PostgreSQL deadlock with rollback/retry are verified.

### T10 — Recovery, retries, and durable waits

- **Dependencies:** T09.
- **Scope:** Medium; scheduler, error policy and recovery tests.
- [x] Recover expired leases and due jobs idempotently without an in-memory timer.
- [x] Keep attempt and failure retry counters distinct; honor retry-after and deadlines.
- **Verify:** make test-target TARGET=tests/integration/test_scheduler.py.
  PostgreSQL 17 integration proofs also pass for the adapted T08 transitions and
  T09 lease/deadlock fixtures, each on its own disposable server because the
  fixtures share cluster-wide role names.

### T11 — Cancellation and terminality

- **Dependencies:** T10.
- **Scope:** Medium; cancellation transition and race tests.
- [x] Commit cancellation, revoke new dispatch authorizations, and release the account slot even when GitHub access has been removed.
- [x] Preserve independent unresolved-effect/resource state; late observations cannot reopen the run.
- **Verify:** make test-target TARGET=tests/integration/test_cancellation.py.

### T12 — Minimal run timeline

- **Dependencies:** T02, T08.
- **Scope:** Medium; run query endpoint, event endpoint, timeline and component tests.
- [x] Render authoritative state and ordered events with explicit empty/error/disconnected states.
- [x] SSE reconnect deduplicates events and refetches a snapshot after gaps.
- **Verify:** make test-ui; make test-target TARGET=tests/integration/test_events.py.

### Checkpoint D — Runtime demonstration

- [x] SIGKILL recovery and paused-stale-worker rejection are separate successful demonstrations.
- [x] A timeline shows what committed, what was rejected, and what resumed.
- [x] Model or GitHub integrations are not required to prove the queue.
- **Evidence:** [Local Checkpoint D process recovery proof](../docs/evidence/checkpoint-d-local.md).

## Phase 2 — Safe workspace execution

### T13 — Repository preflight

- **Dependencies:** T03, T07.
- **Scope:** Medium; repository adapter, profile definitions and eligibility tests.
- [x] Pin a repository/commit and enforce visibility, size, file type, package source and profile boundaries.
- [x] Metadata-only preflight is rate/byte bounded; unsupported repositories fail before model calls or sandbox allocation.
- **Evidence:** [Local preflight contract and verification](../docs/repository-preflight.md); 49 unit tests and a dedicated PostgreSQL 17 quota/cache/RLS proof. Live GitHub App verification remains fail closed until T33.
- **Verify:** make test-target TARGET=tests/unit/test_repository_eligibility.py.

### T14 — Canonical archives and manifests

- **Dependencies:** T13.
- **Scope:** Medium; archive module and adversarial unit tests.
- [x] Produce/verify separate archive and canonical tree digests while preserving permitted executable bits.
- [x] Reject traversal, links, special files, duplicate/case-colliding paths and expansion bombs.
- **Evidence:** [Canonical format and local verification](../docs/canonical-archives.md); 51 adversarial unit tests, including bounded single-member gzip and raw USTAR header verification without filesystem extraction.
- **Verify:** make test-target TARGET=tests/unit/test_archives.py.

### T15 — Committed checkpoint storage

- **Dependencies:** T08, T14.
- **Scope:** Medium; artifact/checkpoint migration, storage service and tests.
- [x] Upload and verify before the fenced reference transaction.
- [x] Reserve upload bytes and handle orphan/unknown upload, missing object, bad digest, and a late stale uploader.
- **Evidence:** [Checkpoint storage contract and local proof](../docs/checkpoint-storage.md); private HTTP fixtures plus a dedicated PostgreSQL 17 integration target. Hosted storage remains pending.
- **Verify:** make test-target TARGET=tests/integration/test_checkpoints.py.

### Checkpoint E — Artifact correctness

- [x] Reconstructed trees match committed manifests exactly.
- [x] Neither upload success alone nor a worker's local files count as durable progress.
- [x] Corruption produces a visible attention state.
- **Evidence:** [Local artifact correctness proof](../docs/evidence/checkpoint-e-local.md). Actual hosted Storage restoration remains pending.

### T16 — Sandbox allocation accounting

- **Dependencies:** T04, T09.
- **Scope:** Medium; allocation persistence, adapter lifecycle and tests.
- [x] Reserve before creation and correlate remote allocation with a stable attempt identity.
- [x] Unknown creation or stop retains capacity; replacement waits for observed termination, never a local-time assumption about a delayed request.
- **Evidence:** [Allocation accounting and local proof](../docs/sandbox-allocation-accounting.md); dedicated PostgreSQL 17 and fake SDK/provider tests. Hosted allocation remains pending.
- **Verify:** make test-target TARGET=tests/integration/test_sandbox_allocations.py.

### T17 — Python profile

- **Dependencies:** T13, T16.
- **Scope:** Medium; pinned profile image/setup and profile tests.
- [x] Implement supported hash-pinned wheel installation, defined local-source paths and bounded pytest; verify locally with synthetic fixtures.
- [x] Implement and locally verify the active-session policy transition, required deny-all probe and rejection of install/build-hook-dependent projects.
- **Evidence:** [Python profile contract and local verification](../docs/python-execution-profile.md). Image publication and actual managed fixture smoke remain pending; Checkpoint F is open.
- **Verify:** make test-target TARGET=tests/adapters/test_python_profile.py; hosted fixture smoke.

### T18 — Node profile

- **Dependencies:** T13, T16.
- **Scope:** Medium; pinned Node image/setup and profile tests.
- [x] Implement and locally verify script-disabled npm ci and the documented bounded test command.
- [x] Reject workspaces, unsupported sources and detected native/browser requirements; enforce deny-all test egress for external-service-dependent tests.
- **Evidence:** [Node profile contract and local verification](../docs/node-execution-profile.md). Image build/publication and actual managed fixture smoke remain pending; Checkpoint F is open.
- **Verify:** make test-target TARGET=tests/adapters/test_node_profile.py; hosted fixture smoke.

### Checkpoint F — Supported execution

- [ ] Both language profiles work in the actual managed environment.
- [ ] Remote commands stop when the control plane disappears.
- [ ] Setup and retries consume the same finite run reservation.

### T19 — Typed file tools and policy

- **Dependencies:** T15, T17, T18.
- **Scope:** Medium; tool schemas, policy module and tests.
- [x] Implement bounded archive-backed list/read/literal-search/apply-patch/diff tools with protected paths.
- [x] Deny arbitrary shell/network tools, known credential paths, dependency/CI edits and baseline test/harness changes.
- **Evidence:** [Source tools and policy](../docs/source-file-tools.md); pure synthetic tests. Durable tool orchestration and cumulative budgets remain assigned to later tasks.
- **Verify:** make test-target TARGET=tests/unit/test_tool_policy.py.

### T20 — Validation on immutable candidates

- **Dependencies:** T19.
- **Scope:** Medium; validation activity, evidence records and tests.
- [x] Test a separate restored candidate; discard validation writes and preserve original test evidence.
- [x] Bind results to the candidate digest and report baseline failure versus final failure accurately.
- **Verify:** make test-target TARGET=tests/integration/test_candidate_validation.py.

### T21 — Worker-to-checkpoint recovery

- **Dependencies:** T16, T19, T20.
- **Scope:** Medium; activity orchestration and process tests.
- [x] Resume from the last committed checkpoint after kills before/after upload and commit.
- [x] Late sandbox output cannot overwrite a newer authoritative result.
- **Evidence:** [Local worker checkpoint recovery proof](../docs/workspace-recovery.md); ten PostgreSQL/process cases and three allocation-identity tests. Hosted recovery remains pending.
- **Verify:** make test-target TARGET=tests/faults/test_workspace_recovery.py.

### Checkpoint G — Recoverable tools

- [x] Real process failures do not lose committed source changes.
- [x] Candidate validation cannot change publication bytes.
- [x] All filesystem/security fixture cases have explicit outcomes.
- **Evidence:** [Local recovery and security fixture outcomes](../docs/workspace-recovery.md), including the T20 immutable-candidate proof. Actual managed execution and publication remain pending.

## Phase 3 — Agent and human review

### T22 — Scripted durable agent loop

- **Dependencies:** T19, T21.
- **Scope:** Medium; turn/invocation persistence, scripted adapter and tests.
- [x] Persist intent, complete response, stable call IDs, sequential tools and accepted results.
- [x] Recovery reuses committed responses; partial responses never dispatch tools.
- **Evidence:** [Scripted durable agent loop](../docs/scripted-agent-loop.md); ten dedicated PostgreSQL/scripted cases, including process death, multi-turn patch lineage, malformed responses, unknown outcomes, quotas and RLS. No live inference is used.
- **Verify:** make test-target TARGET=tests/integration/test_agent_loop.py.

### T23 — Credential encryption and BYOK lifecycle

- **Dependencies:** T07.
- **Scope:** Medium; credential service, settings and security tests.
- [x] Authenticated encryption binds owner/provider/scope and supports key-version rotation.
- [x] Saved and per-run keys are redacted, revocable, and unavailable to tools or traces.
- **Evidence:** [Credential lifecycle contract and local proof](../docs/credential-lifecycle.md); synthetic cryptographic tests and a dedicated PostgreSQL 17 lifecycle/RLS/lease-expiry proof. Public credential routes and live provider wiring remain later tasks.
- **Verify:** make test-target TARGET=tests/security/test_credentials.py.

### T24 — OpenAI adapter

- **Dependencies:** T22, T23.
- **Scope:** Medium; one adapter and protocol fixtures.
- [x] Normalize function calls, IDs, usage, continuation and errors using the chosen model configuration.
- [x] Disable hidden retries; record unknown usage; honor limits and store=false.
- **Verify:** make test-target TARGET=tests/adapters/test_openai.py; explicit make eval-live PROVIDER=openai.

### Checkpoint H — Durable model boundary

- [x] A scripted response can survive process loss without another inference.
- [ ] One real OpenAI task completes through controlled tools.
- [ ] No credential appears in stored or displayed artifacts.

### T25 — Anthropic adapter

- **Dependencies:** T22, T23.
- **Scope:** Medium; one adapter and protocol fixtures.
- [x] Preserve tool_use/tool_result correlation, continuation material, stop reasons and usage.
- [x] Pass the same application contract and unknown-response accounting as OpenAI.
- **Verify:** make test-target TARGET=tests/adapters/test_anthropic.py; explicit make eval-live PROVIDER=anthropic.

### T26 — Model/run limits and context

- **Dependencies:** T24, T25.
- **Scope:** Medium; context builder, reservations and tests.
- [x] Enforce cumulative request/token/context limits with no reset during retry or rework.
- [x] Expose uncertainty and stop rather than silently switching provider/model or exceeding limits.
- **Verify:** make test-target TARGET=tests/integration/test_model_limits.py.

### T27 — Approval manifest and decisions

- **Dependencies:** T20, T22.
- **Scope:** Medium; canonical manifest, approval migration/service and race tests.
- [x] Freeze exact publication content and evidence before presenting approval.
- [x] Accept one current owner decision under expiry/cancellation/revision checks; every later mutation authorization remains deadline-bound.
- **Verify:** make test-target TARGET=tests/integration/test_approvals.py.

### Checkpoint I — Approval safety

- [x] Approved digest equals the independently validated candidate (local verified archives and independent Git proof).
- [x] Stale/expired/conflicting approval never dispatches publication (local PostgreSQL authorization proof; live publication remains pending).
- [ ] Both live providers use the same policy and runtime boundaries.

### T28 — Review interface and bounded rework

- **Dependencies:** T12, T26, T27.
- **Scope:** Medium; review endpoint/UI and tests.
- [x] Display diff, test evidence, manifest and limits with approve/reject/changes actions.
- [x] Two revisions are supported; each gets new evidence/approval and shares existing resource allowance.
- **Verify:** make test-ui; make test-e2e with approval/rework fixtures.

## Phase 4 — Correct external publication

### T29 — GitHub user authorization

- **Dependencies:** T23.
- **Scope:** Medium; OAuth/session service and security tests.
- [x] Link immutable GitHub identity and installation access; use secure rotated sessions.
- [x] Recheck actor/repository rights and protect state-changing browser requests from CSRF.
- **Verify:** make test-target TARGET=tests/security/test_github_auth.py.

### T30 — External-operation ledger

- **Dependencies:** T08, T27.
- **Scope:** Medium; effect migration/service and fake-remote tests.
- [x] Record mutation intent, dispatch, observations, unknown outcome and conflicts with stable identity.
- [x] Database retries never replay a remote mutation and cancelled runs cannot dispatch new effects.
- **Verify:** make test-target TARGET=tests/integration/test_external_ledger.py.

### T31 — Deterministic commit and branch publication

- **Dependencies:** T29, T30.
- **Scope:** Medium; GitHub publication adapter and fixtures.
- [x] Prepare deterministic Git identity before approval, then upload approved content-addressed objects and create/adopt only the exact managed branch state.
- [x] Reject unexpected branch/base changes without force overwrite or implicit rebase.
- **Verify:** make test-target TARGET=tests/adapters/test_github_branch.py.

### Checkpoint J — Remote write boundary

- [x] Publication needs current actor rights and the exact approval (local fixture proof; live verification pending).
- [x] Content-addressed safe repetition is distinct from uncertain non-idempotent creation.
- [x] Repository code never sees publication credentials.

### T32 — PR creation and reconciliation

- **Dependencies:** T31.
- **Scope:** Medium; PR adapter and remote-failure tests.
- [x] Adopt one exact matching PR across states/pages; expose conflicts and closed/merged outcomes.
- [x] A response-lost or temporarily invisible PR never triggers speculative replacement creation.
- **Verify:** make test-target TARGET=tests/faults/test_pr_reconciliation.py.

### T33 — External attention and cancellation display

- **Dependencies:** T11, T28, T32.
- **Scope:** Medium; attention API/UI and tests.
- [x] Show unresolved effects after cancellation without reopening the run.
- [x] Provide read-only investigation/adoption where evidence permits; no force-retry bypass.
- **Verify:** make test-e2e with unknown-publication/cancellation fixtures.

### T34 — End-to-end demonstration milestone

Deferred by the owner on 19 October 2026; continue T35 and later tasks first.

- **Dependencies:** T25, T28, T32, T33.
- **Scope:** Medium; controlled demo script and end-to-end fixtures.
- [ ] Complete a real-provider patch, revision, approval and normal GitHub publication.
- [x] Demonstrate actual worker death, stale-owner rejection and lost-response reconciliation separately (local controlled fixtures).
- **Verify:** make test-faults; controlled live demo with recorded evidence.
- **Status:** Local harness and recovery evidence implemented; live OpenAI/Render/GitHub demonstration remains open pending service setup and trusted admission/model-input/validation composition. The bounded publication worker, persisted turn-input resolver and trusted first-turn pinning API are implemented; candidate-validation and manifest-freeze activity handlers are implemented. Durable baseline/eligibility pins and a concrete persisted validation-input resolver are implemented. Admission/source preparation and tool-policy composition remain open. [Evidence](../docs/evidence/t34-demonstration.md).

### Checkpoint K — Portfolio demonstration

- [ ] A reviewer can see a complete product journey and original recovery mechanisms.
- [ ] This is the 2–3 month target milestone; it is not automatic authorization to open public signup.
- [ ] Unmeasured performance and unimplemented public features are labelled accurately.

## Phase 5 — Public application and GitHub commands

### T35 — Account-scoped repository onboarding

- **Dependencies:** T13, T29.
- **Scope:** Medium; onboarding endpoints/UI and authorization tests.
- [x] A new user connects an eligible repository and can update explicit provider/profile defaults; trusted setup handoff claims one original submission (public comment admission follows T36/T37).
- [x] Another account cannot list or use its registrations, keys, runs or artifacts.
- **Verify:** make test-target TARGET=tests/security/test_account_isolation.py; make test-e2e.
- **Status:** Onboarding API/UI and trusted original-submission handoff implemented. [Contract and evidence](../docs/repository-onboarding.md).

### T36 — Public admission and quotas

- **Dependencies:** T16, T26, T35.
- **Scope:** Medium; quota/admission service and race tests.
- [x] Enforce active-account/global queue limits and reserve before sandbox/model dispatch.
- [x] Rejected work does not spend; unknown work remains charged; operator/public pools and cross-period reservations remain conservative.
- **Verify:** make test-target TARGET=tests/integration/test_admission.py.
- **Status:** Atomic admission and default-off API implemented; no live dispatch enabled. [Contract and bootstrap](../docs/run-admission.md).

### T37 — GitHub command parsing and actor policy

- **Dependencies:** T05, T29, T36.
- **Scope:** Medium; command parser/normalizer and tests.
- [x] Accept only specified newly created issue-comment commands from the authorized account owner.
- [x] Ignore edits/bots/quotes, enforce the 72-hour/pre-registration freshness rule, compare current comments, and deduplicate logical commands.
- **Verify:** make test-target TARGET=tests/security/test_github_commands.py.

### Checkpoint L — Public command safety

- [ ] Public visibility never substitutes for write authorization.
- [ ] A collaborator cannot spend another user's provider key.
- [ ] Concurrent console/comment submissions create one active issue run.

### T38 — Cross-channel decisions and status comments

- **Dependencies:** T27, T30, T37.
- **Scope:** Medium; shared command handlers, comment adapter and tests.
- [x] Approve/reject/changes/cancel follow the exact same transitions in browser and comments.
- [x] Status-comment creation has stable identity/reconciliation and never exposes private run content.
- **Verify:** make test-target TARGET=tests/integration/test_cross_channel_decisions.py.

### T39 — Failed-delivery recovery

- **Dependencies:** T06, T37.
- **Scope:** Medium; delivery scanner, watermark and tests.
- [x] Request bounded redelivery of recoverable missing events with pagination, overlap and rate-limit handling.
- [x] A gap beyond inspection history is surfaced; old mutable comment text is never reconstructed into spending/approval intent.
- **Verify:** make test-target TARGET=tests/integration/test_delivery_recovery.py.

### T40 — Retention and account deletion

- **Dependencies:** T15, T23, T36.
- **Scope:** Medium; cleanup service, deletion commands and races.
- [ ] Expired content becomes inaccessible immediately; bounded upload/GC references are safe and deletion deadlines override content pins.
- [ ] Revoke keys/sessions, journal deletions, remove provider copies, retain minimal tombstones, and remove temporary keys at terminal states.
- **Verify:** make test-target TARGET=tests/integration/test_retention.py.

### Checkpoint M — Complete public lifecycle

- [ ] Web and GitHub paths include onboarding, credentials, review and publication.
- [ ] Deletion/expiry/revocation cannot be bypassed by a stale page or old comment.
- [ ] Storage and quota pressure have clear visible outcomes.

### T41 — Product quality and accessibility

- **Dependencies:** T28, T33, T35, T38, T40.
- **Scope:** Medium; targeted screen polish and browser checks, split by screen if needed.
- [ ] Complete responsive layouts, keyboard/focus behavior, form errors and all specified failure states.
- [ ] Inspect the rendered run/review pages against the documented visual direction; do not substitute decorative screens for functional evidence.
- **Verify:** make test-ui; make test-e2e; keyboard and narrow-screen review.

### T42 — Operator diagnostics and emergency controls

- **Dependencies:** T33, T36, T39, T40.
- **Scope:** Medium; restricted operations interface and tests.
- [ ] Inspect unknown effects/allocations, inbox lag, storage and admission state without exposing credentials.
- [ ] Support global admission stop, account suspension and audited recovery actions without a force-complete bypass.
- **Verify:** make test-target TARGET=tests/security/test_operator_actions.py.

### T43 — Export and quarantined restore

- **Dependencies:** T40, T42.
- **Scope:** Medium; export/restore commands and integrity tests.
- [ ] Export verified references/content without any credentials/session secrets; enforce export expiry and deletion-journal invalidation.
- [ ] In quarantine, apply the current deletion journal/expiry before reads, invalidate sessions/ownership and reconcile effects before writes.
- **Verify:** make export; make restore-check using a disposable environment.

### Checkpoint N — Operability

- [ ] Operator can explain and recover each supported pause/failure class.
- [ ] Backup limitations and retention semantics match the implemented behavior.
- [ ] Restoring old state cannot immediately repeat external publication.

## Phase 6 — Evidence and release

### T44 — Property-based invariant suite

- **Dependencies:** T10, T11, T27, T30, T36, T40.
- **Scope:** Medium; state-machine generators and invariant assertions.
- [ ] Map I01–I20 to test cases and generate competing transition sequences.
- [ ] Persist failing seeds and minimize reproducible counterexamples.
- **Verify:** make test-target TARGET=tests/properties/test_runtime_invariants.py.

### T45 — Process and network fault laboratory

- **Dependencies:** T21, T32, T38, T39, T44.
- **Scope:** Medium; fault controller and scenario fixtures, split by adapter where necessary.
- [ ] Cover every required matrix row with deterministic barriers or a documented deployed test.
- [ ] Report intermediate and final authoritative state, not merely eventual green status.
- **Verify:** make test-faults.

### T46 — Security adversarial suite

- **Dependencies:** T35, T37, T40, T42.
- **Scope:** Medium; account, archive, prompt-injection and browser security tests.
- [ ] Verify cross-account denial, path protection, secret boundaries, webhook replay and CSRF/CSP behavior.
- [ ] Resolve high-severity findings before public access.
- **Verify:** make test-target TARGET=tests/security; make test-e2e.

### Checkpoint O — Release safety

- [ ] No unresolved high-severity isolation or correctness defect remains.
- [ ] Every supported failure has a visible product outcome.
- [ ] A fresh account cannot bypass quotas or authorization through the second interface.

### T47 — Reproducible runtime benchmarks

- **Dependencies:** T45.
- **Scope:** Medium; benchmark harness, naive baseline and report generator.
- [ ] Compare identical scripted workloads at 1/2/4 local workers and publish raw data/configuration.
- [ ] Separate latency/throughput experiments from managed sandbox and model measurements.
- **Verify:** make benchmark; inspect generated report for unsupported claims.

### T48 — Coding-agent evaluation report

- **Dependencies:** T24, T25, T26, T46.
- **Scope:** Medium; 12-task corpus and evaluation runner/report.
- [ ] Run the 12-task deterministic corpus and four live release smokes, one language task per provider; report additional live cases only when measured.
- [ ] Retain hidden checks outside writable workspaces and keep paid/hosted execution explicitly enabled within the owner's model and operator-sandbox allowances.
- **Verify:** deterministic corpus tests; explicit make eval-live PROVIDER=openai and PROVIDER=anthropic.

### T49 — Documentation and demonstration package

- **Dependencies:** T34, T43, T47, T48.
- **Scope:** Medium; README, operations/security/profile docs and real screenshots.
- [ ] Document supported behavior, BYOK retention/costs, beta quotas and quickstart; publish a stable external status/help document.
- [ ] Publish the signature demo and trace every guarantee to implementation/test evidence.
- **Verify:** fresh-checkout quickstart, link checks and independent documentation review.

### T50 — Deployed launch qualification

- **Dependencies:** T41, T46, T49.
- **Scope:** Medium; deployment verification and release checklist.
- [ ] Exercise sleep/wake, webhook persistence, sandbox timeout, object restoration and both providers on the actual deployment.
- [ ] Verify all final acceptance items in the plan before enabling public admission.
- **Verify:** make smoke-hosted; complete the launch checklist with dated evidence.

### Final checkpoint — Public beta

- [ ] All required tasks are complete and their evidence is recorded.
- [ ] Provider free-plan and non-commercial conditions still apply.
- [ ] No paid fallback, unsupported environment, or broader guarantee was silently added.
- [ ] The user has reviewed the implemented release and its public-facing limits.

## Explicitly deferred backlog

Private repositories; shared organizations; paid billing; external runtime SDK; arbitrary workflow graphs; broader package/build support; dependency upgrades; automatic merging; additional providers/models; MCP; higher concurrency; continuous-availability hosting; commercial operation; Temporal comparison.

Deferred items are not partially implemented through generic abstractions in the first release.
