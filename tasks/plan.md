# Continumm — Complete Product and Implementation Plan

**Prepared:** 10 October 2026  
**Status:** Implementation baseline following the planning interview; implementation began on 11 October 2026. See [progress](progress.md) for verified scope and pending gates.
**Working name:** Continumm, using the user's spelling.  
**Purpose:** A portfolio-quality public developer service demonstrating both backend infrastructure and applied AI engineering.  
**Canonical task list:** [Implementation backlog](todo.md).  
**Vocabulary:** [Project glossary](../GLOSSARY.md).  
**Decision record:** [Architecture decisions](../docs/adr/README.md).

This is a fresh specification. The older Continuum blueprint is historical context, not an additional source of binding requirements. Where that document disagrees with this plan, this plan describes the proposed new project.

## 1. Project definition

Continumm helps a developer turn a bounded coding task into a reviewed GitHub pull request. Its custom runtime preserves committed progress across interruptions, controls tool execution, records what happened, and handles uncertain external outcomes without blindly repeating mutations.

The project has two equally important deliverables:

1. **A useful product:** GitHub connection, supported-repository onboarding, a coding agent, understandable progress, patch and test review, revision requests, and approved publication.
2. **A defensible runtime:** durable jobs, transactional state transitions, execution ownership, immutable checkpoints, bounded retries, authorization, external-effect reconciliation, and reproducible failure experiments.

The runtime initially serves Continumm's registered coding workflow. It does not expose a general-purpose workflow language or SDK.

### 1.1 Confirmed decisions

| Area | Decision from the interview |
|---|---|
| Career objective | Backend/infrastructure and applied AI/agent roles |
| Product emphasis | Equal depth in custom runtime and user-facing product |
| Primary workflow | Coding tasks, tests, human review, branch and PR publication |
| Audience | Public signup, individual accounts, bounded beta |
| Repository visibility | Public repositories only; run data remains private |
| Supported ecosystems | Python and JavaScript/TypeScript through defined build profiles |
| Task scope | Localized bug fixes, regression tests, and small changes |
| Runtime ownership | Custom internal runtime; external SDK deferred |
| Backend | Python and PostgreSQL |
| Frontend | TypeScript/React |
| Model support | OpenAI and Anthropic from launch; users supply their own keys |
| Execution isolation | Managed isolated code execution |
| Interfaces | Web console and GitHub comments |
| Publication | Review exact changes and validation evidence before publishing |
| Revisions | At most two user-requested rework rounds per run |
| Retention | Seven-day run-content retention; 48-hour approval expiry |
| Keys | Saved encrypted keys or encrypted per-run keys |
| Uncertain mutations | Pause for investigation rather than unsafe repetition |
| Budget | Operator spending ceiling of ₹1,000/month |
| Availability | Pauses during quota exhaustion or service suspension are acceptable |
| Commercial use | Personal, non-commercial portfolio project |
| Capacity | Planning envelope of 10–15 hours/week |
| Schedule | Demonstrable system in 2–3 months; public release only after its gates pass |

### 1.2 Explicit planning defaults

These make the plan implementable without disguising recommendations as user answers:

- Use Render Free, Supabase Free, and Vercel Sandbox Hobby as the deployment baseline. The user selected “revise hosting,” then supplied “none” as the requested change. This bundle is therefore retained as an explicit default, not recorded as an affirmative provider selection.
- Use one application repository, one PostgreSQL database, a private object bucket, and a modular Python application.
- Use Vite/React rather than a second server-rendering runtime. Serve its production assets from FastAPI.
- Use a light, compact engineering console with a diff-centered review screen and causal execution timeline.
- Use Apache-2.0 as the proposed source-license default; hosting remains non-commercial regardless of source licensing.
- Begin with deliberately small numerical quotas in section 12. These are configured safety limits, not measured capacity or promises of throughput.
- Treat 2–3 months as a demonstration target. Do not label an incomplete public service production-ready to meet that date.

### 1.3 Definition of product success

A new eligible user can sign in, authorize a supported repository, supply a provider key, submit a task, inspect progress, review a patch and validation evidence, request changes, approve the new exact result, and find the resulting PR.

If the service is interrupted, the user can distinguish saved progress, pending work, insufficient capacity, missing credentials, and unresolved publication. The product never reports success merely because an LLM produced a plausible explanation.

### 1.4 First-release exclusions

Exclude private repositories, shared team accounts, billing, subscriptions, arbitrary workflow authoring, public SDKs, arbitrary shell tools for the model, automatic merging, unrestricted repository environments, dependency upgrades, agent edits to CI workflows, MCP, multi-agent execution, RAG infrastructure, generic fan-out/join, Kubernetes, Redis/Celery, Kafka, and Temporal.

These exclusions do not prohibit a later extension. They prevent additional systems from becoming prerequisites for the first complete product.

## 2. First-principles basis and guarantees

### 2.1 Constraints

Processes can die between any two instructions. A stopped heartbeat is evidence of lost contact, not proof that the old process stopped. A remote API can commit a request whose response is lost. PostgreSQL cannot atomically commit a GitHub write. Model responses are nondeterministic. Repository code is untrusted even when the repository is public. Free infrastructure may sleep, suspend, or become unavailable.

Consequently, persistence, ownership, external observations, and user authorization are separate concerns. A database lease does not fence a remote API that does not understand that lease.

### 2.2 Supported guarantees

| Guarantee | Meaning |
|---|---|
| Durable committed progress | Results whose database references and verified objects are committed survive compute-process loss while durable storage remains available |
| At-least-once attempt execution | Activities may execute again after failure; logical results are accepted through fenced transitions |
| Single authoritative result | Only the current valid generation can publish a logical operation's result into runtime state |
| Exact approval binding | Approval authorizes one immutable action manifest, not “whatever the agent does next” |
| Controlled external repetition | Mutation retries depend on operation-specific evidence; uncertain non-idempotent writes do not receive blind retries |
| Account isolation | One account cannot inspect or act on another account's runs, credentials, approvals, or artifacts |
| Recoverable waits | Approvals, retries, and capacity waits do not require a living worker |
| Bounded work | Admission, model requests, sandbox allocations, artifacts, and revisions have explicit limits |
| Explainable outcomes | The console records attempts, ownership changes, checkpoints, authorization decisions, and external observations |

### 2.3 Claims expressly not made

Do not claim arbitrary external effects happen exactly once, all coding tasks can be solved, passing tests proves semantic correctness, cancellation reverses completed effects, infrastructure is continuously available, BYOK spending is a provider-enforced monetary cap, or data survives destruction of all authoritative storage.

Replay means inspecting persisted history and reusing recorded results. It does not mean reproducing an LLM's internal reasoning or rerunning a nondeterministic model identically.

## 3. Product workflow and supported repositories

### 3.1 Main journey

1. Sign in through GitHub.
2. Connect the GitHub App to selected repositories.
3. Select an accessible public repository and run its eligibility check.
4. Choose a supported build profile and provider. Supply a key or select a saved one.
5. Submit an issue or a bounded task with explicit acceptance criteria.
6. Display the pinned base revision, supported commands, resource limits, provider disclosure, and retention window before admission.
7. Admit the run durably or explain why capacity is unavailable.
8. Prepare an isolated workspace and record baseline validation.
9. Let the agent inspect, edit, and run approved checks within its limits.
10. Validate an immutable candidate in an isolated copy.
11. Present its diff, baseline comparison, final checks, usage, and exact proposed publication.
12. Approve, reject, or request changes. Revisions require new validation and new approval.
13. Publish or reconcile the approved branch and PR.
14. Show the final outcome and allow evidence/patch download before expiry.

### 3.2 Repository eligibility

The first release accepts repositories the signed-in actor can currently write to and that the GitHub App can access. Arbitrary third-party repositories and fork-based contributions are deferred.

Eligibility rejects private or archived repositories; Git submodules; Git LFS dependencies; symlinks, hard links, and special files; unsupported package sources; repository size violations; builds needing private packages, service credentials, custom containers, or external services; and workloads outside the supported runtime profiles.

GitHub repository numeric IDs are authoritative. Names and owners are display and lookup metadata that can change.

Repository access is checked again before reads of retained content and before publication. If a public repository becomes private or app access is removed, content access is suspended and the run is paused or cancelled as appropriate.

An authenticated Continumm owner can still cancel, revoke credentials, delete run content, or delete their account after losing GitHub permission. These protective actions require account ownership, not renewed repository access.

Preflight is bounded metadata and manifest inspection, never dependency installation or code execution. Limit checks to five per account/hour, one concurrent download/account, 2 MiB of fetched manifest data/check, and a ten-minute cache keyed by commit/profile. Full archive acquisition, installation and execution occur only inside an admitted, reserved run.

### 3.3 Initial build profiles

| Profile | Preparation | Validation | Limits |
|---|---|---|---|
| Python/pytest | Pinned Python image; hash-pinned wheel dependencies from PyPI; include pytest in the dependency lock | Fixed python -m pytest invocation with bounded output and timeout | Pure-Python root or src-layout projects; import local source through a defined PYTHONPATH; no editable-install/build hooks |
| Node/npm | Pinned Node LTS image; package-lock.json; npm ci --ignore-scripts using the public npm registry | Repository's configured npm test command, CI=true, hard timeout | Single-package projects; no workspaces, native compilation, browser binaries, Git dependencies, or custom install lifecycle requirements |

Profile selection occurs during onboarding. Users cannot submit arbitrary setup shell scripts through the API or comments.

Dependency download is a distinct phase with a restricted network policy. Execution of tests and generated code uses deny-all egress. Explicit test commands still execute untrusted repository code, so disabled install scripts are not treated as a security boundary.

For the selected SDK, apply deny-all through the active session's update_network_policy operation, await completion, refresh the session policy and prove an outbound probe fails before starting repository code. Do not use the sandbox configuration update operation for this transition: its network changes apply to the next session. An unverified or failed policy transition stops the allocation.

Dependency manifests, lockfiles, build-profile configuration, CI workflows, and original test harness configuration are protected from agent edits. The agent can add regression tests and modify permitted source files. Existing tests are retained as independent evidence and cannot be weakened to manufacture a pass.

Baseline tests may fail: fixing an existing failing test is a valid task. Final required checks must pass before publication approval is available. Unsupported setup, flaky verification, and inconclusive validation produce explicit failure or attention states.

Pinned inputs support reproducibility but do not guarantee that upstream packages remain downloadable forever. Missing dependencies stop the run without silently selecting new versions.

## 4. Architecture and deployment

### 4.1 Topology

~~~mermaid
flowchart TD
    Browser[Web console] --> API[FastAPI and static React app]
    GitHub[GitHub webhook] --> Ingress[Small Supabase Edge receiver]
    Ingress --> DB[(PostgreSQL)]
    Wake[Work-triggered wake dispatcher] --> API
    DB --> Wake
    API --> DB
    API --> Runtime[Scheduler and bounded workers]
    Runtime --> DB
    Runtime --> Models[OpenAI / Anthropic BYOK]
    Runtime --> Sandbox[Managed isolated sandbox]
    Runtime --> Store[Private artifact bucket]
    Runtime --> Publisher[Trusted GitHub adapter]
    Publisher --> GitHub
~~~

The API, scheduler, worker, and adapters are modules of one Python application. Local tests can run multiple worker processes. The initial hosted service runs one API process with supervised asynchronous background loops.

Database transactions remain short. No network request, model invocation, package installation, or sandbox command occurs while a transaction waits open.

### 4.2 Stack

| Concern | Selected implementation |
|---|---|
| Python application | Python 3.13, uv, FastAPI, Pydantic v2 |
| Persistence | PostgreSQL; SQLAlchemy 2 Core and psycopg 3; Alembic |
| Queue | Explicit PostgreSQL job rows, leases, and fenced transitions |
| HTTP clients | Bounded async clients; explicit retry ownership |
| UI | React, TypeScript, Vite, React Router, TanStack Query |
| Styling | CSS design tokens and Tailwind utilities; accessible headless primitives where necessary |
| GitHub | GitHub App authentication, REST adapters, signed webhooks |
| Models | Official Python SDKs behind application-owned adapters |
| Sandbox | Official async Vercel Python SDK behind a narrow adapter |
| Artifacts | Private Supabase Storage bucket behind an object-store interface |
| Tests | pytest, Hypothesis, Vitest, React Testing Library, Playwright |
| Telemetry | Structured JSON logs, application metrics, optional OpenTelemetry export |
| CI | GitHub Actions with PostgreSQL service and deterministic fake providers |

Exact dependency patches and images are pinned during bootstrap. CI uses the same PostgreSQL major as the deployed project. Store runtime-image identity, profile version, tool schema version, and provider configuration with each run.

### 4.3 Hosting baseline and cost behavior

Use one Render Free web service with no payment method, one Supabase Free project, and Vercel Sandbox Hobby without automatic upgrade or paid fallback. Use provider subdomains initially.

Render Free can sleep after 15 minutes without inbound activity and take approximately a minute to restart; local files are ephemeral. Its free service is suitable here only because pauses are part of the accepted beta contract. No-payment-method quota exhaustion suspends service rather than enabling ordinary paid bandwidth expansion. [Render Free documentation](https://render.com/docs/free)

Supabase Free supplies a small database and object-store allowance, but no included automatic backups. A paused Supabase project may require operator action to restore; a browser request is not a promise of database recovery. [Supabase pricing](https://supabase.com/pricing), [project pausing](https://supabase.com/docs/guides/platform/free-project-pausing)

Vercel Sandbox Hobby provides recurring free allowances and stops additional creation when exhausted. It is subject to personal/non-commercial eligibility. [Sandbox pricing](https://vercel.com/docs/sandbox/pricing), [fair-use rules](https://vercel.com/docs/limits/fair-use-guidelines)

The target infrastructure bill is zero on these plans. The ₹1,000 ceiling is a budget constraint, not an authorization to upgrade a service. Owner-funded model smoke tests need their own small allowance; public users pay their model providers directly.

Keep an independently reachable status/help document in the public project GitHub repository, initially marked “not launched.” Link it from onboarding and bot comments. It explains cold starts, suspension, pending commands, and operator contact, with manually dated incident updates. A suspended Render service cannot be its own only outage explanation.

Account eligibility, actual billing behavior, and the ability to configure these services are verified before rollout. If a provider no longer offers the required behavior, public deployment is blocked until a revised architecture is approved; do not silently add paid infrastructure.

### 4.4 Wake and scheduling policy

Use work-triggered wakeups, not artificial keepalive.

- While alive, the Python scheduler finds eligible database work.
- A small scheduled database/edge dispatcher checks for actionable inbox entries, due jobs, or expired active leases and requests an authenticated wake of the API.
- Idle queues, ordinary approval waits, and known monthly capacity exhaustion do not generate repeated wake requests.
- Due expiry and cleanup are also actionable work, processed in bounded batches.
- Apply backoff to failed wakes and expose the last successful drain time.
- Database or provider suspension can delay all processing. No deadline is guaranteed to execute exactly on time; all validity checks use current database time when processing resumes.

The dispatcher contains no workflow business logic. It cannot create model calls, approve changes, or publish code.

### 4.5 Module ownership

| Module | Owns | Must not own |
|---|---|---|
| Identity | Users, sessions, GitHub connection, resource authorization | Workflow transitions |
| Commands | Normalized submissions and decisions from web/comments | Long-running execution |
| Runtime | Jobs, state transitions, attempts, waits, generation checks | Provider-specific payload formats |
| Agent | Context assembly, persisted turns, tool dispatch decisions | Authorization policy |
| Policy | Capabilities, protected paths, limits, approval validity | LLM reasoning |
| Sandbox | Allocations, commands, filesystem transfer, teardown | GitHub/model credentials |
| Artifacts | Hashes, manifests, retention, access-controlled downloads | Execution ownership |
| GitHub | Repository reads, publication, effect observations, comments | Blind generic retries |
| Providers | OpenAI/Anthropic protocol translation | Durable orchestration |
| Operations | Admission, cleanup, diagnostics, exports, recovery tools | Unreviewed user-content editing |

## 5. Domain model and persistence

### 5.1 Main entities

| Entity | Essential fields and responsibility |
|---|---|
| User | Internal UUID, immutable GitHub user ID, status, created time |
| Session | Hashed opaque session token, user, expiry, revocation |
| GitHub connection | User authorization, installation IDs, encrypted refresh/access material, checked permissions |
| Repository registration | User, GitHub repository ID, profile version, defaults, eligibility result |
| Credential | User, provider, encrypted bytes, key version, saved/per-run scope, revoked time |
| Workflow release | Immutable workflow/profile/tool/prompt compatibility manifest |
| Run | User, repository, issue/task snapshot, base SHA, provider/model configuration, status, revision, deadline |
| Step | Run, registered activity, logical key, revision, status, input/output references |
| Job | Step, availability, owner, lease expiry, monotonically increasing generation, retry policy |
| Attempt | Job, generation, timestamps, classification, output references, observed errors |
| Agent turn | Run/revision, model intent, normalized response, provider envelope, usage and uncertainty |
| Tool invocation | Stable call ID, typed arguments, policy decision, checkpoint references |
| Sandbox allocation | Stable attempt identity, provider ID/session, reserved lifetime, observed state |
| Artifact | Owner/run, immutable object key, SHA-256, bytes, kind, retention and reference state |
| Checkpoint | Source-tree manifest, archive reference, parent, creating invocation, compatibility version |
| Approval | Run/revision, immutable manifest digest, pending/decision state, expiry, actor |
| External operation | Stable logical key, action digest, remote identity, mutation state, observations |
| Event | Per-run sequence, type, actor, timestamp, bounded payload |
| Inbox/command | Delivery/comment/idempotency identity, accepted payload digest, processing outcome |
| Quota reservation | Account/global period, resource, amount, reservation/consumption/uncertainty |

Keep large content in object storage. Database JSON is for bounded structured metadata, not full source archives or unbounded logs.

### 5.2 Database invariants

Use foreign keys, explicit check constraints, unique logical-operation keys, and indexed ownership columns.

- One active run per account.
- One active issue run for a given account/repository/issue.
- One authoritative result per logical step/tool/model operation.
- One attempt per job generation.
- One event sequence per run.
- One pending approval for the current candidate revision.
- One external-operation record per logical effect.
- One accepted command per repository/comment ID.
- One inbox record per GitHub App/delivery ID.
- One quota reservation per logical charged operation.

Use UTC timestamps and database time for leases, expiry, and admission periods. UUIDs identify application records; GitHub numeric IDs identify remote resources.

All account-owned database access passes through repositories requiring an explicit principal. Keep application tables outside publicly accessible API schemas. Browser clients never receive database service credentials or direct unrestricted table access.

### 5.3 Run states

| State | Meaning and exit |
|---|---|
| QUEUED | Admitted and waiting for eligible execution |
| RUNNING | A bounded activity has a current owner |
| WAITING_APPROVAL | Immutable candidate is ready for a human decision |
| WAITING_CREDENTIALS | No usable provider/GitHub credential; owner can replace or cancel |
| WAITING_CAPACITY | Work is durable but execution capacity is unavailable |
| NEEDS_ATTENTION | Conflict, corrupted state, unsupported upgrade, or unresolved external effect |
| SUCCEEDED | Approved publication was observed and adopted |
| FAILED | Task or supported execution policy failed definitively |
| CANCELLED | New execution is revoked; remote effects may still need observation |
| EXPIRED | Run or approval deadline elapsed |

Terminal states never reopen. Recovery requiring a new attempt at the overall task creates a new run linked to the previous one.

Track external-effect uncertainty separately from terminal run state. “Cancelled; publication outcome unresolved” is a valid and visible combination.

### 5.4 Registered workflow

~~~text
admit
  → pin repository and task
  → prepare and baseline
  → durable agent loop
  → independent candidate validation
  → freeze publication manifest
  → wait for approval
      approve → publish/reconcile → succeed
      changes → next bounded revision → agent loop
      reject  → failed(reason=user_rejected)
      expire  → expired
~~~

Cancellation can occur in any nonterminal state. Retry waits are job availability timestamps, not sleeping coroutines.

## 6. Durable execution protocols

### 6.1 Atomic transition boundary

A successful activity transaction commits its accepted result, relevant state transition, event, and next eligible job together. Approval decisions and cancellation use the same transition service.

The periodic scheduler repairs eligible work idempotently, fires due waits, and recovers expired leases. It does not depend on an in-memory notification for correctness.

Use a documented lock order: quota/account rows when needed, then run, step, job, external-operation, and artifact rows; sort multiple rows of one class by stable ID. Job claim first locks the selected run, then its eligible job, with skip-locked selection and revalidation. Do not acquire a run lock after holding a job lock.

Retry deadlocks/serialization conflicts by restarting the entire database-only transaction, at most three times with jitter. Never replay a remote mutation as part of that retry.

### 6.2 Claims, leases, and fencing

Initial settings: 60-second lease, 15-second heartbeat, worker claim batches of one.

Claiming increments the job generation and creates an immutable attempt. Every durable write from execution carries the run, job, generation, and lease owner. Accept it only if the generation is current, its lease is unexpired, and the run still allows that transition.

Fencing applies to intermediate model responses, tool results, checkpoints, and quota changes, not only final job completion. An expired worker cannot revive itself by sending a late heartbeat.

On heartbeat failure, stop dispatching new external calls and request sandbox termination. A worker may continue physically running after losing ownership; database rejection remains the correctness boundary.

### 6.3 Activity recovery

1. Inspect durable logical-operation state.
2. Reuse an accepted result if present.
3. Reconcile an outstanding remote allocation/effect before creating replacements.
4. Restore the latest committed recoverable checkpoint for local work.
5. Execute under a new attempt identity if policy permits.
6. Publish only through the current fenced transaction.

Attempt count and failure retry count are distinct. Reclaiming a crashed worker creates an attempt; it does not automatically consume the same policy budget as a definitive tool failure.

### 6.4 Error classification

| Class | Policy |
|---|---|
| Invalid input/policy denial | No retry; show actionable error |
| Missing/revoked credential | Wait for credentials; do not spend |
| Definite transient read failure | Up to three attempts with full jitter and provider delay headers |
| Unsupported environment | Fail eligibility/setup; no model workaround |
| Resource exhaustion | Stop allocation or wait for capacity, depending on whether run allowance remains |
| Model response lost | Record unknown usage; permit at most one explicitly accounted regeneration within limits |
| Non-idempotent write outcome unknown | Stop mutation retries; reconcile by reading |
| Conflict with remote state | Needs attention; never force overwrite |
| Missing/corrupt committed artifact | Needs attention; never silently start from a different source state |

Start transient backoff at one second, cap ordinary delay at 60 seconds, and honor longer provider retry-after values by persisting a future availability time. Retry only before applicable run/action deadlines.

### 6.5 Cancellation and expiry

Cancellation commits before responding to the caller: mark the run cancelled, invalidate current execution generations, prevent new dispatch authorizations, and enqueue best-effort sandbox stop and effect observation.

The dispatch-intent commit is the authorization boundary. A worker paused immediately after that commit may send its already-authorized request after cancellation. Neither PostgreSQL fencing nor a final permission check can make the database and remote service atomic. Such requests remain accounted for and are reconciled; the UI must not claim all external activity stopped instantly.

Cancellation releases the account's active-run slot. It does not release an uncertain sandbox allocation or erase a possibly completed remote effect. A separate resource ledger prevents those leftovers from being mistaken for free capacity.

Late observations may update the external ledger and append audit events without changing the terminal cancellation result. No reconciliation path creates another mutation for a cancelled run.

Run lifetime is seven days from admission, including queue, credential, capacity, and approval waits. Each approval lasts 48 hours, capped by the run deadline. Delayed commands are judged using acceptance time, not their original comment timestamp.

## 7. Agent execution, tools, and evaluations

### 7.1 Model adapter contract

Both providers implement the same application interface: submit persisted context and tool schemas, return a normalized response, retain provider-specific continuation material, and classify errors.

Persist requested/returned model IDs, response/request IDs where supplied, visible response text, ordered tool calls, stop reason, token categories, latency, and uncertainty. Do not flatten away call IDs or continuation blocks needed by a provider.

OpenAI uses Responses custom function calling, strict schemas, store=false, and disabled parallel tool calls. Anthropic uses Messages client tools and application-owned tool execution. Validate arguments locally for both. [OpenAI function calling](https://developers.openai.com/api/docs/guides/function-calling), [Anthropic tool use](https://platform.claude.com/docs/en/agents-and-tools/tool-use/how-tool-use-works)

Start with one tested model configuration per provider: OpenAI gpt-5.4-mini-2026-03-17 and Anthropic claude-sonnet-5-5, based on current documentation. Validate account access and task performance before enabling either in a release. These are chosen starting configurations, not claims of universal superiority. [OpenAI model documentation](https://developers.openai.com/api/docs/models/gpt-5.4-mini), [Anthropic models](https://platform.claude.com/docs/en/models/overview)

Pin the model configuration to the run. No silent provider fallback or model switch occurs after recovery. If a provider does not offer immutable model identity, document that limitation; persisted accepted responses, rather than repeated inference, preserve progress.

Disable implicit SDK retries so the runtime owns attempt accounting.

### 7.2 Durable turn protocol

1. Build context only from the pinned task, bounded file/tool results, accepted prior turns, current revision feedback, and policy metadata.
2. Reserve request/token allowance and persist the model intent.
3. Call the selected provider outside a transaction.
4. Persist a complete validated response through fencing.
5. Materialize stable tool invocation IDs before executing tools.
6. Execute tools sequentially.
7. Persist each accepted result and any new checkpoint.
8. Advance only after all calls in that accepted turn have durable outcomes.

Partial JSON, truncated arguments, or incomplete streamed responses never execute. A malformed response may receive one repair turn within the same overall limits.

The agent uses bounded recent context and structured summaries of older visible results. It cannot modify policy, allocate more budget, choose credentials, or authorize publication.

### 7.3 Native tools

Expose typed tools for directory listing, bounded text reading, text search, patch application, diff inspection, and named validation commands.

Do not expose unrestricted shell strings, arbitrary HTTP requests, package installation, GitHub writes, credential access, or host filesystem paths as model tools.

Tool authorization is the intersection of the registered workflow capability, repository profile, account permission, run limits, and current stage. The model's request is never itself authorization.

### 7.4 Validation integrity

Tests run in a fresh workspace restored from the immutable candidate. Discard their filesystem changes. Approval references that original candidate, not post-test files.

Retain baseline results and protect the original test suite/configuration. Regression tests added by the agent supplement independent evidence. For fixture evaluations, hidden acceptance checks live outside the agent-writable workspace.

For new regression tests, attempt to demonstrate failure on the original source and success on the candidate within the run allowance. If this evidence cannot be obtained, label it absent rather than fabricating confidence.

### 7.5 Evaluation set

Create 12 maintained tasks: six Python and six JavaScript/TypeScript. Include ordinary fixes, malformed instructions, malicious repository text, unsolvable tasks, baseline failures, and budget exhaustion.

Run deterministic scripted-model cases in ordinary CI. Run paid model evaluations manually with the owner's separately budgeted keys.

Record task completion, independent test success, unauthorized-action attempts, tool errors, token usage, elapsed active time, patch size, and human review outcome. Publish actual results for both providers, including failures. Do not use an LLM judge as the sole correctness oracle.

The live release gate is four successful protocol/product smoke runs: one Python and one Node task for each provider. The entire 12-task corpus runs deterministically; additional live corpus results are reported as measured, not implied by scripted success. Trusted maintained fixtures can run in local Docker for model-quality evaluation, clearly labelled as local evidence. Public/untrusted repository execution always uses managed isolation. Hosted checks consume the shared operator sandbox allowance; tests stop or continue in a later billing period if allowance is insufficient.

## 8. Sandbox and artifact protocols

### 8.1 Isolation boundary

All dependency installation, tests, and agent-generated code execute in managed sandboxes. The API host never executes repository scripts.

Sandbox environments contain no database credential, bucket master key, model key, GitHub token, application signing key, or unrelated user data. Expose no ports and mount no control-plane filesystem.

Configure resource and server-side time limits before starting work. Use session-scoped handles so a stale attempt cannot accidentally resume a stopped sandbox through an SDK convenience operation. The provider API supports asynchronous process control and file transfer; adapter contract tests must verify actual lifecycle behavior. [Vercel Python SDK](https://vercel.com/docs/sandbox/python-sdk-reference)

### 8.2 Allocation protocol

Reserve capacity before creating a sandbox. Persist the attempt's stable provider name/tag and maximum allocation duration.

If creation succeeds but its response disappears, mark the allocation unknown. Query by stable identity. Do not allocate a replacement until the previous allocation is observed stopped/destroyed. A locally elapsed timeout is insufficient because a delayed create request may start remotely later. If terminal state cannot be established, retain the reservation and require operator investigation.

A global database semaphore is insufficient by itself: a lost remote sandbox may still consume resources. Unknown allocations retain their conservative reservation.

Each session receives a maximum lifetime no greater than the run's remaining reservation. A shorter per-command timeout bounds subprocesses. Stop and destroy unneeded sessions promptly, especially before human waits.

### 8.3 Checkpoint commit

After a successful mutating tool:

1. Stop concurrent workspace activity.
2. Produce a bounded source-only archive and canonical file manifest.
3. Preserve permitted relative paths, file content, and executable bits.
4. Reject links, traversal, duplicate/case-colliding paths, special files, and size violations.
5. Calculate archive SHA-256 and a canonical source-tree digest separately.
6. Upload the immutable object and verify its metadata/checksum.
7. Commit its reference, tool result, and event through fencing.

An upload without a committed reference is an orphan. A committed reference whose object cannot be verified is a recovery error.

Disable persistent provider snapshots and drives in the initial adapter. Recovery uses the committed source archive, reinstalling pinned dependencies within the same finite allowance. Destroy allocations with explicit orphan-snapshot deletion and verify cleanup; do not rely on the provider's default destroy behavior to remove every copy. A future snapshot optimization needs a separate retention review.

### 8.4 Retention and garbage collection

Retain the latest two committed recoverable checkpoints plus any checkpoint pinned by an active approval, validation, or publication. Pins remain valid only until user deletion or the content deadline; unresolved effects cannot extend source retention indefinitely. Older checkpoint events retain digests and provenance, but are explicitly marked non-restorable once their objects are collected.

Store logs with per-command and per-run caps. Redact credentials and common secret forms before persistence; redaction is defense in depth, not permission to pass credentials to repository code.

Reserve each upload's bounded maximum bytes transactionally before transfer. Existing objects, uploads in progress, unknown uploads and pinned checkpoints all count against the 600 MiB storage budget. Reject or pause the upload before dispatch if the reservation cannot fit.

Garbage collection uses upload records, a one-hour orphan grace period, and idempotent deletion. Lock and recheck references before marking an object DELETING; new references cannot attach to it afterward. It must not remove an in-flight upload, approved candidate, or active recovery checkpoint before their retention/deletion boundary.

Run content expires seven days after terminal completion; a run can live at most seven days before that, so content may exist for up to fourteen days from admission. Show both the active-run deadline and final content-expiry date in onboarding and the console. Access checks deny expired content even if cleanup is delayed. Minimal command/effect tombstones may remain for 90 days without source, prompts, logs, or keys.

The content inventory includes source archives, patches, issue/task snapshots, feedback, model responses and continuation envelopes, test output, content-bearing events, and provider-side application-controlled copies. Raw webhook payloads expire after processing plus 24 hours and never later than 72 hours after receipt; accepted task content is copied into its governed run record. Delete associated raw bodies during owner deletion as well.

When a source/manifest body expires, retain only minimal remote IDs, digests, status and actor provenance for unresolved effects. Read-only observation may continue with current authorization; no missing source is reconstructed to enable a new mutation.

Cleanup availability does not imply immediate physical erasure from provider backups or logs. Publish the distinction between access expiry, active-store deletion, and provider retention.

## 9. GitHub authorization and external effects

### 9.1 Identity and permission

Use GitHub App user authorization for user-directed repository actions, plus explicit installation access. GitHub recommends user access tokens for operations on behalf of a user; installation credentials alone do not prove the user has the same rights. [GitHub App security practices](https://docs.github.com/en/apps/creating-github-apps/about-creating-github-apps/best-practices-for-creating-a-github-app)

Request only required repository permissions: metadata read, contents read/write, pull requests read/write, and issues read/write. Do not request workflow administration or organization-wide membership administration.

Match identities through numeric IDs. Recheck current repository rights at submission, approval, and publication. The run owner is the only beta reviewer/canceller. A collaborator cannot spend another person's model key.

Installation credentials may identify the bot when posting status comments, but only after the underlying command has passed user authorization.

### 9.2 Exact publication manifest

Before approval, freeze:

- Repository ID, base branch and base commit.
- Candidate source-tree/checkpoint identity and patch digest.
- Deterministic Git tree/commit content and desired commit SHA.
- Author/committer identity, timestamps, and commit message.
- Managed branch name containing the full run UUID.
- PR title/body/base/head, including the logical-operation marker.
- Validation evidence and profile version.
- Requested remote operations, approval expiry, and policy version.

Compute deterministic Git objects in a clean trusted environment without loading repository Git hooks/configuration. Upload content-addressed blobs/trees/commits through the trusted GitHub adapter only after approval. Repository commands never receive a publication credential.

The approval digest is SHA-256 of canonical serialized manifest data. Both web and GitHub decisions submit that exact approval ID and digest.

The manifest deadline constrains both acceptance of approval and every subsequent publication dispatch authorization. After that deadline, read-only reconciliation is allowed, but no unfinished mutation may be newly authorized. Requests authorized earlier may still finish. Report partial publication and expiry honestly; completing it requires a new reviewed run, not revival of the expired approval.

### 9.3 External-operation ledger

Use PREPARED, DISPATCHING, CONFIRMED, DEFINITELY_FAILED, UNKNOWN, and CONFLICT states. Keep append-only observations and actor provenance.

Before sending a mutation, commit its intent and ownership. Disable generic HTTP retries on mutations. Persist success only through the matching fenced operation.

If a response is lost or a sending attempt disappears, the result is unknown. A subsequent negative lookup does not prove the original mutation cannot still appear.

Reconciliation may read remote state repeatedly with bounded backoff. It may adopt an exact matching result. It must not issue a replacement non-idempotent write merely because the result is not yet visible.

### 9.4 Operation-specific behavior

| Operation | Recovery rule |
|---|---|
| Content-addressed blob/tree/commit | Verify identity; re-uploading identical content is safe under the adapter's explicit content-addressing contract |
| Managed branch creation | Adopt only the approved SHA; foreign/diverged branch is a conflict; never force overwrite |
| PR creation | Search all relevant states/pages by repository, head/base, marker, and approved identity; adopt one exact match |
| Closed or merged matching PR | Record its observed state; do not create another PR or reopen it |
| Multiple matching PRs | Needs attention; do not choose silently |
| Status-comment creation | Stable marker and ledger; reconcile lost response before another creation |
| Base branch moved before publication | Stop; require a new run against the new base instead of silently rebasing approved content |

An unresolved effect can be acknowledged or the run abandoned by an operator, but “force retry unknown create” is not an available beta action.

No global exactly-once claim is made for GitHub. The implementation proves its dispatch/reconciliation policy against a controlled remote service and records live-provider limitations.

### 9.5 GitHub command interface

Accept exact commands on ordinary issue comments:

~~~text
/continumm run
/continumm status <run-id>
/continumm cancel <run-id>
/continumm approve <approval-id> <action-sha256>
/continumm reject <approval-id>
/continumm changes <approval-id>
Required feedback on following lines.
~~~

Only a newly created comment can introduce a command. Edits do not execute new intent; deletion does not undo accepted intent. Fetch and compare the current comment before first acceptance; skip edited/deleted or unverifiable comments.

Accept creation events only within 72 hours of the signed comment creation timestamp, with at most five minutes of future clock skew, and never from before the account and repository registration. This freshness rule applies even after old tombstones are collected. A legitimate command outside the window requires a new comment.

Ignore bot authors, quoted commands, code-fenced commands, and natural-language mentions. Each command is scoped to its repository/issue and current authenticated owner.

The run command uses repository defaults and the author's own model credentials. An unlinked user or unregistered repository receives at most a rate-limited setup link and cannot create pending runs. Missing profile/provider defaults also require setup before run admission.

For a registered, configured account lacking a key, create one WAITING_CREDENTIALS run with the original command identity and return a console link. It consumes the account slot and global queue slot immediately and has the ordinary seven-day lifetime; it consumes sandbox-run allowance only when its first sandbox dispatch is authorized. Completing setup/key entry resumes that same run. Never ask for API keys in comments.

Bot comments contain concise status, review IDs/digests, expiry, and links. Patches, task details, and logs remain in the authenticated console until explicitly published. Comments and PRs are public GitHub records and are not removed by Continumm content expiry.

### 9.6 Durable webhook ingress

A Supabase Edge Function verifies raw-body HMAC signatures and writes a bounded private inbox record before acknowledging. Reject invalid signatures or oversized payloads; return a failure when the database commit fails.

Deduplicate transport by App/delivery ID and logical commands by repository/comment ID. Redelivery must not create a second run or consume another quota reservation.

GitHub expects a response within ten seconds and does not automatically redeliver failed events. Do not send its webhooks directly to a sleeping Render process. [GitHub failure handling](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries)

Use a bounded five-minute recovery scan of failed App deliveries, with an overlapping watermark and deduplicated redelivery requests. GitHub's delivery inspection window is limited; after a gap beyond that window, require users to post new commands rather than infer approval from mutable old text. [Delivery inspection](https://docs.github.com/en/webhooks/testing-and-troubleshooting-webhooks/viewing-webhook-deliveries)

Ingress, wake, and recovery functions are tiny plumbing components. Their secrets and database privileges are separated by responsibility.

## 10. API and frontend contract

### 10.1 HTTP surface

| Method and resource | Purpose |
|---|---|
| GET /auth/github/start, GET /auth/github/callback | GitHub authorization flow |
| POST /auth/logout, GET /api/v1/me | Session lifecycle |
| GET/POST /api/v1/repositories | List/register authorized repositories |
| PATCH /api/v1/repositories/{id} | Update owner-scoped profile/provider/credential defaults for future runs |
| POST /api/v1/repositories/{id}/check | Eligibility and profile check |
| GET/POST /api/v1/credentials | List metadata or save; never return stored secret bytes |
| DELETE /api/v1/credentials/{id} | Revoke an owned credential |
| POST /api/v1/runs | Admit an issue/task with provider/profile/credential selection |
| GET /api/v1/runs, GET /api/v1/runs/{id} | Owned run list and current snapshot |
| POST /api/v1/runs/{id}/cancel | Durable cancellation |
| POST /api/v1/runs/{id}/credential | Bind an owner-supplied replacement credential |
| GET /api/v1/runs/{id}/events | Cursor-based event history and SSE |
| GET /api/v1/runs/{id}/review | Immutable candidate and validation evidence |
| POST /api/v1/approvals/{id}/decision | Approve, reject, or request changes |
| GET /api/v1/artifacts/{id}/download | Authorized download |
| DELETE /api/v1/runs/{id}, DELETE /api/v1/me | Content/account deletion workflow |
| GET /health/live, GET /health/ready | Process/database readiness without secrets |

These endpoints support Continumm's application. They are not a supported general workflow SDK.

### 10.2 Boundary rules

Use strict Pydantic schemas and explicit error codes. Return 202 only after a durable command/run commit. Every mutation checks account ownership. Repository reads, new work, review approval and publication also require applicable current GitHub permission; owner cancellation, credential revocation and privacy/deletion actions remain available without it.

Require an idempotency key for run submission and approval decisions. Scope it by user and route, retain it for 24 hours, and return conflict if the same key carries different input. Workflow/effect identities persist independently of that API cache.

Use opaque cursor pagination, a maximum page size of 100, bounded request bodies, and stable machine-readable errors containing code, message, retryable, and request_id.

Initial body limits are 64 KiB for application JSON, 16 KiB for task/feedback text, 1 MiB for a webhook, and 8 KiB for an event payload. Large evidence is an artifact reference. Return 404 for another account's identifiers rather than reveal whether its records exist. Keep same-account validation errors actionable.

Concurrent web/comment submission of the same active issue run returns the existing run. Free-text tasks are deduplicated by request identity, not by assuming identical wording always means the same intent.

### 10.3 Session and browser security

Use hashed opaque server sessions in Secure, HttpOnly, SameSite cookies. Rotate sessions after login; expire after seven days and revoke on logout or account deletion.

Validate OAuth state, callback origin, and any supported PKCE mechanism. Use a maintained OAuth client rather than constructing token exchange logic ad hoc.

Same-origin frontend hosting simplifies the browser boundary. Apply CSRF tokens and Origin checks to state changes, a restrictive CSP, safe text rendering, and no raw HTML rendering of model/repository content.

### 10.4 Events

Events have run_id, sequence, type, timestamp, actor, and versioned bounded payload. Allocate sequence numbers while holding the run lock.

SSE supports Last-Event-ID and replay from stored events. Clients deduplicate by sequence and reconcile against the latest run snapshot after gaps. An expired replay cursor returns an explicit history-expired response.

Limit to two streams per user and page event history. Reconnect with exponential backoff; stop reconnecting aggressively during known suspension. Do not present SSE output as proof the free backend will stay awake.

### 10.5 Screens and interaction

| Screen | Required content |
|---|---|
| Public overview | Concrete product behavior, supported environments, beta availability, BYOK, demo evidence |
| Onboarding | GitHub identity, selected installation, repository eligibility, provider key and disclosure |
| Run list | Status, repository/task, latest action, expiry, capacity; no ornamental metrics |
| Create run | Issue/task, pinned profile, provider, key choice, limits, admission outcome |
| Run detail | Current state, causal timeline, attempts, usage, recovery/attention explanation |
| Review | File diff, baseline/final tests, exact publication manifest, approve/reject/changes |
| Settings | Connected repositories, saved keys, defaults, limits, deletion |
| Operator view | Queue, stale allocations, unresolved effects, storage, failed deliveries; restricted operator access |

Use a light neutral canvas, near-black text, restrained blue primary actions, 14–16px interface text, monospace code, thin separators, and a small consistent radius. Keep status colors semantic and always pair them with text/icons.

The memorable interface element is the causal timeline: it explains an interruption, rejected stale result, restored checkpoint, and subsequent action in one sequence.

On desktop, place a navigable file list beside the diff and validation summary. On narrow screens, stack those sections and keep review actions reachable. Do not use a mobile code editor as a launch requirement.

All flows require keyboard navigation, visible focus, labelled inputs, readable errors, reduced-motion support, and screen-reader status announcements. Focus moves to the first invalid field. Review submissions show the exact manifest and remain idempotent on retries.

Include loading, empty, disconnected, expired, forbidden, capacity-exhausted, missing-key, unresolved-effect, and content-deleted states in the UI tests.

## 11. Credentials, privacy, and lifecycle

Encrypt provider and GitHub refresh credentials with authenticated encryption under a versioned master key kept in deployment secrets, outside the database. Bind encrypted values to owner/provider/scope as associated data. Use a unique nonce per encryption.

Saved keys require explicit opt-in. Per-run keys are also encrypted durably so recovery can continue; “per-run” does not mean kept only in RAM. Delete per-run model credential material at terminal transition, with an idempotent repair sweep.

Revoking a key prevents future dispatch immediately. It cannot recall a provider request already sent. A run needing the removed key becomes WAITING_CREDENTIALS.

Provider keys never enter the sandbox, prompts, events, traces, URLs, browser storage, or downloadable evidence. Restrict provider endpoints to their official configured hosts; arbitrary base URLs are not accepted.

Keep provider-side retention separate from Continumm retention. OpenAI store=false does not itself eliminate abuse-monitoring retention; Anthropic retention varies with current account/model/feature policy. Link current provider disclosures during onboarding rather than promise zero retention. [OpenAI data controls](https://developers.openai.com/api/docs/guides/your-data), [Anthropic data retention](https://platform.claude.com/docs/en/manage-claude/api-and-data-retention)

“Private run” means inaccessible to other accounts and unpublished by default, not end-to-end encrypted against the service operator. Disclose that source/task content is processed by the chosen model provider and that authorized service administration can access retained application data. Do not send full repository content when bounded relevant excerpts suffice.

Run deletion first revokes future execution and access, then schedules artifact/content cleanup. Account deletion revokes sessions and credentials and cancels owned runs. Preserve minimal non-secret tombstones needed to prevent replay and identify unresolved external effects.

Write deletion receipts to a private append-only journal outside portable exports before marking deletion processing complete. A receipt contains opaque record IDs and deletion time, not source or prompt content. Restores must use the current journal; if it is unavailable, restore non-content metadata only and discard user content. An export is not allowed while deletion receipts are waiting to be journaled.

Observation of an unresolved GitHub effect uses separately valid GitHub authorization or requires reauthentication; deleting a model key does not eliminate that observation path.

## 12. Resource limits and operating policy

### 12.1 Conservative initial limits

| Resource | Starting limit |
|---|---|
| Concurrent sandbox allocations | One globally; unknown allocations hold capacity until observed terminated |
| Active run | One per account |
| Queue | Ten admitted nonterminal runs globally |
| Public executions | Eight sandbox-starting runs per provider billing period globally; two per account |
| Sandbox allowance | Sum of all allocations at most 900 seconds/run at two vCPUs, including setup, retries, orphan reservations, and revisions |
| Single command | At most 120 seconds and never above remaining allocation |
| Model requests | Thirty/run, including repairs and retries |
| Model output | At most 4,096 output tokens/request; additional provider-specific limits recorded |
| Aggregate model input/output | 120,000 input and 30,000 output tokens/run, with conservative reservations |
| Revision requests | Two |
| Repository source archive | 10 MiB compressed, 50 MiB expanded, 5,000 files |
| Individual readable file | 1 MiB; bounded excerpts enter context |
| Logs | 256 KiB/command and 2 MiB/run |
| Run lifetime | Seven days from admission |
| Approval validity | 48 hours, never beyond run deadline |
| Content retention | Seven days after terminal outcome |
| Storage admission stop | 600 MiB artifacts or 300 MiB application database usage |

Eight runs × 900 seconds × two vCPUs is four CPU-hours of conservative reserved compute. This calculation is only useful when all allocations—including failed/unknown creations and rework—count against it. It is not a measured cost estimate or throughput benchmark.

Public, operator, smoke-test and live-evaluation allocations share one provider-period ledger. Reserve at most four CPU-hours for public executions and at most one CPU-hour for operator work; neither pool borrows the other implicitly. Stop hosted tests when their allowance is exhausted. Other provider limits, such as transfer and memory-time, can stop admission earlier.

Reserve global and account allowance before dispatch, under database locks. A queue-full rejection occurs before reservation. Refund only resources proven undispatched; uncertain operations retain their maximum reservation. Active cross-period runs retain their remaining reservations in the next period before new admissions.

This is a deliberately small public beta, not unlimited public usage. The console shows remaining admission capacity and why new work is unavailable.

### 12.2 Model spending

Users pay their selected provider directly. The application limits requests and tokens and shows estimated usage, but cannot impose a guaranteed total cap on the user's provider account.

Reserve maximum configured output before a call. Count input using the provider's supported counting method where available and a conservative fallback otherwise. Unknown requests consume their reserved allowance. Stop rather than bypass a token limit after a timeout.

No owner-funded model fallback is permitted. User keys never subsidize another user's work.

### 12.3 Abuse handling

Require GitHub authentication and current repository write permission. Rate-limit admission, authorization callbacks, webhook payloads, and downloads. Apply account and IP limits without trusting a browser-supplied identity.

The operator can suspend an account and stop new admissions globally. Existing uncertain allocations remain accounted for. Captchas or paid anti-abuse infrastructure are not prerequisites; add them only if observed abuse justifies a separately reviewed change.

## 13. Testing, fault injection, and proof

### 13.1 Test layers

- Unit tests: state transitions, policy, manifest canonicalization, error classification, and resource accounting.
- PostgreSQL integration tests: real transactions, unique constraints, lock ordering, claims, expiry, and event sequencing.
- Property tests: generated transition sequences preserve ownership, authorization, terminality, and quota invariants.
- Adapter tests: recorded protocol fixtures and deterministic fake GitHub/model/sandbox services.
- Process tests: separate worker processes, actual SIGKILL, paused workers, and controlled response loss.
- UI tests: account separation, review races, expiry, recovery states, and accessible interaction.
- Deployed smoke tests: free-host sleep/wake, edge ingress, actual sandbox timeouts, artifact restoration, both providers.

Never substitute SQLite for PostgreSQL concurrency proof.

### 13.2 Required failure matrix

| Scenario | Required outcome |
|---|---|
| Two workers claim the same logical work | One current generation; no duplicate accepted result |
| Worker killed before checkpoint commit | Restore prior committed checkpoint |
| Upload succeeds, database commit fails | Object is orphaned and later collected safely |
| Database commit succeeds, acknowledgment disappears | Recovery adopts committed result |
| Paused worker resumes after lease takeover | All stale writes rejected |
| Heartbeat and expiry race | Database-time ownership rule decides consistently |
| Model output is partial or malformed | No tool execution; bounded repair/failure |
| Model response is lost | Unknown usage recorded; no unaccounted retries |
| Tool call repeated after recovery | Stable invocation identity prevents duplicate accepted effects |
| Sandbox creation response disappears | Unknown allocation consumes reservation; no overlapping speculative replacement |
| API dies while a sandbox command runs | Server-side limit terminates it; stale result cannot commit |
| Validation mutates files | Approved tree remains the original immutable candidate |
| Approval and cancellation race | No new dispatch authorization after cancellation; previously authorized effects remain observable |
| Approval expires during partial publication | No new mutation authorization; preserve partial outcome and read-only reconciliation |
| Approval and rework race across channels | Old approval cannot authorize revised content |
| GitHub creates PR, response disappears | Reconciliation adopts exact PR; no blind second create |
| GitHub hides a created PR temporarily | Negative reads do not authorize another create |
| Branch diverges or base changes | Conflict/new-run requirement; no force push or silent rebase |
| Installation/user permission revoked | Further access and publication denied |
| Bot/unauthorized commenter issues commands | No account spend or mutation |
| Authentic old webhook replayed after tombstone expiry | Freshness check denies admission and spend |
| Repository authorization removed | Owner can still cancel, revoke keys and delete content |
| Repeated onboarding checks | No sandbox/model allocation or unbounded download |
| Webhook commit succeeds, ack is lost | Redelivery is a no-op at command level |
| Render asleep during webhook | Edge receiver commits independently; work processes after wake |
| Supabase unavailable | No false webhook acceptance; visible recovery gap |
| Key removed during inference | No future calls; already-sent call remains accounted |
| Storage/quota exhausted | Stop admission; preserve committed progress |
| Artifact missing/corrupt | Needs attention; no silent substitution |
| Archive traversal, links, zip bomb | Reject before trusted filesystem use |
| Prompt injection requests secrets/publication | Deterministic policy denies access |
| Another account guesses IDs | Run, stream, artifact, and decision access denied |
| Cleanup races with active upload/review | Referenced/in-flight data survives |
| Runtime version changes mid-run | Unsupported work pauses; no silent semantic upgrade |
| Restored database predates remote publication | Restore quarantine and remote reconciliation precede mutation |
| Deleted content appears in an older export | Current deletion journal/expiry prevents restored access |
| Render suspended | Independent status/help document remains reachable |

Use barriers and deterministic fault hooks rather than timing-dependent sleeps wherever possible. A killed worker and a paused stale worker are separate demonstrations.

### 13.3 Core invariant ledger

Assign stable IDs and map tests to these invariants:

I01 current-generation writes only; I02 atomic state/result/event advancement; I03 stable logical-operation identity; I04 no model tool from incomplete output; I05 immutable checkpoint references; I06 exact approval binding; I07 current actor authorization; I08 no blind unknown-write retry; I09 terminal runs never reopen; I10 cancellation blocks new dispatch authorizations; I11 quotas reserve before dispatch; I12 uncertain allocations remain charged; I13 cross-account isolation; I14 secrets remain outside tools/context; I15 event order is unique; I16 replayed commands do not spend twice; I17 cleanup respects valid pins and mandatory deletion/expiry; I18 unsupported code versions cannot claim work; I19 validation cannot change approved bytes; I20 expired approvals never revive.

### 13.4 Measurements

Build a naive in-memory sequential runner using the same scripted workload as the durable runtime. Compare restart behavior, repeated work, failure outcome, and durability overhead.

Benchmark queue-only workloads separately from sandbox/model workloads. Locally measure 1, 2, and 4 workers with fixed fixtures; report throughput, p50/p95 dispatch latency, recovery latency, query load, and storage growth. Do not load-test public free services aggressively.

Every report includes commit, machine, database/runtime versions, configuration, workload, seeds, trial count, raw results, and limitations. Do not publish stable p99 claims from a tiny sample.

Targets while the backend/database are healthy and awake: p95 queue dispatch under two seconds, p95 event visibility under two seconds, and ordinary lease recovery within 90 seconds. These are test targets, not measured achievements or free-host availability guarantees.

## 14. Operations and release engineering

### 14.1 Observability

Emit structured events/logs containing request, run, job, attempt, generation, and remote-operation IDs. Do not log credentials or full prompts/source by default.

Track queue depth, oldest eligible job, lease loss, stale-write rejection, retry classification, unknown effects, sandbox reservations, admission rejection, artifact usage, inbox age, failed delivery recovery, and estimated model usage.

High-cardinality run IDs belong in logs/events, not metric labels. Keep an optional local telemetry profile; a full hosted metrics stack is not a launch prerequisite.

### 14.2 Deployment and upgrades

Deploy one immutable application release containing the code and compatibility manifest. Migrations are explicit, tested, and backward-compatible with active data where possible.

Drain new admissions before incompatible deployments. Retain compatible handlers for active workflow versions; otherwise pause affected runs with an explanatory status. Never bind an old persisted run to a new prompt/tool/profile silently.

CI performs formatting checks, static typing, unit/integration tests, deterministic adapter/fault tests, UI tests, secret scanning, and a production build. Paid live-provider tests are manually invoked and never required on untrusted pull requests.

### 14.3 Backup and restore

Provide an operator export command for database state and referenced objects, with a manifest and checksums. Exclude all provider credentials, including per-run keys, GitHub access/refresh material, session secrets and deployment keys. Restored users must reauthorize.

Take exports before migrations and during active beta operation at least weekly. Keep at most two encrypted local exports. Each export expires at the earlier of seven days after creation or the earliest included content-expiry deadline. User deletion invalidates every export containing that user's affected content; record this in the deletion journal and destroy those exports during the operator cleanup procedure. Do not commit exports to the repository.

Restore into quarantine: block mutations, apply the current deletion journal and expiry rules before exposing any content, revoke restored sessions, expire stale approvals, verify objects, invalidate old execution ownership, and inspect remote effects that could have occurred after the export. Only then enable permitted reads and new work. Without a current deletion journal, restore metadata only and require fresh runs.

This is a limited disaster-recovery procedure, not a zero-data-loss guarantee. Document the last successful export and possible loss window.

### 14.4 Operator runbooks

Provide procedures for provider quota exhaustion, database pause, failed webhook recovery, lost sandbox allocations, uncertain publication, key compromise/rotation, corrupted artifacts, storage pressure, account suspension, deletion, migration rollback, and restore.

Operator actions use an authenticated local administrative CLI with audit events. No public “force complete” or “retry everything” endpoint exists.

The operator web view additionally requires a deployment-configured allowlist of immutable GitHub user IDs. It displays operational metadata, not provider keys or full user content. Operational credentials never appear in browser storage. Operator actions do not bypass immutable publication approval or grant a user rights to another account.

## 15. Implementation sequence

The detailed backlog is in [todo.md](todo.md). Each task has dependencies, acceptance criteria, and verification. Add schema only as its vertical slice needs it.

| Phase | Outcome | Exit gate |
|---|---|---|
| 0 — Contract and feasibility | Repository skeleton, documented decisions, provider/free-tier spike | Actual deployed isolation, timeout, inbox, artifact, and database behavior verified |
| 1 — Durable execution | Jobs, leases, fencing, events, cancellation | SIGKILL and stale-worker proofs pass |
| 2 — Isolated work | Supported profiles and immutable source recovery | Recovery restores exact tree; malicious inputs rejected |
| 3 — Agent and review | Scripted loop, both providers, immutable validation, bounded revisions | Real patch can be reviewed with independent evidence |
| 4 — GitHub publication | Exact approval and external-effect reconciliation | Lost PR response recovers without speculative mutation |
| 5 — Public product | Individual auth, web journey, GitHub commands, quotas and lifecycle | Fresh user completes both supported journeys |
| 6 — Release evidence | Adversarial tests, benchmarks, docs, deployed interruption tests | Public-launch checklist is fully satisfied |

Build a minimal authenticated run timeline early, then evolve it through each phase. Do not defer all UI or all failure testing to the end.

The 2–3 month demonstration milestone is a working end-to-end controlled run with a real provider, approval, GitHub publication, and visible recovery. Public opening may follow later. Do not promise that the complete scope fits 80–195 hours without measurement of actual progress.

Parallel work is appropriate after interfaces stabilize: frontend against generated API fixtures, provider adapters against a fixed normalized contract, and independent fault tests. Database migrations, state-machine changes, and shared contract edits need one owner at a time.

## 16. Documentation and final acceptance

### 16.1 Documentation package

Maintain a concise README, this plan, task checklist, glossary, architecture decisions, API/OpenAPI reference, supported-profile guide, security/threat model, failure matrix, operations runbooks, benchmark report, evaluation report, and demo script.

The README should show one successful workflow and one failure/recovery sequence before long architectural detail. Explain limitations honestly. Use recorded screenshots from the implemented system, not fabricated metrics.

### 16.2 Signature demonstration

1. Submit a bounded task on a fixture repository.
2. Produce and commit an intermediate checkpoint.
3. Kill the worker and show a replacement restoring that checkpoint.
4. In a separate experiment, resume a stale worker and show its result rejected.
5. Review the candidate and request one revision.
6. Approve the new immutable manifest.
7. Let fake GitHub accept PR creation and drop the response.
8. Recover, discover the existing PR, and adopt it.
9. Show the same workflow against real GitHub with normal network behavior.
10. Explain which guarantees the controlled fault test proves and which live-provider assumptions remain.

### 16.3 Public-launch checklist

- [ ] Both supported repository profiles pass eligibility and end-to-end tests.
- [ ] Both real provider adapters pass contract and smoke tests.
- [ ] All twenty core invariants have automated evidence.
- [ ] Every required failure-matrix outcome has a test or an explicitly documented deployed verification.
- [ ] A new user completes web and GitHub-command flows without operator intervention in ordinary conditions.
- [ ] Account isolation covers APIs, events, objects, credentials, and decisions.
- [ ] Sandbox timeouts work after the API process disappears.
- [ ] Unknown remote allocations and mutations cannot bypass quotas or approval.
- [ ] User-visible publication bytes match independently validated bytes.
- [ ] Expiry, cancellation, revocation, and deletion are tested across channels.
- [ ] Free-tier account eligibility, non-overage behavior, and absence of paid fallbacks are verified.
- [ ] Actual memory, database, artifact, and transfer usage fit the configured headroom.
- [ ] Wake, sleep, suspension, and failed delivery recovery are demonstrated.
- [ ] Backup/export and quarantine restore are exercised.
- [ ] UI accessibility, responsive review, and failure states pass browser checks.
- [ ] Benchmarks and model evaluations report measured results and limitations.
- [ ] Security, support boundaries, BYOK cost/retention disclosure, and beta availability are published.
- [ ] No unresolved high-severity correctness or cross-account isolation finding remains.

### 16.4 Later extensions

After the beta is demonstrated and reviewed, consider broader repository profiles, private repositories, more models, shared teams, a public runtime SDK, generic workflow graphs, stronger availability, paid capacity, and a comparison implementation using an established workflow engine.

Each extension needs a separate scope and budget decision. None is an implicit prerequisite for completing this plan.
