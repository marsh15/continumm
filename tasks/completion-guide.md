# Continumm completion guide

> **Draft execution guide.** Hosted procedures below have not been run against the owner's accounts. Follow the linked setup documents for exact provider configuration. Record actual outcomes; do not mark a step complete from local fixtures or expected results.

This is the ordered procedure for completing the first-release scope in [the canonical plan](plan.md). The detailed acceptance checklist remains [tasks/todo.md](todo.md), and measured implementation results belong in [tasks/progress.md](progress.md) and the linked evidence reports. This guide does not provision accounts, spend model or sandbox allowance, or turn on public admission.

## Risk labels

- **Local:** read-only inspection or local verification. Use disposable databases for targets that reset schemas or roles.
- **Account setup:** manual provider-dashboard action. Check current terms and account limits first.
- **Hosted write:** deploys code or mutates the configured hosted fixture. Use only synthetic data and the exact one-time IDs required by the relevant setup document.
- **Model/sandbox spend:** invokes a real provider or managed sandbox. Keep the run within the owner's explicit allowance.
- **Release gate:** evidence review. A failed or unknown result leaves admission disabled.

## Before starting

1. **Local:** begin from a clean checkout and record the commit you are qualifying:

   ```sh
   git status --short --branch
   git rev-parse HEAD
   ```

   If the checkout is dirty, either commit the intended source first or mark any resulting evidence as non-reproducible. Do not use a report from one commit as evidence for another.

2. **Local:** install dependencies and confirm the current local baseline:

   ```sh
   make bootstrap
   make check
   make build
   ```

   Resolve failures before starting hosted qualification. These commands do not prove hosted services work.

3. **Local:** read the [release qualification gates](../docs/release-qualification.md), [acceptance evidence map](../docs/acceptance-evidence.md), [operator runbooks](../docs/operator-runbooks.md), and [credential lifecycle](../docs/credential-lifecycle.md). Keep `CONTINUUM_ENABLE_PUBLIC_ADMISSION=0` on the public deployment throughout this guide. The normal admission path currently has no private qualification allowlist: admission exercises below therefore require an isolated qualification deployment with restricted network access and synthetic accounts before temporarily enabling its admission flag. Do not enable the public deployment to work around this gate.

4. **Account setup:** use the current plan and account dashboards to verify eligibility, usage, region, and payment/overage settings. The plan assumes a non-commercial project and a ₹1,000/month ceiling. Stop if a required step needs an unapproved upgrade, paid fallback, or payment method.

5. **Hosted write:** never put provider secrets in this guide, Git, command arguments, issue comments, evidence reports, browser variables, or logs. Enter them only in the designated provider secret store. Keep migration, runtime, ingress, wake, operator, GitHub, model, and sandbox credentials separate as documented in `.env.example`.

## 1. Prove database and private storage connectivity

**Backlog:** T03 and Checkpoint A. **Detailed procedure:** [hosting setup](../docs/hosting-setup.md). **Evidence file:** [T03 hosted evidence](../docs/evidence/t03-hosted.md).

1. **Account setup:** create or select the owner's Render and Supabase accounts and project. Confirm the plan, region, quotas, billing state, and no-paid-fallback constraint before creating resources. The setup document contains the current intended topology and provider-specific checks.
2. **Account setup:** create the private `continuum-t03-private` bucket and configure the required narrowly scoped database roles and T03-only probe tables as specified in the hosting procedure. Use the provider-generated Session pooler URI; do not construct a hostname or commit the URI.
3. **Account setup:** connect the repository's Render Blueprint and configure secret values in Render's dashboard. Keep the one-time hosted-smoke flag disabled until the rest of this step is ready.
4. **Hosted write:** fill in a fresh UUID in `CONTINUUM_T03_HOSTED_SMOKE_ID`, set `CONTINUUM_ENABLE_HOSTED_SMOKE=1`, and deploy once. The startup probe should claim the UUID, insert/read/delete its probe row, upload/download/hash/clean up its tiny object, and emit only a redacted result.
5. **Hosted write:** immediately set the flag back to `0` and redeploy. Inspect the exact probe UUID and object key if any outcome is unknown. Never reuse the UUID or repeat an upload just because its response was lost.
6. **Release gate:** record commit, UTC timestamp, plans/regions, redacted service identifiers, row result, object digest and cleanup result, usage before/after, and memory readings in the T03 evidence file. Record `FAILED`, `UNKNOWN`, or `BLOCKED` honestly.

**Pass when:** the hosted database commit/readback and private object round trip both pass, cleanup is verified, and account limits satisfy the project constraints. Do not proceed to public qualification if this step fails.

## 2. Prove managed sandbox lifecycle and profile execution

**Backlog:** T04, T17, T18, and Checkpoint F. **Detailed procedures:** [sandbox setup](../docs/sandbox-setup.md), [Python profile](../docs/python-execution-profile.md), [Node profile](../docs/node-execution-profile.md), and [allocation accounting](../docs/sandbox-allocation-accounting.md). **Evidence files:** [T04](../docs/evidence/t04-hosted.md) and profile-specific evidence linked from the profile docs.

1. **Account setup:** verify the sandbox provider's current plan and limits. Configure the project-scoped provider credentials in the trusted local or hosting secret store. Obtain immutable image digests for the actual Python and Node execution profiles.
2. **Local:** build each profile image and record the image digest and build commit. Do not use a mutable tag as the admitted execution identity.
3. **Hosted write:** configure the already implemented durable allocation accounting and the current sandbox billing period. Use a fresh stable attempt identity for each deliberate fixture.
4. **Model/sandbox spend:** run the accounted harmless lifecycle fixture. Verify session creation, bounded command execution, chunked file round trip, no injected credentials, stop/destroy, and final passive absence observation.
5. **Hosted write:** run the separate client-death fixture. Record the exact sandbox and process IDs, kill only the local client process using the sandbox guide, wait past the server-side deadline, then passively observe that the remote process exited. Clean up and confirm absence.
6. **Model/sandbox spend:** run one permitted Python-profile fixture and one permitted Node-profile fixture in their actual managed images. Confirm dependency setup rules, network-policy transition to deny-all, egress probe failure, fixed validation command, timeout, output caps, and cleanup.
7. **Release gate:** inspect the allocation ledger after success, failure, timeout, and unknown outcomes. Unknown allocations remain charged and require observation; do not free capacity by assumption.

**Pass when:** both real profile images execute under the managed provider's restrictions, API/client loss does not leave remote work running past its server deadline, and allocation accounting matches observed provider state. T04's Python 3.14 lifecycle image is not evidence for the Python 3.13 project profile.

## 3. Deploy and prove durable GitHub webhook ingress

**Backlog:** T05. **Detailed procedure:** [ingress setup](../docs/ingress-setup.md). **Evidence file:** [T05 hosted evidence](../docs/evidence/t05-hosted.md).

1. **Account setup:** create/configure the GitHub App webhook secret and the dedicated ingress database role. Use the provider's transaction pooler connection for the Edge Function. Do not give the Edge Function the application, migration, or broad Supabase secret credential.
2. **Local:** run the Edge checks and the PostgreSQL-backed ingress integration against the documented disposable PostgreSQL 17 database. Verify signature over raw bytes, body-size and content-type rejection, deduplication, digest conflicts, rollback, and least-privilege grants.
3. **Hosted write:** apply the reviewed migration with the separate migration connection, deploy the ingress Edge Function, and configure its secrets in Supabase.
4. **Hosted write:** submit one synthetic signed delivery. Confirm a new delivery is committed before `202`; submit the same delivery again and confirm a duplicate is adopted without a second row.
5. **Hosted write:** exercise a lost acknowledgement by allowing the insert to commit and dropping the response, then redeliver the same delivery identity. Confirm exactly one stored row. Confirm invalid signatures and altered bodies are rejected.
6. **Release gate:** repeat with the Render service asleep. Confirm the Edge Function stores the event and acknowledges independently of API availability. Save redacted delivery IDs, row counts/digests, HTTP outcomes, and cleanup in the evidence report.

**Pass when:** the actual deployed handler and hosted PostgreSQL prove signature checking, commit-before-acknowledgement, duplicate adoption, conflict handling, and sleep independence.

## 4. Complete wake dispatch through actual work completion

**Backlog:** T06, T35, T37, T38, and Checkpoint B. **Detailed procedure:** [wake setup](../docs/wake-setup.md), [command execution](../docs/github-command-execution.md), and [activity runner](../docs/activity-runner.md). **Evidence file:** [T06 hosted evidence](../docs/evidence/t06-hosted.md).

The opt-in bounded wake worker now runs inside the API process after receipt commit. It is disabled by default. Configure the separate worker role and runtime dependencies using [bounded wake worker](../docs/bounded-wake-worker.md). Hosted completion remains unproved; automatic public status-comment delivery is still a separate implementation gap.

1. **Local:** trace the path from the pending ingress row through comment parsing, owner/repository authorization, atomic admission or decision, durable job creation, worker execution, and final receipt. Keep each transition within the existing database transaction/fencing rules.
2. **Local:** implement a bounded drain path that the wake request can safely trigger. It must claim only current work, use existing admission/quota and lease controls, consume only eligible inbox deliveries, and record a successful drain watermark only after the bounded work has committed. Do not treat an HTTP receipt, job enqueue, cron tick, or worker start as a completed drain.
3. **Local:** ensure the worker process is actually started or invoked in the deployment architecture. Preserve the existing explicit operator CLI as a controlled path; choose and document how hosted actionable work starts the bounded worker without an idle keepalive.
4. **Local:** add focused PostgreSQL/process tests for duplicate wakes, stale generations, concurrent drainers, backoff, capacity holds, cancellation, partial drains, and worker death. Verify ingress and wake roles cannot mark work consumed or write the drain watermark.
5. **Hosted write:** apply reviewed migrations, deploy the dispatcher and backend, configure separate wake credentials, and schedule the documented bounded cron. Verify idle periods produce no Render request.
6. **Hosted write:** send one synthetic delivery and observe the chain: inbox commit, authenticated wake ticket, wake receipt, bounded worker claim, command result/job state, inbox consumption, and final drain timestamp. Repeat a lost response and prove the same wake is adopted.
7. **Hosted write:** exercise the documented sleep, suspension/quota hold, bad-token, paused/unavailable database, timeout, and response-loss cases. Leave work pending when processing cannot be proved.
8. **Release gate:** record request/receipt IDs, generation, statuses, inbox state, authoritative run/job state, drain timestamp, usage, and cleanup in T06 evidence.

**Pass when:** an actionable delivery results in one correctly authorized, bounded unit of work and the completion watermark changes only after its authoritative completion transaction commits.

## 5. Qualify GitHub login, credentials, and live provider boundaries

**Backlog:** Checkpoints H and I, T23-T29, and the provider portion of T48. **Detailed procedures:** [GitHub authorization](../docs/github-authorization.md), [credential lifecycle](../docs/credential-lifecycle.md), [OpenAI adapter](../docs/openai-adapter.md), [Anthropic adapter](../docs/anthropic-adapter.md), and [model limits](../docs/model-limits.md).

1. **Account setup:** configure the GitHub App/OAuth callback for the final HTTPS console origin, App permissions, webhook URL, installation, encryption keyring, and state key. Keep authentication disabled until migrations and server-side configuration are ready.
2. **Hosted write:** apply the auth migration with the migration identity and deploy with GitHub auth enabled. Sign in as the owner, verify the immutable numeric GitHub identity, secure cookies, logout/revocation, current repository write permission, and App installation access.
3. **Account setup:** add an owner-controlled OpenAI key and Anthropic key through the encrypted credential UI/API. Do not place either key in environment variables or worker arguments. Verify revocation and expiry behavior.
4. **Model/sandbox spend:** admit a small bounded Python run for OpenAI and a separate small bounded Node run for OpenAI. Inspect authoritative run state, model usage/reservations, tool policy, validation, allocations, credential redaction, and termination. Resume only the same run after inspecting unknown effects.
5. **Model/sandbox spend:** repeat for Anthropic, once with Python and once with Node. Keep the evaluation worker publication handler disabled as documented.
6. **Release gate:** save four distinct reports. For each, record provider/model identity, run ID, profile/image digest, request/token usage, validation state, sandbox allocations, unresolved effects, source provenance, and credential-leak checks. Do not report unavailable metrics as zero.

**Pass when:** the four provider/profile runs complete under identical authorization, tool, budget, validation, and credential boundaries, and no secret appears in retained or displayed artifacts.

## 6. Complete the real patch, revision, approval, and pull request

**Backlog:** T31-T34 and Checkpoint K. **Detailed procedures:** [controlled demonstration](../docs/demonstration.md), [activity runner](../docs/activity-runner.md), [approvals](../docs/approvals.md), [branch publication](../docs/github-branch-publication.md), and [PR publication](../docs/github-pr-publication.md). **Evidence file:** [T34 demonstration evidence](../docs/evidence/t34-demonstration.md).

1. **Hosted write:** select a small public repository the owner can write to and the installed App can access. Run preflight and record the pinned base commit and profile. Use synthetic or otherwise non-sensitive task content.
2. **Model/sandbox spend:** admit the run using the normal authenticated product path and a fresh idempotency/request identity. Do not create database rows manually to bypass admission.
3. **Model/sandbox spend:** allow the composed worker to fetch the exact admitted source, validate the baseline, run the bounded model/tool loop, and commit candidate checkpoints. Inspect the persisted timeline after each bounded drain.
4. **Model/sandbox spend:** request one revision through the UI or supported GitHub command. Confirm the revision has a new candidate, fresh validation, and a newly frozen manifest while sharing the original resource budget.
5. **Hosted write:** review the diff, evidence and exact manifest in the product. Approve that manifest as the repository owner. Confirm an old or expired approval cannot publish a changed candidate.
6. **Hosted write:** run the publication worker for that run with the documented explicit execution flag. Confirm the resulting managed branch commit matches the approved bytes, then confirm one matching PR is created or adopted.
7. **Hosted write:** perform a controlled response-loss/reconciliation case only if the provider and account allow a safe test. Inspect the external-operation ledger before any retry; never make a second PR to see what happens.
8. **Release gate:** capture sanitized screenshots and record run, revision, manifest digest, approved commit, PR URL, timestamps, provider usage, sandbox outcomes, and external-operation states. Update T34 evidence without relabeling historical fixture evidence as live.

**Pass when:** an actual model-generated patch and revision are independently validated, the exact approved content is published, and the final PR is visible with no unresolved required effects.

## 7. Prove both public submission channels and lifecycle behavior

**Backlog:** Checkpoints L and M, T35-T40. **Detailed procedures:** [repository onboarding](../docs/repository-onboarding.md), [run admission](../docs/run-admission.md), [GitHub command execution](../docs/github-command-execution.md), and [retention](../docs/retention.md).

1. **Hosted write:** using a fresh non-operator test account, connect GitHub, install the App on a public eligible repository, complete repository preflight, add a provider key, and set allowed profile/provider defaults.
2. **Hosted write:** submit one small task in the web console. Retry the same request identity and confirm it returns/adopts the original run. Confirm a new request for the same active issue does not create a second run.
3. **Hosted write:** submit the equivalent supported task/comment command through GitHub. Verify only a new authorized owner comment is actionable; edited comments, bot comments, quoted commands, stale comments, and unauthorized collaborators do not spend a key or create work.
4. **Hosted write:** complete review decisions through both channels. Verify approval, rejection, revision, and cancellation use the same owner-bound transitions and status comments contain no private task data.
5. **Hosted write:** exercise cancellation, credential revocation, account deletion, run expiry, and retention cleanup with synthetic data. Confirm stale browser sessions and old comments cannot revive work or reveal expired content.
6. **Hosted write:** use safe controlled cases to exhaust the configured queue/storage limit. Confirm admission is rejected before sandbox/model spending and the UI explains the reason.
7. **Release gate:** verify one-active-run-per-account, quota accounting, current repository rights, owner isolation, and deletion journal effects from authoritative database state. Save a sanitized fresh-account journey record.

**Pass when:** both channels enforce the same authorization, deduplication, budget, review, cancellation, and deletion rules, and each failure has a clear user-visible outcome.

## 8. Run deployed fault, restore, and security qualification

**Backlog:** Checkpoints N and O, T43-T46 and T50. **Detailed procedures:** [fault laboratory](../docs/fault-laboratory.md), [backup and restore](../docs/backup-restore.md), [security verification](../docs/security-verification.md), and [acceptance evidence map](../docs/acceptance-evidence.md).

1. **Local:** run `make test-faults CONFIRM_DISPOSABLE_FAULT_DATABASES=1` only after confirming its two configured PostgreSQL databases are disposable. Preserve the generated raw report and inspect all matrix rows.
2. **Release gate:** list the six deployed procedures still marked not run in the acceptance map. For each, identify the deployed target, synthetic fixture, preconditions, observable authoritative state, cleanup, and evidence file before executing it.
3. **Hosted write:** exercise each deployed failure separately: API/worker process death, stale worker, lost GitHub response, webhook delivery gap, sandbox timeout, object restoration/quarantine, and relevant provider/database unavailability. Use the existing documented recovery mechanisms and do not replay a mutation with unknown outcome.
4. **Hosted write:** export a synthetic owner dataset, verify it offline with `make restore-check FILE=<private-export-path>`, and separately test recovery of the private objects. This command validates a quarantined export; it does not import production rows or prove a production database restore.
5. **Release gate:** review security and privacy boundaries after full composition, including public routes, second-channel command handling, role grants, proxy/cookie behavior, logs, object access, hidden checks, and operator controls. Resolve every high-severity isolation or correctness finding before proceeding.
6. **Release gate:** confirm the operator can identify, stop, investigate, and recover each supported pause/failure class. Confirm restoration cannot reinstate sessions/credentials or repeat unresolved external publication.

**Pass when:** all required local and deployed fault cases have truthful evidence, security findings are resolved, and the operator runbook matches observed recovery behavior.

## 9. Finish evaluation, demo materials, and documentation

**Backlog:** T47-T49. **Detailed procedures:** [evaluation evidence](../docs/evaluation.md), [hidden-check contract](../docs/hidden-checks.md), [demo guide](../docs/demonstration.md), and [status page](../STATUS.md).

1. **Local:** run `make eval-corpus` and preserve its report. It makes no model calls and measures the trusted fixture harness, not model coding quality.
2. **Model/sandbox spend:** prepare two already admitted owner-scoped run IDs per provider, one Python and one Node. Create the hidden-suite file outside the checkout with the exact permissions and schema required by [hidden suite files](../docs/hidden-suite-files.md). Do not put expected answers in a candidate workspace.
3. **Model/sandbox spend:** first run the documented `make eval-live ... EVAL_MODE=observe` command. Confirm it selects the exact owner, provider, run IDs, repository/base pins and profile. Observe mode does not load a hidden-suite file; validate that file through the execution preflight described in the hidden-suite guide. Resolve mismatches before authorizing execution.
4. **Model/sandbox spend:** run the bounded execution once per provider with explicit model-charge and operator-sandbox opt-ins and conservative per-run job/time limits. Save each report to a new private path; reports are not overwritten. On exit code 1 or 130, inspect authoritative run/effect state before any continuation.
5. **Release gate:** publish the signature demo with the four live provider/profile results, observed recovery cases, real PR evidence, and clear limits. Keep local fixture reports separate from hosted measurements.
6. **Local:** update README, STATUS, demo, setup, security, and cost documentation to match actual measured service behavior. Correct stale backlog checkboxes only after reviewing their evidence. Add the demo and completion guide to [docs/README.md](../docs/README.md).
7. **Release gate:** run the fresh-checkout quickstart and verify all local documentation links. Make sure the public status page remains reachable if the app sleeps or is suspended.

**Pass when:** the twelve-case fixture results and four actual provider/profile results are accurately distinguished, hidden results are independently observed, and every public guarantee points to current implementation and evidence.

## 10. Final launch gate

1. **Release gate:** review every row of [the acceptance evidence map](../docs/acceptance-evidence.md). Each row must have current evidence for the exact release commit or an explicit, accepted limitation.
2. **Release gate:** verify all hosted reports are dated, redacted, tied to the tested commit, and include cleanup outcomes. Recheck plan eligibility, quotas, usage, and no-paid-fallback settings.
3. **Release gate:** verify the service health/readiness behavior, HTTPS origin, GitHub callback, Edge schedules, API/worker behavior, object privacy, logs, operator controls, and independent status/help page.
4. **Release gate:** keep `CONTINUUM_ENABLE_PUBLIC_ADMISSION=0` while resolving all blockers. Public admission can be considered only after every required gate passes and the owner has reviewed the deployed release and its limits.
5. **Hosted write:** if the release is approved, enable public admission through the protected deployment configuration, then verify one authorized low-risk admission and its bounded completion. If anything is uncertain, disable admission and investigate authoritative state before accepting more work.

**Pass when:** the reviewed release satisfies the final checkpoint in [tasks/todo.md](todo.md#final-checkpoint--public-beta). A deployment alone is not a completed project.

## Evidence recording rules

For each hosted attempt, record the tested commit, UTC time, redacted service identity, configured plan/region, synthetic run/delivery/allocation identifiers, observed intermediate and final database-backed state, resource usage, and cleanup result. Keep secrets, raw prompts, repository archives, provider responses, and private logs out of reports. Preserve failed and unknown outcomes; they are part of the evidence.

Use the report linked from the owning task. Do not overwrite historical reports or convert local fixture results into hosted results. When a check is blocked by missing account setup or provider limits, record the blocker and leave the dependent gate open.
