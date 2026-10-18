# T34 demonstration evidence

**Local status:** PASS on clean commit `832aa42`.
**Live status:** NOT RUN — services are not configured and the live activity runner is incomplete.
**Date:** 18 October 2026.

The selected live target is Render with OpenAI and repository
`marsh15/continumm`. The owner confirmed that setup has not been performed.
No Render service, database, sandbox, provider request or live GitHub PR was
created by this work.

## Observed local results

The controlled runner passed five individually selected cases against disposable
loopback PostgreSQL 17 at commit
`832aa42fbed38c50541fa1484e1860d311948301` on
18 October 2026 at 07:16:33 UTC. The report records `worktree_dirty: false`,
`tested_source_reproducible: true`, `local_only: true` and `live_not_run: true`.
[Machine-readable evidence](t34-local.json).

| Case | Result |
| --- | --- |
| Worker SIGKILL and checkpoint recovery | PASS |
| Separate SIGSTOP and stale-owner write rejection | PASS |
| Accepted PR create with lost response and later reconciliation | PASS |
| Revision, approval and normal fixture branch/PR publication | PASS |
| Revision, approval, response loss and read-only PR adoption | PASS |

`make test-faults CONFIRM_DISPOSABLE_FAULT_DATABASES=1` also passed all three
fault cases through the guarded controller on that clean commit. Without the
confirmation, the target failed before any test execution as intended.

Backend verification passed 576 Python tests with 128 optional database cases
skipped, strict typing, lint, formatting and package build. The runner's 19 unit
cases cover target guards, JUnit outcomes, private report creation and actual
subprocess launch. A combined journey/approval/rework run initially passed 19
cases and found one helper lookup defect; its corrected approval-recovery case
passed separately. All 41 branch/PR publication regression cases also passed
after the helper change. The helper now selects the exact frozen approval after a
revision instead of assuming one approval per run.

## What these results do not prove

The candidate and revision edits are fixture supplied. Remote provider, sandbox
and GitHub behavior are synthetic at their adapter boundaries. The workflow does
not make a live OpenAI patch or a live GitHub publication, and does not establish
hosted recovery, readiness, quotas or performance. The live milestone and
Checkpoint K remain open.

Follow [the demonstration guide](../demonstration.md) for reproduction and the
remaining setup and activity-runner gates. No credential values, database URLs,
model responses, source archives or raw subprocess output belong in this report.

## Subsequent publication-worker proof

The bounded activity runner now scopes claims to one existing admitted run and
maintains heartbeats while trusted handlers run. The opt-in publication CLI
connects that runner to the existing branch and PR services. A separate fenced
completion transaction derives success only from exact approved publication
proof and absence of unresolved required work. Generic activity results still
cannot set success.

Verification on 18 October 2026 passed 607 Python tests (144 optional database
skips), strict typing across 83 source files, lint, formatting and package build.
All 16 new PostgreSQL cases passed: publication activity dispatch including
budgeted continuations and unknown PR responses, admitted-run/owner isolation,
and completion proof, replay, stale-owner and cancellation races. The broader
publication/approval/rework run passed 62 cases; its remaining runner fixture was
corrected to respect the one-active-run-per-account rule and passed in the final
16-case run. No admission constraint was weakened. The CLI without its execution
flag returned `execution_required` before settings/client construction.

Migration `20261018_20` permits successful accepted-result receipts. Downgrade
preserved those existing receipts while restoring rejection of new success rows;
the fixture's older-schema transition proof passed afterward.

This proof uses controlled remote adapters. It does not replace the clean-commit
five-case report above or claim that report tested these later changes. Live
OpenAI generation, server setup and the trusted admission/input-resolution/
validation composition remain pending. The publication worker is an explicit
operator invocation; the Render image still starts only the API.
[Worker contract and commands](../activity-runner.md).

## Subsequent persisted-input resolver proof

`PersistedAgentTurnInputResolver` resolves already initialized turns from the
run's pinned model, task context and tool-schema digest, plus its earliest
accepted baseline artifact. It checks the exact persisted step and current lease
under the owner boundary. Schema inputs are bounded and copied so caller
mutation cannot change later resolutions. Missing pins never fall back to
caller-supplied task inputs.

The PostgreSQL integration test uses the real durable turn service to initialize
and replay the pinned turn. It verifies missing pins, wrong activity, other-owner
access, stale generation, changed revision, schema mismatch and schema mutation
isolation. It passed on disposable PostgreSQL 17 with UTF-8 encoding. No live
provider call or hosted service was involved.

Final verification passed 609 Python tests (145 optional database skips),
formatting, lint, strict typing and package build. Independent Luna/high review
found no blocker within this scope.

This is a resolver for previously initialized turns. First-run input pinning,
policy/validation composition and service setup remain pending; T34 is still
open and T35 has not started.
