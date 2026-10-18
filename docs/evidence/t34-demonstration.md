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
