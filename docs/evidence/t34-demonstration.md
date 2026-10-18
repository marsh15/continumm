# T34 demonstration evidence

**Local status:** PASS in the implementation worktree; clean-commit report pending.
**Live status:** NOT RUN — services are not configured and the live activity runner is incomplete.
**Date:** 18 October 2026.

The selected live target is Render with OpenAI and repository
`marsh15/continumm`. The owner confirmed that setup has not been performed.
No Render service, database, sandbox, provider request or live GitHub PR was
created by this work.

## Observed local results

The controlled runner passed five individually selected cases against disposable
loopback PostgreSQL 17. The cases are actual worker SIGKILL recovery, separate
SIGSTOP/stale-owner rejection, PR lost-response reconciliation, revision and
approval followed by normal fixture publication, and the same journey followed
by lost-response adoption. Initial verification used the T34 working tree based
on commit `4eb4232`; it was explicitly marked dirty and is not a reproducible
source identity by itself. A clean-commit report will identify the committed
implementation separately.

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
