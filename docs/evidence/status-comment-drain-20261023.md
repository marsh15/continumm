# Automatic status-comment drain — 23 October 2026

Implementation: `1d44280` (`feat: drain status comments with bounded reconciliation`).
This is local implementation evidence. No real GitHub comments, provider calls,
managed sandbox calls or hosted deployments were made. The checked-in worker
and public-admission flags remain disabled.

## Result

The wake worker now has a separate bounded status phase after workflow work.
Its private SQL queue persists attempts and cooldowns before external I/O, caps
retries at three per target, and accepts completion only with the current lease
and a matching confirmed publisher ledger row. It can notify terminal states
and return to an earlier state without emitting comments for ordinary step or
external-operation events. A target is identified by status, revision, a private
transition version and the pending approval binding when relevant.

Unknown or dispatching effects use lookup-only reconciliation of the original
marker, even after the run advances. They cannot create a replacement comment.
Fresh publication checks its selected public event sequence before authorization
or network access. Comment failures do not roll back workflow progress.

Migration 32 uses the existing separate worker login and NOLOGIN definer. The
new attempt/version tables force RLS. Direct execution of the transition trigger
function is revoked to prevent callers attaching it to another table and
fabricating version changes. Downgrade removes the new queue/trigger and restores
migration 31's actionable-work function and column privileges.

## Verification

- `make check`: **942 backend tests passed**, with 221 optional database tests
  skipped in the unconfigured baseline; **29 UI tests** and **31 Edge tests**
  passed. Generated API checks, Ruff format/lint and mypy (116 source files) passed.
- Actual disposable PostgreSQL 17: two new queue/migration tests passed, nine
  cross-channel tests passed, and the worker process-death and legacy store
  tests passed. These are **13 distinct cases**; the two new tests were repeated
  after narrowing function/column grants.
- PostgreSQL coverage includes real worker/receiver/runtime privileges, current
  and stale fences, commit-proof requirements, durable cooldowns, retry limits,
  unknown-effect reconciliation, terminal states, unchanged-state deduplication,
  A→B→A transition identities, and migration downgrade grants.
- The cross-channel test loses a synthetic create acknowledgement, changes the
  run state, then reconciles the older marker without another create before
  publishing the terminal state. It uses real PostgreSQL persistence and a fake
  GitHub transport; it is not evidence of actual GitHub delivery.
- Package, frontend and container production builds passed. The first container
  attempt stalled on a registry layer; one bounded retry completed. Its offline,
  non-root import smoke confirmed the reconciliation method and disabled worker/
  admission defaults using explicit loopback development settings. The built
  local image manifest is `sha256:d8f7f1cd475d01c40c925bcd8a86e3f2d7a65015285785101ebf31bf0ac938e9`.
- All 77 Markdown documents checked had no missing local link targets.
- The owned PostgreSQL fixture was stopped and removed after testing.

## Limits and qualification

The [operator guide](../status-comment-drain.md) describes the three-attempt hold
and inspection metadata. A drain watermark can coexist with future-due or held
comments; it does not certify public delivery or complete external reconciliation.
No automatic reset of a held unknown effect is provided.

Real GitHub delivery, Render wake/sleep behavior, hosted fault procedures, the
live T34 demonstration, four provider/profile runs and launch qualification
remain open in the [completion guide](../../tasks/completion-guide.md). This
change closes the previously recorded local status-drain composition gap.
