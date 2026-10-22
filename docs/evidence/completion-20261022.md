# Completion-guide local verification — 22 October 2026

Implementation commit: `6e27153` (`feat: drain durable wakes with bounded fenced workers`).
This report records local work only. No hosted accounts were provisioned, no
provider or managed sandbox requests were made, and no product-generated PR was
published. Public admission and automatic wake execution remain disabled in the
checked-in deployment configuration.

## Completed implementation

- API lifespan retains and cancels the opt-in bounded wake task after receipt commit.
- Migration 31 provides a dedicated function-only worker role, forced-RLS definer,
  renewable global worker lease and independent worker fencing generation.
- Inbox consumption requires a durable matching command result or verified expiry;
  unresolved accepted commands remain pending, including owner-null deliveries.
  Edited deliveries can adopt an existing accepted receipt without creating intent.
- Normal owner/repository/credential/quota/approval checks govern command execution.
  Fixed durable rejection results allow permanent failures to terminate safely.
- Due workflows use normal expired-lease recovery and bounded admitted execution.
  Failed cleanup, unknown effects, time limits and stale leases cannot manufacture
  a successful queue-drain watermark.
- Edge receipts validate scheduled/running/partial/completed states and require a
  timestamp for completion. New work after a drain rotates the wake identity.
- Completion guide, operator configuration, historical T06 evidence note and
  backlog composition gaps now reflect the actual implementation.

## Verification

- `make check`: 928 backend tests passed; 219 optional database tests skipped in
  the unconfigured baseline. Ruff format/lint and mypy (116 source files) passed.
  Frontend API/type checks and 29 UI tests passed; all 31 Edge tests passed.
- Additional PostgreSQL cross-channel decisions (9 tests) and operator controls
  (1 test) passed. The final wrapper initially double-held the operator fixture
  lock; releasing the wrapper allowed the fixture to run and complete. The owned
  disposable cluster was stopped and removed after verification.
- Separate PostgreSQL 17 store and owned-process death tests: 2 passed, including
  actual migration upgrade/downgrade, role restrictions, stale-token rejection,
  same-wake adoption, partial drains and completion-watermark timing.
- Wheel/source package and frontend production builds passed. Docker production
  image built successfully after starting the local daemon. Network-disabled
  non-root import/default-flag smoke passed. Production settings correctly require
  the deployment configuration; the import smoke used explicit loopback development
  settings and made no hosted connection.
- Fresh archive of the implementation commit: `make bootstrap`, package build
  and application import passed using only tracked files.
- All 75 Markdown documents checked had no missing local link targets.
- The [clean-commit evaluation report](evaluation-completion-20261022.json)
  records all 12 deterministic fixtures. Model quality, four live smokes and
  managed hidden checks remain unobserved.

- [Full fault matrix](fault-matrix-completion-20261022.json): all 21 groups and
  190 cases passed, covering 31 local rows. Six deployed-only rows remain unrun.

## Remaining gates

The [completion guide](../../tasks/completion-guide.md) remains authoritative for
hosted steps. Render, Supabase, GitHub App/OAuth, sandbox and owner provider-key
setup is still needed. T34's live patch/revision/approval/PR, four T48 live
provider/profile runs, deployed fault/restore procedures and release qualification
remain unproved. No backlog release checkbox was closed from these local results.

Automatic status comments still need a separate bounded effect drain around the
existing reconciled publisher. Queue completion does not establish comment or PR
delivery. Normal admission currently has no private qualification allowlist;
synthetic admission exercises need an isolated, network-restricted qualification
deployment before temporarily enabling that deployment's admission flag. The
public deployment must remain closed until its launch gates pass.
