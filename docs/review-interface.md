# Candidate review and bounded rework

T28 adds an owner review screen at `/runs/{run_id}/review`. The run timeline links
into it. File navigation, a bounded source diff, baseline/candidate validation,
exact manifest and digest, expiry, cumulative resource usage, and the remaining
rework allowance are shown together. Repository content is rendered as text.
Desktop uses file navigation beside the diff; narrow screens stack the sections.

The API exposes `GET /api/v1/runs/{run_id}/review` and
`POST /api/v1/approvals/{approval_id}/decision`. Review reads verify the canonical
manifest and accepted validation hashes, restore the pinned source archives,
check current repository permission, then recheck current approval/run state and
deadlines before returning. Responses are private and not cached. The decision
body binds the exact approval ID and manifest digest. JSON bodies are bounded to
64 KiB before parsing, including streamed requests; changes require nonempty
feedback of at most 16 KiB in UTF-8.

Decisions run through the shared approval service. The owner/run lock serializes
approval, rejection, changes, cancellation and expiry. An approval command and its
receipt commit with its state change; the owner-scoped idempotency key retains
its input hash and response for 24 hours. A conflicting payload under that key
fails. Repeating the same changes decision under another key must match the
original feedback. The UI keeps the same key and payload when retrying an
uncertain request, and refreshes current state after a conflict.

Changes increment revision 0 to 1, then 1 to 2, and queue a new replay-safe agent
turn. A third rework round fails; approval/rejection remain available for the
second revision. Each revision records owner feedback and its SHA-256. The
original task, model and tools remain pinned. New model context loads feedback
from the database and binds it into its source digest, while original revision-0
hashes remain compatible. Model reservations, token usage, sandbox allowance,
source history and run deadline remain cumulative. A new validated candidate
must freeze a new exact manifest and approval before publication can proceed.

Application construction accepts a trusted principal resolver, review-service
factory and CSRF/origin guard. T29 now supplies the configurable
[GitHub session and repository-access boundary](github-authorization.md). It
remains disabled without explicit server configuration and never trusts a
caller-supplied account header. Live deployment qualification remains pending. T28 performs no GitHub mutation.

## Local verification

- `make check build-package build-frontend` checks the Python/runtime contracts,
  frontend components, edge functions, typing, generated API contract and builds.
- PostgreSQL integration tests in `tests/integration/test_review_rework.py` cover
  actual source checkpoints, new evidence and approvals across both rework
  rounds, shared budgets, provider context preparation, competing decisions,
  cross-owner/access denial, and real HTTP decision persistence/replay.
- `make test-e2e` builds the frontend and runs Chromium against the production
  preview, with explicit API fixtures. Install its browser once with
  `cd frontend && npx playwright install chromium`. Browser tests cover all three
  decisions, two revisions, retry identity, stale/expired/error states, safe text
  rendering, keyboard use and narrow screens. These fixtures do not prove hosted
  sessions or real-provider/GitHub publication.

The design reference and dependency decision are recorded in
[the frontend decision ledger](../frontend/docs/review-interface-decisions.md).
