# Controlled activity runner

T34 supplies a bounded runner for one existing admitted run and an opt-in
publication worker. It does not create runs, approve manifests, or provision
Render. The full live provider-to-publication demonstration is still pending.

## Publication worker

After the owner approves the exact manifest through the review boundary, an
operator can drain that run's `publish_approved` jobs:

```sh
uv run --locked --python 3.13 python -m continuum_api.worker \
  --account-id <account-uuid> --run-id <admitted-run-uuid> \
  --max-jobs 20 --max-seconds 120 --execute-publication
```

The execution flag is required before loading settings or constructing clients.
It permits this bounded worker invocation; it does not replace the stored owner
approval or fresh GitHub authorization. Without it, the CLI refuses execution.
This is a trusted server/operator command, not a browser-supplied identity API.

The worker uses the separately privileged runtime database URL, encrypted
owner-scoped GitHub user tokens, the App private key, and private checkpoint
storage from the existing settings. Apply migration `20261018_20` first. Configure
GitHub authentication and storage as described in [.env.example](../.env.example).
Never pass secrets in command arguments or reuse a migration login as the runtime
login. The worker uses no raw OpenAI environment key.

Output is a small JSON receipt with run ID, started-job count and stop reason.
An error after dispatch can report an unknown count (`null`); it must not claim
that no work happened. Exit zero means the bounded drain ended, **not** that the
workflow succeeded. Inspect the run snapshot and external effects for its actual
state. A time/job limit can require another invocation of the same run.

## Dispatch and publication boundaries

Queue claiming is scoped to the account and exact run. Missing and other-owner
run IDs are indistinguishable. Unknown activity names are rejected before claim
when visible; a post-claim activity mismatch fails closed. Handlers are trusted
async callables registered by server code. The runner maintains the lease while
awaiting a handler and cancels on heartbeat failure. An accepted result committed
just before heartbeat rejection remains authoritative. Limits and cancellation
are cooperative; handlers must retain their own I/O limits and fenced writes.

Each publication activity invokes one bounded T31 branch slice. A pending slice
commits one replay-safe continuation with the same approval binding and a stable
`publish-approved:<approval-id>:part:<n>` key. Once the branch is confirmed, T32
creates or adopts the exact PR. Unknown or conflicting outcomes stop in
`needs_attention`; the existing external ledger prevents a replacement create.
Cancellation and expiry remain terminal, with later read-only observations
available through the [attention interface](external-attention.md).

Successful completion has its own transaction boundary. It checks the exact
current approved manifest and revision, confirmed branch/PR ledger records with
matching request metadata, a canonical PR reference, and absence of other
unresolved steps, jobs, allocations or effects. Only then are the activity result
and run success committed together under the current lease. Generic activity
results still cannot mark a run successful. Replaying that exact accepted result
is read-only. A concurrent cancellation wins or loses under the same run lock;
neither terminal outcome is reopened.

Migration 20 permits successful accepted-result receipts. On downgrade, the old
check is restored as `NOT VALID`: existing immutable success receipts survive,
but new success rows are rejected. Upgrading validates the expanded constraint
again. Downgrade never relabels or deletes successful history.

## Model/tool dispatcher and remaining wiring

`SourcePreparationActivityHandler` now implements the admitted run's first
baseline step. It reads the immutable admission receipt under the current lease,
calls a trusted exact-commit source-fetch adapter, verifies the returned source
against the receipt's complete Git tree and fixed Python/Node profile, and
accepts it as the first retained checkpoint. Acceptance queues one replay-safe
`agent_turn` at revision zero. The fetch adapter is injected; this repository
does not yet provide a production GitHub source-archive adapter, and the
publication-only worker does not register this handler. The next agent-turn
lease still needs admission-to-model/source-pin initialization before dispatch.

`AgentActivityDispatcher` maps persisted `agent_turn` and `agent_tool` steps to
the existing durable model and tool services. The OpenAI adapter uses their
one-use credential grants and durable reservations. Tool execution selects the
stored invocation and ordinal; completed turns schedule candidate validation.

The dispatcher requires trusted owner-scoped input, policy and next-work
resolvers. `PersistedAgentTurnInputResolver.initialize` lets trusted server code
pin a fresh turn's task context, model and fixed tool-schema digest before
calling the dispatcher. It requires a current lease for the exact revision-zero,
replay-safe `agent_turn` and the run's first accepted baseline artifact. Identical
initialization can be repeated; changed or partial pins are rejected. A run with
existing turn history cannot be initialized from missing pins.

Initialization creates no model turn, reserves no tokens and calls no provider.
`prepare_turn` still performs the existing durable reservation and checkpoint
verification. The resolver's read path then supplies the persisted model/context,
checks the schema digest and activity identity, and selects the accepted baseline
under the current owner and lease. Missing pins never fall back to task inputs.

These are trusted Python APIs, not browser endpoints or arbitrary inputs loaded
from the CLI. Admission/source preparation, trusted policy and validation
composition still need wiring for the live workflow. The publication CLI
registers only `publish_approved`.

## Validation and review activities

`CandidateValidationActivityHandler` connects trusted candidate inputs to the
existing `ValidationService` and `ApprovalService.record_validation`. Before
validation begins it checks the current owner/lease, activity and revision, and
binds the baseline/candidate IDs and source hashes to the run's accepted,
committed artifacts with retained content. The candidate must be a distinct
checkpoint in the current revision, which is checked again after validation.
Passing evidence schedules a separately leased
`approval_freeze` activity. `ApprovalFreezeActivityHandler` calls the existing
manifest-freezing service, which verifies immutable source and publication facts
before entering owner review.

`ValidationSourcePinService.pin` stores the trusted original eligibility receipt
and its canonical digest against the run's first accepted baseline. It verifies
the restored baseline's profile plan, Git tree, manifests and size before the
lease-fenced insert. Apply migration `20261019_21` first. The source-pin table has
forced owner isolation and grants the runtime only select/insert access.
Identical pins can be repeated; a changed baseline or receipt is rejected.

`PersistedCandidateValidationInputResolver` reads that pin and the newest
accepted candidate in the current revision, restores both archives through the
checkpoint authorization boundary, derives their cumulative patch and calls the
real source-preparation function. Committed checkpoint attachment already
requires the previous accepted artifact as its parent. The pin preserves the
original baseline across rework revisions; each candidate still needs fresh
validation.

These remain trusted server APIs. Admission must pass the actual T13 receipt to
the pin service; the pin does not independently establish how its caller obtained
that receipt. A trusted policy callback supplies current tool authorization.
There is no browser input path or fallback to a current GitHub branch/cache.
Downgrading migration 21 removes the new pins without rewriting artifacts or
accepted validation results. After re-upgrade, runs missing pins fail closed;
there is no automatic reconstruction from an expired eligibility cache.
Neither activity accepts arbitrary validation evidence from a job payload.
Non-passing validation records bounded typed evidence in `needs_attention` and
schedules no freeze. Unknown allocation outcomes retain the existing accounting
and reconciliation rules. These handlers are not yet registered in the
publication-only CLI.

The Render image still starts the API, not this operator worker. The local tests
use controlled remote adapters and do not establish hosted execution or live
OpenAI generation. See [T34 evidence](evidence/t34-demonstration.md).
