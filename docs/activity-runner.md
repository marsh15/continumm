# Controlled activity runner

T34 supplies a bounded runner for one existing admitted run, an opt-in full
workflow worker and a narrower publication worker. It does not create runs, approve manifests, or provision
Render. The full live provider-to-publication demonstration is still pending.

## Admitted workflow worker

After service setup, drain an existing owner-scoped admitted run explicitly:

```sh
uv run --locked --python 3.13 python -m continuum_api.worker \
  --account-id <account-uuid> --run-id <admitted-run-uuid> \
  --max-jobs 20 --max-seconds 300 --execute-workflow
```

The flag authorizes this bounded invocation, including its configured BYOK and
managed-sandbox work. It is mutually exclusive with `--execute-publication` and
`--retention`. Without an execution flag, settings and clients are not loaded.
The worker selects the provider/model and profile from immutable admission pins;
there are no command-line key, provider or profile overrides.

Configure the existing GitHub, encryption and private storage settings, plus
`CONTINUUM_VERCEL_TOKEN`, `CONTINUUM_VERCEL_TEAM_ID`,
`CONTINUUM_VERCEL_PROJECT_ID`, `CONTINUUM_SANDBOX_PROVIDER_PERIOD_ID` and the
selected `CONTINUUM_PYTHON_PROFILE_IMAGE` or `CONTINUUM_NODE_PROFILE_IMAGE`.
Images must use immutable digests. The accounting period and its public/operator
usage rows must already exist; see [allocation accounting](sandbox-allocation-accounting.md).
The worker does not create an allowance or switch to another pool. Model keys
come only from the existing encrypted owner/run credential service.

The resource factory checks owner-scoped run state under database RLS, retained
content, admitted pins, the configured current period and the selected image.
Only then does it construct the service graph. Construction performs database
reads but no GitHub, storage, model or sandbox requests. Missing or revoked model
keys remain subject to the existing credential-wait gate. Inactive profiles are
not configured for this run. Client/pool cleanup also runs on construction failure
and cancellation.

The existing limits remain 1–100 started jobs and at most 300 seconds per drain.
A limit receipt does not establish completion. Inspect authoritative run/effect
state before resuming the same run, and supply a real owner decision at review
waits. A cancelled or timed-out activity may leave a managed allocation pending
observation; its server timeout and conservative ledger charge remain in force.
This command does not create runs, enable public admission or complete deployed
release qualification.

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
`agent_turn` at revision zero. The fetch adapter is injected. The [GitHub source adapter](source-acquisition.md)
provides bounded exact-commit acquisition using fresh authorization. The
full workflow worker registers this handler; publication-only mode stays narrower.

`AdmittedAgentTurnInputResolver` initializes the next first-turn lease from the
immutable admission row. It checks the accepted source result and its digest,
exact next-job linkage, repository/profile/source identity, provider/model pair
and bounded task context. It then pins validation eligibility against the
verified baseline and initializes the persisted model/context/schema values.
A retry after interruption between these writes completes with identical pins.
The initializer creates no model turn or request reservation. Later turns only
read the persisted inputs; missing pins fail closed and rework never resets them.
This resolver is registered by the opt-in full workflow worker.

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

The [workflow registry](workflow-composition.md) now composes source preparation,
admitted inputs, the model/tool dispatcher, validation, freeze and publication.
Its persisted tool-policy resolver derives authority and baseline protections
from server state and fresh GitHub access checks. These remain trusted Python
APIs connected by the opt-in full workflow CLI. The narrower publication mode
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
and reconciliation rules. These handlers are registered in full workflow mode;
publication-only mode does not execute validation.

The Render image still starts the API, not this operator worker. The local tests
use controlled remote adapters and do not establish hosted execution or live
OpenAI generation. See [T34 evidence](evidence/t34-demonstration.md).
