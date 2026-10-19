# Controlled demonstration (T34)

T34 has a reproducible local journey and separate recovery experiments. Its live
milestone remains open. The selected live target is Render, repository
`marsh15/continumm`, and OpenAI. The owner confirmed that the services are not
configured yet. No real provider call or GitHub publication is recorded here.

## What the local cases demonstrate

The journey starts with a **fixture-supplied candidate**, requests a revision,
restores and edits the workspace through the existing rework fixture, validates
it, freezes a new immutable manifest, approves it, publishes its Git objects and
managed branch, then creates its PR. Normal creation and accepted-but-response-
lost creation run as separate cases. The latter appears in external attention
and is adopted by read-only investigation without a replacement create.

PostgreSQL, review, validation, approval, branch/PR orchestration and the attention
API are exercised through their implemented services. Candidate source edits,
model behavior, sandbox execution and GitHub responses are controlled fixtures.
This is not a live OpenAI-generated patch, hosted worker, or real GitHub PR.

Separate subprocess experiments cover actual worker death and a resumed stale
worker. One sends SIGKILL after a committed checkpoint and verifies a replacement
restores that checkpoint. The other sends SIGSTOP, expires the lease, claims a
replacement, resumes the old worker, and verifies both stale writes are rejected.
The tests move the fixture lease expiry using PostgreSQL time to avoid waiting
for a production lease interval. These experiments are distinct from simulated
remote response loss. See [process-proof details](evidence/checkpoint-d-local.md).

## Reproduce locally

Use a POSIX host with SIGKILL/SIGSTOP support and an isolated disposable
PostgreSQL 17 cluster. The fixtures create and remove
cluster-wide test roles, so do not share the cluster with an application database
or run these suites concurrently. Configure the two loopback URLs privately:

- `CONTINUUM_CHECKPOINT_D_TEST_DATABASE_URL`: database `continuum_checkpoint_d_test`.
- `CONTINUUM_T22_TEST_DATABASE_URL`: database `continuum_t22_test`.

Run the fault experiments with:

```sh
make test-faults CONFIRM_DISPOSABLE_FAULT_DATABASES=1
```

Run the complete local demonstration with `make demo-local`, the same explicit
disposable-database confirmation, and the two test URLs. The runner records actual
case outcomes and labels the report local-only. Missing configuration, skipped
cases and failed experiments cannot produce a successful demonstration report.
Neither target runs OpenAI, deploys Render, or creates GitHub resources.

For a private JSON evidence file, run the same controller directly with a new
output path:

```sh
CONFIRM_DISPOSABLE_FAULT_DATABASES=1 uv run --locked --python 3.13 \
  python scripts/demo_local.py --output /tmp/continuum-local-demo-new.json
```

The two test URLs must already be configured. Existing output files are rejected.
A dirty-worktree report explicitly marks its tested source non-reproducible;
retain a clean-commit run for reproducible evidence.

[Recorded local results and pending live gates](evidence/t34-demonstration.md).

## Live Render/OpenAI preparation

The existing [Render Blueprint](../render.yaml) starts the API web process only.
It now includes the runtime database URL required by production settings. This
addition does not create a worker, a database role, migrations, or a deployment.
The health endpoint `/health/live` proves process liveness; `/health/ready` still
returns 503 and does not establish that a live workflow is available.

Before the live demonstration can run:

1. Configure Render and Supabase, run the runtime migrations with a separate
   migration identity, provision the least-privilege runtime role, and configure
   private checkpoint storage. Follow [hosting setup](hosting-setup.md).
2. Configure GitHub App/OAuth for the final HTTPS origin, install it on
   `marsh15/continumm`, and authenticate the owner with current repository rights.
   Follow [GitHub authorization](github-authorization.md) and the server-only
   variable names in [.env.example](../.env.example). Keep all secret values in
   the hosting secret store.
3. Configure the managed sandbox and execution profile, and supply the OpenAI key
   through the existing encrypted credential service. A raw environment key must
   not bypass durable model reservations or credential dispatch.
4. Complete the trusted admission/input-resolution/validation composition for
   model/tool turns. A [bounded activity runner and approved-publication CLI](activity-runner.md)
   now connect the publication portion, including durable continuations and
   evidence-gated completion. The model/tool dispatcher has trusted first-turn pinning and persisted input
   resolution plus validation/freeze handlers, but still needs admission/source
   preparation, trusted policy and the default validation-input resolver; the CLI registers only publication. The current Docker entrypoint still runs
   Uvicorn. `make eval-live PROVIDER=openai` continues to fail closed rather than
   sending an untracked model request.
5. Run a bounded task, request and validate one revision, inspect the exact new
   manifest, approve it, and publish normally to the managed branch and PR. Record
   real run/approval identifiers, approved commit, PR link, timestamps, provider
   usage and hosted recovery evidence. A real owner approval cannot be replaced
   by a fixture decision.

Render documents repository-backed Blueprint setup in its
[Blueprint guide](https://render.com/docs/infrastructure-as-code) and secret
variable declarations in its [YAML reference](https://render.com/docs/blueprint-spec).
Deploying the API foundation alone does not close T34 or authorize public signup.
