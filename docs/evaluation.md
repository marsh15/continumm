# Coding-agent evaluation evidence

`make eval-corpus` runs twelve trusted fixture repairs: six Python and six Node
cases (doubling, absolute value, clamping, reversal, counting and parity). Each
baseline fails its hidden behavioral checks, a known scripted repair passes,
and the production archive/edit helper preserves the original test artifact.
Hidden check programs live outside the writable fixture workspace. Only literal
fixtures in that runner may execute on the local host; arbitrary repositories
remain subject to the managed-sandbox boundary.

These fixtures measure harness behavior, not model coding ability. They report
zero model calls. The broader planned corpus covering malformed instructions,
hostile repository text, unsolvable tasks, baseline failures and budget exhaustion
is still open. Run `make test-target TARGET=tests/evaluation/test_corpus.py` for
the thirteen existing fixture checks.

## Controlled live protocol harness

The harness accepts two **already admitted** owner-scoped run IDs for the chosen
provider: one Python profile and one Node profile. It does not create runs or
accept raw model keys. Both run/provider/profile bindings are checked before any
workflow resource is constructed. Execution uses the production bounded worker,
encrypted credential dispatch and managed validation. It uses only the configured
operator sandbox allowance; there is no public-pool or paid fallback.

After service setup and admission, observe without starting work:

```sh
make eval-live PROVIDER=openai EVAL_MODE=observe \
  ACCOUNT_ID="$ACCOUNT_ID" PYTHON_RUN_ID="$PYTHON_RUN_ID" NODE_RUN_ID="$NODE_RUN_ID" \
  EVAL_REPORT=.artifacts/live-evaluation-openai-observed.json
```

To authorize a bounded live drain explicitly:

```sh
make eval-live PROVIDER=openai EVAL_MODE=execute \
  ALLOW_MODEL_CHARGES=1 ALLOW_OPERATOR_SANDBOX=1 \
  ACCOUNT_ID="$ACCOUNT_ID" PYTHON_RUN_ID="$PYTHON_RUN_ID" NODE_RUN_ID="$NODE_RUN_ID" \
  EVAL_MAX_JOBS=20 EVAL_MAX_SECONDS=300 \
  EVAL_REPORT=.artifacts/live-evaluation-openai-attempt-1.json
```

Limits apply **per run**, with the two runs processed sequentially. Defaults are
one started job and thirty seconds per run; maximums remain 100 jobs and 300
seconds. A bounded drain can be incomplete. Resuming the same run requires another
explicit invocation and retains existing unknown-effect and reservation fences.
If operator capacity is insufficient, stop and report it; do not change quota
classes to force a pass. Existing reports are never overwritten, and newly
created report files have mode `0600`. The same options are available directly
through `python scripts/eval_live.py --help`.

The evaluation registry omits the publication handler. It never decides an
approval or publishes, including when an owner approves concurrently. Read-only
observation can record actual review status afterward. A protocol-ready snapshot
requires the normal current review gate, coherent validation/approval evidence,
received model work, completed operator validation allocations and no unresolved
ambiguity/effects. It is not a claim that the task is correct or published.

Usage counts are cumulative for each run; drain timing and job counts describe
this invocation. Each case has its database observation timestamp; the complete
report has a separate generation timestamp. Readiness describes those observations,
not a continuing availability guarantee. Reports contain allowlisted state, usage and evidence metadata, not prompts,
source, tool arguments, raw logs or credentials. Unknown usage remains unknown;
unavailable exact patch-byte or unauthorized-attempt metrics are not fabricated.
Completed observation/execution reports record source revision and working-tree
provenance. Exit codes are:

- `0`: a protocol-ready execution report, or a successfully generated observation.
- `1`: operational failure with sanitized metadata; work may already have happened.
- `2`: invalid inputs, missing opt-ins or unavailable output; execution is refused.
- `3`: an execution report whose two runs are not both protocol-ready.
- `130`: interrupted execution with unknown outcome; inspect authoritative state.

Managed profiles currently execute repository tests and protect original tests.
There is **no managed hidden-check oracle API** yet. Live reports therefore always
state `independent_hidden_checks: not_run` and `task_correctness: not_assessed`.
The local fixture runner's hidden checks do not establish managed hidden-check
coverage. No LLM judge replaces independent evidence.

## Recorded results and remaining gates

Measured on 20 October 2026: twelve fixture repairs and thirteen harness tests
passed. [Raw fixture report](evidence/t48-deterministic.json) records zero model
calls and all four live smoke cases as NOT_RUN. Its historical guard result is
preserved; it predates the controlled protocol harness.

No live provider result has been recorded. Release still requires Python and Node
smokes for both OpenAI and Anthropic, the broader deterministic corpus, managed
hidden-check support and actual deployed qualification. The harness implementation
does not close those gates or enable public admission.
