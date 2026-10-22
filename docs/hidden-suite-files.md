# Private hidden-suite files

`make eval-live` can enable maintained hidden checks with
`HIDDEN_SUITES=/absolute/path/to/private-suites.json`. The equivalent CLI option
is `--hidden-suites PATH`. It requires execution mode and both existing model
and operator-sandbox opt-ins. Observation refuses the file; observing already
accepted checks does not require their expected answers.

Keep the file outside the Continuum checkout. It must be a regular file owned
by the current user, have exactly mode `0600`, and have only one hard link.
Symlinks, FIFOs, devices and files larger than 65,536 UTF-8 bytes are refused.
The loader reads it once into immutable data before settings or clients are
constructed and before the output report is created. It never executes file
contents. Do not put it into a candidate repository or commit it to Git.

The exact schema is below. These are example identities and expected answers,
not admitted runs or measured evidence. Replace every identity, repository ID,
full base commit, entry path and check with the maintained task's trusted data.
There must be exactly one Python and one Node suite, matching the two CLI run IDs.
The account and provider must also match the command.

```json
{
  "schema_version": 1,
  "account_id": "00000000-0000-4000-8000-000000000001",
  "provider": "openai",
  "suites": [
    {
      "run_id": "00000000-0000-4000-8000-000000000002",
      "repository_id": 123,
      "base_commit_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "profile_id": "python-pytest",
      "profile_version": "1",
      "suite_id": "python-double-v1",
      "entry_path": "src/solution.py",
      "checks": [
        {"check_id": "positive", "input": 3, "expected": 6},
        {"check_id": "negative", "input": -4, "expected": -8}
      ]
    },
    {
      "run_id": "00000000-0000-4000-8000-000000000003",
      "repository_id": 123,
      "base_commit_sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "profile_id": "node-npm",
      "profile_version": "1",
      "suite_id": "node-double-v1",
      "entry_path": "src/solution.cjs",
      "checks": [
        {"check_id": "positive", "input": 3, "expected": 6},
        {"check_id": "negative", "input": -4, "expected": -8}
      ]
    }
  ]
}
```

Unknown keys, duplicate JSON keys, noncanonical UUID strings, duplicate run or
check identities, unsupported profiles, invalid paths, non-finite/unsafe numbers
and excessive nesting are rejected. Each suite has one to thirty-two checks.
`input` is the single JSON argument to the entry's synchronous `solve(value)`;
`expected` is its trusted JSON answer. Expected answers are operator-authored and
must not come from model output or repository text. Existing JSON comparison
rules and the `.py`/CommonJS `.cjs` runner boundary still apply.

After service setup and admission, an explicitly enabled invocation is:

```sh
chmod 600 "$HOME/.config/continuum/private-suites.json"
make eval-live PROVIDER=openai EVAL_MODE=execute \
  ALLOW_MODEL_CHARGES=1 ALLOW_OPERATOR_SANDBOX=1 \
  ACCOUNT_ID="$ACCOUNT_ID" PYTHON_RUN_ID="$PYTHON_RUN_ID" NODE_RUN_ID="$NODE_RUN_ID" \
  HIDDEN_SUITES="$HOME/.config/continuum/private-suites.json" \
  EVAL_MAX_JOBS=20 EVAL_MAX_SECONDS=300 \
  EVAL_REPORT=.artifacts/live-evaluation-openai-hidden.json
```

The selector verifies each run's original repository, base commit and profile
against the file before validation spending, then binds the checks to its actual
candidate archive and tree. Only public inputs enter the managed runner.
Execution still follows the [fenced hidden-check flow](hidden-checks.md); the file
does not admit runs, approve changes, publish or provide credentials.

When a file is supplied, a successful CLI exit requires both protocol readiness
and matching accepted checks for **both** exact suites. A previous pass for other
answers, another entry or other source bytes cannot qualify. Missing or mismatched
checks return exit `3` and `hidden_suite_ready: false`, even if the protocol-only
flag is true. File or mode refusal returns exit `2` before work begins.

Reports add `hidden_checks_requested`, the loaded file's `hidden_suites_sha256`,
per-language `hidden_suite_matches`, and `hidden_suite_ready`. They contain no
file path or expected answers. The hash records the bytes loaded for that
invocation even if the operator later edits the file. Overall task correctness
remains `not_assessed`; these are maintained checks, not a general model-quality
score. No hosted qualification has yet been recorded.
