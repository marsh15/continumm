# Coding-agent evaluation evidence

`make eval-corpus` runs twelve trusted fixture repairs: six Python and six Node
cases (doubling, absolute value, clamping, reversal, counting and parity). Each
baseline fails its hidden behavioral checks, a known scripted repair passes,
and the production archive/edit helper preserves the original test artifact.
Hidden check programs live outside the writable fixture workspace. Only literal
fixtures in the runner may execute on the local host; arbitrary repositories
remain subject to the managed-sandbox boundary.

The corpus tests validate deterministic harness behavior. They do not measure a
model's ability to solve coding tasks, and they report zero model calls rather
than inventing a success score or token cost. Use
`make test-target TARGET=tests/evaluation/test_corpus.py` for the 13 harness tests.
The JSON report includes case IDs, archive digests, checks, durations, versions,
commit and an explicit `not_run` value for each live smoke.

Release still requires four measured admitted-run smokes: Python and Node with
OpenAI, and Python and Node with Anthropic. They must use T23 credential dispatch,
durable reservations, hidden checks outside writable workspaces and explicit
owner/operator allowances. `make eval-live PROVIDER=...` remains fail-closed
until the live admitted-run harness and T34 infrastructure are configured. Raw
provider keys are not accepted as a shortcut. No live provider result has been
recorded in this package.

Measured on 20 October 2026: twelve fixture repairs and thirteen harness tests
passed. [Raw fixture report](evidence/t48-deterministic.json) records zero model
calls and all four live smoke cases as NOT_RUN. The live CLI guard was exercised
and exited with code 2 without a provider request.
