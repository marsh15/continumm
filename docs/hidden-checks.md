# Independent hidden-check comparison

The application-side comparator in
[`validation/hidden_checks.py`](../src/continuum_api/validation/hidden_checks.py)
validates observations against a trusted suite. It executes no candidate code.
The trusted local corpus uses it. A managed service and fixed runner path are
implemented below, with fenced activity persistence and observation. CLI suite
loading and actual hosted execution remain open.
It does not qualify a live run or change approval/publication behavior.

A suite pins its ID, profile/version, exact candidate archive and source-tree
SHA-256 values, and one to thirty-two uniquely named input/expected pairs. Its
canonical digest includes the protocol version and expected values. Cases and
suites are immutable, and JSON payloads are validated before use. The caller
must select suites from trusted operator-owned data. Accepting expected answers
from a candidate or an untrusted repository would invalidate the oracle.

Only `suite.public_inputs_json()` goes to an execution environment. Expected
answers and comparison logic stay in the application process. The process
returns exactly this envelope:

```json
{"protocol_version":1,"values":[6]}
```

Callers must decode process output as strict UTF-8 before comparison; invalid
bytes must not be replaced and accepted as observed values. The corpus fails
closed on decoding errors.

The comparator checks the exact value count and compares those observations to
its retained expectations. A process-provided `passed` field is rejected; an
exit code of zero alone cannot pass. Object key order is immaterial; booleans
are distinct from numbers, while `1` and `1.0` compare equally. Duplicate keys,
non-finite numbers, unsafe integers, excessive nesting, invalid Unicode and
oversized streams are rejected. Decimal tokens that fail a decimal-to-float-to-shortest-decimal round trip
(such as `1.00000000000000001` or `1e-400`) are rejected rather than silently
compared as equal.

Receipts record the outcome, count of matching checks, total count, suite/source
pins, output stream hashes and a safe code. They retain no expected answers,
inputs or raw output. A missing process exit code is unknown; a nonzero code is
a failure even if stdout contains correct values. Invalid transport or mismatched
source pins cannot pass. Matching counts do not replace the overall outcome.

The comparator checks caller-supplied source identities; it cannot prove that a
remote process used those bytes. The managed service below is designed to restore the exact
authorized candidate in a fresh operator-accounted allocation, use a fixed
input/output entrypoint with deny-all egress, enforce process time/output caps,
use exact provider-session observations, and verify cleanup. Repository test commands
must not run before that entrypoint in the same allocation. Suite/source pins
and the comparison receipt must be retained with the run's accepted evidence.
Observers can now report hash-verified, current accepted hidden outcomes. A
run without accepted hidden evidence remains `not_run`; overall task correctness
remains `not_assessed`. No actual hosted outcome has yet been measured.

The local corpus executes only its allowlisted literal fixtures, passes inputs
without expected answers, and records comparison receipts for both original and
candidate sources. This is a local transport/comparison check, not a managed
sandbox or model-quality measurement. Historical reports retain their original
runner hashes and results.

## Managed execution service

`validation/managed_hidden_checks.py` now provides an internal
`ManagedHiddenCheckService`. Evaluation-mode workflow resources expose it as
`hidden_checks`; normal workflow resources do not. There is no public endpoint.
A trusted operator selector can explicitly enable it inside candidate validation;
the CLI has no suite-loading option yet.

The caller supplies the current execution lease, verified candidate source
authorization, a trusted suite and an operator-selected entry file. Explicit
`allow_operator_sandbox=True` is required. Source/suite/profile, entry membership,
image and argument limits are checked before allocation. The allocator's
`require_new=True` mode rejects an existing logical identity before observing or
executing it. New allocations use only the operator allowance; no public-pool
fallback exists. Source and suite pins plus the entry path and fixed command ID
bind the invocation identity.

Both profiles now expose `run_hidden_probe()`. They restore and reverify the
original baseline and authorized candidate, verify the configured image/runtime,
install dependencies through their existing bounded trusted setup, and verify
deny-all egress. They then invoke the image-owned runner instead of pytest or npm
test. No repository test command runs beforehand in that allocation. Each remote
effect retains the existing lease, exact-session, command/output and cleanup gates.
Normal validation continues to require its original command ID.

The first supported probe interface is deliberately narrow: a synchronous
`solve(value)` function exported by an operator-selected `.py` or CommonJS `.cjs`
file, called once per JSON input. Fixed runner code lives root-owned and read-only
under `/opt/continuum`; expected answers are never copied there. Python preserves
standard-library imports and uses explicit dependency/source roots. Candidate
prints are discarded without accumulating them in memory. Invalid JSON results
or execution failures cannot become a passing comparison.

After a profile returns, the service verifies source, image, runtime, allocation
and stopped/destroyed cleanup identities before comparing observations. Returned
`ManagedHiddenCheckEvidence` binds the run/attempt, suite, entry/invocation and
execution/comparison stream hashes. It contains no raw process output or expected
answers. The service return is **prepared evidence**. The candidate-validation
handler now commits it through the existing fenced transition as described below. Failures
or cancellation attempt owner-scoped, journaled cleanup; uncertain resources stay
charged and cannot produce passing evidence.

The profile Dockerfiles now include these runners. Deployment requires rebuilding
both profile images and configuring their new immutable digests; an old image
without the helper cannot qualify. The two images now build locally, and
trusted literal probes passed inside non-root, read-only Docker containers with no networking. The Node runtime
identity check also passed after fixing duplicate npm config paths.
[Raw local image evidence](evidence/t48-local-image-probes.json) records local
image IDs and source hashes; these are not published registry digests. Fake
managed adapters cover service orchestration. PostgreSQL tests now cover fenced
activity persistence, approval freezing and observation. Actual provider execution
and CLI suite loading remain open; no passing deployed qualification is claimed.


## Fenced candidate-validation integration

Programmatic callers can pass a trusted `hidden_check_selector` to
`build_workflow_resources(..., evaluation=True)` or to
`run_live_evaluation(..., execute=True)`. The selector receives the current lease
and verified candidate inputs and returns a `HiddenCheckSelection` containing
an immutable suite and entry path. It is operator-owned code: expected answers
must not come from a model or the candidate repository. Selection is resolved
and source/entry bindings are checked before validation costs begin. Default
calls install no selector and do not run hidden checks.

Normal baseline/candidate validation runs first. Only a normal pass proceeds to
the independently allocated probe. Revision, artifact and lease fences are
checked before and after it. A passing hidden receipt must match the selected
suite, entry, source, profile image/runtime and current execution identity.
`ApprovalService.record_validation()` rechecks the third operator allocation and
stores the receipt alongside `approval_validation` in the same accepted T08
result. The exact result hash, including hidden metadata, is anchored by the
approval manifest. Freeze rechecks hidden evidence and its persisted allocation
using the original accepted execution identity rather than the later freeze
lease. Both normal validation allocations must match the explicitly configured
public/operator class; normal application services default to public.

A failed, invalid or unknown hidden outcome leads to `needs_attention`, with no
approval-freeze job. Operational errors record a safe code and unknown status.
Cancellation or a lost lease cannot create an accepted result from the stale
attempt. Invalid responses need not be retained to record their invalid status.
Expected answers, inputs and raw command output never enter these receipts.

The owner-scoped observer checks the accepted payload hash, current revision and
step, latest candidate, retained baseline/candidate objects and authoritative
operator allocation before displaying an outcome. Source digests use actual
PostgreSQL bytea values. It preserves invalid/unknown attention states instead
of calling them unexecuted. Only suite IDs/digests and counts are returned.
The combined report distinguishes passed, failed, invalid, unknown, partial and
not-run cases; `task_correctness` stays `not_assessed`. The CLI preserves only
these known outcome labels and still requires its existing execution opt-ins.

The PostgreSQL proofs use fake validation/probe results and seeded allocation
records. They verify persistence and fencing, not hosted execution or model
quality. Public admission and evaluation publication remain disabled.
