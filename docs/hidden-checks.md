# Independent hidden-check comparison

The application-side comparator in
[`validation/hidden_checks.py`](../src/continuum_api/validation/hidden_checks.py)
validates observations against a trusted suite. It executes no candidate code.
The trusted local corpus uses it; a managed execution adapter is still required.
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
remote process used those bytes. A future managed adapter must restore the exact
authorized candidate in a fresh operator-accounted allocation, use a fixed
input/output entrypoint with deny-all egress, enforce process time/output caps,
authenticate provider observations, and verify cleanup. Repository test commands
must not run before that entrypoint in the same allocation. Suite/source pins
and the comparison receipt must be retained with the run's accepted evidence.
Until that integration is implemented and exercised, live reports continue to
say `independent_hidden_checks: not_run` and `task_correctness: not_assessed`.

The local corpus executes only its allowlisted literal fixtures, passes inputs
without expected answers, and records comparison receipts for both original and
candidate sources. This is a local transport/comparison check, not a managed
sandbox or model-quality measurement. Historical reports retain their original
runner hashes and results.
