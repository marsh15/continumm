# Worker checkpoint recovery

T21 connects immutable source work to the existing durable runtime. A replacement
worker restores the latest accepted source checkpoint through
[checkpoint storage](checkpoint-storage.md). An uploaded object, a local file or
late sandbox output cannot independently advance source state.

Recovery selects the latest accepted artifact identity before checking whether
that exact artifact is recoverable. Missing, expired or corrupt latest content
does not authorize an older checkpoint or a fresh repository checkout. Current
repository authorization and a live execution lease remain required for source
restoration.

Candidate source changes are revalidated against the restored baseline and
uploaded through T15. The existing T08 transaction accepts the result, attaches
the verified artifact and advances work together. A worker killed before that
transaction leaves the previous accepted checkpoint authoritative. A worker
killed after commit leaves the new checkpoint authoritative even if it never
received the acknowledgment.

New writes must match the current attempt and generation. Stale output cannot
replace a newer accepted result. Exact replay of an already accepted receipt is
read-only; it does not authorize another upload or sandbox command.

Validation uses the immutable candidate from T20. Test filesystem writes are
discarded, and accepted evidence remains bound to the original candidate digest.
T16 continues to retain capacity for unknown allocations; source restoration
does not authorize a replacement allocation or refund compute.

This is an internal worker service. The trusted workflow must authorize source
tool calls through T19 before preparing a patch; recovery does not grant new
model capabilities. Candidate acceptance reloads the committed base and checks
the patch preimages and cumulative delta again before uploading. It rejects a
supplied snapshot that disagrees with the authoritative artifact.

## Verification

```sh
make test-target TARGET=tests/faults/test_workspace_recovery.py
```

The PostgreSQL process proof requires a dedicated disposable PostgreSQL 17
instance, because the fixture manages cluster-wide runtime roles. It must use
loopback networking and the database name `continuum_t21_test`.

```sh
CONTINUUM_T21_TEST_DATABASE_URL='postgresql://<local-admin>@127.0.0.1:<port>/continuum_t21_test' \
  make test-target TARGET=tests/faults/test_workspace_recovery.py
```

Local process and storage fixtures do not establish hosted acceptance. Render,
Supabase and managed sandbox evidence remain pending until those resources are
configured.

The late-result fixture delays a patch result in a local worker subprocess. It
proves worker-result fencing through T21/T15/T08; it does not run a managed
Vercel sandbox.

## Reading the code

- [Workspace recovery](../src/continuum_api/runtime/workspace_recovery.py)
  adopts accepted receipts, reconciles allocations and restores exact source.
- [Artifact persistence](../src/continuum_api/runtime/artifact_persistence.py)
  selects the latest accepted parent before checking availability and rejects
  stale parent lineage at attachment.
- [Transition service](../src/continuum_api/runtime/transitions.py) provides
  owner-scoped accepted-receipt lookup and the existing fenced commit.
- [Process proof](../tests/faults/test_workspace_recovery.py) kills actual
  workers around upload and commit; the optional database cases exercise the
  production services against PostgreSQL 17.
- [Allocation identity tests](../tests/unit/test_workspace_recovery_service.py)
  reject mismatched reconciliation results before attempting a stop.

The local fixture outcomes are explicit:

| Fixture | Required outcome |
| --- | --- |
| Worker killed before upload | Restore prior accepted bytes and modes |
| Worker killed after upload, before result commit | Ignore unattached object; restore prior bytes |
| Worker killed after commit, before acknowledgment | Adopt accepted receipt; restore new bytes |
| Old worker returns after a new generation wins | Reject late result; preserve the winner |
| Supplied snapshot has forged bytes under the same artifact ID | Reject before upload |
| Recovery uses a different account | Reject before source reads |
| Prior allocation remains unknown | Retain charge and block recovery dispatch |
| Latest accepted artifact is corrupt or expired | Needs attention; no older fallback |
| Validation mutates its workspace | T20 evidence retains the original candidate digest |
| Traversal, links, ambiguous paths, oversized archives | T14/T19 reject with explicit archive or policy errors |
| Original tests, harnesses, manifests or credential paths are targeted | T19/T20 deny the change or inspection |

The final three rows reuse the existing
[archive](canonical-archives.md), [source-tool](source-file-tools.md) and
[validation](candidate-validation.md) fixtures for Checkpoint G.

The dedicated PostgreSQL 17 target passed all ten cases. The portable target
passed one subprocess/archive case with nine database cases skipped; three
allocation-identity unit tests also passed. The existing T15 PostgreSQL proof
passed after the parent-lineage change. Formatting, lint, typing and package
build passed. The full Python suite passed 438 tests with 24 optional database
cases skipped. The dedicated database instance was stopped and removed.
