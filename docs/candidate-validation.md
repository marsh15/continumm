# Immutable candidate validation

T20 tests the original baseline and a separate immutable candidate through the
fixed [Python](python-execution-profile.md) or [Node](node-execution-profile.md)
profile. The baseline keeps its original Git commit and eligibility receipt.
Candidate archive and source-tree digests identify the proposed changes; a
modified candidate never receives a fabricated Git eligibility receipt.

Preparation rechecks the baseline, candidate preimages and cumulative changes
against the [source-tool policy](source-file-tools.md). Original tests, harnesses,
dependency manifests and protected paths remain unchanged. The profile restores
the committed baseline through checkpoint authorization again before accepting
the candidate authorization.

Each phase uses a fresh sandbox allocation. Allocation creation is sequential:
the candidate starts only after the baseline returns verified terminal cleanup.
The default reservation is 300 seconds per phase, consuming 600 seconds of the
900-second cumulative run allowance. Setup, previous allocations and retries
also consume that allowance. Stopping an allocation does not refund dispatched
compute; the T16 accounting service can reject a phase when capacity or budget
is exhausted.

Tests may modify their disposable workspace. Validation never reads those writes
back into the candidate archive. Approval must refer to the original candidate
digest, not the filesystem left by tests.

Evidence preserves baseline and candidate outcomes independently. A failed
baseline followed by passing candidate checks is recorded as
`passed_with_baseline_failure`. A final test failure remains distinct from setup,
unsupported-input and unknown outcomes. Missing exit status, uncertain cleanup,
stale execution and mismatched identity cannot establish a pass. An inconclusive
baseline prevents candidate execution.

The baseline runs its original suite; the candidate runs that preserved suite
plus any new regression tests. This pair does not separately run new regression
tests against original source. Evidence labels that additional proof absent.
Hidden acceptance checks belong to the later fixture evaluation work.

Versioned evidence binds repository and baseline commit, profile and fixed
command, image, allocation, source digests and command-output hashes. It is
bounded to 64 KiB of serialized JSON and does not include raw command logs.
The returned evidence is prepared in memory. It becomes accepted durable state
only when a trusted caller commits it through the current T08 execution lease.
Worker recovery and orchestration belong to T21 and are not part of T20.

## Reading the code

1. [Preparation](../src/continuum_api/sandbox/validation/preparation.py) binds
   candidate bytes to the verified baseline and protects original tests.
2. [Validation service](../src/continuum_api/validation/service.py) creates the
   two allocations in order and classifies the separate results.
3. [Evidence contracts](../src/continuum_api/validation/contracts.py) define the
   bounded JSON returned to the trusted runtime caller.
4. [Python adapter](../src/continuum_api/sandbox/python_profile.py) and
   [Node adapter](../src/continuum_api/sandbox/node_profile.py) restore the
   committed baseline, recheck authorization and send the candidate bytes.
5. [Integration tests](../tests/integration/test_candidate_validation.py) cover
   phase sequencing, failure classification and discarded workspace writes;
   [preparation tests](../tests/unit/test_validation_preparation.py) attack
   protected original tests with recomputed lineage.

## Local verification

```sh
make test-target TARGET=tests/integration/test_candidate_validation.py
make test-target TARGET=tests/adapters/test_python_profile.py
make test-target TARGET=tests/adapters/test_node_profile.py
```

Local tests use synthetic providers and archives. They do not establish hosted
execution or image publication. Those checks remain pending until operator
resources are configured.

The validation target passes 25 tests, preparation passes two, and the Python
and Node adapter targets pass 19 and 34 respectively. The full Python suite
passes 434 tests with 15 optional database-environment skips. Formatting, lint,
typing and package build pass.
