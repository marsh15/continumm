# Runtime invariant evidence

T44 runs deterministic generated operation sequences against the production
PostgreSQL queue, checkpoints, transition and cancellation services. Eight fixed
seeds produce sixteen operations each; the final two transactions compete an
activity result and cancellation when a sequence has not already cancelled.
Every operation checks terminal state and gap-free unique event sequences.

Failures are rerun against fresh runs using deletion-based shrinking. The seed,
original sequence and reduced sequence are retained in
`.artifacts/property-failures/seed-<seed>.json` (ignored by Git). These records
contain operation names only. Reproduce with the parametrized test's seed ID.
The fixed seed corpus makes ordinary CI repeatable; it is bounded exploration,
not a proof over every schedule.

Run `make test-target TARGET=tests/properties/test_runtime_invariants.py` with
`CONTINUUM_T22_TEST_DATABASE_URL` set to a disposable loopback PostgreSQL 17
`continuum_t22_test`. The inherited fixture guards and resets that database.
Serialize PostgreSQL test processes because their restricted roles are shared.

The generated suite supplements the following existing production-boundary tests:

| ID | Guarantee | Authoritative tests |
|---|---|---|
| I01 | Current generation writes | `test_leases.py`, generated stale/checkpoint/reclaim sequences |
| I02 | Atomic result/state/event | `test_transitions.py`, generated result/cancel race |
| I03 | Stable operation identity | `test_transitions.py`, `test_external_ledger.py` |
| I04 | No tools from incomplete output | `test_agent_loop.py::test_invalid_full_turn_never_materializes_or_executes_any_call` |
| I05 | Immutable checkpoints | `test_checkpoints.py`, generated checkpoint replay |
| I06 | Exact approval binding | `test_approvals.py`, `test_approval_manifest.py` |
| I07 | Current actor authorization | `test_github_auth.py`, `test_cross_channel_decisions.py` |
| I08 | No blind unknown-write retry | `test_pr_reconciliation.py`, `test_external_ledger.py` |
| I09 | Terminal runs never reopen | `test_cancellation.py`, generated terminal sequences |
| I10 | Cancellation blocks dispatch | `test_cancellation.py`, generated cancellation/claim sequences |
| I11 | Reserve before dispatch | `test_model_limits.py`, `test_admission.py`, `test_sandbox_allocations.py` |
| I12 | Unknown allocations stay charged | `test_model_limits.py`, `test_sandbox_allocations.py` |
| I13 | Cross-account isolation | `test_account_isolation.py`, generated foreign reads |
| I14 | Secrets outside context/tools | `test_credentials.py`, `test_source_tools.py` |
| I15 | Unique event order | `test_transitions.py`, every generated operation |
| I16 | Replay does not spend twice | `test_admission.py`, `test_model_limits.py`, `test_github_commands.py` |
| I17 | Pins and mandatory expiry | `test_checkpoints.py`, `test_retention.py` |
| I18 | Code compatibility fences claims | `test_leases.py`, `test_workspace_recovery.py` |
| I19 | Validation preserves approved bytes | `test_approvals.py`, `test_approval_git_compatibility.py` |
| I20 | Expired approvals never revive | `test_approval_deadline_race.py`, `test_review_rework.py` |

Paths above are under `tests/`; this map identifies coverage, not new claims that
all optional database or hosted tests run in the default test command. Individual
evidence entries record what actually ran.
