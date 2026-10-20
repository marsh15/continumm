# Final acceptance evidence map

This maps every item in [plan §16.3](../tasks/plan.md#163-public-launch-checklist)
to implementation evidence and the remaining release proof. It is a navigation
index, not a launch certificate. Public admission remains disabled; the plan's
launch checkboxes remain open until the deployed gates are measured.

| Acceptance item | Local implementation and evidence | Remaining release proof |
|---|---|---|
| Both repository profiles | [Python](python-execution-profile.md), [Node](node-execution-profile.md); `tests/integration/test_repository_preflight.py`, `test_candidate_validation.py` | Both profiles in the composed, admitted live workflow |
| Both real provider adapters | [OpenAI](openai-adapter.md), [Anthropic](anthropic-adapter.md); `tests/integration/test_provider_agent_loop.py` | Four actual provider/language smokes; fixture transports do not establish provider availability |
| Twenty core invariants | [Invariant map](runtime-invariants.md); `tests/properties/test_runtime_invariants.py` | Revisit evidence after composition changes; bounded exploration is not exhaustive proof |
| Required failure outcomes | [Fault laboratory](fault-laboratory.md), [raw local report](evidence/t45-local-fault-matrix.json) | Six deployed scenarios remain NOT_RUN |
| New-user web and command journeys | [Console](frontend.md), [command execution](github-command-execution.md); browser and cross-channel tests | Inbox drain and complete workflow composition; fresh-account hosted journeys |
| Account isolation | [Security verification](security-verification.md); `tests/security`, owner-scoped integration tests | Review the composed deployment identities, storage and routes |
| Sandbox timeout after API death | [Sandbox setup](sandbox-setup.md), [allocation accounting](sandbox-allocation-accounting.md) | Actual managed sandbox experiment with API process absent |
| Unknown effects cannot bypass limits | [Effect ledger](external-operation-ledger.md), [model budgets](model-limits.md), [allocations](sandbox-allocation-accounting.md) | Actual provider/sandbox unknown-outcome observations within configured allowances |
| Publication matches validated bytes | [Approvals](approvals.md), [branch publication](github-branch-publication.md), [PR reconciliation](github-pr-publication.md) | Real exact-manifest approval and normal GitHub publication |
| Expiry, cancel, revoke and delete | [Retention](retention.md), [credentials](credential-lifecycle.md), [commands](github-command-execution.md); `tests/integration/test_retention.py`, `test_cross_channel_decisions.py` | Hosted cross-channel checks and external cleanup observation |
| Free tier and no paid fallback | [Hosting setup](hosting-setup.md), [release gates](release-qualification.md) | Current account eligibility, provider terms and non-overage settings checked by operator |
| Resource headroom | [Benchmarks](benchmarks.md) records measured local queue/storage samples | Actual deployed memory, database, object and transfer usage; local samples are not capacity guarantees |
| Wake, sleep and delivery recovery | [Wake setup](wake-setup.md), [delivery recovery](github-delivery-recovery.md) | Actual Render sleep/suspension and GitHub delivery history experiments |
| Export and quarantine restore | [Backup/restore](backup-restore.md); `tests/integration/test_restore.py` | Actual private-object restoration; local restore-check validates temporary quarantine only and does not import a production database |
| UI accessibility and failure states | [Console evidence](frontend.md), [recorded screenshots](evidence/screenshots) | Composed hosted journey and applicable failure states |
| Benchmarks and model evaluations | [Benchmark report](benchmarks.md), [evaluation report](evaluation.md) | Live coding-agent results; twelve scripted repairs measure the fixture pipeline, not model quality |
| Published security, support and costs | [Security](security-verification.md), [credential disclosure](credential-lifecycle.md), [status/help](../STATUS.md), README | Keep disclosures aligned with the final deployed configuration |
| No unresolved high-severity findings | [Security verification](security-verification.md) records tested corrections | Reassess new composition and deployment changes; current review is not an exhaustive audit |

The [signature demonstration](demonstration.md) distinguishes controlled process
faults and fake remote effects from normal live GitHub behavior. The
[release qualification record](release-qualification.md) is the place to add dated,
redacted hosted evidence when setup is available. Never mark a gate passed from
an expected result, a configuration example, or a local fixture response.
