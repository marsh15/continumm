# Failure laboratory

The complete 37-row requirement matrix from plan §13.2 is mapped in
[fault-matrix.json](fault-matrix.json). Each local row identifies real production
boundary tests; the six deployment-only rows include explicit procedures and
remain `not_run_deployed` until measured. A green local runner never qualifies
those rows or enables public admission.

`make test-faults CONFIRM_DISPOSABLE_FAULT_DATABASES=1` runs the local groups in
separate serial processes. It requires a dedicated disposable PostgreSQL 17
cluster and precreated databases with exactly the names in the manifest. Set each
group's database environment variable, or set
`CONTINUUM_FAULT_DATABASE_BASE_URL` to the explicit loopback admin URL; the runner
derives only the documented test database names. It rejects remote hosts and
libpq routing overrides. Fixture roles are cluster-wide and must not be shared
with unrelated work. These tests reset their databases and restricted roles.

The runner executes every selected group without silently accepting skipped
cases. Its JSON report contains source checksums, JUnit observations, test counts,
and separate local/deployed outcomes. Worker-death, paused-stale-worker and
cross-channel race cases capture intermediate/final authoritative properties;
other rows retain the named test assertions as evidence. Repository source,
credentials and raw prompts are not recorded. Every subprocess is bounded.

Reproduce a smaller group using
`uv run --locked --python 3.13 python scripts/fault_matrix.py --groups process cross`.
Omitted groups are explicitly unverified in that report. No model, managed
sandbox, GitHub, Render or Supabase production mutation occurs in local fixtures.

The original five-case `make demo-local` demonstration remains available with
its separate explicit database guards. Actual hosted faults and live T34/T48/T50
proof remain release blockers. [Deployment procedures](release-qualification.md).
