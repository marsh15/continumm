# Local queue benchmark

`make benchmark CONFIRM_DISPOSABLE_BENCHMARK_DATABASE=1` requires the dedicated
loopback PostgreSQL 17 `continuum_t22_test` fixture URL in
`CONTINUUM_T22_TEST_DATABASE_URL`. It resets that database and restricted roles;
use a dedicated disposable cluster and serialize database test processes.

The identical workload contains twelve queue claims and accepted wait results per
trial, three trials each with one, two and four worker **threads** using separate
PostgreSQL transactions. Fixture setup is excluded. A naive sequential in-memory
baseline processes the same inputs without durability. Discarding its state
simulates restart loss; actual SIGKILL/SIGSTOP recovery is measured separately by
the fault laboratory. The benchmark also forces one database lease expiry per
trial and measures generation takeover, excluding the natural lease-wait period.

| Worker threads | Aggregate jobs/sec over three trials | Largest trial dispatch p95 (ms) | Median forced reclaim (ms) |
|---|---|---|---|
| 1 | 118.8 | 119.7 | 2.9 |
| 2 | 196.0 | 59.9 | 3.0 |
| 4 | 77.9 | 238.0 | 3.5 |

[Raw configuration and measurements](evidence/t47-queue-benchmark.json) include
runtime base commit, exact benchmark-source SHA-256, working-tree provenance,
platform/CPU/runtime/database versions, raw dispatch samples, SQL statement counts
and cumulative relation sizes. The source hash pins the newly added harness even
when the runtime base commit predates it. There are no model or sandbox calls.

Dispatch samples start at common batch release, so they include queue wait as
well as claim latency. Storage includes fixtures, indexes and prior trials; it
is not per-request payload size. SQL counts cover the durable workload, excluding
the separate recovery probe. Twelve jobs per trial and three trials are too few
for stable tail or scaling claims. Thread scheduling is not a multi-process API
capacity test, and local performance says nothing about free-host availability.
The baseline provides fewer guarantees and is not a production alternative.
