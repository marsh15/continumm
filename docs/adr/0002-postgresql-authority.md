---
status: proposed
---
# Use PostgreSQL for state, jobs and accepted execution history

PostgreSQL stores materialized current state, jobs, ownership generations and append-only events. A transition commits accepted results, state, events and the next eligible job together. A periodic scheduler repairs eligible work idempotently.

This avoids a cross-system transaction between a queue broker and workflow state. The event history is an audit trail, not a promise of pure event sourcing or deterministic model replay. The cost is deliberate SQL/concurrency work and a bounded initial throughput target.
