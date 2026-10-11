# Continumm architecture decisions

These records distinguish user-confirmed direction from proposed implementation defaults. They form part of the proposed plan; no record asserts that its implementation exists or has passed tests.

| Record | Decision |
|---|---|
| [0001](0001-custom-runtime.md) | Custom internal runtime and an equally important coding product |
| [0002](0002-postgresql-authority.md) | PostgreSQL authority with materialized state and audit events |
| [0003](0003-managed-isolation.md) | Managed isolation and externally committed source checkpoints |
| [0004](0004-approval-and-unknown-effects.md) | Exact approval, evidence-based reconciliation, and safe non-progress |
| [0005](0005-pausable-free-hosting.md) | Pausable free-tier deployment and work-triggered wakeups |
| [0006](0006-shared-command-boundary.md) | Web and GitHub commands share one authorization/transition boundary |
| [0007](0007-byok-and-retention.md) | Individual BYOK accounts and bounded content retention |

Architecture changes during implementation must update the applicable record, plan and tests together. A failed feasibility gate does not authorize quietly changing the budget, trust boundary, or product contract.
