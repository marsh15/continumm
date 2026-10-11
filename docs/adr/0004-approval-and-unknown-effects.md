---
status: proposed
---
# Bind approval to exact content and stop uncertain non-idempotent retries

The user selected review before publication, at most two revisions, and an investigation path for uncertain writes. Approval will bind the exact tested candidate and publication manifest; rework invalidates the previous approval.

An expiring database lease cannot fence GitHub. A missing remote result is not proof that a prior mutation cannot still complete. Continumm therefore reconciles uncertain effects through reads and may remain blocked rather than claim unconditional automatic completion. Cancellation blocks new dispatch authorizations but cannot recall earlier authorizations or erase remote uncertainty. Approval expiry also blocks new mutation authorizations, while allowing read-only reconciliation of partial publication.
