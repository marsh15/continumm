# Pull-request creation and reconciliation

T32 adds a trusted internal PR publication service and a bounded GitHub transport.
It consumes T27's exact approval and T30's durable external-operation ledger.
T33 supplies [browser attention and read-only investigation](external-attention.md).
End-to-end worker wiring and the controlled live demonstration remain later tasks.

## Identity and authorization

Creation uses the frozen title, body, base, head, managed marker and approved
commit SHA. The service validates the exact approval ID/digest and the current
managed branch. Before a new mutation, it obtains a fresh operation-specific
grant and rechecks current human write rights, App installation access, repository
identity, approved base and head revisions.

The installation must grant Pull requests: write and Contents: read. Tokens are
scoped to exactly one numeric repository, stay in trusted backend memory and
never reach repository commands, archives, ledger metadata or browser responses.
Existing Git-object publication continues to request its separate contents-write
scope. Provider origins are fixed; redirects and automatic mutation retries are
disabled.

## Search, adoption and uncertainty

Before creation, search all relevant PR states and pages for the deterministic
managed head. Validate repository IDs, head/base identity, marker and approved
content. A stored PR reference is independently reread; a list lookup cannot
silently erase a previously observed identity.

Adopt one exact match. Multiple matches or conflicting identity require attention.
Record closed and merged matching PRs without creating a replacement or reopening
them. The base branch may have advanced after the PR was created; its current
SHA does not change the PR’s approved base-branch identity. A fresh detail read
takes precedence over stale list state. Human edits are never overwritten: this adapter does not update PR bodies,
retarget, reopen or merge PRs.

Commit logical creation intent and dispatch ownership before sending the one POST.
A lost response, temporarily invisible result or disappeared sending attempt is
uncertain. A negative lookup does not authorize another create. Reconciliation
uses bounded reads and may adopt an exact matching result after cancellation,
approval expiry or source cleanup. It cannot reopen the workflow or bypass a new
mutation's authorization checks. Finding an exact PR or a conflict while an intent
is still prepared atomically seals that intent against later dispatch and stores
the observation, even without a live execution lease. No send grant is returned;
the reservation timestamp records fencing rather than an HTTP send.

The scan is capped at ten pages of 100 PRs, 1 MiB per response page and 4 MiB
aggregate response data, with a 20-second operation deadline. Repositories whose
relevant complete scan exceeds these limits stop safely. An incomplete page scan
or unavailable remote is not proof of absence. Surface
uncertainty or a bounded-scan error instead of creating speculatively. Scheduling
and backoff belong to the durable worker; this service does not poll or sleep in
a retry loop.

## Verification and limits

`make test-target TARGET=tests/faults/test_pr_reconciliation.py` exercises a
controlled remote and disposable PostgreSQL 17 using the T22 database fixture.
See [the ledger test setup](external-operation-ledger.md) before setting its
loopback database URL. HTTP contract fixtures are in
`tests/adapters/test_github_pr.py`.

These fixtures prove the local dispatch and adoption policy. Live GitHub PR
publication, hosted verification and complete product wiring remain pending.
There is no global exactly-once guarantee for arbitrary remote services.

[GitHub's PR REST contract](https://docs.github.com/en/rest/pulls/pulls) documents
state filters, pagination, detail reads and creation.
