# GitHub command intake

T37 implements a trusted, owner-scoped intake service. It does not expose a public
endpoint, drain the webhook inbox, create runs, execute decisions, or post comments.
T38 will connect accepted commands to shared handlers; delivery recovery is T39.
Public admission remains disabled by default and T34 remains deferred.

Only verified `issue_comment` creation events on ordinary issues may enter this
boundary. The caller must obtain the event from the HMAC-verified durable inbox;
a dataclass is not proof of webhook authenticity. Resolve the linked account from
the numeric author identity before constructing its principal. Never accept this
input directly from an unauthenticated browser or arbitrary JSON request.

The first line must match one of these commands exactly:

```text
/continumm run
/continumm status <run-id>
/continumm cancel <run-id>
/continumm approve <approval-id> <action-sha256>
/continumm reject <approval-id>
/continumm changes <approval-id>
Required feedback on following lines.
```

IDs use UUID syntax and action digests contain 64 hexadecimal characters. Only
`changes` carries feedback. Quotes, fences, natural-language prefixes, bots, edits,
PR comments and malformed commands are inert. Bodies are bounded to 16 KiB.

The production GitHub adapter rechecks current numeric actor write permission and
App access, then reads the current comment and ordinary issue from fixed GitHub
API paths with bounded responses, timeouts, no redirects, and environment proxies
disabled. Intake compares repository, issue, comment and author identity, exact body,
and creation/update timestamps with the signed event. Deleted or unverifiable
comments cannot be accepted. This intake permission check does not change the
existing authenticated console cancellation service.

Before committing, intake rechecks the active account and repository registration
inside a short transaction. The signed comment must be no older than 72 hours,
no more than five minutes in the future, and no earlier than account creation or
repository registration.
No network I/O occurs while this transaction is open. The account is read but
not row-locked because the runtime role has no user-update grant; accepting intent
is not authority to execute it after a concurrent account suspension.

Migration 24 stores a unique receipt per repository/comment identity, independent
of transport delivery IDs and run deletion. The receipt includes a digest of the
full original body and immutable identity, normalized command parameters, and a
90-day retention deadline. Changed content cannot replace accepted intent.
Feedback is stored separately with a 72-hour deadline measured from acceptance;
reads deny expired feedback before cleanup. Owner row-level security and restricted grants isolate accounts.
The intake service can read pending intent after response loss, and concurrent
redeliveries converge on one receipt. Pending intent expires 72 hours after
acceptance, independently of the creation freshness check. Later handlers must recheck active account status and current
authority before every effect, and commit effects and their own processing
identities atomically; reading pending intent is not execution.

Run defaults, credential selection, queue seats, missing-key waits and cross-channel
run deduplication remain owned by T36 admission. Intake itself reserves no model,
sandbox, account, or global queue capacity. Missing setup never creates a run here.
T38 must use that admission boundary rather than implementing another run creator.

Verify with `make test-target TARGET=tests/security/test_github_commands.py`.
Set `CONTINUUM_T37_TEST_DATABASE_URL` only to a disposable loopback PostgreSQL 17
database named `continuum_t37_test` to include the migration, concurrency and RLS
case. The test resets its dedicated database and runtime roles; do not run database
suites concurrently on the same PostgreSQL cluster.
