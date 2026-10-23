# Bounded automatic status comments

Migration `20261023_32` adds a status-comment phase to the opt-in wake worker.
It uses the existing owner-scoped GitHub publisher and marker reconciliation;
it does not add a second remote create path. Worker execution and public
admission remain disabled by default. Hosted delivery has not been qualified.

After inbox processing and workflow execution, a wake attempts at most three
status items within the existing 60-second outer deadline. Comment failures
leave committed workflow progress intact. SQL records each attempt and its
15-minute retry cooldown before any network request. A target receives at most
three automatic attempts, including lookup failures and interrupted attempts.
After that it is held for investigation; no idle retry loop or automatic reset
is provided.

Selection uses active owners, registered repository/issue bindings and retained,
undeleted run content. Terminal statuses can be notified too. The private
transition-version table advances only when status or revision changes. The
notification fingerprint includes that version, status, revision and, while
waiting for approval, the pending approval identity, manifest hash and expiry.
Ordinary step events and `external_operation.*` events cannot generate repeated
comments for an unchanged state. Returning to an earlier state after a wait
can generate a new notification.

The selected public event sequence is checked again before fresh publication.
Normal repository write-access verification, immutable ledger intent and
single-dispatch authorization remain mandatory. Confirmation of queue work
requires the current worker lease and a matching confirmed ledger row. A
successful HTTP response or callback alone cannot confirm delivery.

If a create response is lost, the existing effect remains `unknown` or
`dispatching`. A later drain can reconcile that exact stored marker even after
the run changes status. It only looks up and adopts the existing comment; it
never creates a replacement for an unresolved effect. Historical prepared
intents that were never authorized do not cause stale comments to be sent.
Retention, deletion, registration and owner-isolation checks still apply during
reconciliation.

The worker login has EXECUTE access to narrow claim/confirmation functions.
It cannot read or update the private attempt/version tables directly. The
NOLOGIN definer has column-limited reads and forced-RLS policies; the run
transition trigger maintains only the private version counter. The ingress,
Edge dispatcher and wake receiver cannot confirm status effects or finish work.

A drain watermark means the currently due queue snapshot was handled. It does
not prove every comment was delivered: future cooldowns and held unknown effects
can remain. Operators can inspect bounded metadata with the separate migration
identity, for example:

```sql
SELECT owner_id, run_id, event_sequence, attempts, next_attempt_at, completed
FROM wake_private.status_comment_attempt
WHERE NOT completed
ORDER BY next_attempt_at
LIMIT 50;
```

Inspect the corresponding immutable ledger identity and GitHub marker before
recovery. Never reset an unknown effect to `prepared`, replace its marker or
create another comment just because a response was lost. The publisher's
lookup-only `reconcile(run_id, event_sequence)` path preserves that rule.

Public comments contain run identity, status, revision, console link and the
public approval binding when applicable. They do not contain prompts, task
content, diffs, validation output, credentials or provider responses. Live
GitHub delivery and deployed sleep/recovery behavior remain gates in the
[completion guide](../tasks/completion-guide.md).
