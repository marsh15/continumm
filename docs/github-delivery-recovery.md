# GitHub App failed-delivery recovery

T39 adds a Supabase Edge Function invoked by Supabase Cron every five minutes.
It polls the GitHub App webhook delivery API while the Render application is
asleep, then requests one redelivery for each newly observed failed event.
GitHub does not automatically redeliver failed webhook deliveries.

## Scan and retry behavior

The scanner uses GitHub's App JWT endpoints to list deliveries with cursor
pagination and request a delivery retry. Each invocation is capped at 100
seconds, 20 pages, and 25 redelivery requests. When a scan reaches the page cap,
it saves the next cursor and pending watermark so the following cron invocation
can continue. The persisted high watermark advances only after the scan reaches
the overlap boundary. Each completed scan rechecks a 15-minute overlap to catch
late or out-of-order deliveries.

Before the POST, the scanner inserts a durable tombstone keyed by GitHub's
delivery GUID. A repeated list result cannot trigger a second POST. A clear
`202` is recorded as accepted. A rate-limited POST records the server-provided
retry time and may be retried after that time. A network error is recorded as
unknown because GitHub may have accepted the request; unknown outcomes are not
automatically repeated. GitHub may return `422` when redelivery is throttled;
that response is recorded for operator review rather than retried blindly.

GitHub exposes delivery history and redelivery for only the previous three
days. If the scanner's last successful scan is older than that window, it
persists `history_gap_since` and returns the gap in the scan result. It does not
fetch or reconstruct old comments, approvals, or other mutable intent. Users
must post a new command when an event falls outside the inspection window.

The scanner reads delivery metadata only. It never loads the old webhook body
or comment text and never changes command receipts or runtime state. The normal
ingress path still verifies the redelivered raw-body HMAC and applies the
existing delivery and logical-command deduplication rules.

## Database and deployment

Before migration `20261020_26`, provision a separate least-privilege
`continuum_recovery` login and password through the operator channel. The role
must be `NOINHERIT`, have no elevated attributes or memberships, and must not be
the owner of the recovery schema. Migration 26 grants it access only to the
recovery schema's scan state, one-use tickets, and redelivery tombstones. The
existing webhook ingress role receives no new grants. The `continuum_wake_issuer`
role can issue a short-lived one-use cron ticket; only the recovery Edge Function
can consume it.

Set these Supabase Edge Function secrets:

- `CONTINUUM_RECOVERY_DATABASE_URL`: TLS transaction-pooler connection string
  for `continuum_recovery`.
- `CONTINUUM_GITHUB_APP_ID`: decimal GitHub App ID.
- `CONTINUUM_GITHUB_APP_PRIVATE_KEY`: the GitHub App private key in PEM form.

Deploy `github-delivery-recovery` with JWT verification disabled. The function
authenticates cron requests through a database-issued, four-minute, one-use
ticket. Install [`supabase/cron/github-delivery-recovery.sql`](../supabase/cron/github-delivery-recovery.sql)
after deploying migration 26 and the function. The SQL job stores no bearer
secret. Keep recovery role credentials separate from ingress, wake, migration,
and application credentials.

An operator can check scan health with:

```sql
SELECT watermark_at, last_scan_at, history_gap_since, scan_cursor, lease_until
FROM recovery_private.scan_state
WHERE singleton;

SELECT delivery_guid, delivery_id, outcome, http_status, retry_after, claimed_at
FROM recovery_private.delivery_redelivery
ORDER BY claimed_at DESC
LIMIT 100;
```

Do not clear unknown or accepted tombstones to force another automatic redelivery.
Inspect the delivery on GitHub and use an explicit operator procedure if a retry
is required.

## Local verification

`npm --prefix tools/edge run check` type-checks the function and tests;
`npm --prefix tools/edge run test` exercises the scanner with mocked GitHub
responses and verifies pagination, overlap, rate limits, history gaps, durable
deduplication behavior, and unknown POST outcomes. The PostgreSQL integration
test in [`tests/integration/test_delivery_recovery.py`](../tests/integration/test_delivery_recovery.py)
applies the migration to a disposable PostgreSQL 17 database and exercises the
real adapter and one-use-ticket function. Neither test calls GitHub.

## API references

- [GitHub App webhook delivery endpoints](https://docs.github.com/en/rest/apps/webhooks?apiVersion=2026-03-10)
- [Handling failed webhook deliveries](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries)
- [Redelivering webhooks](https://docs.github.com/en/webhooks/testing-and-troubleshooting-webhooks/redelivering-webhooks)
- [Viewing webhook deliveries](https://docs.github.com/en/webhooks/testing-and-troubleshooting-webhooks/viewing-webhook-deliveries)
