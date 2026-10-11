# T05 hosted webhook ingress evidence

**Status: NOT RUN**  
**Last updated:** 2026-10-11  
**Reason:** Local T05 implementation is complete after T04 commit `c995aac`; no Continumm hosted Supabase ingress fixture is configured.  
**Scope:** This is an evidence template. No deployed function, hosted migration, account setting, delivery, or hosted result is claimed.

See [T05 ingress setup](../ingress-setup.md) for the implementation and hosted procedure. Do not put webhook secrets, database URLs, API keys, raw real webhook payloads, account emails, payment details, full project URLs, or private user data in this file.

## Run identity

| Field | Evidence |
|---|---|
| UTC start / finish | NOT RUN |
| T05 status | NOT RUN |
| Git commit | NOT RUN |
| Edge Function name/version | NOT DEPLOYED |
| Redacted Supabase project identifier and region | NOT SET UP |
| GitHub App ID (numeric; safe identifier only) | NOT SET UP |
| GitHub App webhook URL configured | NOT RUN |
| Hosted project and GitHub App test fixture available | NOT RUN |
| Cloud resources created/changed | None |

## Account and budget state

| Check | Evidence |
|---|---|
| Continumm Supabase project/ingress fixture configured | No |
| Plan and account eligibility | NOT RECORDED |
| Payment method attached? (yes/no only) | NOT RECORDED |
| Auto-upgrade/paid fallback state | NOT RECORDED |
| Database size and usage before/after | NOT RECORDED |
| Edge Function invocation usage before/after | NOT RECORDED |
| Plan/quota review performed at run time | NOT RUN |
| Any quota warning or pause observed | NOT RUN |

## Migration and database privilege proof

| Check | Result |
|---|---|
| Alembic migration revision | `20261011_01` |
| Upgrade on local PostgreSQL 17 | PASS |
| Downgrade and re-upgrade on local PostgreSQL 17 | PASS |
| Alembic version metadata in private schema, no `public.alembic_version` | PASS locally |
| Hosted migration applied | NOT RUN |
| Inbox schema absent from Data API Exposed schemas | NOT RUN |
| Ingress role is not schema/table owner | PASS locally |
| Unsafe role with role membership or `BYPASSRLS` rejected before DDL | PASS locally |
| Ingress role has only intended schema/table access | PASS locally |
| Column-limited `INSERT` and key/digest `SELECT` succeed | PASS locally |
| Raw-body `SELECT`, `UPDATE`, `DELETE`, DDL, and unrelated-table access fail | PASS locally |
| RLS enabled and forced for the ingress login | PASS locally |
| Effective inherited `PUBLIC`/role privileges reviewed | Hosted review NOT RUN |
| Migration credential separate from Edge Function secrets | Hosted review NOT RUN |
| Runtime/backend credential separate from ingress credential | Hosted review NOT RUN |

## Signature, bounds, and responses

| Case | Expected | Observed |
|---|---|---|
| GitHub published HMAC-SHA256 known vector | Valid | PASS locally; hosted NOT RUN |
| Missing/malformed signature or wrong secret | 401/403; no inbox row | PASS locally; hosted NOT RUN |
| One raw payload byte changed after signing | 401/403; no inbox row | PASS locally; hosted NOT RUN |
| Missing/malformed `X-GitHub-Delivery` UUID | 400; no inbox row | PASS locally; hosted NOT RUN |
| Valid JSON content type | Accepted for database commit | PASS locally; hosted NOT RUN |
| Unsupported content type or malformed signed JSON | 415/400; no inbox row | PASS locally; hosted NOT RUN |
| Body exactly 1 MiB | May be accepted if valid | PASS locally; hosted NOT RUN |
| Body 1 MiB + 1 byte, with and without `Content-Length` | 413; no inbox row | PASS locally; hosted NOT RUN |
| Method other than POST | 405; no inbox row | PASS locally; hosted NOT RUN |
| Request/database deadline exceeded | Failure; never 2xx before commit | PASS locally; hosted NOT RUN |

## Durable idempotency proof

| Check | Expected | Observed |
|---|---|---|
| First signed fixture delivery | One committed row; 202 | PASS locally; hosted NOT RUN |
| Same App ID, delivery UUID, exact payload redelivered | 202 duplicate/adopted; still one row | PASS locally; hosted NOT RUN |
| Same App ID and UUID, different signed payload | 409; first row unchanged | PASS locally; hosted NOT RUN |
| DB unavailable before insert | 503; no success acknowledgment | PASS locally; hosted NOT RUN |
| Commit acknowledgment uncertain | Failure response; subsequent replay adopts matching committed row if present | PASS with a controlled post-commit failure; hosted NOT RUN |
| Lost HTTP acknowledgment after commit, then replay | One row; duplicate success | PASS with discarded local response; hosted NOT RUN |
| Row count for each synthetic delivery key | 1 | PASS locally; hosted NOT RUN |
| Stored raw body and digest compared to exact fixture bytes | Equal | PASS locally; hosted NOT RUN |
| Render asleep during delivery | Supabase commit and acknowledgment do not depend on Render | NOT RUN |
| Function response elapsed time | Under GitHub's 10-second cutoff | NOT RUN |
| Hosted GitHub delivery result | 2xx / failure reason | NOT RUN |

## Local verification (not hosted evidence)

| Check | Result |
|---|---|
| Deno version | 2.9.6, isolated under `tools/edge` |
| Supabase CLI | Not installed; not needed for local T05 verification |
| Disposable PostgreSQL version | 17.11; local loopback container, removed after final verification |
| Deno unit tests on actual receiver/signature source | 14 passed |
| `deno check` on actual function source | PASS |
| Real local PostgreSQL integration tests through actual Deno driver | 2 passed |
| Migration upgrade/downgrade/re-upgrade integration | PASS |
| Local Supabase `functions serve` | Not needed for T05 local acceptance; NOT RUN |
| `make test-target TARGET=tests/integration/test_webhook_inbox.py` with `CONTINUUM_T05_TEST_DATABASE_URL` | PASS |
| `make check` | PASS: backend 68 passed / 3 optional integration skips, frontend 3 passed, Deno 14 passed |
| `make build` | PASS: Python package, frontend production bundle, local Docker image |
| `make smoke-hosted` | Not a T05 proof; NOT RUN |

A local Deno test, local PostgreSQL test, mocked inbox, or local Supabase stack is development evidence only. It does not prove the hosted Edge Function, hosted schema/role grants, GitHub delivery configuration, Render-sleep independence, or hosted quota state.

## Cleanup and review

| Check | Evidence |
|---|---|
| Synthetic hosted inbox row removed or retained under documented dedup retention policy | NOT RUN |
| Test delivery redacted from evidence | NOT RUN |
| Edge Function secrets checked for source/log exposure | NOT RUN |
| No raw body or secret copied to this report | NOT REVIEWED |
| No paid resource or fallback enabled | NOT RUN |
| Reviewed by / UTC date | NOT RECORDED |
| Blocker or follow-up | Hosted ingress fixture is not configured |
