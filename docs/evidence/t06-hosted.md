# T06 hosted work-triggered wake evidence

**Status: NOT RUN**  
**Prepared:** 2026-10-11  
**Reason:** The T06 implementation and local PostgreSQL 17/API/Edge verification are complete, but there is no configured Render service or Supabase project and no hosted deployment evidence.  
**Scope:** hosted evidence only. Local verification below is explicitly separate and does not claim that a hosted dispatcher, schedule, wake endpoint, runtime worker, or drain is deployed.

See [T06 wake setup](../wake-setup.md) for the proposed contract and hosted procedure. T06 stores and authenticates wake receipts only. An accepted receipt is not a completed drain; leave the last successful drain time unset until an owning runtime task commits a real bounded drain.

Do not record wake tokens, database URLs, API keys, Authorization headers, request bodies, account emails, payment details, full project URLs, real GitHub payloads, or private user data here.

## Run identity and resources

| Field | Evidence |
|---|---|
| UTC start / finish | NOT RUN |
| T06 hosted status | NOT RUN |
| Local implementation revision | `20261011_02` |
| Git commit | NOT RECORDED |
| Wake Function name/version | Local source only; NOT DEPLOYED |
| Backend service identifier (redacted) | NOT SET UP |
| Supabase project identifier and region (redacted) | NOT SET UP |
| Scheduled job name and cron cadence | NOT SET UP |
| Owner authorization for hosted deployment | NOT RUN |
| Cloud resources created/changed | None |
| Database migrations applied | None |

## Account, plan, and usage

| Check | Evidence |
|---|---|
| Render account/service configured | No |
| Supabase account/project configured | No |
| Plan and current quota review | NOT RUN |
| Payment method attached? (yes/no only) | NOT RECORDED |
| Auto-upgrade/paid fallback state | NOT RECORDED |
| Render included usage before / after | NOT RECORDED |
| Supabase database usage before / after | NOT RECORDED |
| Supabase Edge invocation usage before / after | NOT RECORDED |
| Effect of five-minute cron activity on Free project pausing | NOT RUN |
| Any service suspension, pause, or quota restriction observed | NOT RUN |

## T05 and database boundary

| Check | Result |
|---|---|
| T05 inbox migration revision and committed inbox contract | `20261011_01`; local integration pinned to this revision |
| T06 nullable `consumed_at` inbox marker and index migration revision | `20261011_02`; local migration tested |
| T05 ingress column-limited INSERT grant preserved; new marker defaults null | PASS locally; direct INSERT/UPDATE attempts denied |
| Wake role can read pending marker but cannot update it | PASS locally; access through fenced functions only |
| T06 dispatch-control and wake-receipt migration revision | `20261011_02`; local migration tested |
| One-use ticket issue/consume function and grant audit | PASS locally, including expiry/replay/concurrency/fence regressions |
| Wake schema absent from Data API Exposed schemas | NOT RUN |
| Dedicated `continuum_wake` login provisioned | NOT RUN |
| Wake role reads only required inbox predicate/control rows | PASS locally through narrow functions |
| Wake role cannot update/delete inbox rows | PASS locally; direct table access denied |
| Wake role cannot access unrelated application data | PASS locally for Edge and receiver roles |
| Wake role cannot update drain-completion state | PASS locally; drain watermark remains NULL |
| Effective `PUBLIC`, membership, and `pg_net` grants inspected | Local role grants tested; hosted `PUBLIC`/pg_net audit NOT RUN |
| T05 ingress credential separate from T06 wake credential | NOT RUN |
| Migration credential separate from both Edge credentials | NOT RUN |
| Wake token absent from `pg_net` URL, headers, body, DB rows, and logs | NOT RUN |

## Schedule and dispatcher

| Check | Expected | Observed |
|---|---|---|
| Scheduled SQL tick with no actionable pending row | No Edge or Render request queued | NOT RUN (hosted; local DB issue function returns no ticket when idle) |
| Ticket missing, expired, consumed, or generation-stale | Edge dispatcher rejects it without Render call | PASS locally |
| Ticket visible in `pg_net` queue | One-use bounded attempt only; no reusable secret | NOT RUN (hosted pg_net unavailable locally) |
| One `pg_cron` job exists at five-minute cadence | One bounded dispatcher schedule | NOT RUN |
| `pg_net` job result | Queue/request ID only; not Edge or backend success | NOT RUN |
| Idle inbox | No outbound Render request | PASS locally |
| One unconsumed synthetic delivery | One current fenced dispatch generation | PASS locally |
| Concurrent dispatcher invocations | At most one current lease/generation can record outcome | PASS locally |
| Live lease or persisted backoff | No outbound Render request | PASS locally |
| Known capacity hold | No outbound Render request | PASS locally |
| Unknown capacity/network outcome | Persisted retry backoff; work remains pending | NOT RUN |
| Inbox body or GitHub ID sent to Render | Never sent | PASS locally; backend envelope contains only attempt/generation/wake/reason |
| No-action tick updated `last_successful_drain_at` | Must remain unset | PASS locally; remains NULL |

## Authenticated backend wake receipt

| Check | Expected | Observed |
|---|---|---|
| Missing/invalid wake token | 401; no receipt | PASS in API unit tests |
| Valid current generation | Commit one durable receipt before HTTP 202 | PASS locally through FastAPI + real receiver-role PostgreSQL |
| Accepted response body | `accepted: true`, `status: "worker_not_started"`, `drain_status: "not_started"` | PASS locally; exact receipt UUID matched |
| Authenticated read-only wake status | Exposes only receipt state and `last_successful_drain_at` | PASS in API unit tests; real DB role tested |
| `last_successful_drain_at` before first real consumer | `null` | NOT RUN |
| Same active wake ID/reason retried with a later current generation | Adopt same receipt; do not duplicate | PASS locally |
| Same wake ID with a conflicting reason/version | 409; no overwrite | PASS in database tests |
| Stale generation | Rejected; cannot create/adopt current receipt | PASS locally |
| Database unavailable before receipt commit | Failure; no accepted response | Unit-tested redacted 503; no hosted DB outage injected |
| Commit response lost, then same wake ID retried | Resolve the one committed receipt idempotently if present | Retry semantics tested at DB/Edge boundary; no hosted response-loss test |
| Unexpected HTTP 200 or malformed 202 body | Protocol failure; no drain success | PASS in Edge unit tests |
| Wake receipt remains pending without T07+ worker | Explicitly pending; no claim of processing | NOT RUN |
| T06 `consumed_at` marker after accepted wake | Remains null until T35 consumer commits it | NOT RUN |
| `last_successful_drain_at` after HTTP 200/202 | Unchanged/null | NOT RUN |
| `last_successful_drain_at` after an actual committed bounded drain | Updated only by future owning runtime transaction | NOT IMPLEMENTED |

## Failure and suspension behavior

| Case | Expected | Observed |
|---|---|---|
| Render Free service asleep | Request may trigger cold start; status/receipt must be observed | NOT RUN |
| Render service suspended by monthly usage/quota | Wake cannot restore it; pending work remains pending | NOT RUN |
| Supabase project paused | Cron/DB work cannot proceed until owner restores it | NOT RUN |
| Supabase quota/service restriction | No successful processing claim; record provider status | NOT RUN |
| Database unavailable to Edge Function | No wake receipt; no drain timestamp; wait for bounded next attempt | NOT RUN |
| Edge→Render timeout after backend may have committed | Outcome unknown; retry same wake ID after persisted backoff | NOT RUN |
| Known capacity suspension/hold | Skip dispatch until recorded hold expires or owner clears it | NOT RUN |
| Backoff remains durable across Edge isolate restart | Same `next_attempt_at` and generation state read from DB | NOT RUN |

## Local verification (not hosted evidence)

| Check | Result |
|---|---|
| T06 dispatcher/backend implementation | Implemented locally; no worker/drainer |
| Disposable PostgreSQL wake-dispatch integration | PASS on loopback PostgreSQL 17 |
| T06 unit and integration target | PASS: 26 Python API tests; 21 Edge tests; 1 PostgreSQL 17 integration |
| Supabase hosted/local Edge runtime | NOT RUN; Edge handler tested with Deno and a test-only local fetch rewrite |
| `make smoke-hosted` | NOT RUN |
| Locally injected transport/fixture, if later used | Must remain test plumbing; cannot claim a business drain |
| Current production worker or command processor | NOT IMPLEMENTED (T07+ / T35) |

A local fixture, scripted HTTP server, or PostgreSQL state simulation is development evidence only. It cannot prove provider suspension behavior, hosted schedules, deployed Edge grants/secrets, Render cold starts, plan/quota state, or a completed runtime drain.

## Cleanup and review

| Check | Evidence |
|---|---|
| Synthetic wake receipt and inbox fixture cleaned or retained by policy | Cleaned after local integration; owned PostgreSQL fixture stopped after final checks |
| Cron job disabled/removed after controlled test, if required | NOT RUN |
| Wake token rotation/cleanup reviewed | NOT RUN |
| No secret or real user data copied to this report | Reviewed; local-only synthetic values excluded |
| No paid resource or fallback enabled | NOT RUN |
| Reviewed by / UTC date | NOT RECORDED |
| Blocker or follow-up | T05 schema/implementation and hosted account setup pending |
