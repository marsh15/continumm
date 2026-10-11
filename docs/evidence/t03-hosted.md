# T03 hosted connectivity evidence

**Status: NOT RUN**  
**Last updated:** 2026-10-11  
**Reason:** The owner confirmed Render and Supabase are not set up. There is no hosted fixture to test.  
**Scope:** This report is an evidence template. Empty and `NOT RECORDED` fields are intentional; no hosted results or account-specific settings are claimed.

See [T03 hosting setup](../hosting-setup.md) for the procedure and safety limits.

## Local preparation verification — not hosted evidence

| Check | Result |
|---|---|
| Disposable local PostgreSQL version | 17.11; loopback-only Docker container, removed after the test |
| `make test-integration` with explicit local `continuum_t03_test` DSN | 2 passed |
| `make check` | Passed: backend 49 passed, 2 optional integration tests skipped; frontend 3 passed |
| `make build` | Passed: Python sdist/wheel, frontend production build, and local Docker image |
| `make smoke-hosted` from this workstation | Refused with `render_runtime_required` as designed |
| Hosted Render-to-Supabase database and Storage proof | NOT RUN |

The full check emitted one existing Starlette deprecation warning about its `httpx` test client. Local PostgreSQL and mocked HTTPX tests verify adapter behavior only; they do not satisfy T03's hosted connectivity gate.

## Run identity

| Field | Evidence |
|---|---|
| UTC start / finish | NOT RUN |
| Status | NOT RUN |
| Git commit | NOT RUN |
| Operator | NOT RECORDED |
| Render service identifier | NOT SET UP |
| Supabase project identifier | NOT SET UP |
| Service and project regions | NOT SET UP |
| Hosted smoke opt-in flag present | NOT RUN |
| Cloud resources created/changed by T03 | None |

## Plan, payment, overage, and account-specific limits

Published plan documentation is linked in [hosting setup](../hosting-setup.md#plan-and-limits-to-confirm). Values below must be replaced with dashboard evidence from the owner's accounts. Do not record a credential, payment card data, billing address, account email, full project URL, or full account ID.

| Check | Render | Supabase |
|---|---|---|
| Account/workspace configured | No | No |
| Plan selected | NOT SET UP | NOT SET UP |
| Free-tier eligibility confirmed in account | NOT RECORDED | NOT RECORDED |
| Payment method attached? (yes/no only) | NOT RECORDED | NOT RECORDED |
| Paid overage possible on this account? Include dashboard evidence | NOT RECORDED | NOT RECORDED |
| Spend cap / auto-upgrade / paid fallback state | NOT RECORDED | NOT RECORDED |
| Monthly compute allowance and current usage | NOT RECORDED | NOT RECORDED |
| Outbound bandwidth allowance and current usage | NOT RECORDED | NOT RECORDED |
| Build allowance and current usage | NOT RECORDED | NOT RECORDED |
| Database size limit and current usage | NOT RECORDED | NOT RECORDED |
| Storage size limit and current usage | NOT RECORDED | NOT RECORDED |
| Egress limit and current usage | NOT RECORDED | NOT RECORDED |
| Pooler mode, host redacted, and port | NOT SET UP | NOT SET UP |
| Project pause / service suspend state | NOT SET UP | NOT SET UP |
| Account-specific restrictions, warnings, or add-ons | NOT RECORDED | NOT RECORDED |

## Memory baseline

Plan-page capacities are not measured runtime memory. Record observed values only after a successful deployment and controlled smoke.

| Measurement | Value |
|---|---|
| Render Free plan nominal RAM | 512 MB (published allowance; not a measurement) |
| Render process RSS after cold start | NOT RUN |
| Render process RSS after `/health/ready` and DB pool warm-up | NOT RUN |
| Render process RSS after DB probe | NOT RUN |
| Render process RSS after Storage upload/download | NOT RUN |
| Render peak memory shown in service metrics during smoke | NOT RUN |
| Measurement window / dashboard graph period | NOT RUN |
| Supabase Free compute nominal RAM | 500 MB shared CPU (published allowance; not a measurement) |
| Supabase observed memory metric, if exposed | NOT RUN |
| Metric source and capture reference (redacted) | NOT RUN |

## Database probe

| Check | Result |
|---|---|
| Connected from deployed Render service to Supabase shared Session pooler | NOT RUN |
| TLS required by client | NOT RUN |
| Inserted a T03-only row and committed | NOT RUN |
| Read the committed row using a fresh connection | NOT RUN |
| Deleted the exact probe row and confirmed cleanup | NOT RUN |
| Probe UUID (safe to record) | NOT RUN |
| Sanitized error category, if any | NOT RUN |

## Private Storage probe

| Check | Result |
|---|---|
| Bucket name | NOT SET UP |
| Bucket is private (`public = false`) | NOT RUN |
| Small UUID-scoped fixture uploaded with overwrite disabled | NOT RUN |
| Download required authenticated access | NOT RUN |
| Download byte count matched the fixture | NOT RUN |
| Download SHA-256 matched the fixture | NOT RUN |
| Exact probe object deleted after successful verification | NOT RUN |
| Probe object key (UUID only; no secret) | NOT RUN |
| Unknown upload/delete result needing manual inspection | NOT RUN |
| Sanitized error category, if any | NOT RUN |

## Usage and cleanup

| Check | Before | After |
|---|---|---|
| Render included usage | NOT RECORDED | NOT RECORDED |
| Supabase database usage | NOT RECORDED | NOT RECORDED |
| Supabase Storage usage | NOT RECORDED | NOT RECORDED |
| Supabase egress usage | NOT RECORDED | NOT RECORDED |
| Probe row cleaned | NOT RUN | NOT RUN |
| Probe object cleaned | NOT RUN | NOT RUN |
| Remaining probe objects/rows and cleanup owner | NOT RUN | NOT RUN |

## Redaction and sign-off

- Evidence contains no password, connection string, API key, Authorization header, secret request/response body, or private user data: NOT REVIEWED.
- Dashboard captures, if later attached, have account email, card data, full IDs/URLs, and secret values removed: NOT REVIEWED.
- No paid fallback or upgrade was enabled: NOT RUN.
- Result reviewed by / UTC date: NOT RECORDED.
- Follow-up or blocker: Render and Supabase setup is pending.
