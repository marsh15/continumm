# T03 hosting setup: Render, Supabase Postgres, and private Storage

**Purpose:** operator instructions for the T03 hosted database and artifact connectivity spike.  
**Current state:** instructions are prepared; no Render or Supabase account/project/service has been configured. See [T03 evidence](evidence/t03-hosted.md).  
**Provider documentation checked:** 11 October 2026.

T03 proves one narrow path: the deployed Render Free web service opens a TLS PostgreSQL connection through Supabase's shared Session pooler, commits and reads back one uniquely identified probe row, then uploads and downloads a tiny object in a private Supabase Storage bucket and compares its SHA-256 digest. The probe object is removed after a fully verified run. T03 does not create runtime tables or claim the hosted system is production ready.

## Plan and limits to confirm

The repository's architecture retains Render Free and Supabase Free as a planning baseline. Free eligibility and actual account settings remain unverified until the owner sets them up. Re-check the linked provider pages and each account dashboard before creating resources because quotas and plan terms can change.

| Provider | Current published allowance relevant to T03 | Operational consequence |
|---|---|---|
| Render Free web service | 0.1 CPU and 512 MB RAM; one instance; spins down after 15 minutes without inbound traffic; local filesystem is ephemeral. A workspace receives 750 free instance hours per calendar month. | Record measured process RSS and the Render memory graph. Never store probe state on the service filesystem. A sleeping service has a cold start, and Free services may restart. |
| Render outbound usage | Free web services count against workspace bandwidth and build pipeline allowances. If outbound bandwidth is exhausted and no payment method is attached, Render suspends Free services for the rest of the month; with a payment method it may charge for overage. Unusually high service-initiated public traffic can also suspend a Free service. | Keep a payment method absent, check workspace usage before a smoke, and keep this probe tiny. If the account cannot run without a payment method or paid fallback, stop and record the blocker. |
| Supabase Free | $0 plan; 500 MB database size per project, shared CPU with 500 MB RAM, 1 GB file storage, 5 GB egress and 5 GB cached egress; projects with low database activity can be paused after a week; automatic backups are not included. | Keep a large safety margin. Do not upload source trees or real user content in T03. Record the dashboard's actual quota and usage values. A paused project needs an operator restore action. |

These are published plan values, not measurements of this account. Render documents its [Free instance limits](https://render.com/docs/free), [compute sizes](https://render.com/docs/compute-plans), and [outbound bandwidth behavior](https://render.com/docs/outbound-bandwidth). Supabase lists its [Free plan quotas](https://supabase.com/pricing), [database connection modes](https://supabase.com/docs/guides/database/connecting-to-postgres), and [project pause behavior](https://supabase.com/docs/guides/platform/free-project-pausing).

## Account setup

Perform these steps manually in the provider dashboards when ready. This document does not create accounts, accept terms, attach payment methods, create a project or service, or deploy code.

1. Create or sign in to the owner's Render account and Supabase account. Enable account MFA where available. The Blueprint currently selects Render's Singapore region as the nearest supported Render region to India; review the Supabase region and change `region` in `render.yaml` before first sync if another pairing is preferred. Render does not allow a service's region to change after creation.
2. In Render, use a Hobby workspace with no payment method attached. Confirm the workspace is not on a paid plan and note its available Free instance hours, outbound bandwidth, and build pipeline minutes. Do not add a card to get past a quota warning.
3. In Supabase, create one Free project. Confirm its plan, database size limit, file storage limit, egress allowance, shared compute, pause state, and billing page. Do not upgrade the organization or enable a paid add-on.
4. Confirm the Render and Supabase regions before first deployment. Render's default outbound IP ranges are shared and may rotate; T03 does not depend on allowlisting a single egress address. Avoid configuring IP allowlists unless a later documented need requires them.
5. In Supabase **Connect**, copy the **Session pooler** connection string for the project's actual host, port, username, and region. Do not construct the pooler host. On the Free plan, the shared Session pooler is IPv4 reachable on port `5432`. Supabase recommends this mode for persistent backends on IPv4-only networks; transaction mode on `6543` is intended for short-lived/serverless connections and has different session-state limitations. Store the whole URL as a secret, URL-encoding reserved password characters if the provider-generated string requires it.
6. In Supabase Storage, create a bucket named `continuum-t03-private` with **Public bucket disabled** and a very small upload-size limit that admits only the smoke fixture. The application will access it only from its server process. A private bucket requires authenticated access for reads as well as writes.
7. In Supabase **Settings → API Keys**, create a server-only `sb_secret_...` key for the backend. It bypasses RLS and has broad project access. Keep it only in the server's secret store; never place it in a browser bundle, a URL, source control, shell history, logs, or this evidence file. Current Supabase API-key guidance says to send this key in the `apikey` header, not as a Bearer JWT. Do not copy the legacy `service_role` key into new configuration.
8. In Supabase SQL Editor, create only the T03 probe and one-shot claim tables shown below. Enable RLS on both tables and create no `anon` or `authenticated` policies. The application connects with the server database role to these T03-only tables.
9. Prepare the repository's Render Blueprint only after T01/T02 have established the production start/build commands. The service must explicitly select `plan: free`, deploy one instance, and refer to secrets by environment-variable name. Keep all secret values out of `render.yaml`; configure them in Render's secret environment-variable form. The [Blueprint reference](https://render.com/docs/blueprint-spec) documents the `plan` field.

The implementation uses `CONTINUUM_DATABASE_URL` for the Session-pooler URL, `CONTINUUM_SUPABASE_URL` for the project URL, `CONTINUUM_SUPABASE_SECRET_KEY` for the server key, and `CONTINUUM_SUPABASE_BUCKET` for the private bucket name. Production settings prefer Render's platform `PORT` value over the image's local `CONTINUUM_PORT` default; local development continues to use `CONTINUUM_PORT`. Do not put a real credential in `.env.example` or evidence. An ignored `.env` may hold local adapter-test credentials; `make smoke-hosted` still refuses outside a Render web process.

## T03 implementation contract

### Database

- Use SQLAlchemy 2 Core with the psycopg 3 driver. Select the provider-generated Supavisor **Session** pooler URI, with TLS required.
- Keep the application-side pool deliberately small for one 512 MB Render instance: start with `pool_size=1`, `max_overflow=0`, and `pool_pre_ping=True`. Set timeouts and recycle connections only as needed by observed behavior. The pooler can reset a long-lived connection, so a broken connection must fail the current operation and be re-opened for a later one.
- T03 is not a runtime schema task. Before the hosted smoke, create these two T03-only tables in Supabase SQL Editor:

  ```sql
  create table if not exists public.t03_connectivity_probe (
      probe_id uuid primary key,
      created_at timestamptz not null default now()
  );
  alter table public.t03_connectivity_probe enable row level security;

  create table if not exists public.t03_hosted_smoke_claims (
      run_id uuid primary key,
      probe_id uuid not null unique,
      object_key text not null unique,
      status text not null check (status in ('claimed', 'completed', 'failed', 'unknown')),
      result_code text,
      claimed_at timestamptz not null default now(),
      finished_at timestamptz
  );
  alter table public.t03_hosted_smoke_claims enable row level security;
  ```

  Insert one row keyed by a random UUID, commit it, read it using a fresh connection, then delete that exact row. Do not create generic job, event, user, or artifact tables here. If the probe fails before cleanup, record the UUID and clean it up explicitly after inspection. Drop the probe table after T03 evidence is accepted if no later task needs it. Retain the tiny claim rows so consumed run IDs cannot execute again.
- Before any probe mutation, the service inserts and commits one `t03_hosted_smoke_claims` row keyed by the explicitly configured run UUID. This tiny T03 tombstone prevents the same opt-in run from repeating across process restarts. Keep the claimed run ID immutable and retain the claim row after completion; update only its status, safe result code, and finish time. A duplicate run ID is never rerun: report the stored `completed` result or the prior unresolved/failed state without inferring success. If the claim commit response is ambiguous, stop before touching Storage or probe rows. Do not create a retry loop or delete claim rows.
- Do not put network calls or object transfers inside an open database transaction. A database connection is used only for the bounded probe insert/read/delete.

### Private object storage

- Keep storage behind a small object-store interface. The hosted adapter uses bounded HTTPX calls to Supabase Storage's REST API, under the configured project URL and exact private bucket. Do not add the Supabase client SDK solely for this connectivity spike.
- Use one random T03-only object key per run, for example `t03-probes/<uuid>.bin`. The fixed tiny fixture contains no user or repository data. Upload with overwrite/upsert disabled, read the object through the authenticated/private endpoint, compare exact bytes and SHA-256, then issue one cleanup request for that exact key after successful verification.
- Use the new server secret API key in the `apikey` header and TLS. The key bypasses RLS, so restrict it to the trusted backend and redact request headers. The bucket stays private; a public URL or signed URL is not needed for this server-to-server proof.
- Apply explicit connect, write, read, and overall deadlines. Reject a fixture larger than the T03 byte cap before sending it. Stream downloads with an accumulated-byte cap; do not trust only `Content-Length`.
- Do not automatically retry upload or delete. A timeout after sending a mutation has an unknown result. Preserve the unique probe key and report `UNKNOWN` for manual inspection/cleanup instead of repeating the write. Do not mark an ambiguous upload as successful.
- After a successful DELETE acknowledgment, verify the private authenticated object endpoint returns HTTP 404 before claiming cleanup. Current Supabase Storage error documentation maps a missing key (`NoSuchKey` or legacy `not_found`) to 404. Treat other responses, including a 400 body that appears to describe a missing object, as unresolved until the hosted endpoint's behavior is evidenced; do not infer absence from body text. See [Supabase Storage error codes](https://supabase.com/docs/guides/storage/debugging/error-codes).

### Explicit hosted-smoke gate

The startup hook must refuse to run unless it sees all of: production mode, Render's runtime marker and service metadata, `CONTINUUM_ENABLE_HOSTED_SMOKE=1`, and a valid one-time `CONTINUUM_T03_HOSTED_SMOKE_ID` UUID. The startup process consumes that ID before any probe mutation. Setting the flag in Render and choosing **Save and deploy** runs the bounded probe before the API begins serving; inspect the sanitized result in Render logs. After the attempt, immediately set the flag back to `0` and choose **Save and deploy** again. Never reuse the consumed ID; choose a new UUID only for a deliberate additional smoke. The durable claim prevents duplicate writes if Render restarts while the flag is still enabled.

The local `make smoke-hosted` command is a guard, not a way to fake remote proof. It must refuse unless the process is actually running with Render runtime/service metadata and the explicit opt-in settings. Render Free web services do not provide a shell, SSH, or one-off jobs, so for this plan the operator enables the one-startup probe in Render's environment settings instead. Render documents this restriction in [SSH and Shell Access](https://render.com/docs/ssh) and the [Free service limits](https://render.com/docs/free).

For one deliberate hosted attempt, set these values in Render's Environment dashboard and choose **Save and deploy**:

| Variable | Value |
|---|---|
| `CONTINUUM_ENABLE_HOSTED_SMOKE` | `1` |
| `CONTINUUM_T03_HOSTED_SMOKE_ID` | A fresh UUID |

Free Render does not offer a shell, SSH, or one-off jobs, so this dashboard change triggers the startup hook in the web process. `make smoke-hosted` is a guard for compatible Render processes and refuses on an operator laptop. The target and startup hook may use only the configured Render/Supabase fixture. They must not call paid model APIs, create or change cloud resources, enable billing, or silently substitute local fakes for missing credentials. A run using local fake Postgres/Storage proves adapter behavior only; it is not hosted T03 evidence. Do not execute the hosted target from CI or an untrusted pull request.

For each attempted hosted run, record the exact commit, UTC timestamp, selected plans/regions, redacted service/project identifiers, database probe result, object upload/download/hash/cleanup result, provider usage before and after, and memory readings in [the evidence report](evidence/t03-hosted.md). Record `NOT RUN`, `BLOCKED`, or `FAILED` accurately; never fill a template field with a plan-page value and call it account evidence.

## Evidence and recovery

Use the [T03 hosted evidence template](evidence/t03-hosted.md). Capture account-specific facts without copying account email addresses, full project URLs, passwords, API keys, connection strings, access tokens, request headers, or private data. Before and after the probe, inspect Render workspace usage and Supabase database, Storage, and egress usage. Stop if the smoke would require an upgrade, paid fallback, a payment method, or a quota increase.

If an upload result is uncertain, leave the random key recorded as unresolved and inspect the private bucket before cleanup. If a database commit result is uncertain, look up the specific probe UUID before issuing cleanup. Do not repeat a mutation just because its response was lost. If a resource is paused or suspended, record the pause and use only the provider's free-plan recovery path; do not upgrade it to make T03 pass.

## Official references

- [Render Free services and usage limits](https://render.com/docs/free)
- [Render compute plans](https://render.com/docs/compute-plans)
- [Render outbound bandwidth](https://render.com/docs/outbound-bandwidth)
- [Render outbound IP addresses](https://render.com/docs/outbound-ip-addresses)
- [Render Blueprint YAML reference](https://render.com/docs/blueprint-spec)
- [Render web service port binding](https://render.com/docs/web-services#port-binding)
- [Supabase Postgres connection methods](https://supabase.com/docs/guides/database/connecting-to-postgres)
- [Supabase connection pooling and limits](https://supabase.com/docs/guides/database/connecting-to-postgres/pooling-and-limits)
- [Supabase Storage private buckets](https://supabase.com/docs/guides/storage/buckets/fundamentals)
- [Supabase Storage standard uploads and overwrite behavior](https://supabase.com/docs/guides/storage/uploads/standard-uploads)
- [Supabase Storage access control](https://supabase.com/docs/guides/storage/security/access-control)
- [Supabase Storage REST API reference](https://supabase.com/docs/reference/self-hosting-storage)
- [Supabase API keys and safe server use](https://supabase.com/docs/guides/getting-started/api-keys)
- [Supabase Free plan pricing](https://supabase.com/pricing)
- [Supabase Free project pausing](https://supabase.com/docs/guides/platform/free-project-pausing)
