# T05 durable webhook ingress design

**Purpose:** implement the small GitHub webhook receiver in T05.  
**State:** local implementation is complete after T04 commit `c995aac`; hosted acceptance is **NOT RUN** because no Continumm Supabase project/webhook fixture is configured.

The canonical acceptance criteria are in [T05](../tasks/todo.md#t05--durable-webhook-ingress-spike), with the durable-inbox requirements in [plan §9.6](../tasks/plan.md#96-durable-webhook-ingress), the wake policy in [plan §4.4](../tasks/plan.md#44-wake-and-scheduling-policy), and database invariants in [plan §5.2](../tasks/plan.md#52-database-invariants).

## Proposed minimum design

Keep the receiver as a standalone Supabase Edge Function. Supabase Edge Functions run TypeScript on a Deno-compatible runtime, and Supabase documents connecting to Postgres directly from an Edge Function with the Deno Postgres driver. The request handler should be a small Web API TypeScript module with injected inbox persistence. The entry point exports the handler through the Supabase `fetch` contract and wires in the database pool and secrets. `Deno.test` exercises the actual handler and signature code; do not reimplement signature verification in Python as the only test. The Python backend remains the owner of later command interpretation and runtime work.

Use Web Crypto `crypto.subtle` to import the configured GitHub webhook secret as an HMAC-SHA256 key and call `crypto.subtle.verify` on the decoded `X-Hub-Signature-256` signature and the exact request bytes. Require the `sha256=` prefix and exactly 32 decoded signature bytes. Read the request stream with a hard 1 MiB accumulated-byte cap; use `Content-Length` only for early rejection, never as the sole limit. Reject unsupported content types and malformed JSON after signature verification. Hash the accepted raw bytes with SHA-256 for inbox conflict checks. Do not normalize, parse, or re-serialize bytes before checking the HMAC.

Give the HTTP request an eight-second application deadline. The timer covers body streaming (including a slow or stalled stream and chunked requests without `Content-Length`), pool acquisition, and database statements. Cancel body reading when its deadline expires. Treat `Content-Length` as an early-reject hint only; enforce the byte count from stream chunks even when the header is absent or forged. This leaves time within GitHub's ten-second response limit. Do not acknowledge before the database transaction commits. A deadline, connection error, or uncertain commit returns a failure response; a later retry resolves through the delivery key and payload digest. Do not retry an insert after a database error or uncertain commit. There is no background task, Render wake call, or outbound GitHub API call in T05.

Persist one raw, signed body per GitHub App delivery in a private PostgreSQL schema. Suggested table fields are configured GitHub App ID as `bigint`, `X-GitHub-Delivery` UUID, `X-GitHub-Event`, optional payload `action`, 32-byte SHA-256 digest, raw body bytes (at most 1 MiB), and database receipt time. Read the configured App ID as a positive decimal string and pass it through as a decimal string or `bigint`; never parse it through JavaScript `Number`, which can truncate larger IDs. Enforce `UNIQUE (github_app_id, delivery_id)` and a database check that the payload byte length is within the cap. Use parameterized SQL and schema-qualified names. Persist no authentication headers, signature headers, secrets, or other request headers.

On a first delivery, insert and commit, then return 202. If the key already exists, read the committed row: a matching digest is an adopted duplicate and may return 202; a different digest returns 409 and must not overwrite the first payload. This makes a lost HTTP acknowledgment safe: GitHub redelivers with the same delivery UUID, and the second request observes the committed row. Database failure or uncertain commit returns 503. Do not convert a database failure into a success response.

Suggested response mapping: 202 for committed new or same-digest duplicate; 400 for malformed required headers/JSON; 401 or 403 for missing/invalid signature; 405 for other methods; 409 for one delivery UUID carrying different signed bytes; 413 for a body over 1 MiB; 415 for an unsupported content type; 503 for database/deadline failure. Return short generic bodies and safe request IDs; never echo payloads or credentials.

## Schema, migration, and credentials

T05 adds Alembic configuration and an additive, reversible migration for the inbox schema and table. Alembic's own version table is created in a separate, private `continuum_migrations` schema, never in `public`. Provision the dedicated login role and password through an operator-controlled channel first; keep the password out of migrations and source control. The role must be `LOGIN`, `NOINHERIT`, and have no `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `REPLICATION`, or `BYPASSRLS` flags and no membership in or to another role. The migration checks those attributes, then creates the inbox schema/table, policies, and grants. Use the sanitized `python -m continuum_api.migrations upgrade head` or `downgrade base` wrapper so malformed DSNs and migration failures do not print connection details. Migrations are online-only because they check that the precreated database role and target database exist. A privileged migration connection is used only for migration/provisioning and never stored in Edge Function secrets.

The Edge Function uses a different `continuum_ingress` login with database connect, `USAGE` on the private schema, column-limited `INSERT` for the immutable event fields, and column-limited `SELECT` of the delivery key and digest only. `received_at` comes from the database default. The table has RLS enabled and forced, with insert/select policies only for that role. It is not the schema/table owner; it has no `UPDATE`, `DELETE`, or permanent-object `CREATE` rights and is not a member of `postgres`, `service_role`, or another privileged role. PostgreSQL may grant temporary-table capability through `PUBLIC`; audit and record effective grants without changing database-wide `PUBLIC` settings. The Edge Function does not use the T03 backend connection, a Supabase secret API key, or the later runtime role.

Keep the inbox schema out of Supabase Data API **Exposed schemas**. Use direct PostgreSQL access from the Edge Function through the project's shared transaction pooler, with TLS, one application connection per warm function instance, and bounded connection/query timeouts. This is the Edge Function's own credential, using the transaction pooler endpoint and role-specific credentials. Store the GitHub webhook secret and this database URL as separate Supabase Function secrets. The App ID is configuration, not the HMAC secret. Keep all three distinct from Render credentials and the migration credential.

Use the Supabase-documented Deno Postgres dependency (`jsr:@db/postgres`) rather than adding a Supabase JS client or using a broad `sb_secret_` key. The official guide demonstrates a pool size of one and direct SQL from Edge Functions; the general connection guide recommends shared transaction pooling for serverless/edge functions. Verify that the deployed function's connection string comes from the project's Connect panel and uses the role's transaction-pooler login form; do not construct the pooler hostname or username.

A private schema plus narrow direct-Postgres grants is the access boundary; browser clients receive no database credential. Do not expose the inbox table through PostgREST/GraphQL. RLS is not a substitute for these grants: the Edge Function connects as a dedicated database role, not an end-user JWT.

## Local verification contract

Use the isolated Deno CLI package under `tools/edge` to run `deno check` and `deno test` on the exact TypeScript source; this needs no frontend test harness. The local PostgreSQL integration test exercises the real Deno driver against PostgreSQL 17. Docker is available for the disposable database. The Supabase CLI and its full local Edge runtime are not needed for these checks, and T05 does not install or invoke them. The T05 database fixture is named `continuum_t05_test` and uses `CONTINUUM_T05_TEST_DATABASE_URL`; the migration wrapper receives that value explicitly through `CONTINUUM_MIGRATION_DATABASE_URL`. Do not use T03's `CONTINUUM_TEST_DATABASE_URL` for T05.

Use a disposable local PostgreSQL 17 database for integration tests, separate from the existing T03 database. Tests must exercise real PostgreSQL and the actual TypeScript receiver/driver path for:

- migration upgrade, downgrade, and re-upgrade, with Alembic metadata confined to `continuum_migrations`;
- first delivery commits exactly one row;
- same App ID, delivery UUID, and payload returns duplicate success and still leaves exactly one row;
- same App ID and delivery UUID with a different payload digest returns conflict and preserves the original row;
- a deliberately discarded success response followed by redelivery adopts the committed row, leaving one row (lost-ack recovery);
- database failure or commit uncertainty produces failure, never acknowledgment;
- the ingress login can insert the allowed inbox columns and read only the delivery key/digest, while update/delete, raw-body reads, unrelated-table reads, and permanent DDL fail;
- known GitHub HMAC vector, wrong secret, altered raw byte, malformed signature, missing signature, missing delivery UUID, malformed JSON, wrong content type, exactly-at-limit body, and one-byte-over-limit body.

Signature and request parsing tests may use the Deno test runner with injected persistence. Deduplication and rollback tests must use real local PostgreSQL. A fake inbox or local handler result is preparation evidence only and is never recorded as hosted proof. `make smoke-hosted` is not the T05 acceptance command; hosted evidence must exercise the actual deployed Edge Function and hosted Supabase database.

## Tool and dependency choice

The workstation has Node 26.10.0 and Docker. Node's TypeScript stripping is not selected for source tests because production runs on Deno and Deno's Web APIs and module resolution are the target runtime. Deno 2.9.6 is pinned as an isolated developer dependency in `tools/edge`; the Supabase CLI is unnecessary for local handler and database verification.

Implementation dependencies and tools:

- Deno CLI for `deno fmt`, `deno check`, and `deno test`; pin it as an isolated official `deno` npm package in `tools/edge` rather than changing global machine settings.
- `jsr:@db/postgres@0.19.5` pinned in the function's own `deno.json` for the database client.
- `jsr:@std/assert@1.0.14` pinned through the Deno lockfile for Deno tests.
- Alembic `1.20.0` and its small configuration/migration environment for the inbox schema.

No new frontend package, Python GitHub SDK, Supabase JS client, HMAC library, Node TypeScript runner, or secret API key is needed for the receiver. Pin Deno module versions and lock the Deno dependency graph. A Supabase CLI can be added later if a deployment workflow requires it; it is not part of T05 local verification.

## Hosted setup (not performed)

When a Continumm Supabase project and webhook fixture are available for the hosted stage:

1. Confirm the selected plan, project region, budget settings, and current quota. No Continumm hosted ingress fixture is configured today.
2. Create the `continuum_ingress` login through an operator-controlled `psql` connection. Run the role statement, then use psql's interactive password command so the password is not part of a saved SQL statement or shell history:

   ```sql
   CREATE ROLE continuum_ingress LOGIN NOINHERIT NOSUPERUSER NOCREATEDB
     NOCREATEROLE NOREPLICATION NOBYPASSRLS;
   ```

   ```text
   \password continuum_ingress
   ```

   Confirm the role flags and that it has no role membership, then apply the T05 Alembic migration through the separate privileged migration channel. Verify the schema is absent from Data API Exposed schemas and inspect effective database privileges for the ingress login.
3. Set Supabase Function secrets for the GitHub App numeric ID, the high-entropy webhook secret, and the least-privilege ingress transaction-pooler connection string. Do not put secret values in CLI args, terminal history, this document, or evidence.
4. Configure the Edge Function with `verify_jwt = false`; GitHub does not send Supabase user JWTs. Signature verification inside the handler is mandatory. Do not use the local `--no-verify-jwt` flag as a production setting substitute.
5. Deploy only the T05 function. Configure the GitHub App's webhook URL to that Edge Function, select JSON payloads and only the required events, and set the same random webhook secret in the GitHub App. Save the secret in both systems without copying it into source control.
6. Use a non-sensitive test fixture. Confirm the first signed delivery commits and receives 202, then redeliver the same GitHub delivery and confirm 202 with one row. Exercise changed content under a reused delivery UUID in a controlled local/hosted test fixture; expect 409 and no overwrite. Exercise an invalid signature and a payload over 1 MiB; expect rejection and no insert.
7. To demonstrate the lost acknowledgment case, allow the receiver transaction to commit while a controlled test client discards the HTTP response, then replay the original delivery. Confirm one stored row and a duplicate success. Record a hosted result only after checking the database directly and the GitHub delivery outcome.
8. Record plan/quota, redacted project/app identifiers, commit, UTC time, Edge invocation, database row count/digest, expected statuses, and cleanup in [T05 hosted evidence](evidence/t05-hosted.md). Keep the actual raw fixture limited to synthetic data. Leave hosted status **NOT RUN** until these observations exist.

T05 does not implement the five-minute recovery scan or GitHub failed-delivery polling; those belong to [T39](../tasks/todo.md#t39--failed-delivery-recovery). Queue drain and the Render wake dispatcher belong to T06 and must follow the plan's work-triggered policy. GitHub does not automatically redeliver failed deliveries, so successful T05 ingress is necessary but not a complete recovery mechanism.

## Official references

- [Supabase Edge Functions quickstart](https://supabase.com/docs/guides/functions/quickstart)
- [Connect to Postgres from an Edge Function](https://supabase.com/docs/guides/functions/connect-to-postgres)
- [Supabase Postgres connection methods and poolers](https://supabase.com/docs/guides/database/connecting-to-postgres)
- [Supabase private and custom schema exposure](https://supabase.com/docs/guides/api/using-custom-schemas)
- [Supabase least-privilege custom roles and inherited grants](https://supabase.com/docs/guides/troubleshooting/custom-role-inherits-privileges-that-were-not-explicitly-granted-ddaa1c)
- [Supabase Edge Function JWT configuration](https://supabase.com/docs/guides/functions/function-configuration)
- [Deno npm installation and runtime](https://docs.deno.com/runtime/getting_started/installation/)
- [Deno TLS connection verification](https://docs.deno.com/api/deno/network/)
- [Deno test runner](https://docs.deno.com/runtime/reference/cli/test/)
- [GitHub webhook signature validation](https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries)
- [GitHub webhook delivery latency and idempotency guidance](https://docs.github.com/en/webhooks/using-webhooks/best-practices-for-using-webhooks)
- [GitHub failed webhook deliveries](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries)
