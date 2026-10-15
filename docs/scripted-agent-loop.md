# Scripted durable agent loop

T22 exercises the model boundary with a deterministic scripted adapter. It does
not call OpenAI or Anthropic. Provider credentials and live adapters belong to
T23–T25; cumulative model spending and context construction belong to T26.

The runtime persists a server-owned intent before invoking the adapter outside
a database transaction. A complete response must pass local schema and size
validation before its ordered tool calls become available. Partial JSON,
truncated arguments and incomplete responses cannot dispatch tools.

Turn and invocation identities remain stable across worker generations. A
replacement worker reuses a complete stored response and its call IDs, rather
than invoking the scripted model again. An uncertain dispatched request is not
permission to retry inference blindly.

Each tool call runs as a separate durable activity in response order. The
existing T08 transaction accepts one result and queues the next call together.
Patch results also use T21/T15 to attach a verified immutable checkpoint in that
transaction. Accepted state is derived from durable accepted results, avoiding
a second mutable flag that could disagree after a crash.

T19 policy controls tool permissions and preserves original tests and harnesses.
The response cannot supply its own account permissions, path allowlists, model
configuration or resource allowance. Current source is restored from committed
artifacts; validation writes never become candidate source.

This internal scripted boundary is not a public agent endpoint. Full provider
token counting, continuation formats, live task evaluation and user credentials
remain separate tasks.

The scripted boundary limits request and response JSON to 64 KiB, ten calls per
response and at most 30 reserved intents per run. It records per-request usage
and output reservations up to 4,096 tokens. These records provide the seam for
T26; they do not establish cumulative input/output token enforcement or provider
token counting.

## Verification

```sh
make test-target TARGET=tests/integration/test_agent_loop.py
```

The database proof uses a dedicated disposable PostgreSQL 17 instance with
loopback networking and the database name `continuum_t22_test`. The fixture
creates and removes cluster-wide runtime roles.

```sh
CONTINUUM_T22_TEST_DATABASE_URL='postgresql://<local-admin>@127.0.0.1:<port>/continuum_t22_test' \
  make test-target TARGET=tests/integration/test_agent_loop.py
```

## Code and evidence

- [Agent loop](../src/continuum_api/runtime/agent_loop.py) owns intent,
  response validation, stable calls and sequential execution.
- [Migration 11](../migrations/versions/20261013_11_agent_turns.py) defines
  owner-isolated intent and invocation records.
- [Integration proof](../tests/integration/test_agent_loop.py) and its
  [worker](../tests/integration/agent_loop_worker.py) exercise actual process
  death and the production persistence/tool paths.

The dedicated PostgreSQL 17 target passes ten cases. These include SIGKILL
after response persistence and after the first accepted tool call, with exactly
one inference for that response; two successive patch turns against the same
original baseline; malformed, truncated and incomplete response rejection;
unknown request attention; owner RLS; retained request reservations; output
reservation enforcement; protected-test denial; and migration rollback with a
working legacy transition. Repository code and live model providers are never
executed by these fixtures.

The full Python suite passes 439 tests with 33 optional database cases skipped.
Formatting, lint, typing across 52 source files and package build pass.
