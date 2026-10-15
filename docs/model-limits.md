# Run-scoped model limits

At its first durable turn preparation, each run pins the provider/model identity and its initial task,
objective, constraints, policy, and tool-schema digest. Later turns cannot
switch provider or exact model ID or replace the pinned task inputs. Rework and
repair turns share the same run ledger; they do not receive a fresh allowance.
Public command admission is a later task; it must supply the initial task snapshot
before this boundary. Revision feedback is rejected until T28 provides a durable
feedback record, rather than accepting caller-supplied history or feedback.

The current limits are 30 durable model request intents per run, 120,000
input tokens per run, 30,000 output tokens per run (used plus reserved), and
4,096 output tokens per request. Reservations are made under a row lock before
credential dispatch. Replaying the same logical turn returns its original
intent without reserving twice. Concurrent prepares serialize against the
same ledger, so both cannot reserve the same remaining capacity.

Input is estimated locally from the canonical JSON of the complete persisted
provider request, including transformed accepted history, tool schemas and
the bounded server-built context. A fixed 1,024-token envelope allowance is
added for provider framing. This conservative byte-based estimate makes no
remote token-count request and can differ from provider billing usage. The
canonical request plus provider continuation is limited to 65,536 UTF-8 bytes;
required native continuation is never silently truncated or dropped.

Known provider usage settles the corresponding token reservation from the
reported totals. If a provider reports more than reserved, the known amount is
recorded and the run's budget is latched as exceeded; no tool calls are
executed. Missing usage, incomplete responses, timeouts, transport failures,
rate limits, server errors, and other ambiguous outcomes keep the full token
reservation and mark usage uncertain. The loop does not retry inference
automatically, so an unknown outcome cannot cause an unbounded regeneration.
Definitive HTTP authentication and request rejections are recorded as invalid
responses and release the token reservations because no model generation was
accepted; their request intent remains counted. Correcting an invalid key
currently requires a new run.

`DurableAgentLoopService.read_model_budget(run_id)` provides an owner-scoped
snapshot with the pinned model, request intents reserved, dispatches
authorized, responses received, ambiguous/in-flight counts, used and reserved
input/output tokens, remaining allowances, unknown-usage count and an
exceeded flag. `requests_reserved` counts durable intents, not proof that an
HTTP request was sent. Authorized but unresolved requests are visible as
in-flight or ambiguous.

New provider context is reconstructed only from the run-pinned inputs and
verified durable accepted turns and tool results. It preserves provider-native
continuation (including encrypted or signed blocks), call/result IDs and
order. Unaccepted or legacy turns with missing T26 provenance cannot enter a
new provider request; an already persisted legacy accepted result may still be
reused by its existing workflow. The caller cannot inject accepted results or
provider history. Existing prepared turns with no verifiable source manifest
fail closed rather than being sent after upgrade.

No live or paid provider evaluation is part of CI. The `eval-live` command
continues to refuse dispatch until the controlled admitted-run harness is
available and explicitly configured.
