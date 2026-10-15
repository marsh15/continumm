# OpenAI model adapter

The OpenAI adapter translates the durable provider-neutral model turn into one
call to the official Python SDK's Responses API (locked to SDK 2.54.0). It pins
`gpt-5.4-mini-2026-03-17`, uses `max_retries=0`, applies a 45-second total
deadline (maximum 60 seconds), sends the durable output reservation as
`max_output_tokens`, sets `parallel_tool_calls=false`, and always sends
`store=false`. There is no model or provider fallback.

Only the supported workspace functions are sent. Schemas must be strict JSON
objects with all properties required and `additionalProperties=false`. The
adapter preserves provider call IDs and response order. The durable loop
validates the complete response and every argument against local tool
contracts before it persists any invocation; incomplete responses cannot
dispatch tools.

The `NormalizedModelResponse` contract records visible text, ordered calls,
finish status, the configured model ID, provider response and request IDs,
input and output tokens, cached input and reasoning output categories when
reported, latency, and continuation items. With `store=false`, continuation
items are returned in the normalized response and can be supplied by the
server in the next turn as `context.openai_continuation_items`. That key is
sent as Responses API input items and removed from the serialized context text.
The continuation can include provider-encrypted reasoning content; treat it as
private run content and apply the normal run retention and deletion rules.

When OpenAI omits usage or omits either token total, both totals remain `null`
in the accepted response and database columns. The durable output reservation
still bounds the request, and a missing usage total is not represented as
zero. Run-wide request and token caps, conservative reservations, context
provenance, and budget snapshots are described in [run-scoped model
limits](model-limits.md).

The worker authorizes one credential dispatch using the stable turn UUID
before calling the adapter. The grant must match that request ID and provider;
the adapter creates one SDK client with retries disabled, closes it after the
request, and never includes credential values in normalized output or errors.
Transport, timeout, rate-limit and server failures are surfaced with stable
sanitized error codes. Since request outcome can be uncertain, the durable
loop retains the reservation and marks the response outcome unknown instead
of retrying inference.

Completed refusals preserve visible refusal text and use the distinct
`refusal` stop reason. The durable loop stores the sanitized refusal and rejects
it before creating any tool invocation. An HTTP 401/403 is a definitive
authentication rejection: the durable turn records a sanitized invalid
response and fails the attempt instead of calling it an uncertain response
loss. The one-use credential grant binds to that turn UUID, so replacing the
rejected key requires a new run until a bounded same-run credential repair
flow is implemented.

The protocol fixture is offline and uses synthetic response objects. It does
not establish live-provider behavior, successful key authorization, or an
end-to-end real task. No paid OpenAI request was run for this implementation.
`make eval-live PROVIDER=openai` currently fails closed with
`live_evaluation_harness_unavailable`; no provider request is sent. The
controlled admitted-run harness must be implemented before this command can
perform a live task, and it must not run as part of CI.

## SDK/API references

- [Official OpenAI Python SDK](https://github.com/openai/openai-python): async
  client, retries, timeouts and Responses API.
- [Responses API reference](https://platform.openai.com/docs/api-reference/responses/create):
  `store`, `max_output_tokens`, tools and continuation behavior.
