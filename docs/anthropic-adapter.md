# Anthropic model adapter

The adapter translates durable turns into one call to the official Anthropic
Python SDK's Messages API (locked to SDK 1.14.0). It pins
`claude-sonnet-5-5`, disables SDK retries, uses a 45-second total deadline
(maximum 60 seconds), and sends the reserved output limit as `max_tokens`.
There is no model or provider fallback. The Messages API has no `store=false`
request option; provider-side retention is governed by Anthropic's policy and
the applicable account terms, while Continuum applies its own run retention
and deletion rules.

Only the supported workspace tools are sent. Each tool uses a strict object
schema with all properties required and `additionalProperties=false`, and the
adapter requests `tool_choice=auto` with `disable_parallel_tool_use=true`.
Provider `tool_use` IDs and content order are retained. The durable loop
validates the complete response and each argument against local tool
contracts before persisting any invocation.

Anthropic Messages is stateless. The normalized continuation holds the full
assistant content in its original order, including thinking signatures and
redacted-thinking blocks. The server can pass ordered history through
`context.anthropic_continuation_messages`; after tool execution it must append
the matching `tool_result` user message for each `tool_use` before the next
request. The adapter appends the current server-built context as the final user
message. Continuation material is private run content and follows the normal
retention and deletion rules.

The normalized input total sums uncached `input_tokens`, cache-read tokens and
cache-creation tokens. It records cache-read tokens in `cached_input_tokens`,
cache creation in `cache_creation_input_tokens`, and reported thinking output
in `reasoning_output_tokens`. If either cache category or either token total is
missing, both normalized totals remain unknown (`null`), not zero. T26 owns
cumulative and conservative unknown-usage accounting.

The worker authorizes a one-use T23 credential grant using the stable turn
UUID. The grant must match the provider and request ID; the adapter closes the
SDK client after one request and excludes credentials and remote error bodies
from output. HTTP 401/403 is a definitive authentication rejection recorded
as a sanitized invalid response. Because that grant consumes the turn UUID,
the rejected key must be replaced before starting a new run until a bounded
same-run credential repair flow exists. Other uncertain provider outcomes
retain the reservation and do not trigger automatic inference retries.

The synthetic protocol tests do not establish live-provider behavior or an
end-to-end real task. `make eval-live PROVIDER=anthropic` currently fails
closed with `live_evaluation_harness_unavailable`; no provider request is sent.

## SDK/API references

- [Anthropic Python SDK](https://github.com/anthropics/anthropic-sdk-python):
  async client, timeouts, retries and Messages API.
- [Messages API reference](https://platform.claude.com/docs/en/api/messages/create):
  stateless history, cache usage, stop reasons and content blocks.
- [Tool use guide](https://platform.claude.com/docs/en/agents-and-tools/tool-use/how-tool-use-works):
  `tool_use` / `tool_result` correlation and continuation.
- [Sonnet 5.5 migration guide](https://platform.claude.com/docs/en/models/sonnet-5-5/migration-guide):
  supported model configuration and behavior.
