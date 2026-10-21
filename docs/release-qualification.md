# Deployed launch qualification

Status on 20 October 2026: **BLOCKED / NOT RUN**. Public admission remains off.
The owner deferred T34's live demonstration until service setup is available.
A local configuration-presence check found no runtime database, GitHub App ID/key,
or Supabase URL/key. No secret values were inspected or recorded.

Local tests, rendered screenshots, fixture evaluations and queue benchmarks do
not qualify a deployed release. The following must run against the actual
owner-configured Render/Supabase/GitHub/sandbox/provider environment:

| Gate | Evidence required | Current result |
|---|---|---|
| Connectivity | Dated `make smoke-hosted` result and private object hash/cleanup proof | Not run |
| Sandbox | Actual server-side timeout, API death and no stale result acceptance | Not run |
| Sleep/wake | Render asleep while Edge ingress commits; bounded wake/drain | Not run |
| Delivery recovery | Failed App deliveries, rate limits and inspection-history gap | Local fixtures only |
| Restoration | Private object restoration and quarantine before writes | Local fixtures only |
| OpenAI | One Python and one Node admitted-run smoke using owner BYOK | Not run |
| Anthropic | One Python and one Node admitted-run smoke using owner BYOK | Not run |
| Signature demo | Real generation, revision, exact approval and normal GitHub publication | T34 deferred |
| Public lifecycle | Fresh account completes console and comment journeys | Not run |
| Hidden correctness | Managed independent checker outside the writable workspace | Comparator/managed service implemented; activity/live execution missing |
| Adversarial corpus | Twelve deterministic tasks covering the required failure and attack classes | Local scripted coverage; model behavior unmeasured |
| Final acceptance | Every acceptance item in `tasks/plan.md` evidenced | Open |

Before qualification, follow hosting, ingress, wake, credential, operator and
retention setup documents. Keep `CONTINUUM_ENABLE_PUBLIC_ADMISSION=0`. Confirm
operator/model/sandbox allowances and explicit opt-ins before a paid or hosted
smoke. Record the immutable release commit, UTC time, redacted service identity,
resource usage, authoritative intermediate/final state and cleanup outcome.
Never turn a configuration example or expected result into measured evidence.

Only after all gates pass should the operator explicitly enable public admission.
The independent [status/help document](../STATUS.md) must remain reachable if
Render sleeps or is suspended. No automatic release or admission enablement is
part of the local test commands.

Use the [final acceptance evidence map](acceptance-evidence.md) to locate local
proof and the remaining release experiment for each plan item.
