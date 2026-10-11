# T04 hosted sandbox lifecycle evidence

**Status:** NOT RUN  
**Prepared:** 11 October 2026  
**Related instructions:** [T04 Vercel Sandbox setup](../sandbox-setup.md)  
**SDK:** `vercel==0.11.6`, pinned in `pyproject.toml` and `uv.lock`  
**Provider documentation:** [Vercel Python SDK reference](https://vercel.com/docs/sandbox/python-sdk-reference)

Local implementation and fake-SDK contract tests are not hosted isolation evidence. No Vercel project or sandbox has been configured for this task. No hosted Vercel calls or paid operations were made.

## Local verification

| Check | Result |
|---|---|
| SDK version and async method signatures inspected from installed package source | COMPLETE |
| SDK create/stop/destroy HTTP retry source reviewed | COMPLETE; transport sends once, no configured retries |
| Exact-session direct process/filesystem methods checked for auto-resume/replay | COMPLETE; no replay on the `SandboxRuntimeSession` handle |
| Focused sandbox unit and adapter tests | COMPLETE; `pytest -q tests/unit/test_sandbox.py tests/adapters/test_sandbox_sdk.py` (17 passed) |
| Real pinned SDK exact-session handle regression | COMPLETE; included in the focused adapter tests |
| Full `make check` backend suite | COMPLETE; 66 passed, 2 PostgreSQL-gated skips |
| Full `make check` frontend typecheck and tests | COMPLETE; `make test-ui` passed, 3 tests |
| Package, frontend, and container build | COMPLETE; `make build` exited 0 |
| Hosted CLI default refusal | COMPLETE; `CONTINUUM_ENABLE_SANDBOX_SMOKE=0 make smoke-sandbox` refused before SDK calls |
| Malformed-flag and secret-sentinel CLI refusal tests | COMPLETE; blocked without echoing inputs |
| Hosted create, command, file stream, stop, destroy | NOT RUN |
| Server-side command timeout after killing local API process | NOT RUN |
| Credential environment check inside hosted fixture | NOT RUN |

## Account and resource record

| Field | Value |
|---|---|
| Vercel account and Sandbox plan eligibility | NOT RECORDED |
| Project ID (redacted) | NOT SET UP |
| Image reference and SHA-256 digest | NOT SET UP |
| Project usage before/after | NOT RECORDED |
| Payment method / paid fallback state | NOT RECORDED |
| Operator smoke UUID | NOT RUN |
| Sandbox name | NOT RUN |
| Session ID suffix | NOT RUN |
| Requested session limit | 120 seconds |
| Requested resources | 1 vCPU, 2048 MiB |
| Persistence / ports / mounts / source | `persistent=False`; none / none / none |
| Create network policy and sandbox env | deny-all; no environment variables |

## Lifecycle and server timeout

| Observation | Value |
|---|---|
| Named create response | NOT RUN |
| Harmless direct command exit code | NOT RUN |
| Credential variable-presence result (boolean only) | NOT RUN |
| Fixture bytes / SHA-256 | NOT RUN |
| File round-trip result | NOT RUN |
| Stop response and observed session state | NOT RUN |
| Destroy response and passive absence check | NOT RUN |
| Crash-proof local PID killed with SIGKILL | NOT RUN |
| Server `kill_after` deadline and process state afterward | NOT RUN |
| Cleanup after crash fixture | NOT RUN |
| Unknown or unresolved resource names | NOT RUN |

## Review and redaction

- Hosted test run commit and UTC timestamp: NOT RECORDED.
- No Vercel token, team/project credential, user key, environment dump, or private data is included: COMPLETE.
- No hosted resource or paid fallback was created or enabled: COMPLETE.
- Operator review and follow-up: NOT RECORDED.

When a controlled hosted fixture is run, record only the safe JSON results from the setup instructions. If create, stop, destroy, or observation is unknown, preserve the exact sandbox name and require manual inspection; do not retry the mutation.
