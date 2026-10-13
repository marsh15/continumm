# T04 Vercel Sandbox setup

T16 replaces unaccounted live mutations with
[durable allocation accounting](sandbox-allocation-accounting.md). The legacy
`lifecycle`, `crash-fixture`, and `cleanup` CLI commands below now refuse with
`sandbox_allocation_accounting_required` before provider construction, even when
the old opt-in flag is set. These are historical spike procedures rather than a
current hosted execution path. Passive `sandbox-observe` remains available.
Use provider dashboard controls for protective cleanup of a legacy resource;
new hosted experiments require an accounted allocation and explicit billing
period configuration. No hosted acceptance has been claimed.

**Purpose:** run one explicitly enabled, owner-controlled lifecycle fixture for T04.  
**Current state:** local preparation is complete; no Vercel project has been configured and no hosted call has been made. See [the T04 evidence record](evidence/t04-hosted.md).  
**SDK reference checked:** 11 October 2026; `vercel==0.11.6` is pinned in `pyproject.toml` and `uv.lock`.

The adapter creates one named Vercel sandbox with a 120-second server session limit, 1 vCPU, 2048 MiB, `persistent=False`, deny-all network egress, no ports, no mounts, no source checkout, and no sandbox environment variables. It runs one direct Python command with a 10-second server-side process deadline, checks that credential environment variables are absent without printing their names or values, and transfers a deterministic 1 KiB fixture in 64-byte chunks. Captured command output is capped at 4 KiB. The adapter uses the exact `SandboxRuntimeSession` returned by create; it does not use sandbox convenience calls that can resume a stopped session.

The configured image must be an immutable reference such as `vercel/sandbox/python:3.14@sha256:<operator-supplied-digest>`. No digest is supplied by this repository. The managed Python 3.14 image is only the T04 lifecycle fixture; it does not prove the later Python 3.13 build profile in T17.

## Account and local configuration

Before running a fixture, confirm the current Vercel Sandbox plan eligibility, Hobby limits, and account usage. Keep the account on its eligible no-upgrade plan and do not enable paid fallback or attach a payment method to bypass a limit. Stop if account settings or plan terms do not meet the project’s non-commercial budget assumptions. The [Sandbox pricing and quotas](https://vercel.com/docs/sandbox/pricing) and [fair-use rules](https://vercel.com/docs/limits/fair-use-guidelines) are provider policy, not a promise that any particular account can create a sandbox.

Create or select the owner’s Vercel project in the dashboard and obtain a project-scoped token, team ID, project ID, and immutable managed-image digest. Keep them in a local ignored `.env`; do not commit them or paste them into evidence. The SDK uses these values only to call Vercel’s control plane. The adapter supplies no credentials to the sandbox’s `env`, command environment, source, or mounts.

Set these local values for one deliberate attempt:

```dotenv
CONTINUUM_ENABLE_SANDBOX_SMOKE=1
CONTINUUM_SANDBOX_IMAGE=vercel/sandbox/python:3.14@sha256:<operator-supplied-digest>
CONTINUUM_T04_SMOKE_ID=<fresh-canonical-uuid-used-once>
CONTINUUM_VERCEL_TOKEN=<project-scoped-token>
CONTINUUM_VERCEL_TEAM_ID=<team-id>
CONTINUUM_VERCEL_PROJECT_ID=<project-id>
```

Use a fresh canonical UUID for each new, deliberate create attempt. The deterministic provider name is `continuum-t04-` followed by that UUID’s 32 lowercase hex digits. Do not reuse the ID after an uncertain create result. `make smoke-sandbox` and `make sandbox-crash-fixture` require this explicit ID and opt-in; without them they stop before calling the SDK. A duplicate name is surfaced as a provider outcome and never replaced with a random name.

## Bounded lifecycle fixture

Run `make smoke-sandbox` once. It writes one redacted JSON result to stdout, including the stable sandbox name, session ID suffix, command result, credential-presence boolean, fixture byte count and digest, and stop/destroy observations. The expected result is `passed`, `credential_environment_present: false`, `file_round_trip_matches: true`, and confirmed stop and destroy. Save the JSON fields in the evidence record, excluding the token, team/project IDs, and any dashboard secrets.

The filesystem write and read use the SDK’s streaming `fs.open()` methods on the exact session handle. Each write chunk is at most 64 bytes and the total transfer cap is 64 KiB. The smoke fixture is 1 KiB, so the upload contains multiple chunks. The adapter never calls `capture_output=True`; it drains stdout and stderr incrementally and kills a process once if combined output exceeds the cap. That command’s effect is then reported as unknown because it may have run partially.

Create, command, transfer, stop, and destroy each have bounded client deadlines in addition to the server-side session or process limit. A successful remote acknowledgment is recorded as applied. A definite provider rejection is recorded as not applied. A timeout, connection loss, 5xx, malformed response, or failed follow-up observation is unknown. No mutation is retried. After an unknown create, the harness does one passive lookup by the same stable name and reports the observation; it does not stop/destroy automatically or create a replacement. After an unknown stop or destroy, use the exact reported name with the manual observation and cleanup commands below.

`make sandbox-observe NAME=continuum-t04-<uuid>` is read-only. `make sandbox-cleanup NAME=continuum-t04-<uuid>` stops a running exact session once, destroys the named sandbox once with orphan snapshot deletion requested, then confirms absence with one passive lookup. If either result is unknown, inspect that exact name in the Vercel project before deciding on any further action. Do not issue a second create, stop, or destroy simply because the first response was lost.

The adapter does not persist allocation reservations across API process death. T16 owns durable allocation persistence. Until then, unknown remote allocations require manual inspection and must not be treated as free capacity.

## Manual server-timeout proof after killing the client

This fixture starts a harmless sleeping process with a 20-second Vercel-side `kill_after` and then leaves the client process idle. It does not access credentials or perform repository work.

1. Set a fresh `CONTINUUM_T04_SMOKE_ID` and the explicit local configuration above. In terminal A, run `make sandbox-crash-fixture` and record the JSON `sandbox_name`, `process_id`, and `local_pid`.
2. From terminal B, kill exactly that local API process with `kill -KILL <local_pid>`. Do not use Ctrl-C for this proof because a graceful interruption may close the SDK context.
3. Wait at least 25 seconds. In terminal B, run `make sandbox-observe NAME=<sandbox_name> PROCESS_ID=<process_id>`. This path calls the SDK’s passive `get_sandbox(name=...)` and reads the named process on the current session; it does not resume a stopped sandbox. Record the sandbox/session status, process status, and exit code. The process should be `exited` after Vercel enforces its 20-second server-side deadline even though the local process died.
4. Run `make sandbox-cleanup NAME=<sandbox_name>` and record the stop and destroy observations. Confirm a final passive observation reports `absent`.

If the observation is unknown, the session still appears active, the process remains running after the server deadline, or cleanup is not confirmed, record that result and stop. Do not repeat create or lifecycle mutations with the same ID. Inspect the exact name in Vercel and reconcile it manually before any new fixture.

This is a separate, explicit manual proof; it is not run by unit tests, CI, `make check`, or `make build`. The standard lifecycle smoke’s credential check tests only whether known credential variable names are present and never prints those names or values. The sandbox is configured with no environment injection regardless of that check.

## SDK behavior and later network-policy transition

The official [Python SDK reference](https://vercel.com/docs/sandbox/python-sdk-reference) documents `create_sandbox`, exact runtime sessions, bounded `run_process`/`create_process`, streaming `fs.open`, stop, destroy, and passive `get_sandbox(name=...)`. The pinned `get_sandbox` function has no `resume` argument and is passive by default. Sandbox-level process/filesystem convenience methods use a lifecycle recovery helper that can resume and replay an operation once. The adapter takes `box.current_session` from the create response and uses only its direct process and filesystem methods. `SandboxRuntimeSession` calls do not auto-resume or replay after stop. The SDK transport makes one `client.send()` call; no transport retry is configured.

T04 creates the initial fixture with deny-all egress and executes no repository code. Before later dependency downloads or test execution, follow [plan section 3.3](../tasks/plan.md): use `SandboxRuntimeSession.update_network_policy()` for an active-session transition, await it, refresh the policy, and prove an outbound probe fails before starting repository code. Sandbox configuration `update(network_policy=...)` changes the next session only.
