# Trusted workflow composition

`build_workflow_activity_runner` connects the existing handlers into one fixed
`ActivityRunner` registry:

| Activity | Trusted implementation |
|---|---|
| `source_preparation` | Exact admitted source acquisition and baseline checkpoint |
| `agent_turn` | Admission-bound inputs, durable model reservation and credential dispatch |
| `agent_tool` | Persisted invocation, current policy, source tools and candidate checkpoint |
| `candidate_validation` | Persisted baseline/candidate resolution and managed validation |
| `approval_freeze` | Exact validated manifest and review wait |
| `publish_approved` | Existing approved branch/PR publication and reconciliation |

Agent turns and tools share one dispatcher. A final turn schedules only
revision-bound, replay-safe candidate validation. Approval and publication retain
the existing owner decision and exact-manifest requirements; registration does
not approve a candidate or invoke a handler.

Construction performs no network or provider work. The caller explicitly invokes
`run_admitted` with a run ID, job limit and time limit, and remains responsible for
closing the injected storage, database and adapter resources. The registry factory accepts
trusted service dependencies, not an arbitrary handler map. The production
resource builder supplies them for the explicitly selected workflow CLI mode.
Choose the model adapter for the admitted provider; a mismatched adapter must
fail its existing request validation before credential dispatch.

## Tool policy and schemas

`PersistedToolPolicyContextResolver` supplies both the agent-tool callback and
its `for_validation` callback. It derives the source/profile identity and baseline
from immutable admission and validation pins, verifies source bytes, discovers
original test paths, and protects the pinned manifests. It checks current owner,
lease, revision and budget state, obtains fresh repository/App authority, then
rechecks the lease and budget after authorization returns. A proof for another
repository or actor cannot authorize the tool.

Only `list_files`, `read_file`, `search_text`, `apply_patch` and `diff` are granted.
Full tool execution still performs path authorization and enforces sensitive-file,
protected-test, patch and artifact limits. The context gate alone is insufficient
to read or modify source. Tools already queued by a valid final model turn do not
need an allowance for another model request; explicit model overrun and existing
turn/invocation bounds remain enforced.

`fixed_model_tool_schemas` derives exactly these five schemas from the argument
contracts. It removes the internal tool-name discriminator, inlines nested edit
schemas and marks every property required with extra properties forbidden.
Nullable fields retain their null option. This follows the
[OpenAI strict-mode contract](https://developers.openai.com/api/docs/guides/function-calling#strict-mode)
and is checked against both existing provider adapter normalizers. Each call
returns independent schema values so one run cannot mutate another's schemas.

`build_workflow_resources` now selects the immutable admitted provider/profile,
validates owner/run/retention and sandbox period/image configuration, and builds
the concrete graph for `--execute-workflow`. It owns storage/client/pool cleanup
and uses the selected profile image only; no model API key is loaded from the
environment. See the [worker command and configuration](activity-runner.md).

Registry tests use local storage and synthetic provider credentials with a
fake SDK response. They do not qualify a hosted model or sandbox run, and public
admission remains disabled. See [release qualification](release-qualification.md).
