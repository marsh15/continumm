# Publication approvals

T27 freezes one exact, validated source candidate before the owner can approve
publication. The approval service is internal to the runtime; this task adds no
HTTP route, browser authentication, review screen, or GitHub mutation adapter.

## Evidence and manifest creation

The T20 worker runs as the `candidate_validation` activity and passes its strict
`ValidationEvidence` value to `ApprovalService.record_validation`. The service
checks both phase summaries, fixed profile command and runtime, source and base
identities, committed candidate checkpoint, latest accepted candidate and its hash-verified apply-patch checkpoint chain back
to the validation baseline, and the
owner/run-scoped T16 allocation records. T08 persists that evidence with the
candidate identity and queues a replay-safe `approval_freeze` activity.

`ApprovalService.freeze` requires a current lease for that activity. It verifies
current actor write access and App access before downloading the committed
baseline and candidate objects, verifies both archives against T15 metadata,
and checks repository access and the pinned default branch again before
acceptance. It derives the changed-path patch digest from the verified file
contents and modes, then computes Git blob, tree, and commit objects in process.
The commit uses the run creation time and the fixed Continumm author and
committer identity. No repository hook, Git configuration, or repository code
participates.

The canonical manifest binds the GitHub numeric repository ID, current default
branch and pinned base commit, candidate checkpoint/archive/source tree, patch
digest and changed paths, Git tree and commit bytes/SHA, author and committer,
full-run-UUID managed branch, exact PR title/body/base/head/marker, accepted
validation result ID and SHA-256, profile, requested Git-object/branch/PR
operations, policy version, content deadline, and approval deadline. Its digest
is SHA-256 over UTF-8 JSON with sorted keys, no extra whitespace, and no NaN.
Patch paths are derived cumulatively from the verified baseline and final
candidate, so multiple incremental checkpoints remain approvable.
Git commit content is independently re-derived from the verified candidate;
caller-provided desired SHAs are not accepted.

The service inserts the immutable pending approval row while holding the
current run, step, job, and attempt locks, and then commits the manifest result
through T08 to move the run to `waiting_approval`. The row stays private while
the run is still running. If the process stops after row insertion, a replay-safe
freeze activity adopts the saved manifest and deadline, then completes the T08
transition. The candidate artifact and accepted evidence result are pinned by
owner-scoped foreign keys. The run deadline and artifact content deadline are
stronger than those pins.

## Decisions and publication handoff

The owner submits the exact approval ID and lowercase manifest SHA-256. The
service obtains a fresh typed repository access proof, locks the run before the
approval, and reads PostgreSQL time after lock waits. It accepts a decision only
for the current revision while the run waits for approval and both deadlines
remain in the future. Replaying the same stored decision is idempotent; a
different decision, digest, revision, or owner conflicts. A terminal run cannot
accept a new decision or dispatch grant; reading a prior identical decision is
still idempotent.

An approval atomically queues one `publish_approved` job using
`ATTENTION_ON_UNKNOWN` replay policy and moves the run to `running`. Rejection
terminalizes the run through the existing T11 path and erases run-scoped
credentials. A changes decision is recorded for T28's bounded rework flow.
T27 does not send GitHub writes. The later worker must request a fresh
operation-specific grant for `write_git_objects`, `update_managed_branch`, or
`create_pull_request`; the grant binds the approval digest, current revision,
run lease generation, and attempt. Each authorization is recorded as an
immutable run event. An expired or cancelled run cannot receive a grant.

`ApprovalService.expire_due(limit=...)` expires pending and approved approvals
in bounded owner-scoped batches. It locks run then approval, records expiry,
terminalizes an unfinished run as expired, and erases run-scoped credentials.
Calls that encounter expiry during decision processing commit expiry first and
return an expiry error after the transaction. The scheduler must invoke this
service for owners with due approvals; T27 does not add a global scheduler
drain.

## Repository authorization boundary

The service requires a `PublicationFactsProvider` that returns current owner,
repository name, default branch, branch head, and a `RepositoryAccessProof` for
the signed-in GitHub numeric user. The built-in default provider fails closed.
Installing the GitHub App alone is not considered user write permission. T29
must provide the live verifier and current repository facts before this flow is
available. If the default branch or its head changes after freeze, decision or
dispatch authorization fails and requires a new reviewed run.
