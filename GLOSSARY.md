# Continumm

Continumm is a developer service that turns bounded coding tasks into reviewed pull requests while preserving committed work through interruptions.

## Language

**Run:** One submitted coding task and its bounded revisions, ending in a recorded outcome.  
_Avoid_: Session, conversation, job when referring to the entire task.

**Workflow release:** A named, fixed edition of the sequence and policies used to carry out a run.  
_Avoid_: Latest workflow, mutable template.

**Step:** A logical unit of work within a run that has one accepted outcome.  
_Avoid_: Attempt when referring to the intended work.

**Attempt:** One effort to carry out a step or operation; a step may require several attempts.  
_Avoid_: New run when only execution ownership changed.

**Committed progress:** Work that Continumm has accepted as the authoritative basis for continuation.  
_Avoid_: Whatever was last visible in a worker.

**Checkpoint:** A recoverable saved version of a run's source workspace.  
_Avoid_: Event, log, model response.

**Candidate:** The exact proposed source version presented for validation and review.  
_Avoid_: Current files, latest patch.

**Validation evidence:** Recorded checks and observations associated with a particular candidate.  
_Avoid_: Proof that all possible behavior is correct.

**Approval manifest:** The fixed description of the candidate, evidence and proposed publication that a reviewer can authorize.  
_Avoid_: Blanket permission, approve latest.

**Approval:** A reviewer's decision authorizing one approval manifest while it remains valid.  
_Avoid_: Permission for future revisions.

**Revision:** A new candidate produced in response to a user-requested change within the same run.  
_Avoid_: Retry, recovery attempt.

**External effect:** A change to a system outside Continumm, such as creating a branch, pull request, or status comment.  
_Avoid_: Local tool result.

**Unknown outcome:** A situation where Continumm cannot establish whether a requested external action completed.  
_Avoid_: Failed, safe to retry.

**Reconciliation:** Establishing an external action's outcome from available evidence and linking an observed result to its intended operation.  
_Avoid_: Repeat the request.

**Cancellation:** Revocation of new execution authorizations for a run; previously authorized external actions may still occur and require observation.  
_Avoid_: Rollback, undo everything.

**Publication:** Creation or adoption of the exact reviewed GitHub branch and pull request.  
_Avoid_: Merge.

**Build profile:** A supported repository preparation and validation environment with explicit eligibility boundaries.  
_Avoid_: Arbitrary shell setup.

**Capacity wait:** A saved run that cannot proceed until execution resources become available.  
_Avoid_: Lost work, failed task.

**Run owner:** The individual account whose permissions, provider credentials and limits govern a run.  
_Avoid_: Repository owner when referring to the Continumm account.

**Recovery history:** The recorded explanation of interruptions, accepted work and subsequent continuation.  
_Avoid_: Deterministic reproduction of model reasoning.
