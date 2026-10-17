# Deterministic Git and branch publication

T31 adds a trusted internal publication boundary. It does not expose a public
publication endpoint or wire the workflow to a live GitHub App. PR creation and
reconciliation remain T32 work.

## Exact approved objects

T27 freezes the candidate archive and source tree, Git tree and commit SHA, parent,
author/committer identity, timestamp, message, managed branch, and approval digest
before the user decides. T31 loads those frozen bytes and rebuilds the objects in
trusted Python code, without running repository code, Git hooks or Git config.
An archive, manifest, tree or commit mismatch stops publication.

The adapter sends explicit author and committer dates when creating the commit
and checks GitHub's returned object SHA. Trees reference approved child hashes;
blobs carry the verified bytes. A returned hash different from the approved
identity is a conflict, not permission to publish an altered commit.

## Authorization and remote state

Mutations require the exact approval ID and manifest digest, current execution
ownership, the current revision and an unexpired approval. The GitHub boundary
rechecks the human's immutable identity, write rights and App installation access.
Publication tokens are scoped to one numeric repository ID with contents-write
permission. The GitHub App installation must grant Contents: write before this
publication path can be used; ordinary review checks still request read-only
tokens. Publication credentials remain in trusted memory and never enter repository commands,
archives, browser responses or ledger metadata.

The approved default branch and base SHA must still match. Managed branch reads
adopt only the exact approved commit SHA. Creation uses the exact managed branch
name from the manifest. Diverged branches or moved bases stop publication; the
adapter does not force update a branch, rebase a candidate or silently choose new
commit metadata.

## Durable uncertainty

The external-operation ledger commits mutation ownership before sending. Remote
I/O is outside database retry closures. A lost branch-creation response remains
uncertain; later calls read and may adopt the matching branch. A negative lookup
never authorizes another branch creation while the first may still appear.
Read-only reconciliation remains possible after cancellation or approval expiry
without reopening the run or granting a new send.

Git object hashes give content-addressed uploads an explicit identity; this is
distinct from a non-idempotent branch creation. A different approved run may upload
identical bytes under the same hash. Within one logical operation, a lost upload
response is recovered by reading that hash; no speculative resend is performed.
HTTP mutation retries are disabled.
The ledger retains bounded metadata and observations rather than source content
or credentials. Confirmed immutable objects are reused on subsequent calls. Branch
reconciliation rereads the current ref and can proceed after the source archive has
been cleaned up; a temporary read failure remains unknown instead of inventing a
branch conflict.

Publication uses resumable slices of at most 16 new Git mutations. Source archives
allow at most 19,998 file and directory nodes, and publication allows at most
20,000 Git objects including the root tree and commit. Source bytes are capped at
50 MiB. A `pending` receipt means the caller must schedule another invocation
under current execution ownership; confirmed objects are reused. The service
yields between objects so heartbeat tasks can run. The worker caller remains
responsible for lease heartbeats and durable rescheduling. A slice does not
loop until completion, authorize a replacement branch create, or mark the
workflow succeeded.

## Verification and deployment limits

`make test-target TARGET=tests/adapters/test_github_branch.py` runs controlled HTTP
fixtures. PostgreSQL service proofs use
`tests/integration/test_github_publication.py` with the disposable T22 database
fixture described in [the ledger contract](external-operation-ledger.md).

Local fixtures prove the dispatch, identity and reconciliation policy. Real GitHub
publication, hosted verification and the later end-to-end workflow remain
unverified. No arbitrary exactly-once guarantee is made for GitHub.

## Provider references

- [GitHub Git blobs](https://docs.github.com/en/rest/git/blobs)
- [GitHub Git trees](https://docs.github.com/en/rest/git/trees)
- [GitHub Git commits](https://docs.github.com/en/rest/git/commits)
- [GitHub Git references](https://docs.github.com/en/rest/git/refs)
