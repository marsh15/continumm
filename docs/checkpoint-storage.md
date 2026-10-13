# Committed checkpoint storage

T15 follows the archive protocol in [canonical-archives.md](canonical-archives.md).
The service validates the source archive locally, reserves its bounded upload
bytes in PostgreSQL, and records an immutable private object identity before
dispatch. A successful upload response alone does not advance the run.

After uploading, the service downloads the stored bytes and verifies their exact
archive and canonical tree digests. Only then can the activity transition attach
the verified object, accept the result, emit its event, and advance the state in
one fenced transaction. Exact committed receipt adoption is read-only and does
not authorize a second upload.

The global 600 MiB storage allowance counts in-flight, unknown, verified,
committed, and orphaned uploads. Unknown outcomes retain their reservation; a
404 after a timed-out upload does not prove that a delayed write will never
arrive. Replayed upload intents do not blindly dispatch again. Garbage collection
and confirmed deletion are separate later work. A database commit whose outcome
cannot be confirmed can leave a verified, unattached record charged; recovery
must check the accepted receipt before classifying it as an orphan. Collection
must lock and recheck references before deleting any such object.

Recovery reads only an owned, committed artifact after current repository access
and public visibility checks. The default access verifier denies reads until the
GitHub integration is configured. Missing or corrupt committed content produces
an attention state under the current lease, without substituting another tree.
Upload readback and reconciliation also recheck repository access before object
downloads. This worker service requires a current running lease; public artifact
downloads and terminal retention enforcement belong to later tasks. The stored
absolute content ceiling is admission plus fourteen days, while T29 must enforce
the earlier seven-day deadline after terminal completion for those later paths.

The database stores bounded digests, provenance, version and size metadata. The
canonical manifest is reconstructed from verified archive bytes rather than
duplicating file content or large manifests in database JSON.

Hosted storage verification remains pending. Local tests use a controlled HTTP
storage fixture and a **dedicated disposable PostgreSQL 17 instance**, not just a
database in a shared cluster: the fixture creates and removes cluster-wide
runtime roles as well as its test schemas.

```sh
make test-target TARGET=tests/unit/test_checkpoint_storage.py
CONTINUUM_T15_TEST_DATABASE_URL='postgresql://<local-admin>@127.0.0.1:<port>/continuum_t15_test' \
  make test-target TARGET=tests/integration/test_checkpoints.py
```

Construct the worker storage adapter with an explicit private bucket and a
10 MiB response limit, and supply the same `ArtifactPersistence` to the
`TransitionService` through `artifact_persistence`. The production access
verifier remains denied by default; positive local tests inject trusted fixtures.

## Local proof

The dedicated PostgreSQL 17 target passes one combined integration proof. It
checks an actual reference/result/event commit, a two-checkpoint parent chain,
exact reconstruction, lost-acknowledgment adoption, and transaction rollback
after event insertion. It also checks unknown POST/404 retention, stale uploader
rejection, orphan grace without replay extension, known-absence refunds,
concurrent capacity admission, owner RLS, and corrupt/missing restores producing
`needs_attention`. Downgrading to revision 07 still permits legacy artifact-less
transition and receipt adoption.

The checkpoint, transition retry and connectivity targets pass 63 focused tests.
The full Python suite passes 243 tests with 14 integration environment skips;
the T15 PostgreSQL target was run separately rather than counted as a hosted
test. Formatting, lint, typing and package build pass.
