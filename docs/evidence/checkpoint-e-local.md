# Checkpoint E: local artifact correctness

The T15 integration proof runs against a dedicated PostgreSQL 17 instance with
the real runtime role and migrations through `20261012_09`. Storage and GitHub
metadata requests use controlled HTTP fixtures; no hosted provider was called.

- Two committed checkpoints restore the exact file bytes and executable modes
  from their verified archives. Archive and canonical tree digests match, and
  the second checkpoint records the first as its parent.
- Upload success is insufficient: forcing a database failure after event
  insertion rolls back the artifact reference, accepted result, event and state
  advancement. The uploaded object remains charged and unreferenced. A late
  uploader also fails to attach its object after losing execution ownership.
- Corrupt or missing committed objects move the current run to `needs_attention`
  with a `corrupt_artifact` failure event. Recovery does not choose another
  checkpoint or trust local worker files. Pre-commit corrupt readback likewise
  cannot create an accepted result.

Verification: `tests/integration/test_checkpoints.py` passed its combined proof;
the focused checkpoint/transition/connectivity targets passed 63 tests. The
full Python regression suite passed 243 tests, with the optional integration
targets run separately where configured. See the
[reproduction instructions](../checkpoint-storage.md#local-proof).

This closes the local artifact-correctness milestone. Actual Supabase Storage
restoration, terminal retention cleanup and hosted failure experiments remain
pending their configured integrations and later tasks.
