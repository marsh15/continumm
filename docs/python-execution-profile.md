# Python execution profile

T17 implements profile version 1 for CPython 3.13.12 on Linux x86_64.
It consumes the T13 eligibility receipt and a T14 canonical archive, and uses
the T16 allocation reservation. Repository code runs inside the managed sandbox.
The API host only validates inputs and coordinates bounded operations.

## Trust boundaries

The bootstrap lives under `/opt/continuum`, outside the repository workspace at
`/vercel/sandbox/workspace`. Trusted setup and network probes use isolated Python
startup. Dependencies are installed into a separate target directory; repository
modules and dependency startup files must not execute during the download phase.
Root and `src` layouts become importable only for the test phase.
The fixed test command uses `-P -S` and puts verified dependencies before local
source in `PYTHONPATH`, so a workspace `pytest.py` cannot replace the runner.

Download access is restricted to public PyPI and its file host. The driver changes
the **active session** through `update_network_policy`, waits for completion and
refreshes its policy. Tests require both deny-all readback and a failed outbound
probe. A positive probe before the transition prevents a broken probe from being
accepted as proof. Sandbox configuration updates cannot establish this boundary.

Hash-pinned dependencies must be wheels in the supported portable subset. The
bootstrap checks downloaded archive contents before installation; metadata-only
eligibility does not prove wheel contents safe. Editable installation, source
builds and repository setup scripts are unsupported.
Wheel records are checked through bounded reads, including their hashes and
sizes. Native extensions, startup hooks, links, conflicting paths and unsupported
installation destinations fail closed. Installation uses only the verified local
wheelhouse after egress has been disabled.

Each remote mutation requires a current execution lease and the exact reserved
session. PostgreSQL computes the remaining allocation, run and provider-period
budget. Commands have a server kill deadline of at most 120 seconds and a combined
stdout/stderr cap of 256 KiB. Each profile invocation limits aggregate output to
2 MiB; T30 must account for cumulative run output across attempts and revisions
before the run-wide budget can be claimed.
Source archives retain the canonical 10 MiB compressed, 50 MiB expanded and 5,000
file bounds. Unknown outcomes cannot authorize a blind retry or replacement.
Both success and failure use T16's protected stop operation. A successful test
result is returned only after terminal cleanup is observed; uncertain termination
retains capacity. Cleanup is bound to the validated execution identity and cannot
stop an unrelated allocation owned by the same account.

## Operator setup

The [image recipe](../profiles/python/Dockerfile) will be built and published by
the operator before hosted use.
Use an immutable digest of the resulting profile image, not the base Python image
alone. Configure the existing Vercel credentials and an explicit current provider
period as described in [allocation accounting](sandbox-allocation-accounting.md).
Keep credentials in the control plane; never add them to image layers, process
environment or the source archive.

No profile image has been built or published as part of the local implementation.
The managed Python fixture and Checkpoint F remain pending until the cloud
resources are configured and actual managed-environment evidence is recorded.

## Local verification

The Python profile target passed 16 tests. Bootstrap verification passed 14
synthetic-data tests, and the SDK target passed 11 tests. These cover independent
Git tree binding, protected manifests, ordered policy/probe/install/test phases,
lease loss, cancellation, clipped deadlines, failed cleanup, ZIP expansion and
path attacks, exact extraction and probe behavior. A dedicated PostgreSQL 17
allocation fixture passed with the new per-action fence and exact-session checks.

The complete Python suite passed 300 tests, with 15 optional integration targets
skipped because their separate database environments were not configured.
Formatting, lint, typing across 38 source files and package build passed.
Frontend and Edge sources are unchanged by T17.

Reproduce the profile targets with:

```sh
make test-target TARGET=tests/adapters/test_python_profile.py
make test-target TARGET=tests/unit/test_python_profile_bootstrap.py
make test-target TARGET=tests/adapters/test_sandbox_sdk.py
```

The PostgreSQL target requires its guarded disposable loopback database via
`CONTINUUM_T16_TEST_DATABASE_URL`. Never point the fixture at retained data.

## Sources

- [Vercel Python SDK reference](https://vercel.com/docs/sandbox/python-sdk-reference),
  checked against the locked `vercel==0.11.6` implementation.
- [Python isolated startup options](https://docs.python.org/3.13/using/cmdline.html).
- [pip secure installation](https://pip.pypa.io/en/stable/topics/secure-installs/).
- [Wheel format and installation schemes](https://packaging.python.org/en/latest/specifications/binary-distribution-format/).
