# Node execution profile

T18 targets the `node-npm` profile version 1 with Node 24.21.0 LTS and npm
11.19.0 on Linux x86_64. It uses the same committed-source authorization,
eligibility binding, allocation fencing and protective cleanup as the
[Python profile](python-execution-profile.md).

The supported input is one package with a pinned public-registry lockfile and a
configured test script. Lockfile versions 2 and 3 are supported. Workspaces,
private or arbitrary package sources, custom
installation hooks, native compilation and browser downloads are unsupported.
Tests needing external services cannot pass the deny-all execution boundary.
Matching `.nvmrc`/`.node-version` pins are accepted; mismatched pins and an
incompatible `packageManager` declaration are rejected before remote setup.
These structural checks reject declared or detected unsupported inputs; they do
not prove the semantics of arbitrary repository code or encoded data.

Dependency preparation uses `npm ci --ignore-scripts` with explicit public
registry configuration and no credentials. The test command also retains
`--ignore-scripts`: npm still executes the requested test script while suppressing
implicit pretest and posttest hooks. The test process receives `CI=true`.
Source and dependency content remains untrusted even with installation scripts
disabled.
The installed tree is scanned without executing packages. The scan checks actual
package identities, hooks, native file headers, links and bounded file/entry
counts. See the [image recipe contract](../profiles/node/README.md) for exact
limits. Acquisition is bounded by time, concurrency and retries; the profile
does not claim a pre-download byte ceiling for npm tarballs or its cache.

The driver must change the exact active session policy, await completion,
refresh and verify deny-all readback, and prove an outbound probe fails before
running repository tests. Failed or uncertain setup requires protected T16 stop;
uncertain termination retains the allocation's capacity charge.

Commands retain the 120-second deadline and 256 KiB output cap. Aggregate output
is capped at 2 MiB per invocation; T30's durable run ledger remains responsible
for cumulative limits across attempts and revisions.

The image recipe and local fixtures do not establish hosted acceptance.
Image publication, actual managed fixture execution and Checkpoint F remain
pending until operator resources are configured.
The Docker daemon was unavailable during local verification, so the composite
image recipe has not been built or started locally.

## Local verification

The Node profile target passed 32 tests and the bootstrap target passed 26.
Coverage includes independent Git tree binding, manifests and runtime pins,
phase ordering, active policy/probe checks, lease loss, cancellation, cleanup,
exact extraction, installed links, scan limits and native/browser rejection.
All test providers and registry probes are synthetic; no hosted sandbox or
repository dependency command was executed locally.

The full Python suite passed 359 tests with 15 optional integration targets
skipped for missing separate database environments. Formatting, lint, typing
across 41 source files and package build passed. The reused PostgreSQL action
fence was verified in T17; T18 does not change that store or its schema.
Frontend and Edge sources are unchanged.

```sh
make test-target TARGET=tests/adapters/test_node_profile.py
make test-target TARGET=tests/unit/test_node_profile_bootstrap.py
```

## Sources

- [Official Node release index](https://nodejs.org/dist/index.json).
- [npm ci contract](https://docs.npmjs.com/cli/v11/commands/npm-ci/).
- [npm ignore-scripts configuration](https://docs.npmjs.com/cli/v11/using-npm/config/#ignore-scripts).
- [Vercel Python SDK reference](https://vercel.com/docs/sandbox/python-sdk-reference),
  checked against the locked `vercel==0.11.6` adapter.
