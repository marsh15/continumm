# Node/npm profile image

The T18 profile targets Node.js 24.21.0 LTS and its bundled npm 11.19.0 on
Linux x86_64. The Node release index lists the exact patch and bundled npm
version, and the Node release table identifies v24 as LTS:
[Node release index](https://nodejs.org/dist/index.json),
[Node release schedule](https://nodejs.org/en/about/previous-releases).

The Dockerfile uses two immutable official image inputs. The official Node.js
24.21.0 Bookworm slim linux/amd64 OCI manifest is pinned to
`sha256:51b1100cc2a83d370c6a60952e3f2989c8a43159d0e38586e090f3b3326efefd`;
the SHA256 of the manifest body returned by the Docker Registry matched both
that descriptor and its `Docker-Content-Digest` header. The official Python
3.13.12 Bookworm slim linux/amd64 manifest is pinned to
`sha256:3121f8b0804aa3698ab750d9a39ea4a42657a385c9b133722b915e55c51551a6`,
verified for T17. The final image keeps the Python base and copies only the
Node executable, npm CLI tree, and Node's `libstdc++`, `libgcc_s`, and
`libatomic` runtime libraries from the pinned Node stage. The Dockerfile checks
the linked libraries and exact Python, Node, and npm versions when built.

This is an image recipe, not a published Vercel image. At T18, no image was
built or published because the local Docker daemon was unavailable. On 21 October
2026, the image built locally and the runtime identity/fixed hidden-probe checks
passed in a non-root read-only container with no network. See
[the measured local image evidence](../../docs/evidence/t48-local-image-probes.json). The runtime must remain disabled until an operator
configures a Vercel image reference pinned to a verified digest and the managed
profile passes its real-session fixture.

The root-owned helper and T14 archive verifier live under `/opt/continuum`.
Source is extracted only to `/vercel/sandbox/workspace`. npm reads only the
validated package manifests and uses `/opt/continuum/npm-cache`; the project
`.npmrc` is rejected, user/global config uses distinct root-owned, read-only empty files under
`/opt/continuum` (npm rejects using `/dev/null` for both), and helper
subprocesses receive an explicit environment without proxy, token, `NODE_PATH`,
or `NODE_OPTIONS` variables.

Before dependency acquisition, the trusted helper requires a package lock v2
or v3, at most 24 distinct package name/version pairs, public
`registry.npmjs.org` tarball URLs, SHA-512 integrity values, no workspaces,
no links, no platform-specific or browser packages, no native build files,
no lifecycle hooks, and a compatible Node/npm version declaration. It runs
`npm ci` with `--ignore-scripts --engine-strict --no-audit --no-fund`, the
explicit public registry, at most four concurrent fetches, zero retries,
15-second fetch timeouts, and a 120-second helper deadline. The package
manifests are each limited to 256 KiB and the lock is limited to 24 distinct
package/version pairs. Known Chromium and browser-automation packages are
rejected, as are common Chromium and SwiftShader archive names. This bounds the
acquisition by work and time; it does not claim a pre-download byte ceiling
for npm tarballs or its cache. npm verifies each fetched tarball against the
lockfile integrity field.

After install, a read-only scan checks the actual installed tree before any
repository test starts. It rejects native/build files and install hooks, all
links except npm-generated `.bin` links whose resolved targets stay inside
`node_modules`, and unsafe entries. The scan caps individual files at 50 MiB,
file content at 512 MiB total, files at 50,000, directory entries at 100,000,
directories at 20,000, and nesting depth at 64.

Network access is allowed only to `registry.npmjs.org` during `npm ci`. After
that phase, the exact active session is switched to deny-all, refreshed and
checked, and the trusted helper proves the registry probe fails before tests
run. The only test command is the package's explicit test script with
`CI=true` and `--ignore-scripts`; npm documents that this preserves the
requested script while suppressing `pretest` and `posttest` hooks. Tests that
need external services cannot reach those services under deny-all and therefore
do not pass this profile.

The profile uses the fixed Node/npm setup documented by the official
[Node 24 Bookworm slim image recipe](https://github.com/nodejs/docker-node/blob/main/24/bookworm-slim/Dockerfile)
and [Node Docker image documentation](https://github.com/nodejs/docker-node),
[`npm ci`](https://docs.npmjs.com/cli/v11/commands/npm-ci/), and
[npm's `ignore-scripts` setting](https://docs.npmjs.com/cli/v11/using-npm/config/#ignore-scripts).
