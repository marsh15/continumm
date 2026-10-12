# Repository preflight

T13 inspects repository metadata and selected manifests without downloading an
archive, installing dependencies, executing repository code, allocating a
sandbox, or calling a model. Its output describes metadata eligibility; later
archive and build-profile gates must still verify the actual downloaded files.

The repository numeric ID, full commit SHA, and fixed profile ID/version identify
the check. The actor must currently have write permission and the GitHub App must
have access, in addition to the repository being public and unarchived. The
trusted verifier boundary defaults to denial. Live account authentication and
GitHub App integration are later tasks; fake verifiers are only test fixtures.

Each account receives at most five checks in a sliding hour and one active
preflight reservation. PostgreSQL stores these limits, reservation deadlines,
and a ten-minute technical cache. Cache access requires a current reservation;
the service checks current permission and visibility before using cached
technical results. An expired check cannot release a replacement reservation.

HTTP reads use fixed public service origins, request-scoped GitHub credentials,
disabled redirects and environment proxy inheritance, a finite total deadline,
and aggregate byte/request limits. Returned URLs do not authorize arbitrary
outbound requests. Rate-limit responses stop the check without automatic retry.
Compressed metadata responses are rejected before decoding; clients explicitly
request identity encoding.

The adapter rejects incomplete trees, unsupported file modes, declared Git LFS,
and source-size/file-count violations. Metadata cannot establish the absence of
hard links or embedded LFS pointers in an archive; T14 performs those checks.

## Sources

- [GitHub tree modes and truncation](https://docs.github.com/en/rest/git/trees#get-a-tree).
- [GitHub REST usage and rate limits](https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api).
- [HTTPX timeouts](https://www.python-httpx.org/advanced/timeouts/): inactivity
  timeouts are supplemented by a total check deadline.
- [PyPI release JSON metadata](https://docs.pypi.org/api/json/#get-a-release):
  declared filenames and hashes support portable-wheel selection; actual
  artifacts still require verification during dependency preparation.
- [Packaging requirements](https://packaging.pypa.io/en/stable/requirements.html)
  and [marker evaluation](https://packaging.pypa.io/en/stable/markers.html).
- [npm lockfile package records](https://docs.npmjs.com/cli/v11/configuring-npm/package-lock-json/#packages).

## Supported manifest subset

`python-pytest` version `1` accepts a root `requirements.lock` containing one
`name==version --hash=sha256:<hex>` requirement per line, with optional blank or
comment lines. It requires pytest and at most 24 locked packages. Extras,
markers, recursive requirements, index options, URLs, editable installs, and
unhashed requirements are unsupported. Every allowed hash must match a declared
`py3-none-any` wheel in the exact public PyPI release metadata. Root or `src/`
Python source is required; build hooks and declared native source are rejected.
Active `Requires-Dist` dependencies must also appear in the lock at compatible
versions. Marker and `Requires-Python` checks use CPython 3.13.12, Linux x86_64;
T17 must pin a matching runtime before execution. Unknown kernel-dependent
markers and active extra requirements are unsupported. A different target
requires a profile-version change and a fresh check.

`node-npm` version `1` requires root `package.json` and a version 2 or 3
`package-lock.json`, a configured test script, matching root dependency maps,
public npm tarball URLs and SHA-512 integrity. It accepts at most 24 package
records and rejects workspaces, local/Git/URL/alias dependency specifications,
install lifecycle requirements, bundled/link/shrinkwrap records and declared
native or platform-specific package requirements. Project `.npmrc`, root
`npm-shrinkwrap.json`, `binding.gyp`, and native `.node` binaries are rejected.
These conservative subsets
can reject otherwise valid projects rather than guessing their setup.

The existing locked `packaging==26.3` dependency is declared directly for Python
requirement, marker, and version parsing. The standard library has no equivalent
PEP 508 parser, and maintaining a partial parser would weaken these checks. This
adds no version upgrade; it makes the runtime dependency explicit. It can be
removed if Python dependency metadata validation moves out of this process.

The combined metadata budget is 2 MiB and at most 32 serial HTTP requests, with a
30-second total check deadline. Individual manifests are limited to 256 KiB;
trees are limited to 5,000 files and 50 MiB of declared source content. No package
artifact or source archive is downloaded by preflight.

## Local verification

The focused unit target passes 49 tests, and the dedicated PostgreSQL 17
integration target passes one combined quota/cache/isolation proof. These are
local fixtures; they do not establish live GitHub App permissions.

```sh
make test-target TARGET=tests/unit/test_repository_eligibility.py
CONTINUUM_T13_TEST_DATABASE_URL='postgresql://<local-admin>@127.0.0.1:<port>/continuum_t13_test' \
  make test-target TARGET=tests/integration/test_repository_preflight.py
```

Use a dedicated disposable PostgreSQL 17 **instance**, not just another database
in a shared cluster: the integration fixture creates and removes cluster-wide
test roles. It verifies owner isolation through the runtime role, concurrent
reservation admission, rate limits, expired-cache misses, stale-check fencing,
and migration rollback. Hosted GitHub authentication remains unverified until
configured and tested.
