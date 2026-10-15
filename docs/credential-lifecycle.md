# BYOK credential lifecycle

T23 provides an internal owner-scoped credential service for OpenAI and Anthropic.
It stores encrypted saved keys only with explicit opt-in, or encrypted per-run
keys with a run deadline. The public credential UI/API, live model adapters and
GitHub refresh-token integration belong to later tasks. No live keys or provider
requests were used for this implementation.

## Trust boundaries

| Boundary | Enforced rule |
| --- | --- |
| Account to credential store | Explicit principal, owner predicates and forced RLS |
| Database to crypto | AES-256-GCM authenticates owner, provider, scope, run ID, credential ID and key version |
| Store to provider adapter | A live lease and an owned binding authorize one stable request ID once |
| Credential revocation | Clear ciphertext and nonce; fence affected running work and wait for replacement credentials |
| Run terminal transition | Delete active per-run material in the same database transaction |
| Model/tool context | Only metadata is returned for listing; provider grants are internal and contain a redacted `SecretStr` |

The cipher uses a fresh 96-bit CSPRNG nonce for every encryption and the full
128-bit authentication tag. Nonces are unique per stored key version. The
database never stores the master key. Tampering, wrong associated data and
unknown key versions fail closed.
[Cryptography 50.0.2 AEAD contract](https://cryptography.io/en/50.0.2/hazmat/primitives/aead/).

Credential mutations serialize per account with transaction-scoped advisory
locks. Run and credential row locks serialize revocation against dispatch
authorization. The runtime retains SELECT-only access to the user table.
[PostgreSQL 17 advisory locks](https://www.postgresql.org/docs/17/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS).

## Configuration and rotation

`CONTINUUM_ENCRYPTION_KEY` must contain the canonical URL-safe base64 encoding
of 32 random bytes. Store it in deployment secrets. `CONTINUUM_ENCRYPTION_KEY_VERSION`
defaults to `v1`; versions contain 1–32 ASCII letters, digits, underscores or
hyphens. `CONTINUUM_ENCRYPTION_PREVIOUS_KEYS` optionally contains a JSON object
mapping old versions to their secret base64 values. Both secret-valued settings
are redacted; version identifiers remain ordinary metadata.

To rotate, configure a new version with new key material, retain needed old keys,
and call the owner-scoped rotation operation for active credentials. Rotation
authenticates the old record, encrypts with a fresh nonce under the new version
and commits the replacement atomically. Retire an old master key only when no
active record needs it. Reusing a version for different material makes existing
records unreadable; unavailable versions never trigger a fallback or new key.
Revocation and cleanup work without a configured cipher.

## Dispatch and retention

A binding pins the selected provider for a run; replacing a key cannot silently
switch providers. The internal `authorize_dispatch` operation checks current
ownership, account state, lease, run deadline, binding, scope, expiry and
revocation. A final database-clock check occurs after lock waits. Only then does
it return a one-use grant to trusted adapter code; no provider I/O happens while
the transaction is open. Reusing the request ID cannot obtain another grant.
Provider integrations must compose this gate with their model intent before
dispatch; T24 owns that wiring.

Metadata listing returns at most 50 active credentials and never ciphertext,
nonces, key fragments or secret hashes. Active credential admission is bounded
to the same 50 records per owner. Revocation locks only affected active runs,
rather than an unbounded history of terminal runs.

Revocation blocks new authorizations and invalidates affected work leases. It
cannot recall a request already sent or an authorization committed before the
revocation transaction. Replacing a credential resumes eligible waiting work
under a newer generation. Protective owner revocation does not require renewed
GitHub permission.

Per-run encrypted material is cleared on success, failure, cancellation or
expiry, and a bounded idempotent repair sweep handles terminal/expired remnants.
Non-secret identity and dispatch tombstones remain for replay protection. A
deleted per-run key cannot bind to another run. Clearing active database columns
does not promise physical erasure from historical backups; future export and
quarantine-restore work must exclude all credential material. Python also cannot
guarantee zeroization of all copies in process memory.

## Verification

```sh
make test-target TARGET=tests/security/test_credentials.py
CONTINUUM_T23_TEST_DATABASE_URL='postgresql://<local-admin>@127.0.0.1:<port>/continuum_t23_test' \
  make test-target TARGET=tests/security/test_credentials.py
```

The PostgreSQL fixture requires a dedicated disposable PostgreSQL 17 instance
because it manages cluster-wide roles. It uses only generated/synthetic key
material. It checks owner RLS, opt-in, dispatch replay rejection, rotation,
revocation, key replacement, provider pinning, all four terminal cleanup states,
legacy-ciphertext repair, scope/expiry denial and lease expiry during a row-lock
wait. Errors, metadata and events contain no plaintext credentials.

The focused target passes ten tests with the dedicated PostgreSQL proof enabled
(nine tests and one database-case skip without it). The full Python suite passes
448 tests with 34 optional database cases skipped. Formatting, lint, typing and
package build pass. The test database instance was stopped and removed after
verification.

Read [the cipher](../src/continuum_api/credentials.py),
[the lifecycle service](../src/continuum_api/runtime/provider_credentials.py),
[migration 12](../migrations/versions/20261015_12_provider_credentials.py) and
[the security proof](../tests/security/test_credentials.py) for implementation details.

## Dependency decision and audit

`cryptography==50.0.2` was already a locked runtime transitive dependency; T23
declares that exact version directly and adds no new packages or version changes.
It supplies the cryptographic primitive rather than a custom encryption scheme.
Its [release notes](https://cryptography.io/en/50.0.2/changelog/) and
[security policy](https://github.com/pyca/cryptography/security/policy) were reviewed.
The project uses its existing vetted-and-locked dependency posture.

`uv audit --locked --no-dev` found zero runtime advisories. The full audit reports
one dev-only pytest temporary-directory issue under two identifiers,
`GHSA-6w46-j5rx-g56g` / `PYSEC-2026-1845` (`CVE-2025-71176`), fixed in pytest 9.0.3.
The current declared pytest range is below 9. Maintained Make test targets now
run through an atomically created private base directory instead of pytest's
predictable default. Direct invocations outside that wrapper need an equally
private base directory. A separate reviewed pytest major upgrade is deferred to
the repository maintainer, with review due 22 October 2026; no audit suppression
or forced dependency upgrade is applied. This is local implementation evidence,
not a public-release security sign-off.
