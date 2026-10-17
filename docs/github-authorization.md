# GitHub identity and repository authorization

T29 supplies the server identity and authorization boundary used by the run and
review APIs. It remains disabled until explicitly configured. No real GitHub
account, installed App, or deployed login has been exercised by the local tests.

## Configuration and flow

Set `CONTINUUM_ENABLE_GITHUB_AUTH=1` only after migration 18 and the server-only
settings in `.env.example` are configured. Register exactly
`https://<console-origin>/api/v1/auth/github/callback` as the GitHub App callback.
Use the App's OAuth client ID/secret and its separate numeric App ID/private key;
these are different credentials. Use a canonical HTTPS origin without a path.
Configure the existing encryption keyring for token encryption, and supply a
separate random state key of at least 32 bytes. Configure the private Supabase
checkpoint bucket to enable review source restoration. Never expose these
values through frontend environment variables.

- `GET /api/v1/auth/github/start` creates a ten-minute, browser-bound one-use
  state record and redirects to GitHub with S256 PKCE and the fixed callback.
- `GET /api/v1/auth/github/callback` validates the actual origin, state and
  HttpOnly browser-binding cookie, consumes state before exchanging the code,
  fetches `/user`, and links its immutable numeric GitHub ID. It never identifies
  an account by its changeable login name or a client-supplied account header.
- `GET /api/v1/auth/session` returns only internal/numeric identity, display
  login and session expiry. `POST /api/v1/auth/logout` revokes the session and
  clears cookies only after session-bound CSRF and exact Origin validation.

Sessions are opaque random tokens, stored only as SHA-256 hashes, with a maximum
seven-day lifetime. Login rotates the prior browser session. The session cookie
is `__Host-continumm_session`, Secure, HttpOnly, SameSite=Lax, Path=/, without a
Domain attribute. The separate `csrf_token` cookie is Secure, SameSite=Strict and
readable by the UI, but its matching header must also verify against the stored
session hash. Session and token reads check active account status every time;
suspended/deleting/deleted accounts cannot authenticate. Internal account
revocation also erases encrypted GitHub token material.

The maintained Authlib client performs code exchange; the application bounds
responses to 64 KiB, applies a 20-second overall timeout, disables environment
proxy inheritance, redirects and transport retries, and never logs codes or
tokens. The App flow requests `offline_access` to obtain an expiring token. This
implementation deliberately discards refresh tokens and requires reauthorization
after user-token expiry (at most eight hours). A seven-day session therefore
establishes local ownership, not seven days of GitHub repository permission.
This keeps owner logout/cancellation identity usable after GitHub access expires.
GitHub documents these [App token and PKCE semantics](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-a-user-access-token-for-a-github-app).

## Repository checks and trust boundaries

The owner-bound token store decrypts a token only for that account's numeric
GitHub identity. Every repository check obtains current `/user` identity and
user write permission, confirms the repository is public, checks the App's
installation and the user's repository intersection, and independently verifies
App access with a repository-scoped read-only installation token. Numeric IDs
must agree throughout. Pagination, request count, response bytes and elapsed
time are bounded. Provider URLs are fixed and redirects are rejected. A lost
permission, removed installation, identity mismatch, or unavailable proof stops
the operation; installation alone never grants user write permission.

The authorization client implements both the T13 repository verifier and T27
publication-facts interfaces. The configured browser boundary composes it with
T28 review reads/decisions. The adapter creates only short-lived authentication
tokens and reads repository metadata; it exposes no Git mutation. T31 adds a
narrow capability that repeats these checks and mints a one-repository contents-write
token for the [trusted publication adapter](github-branch-publication.md). Ordinary
verification and review paths continue to request read-only tokens. Token responses
must confirm exactly the requested repository and permissions and a current bounded
expiry. Local write fixtures do not establish live GitHub publication evidence.

Auth tables have FORCE RLS and no direct runtime-role grants. Narrow SECURITY
DEFINER routines revoke PUBLIC execution, pin `search_path=pg_catalog`, qualify
all tables, and perform hashed-token/account checks. Their owner must be the
operator-controlled migration role with the necessary owner/bypass-RLS rights;
the application continues to use only the restricted runtime login.

Authentication endpoints share a database-backed ten-attempt/15-minute client-IP
limit. Only HMAC hashes of normalized transport IP addresses are stored; the app
does not trust arbitrary forwarding headers. Production proxy trust/client-IP
behavior must be qualified on the actual deployment. OAuth state expiry is
rechecked using database time after acquiring its row lock.

## Retention, headers and verification

Numeric GitHub ID and login are identity metadata. Access tokens are encrypted
with owner/token/provider-bound AES-GCM AAD and never returned to the browser.
Expired token ciphertext is erased during subsequent login-state maintenance;
state/rate records and expired/revoked session records are pruned after their
one-day retention window on the same path. An inactive deployment still requires
the later scheduled cleanup/deletion service. No refresh-token copy is retained.

Responses carry nosniff, no-referrer, frame denial and CSP; HTTPS responses add
HSTS. Auth responses are private/no-store. The existing documentation UI uses
per-response script nonces, and review usage meters use native progress elements
so the frontend needs no inline script/style exception.

`make test-target TARGET=tests/security/test_github_auth.py` runs pure boundary
checks and optional PostgreSQL proofs. To include the latter, set
`CONTINUUM_T22_TEST_DATABASE_URL` to a disposable loopback PostgreSQL 17 database
named `continuum_t22_test`; the shared fixture creates/drops runtime schemas and
roles. HTTP mocks in `tests/unit/auth` cover OAuth protocol, response caps,
identity/rights changes, installation access, pagination and read-only scope.
Real account consent, deployment proxy/cookie behavior and live publication
remain release gates.

Authlib 1.9.1 is a pinned BSD-3-Clause runtime dependency for maintained OAuth
handling; HTTPX2 2.13.1 was already locked transitively and is declared directly
for that client integration. Its only new transitive dependency is joserfc.
`uv audit --locked --no-dev` and both npm audits found no known vulnerabilities
on 17 October 2026. The full Python audit retains the previously documented
[test-only pytest advisory and private-directory mitigation](credential-lifecycle.md#dependency-decision-and-audit).
