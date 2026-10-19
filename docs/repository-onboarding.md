# Account-scoped repository onboarding (T35)

The authenticated console at `/settings/repositories` registers public GitHub
repositories by numeric ID, checks an explicit commit against a supported test
profile, and saves provider/profile/credential defaults for future runs. It does
not start a model or sandbox job and does not enable public run admission.
T34's live demonstration is deferred at the owner's request.

Apply migration `20261019_22` and configure the existing GitHub App/OAuth boundary.
Registration, default updates and eligibility checks freshly verify the numeric
user's write rights and the App's repository access. Listing and mutation queries
use the authenticated account and forced database row isolation. A saved key
must belong to that account, be active, have saved scope and match the provider.
One saved key can be selected for several repositories. Secret values are never
returned by these endpoints.

| Endpoint | Behavior |
|---|---|
| `GET /api/v1/repositories` | List this account's registrations and eligibility summaries |
| `POST /api/v1/repositories` | Register the numeric GitHub repository and explicit defaults |
| `PATCH /api/v1/repositories/{id}` | Change future-run defaults; profile changes invalidate eligibility |
| `POST /api/v1/repositories/{id}/check` | Check the supplied exact commit SHA with T13 preflight |
| `GET /api/v1/credentials` | List active saved credential metadata for selection |

Mutations require the existing session CSRF token and valid Origin. Request
bodies are strict and bounded to 8 KiB before JSON parsing. OpenAPI includes
request and response shapes. Eligibility results are fenced to the registration's
defaults version, so an old check cannot overwrite changed configuration. An
eligibility result is for the displayed commit/profile and is not a promise
about another commit or future access.

## Original comment setup handoff

`SetupContinuationService` is a trusted internal interface, not a public comment
endpoint. A verified ingress adapter can preserve one bounded original issue
comment, numeric author/repository/issue identities and a content digest. The
comment must follow account and repository registration, be no older than
72 hours and have at most five minutes of future clock skew. Unregistered
comments cannot become pending runs through this interface.

After defaults are configured, one claim returns that same original submission;
a repeated claim cannot create another intent. Missing keys are permitted at
this handoff because T36 owns waiting-for-credentials admission. Capture/claim
create no run. T36 must combine handoff and run admission transactionally, and
T37 must fetch and compare the current comment before accepting its command.

Expired text can be scrubbed in bounded batches while retaining minimal replay
identity and a digest. Repository listing opportunistically invokes the owner's
sweep; regular background cleanup remains part of the later retention task.
Expiry denies access even if cleanup is delayed. The database prevents changing
the captured body or provenance and permits only its expiry scrub and claim time.

## Verification and rollback

Verification passed 610 Python tests (153 optional database skips), three
PostgreSQL account-isolation/security cases, 27 component tests and 11 browser
tests, plus formatting, lint, strict typing and backend/frontend builds. Browser
checks use mocked APIs. Desktop and mobile visual checks, including a 320-pixel
viewport, preserve the existing interface without horizontal overflow.

The PostgreSQL cases cover owner isolation, shared saved-key defaults, lost
repository access, a racing eligibility check, strict UUID response serialization,
CSRF/body limits, original-comment claim identity and expired-content scrubbing.
No hosted authorization, provider generation or sandbox execution was performed.
Downgrade removes the two new tables before returning to revision 21; preserve
needed registration metadata before a production rollback.
