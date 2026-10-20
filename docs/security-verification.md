# Local adversarial verification

The security suite covers account isolation, encrypted credential ownership,
GitHub authorization and command freshness, archive traversal/link rejection,
fixed tool capabilities, immutable review bindings, CSRF and browser headers,
and operator-role privilege denial. Browser tests render hostile source as text
and exercise expired/stale decisions and request identity reuse.

The combined lifecycle/fault pass identified and resolved these defects before
public access:

- Suspension now blocks new queue claims and fenced writes from existing leases,
  in addition to session/token revocation and admission denial.
- Migration 30 prevents retention-only column grants from changing active frozen
  manifests, accepted results, events, tool arguments, and source/task pins.
  After deletion or expiry, only exact irreversible scrub values are accepted.
- Deletion scrubs original setup snapshots and private external-request content,
  retaining only minimal remote identity and digest tombstones. Review feedback
  is cleared by its exact approval binding rather than by a shared issue number.

The privileged corruption test deliberately disables one guard within its test
transaction, injects corrupt bytes, then restores the guard. That verifies the
consumer still rejects lower-layer corruption; ordinary runtime writes remain
denied. Operator credentials cannot disable those guards or read user content.

Run `make test-target TARGET=tests/security` and `make test-e2e`. PostgreSQL cases
require the explicit disposable URLs named by each fixture; skipped optional
cases are not database proof. The final progress log records actual counts.
These checks do not claim an exhaustive audit or substitute for deployed launch
qualification. Public admission remains disabled.
