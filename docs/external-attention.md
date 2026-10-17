# External effects after cancellation

T33 adds a separate external-effects panel to the run detail page. A cancelled
run stays cancelled while publication outcomes and cleanup remain uncertain.
Investigation can update recorded evidence; it cannot resume the run, create a
replacement PR, force a branch, or authorize another dispatch.

## API and trust boundary

`GET /api/v1/runs/{run_id}/external-effects` reads owner-scoped summaries from
the publication ledger and the earlier dispatch, reservation and cleanup records.
It exposes fixed identifiers, state labels, timestamps and validated GitHub PR
references. It does not return request metadata, PR prose, arbitrary observation
JSON, credentials, worker identities or source contents. Unknown and other-owner
runs are indistinguishable. Responses are private and are not cached. The
summary is capped at 100 records per effect class and reports truncation; an
incomplete summary never claims that all effects are resolved. Legacy dispatch
observations have no typed resolution contract, so their presence alone does
not clear uncertainty.

`POST /api/v1/runs/{run_id}/external-effects/{effect_id}/reconcile` requires the
same authenticated owner and Origin/CSRF checks as review decisions. The server
looks up the effect's stored approval and digest; the browser cannot substitute
publication inputs. Only the existing managed PR operation supports automatic
investigation. Other unresolved effects remain visible for investigation by an
operator.

The POST performs **reads only against GitHub** through the T32 reconciler. It
may append local observations and adopt one exact existing PR. Open, closed and
merged PR outcomes are preserved; conflicting identities remain conflicts.
Missing or unavailable remote evidence never proves that an uncertain request
failed and never permits replacement creation. No execution lease is acquired
and no run-state transition occurs.

The configured GitHub authentication boundary supplies current owner/App read
access. Read-only reconciliation uses the frozen manifest without requiring the
candidate archive to remain available. The default application fails closed
when GitHub authentication or authorization is unavailable.

## Interface decisions

The existing run detail and review screens are the visual target: retain their
canvas, typography, compact bordered sections and button roles. Refero's bundled
craft guidance supplies semantic headings, visible keyboard focus and announced
asynchronous results; its copy guidance supplies specific outcome and action
labels. The panel distinguishes terminal workflow state from external outcome,
shows loading/unavailable states honestly, and never offers a force-retry action.

## Verification and limits

Browser fixtures cover unknown publication on a cancelled run and evidence
adoption while the displayed run remains cancelled. PostgreSQL-backed tests
exercise owner isolation, retained uncertainty, safe projection and reconciliation
without another create. These local fixtures do not prove a live GitHub outcome
or hosted end-to-end operation. T34 owns the controlled live demonstration.
