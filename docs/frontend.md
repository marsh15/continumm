# Frontend console

The root route remains a foundation status page. The public service has not launched, and it does not imply that sign-in, repository connection, run creation, review, or publication are available. Its live checks read `/health/live` and `/health/ready`; readiness currently reports the database check as unverified, so the page displays that API error and request ID.

The direct `/runs/:runId` route reads a real run snapshot and its committed event history. There is no run-list API yet, so the route is not linked from the status page or header. Authentication is an injected backend boundary; the default app resolver returns 401. The route therefore shows the API's access error until a trusted session boundary is configured.

## Reference lock and decision ledger

Refero MCP research was unavailable for this task. The direct-build reference is the product direction in [plan §10.5](../tasks/plan.md#105-screens-and-interaction), supported by the bundled Refero craft guidance on work-tool typography, restrained color, focus states, semantic controls, and async status announcements.

| Decision | Source and role | Application |
|---|---|---|
| Light neutral canvas, near-black text, blue primary links/actions | Plan §10.5 is the dominant visual direction; blue stays an action accent | Neutral page and white status surface; blue appears only on links and keyboard focus |
| Compact 14–16px interface type; monospace for codes/IDs | Plan §10.5 and bundled work-tool typography guidance | Dense status labels remain readable; API codes and request IDs use monospace |
| Thin separators and one small radius | Plan §10.5 | Content groups rely on spacing and separators, not a grid of decorative cards |
| Status uses text with semantic color | Plan §10.5 accessibility requirement | A process result says “Available”; errors include code, message, retry guidance, and request ID |
| Keyboard focus, labelled structure, responsive stacking, reduced motion | Plan §10.5 and bundled craft guidance | Links use native anchors; focus is visible; health updates use announced status/error regions |
| No bitmap imagery | The foundation screen has no image-led reference or product evidence | Keep the surface code-native and use no decorative illustration |

The status page keeps its compact live-check row. Run detail shows a causal timeline only for the run ID supplied in the URL and only from API responses. It must not gain sample runs, timeline entries, aggregate metrics, or success wording without corresponding API evidence.

## Frontend and API contract

The app uses React, TypeScript, Vite, React Router, and TanStack Query. `frontend/openapi.json` is exported from the FastAPI app by `scripts/export_openapi.py`; `frontend/src/api/generated.ts` is generated from that committed schema with `openapi-typescript`. Application code imports endpoint and error types from the generated file. Do not maintain a second handwritten API model.

API errors use `code`, `message`, `retryable`, and `request_id`; the server also returns the request ID in `X-Request-ID`. The typed client validates health, run snapshot, event page, and event payload responses against the generated types before rendering them. An unexpected HTML page or unknown status cannot appear as available, and the timeline displays only the backend's allowlisted event metadata.

Run event sequence numbers and cursors stay as decimal strings. The client uses `BigInt` to order and deduplicate them without losing PostgreSQL `int8` precision. Event history loads in pages of 50, with a maximum of 10 saved pages and a bounded 500-event live buffer. The “Load more events” control advances the history cursor; the run snapshot remains the source of current status.

The same-origin `EventSource` uses the session cookie and listens for `run.event`, `history.reset`, and `stream.error`. A committed event advances the timeline and triggers a run snapshot refetch. Duplicate replay is ignored. A sequence gap or reset closes the current source, refetches the snapshot and first event page, then starts a replacement stream at the refreshed watermark. Native reconnects preserve `Last-Event-ID`; new sources start from the latest snapshot watermark. Connection retries use bounded exponential delays, stop after three failed attempts, and require a user retry after the budget is exhausted or the server closes the stream. A CLOSED stream or authentication event also clears and rechecks cached run data so a revoked session cannot leave stale private details on screen. Unmounting or changing the run closes the stream.

FastAPI serves the Vite production output from `frontend/dist` in a local checkout and `/app/static` in the container. Only files resolved within that static directory are served. Extensionless browser routes receive the SPA shell; missing `/api`, `/auth`, and `/health` routes remain JSON errors.

## Local commands

Run the API and Vite in separate terminals:

```sh
make bootstrap
make dev
```

```sh
make dev-web
```

Vite proxies health and API requests to `127.0.0.1:8000`. To refresh the checked-in schema and types after a backend contract change, run `make openapi`. `make frontend-openapi-check` detects schema or generated type drift; `make test-ui` also runs the frontend type check and component tests. `make build` compiles the frontend and creates the production Docker image with the built assets.
