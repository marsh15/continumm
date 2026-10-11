# Frontend foundation

The current browser experience is a foundation status page. The public service has not launched, and the page does not imply that sign-in, repository connection, runs, review, or publication are available. Its two live checks read the FastAPI `/health/live` and `/health/ready` endpoints. The readiness endpoint currently reports the database check as unverified, so the page displays that API error and its request ID.

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

The distinctive detail is a compact live-check row that reports real process and readiness responses. It must not gain sample runs, timeline entries, aggregate metrics, or success wording without corresponding API evidence.

## Frontend and API contract

The app uses React, TypeScript, Vite, React Router, and TanStack Query. `frontend/openapi.json` is exported from the FastAPI app by `scripts/export_openapi.py`; `frontend/src/api/generated.ts` is generated from that committed schema with `openapi-typescript`. Application code imports endpoint and error types from the generated file. Do not maintain a second handwritten API model.

API errors use `code`, `message`, `retryable`, and `request_id`; the server also returns the request ID in `X-Request-ID`. The typed API client rejects malformed successful health responses as well as non-2xx responses, so an unexpected HTML page or unknown status cannot appear as “Available.”

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
