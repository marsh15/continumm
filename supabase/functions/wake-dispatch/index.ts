import { createWakeDispatcher } from "./handler.ts";
import {
  createLazyPostgresWakePool,
  createPostgresWakeStore,
} from "./postgres_wake.ts";

const databaseUrl = Deno.env.get("CONTINUUM_WAKE_DATABASE_URL") ?? "";
const backendUrl = Deno.env.get("CONTINUUM_WAKE_BACKEND_URL") ?? "";
const backendToken = Deno.env.get("CONTINUUM_WAKE_BACKEND_TOKEN") ?? "";
if (!databaseUrl || !backendUrl || !backendToken) {
  throw new Error("wake_configuration_missing");
}

let handler: (request: Request) => Promise<Response>;
let pool: ReturnType<typeof createLazyPostgresWakePool>;
try {
  pool = createLazyPostgresWakePool(databaseUrl);
  handler = createWakeDispatcher({
    backendUrl,
    backendToken,
    store: createPostgresWakeStore(pool),
  });
} catch {
  throw new Error("wake_configuration_invalid");
}

addEventListener("beforeunload", () => {
  void pool.end();
});

export default { fetch: handler };
