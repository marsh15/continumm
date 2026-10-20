import { createRecoveryHandler } from "./handler.ts";
import { createGitHubRecoveryClient } from "./github_api.ts";
import {
  createLazyRecoveryPool,
  createPostgresRecoveryStore,
} from "./postgres_recovery.ts";

const databaseUrl = Deno.env.get("CONTINUUM_RECOVERY_DATABASE_URL") ?? "";
const appId = Deno.env.get("CONTINUUM_GITHUB_APP_ID") ?? "";
const privateKeyPem = Deno.env.get("CONTINUUM_GITHUB_APP_PRIVATE_KEY") ?? "";
if (!databaseUrl || !appId || !privateKeyPem) {
  throw new Error("github_recovery_configuration_missing");
}

let pool: ReturnType<typeof createLazyRecoveryPool>;
let handler: (request: Request) => Promise<Response>;
try {
  pool = createLazyRecoveryPool(databaseUrl);
  handler = createRecoveryHandler({
    store: createPostgresRecoveryStore(pool),
    github: createGitHubRecoveryClient({ appId, privateKeyPem }),
  });
} catch {
  throw new Error("github_recovery_configuration_invalid");
}

addEventListener("beforeunload", () => void pool.end());
export default { fetch: handler };
