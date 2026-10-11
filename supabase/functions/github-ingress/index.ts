import { createWebhookHandler } from "./handler.ts";
import {
  createLazyPostgresPool,
  createPostgresInboxWriter,
} from "./postgres_inbox.ts";

const appId = Deno.env.get("CONTINUUM_GITHUB_APP_ID") ?? "";
const webhookSecret = Deno.env.get("CONTINUUM_GITHUB_WEBHOOK_SECRET") ?? "";
const databaseUrl = Deno.env.get("CONTINUUM_INGRESS_DATABASE_URL") ?? "";

if (new TextEncoder().encode(webhookSecret).byteLength < 32) {
  throw new Error("github_webhook_secret_invalid");
}
if (!databaseUrl) throw new Error("ingress_database_url_missing");

const handler = (() => {
  try {
    const pool = createLazyPostgresPool(databaseUrl);
    return createWebhookHandler({
      appId,
      secret: webhookSecret,
      persist: createPostgresInboxWriter(pool),
    });
  } catch {
    throw new Error("ingress_configuration_invalid");
  }
})();

export default { fetch: handler };
