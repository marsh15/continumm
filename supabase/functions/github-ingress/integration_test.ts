import { assertEquals } from "jsr:@std/assert@1.0.14";
import { Pool } from "@db/postgres";
import {
  createWebhookHandler,
  type InboxDelivery,
  type PersistOutcome,
} from "./handler.ts";
import { createPostgresInboxWriter } from "./postgres_inbox.ts";

const APP_ID = "9223372036854775000";
const SECRET = "local-integration-secret-do-not-reuse";
const EVENT_TYPE = "issue_comment";
const textEncoder = new TextEncoder();

Deno.test("Postgres inbox deduplicates exact deliveries and adopts a committed event after lost ack", async () => {
  const databaseUrl = Deno.env.get("CONTINUUM_TEST_INGRESS_DATABASE_URL");
  if (!databaseUrl) {
    throw new Error(
      "CONTINUUM_TEST_INGRESS_DATABASE_URL_required_for_integration_test",
    );
  }
  const pool = new Pool(databaseUrl, 1, true);
  const persist = createPostgresInboxWriter(pool);
  const handler = createWebhookHandler({
    appId: APP_ID,
    secret: SECRET,
    persist,
  });

  try {
    const bodyA = textEncoder.encode(
      '{"action":"created","issue":{"number":7}}',
    );
    const firstId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    assertEquals((await handler(await request(firstId, bodyA))).status, 202);
    assertEquals((await handler(await request(firstId, bodyA))).status, 202);

    const bodyB = textEncoder.encode(
      '{"action":"edited","issue":{"number":7}}',
    );
    assertEquals((await handler(await request(firstId, bodyB))).status, 409);
    await assertStored(pool, firstId, bodyA);

    const commitAckId = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    const commitAckBody = textEncoder.encode(
      '{"action":"created","issue":{"number":8}}',
    );
    let loseNextDatabaseAck = true;
    let insertAttempts = 0;
    const dropCommitAck = createWebhookHandler({
      appId: APP_ID,
      secret: SECRET,
      persist: async (
        delivery: InboxDelivery,
        signal: AbortSignal,
      ): Promise<PersistOutcome> => {
        insertAttempts += 1;
        const outcome = await persist(delivery, signal);
        if (loseNextDatabaseAck) {
          loseNextDatabaseAck = false;
          throw new Error("simulated_commit_ack_lost_after_database_commit");
        }
        return outcome;
      },
    });
    assertEquals(
      (await dropCommitAck(await request(commitAckId, commitAckBody))).status,
      503,
    );
    assertEquals(
      (await handler(await request(commitAckId, commitAckBody))).status,
      202,
    );
    assertEquals(insertAttempts, 1);
    await assertStored(pool, commitAckId, commitAckBody);

    const httpAckId = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
    const httpAckBody = textEncoder.encode(
      '{"action":"created","issue":{"number":9}}',
    );
    await handler(await request(httpAckId, httpAckBody)); // Simulate the caller losing the 202 response.
    assertEquals(
      (await handler(await request(httpAckId, httpAckBody))).status,
      202,
    );
    await assertStored(pool, httpAckId, httpAckBody);

    const delayedAcquireId = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
    const delayedAcquireBody = textEncoder.encode(
      '{"action":"created","issue":{"number":10}}',
    );
    const heldClient = await pool.connect();
    const timedOutHandler = createWebhookHandler({
      appId: APP_ID,
      secret: SECRET,
      deadlineMs: 100,
      persist,
    });
    try {
      assertEquals(
        (await timedOutHandler(
          await request(delayedAcquireId, delayedAcquireBody),
        ))
          .status,
        503,
      );
    } finally {
      heldClient.release();
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
    await assertMissing(pool, delayedAcquireId);
  } finally {
    await pool.end();
  }
});

Deno.test("Postgres connection failure returns 503 without acknowledging", async () => {
  const databaseUrl = Deno.env.get("CONTINUUM_TEST_INGRESS_DATABASE_URL");
  if (!databaseUrl) {
    throw new Error(
      "CONTINUUM_TEST_INGRESS_DATABASE_URL_required_for_integration_test",
    );
  }
  const unreachableUrl = new URL(databaseUrl);
  unreachableUrl.hostname = "127.0.0.1";
  unreachableUrl.port = "1";
  const failedPool = new Pool(unreachableUrl.toString(), 1, true);
  const failingHandler = createWebhookHandler({
    appId: APP_ID,
    secret: SECRET,
    deadlineMs: 2_000,
    persist: createPostgresInboxWriter(failedPool),
  });

  const response = await failingHandler(
    await request(
      "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
      textEncoder.encode("{}"),
    ),
  );
  assertEquals(response.status, 503);
});

async function assertStored(
  pool: Pool,
  deliveryId: string,
  rawBody: Uint8Array,
): Promise<void> {
  const client = await pool.connect();
  try {
    const rows = await client.queryObject<
      { payload_sha256: string; delivery_id: string }
    >`
      SELECT encode(payload_sha256, 'hex') AS payload_sha256, delivery_id::text AS delivery_id
      FROM webhook_private.github_delivery_inbox
      WHERE github_app_id = ${APP_ID}::bigint
        AND delivery_id = ${deliveryId}::uuid
    `;
    assertEquals(rows.rows.length, 1);
    assertEquals(rows.rows[0]?.delivery_id, deliveryId);
    assertEquals(rows.rows[0]?.payload_sha256, await digestHex(rawBody));
  } finally {
    client.release();
  }
}

async function assertMissing(pool: Pool, deliveryId: string): Promise<void> {
  const client = await pool.connect();
  try {
    const rows = await client.queryObject<{ delivery_id: string }>`
      SELECT delivery_id::text AS delivery_id
      FROM webhook_private.github_delivery_inbox
      WHERE github_app_id = ${APP_ID}::bigint
        AND delivery_id = ${deliveryId}::uuid
    `;
    assertEquals(rows.rows.length, 0);
  } finally {
    client.release();
  }
}

async function request(deliveryId: string, body: Uint8Array): Promise<Request> {
  return new Request("http://localhost/github-webhook", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-github-delivery": deliveryId,
      "x-github-event": EVENT_TYPE,
      "x-hub-signature-256": await sign(body),
    },
    body: toArrayBuffer(body),
  });
}

async function sign(body: Uint8Array): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    textEncoder.encode(SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = new Uint8Array(
    await crypto.subtle.sign("HMAC", key, toArrayBuffer(body)),
  );
  return `sha256=${
    Array.from(signature, (byte) => byte.toString(16).padStart(2, "0")).join("")
  }`;
}

async function digestHex(body: Uint8Array): Promise<string> {
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", toArrayBuffer(body)),
  );
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const buffer = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(buffer).set(bytes);
  return buffer;
}
