import { assert, assertEquals } from "jsr:@std/assert@1.0.14";
import {
  createWebhookHandler,
  type InboxDelivery,
  MAX_BODY_BYTES,
  type PersistOutcome,
  verifyGitHubSignature,
} from "./handler.ts";

const textEncoder = new TextEncoder();
const secret = "It's a Secret to Everybody";

Deno.test("verifies GitHub's published HMAC-SHA256 test vector", async () => {
  const valid = await verifyGitHubSignature(
    textEncoder.encode("Hello, World!"),
    "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
    secret,
  );

  assertEquals(valid, true);
});

Deno.test("HMAC verification rejects malformed signatures and changed raw bytes", async () => {
  const rawBody = textEncoder.encode('{"action":"created"}');
  const signature = await sign(rawBody, secret);

  assertEquals(await verifyGitHubSignature(rawBody, signature, secret), true);
  assertEquals(
    await verifyGitHubSignature(rawBody, "sha256=short", secret),
    false,
  );
  assertEquals(
    await verifyGitHubSignature(
      textEncoder.encode('{"action":"edited"}'),
      signature,
      secret,
    ),
    false,
  );
  assertEquals(
    await verifyGitHubSignature(rawBody, signature, "wrong secret"),
    false,
  );
  assertEquals(await verifyGitHubSignature(rawBody, null, secret), false);
});

Deno.test("commits an authenticated event and preserves a large App ID exactly", async () => {
  const rawBody = textEncoder.encode('{"action":"created","number":7}');
  const persisted: InboxDelivery[] = [];
  const handler = createWebhookHandler({
    appId: "9007199254740993",
    secret,
    persist: async (delivery) => {
      persisted.push(delivery);
      return "inserted";
    },
  });

  const response = await handler(await request(rawBody, secret));

  assertEquals(response.status, 202);
  assertEquals(persisted.length, 1);
  assertEquals(persisted[0]?.appId, "9007199254740993");
  assertEquals(
    persisted[0]?.deliveryId,
    "123e4567-e89b-12d3-a456-426614174000",
  );
  assertEquals(persisted[0]?.eventType, "issue_comment");
  assertEquals(persisted[0]?.action, "created");
  assertEquals(
    new TextDecoder().decode(persisted[0]?.rawBody),
    new TextDecoder().decode(rawBody),
  );
  assertEquals(persisted[0]?.payloadSha256.byteLength, 32);
});

Deno.test("checks the HMAC before attempting to parse JSON", async () => {
  const malformed = textEncoder.encode("{malformed JSON");
  let persistCalled = false;
  const handler = createWebhookHandler({
    appId: "12345",
    secret,
    persist: async () => {
      persistCalled = true;
      return "inserted";
    },
  });

  const response = await handler(await request(malformed, "wrong secret"));

  assertEquals(response.status, 403);
  assertEquals(persistCalled, false);
});

Deno.test("rejects a body above the byte cap without Content-Length", async () => {
  const rawBody = new Uint8Array(MAX_BODY_BYTES + 1).fill(0x20);
  let persistCalled = false;
  const handler = createWebhookHandler({
    appId: "12345",
    secret,
    persist: async () => {
      persistCalled = true;
      return "inserted";
    },
  });

  const response = await handler(await request(rawBody, secret));

  assertEquals(response.status, 413);
  assertEquals(persistCalled, false);
});

Deno.test("does not trust a forged Content-Length when enforcing the stream cap", async () => {
  const rawBody = new Uint8Array(MAX_BODY_BYTES + 1).fill(0x20);
  const req = await request(rawBody, secret, { "content-length": "1" });
  let persistCalled = false;
  const handler = createWebhookHandler({
    appId: "12345",
    secret,
    persist: async () => {
      persistCalled = true;
      return "inserted";
    },
  });

  const response = await handler(req);

  assertEquals(response.status, 413);
  assertEquals(persistCalled, false);
});

Deno.test("accepts a valid payload exactly at the byte cap", async () => {
  const prefix = textEncoder.encode("{}");
  const rawBody = new Uint8Array(MAX_BODY_BYTES).fill(0x20);
  rawBody.set(prefix);
  let persistCalled = false;
  const handler = createWebhookHandler({
    appId: "12345",
    secret,
    persist: async () => {
      persistCalled = true;
      return "inserted";
    },
  });

  const response = await handler(await request(rawBody, secret));

  assertEquals(response.status, 202);
  assertEquals(persistCalled, true);
});

Deno.test("maps same-digest delivery adoption and different-digest conflict to distinct results", async () => {
  const rawBody = textEncoder.encode('{"action":"created"}');
  const outcomes: PersistOutcome[] = ["duplicate", "conflict"];
  const handler = createWebhookHandler({
    appId: "12345",
    secret,
    persist: async () => outcomes.shift() ?? "inserted",
  });

  assertEquals((await handler(await request(rawBody, secret))).status, 202);
  assertEquals((await handler(await request(rawBody, secret))).status, 409);
});

Deno.test("turns database failure and a deadline into service unavailable", async () => {
  const rawBody = textEncoder.encode("{}");
  const failed = createWebhookHandler({
    appId: "12345",
    secret,
    persist: async () => {
      throw new Error("private database detail");
    },
  });
  assertEquals((await failed(await request(rawBody, secret))).status, 503);

  const timedOut = createWebhookHandler({
    appId: "12345",
    secret,
    deadlineMs: 5,
    persist: (_delivery, signal) =>
      new Promise<PersistOutcome>((_resolve, reject) => {
        signal.addEventListener("abort", () => reject(new Error("aborted")), {
          once: true,
        });
      }),
  });
  assertEquals((await timedOut(await request(rawBody, secret))).status, 503);
});

Deno.test("deadline cancels a stalled body stream before persistence", async () => {
  let canceled = false;
  const body = new ReadableStream<Uint8Array>({
    cancel() {
      canceled = true;
    },
  });
  const handler = createWebhookHandler({
    appId: "12345",
    secret,
    deadlineMs: 5,
    persist: async () => "inserted",
  });
  const req = new Request("http://localhost/github-webhook", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-github-delivery": "123e4567-e89b-12d3-a456-426614174000",
      "x-github-event": "issue_comment",
      "x-hub-signature-256":
        "sha256=0000000000000000000000000000000000000000000000000000000000000000",
    },
    body,
  });

  assertEquals((await handler(req)).status, 503);
  assertEquals(canceled, true);
});

Deno.test("deadline returns failure while a delayed persistence result is still pending", async () => {
  let completedAfterAbort = false;
  const handler = createWebhookHandler({
    appId: "12345",
    secret,
    deadlineMs: 5,
    persist: (_delivery, signal) =>
      new Promise<PersistOutcome>((resolve) => {
        setTimeout(() => {
          completedAfterAbort = signal.aborted;
          resolve("inserted");
        }, 25);
      }),
  });

  assertEquals(
    (await handler(await request(textEncoder.encode("{}"), secret))).status,
    503,
  );
  await new Promise((resolve) => setTimeout(resolve, 30));
  assertEquals(completedAfterAbort, true);
});

Deno.test("rejects request shape and invalid JSON before persistence", async () => {
  let persistCalled = false;
  const handler = createWebhookHandler({
    appId: "12345",
    secret,
    persist: async () => {
      persistCalled = true;
      return "inserted";
    },
  });

  const methodResponse = await handler(
    new Request("http://localhost", { method: "GET" }),
  );
  assertEquals(methodResponse.status, 405);
  assertEquals(methodResponse.headers.get("allow"), "POST");
  assertEquals(
    (await handler(
      await request(textEncoder.encode("{}"), secret, {}, "not-a-uuid"),
    )).status,
    400,
  );
  assertEquals(
    (await handler(
      await request(
        textEncoder.encode("{}"),
        secret,
        {},
        undefined,
        "text/plain",
      ),
    ))
      .status,
    415,
  );
  assertEquals(
    (await handler(await request(textEncoder.encode("{broken"), secret)))
      .status,
    400,
  );

  const signedRequest = await request(textEncoder.encode("{}"), secret);
  const missingDeliveryHeaders = new Headers(signedRequest.headers);
  missingDeliveryHeaders.delete("x-github-delivery");
  const missingDelivery = new Request(signedRequest.url, {
    method: signedRequest.method,
    headers: missingDeliveryHeaders,
    body: await signedRequest.clone().arrayBuffer(),
  });
  assertEquals((await handler(missingDelivery)).status, 400);

  const missingSignatureHeaders = new Headers(signedRequest.headers);
  missingSignatureHeaders.delete("x-hub-signature-256");
  const missingSignature = new Request(signedRequest.url, {
    method: signedRequest.method,
    headers: missingSignatureHeaders,
    body: await signedRequest.arrayBuffer(),
  });
  assertEquals((await handler(missingSignature)).status, 403);
  assertEquals(persistCalled, false);
});

async function request(
  body: Uint8Array,
  signingSecret: string,
  extraHeaders: Record<string, string> = {},
  deliveryId = "123e4567-e89b-12d3-a456-426614174000",
  contentType = "application/json",
): Promise<Request> {
  return new Request("http://localhost/github-webhook", {
    method: "POST",
    headers: {
      "content-type": contentType,
      "x-github-delivery": deliveryId,
      "x-github-event": "issue_comment",
      "x-hub-signature-256": await sign(body, signingSecret),
      ...extraHeaders,
    },
    body: toArrayBuffer(body),
  });
}

async function sign(body: Uint8Array, signingSecret: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    textEncoder.encode(signingSecret),
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

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const buffer = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(buffer).set(bytes);
  return buffer;
}
