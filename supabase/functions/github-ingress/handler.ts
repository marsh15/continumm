export const MAX_BODY_BYTES = 1_048_576;
export const DEFAULT_DEADLINE_MS = 8_000;
const MAX_POSTGRES_BIGINT = 9_223_372_036_854_775_807n;
const DELIVERY_UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const EVENT_NAME = /^[A-Za-z][A-Za-z0-9_]{0,63}$/;
const HEX_SIGNATURE = /^sha256=([0-9a-f]{64})$/;
const textEncoder = new TextEncoder();

export interface InboxDelivery {
  appId: string;
  deliveryId: string;
  eventType: string;
  action: string | null;
  payloadSha256: Uint8Array;
  rawBody: Uint8Array;
}

export type PersistOutcome = "inserted" | "duplicate" | "conflict";

export interface WebhookHandlerOptions {
  appId: string;
  secret: string;
  persist: (
    delivery: InboxDelivery,
    signal: AbortSignal,
  ) => Promise<PersistOutcome>;
  deadlineMs?: number;
}

export function createWebhookHandler(
  options: WebhookHandlerOptions,
): (request: Request) => Promise<Response> {
  validateAppId(options.appId);
  if (!options.secret) throw new Error("webhook_secret_missing");
  const deadlineMs = options.deadlineMs ?? DEFAULT_DEADLINE_MS;
  if (!Number.isInteger(deadlineMs) || deadlineMs < 1) {
    throw new Error("deadline_invalid");
  }

  return async (request: Request): Promise<Response> => {
    const controller = new AbortController();
    const onRequestAbort = () => controller.abort(new DeadlineError());
    if (request.signal.aborted) controller.abort(new DeadlineError());
    else {request.signal.addEventListener("abort", onRequestAbort, {
        once: true,
      });}
    const timer = setTimeout(
      () => controller.abort(new DeadlineError()),
      deadlineMs,
    );

    try {
      const response = await handleRequest(request, options, controller.signal);
      if (controller.signal.aborted) return unavailable();
      return response;
    } catch (error) {
      if (error instanceof PayloadTooLargeError) return responseWithStatus(413);
      if (error instanceof InvalidRequestError) return responseWithStatus(400);
      return unavailable();
    } finally {
      clearTimeout(timer);
      request.signal.removeEventListener("abort", onRequestAbort);
    }
  };
}

export async function verifyGitHubSignature(
  rawBody: Uint8Array,
  signatureHeader: string | null,
  secret: string,
): Promise<boolean> {
  if (signatureHeader === null) return false;
  const match = HEX_SIGNATURE.exec(signatureHeader);
  if (!match?.[1]) return false;

  const key = await crypto.subtle.importKey(
    "raw",
    textEncoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"],
  );
  return await crypto.subtle.verify(
    "HMAC",
    key,
    toArrayBuffer(hexToBytes(match[1])),
    toArrayBuffer(rawBody),
  );
}

function validateAppId(appId: string): void {
  if (
    !/^[1-9][0-9]{0,18}$/.test(appId) || BigInt(appId) > MAX_POSTGRES_BIGINT
  ) {
    throw new Error("github_app_id_invalid");
  }
}

async function handleRequest(
  request: Request,
  options: WebhookHandlerOptions,
  signal: AbortSignal,
): Promise<Response> {
  if (request.method !== "POST") {
    return new Response(null, {
      status: 405,
      headers: { allow: "POST", "cache-control": "no-store" },
    });
  }
  if (!isJsonContentType(request.headers.get("content-type"))) {
    return responseWithStatus(415);
  }

  const contentLength = request.headers.get("content-length");
  if (contentLength !== null) {
    if (!/^\d+$/.test(contentLength)) throw new InvalidRequestError();
    if (BigInt(contentLength) > BigInt(MAX_BODY_BYTES)) {
      throw new PayloadTooLargeError();
    }
  }

  const deliveryIdHeader = request.headers.get("x-github-delivery");
  const eventType = request.headers.get("x-github-event");
  if (!deliveryIdHeader || !DELIVERY_UUID.test(deliveryIdHeader)) {
    throw new InvalidRequestError();
  }
  if (!eventType || !EVENT_NAME.test(eventType)) {
    throw new InvalidRequestError();
  }

  const rawBody = await waitForSignal(
    readBoundedBody(request.body, signal),
    signal,
  );
  throwIfAborted(signal);
  const verified = await waitForSignal(
    verifyGitHubSignature(
      rawBody,
      request.headers.get("x-hub-signature-256"),
      options.secret,
    ),
    signal,
  );
  if (!verified) return responseWithStatus(403);
  throwIfAborted(signal);

  let parsed: unknown;
  try {
    parsed = JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(rawBody),
    );
  } catch {
    throw new InvalidRequestError();
  }
  const payload = asRecord(parsed);
  if (!payload) throw new InvalidRequestError();
  const action = payload.action;
  if (
    action !== undefined && (typeof action !== "string" || action.length > 128)
  ) {
    throw new InvalidRequestError();
  }

  const payloadSha256 = new Uint8Array(
    await waitForSignal(
      crypto.subtle.digest("SHA-256", toArrayBuffer(rawBody)),
      signal,
    ),
  );
  throwIfAborted(signal);
  const delivery: InboxDelivery = {
    appId: options.appId,
    deliveryId: deliveryIdHeader.toLowerCase(),
    eventType,
    action: typeof action === "string" ? action : null,
    payloadSha256,
    rawBody,
  };
  const outcome = await waitForSignal(
    options.persist(delivery, signal),
    signal,
  );
  throwIfAborted(signal);

  if (outcome === "conflict") return responseWithStatus(409);
  if (outcome !== "inserted" && outcome !== "duplicate") return unavailable();
  return responseWithStatus(202);
}

async function readBoundedBody(
  body: ReadableStream<Uint8Array> | null,
  signal: AbortSignal,
): Promise<Uint8Array> {
  if (body === null) return new Uint8Array();

  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  try {
    while (true) {
      throwIfAborted(signal);
      const chunk = await waitForSignal(reader.read(), signal);
      throwIfAborted(signal);
      if (chunk.done) break;
      totalBytes += chunk.value.byteLength;
      if (totalBytes > MAX_BODY_BYTES) throw new PayloadTooLargeError();
      chunks.push(chunk.value);
    }
  } catch (error) {
    void reader.cancel(error).catch(() => undefined);
    throw error;
  } finally {
    reader.releaseLock();
  }

  const bodyBytes = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    bodyBytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bodyBytes;
}

function isJsonContentType(contentType: string | null): boolean {
  if (contentType === null) return false;
  return contentType.split(";", 1)[0]?.trim().toLowerCase() ===
    "application/json";
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    const start = index * 2;
    bytes[index] = Number.parseInt(hex.slice(start, start + 2), 16);
  }
  return bytes;
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const buffer = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(buffer).set(bytes);
  return buffer;
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) throw new DeadlineError();
}

function waitForSignal<T>(
  promise: Promise<T>,
  signal: AbortSignal,
): Promise<T> {
  if (signal.aborted) return Promise.reject(new DeadlineError());
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      cleanup();
      reject(new DeadlineError());
    };
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        cleanup();
        resolve(value);
      },
      (error: unknown) => {
        cleanup();
        reject(error);
      },
    );
  });
}

function responseWithStatus(status: number): Response {
  return new Response(null, {
    status,
    headers: { "cache-control": "no-store" },
  });
}

function unavailable(): Response {
  return responseWithStatus(503);
}

class PayloadTooLargeError extends Error {}
class InvalidRequestError extends Error {}
class DeadlineError extends Error {}
