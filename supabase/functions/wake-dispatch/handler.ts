export const MAX_REQUEST_BYTES = 2_048;
export const MAX_RESPONSE_BYTES = 2_048;
export const REQUEST_BODY_TIMEOUT_MS = 5_000;
export const BACKEND_REQUEST_TIMEOUT_MS = 100_000;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const GENERATION_PATTERN = /^[1-9][0-9]{0,18}$/;

export interface DispatchAttempt {
  attemptId: string;
  wakeId: string;
  generation: string;
  reason: "pending_inbox";
}

export type DispatchOutcomeKind =
  | "accepted"
  | "retryable_failure"
  | "unknown"
  | "protocol_failure";

export interface DispatchStore {
  consumeTicket(
    ticketId: string,
    ticketValue: string,
  ): Promise<DispatchAttempt | null>;
  recordResult(
    attempt: DispatchAttempt,
    outcome: DispatchOutcomeKind,
    httpStatus: number | null,
  ): Promise<boolean>;
}

export interface WakeDispatcherConfig {
  backendUrl: string;
  backendToken: string;
  store: DispatchStore;
  fetcher?: typeof fetch;
  bodyTimeoutMs?: number;
  requestTimeoutMs?: number;
}

type TicketBody = { ticket_id: string; ticket: string };

export function createWakeDispatcher(
  config: WakeDispatcherConfig,
): (request: Request) => Promise<Response> {
  const endpoint = backendEndpoint(config.backendUrl);
  if (new TextEncoder().encode(config.backendToken).byteLength < 32) {
    throw new Error("wake_backend_configuration_invalid");
  }
  const fetcher = config.fetcher ?? fetch;
  const bodyTimeoutMs = config.bodyTimeoutMs ?? REQUEST_BODY_TIMEOUT_MS;
  const requestTimeoutMs = config.requestTimeoutMs ??
    BACKEND_REQUEST_TIMEOUT_MS;
  if (
    !Number.isInteger(bodyTimeoutMs) || bodyTimeoutMs < 1 ||
    bodyTimeoutMs > 10_000 ||
    !Number.isInteger(requestTimeoutMs) || requestTimeoutMs < 1 ||
    requestTimeoutMs >= 140_000
  ) {
    throw new Error("wake_backend_configuration_invalid");
  }

  return async (request: Request): Promise<Response> => {
    if (request.method !== "POST") return jsonError(405, "method_not_allowed");
    if (!hasJsonContentType(request.headers.get("content-type"))) {
      return jsonError(415, "unsupported_content_type");
    }

    const bodyBytes = await readBoundedBody(
      request,
      MAX_REQUEST_BYTES,
      bodyTimeoutMs,
    );
    if (bodyBytes.kind === "too_large") {
      return jsonError(413, "request_too_large");
    }
    if (bodyBytes.kind === "timeout") return jsonError(408, "request_timeout");
    if (bodyBytes.kind !== "body") return jsonError(400, "invalid_request");

    const body = parseTicketBody(bodyBytes.value);
    if (!body) return jsonError(400, "invalid_request");

    let attempt: DispatchAttempt | null;
    try {
      attempt = await config.store.consumeTicket(body.ticket_id, body.ticket);
    } catch {
      return jsonError(503, "dispatcher_unavailable");
    }
    if (!attempt) return new Response(null, { status: 204 });

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
    let response: Response;
    try {
      response = await fetcher(endpoint, {
        method: "POST",
        headers: {
          "authorization": `Bearer ${config.backendToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({
          attempt_id: attempt.attemptId,
          generation: attempt.generation,
          wake_id: attempt.wakeId,
          reason: attempt.reason,
        }),
        redirect: "error",
        signal: controller.signal,
      });
    } catch {
      clearTimeout(timeout);
      return recordAndRespond(config.store, attempt, "unknown", null, 503);
    }
    if (response.status !== 202) {
      await response.body?.cancel().catch(() => undefined);
      clearTimeout(timeout);
      const outcome: DispatchOutcomeKind = response.status >= 500
        ? "retryable_failure"
        : "protocol_failure";
      return recordAndRespond(
        config.store,
        attempt,
        outcome,
        response.status,
        outcome === "protocol_failure" ? 502 : 503,
      );
    }

    const responseBytes = await readBoundedResponse(
      response,
      MAX_RESPONSE_BYTES,
      controller.signal,
    );
    if (responseBytes.kind === "timeout" || controller.signal.aborted) {
      clearTimeout(timeout);
      return recordAndRespond(config.store, attempt, "unknown", 202, 503);
    }
    const accepted = responseBytes.kind === "body" &&
      isAcceptedReceipt(response, responseBytes.value, attempt);
    clearTimeout(timeout);
    if (!accepted) {
      const outcome: DispatchOutcomeKind = responseBytes.kind !== "body" ||
          controller.signal.aborted
        ? "unknown"
        : "protocol_failure";
      return recordAndRespond(
        config.store,
        attempt,
        outcome,
        202,
        outcome === "protocol_failure" ? 502 : 503,
      );
    }
    return recordAndRespond(config.store, attempt, "accepted", 202, 204);
  };
}

async function recordAndRespond(
  store: DispatchStore,
  attempt: DispatchAttempt,
  outcome: DispatchOutcomeKind,
  httpStatus: number | null,
  responseStatus: number,
): Promise<Response> {
  try {
    const recorded = await store.recordResult(attempt, outcome, httpStatus);
    if (!recorded) return jsonError(409, "dispatch_stale");
    if (responseStatus === 204) return new Response(null, { status: 204 });
    return jsonError(
      responseStatus,
      responseStatus === 502 ? "backend_protocol_error" : "backend_unavailable",
    );
  } catch {
    return jsonError(503, "dispatcher_unavailable");
  }
}

function backendEndpoint(backendUrl: string): URL {
  try {
    const base = new URL(backendUrl);
    if (
      base.protocol !== "https:" || base.username || base.password ||
      base.pathname !== "/" ||
      base.search || base.hash
    ) {
      throw new Error("invalid_origin");
    }
    return new URL("/internal/v1/wake", base.origin);
  } catch {
    throw new Error("wake_backend_configuration_invalid");
  }
}

function hasJsonContentType(value: string | null): boolean {
  if (!value) return false;
  const [mediaType, ...parameters] = value.split(";");
  if (mediaType?.trim().toLowerCase() !== "application/json") return false;
  return parameters.every((parameter) => {
    const [name, rawValue] = parameter.trim().split("=", 2);
    return name?.toLowerCase() === "charset" &&
      rawValue?.replaceAll('"', "").toLowerCase() === "utf-8";
  });
}

async function readBoundedBody(
  request: Request,
  limit: number,
  timeoutMs: number,
): Promise<
  | { kind: "body"; value: string }
  | { kind: "too_large" }
  | { kind: "timeout" }
  | { kind: "invalid" }
> {
  const length = request.headers.get("content-length");
  if (length !== null) {
    if (!/^(0|[1-9][0-9]*)$/.test(length)) return { kind: "invalid" };
    if (Number(length) > limit) return { kind: "too_large" };
  }
  if (!request.body) return { kind: "invalid" };
  const controller = new AbortController();
  const abortFromRequest = () => controller.abort();
  if (request.signal.aborted) controller.abort();
  else {request.signal.addEventListener("abort", abortFromRequest, {
      once: true,
    });}
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await readBoundedStream(request.body, limit, controller.signal);
  } finally {
    clearTimeout(timeout);
    request.signal.removeEventListener("abort", abortFromRequest);
  }
}

async function readBoundedResponse(
  response: Response,
  limit: number,
  signal: AbortSignal,
): Promise<
  | { kind: "body"; value: string }
  | { kind: "invalid" }
  | { kind: "too_large" }
  | { kind: "timeout" }
> {
  if (
    !hasJsonContentType(response.headers.get("content-type")) || !response.body
  ) {
    await response.body?.cancel().catch(() => undefined);
    return { kind: "invalid" };
  }
  const length = response.headers.get("content-length");
  if (
    length !== null &&
    (!/^(0|[1-9][0-9]*)$/.test(length) || Number(length) > limit)
  ) {
    await response.body.cancel().catch(() => undefined);
    return { kind: "too_large" };
  }
  return await readBoundedStream(response.body, limit, signal);
}

async function readBoundedStream(
  stream: ReadableStream<Uint8Array>,
  limit: number,
  signal: AbortSignal,
): Promise<
  | { kind: "body"; value: string }
  | { kind: "invalid" }
  | { kind: "too_large" }
  | { kind: "timeout" }
> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      let result: ReadableStreamReadResult<Uint8Array>;
      try {
        result = await readWithSignal(reader, signal);
      } catch {
        if (signal.aborted) return { kind: "timeout" };
        return { kind: "invalid" };
      }
      if (result.done) break;
      total += result.value.byteLength;
      if (total > limit) {
        await reader.cancel().catch(() => undefined);
        return { kind: "too_large" };
      }
      chunks.push(result.value);
    }
  } catch {
    await reader.cancel().catch(() => undefined);
    return { kind: "invalid" };
  } finally {
    if (!signal.aborted) reader.releaseLock();
    else {
      void reader.closed.then(() => reader.releaseLock()).catch(() => {
        try {
          reader.releaseLock();
        } catch {
          // The aborted read may have already released its lock.
        }
      });
    }
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    return {
      kind: "body",
      value: new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    };
  } catch {
    return { kind: "invalid" };
  }
}

function readWithSignal<T>(
  reader: ReadableStreamDefaultReader<T>,
  signal: AbortSignal,
): Promise<ReadableStreamReadResult<T>> {
  if (signal.aborted) {
    void reader.cancel().catch(() => undefined);
    return Promise.reject(new Error("wake_body_timeout"));
  }
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      cleanup();
      void reader.cancel().catch(() => undefined);
      reject(new Error("wake_body_timeout"));
    };
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    signal.addEventListener("abort", onAbort, { once: true });
    reader.read().then(
      (result) => {
        cleanup();
        resolve(result);
      },
      (error: unknown) => {
        cleanup();
        reject(error);
      },
    );
  });
}

function parseTicketBody(body: string): TicketBody | null {
  let value: unknown;
  try {
    value = JSON.parse(body);
  } catch {
    return null;
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  if (keys.length !== 2 || keys[0] !== "ticket" || keys[1] !== "ticket_id") {
    return null;
  }
  if (
    typeof record.ticket_id !== "string" ||
    !UUID_PATTERN.test(record.ticket_id) ||
    typeof record.ticket !== "string" || !UUID_PATTERN.test(record.ticket)
  ) {
    return null;
  }
  return { ticket_id: record.ticket_id, ticket: record.ticket };
}

function isAcceptedReceipt(
  response: Response,
  body: string,
  attempt: DispatchAttempt,
): boolean {
  if (response.redirected || response.headers.has("location")) return false;
  let value: unknown;
  try {
    value = JSON.parse(body);
  } catch {
    return false;
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  const expectedKeys = [
    "accepted",
    "drain_status",
    "last_successful_drain_at",
    "receipt_id",
    "status",
  ];
  if (Object.keys(record).sort().join(",") !== expectedKeys.join(",")) {
    return false;
  }
  const validState = ((record.status === "worker_not_started" ||
    record.status === "scheduled") &&
    record.drain_status === "not_started") ||
    (record.status === "running" && record.drain_status === "running") ||
    (record.status === "partial" && record.drain_status === "partial") ||
    (record.status === "drained" && record.drain_status === "completed" &&
      typeof record.last_successful_drain_at === "string");
  if (
    record.accepted !== true || !validState ||
    typeof record.receipt_id !== "string" ||
    !UUID_PATTERN.test(record.receipt_id) ||
    record.receipt_id !== attempt.wakeId
  ) {
    return false;
  }
  return record.last_successful_drain_at === null ||
    (typeof record.last_successful_drain_at === "string" &&
      Number.isFinite(Date.parse(record.last_successful_drain_at)));
}

function jsonError(status: number, code: string): Response {
  return Response.json({ code }, {
    status,
    headers: { "cache-control": "no-store" },
  });
}
