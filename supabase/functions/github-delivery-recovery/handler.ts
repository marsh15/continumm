export const MAX_SCAN_MS = 100_000;
export const MAX_PAGES = 20;
export const MAX_REDELIVERIES = 25;
export const PAGE_SIZE = 100;
export const HISTORY_WINDOW_MS = 3 * 24 * 60 * 60 * 1000;
export const WATERMARK_OVERLAP_MS = 15 * 60 * 1000;
export const TICKET_BODY_TIMEOUT_MS = 5_000;

export interface Delivery {
  id: string;
  guid: string;
  delivered_at: string;
  status_code: number;
}

export interface DeliveryPage {
  deliveries: Delivery[];
  nextCursor: string | null;
  rateLimitedUntil: number | null;
}

export interface RecoveryState {
  watermark: string | null;
  scanCursor: string | null;
  pendingWatermark: string | null;
  lastScanAt: string | null;
  historyGapSince: string | null;
}

export type ClaimOutcome = "claimed" | "already_requested" | "deferred";

export interface RecoveryStore {
  consumeTicket(ticketId: string, token: string): Promise<boolean>;
  beginScan(): Promise<RecoveryState | null>;
  saveScanProgress(
    cursor: string | null,
    pendingWatermark: string | null,
  ): Promise<void>;
  claimDelivery(delivery: Delivery): Promise<ClaimOutcome>;
  recordDeliveryOutcome(
    guid: string,
    outcome: "accepted" | "unknown" | "rate_limited" | "rejected",
    status: number | null,
    retryAt: string | null,
  ): Promise<void>;
  completeScan(input: {
    watermark: string | null;
    scannedAt: string;
    historyGapSince: string | null;
    successful: boolean;
  }): Promise<void>;
}

export interface GitHubRecoveryClient {
  listFailed(cursor: string | null): Promise<DeliveryPage>;
  redeliver(
    deliveryId: string,
  ): Promise<{ status: number; retryAt: number | null }>;
}

export interface ScanResult {
  pages: number;
  failedDeliveriesSeen: number;
  redeliveryRequests: number;
  stoppedBy:
    | "complete"
    | "page_limit"
    | "request_limit"
    | "rate_limit"
    | "time_limit";
  historyGapSince: string | null;
}

export async function scanFailedDeliveries(
  store: RecoveryStore,
  github: GitHubRecoveryClient,
  options: { now?: () => number; maxScanMs?: number } = {},
): Promise<ScanResult> {
  const now = options.now ?? Date.now;
  const started = now();
  const deadline = started + (options.maxScanMs ?? MAX_SCAN_MS);
  const state = await store.beginScan();
  if (state === null) {
    return {
      pages: 0,
      failedDeliveriesSeen: 0,
      redeliveryRequests: 0,
      stoppedBy: "complete",
      historyGapSince: null,
    };
  }
  let watermark = state.pendingWatermark ?? state.watermark;
  let oldestSeen: number | null = null;
  let pages = 0;
  let failedDeliveriesSeen = 0;
  let redeliveryRequests = 0;
  let cursor: string | null = state.scanCursor;
  let stoppedBy: ScanResult["stoppedBy"] = "complete";
  const overlapCutoff = state.watermark
    ? Date.parse(state.watermark) - WATERMARK_OVERLAP_MS
    : Number.NEGATIVE_INFINITY;
  const retentionCutoff = now() - HISTORY_WINDOW_MS;
  const scanCutoff = Math.max(overlapCutoff, retentionCutoff);
  let exhaustedPages = false;

  while (pages < MAX_PAGES) {
    if (now() >= deadline) {
      stoppedBy = "time_limit";
      break;
    }
    const page = await github.listFailed(cursor);
    if (page.rateLimitedUntil !== null) {
      stoppedBy = "rate_limit";
      break;
    }
    pages += 1;
    let reachedOverlap = false;
    for (const delivery of page.deliveries) {
      const deliveredAt = Date.parse(delivery.delivered_at);
      if (!Number.isFinite(deliveredAt)) continue;
      oldestSeen = oldestSeen === null
        ? deliveredAt
        : Math.min(oldestSeen, deliveredAt);
      if (watermark === null || deliveredAt > Date.parse(watermark)) {
        watermark = new Date(deliveredAt).toISOString();
      }
      if (deliveredAt < scanCutoff) {
        reachedOverlap = true;
        continue;
      }
      if (delivery.status_code >= 200 && delivery.status_code < 300) continue;
      failedDeliveriesSeen += 1;
      if (redeliveryRequests >= MAX_REDELIVERIES) {
        stoppedBy = "request_limit";
        break;
      }
      if (now() >= deadline) {
        stoppedBy = "time_limit";
        break;
      }
      const claim = await store.claimDelivery(delivery);
      if (claim === "already_requested" || claim === "deferred") continue;
      redeliveryRequests += 1;
      try {
        const result = await github.redeliver(delivery.id);
        if (result.status === 202) {
          await store.recordDeliveryOutcome(
            delivery.guid,
            "accepted",
            202,
            null,
          );
        } else if (
          (result.status === 403 || result.status === 429) &&
          result.retryAt !== null
        ) {
          await store.recordDeliveryOutcome(
            delivery.guid,
            "rate_limited",
            result.status,
            result.retryAt === null
              ? null
              : new Date(result.retryAt).toISOString(),
          );
          stoppedBy = "rate_limit";
          break;
        } else {
          await store.recordDeliveryOutcome(
            delivery.guid,
            "rejected",
            result.status,
            null,
          );
        }
      } catch {
        // The POST may have reached GitHub. Keep the claim as an unknown outcome;
        // repeating a non-idempotent request could create another delivery.
        await store.recordDeliveryOutcome(delivery.guid, "unknown", null, null);
      }
    }
    if (stoppedBy !== "complete") break;
    if (reachedOverlap || page.nextCursor === null) {
      exhaustedPages = true;
      await store.saveScanProgress(null, watermark);
      break;
    }
    cursor = page.nextCursor;
    await store.saveScanProgress(cursor, watermark);
  }
  if (!exhaustedPages && stoppedBy === "complete") stoppedBy = "page_limit";

  const previousSuccess = state.lastScanAt
    ? Date.parse(state.lastScanAt)
    : null;
  const missedInspectionWindow = previousSuccess !== null &&
    now() - previousSuccess > HISTORY_WINDOW_MS;
  const pageCapHidesOlderHistory = pages >= MAX_PAGES && oldestSeen !== null &&
    oldestSeen > retentionCutoff;
  let historyGapSince = state.historyGapSince;
  if (missedInspectionWindow || pageCapHidesOlderHistory) {
    historyGapSince ??= new Date(previousSuccess ?? retentionCutoff)
      .toISOString();
  }
  await store.completeScan({
    watermark,
    scannedAt: new Date(now()).toISOString(),
    historyGapSince,
    successful: exhaustedPages,
  });
  return {
    pages,
    failedDeliveriesSeen,
    redeliveryRequests,
    stoppedBy,
    historyGapSince,
  };
}

export function createRecoveryHandler(config: {
  store: RecoveryStore;
  github: GitHubRecoveryClient;
  now?: () => number;
}): (request: Request) => Promise<Response> {
  return async (request) => {
    if (request.method !== "POST") return new Response(null, { status: 405 });
    if (!hasJsonContentType(request.headers.get("content-type"))) {
      return json(415, { error: "unsupported_content_type" });
    }
    const bounded = await readTicketBody(request);
    if (bounded.kind === "too_large") {
      return json(413, { error: "request_too_large" });
    }
    if (bounded.kind === "timeout") {
      return json(408, { error: "request_timeout" });
    }
    if (bounded.kind !== "body") return json(400, { error: "invalid_request" });
    let input: unknown;
    try {
      input = JSON.parse(
        new TextDecoder("utf-8", { fatal: true }).decode(bounded.bytes),
      );
    } catch {
      return json(400, { error: "invalid_request" });
    }
    if (!isTicket(input)) return json(400, { error: "invalid_request" });
    let valid = false;
    try {
      valid = await config.store.consumeTicket(input.ticket_id, input.ticket);
    } catch {
      return json(503, { error: "recovery_unavailable" });
    }
    if (!valid) return new Response(null, { status: 204 });
    try {
      const result = await scanFailedDeliveries(config.store, config.github, {
        now: config.now,
      });
      return json(200, result);
    } catch {
      return json(503, { error: "recovery_unavailable" });
    }
  };
}

function isTicket(
  value: unknown,
): value is { ticket_id: string; ticket: string } {
  if (value === null || typeof value !== "object") return false;
  const item = value as Record<string, unknown>;
  return typeof item.ticket_id === "string" &&
    /^[0-9a-f-]{36}$/.test(item.ticket_id) &&
    typeof item.ticket === "string" && /^[0-9a-f-]{36}$/.test(item.ticket);
}

function json(status: number, value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

type TicketBodyResult =
  | { kind: "body"; bytes: Uint8Array }
  | { kind: "too_large" | "timeout" | "invalid" };

async function readTicketBody(request: Request): Promise<TicketBodyResult> {
  const contentLength = request.headers.get("content-length");
  if (
    contentLength !== null &&
    (!/^\d+$/.test(contentLength) || Number(contentLength) > 1024)
  ) {
    return { kind: "too_large" };
  }
  if (!request.body) return { kind: "invalid" };
  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  const deadline = Date.now() + TICKET_BODY_TIMEOUT_MS;
  try {
    for (;;) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) return { kind: "timeout" };
      const read = reader.read();
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const result = await Promise.race([
        read,
        new Promise<"timeout">((resolve) => {
          timeout = setTimeout(() => resolve("timeout"), remaining);
        }),
      ]);
      if (timeout !== undefined) clearTimeout(timeout);
      if (result === "timeout") {
        await reader.cancel().catch(() => undefined);
        return { kind: "timeout" };
      }
      if (result.done) break;
      size += result.value.byteLength;
      if (size > 1024) {
        await reader.cancel().catch(() => undefined);
        return { kind: "too_large" };
      }
      chunks.push(result.value);
    }
  } catch {
    return { kind: "invalid" };
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return { kind: "body", bytes };
}

function hasJsonContentType(value: string | null): boolean {
  if (value === null) return false;
  return /^application\/(?:[a-z0-9.+-]+\+)?json(?:\s*;|$)/i.test(value.trim());
}
