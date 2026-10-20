import {
  type ClaimOutcome,
  createRecoveryHandler,
  type Delivery,
  type GitHubRecoveryClient,
  type RecoveryState,
  type RecoveryStore,
  scanFailedDeliveries,
} from "./handler.ts";

const GUID_A = "11111111-1111-4111-8111-111111111111";
const GUID_B = "22222222-2222-4222-8222-222222222222";

class TestStore implements RecoveryStore {
  state: RecoveryState = {
    watermark: null,
    scanCursor: null,
    pendingWatermark: null,
    lastScanAt: null,
    historyGapSince: null,
  };
  claimed = new Set<string>();
  outcomes: Array<{ guid: string; outcome: string; status: number | null }> =
    [];
  progress: Array<{ cursor: string | null; watermark: string | null }> = [];
  ticketValid = true;
  busy = false;
  successful = false;

  async consumeTicket(): Promise<boolean> {
    return this.ticketValid;
  }

  async beginScan(): Promise<RecoveryState | null> {
    return this.busy ? null : structuredClone(this.state);
  }

  async saveScanProgress(
    cursor: string | null,
    watermark: string | null,
  ): Promise<void> {
    this.progress.push({ cursor, watermark });
    this.state.scanCursor = cursor;
    this.state.pendingWatermark = watermark;
  }

  async claimDelivery(delivery: Delivery): Promise<ClaimOutcome> {
    if (this.claimed.has(delivery.guid)) return "already_requested";
    this.claimed.add(delivery.guid);
    return "claimed";
  }

  async recordDeliveryOutcome(
    guid: string,
    outcome: "accepted" | "unknown" | "rate_limited" | "rejected",
    status: number | null,
  ): Promise<void> {
    this.outcomes.push({ guid, outcome, status });
  }

  async completeScan(input: {
    watermark: string | null;
    scannedAt: string;
    historyGapSince: string | null;
    successful: boolean;
  }): Promise<void> {
    if (input.successful) {
      this.state.watermark = input.watermark;
      this.state.lastScanAt = input.scannedAt;
      this.state.scanCursor = null;
      this.state.pendingWatermark = null;
    }
    this.state.historyGapSince = input.historyGapSince;
    this.successful = input.successful;
  }
}

class TestGitHub implements GitHubRecoveryClient {
  pages: Map<
    string,
    {
      deliveries: Delivery[];
      nextCursor: string | null;
      rateLimitedUntil: number | null;
    }
  >;
  listCursors: Array<string | null> = [];
  redelivered: string[] = [];
  redeliveryResult: { status: number; retryAt: number | null } = {
    status: 202,
    retryAt: null,
  };
  throwOnRedelivery = false;

  constructor(pages: TestGitHub["pages"]) {
    this.pages = pages;
  }

  async listFailed(cursor: string | null) {
    this.listCursors.push(cursor);
    return this.pages.get(cursor ?? "") ?? {
      deliveries: [],
      nextCursor: null,
      rateLimitedUntil: null,
    };
  }

  async redeliver(id: string) {
    this.redelivered.push(id);
    if (this.throwOnRedelivery) throw new Error("network_unknown");
    return this.redeliveryResult;
  }
}

function delivery(
  id: string,
  guid: string,
  deliveredAt: string,
  status = 500,
): Delivery {
  return { id, guid, delivered_at: deliveredAt, status_code: status };
}

Deno.test("overlapping cursor scan filters successful events and deduplicates by delivery GUID", async () => {
  const now = Date.parse("2026-10-20T12:00:00.000Z");
  const store = new TestStore();
  store.state.watermark = "2026-10-20T11:55:00.000Z";
  const github = new TestGitHub(
    new Map([
      ["", {
        deliveries: [
          delivery("10", GUID_A, "2026-10-20T11:59:00.000Z", 200),
          delivery("11", GUID_B, "2026-10-20T11:58:00.000Z"),
        ],
        nextCursor: "old-page",
        rateLimitedUntil: null,
      }],
      ["old-page", {
        deliveries: [delivery("12", GUID_A, "2026-10-20T11:30:00.000Z")],
        nextCursor: null,
        rateLimitedUntil: null,
      }],
    ]),
  );

  const result = await scanFailedDeliveries(store, github, { now: () => now });
  assert(result.pages === 2, "expected cursor pagination");
  assert(
    result.failedDeliveriesSeen === 1,
    "successful event must only advance watermark",
  );
  assert(
    result.redeliveryRequests === 1 && github.redelivered[0] === "11",
    "redelivery mismatch",
  );
  assert(github.listCursors.join(",") === ",old-page", "cursor not followed");
  assert(
    store.state.watermark === "2026-10-20T11:59:00.000Z",
    "watermark did not advance",
  );
  assert(
    store.outcomes[0]?.outcome === "accepted",
    "accepted attempt not persisted",
  );
});

Deno.test("bounded page scan persists cursor and resumes without skipping the watermark", async () => {
  const store = new TestStore();
  const pages = new Map();
  for (let i = 0; i < 20; i++) {
    pages.set(i === 0 ? "" : `c${i}`, {
      deliveries: [
        delivery(
          `${100 + i}`,
          `${String(i + 10).padStart(8, "0")}-1111-4111-8111-111111111111`,
          `2026-10-20T11:${String(59 - i).padStart(2, "0")}:00.000Z`,
          200,
        ),
      ],
      nextCursor: `c${i + 1}`,
      rateLimitedUntil: null,
    });
  }
  const github = new TestGitHub(pages);
  const result = await scanFailedDeliveries(store, github, {
    now: () => Date.parse("2026-10-20T12:00:00Z"),
  });
  assert(result.stoppedBy === "page_limit", "scan must stop at page limit");
  assert(store.state.scanCursor === "c20", "next cursor was not durably saved");
  assert(
    store.state.watermark === null,
    "watermark advanced before finishing scan window",
  );
  const resumed = new TestGitHub(
    new Map([["c20", {
      deliveries: [],
      nextCursor: null,
      rateLimitedUntil: null,
    }]]),
  );
  const next = await scanFailedDeliveries(store, resumed, {
    now: () => Date.parse("2026-10-20T12:01:00Z"),
  });
  assert(
    resumed.listCursors[0] === "c20",
    "resumed scan did not use saved cursor",
  );
  assert(next.stoppedBy === "complete", "resumed scan did not complete");
});

Deno.test("rate limits stop the scan without claiming or advancing the checkpoint", async () => {
  const store = new TestStore();
  const github = new TestGitHub(
    new Map([["", {
      deliveries: [],
      nextCursor: null,
      rateLimitedUntil: Date.parse("2026-10-20T12:10:00Z"),
    }]]),
  );
  const result = await scanFailedDeliveries(store, github, {
    now: () => Date.parse("2026-10-20T12:00:00Z"),
  });
  assert(result.stoppedBy === "rate_limit", "rate limit was not surfaced");
  assert(
    !store.successful && store.state.lastScanAt === null,
    "rate-limited scan marked successful",
  );
});

Deno.test("missing-response and redirect status codes remain recoverable failures", async () => {
  const store = new TestStore();
  const noResponseGuid = "33333333-3333-4333-8333-333333333333";
  const redirectGuid = "44444444-4444-4444-8444-444444444444";
  const github = new TestGitHub(
    new Map([["", {
      deliveries: [
        delivery("31", noResponseGuid, "2026-10-20T11:59:00Z", 0),
        delivery("32", redirectGuid, "2026-10-20T11:58:00Z", 302),
      ],
      nextCursor: null,
      rateLimitedUntil: null,
    }]]),
  );
  const result = await scanFailedDeliveries(store, github, {
    now: () => Date.parse("2026-10-20T12:00:00Z"),
  });
  assert(
    result.failedDeliveriesSeen === 2,
    "timeout or redirect was treated as success",
  );
  assert(
    github.redelivered.length === 2,
    "failed attempts were not redelivered",
  );
});

Deno.test("missed inspection window is surfaced and uncertain POST is never repeated", async () => {
  const now = Date.parse("2026-10-20T12:00:00.000Z");
  const store = new TestStore();
  const priorScanAt = "2026-10-16T00:00:00.000Z";
  store.state.lastScanAt = priorScanAt;
  const github = new TestGitHub(
    new Map([["", {
      deliveries: [delivery("44", GUID_A, "2026-10-20T11:59:00.000Z")],
      nextCursor: null,
      rateLimitedUntil: null,
    }]]),
  );
  github.throwOnRedelivery = true;
  const result = await scanFailedDeliveries(store, github, { now: () => now });
  assert(result.historyGapSince === priorScanAt, "history gap not surfaced");
  assert(
    store.outcomes[0]?.outcome === "unknown",
    "uncertain mutation outcome not recorded",
  );
  await scanFailedDeliveries(store, github, { now: () => now + 1_000 });
  assert(github.redelivered.length === 1, "uncertain request was sent twice");
});

Deno.test("recovery endpoint consumes one-use ticket and reports history gap", async () => {
  const store = new TestStore();
  store.ticketValid = false;
  let listCalls = 0;
  const github = new TestGitHub(new Map());
  const handler = createRecoveryHandler({ store, github });
  const unauthorized = await handler(request());
  assert(unauthorized.status === 204, "invalid ticket should be ignored");
  assert(
    listCalls === 0 && github.listCursors.length === 0,
    "invalid ticket reached GitHub",
  );
  store.ticketValid = true;
  const ok = await handler(request());
  assert(ok.status === 200, "valid one-use ticket did not scan");
  const json = await ok.json() as { stoppedBy: string };
  assert(json.stoppedBy === "complete", "scan result missing");
});

Deno.test("recovery endpoint enforces JSON content type and streamed body cap", async () => {
  const store = new TestStore();
  const handler = createRecoveryHandler({
    store,
    github: new TestGitHub(new Map()),
  });
  const wrongType = await handler(
    new Request("https://local/recovery", {
      method: "POST",
      headers: { "content-type": "text/plain" },
      body: "{}",
    }),
  );
  assert(wrongType.status === 415, "unsupported content type was accepted");
  const oversized = await handler(
    new Request("https://local/recovery", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        ticket_id: "a".repeat(36),
        ticket: "b".repeat(36),
        extra: "x".repeat(1200),
      }),
    }),
  );
  assert(oversized.status === 413, "streamed request over 1 KiB was accepted");
});

function request(): Request {
  return new Request("https://local/functions/v1/github-delivery-recovery", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      ticket_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      ticket: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    }),
  });
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
