import {
  createWakeDispatcher,
  type DispatchAttempt,
  type DispatchOutcomeKind,
  type DispatchStore,
} from "./handler.ts";
import type { Pool, PoolClient } from "@db/postgres";
import {
  createPostgresWakeStore,
  type WakeDatabaseTimeouts,
} from "./postgres_wake.ts";

const TICKET_ID = "11111111-1111-4111-8111-111111111111";
const TICKET_VALUE = "22222222-2222-4222-8222-222222222222";
const ATTEMPT_ID = "33333333-3333-4333-8333-333333333333";
const WAKE_ID = "44444444-4444-4444-8444-444444444444";
const BACKEND_TOKEN = "local-only-test-backend-token-000000000000";
const TICKET_BODY = JSON.stringify({
  ticket_id: TICKET_ID,
  ticket: TICKET_VALUE,
});

class TestStore implements DispatchStore {
  consumeCalls = 0;
  recordCalls: Array<
    { outcome: DispatchOutcomeKind; httpStatus: number | null }
  > = [];
  attempt: DispatchAttempt | null = {
    attemptId: ATTEMPT_ID,
    wakeId: WAKE_ID,
    generation: "19",
    reason: "pending_inbox",
  };
  recorded = true;

  async consumeTicket(
    ticketId: string,
    ticketValue: string,
  ): Promise<DispatchAttempt | null> {
    this.consumeCalls += 1;
    assert(ticketId === TICKET_ID, "ticket id not passed to store");
    assert(ticketValue === TICKET_VALUE, "ticket not passed to store");
    return this.attempt;
  }

  async recordResult(
    _attempt: DispatchAttempt,
    outcome: DispatchOutcomeKind,
    httpStatus: number | null,
  ): Promise<boolean> {
    this.recordCalls.push({ outcome, httpStatus });
    return this.recorded;
  }
}

Deno.test("valid ticket sends one bounded authenticated request and records receipt only", async () => {
  const store = new TestStore();
  let calls = 0;
  const handler = createWakeDispatcher({
    backendUrl: "https://continumm-api.example",
    backendToken: BACKEND_TOKEN,
    store,
    fetcher: async (input, init) => {
      calls += 1;
      assert(
        String(input) === "https://continumm-api.example/internal/v1/wake",
        "wrong fixed endpoint",
      );
      assert(init?.method === "POST", "wake method must be POST");
      assert(init?.redirect === "error", "redirects must be refused");
      const headers = new Headers(init?.headers);
      assert(
        headers.get("authorization") === `Bearer ${BACKEND_TOKEN}`,
        "wake token missing",
      );
      const sent = JSON.parse(String(init?.body)) as Record<string, unknown>;
      assert(sent.attempt_id === ATTEMPT_ID, "attempt id missing");
      assert(sent.generation === "19", "generation must remain decimal text");
      assert(
        sent.wake_id === WAKE_ID && sent.reason === "pending_inbox",
        "wake envelope invalid",
      );
      assert(
        !("ticket" in sent) && !("ticket_id" in sent),
        "first-hop ticket leaked to Render",
      );
      return acceptedResponse();
    },
  });

  const response = await handler(dispatchRequest(TICKET_BODY));
  assert(
    response.status === 204,
    "dispatch response must not imply work was drained",
  );
  assert(
    calls === 1 && store.consumeCalls === 1,
    "expected one consume and one wake",
  );
  assert(
    store.recordCalls.length === 1,
    "accepted receipt outcome was not persisted",
  );
  assert(
    store.recordCalls[0]?.outcome === "accepted",
    "wrong dispatch outcome",
  );
  assert(
    store.recordCalls[0]?.httpStatus === 202,
    "wrong accepted HTTP status",
  );
});

Deno.test("idle result and invalid first-hop input never reach the backend", async () => {
  const idleStore = new TestStore();
  idleStore.attempt = null;
  let idleFetches = 0;
  const idleHandler = createWakeDispatcher({
    backendUrl: "https://continumm-api.example",
    backendToken: BACKEND_TOKEN,
    store: idleStore,
    fetcher: async () => {
      idleFetches += 1;
      return acceptedResponse();
    },
  });
  assert(
    (await idleHandler(dispatchRequest(TICKET_BODY))).status === 204,
    "idle tick should be empty",
  );
  assert(
    idleFetches === 0 && idleStore.recordCalls.length === 0,
    "idle tick reached backend",
  );

  const invalidStore = new TestStore();
  let invalidFetches = 0;
  const invalidHandler = createWakeDispatcher({
    backendUrl: "https://continumm-api.example",
    backendToken: BACKEND_TOKEN,
    store: invalidStore,
    fetcher: async () => {
      invalidFetches += 1;
      return acceptedResponse();
    },
  });
  assert(
    (await invalidHandler(dispatchRequest('{"ticket":"bad"}'))).status === 400,
  );
  assert(
    (await invalidHandler(dispatchRequest(" ".repeat(2_049)))).status === 413,
  );
  assert(
    (await invalidHandler(
      new Request("https://edge.example/wake", { method: "GET" }),
    )).status === 405,
  );
  assert(
    invalidFetches === 0 && invalidStore.consumeCalls === 0,
    "invalid input was dispatched",
  );
});

Deno.test("stalled first-hop body is bounded and caller abort cancels its stream", async () => {
  const store = new TestStore();
  let timeoutFetches = 0;
  let timedBodyCancelled = false;
  const handler = createWakeDispatcher({
    backendUrl: "https://continumm-api.example",
    backendToken: BACKEND_TOKEN,
    store,
    bodyTimeoutMs: 20,
    fetcher: async () => {
      timeoutFetches += 1;
      return acceptedResponse();
    },
  });
  const timedRequest = dispatchRequest(
    stallingStream(() => timedBodyCancelled = true),
  );
  assert(
    (await handler(timedRequest)).status === 408,
    "stalled body was not bounded",
  );
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert(timedBodyCancelled, "timed-out request stream was not cancelled");
  assert(
    timeoutFetches === 0 && store.consumeCalls === 0,
    "stalled body was dispatched",
  );

  let callerBodyCancelled = false;
  const requestController = new AbortController();
  const abortedRequest = new Request("https://edge.example/wake-dispatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: stallingStream(() => callerBodyCancelled = true),
    signal: requestController.signal,
  });
  setTimeout(() => requestController.abort(), 10);
  assert(
    (await handler(abortedRequest)).status === 408,
    "caller abort was not observed",
  );
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert(callerBodyCancelled, "caller abort did not cancel its stream");
  assert(
    timeoutFetches === 0 && store.consumeCalls === 0,
    "aborted body was dispatched",
  );
});

Deno.test("backend 200, malformed 202, outage, timeout, and stale fences are not success", async () => {
  const cases: Array<{
    name: string;
    fetcher: typeof fetch;
    expectedOutcome: DispatchOutcomeKind;
    expectedStatus: number | null;
    expectedResponse: number;
  }> = [
    {
      name: "http 200",
      fetcher: async () => acceptedResponse(200),
      expectedOutcome: "protocol_failure",
      expectedStatus: 200,
      expectedResponse: 502,
    },
    {
      name: "malformed 202",
      fetcher: async () =>
        new Response("{}", {
          status: 202,
          headers: { "content-type": "application/json" },
        }),
      expectedOutcome: "protocol_failure",
      expectedStatus: 202,
      expectedResponse: 502,
    },
    {
      name: "receipt belongs to another wake",
      fetcher: async () =>
        Response.json({
          accepted: true,
          receipt_id: "55555555-5555-4555-8555-555555555555",
          status: "worker_not_started",
          drain_status: "not_started",
          last_successful_drain_at: null,
        }, { status: 202 }),
      expectedOutcome: "protocol_failure",
      expectedStatus: 202,
      expectedResponse: 502,
    },
    {
      name: "backend unavailable",
      fetcher: async () => new Response(null, { status: 503 }),
      expectedOutcome: "retryable_failure",
      expectedStatus: 503,
      expectedResponse: 503,
    },
    {
      name: "response lost",
      fetcher: async () => {
        throw new Error("synthetic_transport_error");
      },
      expectedOutcome: "unknown",
      expectedStatus: null,
      expectedResponse: 503,
    },
  ];
  for (const item of cases) {
    const store = new TestStore();
    const handler = createWakeDispatcher({
      backendUrl: "https://continumm-api.example",
      backendToken: BACKEND_TOKEN,
      store,
      fetcher: item.fetcher,
    });
    const response = await handler(dispatchRequest(TICKET_BODY));
    assert(
      response.status === item.expectedResponse,
      `${item.name}: response status`,
    );
    assert(store.recordCalls.length === 1, `${item.name}: result not recorded`);
    assert(
      store.recordCalls[0]?.outcome === item.expectedOutcome,
      `${item.name}: wrong outcome`,
    );
    assert(
      store.recordCalls[0]?.httpStatus === item.expectedStatus,
      `${item.name}: wrong HTTP status`,
    );
  }

  const staleStore = new TestStore();
  staleStore.recorded = false;
  const staleHandler = createWakeDispatcher({
    backendUrl: "https://continumm-api.example",
    backendToken: BACKEND_TOKEN,
    store: staleStore,
    fetcher: async () => acceptedResponse(),
  });
  assert((await staleHandler(dispatchRequest(TICKET_BODY))).status === 409);
  assert(
    staleStore.recordCalls[0]?.outcome === "accepted",
    "stale callback changed to drain",
  );
});

Deno.test("backend response body timeout remains unknown and is not retried", async () => {
  const store = new TestStore();
  let backendCalls = 0;
  let bodyCancelled = false;
  const handler = createWakeDispatcher({
    backendUrl: "https://continumm-api.example",
    backendToken: BACKEND_TOKEN,
    store,
    requestTimeoutMs: 20,
    fetcher: async () => {
      backendCalls += 1;
      return new Response(stallingStream(() => bodyCancelled = true), {
        status: 202,
        headers: { "content-type": "application/json" },
      });
    },
  });
  const response = await handler(dispatchRequest(TICKET_BODY));
  assert(response.status === 503, "response-body timeout was accepted");
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert(bodyCancelled, "timed-out backend body was not cancelled");
  assert(backendCalls === 1, "backend call was retried in memory");
  assert(store.recordCalls.length === 1, "unknown outcome was not persisted");
  assert(
    store.recordCalls[0]?.outcome === "unknown",
    "timeout was not unknown",
  );
  assert(store.recordCalls[0]?.httpStatus === 202, "observed status was lost");
});

Deno.test("late pool acquisition is closed and released after its deadline", async () => {
  let resolveConnection!: (client: PoolClient) => void;
  const acquisition = new Promise<PoolClient>((resolve) => {
    resolveConnection = resolve;
  });
  const order: string[] = [];
  const client = fakeDatabaseClient(
    () => Promise.resolve({ rows: [] }),
    async () => {
      order.push("close");
    },
    () => order.push("release"),
  );
  const pool = { connect: () => acquisition } as unknown as Pool;
  const timeouts: WakeDatabaseTimeouts = {
    acquireMs: 10,
    queryMs: 20,
    closeMs: 20,
  };
  const store = createPostgresWakeStore(pool, timeouts);
  let failed = false;
  try {
    await store.consumeTicket(TICKET_ID, TICKET_VALUE);
  } catch {
    failed = true;
  }
  assert(failed, "pool acquisition did not honor its deadline");
  assert(order.length === 0, "unacquired client was released prematurely");

  resolveConnection(client);
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert(order.join(",") === "close,release", "late client was not discarded");
});

Deno.test("failed or timed-out query closes a checked-out client before release", async () => {
  const order: string[] = [];
  const client = fakeDatabaseClient(
    () => new Promise(() => undefined),
    async () => {
      order.push("close");
    },
    () => order.push("release"),
  );
  const pool = { connect: async () => client } as unknown as Pool;
  const store = createPostgresWakeStore(pool, {
    acquireMs: 20,
    queryMs: 10,
    closeMs: 20,
  });
  let failed = false;
  try {
    await store.consumeTicket(TICKET_ID, TICKET_VALUE);
  } catch {
    failed = true;
  }
  assert(failed, "query timeout was not surfaced");
  assert(order.join(",") === "close,release", "failed connection was reused");
});

function dispatchRequest(body: BodyInit): Request {
  return new Request("https://edge.example/wake-dispatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });
}

function acceptedResponse(status = 202): Response {
  return Response.json({
    accepted: true,
    receipt_id: WAKE_ID,
    status: "worker_not_started",
    drain_status: "not_started",
    last_successful_drain_at: null,
  }, { status });
}

function fakeDatabaseClient(
  query: () => Promise<{ rows: unknown[] }>,
  end: () => Promise<void>,
  release: () => void,
): PoolClient {
  return {
    queryObject: query,
    end,
    release,
  } as unknown as PoolClient;
}

function stallingStream(onCancel: () => void): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode("{"));
    },
    cancel() {
      onCancel();
    },
  });
}

function assert(
  condition: unknown,
  message = "assertion failed",
): asserts condition {
  if (!condition) throw new Error(message);
}
