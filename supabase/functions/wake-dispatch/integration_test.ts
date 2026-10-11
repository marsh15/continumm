import { Pool } from "@db/postgres";
import { createWakeDispatcher } from "./handler.ts";
import { createPostgresWakeStore } from "./postgres_wake.ts";

Deno.test("Postgres ticket consumption and mocked Render receipt use the real Edge handler", async () => {
  const edgeUrl = Deno.env.get("CONTINUUM_TEST_WAKE_DATABASE_URL");
  const ticketId = Deno.env.get("CONTINUUM_TEST_WAKE_TICKET_ID");
  const ticketValue = Deno.env.get("CONTINUUM_TEST_WAKE_TICKET_VALUE");
  const backendToken = Deno.env.get("CONTINUUM_TEST_WAKE_BACKEND_TOKEN");
  const backendFixtureUrl = Deno.env.get(
    "CONTINUUM_TEST_WAKE_BACKEND_FIXTURE_URL",
  );
  if (
    !edgeUrl || !ticketId || !ticketValue || !backendToken ||
    !backendFixtureUrl
  ) {
    throw new Error("CONTINUUM_TEST_WAKE_FIXTURE_REQUIRED");
  }

  const edgePool = new Pool(edgeUrl, 1, true);
  let backendCalls = 0;
  let backendFixtureStage = "not_called";
  const handler = createWakeDispatcher({
    backendUrl: "https://continumm-api.example",
    backendToken,
    store: createPostgresWakeStore(edgePool),
    fetcher: async (input, init) => {
      backendCalls += 1;
      backendFixtureStage = "backend_request";
      assert(
        String(input) === "https://continumm-api.example/internal/v1/wake",
      );
      assert(init?.redirect === "error");
      assert(
        new Headers(init?.headers).get("authorization") ===
          `Bearer ${backendToken}`,
      );
      const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
      assert(body.ticket === undefined && body.ticket_id === undefined);
      assert(body.reason === "pending_inbox");
      assert(typeof body.attempt_id === "string");
      assert(typeof body.wake_id === "string");
      assert(typeof body.generation === "string");
      assert(/^[1-9][0-9]{0,18}$/.test(body.generation));
      backendFixtureStage = "backend_http";
      return await fetch(
        new URL("/internal/v1/wake", backendFixtureUrl),
        init,
      );
    },
  });

  try {
    const response = await handler(
      new Request("https://edge.example/wake-dispatch", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ ticket_id: ticketId, ticket: ticketValue }),
      }),
    );
    const responseBody = await response.text();
    assert(
      response.status === 204,
      `Edge dispatcher returned ${response.status} (${responseBody}); calls=${backendCalls}; ` +
        `stage=${backendFixtureStage}`,
    );
    assert(
      backendCalls === 1,
      "Edge handler did not call the backend exactly once",
    );
  } finally {
    await edgePool.end();
  }
});

function assert(
  condition: unknown,
  message = "assertion failed",
): asserts condition {
  if (!condition) throw new Error(message);
}
