import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { OperatorPage } from "./OperatorPage";

const overview = {
  admissions_stopped: true,
  active_runs: 2,
  unknown_effects: 1,
  uncertain_allocations: 1,
  pending_inbox_deliveries: 3,
  oldest_pending_inbox_at: "2026-10-20T10:00:00Z",
  artifact_charged_bytes: 1024,
  artifact_capacity_limit_bytes: 4096,
  suspended_accounts: 1,
};

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0, refetchInterval: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter><OperatorPage /></MemoryRouter>
    </QueryClientProvider>,
  );
}

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("restricted operator view", () => {
  it("shows only sanitized operational metadata and no mutation controls", async () => {
    const calls: Array<{ url: string; method: string; credentials: RequestCredentials | undefined }> = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      calls.push({ url, method: init?.method ?? "GET", credentials: init?.credentials });
      if (url.endsWith("/overview")) return json(overview);
      if (url.endsWith("/unknown-effects")) return json([{
        effect_id: "11111111-1111-4111-8111-111111111111",
        owner_id: "22222222-2222-4222-8222-222222222222",
        run_id: "33333333-3333-4333-8333-333333333333",
        status: "unknown",
        created_at: "2026-10-20T10:00:00Z",
        updated_at: "2026-10-20T10:01:00Z",
      }]);
      if (url.endsWith("/uncertain-allocations")) return json([]);
      if (url.endsWith("/inbox-lag")) return json([{
        delivery_id: "44444444-4444-4444-8444-444444444444",
        event_type: "issue_comment",
        received_at: "2026-10-20T10:00:00Z",
      }]);
      if (url.endsWith("/suspended-accounts")) return json([]);
      if (url.endsWith("/audit")) return json([]);
      return json({ code: "not_found", message: "Not found", retryable: false, request_id: "t42" }, 404);
    }));

    renderPage();
    expect(await screen.findByText("Admissions stopped")).toBeTruthy();
    expect(await screen.findByText("Unknown external effects")).toBeTruthy();
    expect(await screen.findByText("issue_comment")).toBeTruthy();
    expect(screen.getByText("Runtime status")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /stop|resume|suspend|retry|complete/i })).toBeNull();
    await waitFor(() => expect(calls.length).toBe(6));
    expect(calls.every((call) => call.method === "GET" && call.credentials === "same-origin")).toBe(true);
    expect(calls.every((call) => call.url.startsWith("/api/v1/operator/"))).toBe(true);
  });

  it("does not show operational metadata to an account outside the allowlist", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => json({
      code: "not_found",
      message: "Not found",
      retryable: false,
      request_id: "t42-denied",
    }, 404)));
    renderPage();
    expect(await screen.findByText("Access is restricted")).toBeTruthy();
    expect(screen.queryByText("Runtime status")).toBeNull();
  });
});
