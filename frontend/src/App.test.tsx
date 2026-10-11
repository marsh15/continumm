import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { App } from "./App";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

function renderApp(path = "/") {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0, refetchInterval: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[path]}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("foundation screen", () => {
  it("shows a live process response and the typed readiness error with its request ID", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url === "/health/live") {
          return new Response(JSON.stringify({ status: "ok", checks: null }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          });
        }
        return new Response(
          JSON.stringify({
            code: "not_ready",
            message: "Database readiness has not been verified.",
            retryable: true,
            request_id: "f48f3a9a-5f95-4efa-8a67-b6eecafc9120",
          }),
          { status: 503, headers: { "Content-Type": "application/json" } },
        );
      }),
    );

    renderApp();

    expect(
      await screen.findByRole("heading", { name: "The foundation is taking shape." }),
    ).toBeInTheDocument();
    expect(await screen.findByText("Available")).toBeInTheDocument();
    expect(
      await screen.findByRole("alert", { name: "Readiness check failed" }),
    ).toHaveTextContent("not_ready");
    expect(screen.getByRole("alert")).toHaveTextContent(
      "f48f3a9a-5f95-4efa-8a67-b6eecafc9120",
    );
    expect(screen.getByText(/The public service has not launched\./)).toBeInTheDocument();
  });

  it("provides a real route fallback without suggesting product functionality", () => {
    renderApp("/future-product-page");

    expect(screen.getByRole("heading", { name: "This page is not part of the current foundation." }))
      .toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Return to service status" })).toHaveAttribute(
      "href",
      "/",
    );
  });

  it("displays typed errors for malformed successful health responses", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        if (String(input) === "/health/live") {
          return new Response("upstream HTML", {
            status: 200,
            headers: { "Content-Type": "text/html", "X-Request-ID": "live-probe-17" },
          });
        }
        return new Response(JSON.stringify({ status: "ready-ish", checks: {} }), {
          status: 200,
          headers: { "Content-Type": "application/json", "X-Request-ID": "ready-probe-18" },
        });
      }),
    );

    renderApp();

    expect(await screen.findByRole("alert", { name: "Process check failed" })).toHaveTextContent(
      "invalid_response",
    );
    expect(screen.getByRole("alert", { name: "Process check failed" })).toHaveTextContent(
      "live-probe-17",
    );
    expect(await screen.findByRole("alert", { name: "Readiness check failed" })).toHaveTextContent(
      "ready-probe-18",
    );
    expect(screen.queryByText("Available")).not.toBeInTheDocument();
  });
});
