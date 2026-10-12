import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../App";
import type { RunEvent, RunEventPage } from "../api/client";

const RUN_ONE = "f48f3a9a-5f95-4efa-8a67-b6eecafc9120";
const RUN_TWO = "ac7dd330-d1a8-4313-8476-40987ea9a255";

class FakeEventSource {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSED = 2;
  static instances: FakeEventSource[] = [];

  readyState = FakeEventSource.CONNECTING;
  closed = false;
  onopen: ((event: Event) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  private listeners = new Map<string, Array<(event: Event) => void>>();

  constructor(
    readonly url: string,
    readonly options?: EventSourceInit,
  ) {
    FakeEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
    const handlers = this.listeners.get(type) ?? [];
    handlers.push(typeof listener === "function" ? listener : (event) => listener.handleEvent(event));
    this.listeners.set(type, handlers);
  }

  close(): void {
    this.closed = true;
    this.readyState = FakeEventSource.CLOSED;
  }

  open(): void {
    this.readyState = FakeEventSource.OPEN;
    this.onopen?.(new Event("open"));
  }

  fail(readyState: number): void {
    this.readyState = readyState;
    this.onerror?.(new Event("error"));
  }

  emit(name: string, payload: unknown): void {
    const lastEventId =
      typeof payload === "object" && payload !== null && "sequence" in payload
        ? String(payload.sequence)
        : "";
    const event = new MessageEvent(name, { data: JSON.stringify(payload), lastEventId });
    this.listeners.get(name)?.forEach((listener) => listener(event));
  }

  static reset(): void {
    FakeEventSource.instances = [];
  }
}

function snapshot(runId: string, eventWatermark = "0", status = "queued") {
  return {
    run_id: runId,
    status,
    revision: 1,
    created_at: "2026-10-12T08:00:00Z",
    deadline_at: null,
    event_watermark: eventWatermark,
  };
}

function event(runId: string, sequence: string, type = "run.admitted"): RunEvent {
  return {
    event_id: `event-${sequence}`,
    run_id: runId,
    sequence,
    type,
    occurred_at: "2026-10-12T08:01:00Z",
    data: {},
  };
}

function page(
  runId: string,
  events: RunEvent[] = [],
  highWatermark = "0",
  nextCursor: string | null = null,
): RunEventPage {
  return {
    run_id: runId,
    after_sequence: "0",
    events,
    next_cursor: nextCursor,
    high_watermark: highWatermark,
    history_gap: false,
  };
}

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function apiError(code: string, message: string, status: number): Response {
  return jsonResponse(
    { code, message, retryable: false, request_id: "run-request-18" },
    status,
  );
}

function renderRun(path = `/runs/${RUN_ONE}`) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0, refetchOnWindowFocus: false },
    },
  });
  const router = createMemoryRouter([{ path: "*", element: <App /> }], {
    initialEntries: [path],
  });
  const result = render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
  return { ...result, queryClient, router };
}

function stubEventSource(): void {
  vi.stubGlobal("EventSource", FakeEventSource);
}

beforeEach(() => {
  FakeEventSource.reset();
  stubEventSource();
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.useRealTimers();
  FakeEventSource.reset();
});

describe("run detail", () => {
  it("shows an explicit loading state before the authoritative snapshot arrives", () => {
    vi.stubGlobal("fetch", vi.fn(() => new Promise<Response>(() => {})));
    renderRun();

    expect(screen.getByRole("heading", { name: "Loading run" })).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("Loading the current run snapshot");
    expect(FakeEventSource.instances).toHaveLength(0);
  });

  it("shows an empty event history and connects with same-origin credentials", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input), window.location.origin);
        if (url.pathname.endsWith("/events")) return jsonResponse(page(RUN_ONE));
        return jsonResponse(snapshot(RUN_ONE));
      }),
    );
    renderRun();

    expect(await screen.findByText("No events have been recorded for this run yet.")).toBeInTheDocument();
    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));
    const source = FakeEventSource.instances[0];
    source.open();
    expect(source.url).toContain("after=0");
    expect(source.options?.withCredentials).toBe(true);
    expect(await screen.findByText("Live updates connected")).toBeInTheDocument();
  });

  it("shows typed snapshot errors and retries the failed request", async () => {
    let snapshotRequests = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input), window.location.origin);
        if (url.pathname.endsWith("/events")) return jsonResponse(page(RUN_ONE));
        snapshotRequests += 1;
        return snapshotRequests === 1
          ? apiError("authentication_required", "Sign in to view this run.", 401)
          : jsonResponse(snapshot(RUN_ONE));
      }),
    );
    renderRun();

    expect(await screen.findByRole("alert", { name: "Could not load this run" })).toHaveTextContent(
      "authentication_required",
    );
    fireEvent.click(screen.getByRole("button", { name: "Retry loading run" }));
    expect(await screen.findByRole("heading", { name: "Current run" })).toBeInTheDocument();
    expect(snapshotRequests).toBe(2);
  });

  it("orders decimal-string events, ignores replay duplicates, and refreshes the snapshot", async () => {
    let watermark = "9007199254740992";
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), window.location.origin);
      if (url.pathname.endsWith("/events")) return jsonResponse(page(RUN_ONE, [], watermark));
      return jsonResponse(snapshot(RUN_ONE, watermark));
    });
    vi.stubGlobal("fetch", fetchMock);
    renderRun();
    await screen.findByText("No events have been recorded for this run yet.");
    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));
    const source = FakeEventSource.instances[0];
    source.open();

    watermark = "9007199254740993";
    act(() => source.emit("run.event", event(RUN_ONE, watermark, "job.claimed")));
    act(() => source.emit("run.event", event(RUN_ONE, watermark, "job.claimed")));
    watermark = "9007199254740994";
    act(() => source.emit("run.event", event(RUN_ONE, watermark, "run.waiting_capacity")));

    expect(await screen.findByRole("heading", { name: "Job Claimed" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Run Waiting Capacity" })).toBeInTheDocument();
    expect(
      screen.getAllByText(
        (_, element) =>
          element !== null &&
          element.classList.contains("timeline-sequence") &&
          element.textContent === "Event #9007199254740993",
      ),
    ).toHaveLength(1);
    await waitFor(() => expect(fetchMock.mock.calls.length).toBeGreaterThan(2));
    expect(screen.getByText("9007199254740994")).toBeInTheDocument();
  });

  it("reconciles a sequence gap and resumes from the refreshed snapshot watermark", async () => {
    let watermark = "5";
    let eventPageRequests = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input), window.location.origin);
        if (url.pathname.endsWith("/events")) {
          eventPageRequests += 1;
          return jsonResponse(page(RUN_ONE, [], watermark));
        }
        return jsonResponse(snapshot(RUN_ONE, watermark));
      }),
    );
    renderRun();
    await screen.findByText("No events have been recorded for this run yet.");
    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));
    const firstSource = FakeEventSource.instances[0];

    watermark = "8";
    act(() => firstSource.emit("run.event", event(RUN_ONE, "7", "job.claimed")));

    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(2));
    expect(firstSource.closed).toBe(true);
    expect(new URL(FakeEventSource.instances[1].url, window.location.origin).searchParams.get("after")).toBe("8");
    expect(eventPageRequests).toBeGreaterThanOrEqual(2);
    expect(await screen.findByText("8")).toBeInTheDocument();

    const secondSource = FakeEventSource.instances[1];
    watermark = "12";
    act(() =>
      secondSource.emit("history.reset", { reason: "history_gap", high_watermark: watermark }),
    );
    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(3));
    expect(secondSource.closed).toBe(true);
    expect(new URL(FakeEventSource.instances[2].url, window.location.origin).searchParams.get("after")).toBe("12");
    expect(eventPageRequests).toBeGreaterThanOrEqual(3);
  });

  it("shows expired-history context after one bounded page reconciliation", async () => {
    let eventPageRequests = 0;
    let snapshotRequests = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), window.location.origin);
      if (url.pathname.endsWith("/events")) {
        eventPageRequests += 1;
        return jsonResponse({
          ...page(RUN_ONE, [event(RUN_ONE, "40")], "40"),
          history_gap: true,
        });
      }
      snapshotRequests += 1;
      return jsonResponse(snapshot(RUN_ONE, "40"));
    }));
    renderRun();

    expect(await screen.findByText(/Earlier event history is unavailable/)).toBeInTheDocument();
    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));
    expect(new URL(FakeEventSource.instances[0].url, window.location.origin).searchParams.get("after")).toBe("40");
    expect(eventPageRequests).toBe(2);
    expect(snapshotRequests).toBe(2);
  });

  it("caps network retries and waits for a manual retry after refreshing the run", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), window.location.origin);
      if (url.pathname.endsWith("/events")) return jsonResponse(page(RUN_ONE));
      return jsonResponse(snapshot(RUN_ONE));
    }));
    renderRun();
    await screen.findByText("No events have been recorded for this run yet.");
    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));
    vi.useFakeTimers();

    act(() => FakeEventSource.instances[0].fail(FakeEventSource.CONNECTING));
    expect(screen.getByText(/Retrying in 1 seconds/)).toBeInTheDocument();
    await act(async () => vi.advanceTimersByTimeAsync(1_000));
    act(() => FakeEventSource.instances[1].fail(FakeEventSource.CONNECTING));
    await act(async () => vi.advanceTimersByTimeAsync(2_000));
    vi.useRealTimers();
    act(() => FakeEventSource.instances[2].fail(FakeEventSource.CONNECTING));
    expect(await screen.findByRole("button", { name: "Reconnect" })).toBeInTheDocument();
    expect(FakeEventSource.instances).toHaveLength(3);
  });

  it("rechecks access when the capped reconnect budget is exhausted", async () => {
    let snapshotRequests = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), window.location.origin);
      if (url.pathname.endsWith("/events")) return jsonResponse(page(RUN_ONE));
      snapshotRequests += 1;
      return snapshotRequests === 1
        ? jsonResponse(snapshot(RUN_ONE))
        : apiError("authentication_required", "Sign in to view this run.", 401);
    }));
    renderRun();
    await screen.findByText("No events have been recorded for this run yet.");
    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));
    vi.useFakeTimers();

    act(() => FakeEventSource.instances[0].fail(FakeEventSource.CONNECTING));
    await act(async () => vi.advanceTimersByTimeAsync(1_000));
    act(() => FakeEventSource.instances[1].fail(FakeEventSource.CONNECTING));
    await act(async () => vi.advanceTimersByTimeAsync(2_000));
    vi.useRealTimers();
    act(() => FakeEventSource.instances[2].fail(FakeEventSource.CONNECTING));

    expect(await screen.findByRole("alert", { name: "Could not load this run" })).toHaveTextContent(
      "authentication_required",
    );
    expect(FakeEventSource.instances).toHaveLength(3);
  });

  it("rechecks run access once after a closed stream and hides stale private data on 401", async () => {
    let snapshotRequests = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input), window.location.origin);
        if (url.pathname.endsWith("/events")) return jsonResponse(page(RUN_ONE));
        snapshotRequests += 1;
        return snapshotRequests === 1
          ? jsonResponse(snapshot(RUN_ONE))
          : apiError("authentication_required", "Sign in to view this run.", 401);
      }),
    );
    renderRun();
    await screen.findByText("No events have been recorded for this run yet.");
    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));

    act(() => FakeEventSource.instances[0].fail(FakeEventSource.CLOSED));

    expect(await screen.findByRole("alert", { name: "Could not load this run" })).toHaveTextContent(
      "authentication_required",
    );
    expect(screen.queryByText(RUN_ONE)).not.toBeInTheDocument();
    expect(FakeEventSource.instances).toHaveLength(1);
  });

  it("closes the stream on unmount and ignores events from a previous run after navigation", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = new URL(String(input), window.location.origin);
        const runId = url.pathname.split("/")[4] ?? RUN_ONE;
        if (url.pathname.endsWith("/events")) return jsonResponse(page(runId));
        return jsonResponse(snapshot(runId));
      }),
    );
    const view = renderRun();
    await screen.findByText("No events have been recorded for this run yet.");
    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(1));
    const oldSource = FakeEventSource.instances[0];

    await act(async () => view.router.navigate(`/runs/${RUN_TWO}`));
    await screen.findByText("No events have been recorded for this run yet.");
    await waitFor(() => expect(FakeEventSource.instances).toHaveLength(2));
    expect(oldSource.closed).toBe(true);
    act(() => oldSource.emit("run.event", event(RUN_ONE, "1", "job.claimed")));
    expect(screen.queryByRole("heading", { name: "Job Claimed" })).not.toBeInTheDocument();
    expect(screen.getByText(RUN_TWO)).toBeInTheDocument();

    view.unmount();
    expect(FakeEventSource.instances[1].closed).toBe(true);
  });
});
