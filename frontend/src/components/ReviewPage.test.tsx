import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createMemoryRouter, RouterProvider } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../App";

const RUN_ID = "f48f3a9a-5f95-4efa-8a67-b6eecafc9120";
const DIGEST = "a".repeat(64);

function phase(name: string) {
  return {
    phase: name,
    outcome: "passed",
    allocation_id: null,
    artifact_id: `${name}-artifact`,
    repository_id: 123,
    base_commit_sha: "1".repeat(40),
    baseline_git_tree_sha256: "2".repeat(64),
    profile_id: "profile-default",
    profile_version: "1",
    image: null,
    runtime_version: "22.12.0",
    npm_version: "11.0.0",
    command_id: `${name}-command`,
    baseline_archive_sha256: "3".repeat(64),
    baseline_archive_tree_sha256: "4".repeat(64),
    source_archive_sha256: "5".repeat(64),
    source_tree_sha256: "6".repeat(64),
    test_exit_code: 0,
    package_count: 12,
    allocation_status: "completed",
    error_code: null,
    commands: [{ phase: "unit_tests", exit_code: 0, stdout_bytes: 120, stderr_bytes: 0, stdout_sha256: "b".repeat(64), stderr_sha256: "c".repeat(64) }],
  };
}

function review(overrides: Record<string, unknown> = {}) {
  return {
    run_id: RUN_ID,
    revision: 0,
    status: "waiting_approval",
    diff: {
      text: "diff --git a/src/one.ts b/src/one.ts\n--- a/src/one.ts\n+++ b/src/one.ts\n@@ -1 +1 @@\n-old\n+new\ndiff --git a/src/two.ts b/src/two.ts\n--- a/src/two.ts\n+++ b/src/two.ts\n@@ -1 +1 @@\n-old-two\n+new-two\n",
      changed_paths: ["src/one.ts", "src/two.ts"],
      truncated: false,
    },
    evidence: {
      evidence_version: 1,
      classification: "passed",
      baseline_artifact_id: "baseline-artifact",
      candidate_archive_sha256: "d".repeat(64),
      candidate_source_tree_sha256: "e".repeat(64),
      candidate_lineage_sha256: "f".repeat(64),
      changed_path_count: 2,
      regression_on_original: "absent",
      baseline: phase("baseline"),
      candidate: phase("candidate"),
    },
    manifest: { manifest_version: 1, branch: "continuum/task-123", candidate: { changed_paths: ["src/one.ts", "src/two.ts"] } },
    manifest_sha256: DIGEST,
    approval: { approval_id: "approval-1", manifest_sha256: DIGEST, expires_at: "2099-12-31T23:59:59Z", status: "pending" },
    limits: {
      requests: { used: 5, limit: 30 },
      input_tokens: { used: 17000, limit: 120000 },
      output_tokens: { used: 3200, limit: 30000 },
      sandbox_seconds: { used: 120, limit: 900 },
    },
    revisions: { used: 0, limit: 2, remaining: 2 },
    decision_eligible: true,
    ...overrides,
  };
}

function snapshot(overrides: Record<string, unknown> = {}) {
  return {
    run_id: RUN_ID,
    status: "waiting_approval",
    revision: 0,
    created_at: "2026-10-12T08:00:00Z",
    deadline_at: "2099-12-31T23:59:59Z",
    event_watermark: "0",
    ...overrides,
  };
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

function renderReview() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  const router = createMemoryRouter([{ path: "*", element: <App /> }], { initialEntries: [`/runs/${RUN_ID}/review`] });
  return render(<QueryClientProvider client={queryClient}><RouterProvider router={router} /></QueryClientProvider>);
}

function routeFetch(onDecision?: (request: RequestInit) => Response | Promise<Response>) {
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input), window.location.origin);
    if (url.pathname.endsWith("/review")) return jsonResponse(review());
    if (url.pathname.endsWith("/decision")) return onDecision ? onDecision(init ?? {}) : jsonResponse({ approval_id: "approval-0", run_id: RUN_ID, revision: 0, status: "waiting_approval", manifest_sha256: DIGEST }, 202);
    if (url.pathname === `/api/v1/runs/${RUN_ID}`) return jsonResponse(snapshot());
    return jsonResponse({ code: "not_found", message: "Not found", retryable: false, request_id: "test" }, 404);
  });
}

beforeEach(() => {
  document.cookie = "csrf_token=csrf-value; path=/";
});

afterEach(() => {
  cleanup();
  document.cookie = "csrf_token=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT";
  vi.unstubAllGlobals();
});

describe("candidate review", () => {
  it("shows file navigation, validation evidence, exact manifest, expiry, and cumulative limits", async () => {
    vi.stubGlobal("fetch", routeFetch());
    renderReview();

    expect(await screen.findByRole("heading", { name: "Review the proposed changes" })).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Changed files" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /src\/one.ts/ })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByText(/diff --git a\/src\/one.ts/)).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Baseline and candidate" })).toBeInTheDocument();
    expect(screen.getByText("a".repeat(64))).toBeInTheDocument();
    expect(screen.getByText(/Model requests/)).toBeInTheDocument();
    expect(screen.getByText((_, element) => element?.textContent === "5 / 30")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /src\/two.ts/ }));
    expect(screen.getByRole("button", { name: /src\/two.ts/ })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByText(/diff --git a\/src\/two.ts/)).toBeInTheDocument();
  });

  it("requires bounded feedback for changes and posts the displayed digest with CSRF", async () => {
    const fetchMock = routeFetch();
    vi.stubGlobal("fetch", fetchMock);
    renderReview();
    const requestChanges = await screen.findByRole("button", { name: "Request changes" });
    await userEvent.click(requestChanges);
    expect(screen.getByRole("alert")).toHaveTextContent("Add feedback before requesting changes");

    await userEvent.type(screen.getByLabelText(/Feedback for the next revision/), "Please update the parser.");
    await userEvent.click(requestChanges);
    await waitFor(() => expect(fetchMock.mock.calls.some(([input]) => String(input).includes("/decision"))).toBe(true));
    const decisionCall = fetchMock.mock.calls.find(([input]) => String(input).includes("/decision"));
    expect(decisionCall?.[1]?.method).toBe("POST");
    expect(decisionCall?.[1]?.headers).toMatchObject({ "Idempotency-Key": expect.any(String), "X-CSRF-Token": "csrf-value" });
    expect(JSON.parse(String(decisionCall?.[1]?.body))).toEqual({ decision: "changes", manifest_sha256: DIGEST, feedback: "Please update the parser." });
  });

  it("does not send revision feedback with approve or reject decisions", async () => {
    const fetchMock = routeFetch();
    vi.stubGlobal("fetch", fetchMock);
    renderReview();
    await userEvent.type(await screen.findByLabelText(/Feedback for the next revision/), "A note only for rework.");
    await userEvent.click(screen.getByRole("button", { name: "Approve candidate" }));
    await waitFor(() => expect(fetchMock.mock.calls.some(([input]) => String(input).includes("/decision"))).toBe(true));
    const decisionCall = fetchMock.mock.calls.find(([input]) => String(input).includes("/decision"));
    expect(JSON.parse(String(decisionCall?.[1]?.body))).toEqual({ decision: "approve", manifest_sha256: DIGEST });
  });

  it("uses the same idempotency key and body when a decision response can be retried", async () => {
    let decisions = 0;
    const fetchMock = routeFetch(() => {
      decisions += 1;
      return decisions === 1
        ? jsonResponse({ code: "service_unavailable", message: "Try again.", retryable: true, request_id: "retry" }, 503)
        : jsonResponse({ approval_id: "approval-0", run_id: RUN_ID, revision: 0, status: "waiting_approval", manifest_sha256: DIGEST }, 202);
    });
    vi.stubGlobal("fetch", fetchMock);
    renderReview();
    await userEvent.click(await screen.findByRole("button", { name: "Approve candidate" }));
    const retry = await screen.findByRole("button", { name: "Retry the same decision" });
    const first = fetchMock.mock.calls.find(([input]) => String(input).includes("/decision"))?.[1];
    await userEvent.click(retry);
    await waitFor(() => expect(decisions).toBe(2));
    const calls = fetchMock.mock.calls.filter(([input]) => String(input).includes("/decision")).map(([, init]) => init);
    expect(calls[0]?.headers).toMatchObject({ "Idempotency-Key": (first?.headers as Record<string, string>)["Idempotency-Key"] });
    expect(calls[0]?.body).toBe(calls[1]?.body);
  });

  it("reconciles to the latest run after a stale decision conflict and keeps the feedback draft", async () => {
    let currentRevision = 0;
    let currentStatus = "waiting_approval";
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), window.location.origin);
      if (url.pathname.endsWith("/review")) return jsonResponse(review({
        revision: currentRevision,
        status: currentStatus,
        decision_eligible: currentStatus === "waiting_approval",
        revisions: { used: currentRevision, limit: 2, remaining: 2 - currentRevision },
      }));
      if (url.pathname.endsWith("/decision")) {
        currentRevision = 1;
        currentStatus = "running";
        return jsonResponse({ code: "approval_stale", message: "The candidate changed.", retryable: false, request_id: "stale" }, 409);
      }
      if (url.pathname === `/api/v1/runs/${RUN_ID}`) return jsonResponse(snapshot({ revision: currentRevision, status: currentStatus }));
      return jsonResponse({ code: "not_found", message: "Not found", retryable: false, request_id: "test" }, 404);
    });
    vi.stubGlobal("fetch", fetchMock);
    renderReview();
    const field = await screen.findByLabelText(/Feedback for the next revision/);
    await userEvent.type(field, "Keep this note while the run refreshes.");
    await userEvent.click(screen.getByRole("button", { name: "Request changes" }));
    expect(await screen.findByText(/This review changed while it was open/)).toBeInTheDocument();
    await waitFor(() => expect(screen.getByText("Running")).toBeInTheDocument());
    expect(screen.getByLabelText(/Feedback for the next revision/)).toHaveValue("Keep this note while the run refreshes.");
    expect(screen.getByRole("button", { name: "Approve candidate" })).toBeDisabled();
  });

  it("disables submissions after expiry and rejects feedback above 16 KiB", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), window.location.origin);
      if (url.pathname.endsWith("/review")) return jsonResponse(review({ approval: { approval_id: "approval-0", manifest_sha256: DIGEST, expires_at: "2020-01-01T00:00:00Z", status: "expired" } }));
      return jsonResponse(snapshot());
    });
    vi.stubGlobal("fetch", fetchMock);
    renderReview();
    expect(await screen.findByText(/approval has expired/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Approve candidate" })).toBeDisabled();
    expect(fetchMock.mock.calls.some(([input]) => String(input).includes("/decision"))).toBe(false);

    cleanup();
    vi.stubGlobal("fetch", routeFetch());
    renderReview();
    const field = await screen.findByLabelText(/Feedback for the next revision/);
    fireEvent.change(field, { target: { value: "é".repeat(8193) } });
    expect(screen.getByText(/16,386 \/ 16,384 bytes/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Request changes" })).toBeDisabled();
  });
});
