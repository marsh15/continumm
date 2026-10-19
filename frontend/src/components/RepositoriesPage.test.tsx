import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { RepositoriesPage } from "./RepositoriesPage";

const repository = {
  id: 7,
  github_repository_id: 123456789,
  owner: "octo",
  name: "project",
  full_name: "octo/project",
  profile_id: "python-pytest",
  profile_version: "1",
  provider: null,
  credential_id: null,
  eligibility: { status: "unchecked", checked_sha: null, checked_at: null, error: null },
};

function renderPage() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter><RepositoriesPage /></MemoryRouter>
    </QueryClientProvider>,
  );
}

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  document.cookie = "csrf_token=; Max-Age=0; path=/";
});

describe("repository settings", () => {
  it("registers the numeric GitHub ID without checking a commit and sends CSRF on writes", async () => {
    const user = userEvent.setup();
    const calls: Array<{ url: string; method: string; body?: unknown; csrf?: string }> = [];
    document.cookie = "csrf_token=csrf-test-token; path=/";
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const method = init?.method ?? "GET";
      calls.push({ url, method, body: init?.body ? JSON.parse(String(init.body)) : undefined, csrf: new Headers(init?.headers).get("X-CSRF-Token") ?? undefined });
      if (url === "/api/v1/repositories" && method === "GET") return json({ repositories: [] });
      if (url === "/api/v1/credentials") return json({ credentials: [] });
      if (url === "/api/v1/repositories" && method === "POST") return json(repository);
      return json({ code: "not_found", message: "Not found", retryable: false, request_id: "test" }, 404);
    }));

    renderPage();
    await user.type(await screen.findByLabelText("GitHub repository ID"), "123456789");
    await user.selectOptions(screen.getByLabelText("Test profile"), "node-npm");
    await user.click(screen.getByRole("button", { name: "Register repository" }));

    await screen.findByRole("status");
    await waitFor(() => expect(calls.some((call) => call.method === "POST")).toBe(true));
    const registration = calls.find((call) => call.method === "POST");
    expect(registration).toMatchObject({
      url: "/api/v1/repositories",
      body: { github_repository_id: 123456789, profile_id: "node-npm" },
      csrf: "csrf-test-token",
    });
    expect(calls.some((call) => call.url.endsWith("/check"))).toBe(false);
  });

  it("validates commit SHAs before the explicit check and shows saved eligibility", async () => {
    const user = userEvent.setup();
    const calls: Array<{ url: string; body?: unknown; csrf?: string }> = [];
    let checked = false;
    document.cookie = "csrf_token=check-token; path=/";
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url === "/api/v1/repositories" && !init?.method) return json({ repositories: [{
        ...repository,
        eligibility: checked ? { status: "eligible", checked_sha: "a".repeat(40), checked_at: "2026-10-19T08:00:00Z", error: null } : repository.eligibility,
      }] });
      if (url === "/api/v1/credentials") return json({ credentials: [] });
      if (url.endsWith("/check")) {
        calls.push({ url, body: JSON.parse(String(init?.body)), csrf: new Headers(init?.headers).get("X-CSRF-Token") ?? undefined });
        checked = true;
        return json({ status: "eligible", checked_sha: "a".repeat(40), checked_at: "2026-10-19T08:00:00Z", profile_id: "python-pytest", profile_version: "1", package_count: 2, file_count: 3, source_bytes: 128, cache_hit: false });
      }
      return json({ repositories: [repository] });
    }));

    renderPage();
    const checkButton = await screen.findByRole("button", { name: "Check eligibility" });
    const sha = screen.getByLabelText("Commit SHA");
    await user.type(sha, "abc");
    await user.click(checkButton);
    expect(await screen.findByRole("alert")).toHaveTextContent("40- or 64-character lowercase commit SHA");
    expect(calls).toHaveLength(0);

    await user.clear(sha);
    await user.type(sha, "a".repeat(40));
    await user.click(checkButton);
    await waitFor(() => expect(calls).toHaveLength(1));
    expect(calls[0]).toMatchObject({ url: "/api/v1/repositories/7/check", body: { commit_sha: "a".repeat(40) }, csrf: "check-token" });
    expect(await screen.findByText("Eligible")).toBeInTheDocument();
    expect(screen.getByText("a".repeat(40))).toBeInTheDocument();
  });

  it("only offers active credentials matching the selected provider", async () => {
    const user = userEvent.setup();
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      if (String(input) === "/api/v1/repositories") return json({ repositories: [] });
      return json({ credentials: [
        { credential_id: "openai-1", provider: "openai", scope: "saved", status: "active", created_at: "2026-10-01T00:00:00Z", expires_at: null },
        { credential_id: "anthropic-1", provider: "anthropic", scope: "saved", status: "active", created_at: "2026-10-01T00:00:00Z", expires_at: null },
        { credential_id: "openai-old", provider: "openai", scope: "saved", status: "active", created_at: "2026-10-01T00:00:00Z", expires_at: "2020-01-01T00:00:00Z" },
      ] });
    }));
    renderPage();
    const provider = await screen.findByLabelText("Default provider");
    await user.selectOptions(provider, "openai");
    const credential = screen.getByLabelText("Saved credential");
    const options = within(credential).getAllByRole("option").map((option) => option.textContent);
    expect(options.some((option) => option?.includes("openai-1"))).toBe(true);
    expect(options.some((option) => option?.includes("anthropic-1"))).toBe(false);
    expect(options.some((option) => option?.includes("openai-old"))).toBe(false);
  });

  it("saves future defaults with a credential that matches the selected provider", async () => {
    const user = userEvent.setup();
    const calls: Array<{ url: string; method: string; body: unknown; csrf?: string }> = [];
    document.cookie = "csrf_token=defaults-token; path=/";
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url === "/api/v1/repositories" && !init?.method) return json({ repositories: [repository] });
      if (url === "/api/v1/credentials") return json({ credentials: [
        { credential_id: "anthropic-saved", provider: "anthropic", scope: "saved", status: "active", created_at: "2026-10-01T00:00:00Z", expires_at: null },
      ] });
      calls.push({ url, method: init?.method ?? "GET", body: JSON.parse(String(init?.body)), csrf: new Headers(init?.headers).get("X-CSRF-Token") ?? undefined });
      return json({ ...repository, profile_id: "node-npm", provider: "anthropic", credential_id: "anthropic-saved" });
    }));

    renderPage();
    const card = within(await screen.findByRole("article", { name: "octo/project" }));
    await user.selectOptions(card.getByLabelText("Test profile"), "node-npm");
    await user.selectOptions(card.getByLabelText("Default provider"), "anthropic");
    await user.selectOptions(card.getByLabelText("Saved credential"), "anthropic-saved");
    await user.click(screen.getByRole("button", { name: "Save defaults" }));

    await screen.findByText("Repository defaults saved.");
    expect(calls[0]).toMatchObject({
      url: "/api/v1/repositories/7",
      method: "PATCH",
      body: { profile_id: "node-npm", provider: "anthropic", credential_id: "anthropic-saved" },
      csrf: "defaults-token",
    });
  });
});
