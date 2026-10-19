import { expect, test } from "@playwright/test";

const repo = {
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

test("registers by GitHub ID, then checks a commit through a separate authenticated request", async ({ page }) => {
  let registered: typeof repo | null = null;
  const writes: Array<{ method: string; url: string; body: unknown; csrf?: string }> = [];
  await page.context().addCookies([{ name: "csrf_token", value: "repository-test-csrf", domain: "127.0.0.1", path: "/", httpOnly: false, secure: false, sameSite: "Lax" }]);
  await page.route("**/api/v1/repositories", async (route) => {
    const request = route.request();
    if (request.method() === "GET") {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ repositories: registered ? [registered] : [] }) });
      return;
    }
    writes.push({ method: request.method(), url: request.url(), body: request.postDataJSON(), csrf: request.headers()["x-csrf-token"] });
    registered = repo;
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(registered) });
  });
  await page.route("**/api/v1/credentials", async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ credentials: [] }) });
  });
  await page.route("**/api/v1/repositories/7/check", async (route) => {
    const request = route.request();
    writes.push({ method: request.method(), url: request.url(), body: request.postDataJSON(), csrf: request.headers()["x-csrf-token"] });
    registered = { ...repo, eligibility: { status: "eligible", checked_sha: "a".repeat(40), checked_at: "2026-10-19T08:00:00Z", error: null } };
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ status: "eligible", checked_sha: "a".repeat(40), checked_at: "2026-10-19T08:00:00Z", profile_id: "python-pytest", profile_version: "1", package_count: 12, file_count: 8, source_bytes: 4096, cache_hit: false }) });
  });

  await page.goto("/");
  await page.getByRole("link", { name: "Repositories" }).click();
  await expect(page).toHaveURL(/\/settings\/repositories$/);
  await page.getByLabel("GitHub repository ID").fill("123456789");
  await page.getByLabel("Test profile").selectOption("node-npm");
  await page.getByRole("button", { name: "Register repository" }).click();
  await expect(page.getByRole("heading", { name: "octo/project" })).toBeVisible();
  expect(writes[0]).toMatchObject({ method: "POST", body: { github_repository_id: 123456789, profile_id: "node-npm" }, csrf: "repository-test-csrf" });
  expect(writes.some((write) => write.url.endsWith("/check"))).toBe(false);

  await page.getByLabel("Commit SHA").fill("a".repeat(40));
  await page.getByRole("button", { name: "Check eligibility" }).click();
  await expect(page.getByText("Eligible")).toBeVisible();
  expect(writes[1]).toMatchObject({ method: "POST", url: /\/api\/v1\/repositories\/7\/check$/, body: { commit_sha: "a".repeat(40) }, csrf: "repository-test-csrf" });
  await expect(page.getByText("a".repeat(40))).toBeVisible();
});

test("sign-in state and repository settings remain usable on a narrow viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.route("**/api/v1/repositories", async (route) => {
    await route.fulfill({ status: 401, contentType: "application/json", body: JSON.stringify({ code: "authentication_required", message: "Sign in first.", retryable: false, request_id: "fixture" }) });
  });
  await page.route("**/api/v1/credentials", async (route) => {
    await route.fulfill({ status: 401, contentType: "application/json", body: JSON.stringify({ code: "authentication_required", message: "Sign in first.", retryable: false, request_id: "fixture" }) });
  });

  await page.goto("/settings/repositories");
  await expect(page.getByRole("link", { name: "Sign in with GitHub" })).toHaveAttribute("href", "/auth/github/start");
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth);
  expect(overflow).toBe(false);
  await page.getByRole("link", { name: "Sign in with GitHub" }).focus();
  await expect(page.getByRole("link", { name: "Sign in with GitHub" })).toBeFocused();
});
