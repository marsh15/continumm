import { expect, test, type Page, type Request } from "@playwright/test";

const runId = "f48f3a9a-5f95-4efa-8a67-b6eecafc9120";
const effectId = "780f5a75-8f6f-499a-a15f-a4bb15dc6318";

async function installEffectRoutes(page: Page) {
  await page.context().addCookies([{
    name: "csrf_token",
    value: "browser-effect-csrf",
    domain: "127.0.0.1",
    path: "/",
    httpOnly: false,
    secure: false,
    sameSite: "Lax",
  }]);

  let adopted = false;
  let request: Request | undefined;
  await page.route("**/api/v1/runs/*/events/stream**", (route) => route.abort());
  await page.route("**/api/v1/runs/*/events?**", (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      run_id: runId,
      after_sequence: "0",
      events: [],
      next_cursor: null,
      high_watermark: "0",
      history_gap: false,
    }),
  }));
  await page.route(`**/api/v1/runs/${runId}`, (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      run_id: runId,
      status: "cancelled",
      revision: 4,
      created_at: "2026-10-12T08:00:00Z",
      deadline_at: null,
      event_watermark: "0",
    }),
  }));
  await page.route(`**/api/v1/runs/${runId}/external-effects`, (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      run_id: runId,
      truncated: false,
      effects: adopted ? [] : [{
        kind: "external_operation",
        effect_id: effectId,
        status: "unknown",
        created_at: "2026-10-12T08:02:00Z",
        updated_at: "2026-10-12T08:04:00Z",
        evidence: {
          pr_number: null,
          pr_id: null,
          pr_url: null,
          pr_state: null,
          details: "<script>window.pwned = true</script>",
          body: "private pull request body",
        },
        can_reconcile: true,
      }],
    }),
  }));
  await page.route(`**/api/v1/runs/${runId}/external-effects/${effectId}/reconcile`, async (route) => {
    request = route.request();
    adopted = true;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        effect_id: effectId,
        status: "adopted",
        pr_number: 42,
        pr_id: 1234,
        pr_url: "https://github.com/acme/repo/pull/42",
        pr_state: "open",
      }),
    });
  });
  return { getRequest: () => request };
}

test("shows canceled run effects and records a matching PR as evidence", async ({ page }) => {
  const { getRequest } = await installEffectRoutes(page);
  await page.goto(`/runs/${runId}`);

  await expect(page.getByText("Cancelled")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Unresolved effects" })).toBeVisible();
  await expect(page.getByText("Outcome unknown")).toBeVisible();
  await expect(page.getByRole("button", { name: "Investigate outcome" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Review candidate" })).toHaveCount(0);
  await expect(page.getByText("private pull request body")).toHaveCount(0);
  await expect(page.getByText(/window\.pwned/)).toHaveCount(0);
  expect(await page.locator("[onerror]").count()).toBe(0);
  expect(await page.evaluate(() => (window as Window & { pwned?: boolean }).pwned)).toBeUndefined();
  await page.screenshot({ path: "/tmp/continuum-effects-desktop.png", fullPage: true });

  await page.getByRole("button", { name: "Investigate outcome" }).click();
  await expect(page.getByRole("link", { name: "View pull request #42" })).toHaveAttribute(
    "href",
    "https://github.com/acme/repo/pull/42",
  );
  await expect(page.getByText("Cancelled")).toBeVisible();
  const request = getRequest();
  expect(request?.method()).toBe("POST");
  expect(request?.headers()["x-csrf-token"]).toBe("browser-effect-csrf");
  expect(request?.headers().origin).toContain("127.0.0.1");
  expect(request?.postData()).toBeNull();
});

test("does not offer investigation for other effect classes or overclaim a complete list", async ({ page }) => {
  await installEffectRoutes(page);
  await page.unroute(`**/api/v1/runs/${runId}/external-effects`);
  await page.route(`**/api/v1/runs/${runId}/external-effects`, (route) => route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({
      run_id: runId,
      truncated: true,
      effects: [{
        kind: "dispatch_intent",
        effect_id: effectId,
        status: "unknown",
        created_at: "2026-10-12T08:02:00Z",
        evidence: {},
        can_reconcile: false,
      }],
    }),
  }));
  await page.goto(`/runs/${runId}`);
  await expect(page.getByText("The list is incomplete. More unresolved effects may exist.")).toBeVisible();
  await expect(page.getByRole("button", { name: "Investigate outcome" })).toHaveCount(0);
  await expect(page.getByText("This effect needs service-side review.")).toBeVisible();
});

test("keeps the effects panel usable on mobile with keyboard controls", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await installEffectRoutes(page);
  await page.goto(`/runs/${runId}`);
  await expect(page.getByRole("button", { name: "Investigate outcome" })).toBeVisible();
  await page.screenshot({ path: "/tmp/continuum-effects-mobile.png", fullPage: true });
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)).toBe(false);
  await page.getByRole("button", { name: "Investigate outcome" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("link", { name: "View pull request #42" })).toBeVisible();
});
