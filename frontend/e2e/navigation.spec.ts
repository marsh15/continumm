import { expect, test } from "@playwright/test";

test("navigation moves focus and distinguishes inaccessible content", async ({ page }) => {
  await page.route("**/health/**", route => route.fulfill({
    status: 200, contentType: "application/json", body: JSON.stringify({ status: "ok", checks: null }),
  }));
  await page.route("**/api/v1/**", route => route.fulfill({
    status: 401, contentType: "application/json",
    body: JSON.stringify({ code: "authentication_required", message: "Sign in to continue.", retryable: false, request_id: "navigation-fixture" }),
  }));
  await page.goto("/");
  await page.getByRole("link", { name: "Repositories", exact: true }).click();
  await expect(page.getByRole("main")).toBeFocused();
  await expect(page).toHaveTitle("Repositories · Continumm");
  await expect(page.getByRole("link", { name: /Sign in/ })).toBeVisible();
  for (const width of [320, 390, 1280]) {
    await page.setViewportSize({ width, height: 900 });
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  }
  await page.setViewportSize({ width: 390, height: 900 });
  await page.screenshot({ path: "/tmp/continuum-t41-repositories.png", fullPage: true });
});
