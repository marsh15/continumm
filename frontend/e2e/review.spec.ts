import { expect, test, type Page, type Request } from "@playwright/test";

const runId = "f48f3a9a-5f95-4efa-8a67-b6eecafc9120";
const digest = "a".repeat(64);

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

function review(revision = 0, used = 0) {
  return {
    run_id: runId,
    revision,
    status: "waiting_approval",
    diff: {
      text: "diff --git a/src/one.ts b/src/one.ts\n--- a/src/one.ts\n+++ b/src/one.ts\n@@ -1 +1 @@\n-old\n+new\ndiff --git a/src/two.ts b/src/two.ts\n--- a/src/two.ts\n+++ b/src/two.ts\n@@ -1 +1 @@\n-old-two\n+new-two\n",
      changed_paths: ["src/one.ts", "src/two.ts"],
      truncated: false,
    },
    evidence: {
      evidence_version: 1,
      classification: "passed",
      baseline_artifact_id: "base-artifact",
      candidate_archive_sha256: "d".repeat(64),
      candidate_source_tree_sha256: "e".repeat(64),
      candidate_lineage_sha256: "f".repeat(64),
      changed_path_count: 2,
      regression_on_original: "absent",
      baseline: phase("baseline"),
      candidate: phase("candidate"),
    },
    manifest: { manifest_version: 1, branch: "continuum/task-123", candidate: { changed_paths: ["src/one.ts", "src/two.ts"] } },
    manifest_sha256: digest,
    approval: { approval_id: `approval-${revision}`, manifest_sha256: digest, expires_at: "2099-12-31T23:59:59Z", status: "pending" },
    limits: {
      requests: { used: 5, limit: 30 },
      input_tokens: { used: 17000, limit: 120000 },
      output_tokens: { used: 3200, limit: 30000 },
      sandbox_seconds: { used: 120, limit: 900 },
    },
    revisions: { used, limit: 2, remaining: 2 - used },
    decision_eligible: true,
  };
}

function snapshot(revision = 3, status = "waiting_approval") {
  return { run_id: runId, status, revision, created_at: "2026-10-12T08:00:00Z", deadline_at: "2099-12-31T23:59:59Z", event_watermark: "0" };
}

async function installReviewRoutes(page: Page, options: {
  initialRevision?: number;
  expired?: boolean;
  hostileContent?: boolean;
  reviewError?: number;
  runStatus?: string;
  decision?: (request: Request, state: { revision: number; changes: number }) => Promise<{ status: number; body: unknown }>;
} = {}) {
  const state = { revision: options.initialRevision ?? 0, changes: 0 };
  const decisions: Array<{ body: Record<string, unknown>; key: string | undefined; csrf: string | undefined }> = [];
  await page.context().addCookies([{ name: "csrf_token", value: "browser-test-csrf", domain: "127.0.0.1", path: "/", httpOnly: false, secure: false, sameSite: "Lax" }]);
  await page.route("**/api/v1/runs/*/review", async (route) => {
    if (options.reviewError) {
      await route.fulfill({ status: options.reviewError, contentType: "application/json", body: JSON.stringify({ code: "review_unavailable", message: "Review is unavailable.", retryable: false, request_id: "fixture" }) });
      return;
    }
    const payload = review(state.revision, state.changes);
    if (options.expired) payload.approval = { ...payload.approval, expires_at: "2020-01-01T00:00:00Z", status: "expired" };
    if (options.hostileContent) {
      payload.diff.text = "<script>window.pwned = true</script>";
      payload.manifest.branch = "<img src=x onerror=alert(1)>";
    }
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(payload) });
  });
  await page.route(`**/api/v1/runs/${runId}`, async (route) => {
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(snapshot(state.revision, options.runStatus)) });
  });
  await page.route("**/api/v1/approvals/*/decision", async (route) => {
    const request = route.request();
    const body = request.postDataJSON() as Record<string, unknown>;
    decisions.push({ body, key: request.headers()["idempotency-key"], csrf: request.headers()["x-csrf-token"] });
    const response = options.decision ? await options.decision(request, state) : { status: 202, body: { approval_id: `approval-${state.revision}`, run_id: runId, revision: state.revision + (body.decision === "changes" ? 1 : 0), status: body.decision === "reject" ? "rejected" : body.decision === "changes" ? "changes" : "approved", manifest_sha256: digest } };
    if (response.status === 202 && body.decision === "changes") {
      state.changes += 1;
      state.revision += 1;
    }
    await route.fulfill({ status: response.status, contentType: "application/json", body: JSON.stringify(response.body) });
  });
  return { decisions, state };
}

test("approve, reject, and request changes use the exact manifest and CSRF boundary", async ({ page }) => {
  const { decisions } = await installReviewRoutes(page);
  await page.goto(`/runs/${runId}/review`);
  await expect(page.getByRole("heading", { name: "Review the proposed changes" })).toBeVisible();
  await page.screenshot({ path: "/tmp/continuum-review-desktop.png", fullPage: true });
  await expect(page.getByText(digest)).toBeVisible();
  await page.getByRole("button", { name: "Approve candidate" }).click();
  await expect(page.getByRole("status").filter({ hasText: "Decision recorded" })).toBeVisible();
  expect(decisions[0]).toMatchObject({ body: { decision: "approve", manifest_sha256: digest }, csrf: "browser-test-csrf" });
  expect(decisions[0].key).toBeTruthy();

  await page.reload();
  await page.getByRole("button", { name: "Reject candidate" }).click();
  await expect(page.getByRole("status").filter({ hasText: "Decision recorded" })).toBeVisible();
  expect(decisions[1]?.body.decision).toBe("reject");

  await page.reload();
  await page.getByLabel(/Feedback for the next revision/).fill("Update the error copy in src/one.ts.");
  await page.getByRole("button", { name: "Request changes" }).click();
  await expect(page.getByRole("status").filter({ hasText: "Decision recorded" })).toBeVisible();
  expect(decisions[2]?.body).toMatchObject({ decision: "changes", feedback: "Update the error copy in src/one.ts.", manifest_sha256: digest });
});

test("permits two revision rounds and disables a third request", async ({ page }) => {
  const { state } = await installReviewRoutes(page);
  await page.goto(`/runs/${runId}/review`);
  const feedback = page.getByLabel(/Feedback for the next revision/);
  await feedback.fill("First correction.");
  await page.getByRole("button", { name: "Request changes" }).click();
  await expect(page.getByText("Candidate review · Revision 1 of 2")).toBeVisible();
  await feedback.fill("Second correction.");
  await page.getByRole("button", { name: "Request changes" }).click();
  await expect(page.getByText("Candidate review · Revision 2 of 2")).toBeVisible();
  await expect(page.getByText("The two revision rounds have been used.")).toBeVisible();
  await expect(page.getByRole("button", { name: "Request changes" })).toBeDisabled();
  expect(state.changes).toBe(2);
});

test("retries a possibly committed request with the same idempotency key", async ({ page }) => {
  let attempts = 0;
  const { decisions } = await installReviewRoutes(page, {
    decision: async () => {
      attempts += 1;
      return attempts === 1
        ? { status: 503, body: { code: "service_unavailable", message: "Retry the decision.", retryable: true, request_id: "fixture" } }
        : { status: 202, body: { approval_id: "approval-0", run_id: runId, revision: 0, status: "approved", manifest_sha256: digest } };
    },
  });
  await page.goto(`/runs/${runId}/review`);
  await page.getByRole("button", { name: "Approve candidate" }).click();
  await page.getByRole("button", { name: "Retry the same decision" }).click();
  await expect(page.getByRole("status").filter({ hasText: "Decision recorded" })).toBeVisible();
  expect(decisions).toHaveLength(2);
  expect(decisions[0]?.key).toBe(decisions[1]?.key);
  expect(decisions[0]?.body).toEqual(decisions[1]?.body);
});

test("disables expired and stale reviews and shows read errors", async ({ page }) => {
  await installReviewRoutes(page, { expired: true });
  await page.goto(`/runs/${runId}/review`);
  await expect(page.getByText(/approval has expired/i)).toBeVisible();
  await expect(page.getByRole("button", { name: "Approve candidate" })).toBeDisabled();

  await installReviewRoutes(page, { runStatus: "running" });
  await page.goto(`/runs/${runId}/review`);
  await expect(page.getByText(/no longer matches the current run revision/i)).toBeVisible();
  await expect(page.getByRole("button", { name: "Reject candidate" })).toBeDisabled();

  await installReviewRoutes(page, { reviewError: 503 });
  await page.goto(`/runs/${runId}/review`);
  await expect(page.getByRole("alert", { name: "Could not load the current review" })).toBeVisible();
});

test("keeps the mobile review keyboard-operable without horizontal page overflow", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await installReviewRoutes(page);
  await page.goto(`/runs/${runId}/review`);
  await page.screenshot({ path: "/tmp/continuum-review-mobile.png", fullPage: true });
  await expect(page.getByRole("navigation", { name: "Changed files" })).toBeVisible();
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth);
  expect(overflow).toBe(false);
  await page.getByRole("button", { name: /src\/two.ts/ }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("button", { name: /src\/two.ts/ })).toHaveAttribute("aria-pressed", "true");
  await page.getByLabel(/Feedback for the next revision/).fill("Keyboard test feedback");
  await page.getByRole("button", { name: "Request changes" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("status").filter({ hasText: "Decision recorded" })).toBeVisible();
});

test("renders repository diff and manifest content as inert text", async ({ page }) => {
  await installReviewRoutes(page, { hostileContent: true });
  await page.goto(`/runs/${runId}/review`);
  await expect(page.getByText("<script>window.pwned = true</script>")).toBeVisible();
  await expect(page.getByText("<img src=x onerror=alert(1)>")).toBeVisible();
  expect(await page.locator("[onerror]").count()).toBe(0);
  expect(await page.evaluate(() => (window as Window & { pwned?: boolean }).pwned)).toBeUndefined();
});
