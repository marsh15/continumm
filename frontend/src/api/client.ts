import type { components, paths } from "./generated";

export type ErrorBody = components["schemas"]["ErrorResponse"];
export type HealthBody = components["schemas"]["HealthResponse"];
export type RunSnapshot = paths["/api/v1/runs/{run_id}"]["get"]["responses"][200]["content"]["application/json"];
export type RunEventPage = paths["/api/v1/runs/{run_id}/events"]["get"]["responses"][200]["content"]["application/json"];
export type RunEvent = RunEventPage["events"][number];
export type HistoryReset = components["schemas"]["HistoryResetPayload"];
export type EventStreamError = components["schemas"]["EventStreamErrorPayload"];

export type ReviewPhaseEvidence = components["schemas"]["PhaseEvidence"];
export type ValidationEvidence = components["schemas"]["ValidationEvidence"];
export type ReviewSnapshot = paths["/api/v1/runs/{run_id}/review"]["get"]["responses"][200]["content"]["application/json"];
export type ApprovalDecision = components["schemas"]["ReviewDecisionRequest"]["decision"];
export type ApprovalDecisionResponse = paths["/api/v1/approvals/{approval_id}/decision"]["post"]["responses"][202]["content"]["application/json"];

export type ExternalEffectsResponse = paths["/api/v1/runs/{run_id}/external-effects"]["get"]["responses"][200]["content"]["application/json"];
export type ExternalEffect = ExternalEffectsResponse["effects"][number];
export type ExternalEffectKind = ExternalEffect["kind"];
export type ExternalEffectStatus = ExternalEffect["status"];
export type ExternalEffectEvidence = ExternalEffect["evidence"];
export type ExternalEffectReconcileResponse = paths["/api/v1/runs/{run_id}/external-effects/{effect_id}/reconcile"]["post"]["responses"][200]["content"]["application/json"];

export const RUN_EVENT_PAGE_SIZE = 50;

const decimalSequence = /^(0|[1-9][0-9]{0,18})$/;
const maximumSequence = 9_223_372_036_854_775_807n;
const runStatuses = [
  "queued",
  "running",
  "waiting_approval",
  "waiting_credentials",
  "waiting_capacity",
  "needs_attention",
  "succeeded",
  "failed",
  "cancelled",
  "expired",
] as const satisfies readonly RunSnapshot["status"][];
const eventDataKeys = [
  "attempt_id",
  "job_id",
  "step_id",
  "next_job_id",
  "failure_count",
  "generation",
  "cancelled_jobs",
  "invalidated_attempts",
  "retry_at",
  "capacity_resume_at",
  "target_run_status",
  "status",
  "failure_class",
  "replay_policy",
  "retry_scheduled",
] as const satisfies readonly (keyof RunEvent["data"])[];
const failureClasses = [
  "invalid_input",
  "missing_credential",
  "transient_read",
  "unsupported_environment",
  "resource_exhaustion",
  "model_response_lost",
  "unknown_effect",
  "remote_conflict",
  "corrupt_artifact",
] as const satisfies readonly NonNullable<RunEvent["data"]["failure_class"]>[];

function isDecimalSequence(value: unknown): value is string {
  return (
    typeof value === "string" &&
    decimalSequence.test(value) &&
    BigInt(value) <= maximumSequence
  );
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly body: ErrorBody,
  ) {
    super(body.message);
    this.name = "ApiError";
  }
}

function isErrorBody(value: unknown): value is ErrorBody {
  return (
    typeof value === "object" &&
    value !== null &&
    "code" in value &&
    typeof value.code === "string" &&
    "message" in value &&
    typeof value.message === "string" &&
    "retryable" in value &&
    typeof value.retryable === "boolean" &&
    "request_id" in value &&
    typeof value.request_id === "string"
  );
}

async function requestJson<T>(
  path: string,
  isResponse: (value: unknown) => value is T,
  signal?: AbortSignal,
  init?: Pick<RequestInit, "method" | "headers" | "body">,
): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      ...init,
      headers: { Accept: "application/json", ...init?.headers },
      credentials: "same-origin",
      signal,
    });
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") throw error;
    throw new ApiError(0, {
      code: "network_error",
      message: "Could not reach the service API. Check the connection and try again.",
      retryable: true,
      request_id: "unavailable",
    });
  }

  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    payload = undefined;
  }

  if (!response.ok) {
    const requestId = response.headers.get("X-Request-ID") ?? "unavailable";
    const body = isErrorBody(payload)
      ? payload
      : {
          code: "invalid_error_response",
          message: "The service returned an unreadable error response.",
          retryable: response.status >= 500,
          request_id: requestId,
        } satisfies ErrorBody;
    throw new ApiError(response.status, body);
  }

  if (!isResponse(payload)) {
    throw new ApiError(response.status, {
      code: "invalid_response",
      message: "The service returned an unreadable API response.",
      retryable: response.status >= 500,
      request_id: response.headers.get("X-Request-ID") ?? "unavailable",
    });
  }

  return payload;
}

function isHealthBody(value: unknown): value is HealthBody {
  if (typeof value !== "object" || value === null || !("status" in value)) {
    return false;
  }
  if (value.status !== "ok" && value.status !== "ready") {
    return false;
  }
  if (!("checks" in value) || value.checks === undefined || value.checks === null) {
    return true;
  }
  return (
    typeof value.checks === "object" &&
    !Array.isArray(value.checks) &&
    Object.values(value.checks).every((check) => check === "ok")
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isReviewPhaseEvidence(value: unknown): value is ReviewPhaseEvidence {
  if (!isRecord(value)) return false;
  return (
    (value.phase === "baseline" || value.phase === "candidate") &&
    ["passed", "test_failed", "unsupported", "setup_failed", "unknown", "not_run"].includes(String(value.outcome)) &&
    typeof value.artifact_id === "string" &&
    typeof value.base_commit_sha === "string" &&
    typeof value.baseline_archive_sha256 === "string" &&
    typeof value.baseline_archive_tree_sha256 === "string" &&
    typeof value.baseline_git_tree_sha256 === "string" &&
    typeof value.command_id === "string" &&
    typeof value.profile_id === "string" &&
    typeof value.profile_version === "string" &&
    typeof value.repository_id === "number" &&
    typeof value.source_archive_sha256 === "string" &&
    typeof value.source_tree_sha256 === "string" &&
    typeof value.package_count === "number" &&
    ((value.test_exit_code === undefined) || value.test_exit_code === null || typeof value.test_exit_code === "number") &&
    ["allocation_id", "allocation_status", "error_code", "image", "npm_version", "runtime_version"].every(
      (key) => value[key] === undefined || value[key] === null || typeof value[key] === "string",
    ) &&
    Array.isArray(value.commands) &&
    value.commands.every(
      (command) =>
        isRecord(command) &&
        typeof command.phase === "string" &&
        ((command.exit_code === undefined) || command.exit_code === null || typeof command.exit_code === "number") &&
        typeof command.stdout_bytes === "number" &&
        typeof command.stderr_bytes === "number" &&
        typeof command.stdout_sha256 === "string" &&
        typeof command.stderr_sha256 === "string",
    )
  );
}

export function isReviewSnapshot(value: unknown): value is ReviewSnapshot {
  if (!isRecord(value) || !isRecord(value.diff) || !isRecord(value.approval) || !isRecord(value.limits) || !isRecord(value.revisions)) return false;
  const evidence = value.evidence;
  const limits = value.limits;
  const limitNames = ["requests", "input_tokens", "output_tokens", "sandbox_seconds"] as const;
  return (
    typeof value.run_id === "string" &&
    typeof value.revision === "number" &&
    typeof value.status === "string" &&
    typeof value.diff.text === "string" &&
    Array.isArray(value.diff.changed_paths) &&
    value.diff.changed_paths.every((path) => typeof path === "string") &&
    typeof value.diff.truncated === "boolean" &&
    isRecord(evidence) &&
    evidence.evidence_version === 1 &&
    ["passed", "passed_with_baseline_failure", "candidate_test_failed", "baseline_unsupported", "baseline_setup_failed", "baseline_unknown", "candidate_unsupported", "candidate_setup_failed", "candidate_unknown"].includes(String(evidence.classification)) &&
    typeof evidence.baseline_artifact_id === "string" &&
    typeof evidence.candidate_archive_sha256 === "string" &&
    typeof evidence.candidate_source_tree_sha256 === "string" &&
    typeof evidence.candidate_lineage_sha256 === "string" &&
    typeof evidence.changed_path_count === "number" &&
    evidence.regression_on_original === "absent" &&
    isReviewPhaseEvidence(evidence.baseline) &&
    isReviewPhaseEvidence(evidence.candidate) &&
    isRecord(value.manifest) &&
    typeof value.manifest_sha256 === "string" &&
    /^[0-9a-f]{64}$/.test(value.manifest_sha256) &&
    typeof value.approval.approval_id === "string" &&
    value.approval.manifest_sha256 === value.manifest_sha256 &&
    typeof value.approval.expires_at === "string" &&
    ["pending", "approved", "rejected", "changes", "expired"].includes(String(value.approval.status)) &&
    limitNames.every((name) => {
      const metric = limits[name];
      return isRecord(metric) && typeof metric.used === "number" && typeof metric.limit === "number";
    }) &&
    typeof value.revisions.used === "number" &&
    value.revisions.limit === 2 &&
    typeof value.revisions.remaining === "number" &&
    value.revisions.used === value.revision &&
    value.revisions.remaining === value.revisions.limit - value.revisions.used &&
    typeof value.decision_eligible === "boolean"
  );
}

function isApprovalDecisionResponse(value: unknown): value is ApprovalDecisionResponse {
  return (
    isRecord(value) &&
    typeof value.approval_id === "string" &&
    typeof value.run_id === "string" &&
    typeof value.revision === "number" &&
    typeof value.status === "string" &&
    typeof value.manifest_sha256 === "string"
  );
}

const externalEffectKinds: readonly ExternalEffectKind[] = [
  "external_operation",
  "dispatch_intent",
  "resource_reservation",
  "cleanup_intent",
];
const externalEffectStatuses: readonly ExternalEffectStatus[] = [
  "prepared",
  "dispatching",
  "unknown",
  "conflict",
  "reserved",
  "pending",
];
const reconcileStatuses: readonly ExternalEffectReconcileResponse["status"][] = [
  "adopted",
  "closed",
  "merged",
  "unknown",
  "conflict",
  "unavailable",
  "not_dispatched",
  "not_started",
  "definitely_failed",
];

function isGithubPullRequestUrl(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^https:\/\/github\.com\/[A-Za-z0-9-]+\/[A-Za-z0-9_.-]+\/pull\/[1-9][0-9]*$/.test(value)
  );
}

function isExternalEffectEvidence(value: unknown): value is ExternalEffectEvidence {
  if (!isRecord(value)) return false;
  return (
    ["pr_number", "pr_id"].every(
      (key) => value[key] === undefined || value[key] === null || (typeof value[key] === "number" && Number.isInteger(value[key]) && value[key] > 0),
    ) &&
    (value.pr_url === undefined || value.pr_url === null || isGithubPullRequestUrl(value.pr_url)) &&
    (value.pr_state === undefined || value.pr_state === null || value.pr_state === "open" || value.pr_state === "closed" || value.pr_state === "merged")
  );
}

function isExternalEffect(value: unknown): value is ExternalEffect {
  return (
    isRecord(value) &&
    externalEffectKinds.includes(value.kind as ExternalEffectKind) &&
    typeof value.effect_id === "string" &&
    /^[0-9a-f-]{36}$/i.test(value.effect_id) &&
    externalEffectStatuses.includes(value.status as ExternalEffectStatus) &&
    typeof value.created_at === "string" &&
    !Number.isNaN(Date.parse(value.created_at)) &&
    (value.updated_at === undefined || value.updated_at === null || (typeof value.updated_at === "string" && !Number.isNaN(Date.parse(value.updated_at)))) &&
    isExternalEffectEvidence(value.evidence) &&
    typeof value.can_reconcile === "boolean"
  );
}

function isExternalEffectsResponse(value: unknown): value is ExternalEffectsResponse {
  return (
    isRecord(value) &&
    typeof value.run_id === "string" &&
    Array.isArray(value.effects) &&
    value.effects.every(isExternalEffect) &&
    typeof value.truncated === "boolean"
  );
}

function isExternalEffectReconcileResponse(value: unknown): value is ExternalEffectReconcileResponse {
  return (
    isRecord(value) &&
    typeof value.effect_id === "string" &&
    /^[0-9a-f-]{36}$/i.test(value.effect_id) &&
    reconcileStatuses.includes(value.status as ExternalEffectReconcileResponse["status"]) &&
    isExternalEffectEvidence(value)
  );
}

function isRunSnapshot(value: unknown): value is RunSnapshot {
  return (
    isRecord(value) &&
    typeof value.run_id === "string" &&
    runStatuses.some((status) => status === value.status) &&
    typeof value.revision === "number" &&
    Number.isInteger(value.revision) &&
    typeof value.created_at === "string" &&
    (value.deadline_at === null || typeof value.deadline_at === "string") &&
    isDecimalSequence(value.event_watermark)
  );
}

function isRunEventData(value: unknown): value is RunEvent["data"] {
  if (!isRecord(value)) return false;
  const identifiers = new Set(["attempt_id", "job_id", "step_id", "next_job_id"]);
  const counters = new Set(["failure_count", "generation", "cancelled_jobs", "invalidated_attempts"]);
  const dates = new Set(["retry_at", "capacity_resume_at"]);
  for (const [key, field] of Object.entries(value)) {
    if (!eventDataKeys.includes(key as (typeof eventDataKeys)[number])) return false;
    if (field === null) continue;
    if (key === "retry_scheduled") {
      if (typeof field !== "boolean") return false;
    } else if (identifiers.has(key)) {
      if (typeof field !== "string" || !/^[0-9a-f-]{36}$/i.test(field)) return false;
    } else if (counters.has(key)) {
      if (!isDecimalSequence(field)) return false;
    } else if (dates.has(key)) {
      if (typeof field !== "string" || Number.isNaN(Date.parse(field))) return false;
    } else if (key === "target_run_status" || key === "status") {
      if (!runStatuses.some((status) => status === field)) return false;
    } else if (key === "failure_class") {
      if (!failureClasses.some((failureClass) => failureClass === field)) return false;
    } else if (key === "replay_policy") {
      if (field !== "replay_safe" && field !== "attention_on_unknown") return false;
    } else {
      return false;
    }
  }
  return true;
}

export function isRunEvent(value: unknown): value is RunEvent {
  return (
    isRecord(value) &&
    typeof value.event_id === "string" &&
    typeof value.run_id === "string" &&
    isDecimalSequence(value.sequence) &&
    typeof value.type === "string" &&
    typeof value.occurred_at === "string" &&
    isRunEventData(value.data)
  );
}

function isRunEventPage(value: unknown): value is RunEventPage {
  return (
    isRecord(value) &&
    typeof value.run_id === "string" &&
    isDecimalSequence(value.after_sequence) &&
    Array.isArray(value.events) &&
    value.events.every(isRunEvent) &&
    (value.next_cursor === null ||
      isDecimalSequence(value.next_cursor)) &&
    isDecimalSequence(value.high_watermark) &&
    typeof value.history_gap === "boolean"
  );
}

export function isHistoryReset(value: unknown): value is HistoryReset {
  return (
    isRecord(value) &&
    (value.reason === "cursor_ahead" || value.reason === "history_gap") &&
    isDecimalSequence(value.high_watermark)
  );
}

export function isEventStreamError(value: unknown): value is EventStreamError {
  return (
    isRecord(value) &&
    (value.code === "authentication_required" || value.code === "event_stream_unavailable") &&
    typeof value.retryable === "boolean"
  );
}

type LiveResponse =
  paths["/health/live"]["get"]["responses"][200]["content"]["application/json"];
type ReadyResponse =
  paths["/health/ready"]["get"]["responses"][200]["content"]["application/json"];

function isLiveResponse(value: unknown): value is LiveResponse {
  return isHealthBody(value) && value.status === "ok";
}

function isReadyResponse(value: unknown): value is ReadyResponse {
  return isHealthBody(value) && value.status === "ready";
}

export function getLiveness(): Promise<LiveResponse> {
  return requestJson<LiveResponse>("/health/live", isLiveResponse);
}

export function getReadiness(): Promise<ReadyResponse> {
  return requestJson<ReadyResponse>("/health/ready", isReadyResponse);
}

export function getRunSnapshot(runId: string, signal?: AbortSignal): Promise<RunSnapshot> {
  return requestJson<RunSnapshot>(
    `/api/v1/runs/${encodeURIComponent(runId)}`,
    isRunSnapshot,
    signal,
  );
}

export function getRunReview(runId: string, signal?: AbortSignal): Promise<ReviewSnapshot> {
  return requestJson<ReviewSnapshot>(
    `/api/v1/runs/${encodeURIComponent(runId)}/review`,
    isReviewSnapshot,
    signal,
  );
}

export function getRunExternalEffects(runId: string, signal?: AbortSignal): Promise<ExternalEffectsResponse> {
  return requestJson<ExternalEffectsResponse>(
    `/api/v1/runs/${encodeURIComponent(runId)}/external-effects`,
    isExternalEffectsResponse,
    signal,
  );
}

export function reconcileRunExternalEffect(
  runId: string,
  effectId: string,
): Promise<ExternalEffectReconcileResponse> {
  const csrfCookie = typeof document === "undefined"
    ? undefined
    : document.cookie.split(";").map((part) => part.trim()).find((part) => part.startsWith("csrf_token="))?.slice("csrf_token=".length);
  return requestJson<ExternalEffectReconcileResponse>(
    `/api/v1/runs/${encodeURIComponent(runId)}/external-effects/${encodeURIComponent(effectId)}/reconcile`,
    isExternalEffectReconcileResponse,
    undefined,
    {
      method: "POST",
      headers: {
        ...(csrfCookie ? { "X-CSRF-Token": decodeURIComponent(csrfCookie) } : {}),
      },
    },
  );
}

export function submitApprovalDecision(
  approvalId: string,
  decision: ApprovalDecision,
  manifestSha256: string,
  idempotencyKey: string,
  feedback?: string,
): Promise<ApprovalDecisionResponse> {
  const body: { decision: ApprovalDecision; manifest_sha256: string; feedback?: string } = {
    decision,
    manifest_sha256: manifestSha256,
  };
  if (feedback !== undefined) body.feedback = feedback;
  const csrfCookie = typeof document === "undefined"
    ? undefined
    : document.cookie.split(";").map((part) => part.trim()).find((part) => part.startsWith("csrf_token="))?.slice("csrf_token=".length);
  return requestJson<ApprovalDecisionResponse>(
    `/api/v1/approvals/${encodeURIComponent(approvalId)}/decision`,
    isApprovalDecisionResponse,
    undefined,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey,
        ...(csrfCookie ? { "X-CSRF-Token": decodeURIComponent(csrfCookie) } : {}),
      },
      body: JSON.stringify(body),
    },
  );
}

export function getRunEventPage(
  runId: string,
  after: string,
  signal?: AbortSignal,
): Promise<RunEventPage> {
  const query = new URLSearchParams({ after, limit: String(RUN_EVENT_PAGE_SIZE) });
  return requestJson<RunEventPage>(
    `/api/v1/runs/${encodeURIComponent(runId)}/events?${query.toString()}`,
    isRunEventPage,
    signal,
  );
}
