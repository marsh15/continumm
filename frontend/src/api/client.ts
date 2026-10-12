import type { components, paths } from "./generated";

export type ErrorBody = components["schemas"]["ErrorResponse"];
export type HealthBody = components["schemas"]["HealthResponse"];
export type RunSnapshot = paths["/api/v1/runs/{run_id}"]["get"]["responses"][200]["content"]["application/json"];
export type RunEventPage = paths["/api/v1/runs/{run_id}/events"]["get"]["responses"][200]["content"]["application/json"];
export type RunEvent = RunEventPage["events"][number];
export type HistoryReset = components["schemas"]["HistoryResetPayload"];
export type EventStreamError = components["schemas"]["EventStreamErrorPayload"];

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
): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      headers: { Accept: "application/json" },
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
