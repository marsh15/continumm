import type { components, paths } from "./generated";

export type ErrorBody = components["schemas"]["ErrorResponse"];
export type HealthBody = components["schemas"]["HealthResponse"];

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
): Promise<T> {
  let response: Response;
  try {
    response = await fetch(path, {
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    });
  } catch {
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
      message: "The service returned an unreadable status response.",
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
