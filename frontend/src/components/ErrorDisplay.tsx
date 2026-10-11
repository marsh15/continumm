import { ApiError } from "../api/client";

type ErrorDisplayProps = {
  error: unknown;
  title?: string;
};

export function ErrorDisplay({ error, title = "Request failed" }: ErrorDisplayProps) {
  const body =
    error instanceof ApiError
      ? error.body
      : {
          code: "unexpected_error",
          message: "The service could not complete this request.",
          retryable: false,
          request_id: "unavailable",
        };

  return (
    <div className="error-display" role="alert" aria-label={title} aria-live="assertive">
      <span className="error-mark" aria-hidden="true">
        !
      </span>
      <div className="error-copy">
        <h3>{title}</h3>
        <p>{body.message}</p>
        <dl className="error-meta">
          <div>
            <dt>Code</dt>
            <dd>{body.code}</dd>
          </div>
          <div>
            <dt>Request ID</dt>
            <dd>{body.request_id}</dd>
          </div>
          <div>
            <dt>Retry</dt>
            <dd>{body.retryable ? "Try again later" : "Not available"}</dd>
          </div>
        </dl>
      </div>
    </div>
  );
}
