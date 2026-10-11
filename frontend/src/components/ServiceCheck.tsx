import type { UseQueryResult } from "@tanstack/react-query";
import type { HealthBody } from "../api/client";
import { ErrorDisplay } from "./ErrorDisplay";

type ServiceCheckProps = {
  label: string;
  query: UseQueryResult<HealthBody, Error>;
};

export function ServiceCheck({ label, query }: ServiceCheckProps) {
  return (
    <section className="service-check" aria-labelledby={`${label}-heading`}>
      <div className="service-check-heading">
        <h3 id={`${label}-heading`}>{label}</h3>
        {query.isFetching && !query.isPending ? (
          <span className="check-updating">Checking</span>
        ) : null}
      </div>
      {query.isPending ? (
        <p className="check-message" role="status">
          Checking the service…
        </p>
      ) : query.isError ? (
        <ErrorDisplay error={query.error} title={`${label} check failed`} />
      ) : (
        <p className="check-result">
          <span className="status-dot" aria-hidden="true" />
          <span>{query.data.status === "ok" ? "Available" : "Ready"}</span>
          <code>{query.data.status}</code>
        </p>
      )}
    </section>
  );
}
