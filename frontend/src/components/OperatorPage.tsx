import { useQuery } from "@tanstack/react-query";
import {
  getOperatorAudit,
  getOperatorOverview,
  getPendingInbox,
  getSuspendedAccounts,
  getUncertainAllocations,
  getUnknownEffects,
  OperatorApiError,
} from "../api/operator";

const refreshInterval = 30_000;

function relativeTime(value: string | null): string {
  if (value === null) return "None";
  const time = Date.parse(value);
  if (Number.isNaN(time)) return "Unavailable";
  const minutes = Math.max(0, Math.floor((Date.now() - time) / 60_000));
  if (minutes < 1) return "Just now";
  if (minutes < 60) return `${minutes} min ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours} hr ago`;
  return `${Math.floor(hours / 24)} days ago`;
}

function Restricted({ signedIn }: { signedIn: boolean }) {
  return (
    <main id="main-content" tabIndex={-1} className="page-shell operator-page">
      <p className="eyebrow">Operations</p>
      <h1 id="page-title">Access is restricted</h1>
      <p className="operator-copy">
        {signedIn
          ? "This view is available to allowlisted operator accounts."
          : "Sign in with an allowlisted GitHub account to view operational metadata."}
      </p>
    </main>
  );
}

export function OperatorPage() {
  const overview = useQuery({
    queryKey: ["operator", "overview"],
    queryFn: getOperatorOverview,
    retry: false,
    refetchInterval: refreshInterval,
  });
  const allowed = overview.isSuccess;
  const effects = useQuery({ queryKey: ["operator", "unknown-effects"], queryFn: getUnknownEffects, enabled: allowed, retry: false, refetchInterval: refreshInterval });
  const allocations = useQuery({ queryKey: ["operator", "allocations"], queryFn: getUncertainAllocations, enabled: allowed, retry: false, refetchInterval: refreshInterval });
  const inbox = useQuery({ queryKey: ["operator", "inbox"], queryFn: getPendingInbox, enabled: allowed, retry: false, refetchInterval: refreshInterval });
  const accounts = useQuery({ queryKey: ["operator", "accounts"], queryFn: getSuspendedAccounts, enabled: allowed, retry: false, refetchInterval: refreshInterval });
  const audit = useQuery({ queryKey: ["operator", "audit"], queryFn: getOperatorAudit, enabled: allowed, retry: false, refetchInterval: refreshInterval });

  if (overview.isError) {
    const error = overview.error;
    if (error instanceof OperatorApiError && error.status === 401) return <Restricted signedIn={false} />;
    if (error instanceof OperatorApiError && (error.status === 403 || error.status === 404)) {
      return <Restricted signedIn />;
    }
    return (
      <main id="main-content" tabIndex={-1} className="page-shell operator-page">
        <p className="eyebrow">Operations</p>
        <h1 id="page-title">Operational data is unavailable</h1>
        <p className="operator-copy">The metadata service could not be reached. Retry when the database is available.</p>
        <button className="text-button" type="button" onClick={() => void overview.refetch()}>Retry</button>
      </main>
    );
  }

  if (!overview.data) {
    return (
      <main id="main-content" tabIndex={-1} className="page-shell operator-page" aria-busy="true">
        <p className="eyebrow">Operations</p>
        <h1 id="page-title">Loading operational metadata</h1>
      </main>
    );
  }

  const data = overview.data;
  const storagePercent = data.artifact_capacity_limit_bytes > 0
    ? Math.min(100, Math.round(data.artifact_charged_bytes / data.artifact_capacity_limit_bytes * 100))
    : 0;
  const listsLoading = effects.isLoading || allocations.isLoading || inbox.isLoading || accounts.isLoading || audit.isLoading;

  return (
    <main id="main-content" tabIndex={-1} className="page-shell operator-page">
      <section className="operator-heading" aria-labelledby="page-title">
        <p className="eyebrow">Restricted operations view</p>
        <h1 id="page-title">Runtime status</h1>
        <p className="operator-copy">Metadata for admission, unresolved external work, inbox lag, and storage. This view contains no source, comments, credentials, or effect payloads.</p>
        <p className={`operator-stop-state ${data.admissions_stopped ? "is-stopped" : "is-running"}`}>
          Admissions {data.admissions_stopped ? "stopped" : "open"}
        </p>
      </section>

      <section className="operator-metrics" aria-label="Runtime counters">
        <Metric label="Active runs" value={data.active_runs} />
        <Metric label="Unknown effects" value={data.unknown_effects} />
        <Metric label="Uncertain allocations" value={data.uncertain_allocations} />
        <Metric label="Pending deliveries" value={data.pending_inbox_deliveries} detail={`Oldest ${relativeTime(data.oldest_pending_inbox_at)}`} />
        <Metric label="Suspended accounts" value={data.suspended_accounts} />
        <Metric label="Artifact capacity" value={`${storagePercent}%`} detail={`${formatBytes(data.artifact_charged_bytes)} of ${formatBytes(data.artifact_capacity_limit_bytes)}`} />
      </section>

      <section className="operator-panels" aria-label="Operational details">
        <MetadataList title="Unknown external effects" failed={effects.isError} busy={effects.isFetching} rows={(effects.data ?? []).map((item) => ({
          id: item.effect_id,
          state: item.status,
          detail: `Run ${item.run_id.slice(0, 8)} · updated ${relativeTime(item.updated_at)}`,
        }))} />
        <MetadataList title="Uncertain allocations" failed={allocations.isError} busy={allocations.isFetching} rows={(allocations.data ?? []).map((item) => ({
          id: item.allocation_id,
          state: item.status,
          detail: `${item.provider_name} · last observed ${relativeTime(item.last_observed_at)}`,
        }))} />
        <MetadataList title="Inbox lag" failed={inbox.isError} busy={inbox.isFetching} rows={(inbox.data ?? []).map((item) => ({
          id: item.delivery_id,
          state: item.event_type,
          detail: `Received ${relativeTime(item.received_at)}`,
        }))} />
        <MetadataList title="Suspended accounts" failed={accounts.isError} busy={accounts.isFetching} rows={(accounts.data ?? []).map((item) => ({
          id: String(item.github_user_id),
          state: "suspended",
          detail: `Account ${item.user_id.slice(0, 8)}`,
        }))} />
        <MetadataList title="Recent control audit" failed={audit.isError} busy={audit.isFetching} rows={(audit.data ?? []).map((item) => ({
          id: item.audit_id,
          state: item.action.replaceAll("_", " "),
          detail: `Operator ${item.actor_github_user_id} · ${relativeTime(item.created_at)}`,
        }))} />
      </section>
      {listsLoading && <p className="operator-refresh" role="status">Refreshing details…</p>}
    </main>
  );
}

function Metric({ label, value, detail }: { label: string; value: number | string; detail?: string }) {
  return (
    <article className="operator-metric">
      <p className="operator-label">{label}</p>
      <strong>{value}</strong>
      {detail && <span>{detail}</span>}
    </article>
  );
}

function MetadataList({
  title,
  rows,
  busy,
  failed,
}: {
  title: string;
  rows: Array<{ id: string; state: string; detail: string }>;
  busy: boolean;
  failed: boolean;
}) {
  return (
    <article className="operator-list">
      <h2>{title}<span>{rows.length}</span></h2>
      {failed
        ? <p className="operator-empty">This metadata is unavailable.</p>
        : busy && rows.length === 0
        ? <p className="operator-empty">Loading metadata…</p>
        : rows.length === 0
        ? <p className="operator-empty">No unresolved records.</p>
        : <ul>{rows.slice(0, 20).map((row) => (
          <li key={row.id}>
            <strong>{row.state}</strong>
            <span>{row.detail}</span>
          </li>
        ))}</ul>}
    </article>
  );
}

function formatBytes(bytes: number): string {
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} KiB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
}
