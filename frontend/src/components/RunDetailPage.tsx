import { useEffect, useMemo, useRef, useState } from "react";
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router";
import {
  getRunEventPage,
  getRunSnapshot,
  RUN_EVENT_PAGE_SIZE,
  type RunEvent,
  type RunSnapshot,
} from "../api/client";
import { mergeBySequence } from "../api/sequence";
import { ErrorDisplay } from "./ErrorDisplay";
import {
  runEventPagesQueryKey,
  runSnapshotQueryKey,
  useRunEventStream,
} from "./useRunEventStream";

const MAX_EVENT_PAGES = 10;

function humanize(value: string): string {
  return value
    .replace(/[._]+/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatTimestamp(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.valueOf())
    ? value
    : new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(date);
}

function statusTone(status: RunSnapshot["status"]): string {
  if (status === "succeeded") return "positive";
  if (status === "failed" || status === "cancelled" || status === "expired" || status === "needs_attention") {
    return "negative";
  }
  if (status.startsWith("waiting_")) return "waiting";
  return "active";
}

function RunTimelineEvent({ event }: { event: RunEvent }) {
  const details = Object.entries(event.data).filter(([, value]) =>
    ["string", "number", "boolean"].includes(typeof value),
  );

  return (
    <li className="timeline-event">
      <span className="timeline-marker" aria-hidden="true" />
      <article>
        <div className="timeline-event-heading">
          <h3>{humanize(event.type)}</h3>
          <time dateTime={event.occurred_at} title={event.occurred_at}>
            {formatTimestamp(event.occurred_at)}
          </time>
        </div>
        <p className="timeline-sequence">
          Event <code>#{event.sequence}</code>
        </p>
        {details.length > 0 ? (
          <dl className="timeline-details">
            {details.map(([key, value]) => (
              <div key={key}>
                <dt>{humanize(key)}</dt>
                <dd>
                  {key.endsWith("_id") && typeof value === "string" ? (
                    <code>{value}</code>
                  ) : typeof value === "boolean" ? (
                    value ? "Yes" : "No"
                  ) : (
                    String(value)
                  )}
                </dd>
              </div>
            ))}
          </dl>
        ) : null}
      </article>
    </li>
  );
}

function StreamStatus({
  connection,
  notice,
  reconnect,
  historyPending,
  historyError,
  terminal,
}: {
  connection: ReturnType<typeof useRunEventStream>["connection"];
  notice: string | null;
  reconnect: () => void;
  historyPending: boolean;
  historyError: boolean;
  terminal: boolean;
}) {
  const statusText = historyError
    ? "Live updates paused"
    : historyPending
    ? "Checking event history"
    : connection === "connected"
      ? "Live updates connected"
      : connection === "reconnecting"
        ? "Reconnecting to live updates"
        : connection === "syncing"
          ? "Refreshing run history"
          : connection === "disconnected"
            ? "Live updates disconnected"
            : "Connecting to live updates";

  return (
    <div className="stream-status-row">
      <p className={`stream-status stream-status-${connection}`} role="status" aria-live="polite">
        <span className="status-dot" aria-hidden="true" />
        <span>{statusText}</span>
        {terminal ? <span className="stream-terminal-note"> · terminal run</span> : null}
      </p>
      {notice ? <p className="stream-notice">{notice}</p> : null}
      {connection === "disconnected" && !historyPending ? (
        <button className="text-button" type="button" onClick={reconnect}>
          Reconnect
        </button>
      ) : null}
    </div>
  );
}

function RunDetailForRun({ runId }: { runId: string }) {
  const snapshotQuery = useQuery({
    queryKey: runSnapshotQueryKey(runId),
    queryFn: ({ signal }) => getRunSnapshot(runId, signal),
    retry: false,
    staleTime: 0,
    refetchOnMount: "always",
  });
  const eventPagesQuery = useInfiniteQuery({
    queryKey: runEventPagesQueryKey(runId),
    queryFn: ({ pageParam, signal }) => getRunEventPage(runId, pageParam, signal),
    initialPageParam: "0" as string,
    getNextPageParam: (lastPage) => lastPage.next_cursor ?? undefined,
    enabled: snapshotQuery.isSuccess,
    retry: false,
    staleTime: 0,
    refetchOnMount: "always",
  });
  const reconciledGaps = useRef(new Set<string>());
  const [reconcilingHistory, setReconcilingHistory] = useState(false);
  const pendingGapCursor = eventPagesQuery.data?.pages.find(
    (page) => page.history_gap && !reconciledGaps.current.has(page.after_sequence),
  )?.after_sequence;

  useEffect(() => {
    if (pendingGapCursor === undefined) return;
    reconciledGaps.current.add(pendingGapCursor);
    setReconcilingHistory(true);
    void Promise.all([snapshotQuery.refetch(), eventPagesQuery.refetch()]).finally(() => {
      setReconcilingHistory(false);
    });
  }, [eventPagesQuery.refetch, pendingGapCursor, snapshotQuery.refetch]);

  const streamEnabled =
    snapshotQuery.isFetchedAfterMount &&
    eventPagesQuery.isFetchedAfterMount &&
    eventPagesQuery.isSuccess &&
    !reconcilingHistory &&
    pendingGapCursor === undefined;
  const stream = useRunEventStream(
    runId,
    snapshotQuery.data?.event_watermark ?? "0",
    streamEnabled,
  );
  const historyEvents = eventPagesQuery.data?.pages.flatMap((page) => page.events) ?? [];
  const events = useMemo(
    () => mergeBySequence(historyEvents, stream.liveEvents),
    [historyEvents, stream.liveEvents],
  );
  const hasHistoryGap = eventPagesQuery.data?.pages.some((page) => page.history_gap) ?? false;
  const hasLoadedMaximum = (eventPagesQuery.data?.pages.length ?? 0) >= MAX_EVENT_PAGES;
  const canLoadMore = eventPagesQuery.hasNextPage && !hasLoadedMaximum;
  const snapshot = snapshotQuery.data;
  const historyUnavailable = eventPagesQuery.isPending || reconcilingHistory || pendingGapCursor !== undefined;
  const terminal = snapshot?.status === "succeeded" || snapshot?.status === "failed" || snapshot?.status === "cancelled" || snapshot?.status === "expired";

  if (snapshotQuery.isPending || !snapshotQuery.isFetchedAfterMount) {
    return (
      <main className="page-shell run-page">
        <p className="eyebrow">Run detail</p>
        <h1>Loading run</h1>
        <p className="run-loading" role="status">Loading the current run snapshot…</p>
      </main>
    );
  }

  if (snapshotQuery.isError || !snapshot) {
    return (
      <main className="page-shell run-page">
        <p className="eyebrow">Run detail</p>
        <h1>Run unavailable</h1>
        <ErrorDisplay error={snapshotQuery.error} title="Could not load this run" />
        <button className="text-button retry-button" type="button" onClick={() => void snapshotQuery.refetch()}>
          Retry loading run
        </button>
      </main>
    );
  }

  return (
    <main className="page-shell run-page">
      <div className="run-page-heading">
        <div>
          <p className="eyebrow">Run detail</p>
          <h1>Current run</h1>
        </div>
        <div className="run-heading-links">
          {snapshot.status === "waiting_approval" ? (
            <Link to={`/runs/${encodeURIComponent(runId)}/review`} className="review-route-link">Review candidate</Link>
          ) : null}
          <Link to="/" className="text-link">Service status</Link>
        </div>
      </div>

      <p className="run-id-line">
        <span>Run ID</span>
        <code>{snapshot.run_id}</code>
      </p>

      <section className="run-snapshot" aria-labelledby="run-snapshot-heading">
        <div className="run-section-heading">
          <div>
            <p className="eyebrow">Run status</p>
            <h2 id="run-snapshot-heading">Current state</h2>
          </div>
          <button
            className="text-button"
            type="button"
            onClick={() => void snapshotQuery.refetch()}
            disabled={snapshotQuery.isFetching}
          >
            {snapshotQuery.isFetching ? "Refreshing…" : "Refresh status"}
          </button>
        </div>
        <dl className="run-facts">
          <div>
            <dt>Status</dt>
            <dd>
              <span className={`run-status run-status-${statusTone(snapshot.status)}`}>
                {humanize(snapshot.status)}
              </span>
            </dd>
          </div>
          <div><dt>Revision</dt><dd>{snapshot.revision}</dd></div>
          <div>
            <dt>Created</dt>
            <dd><time dateTime={snapshot.created_at}>{formatTimestamp(snapshot.created_at)}</time></dd>
          </div>
          <div>
            <dt>Deadline</dt>
            <dd>
              {snapshot.deadline_at ? (
                <time dateTime={snapshot.deadline_at}>{formatTimestamp(snapshot.deadline_at)}</time>
              ) : "No deadline recorded"}
            </dd>
          </div>
          <div><dt>Latest event sequence</dt><dd><code>{snapshot.event_watermark}</code></dd></div>
        </dl>
      </section>

      <section className="timeline-section" aria-labelledby="event-history-heading">
        <div className="run-section-heading timeline-section-heading">
          <div>
            <p className="eyebrow">Committed runtime events</p>
            <h2 id="event-history-heading">Event history</h2>
          </div>
          <StreamStatus
            connection={stream.connection}
            notice={stream.notice}
            reconnect={stream.reconnect}
            historyPending={historyUnavailable}
            historyError={eventPagesQuery.isError}
            terminal={terminal}
          />
        </div>

        {hasHistoryGap ? (
          <p className="history-gap" role="status">
            Earlier event history is unavailable. This view shows the events the service still retains.
          </p>
        ) : null}
        {eventPagesQuery.isError ? (
          <>
            <ErrorDisplay error={eventPagesQuery.error} title="Could not load event history" />
            <button
              className="text-button retry-button"
              type="button"
              onClick={() => void Promise.all([snapshotQuery.refetch(), eventPagesQuery.refetch()])}
            >
              Retry event history
            </button>
          </>
        ) : null}
        {eventPagesQuery.isPending || reconcilingHistory ? (
          <p className="timeline-message" role="status">
            {reconcilingHistory ? "Checking the current snapshot and retained events…" : "Loading committed events…"}
          </p>
        ) : events.length === 0 ? (
          <p className="timeline-message">
            {hasHistoryGap
              ? "No events are available in the retained history."
              : "No events have been recorded for this run yet."}
          </p>
        ) : (
          <ol className="run-timeline" aria-label="Run event history">
            {events.map((event) => <RunTimelineEvent key={event.sequence} event={event} />)}
          </ol>
        )}

        {canLoadMore ? (
          <button
            className="text-button load-more-button"
            type="button"
            onClick={() => void eventPagesQuery.fetchNextPage()}
            disabled={eventPagesQuery.isFetchingNextPage}
          >
            {eventPagesQuery.isFetchingNextPage ? "Loading more events…" : "Load more events"}
          </button>
        ) : null}
        {eventPagesQuery.hasNextPage && hasLoadedMaximum ? (
          <p className="timeline-limit" role="status">
            This view keeps at most {MAX_EVENT_PAGES * RUN_EVENT_PAGE_SIZE} saved events. The live buffer is also bounded.
          </p>
        ) : null}
        {eventPagesQuery.isFetchingNextPage ? (
          <p className="visually-hidden" role="status">Loading the next event page…</p>
        ) : null}
      </section>
    </main>
  );
}

export function RunDetailPage() {
  const { runId } = useParams();
  if (!runId) {
    return (
      <main className="page-shell run-page">
        <p className="eyebrow">Run detail</p>
        <h1>Run unavailable</h1>
        <p className="timeline-message">A run ID is required to open this page.</p>
      </main>
    );
  }
  return <RunDetailForRun key={runId} runId={runId} />;
}
