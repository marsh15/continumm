import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  isEventStreamError,
  isHistoryReset,
  isRunEvent,
  RUN_EVENT_PAGE_SIZE,
  type RunEvent,
  type RunSnapshot,
} from "../api/client";
import { classifySequence, mergeBySequence } from "../api/sequence";

export type StreamConnection = "connecting" | "connected" | "reconnecting" | "syncing" | "disconnected";

export type RunEventStream = {
  connection: StreamConnection;
  liveEvents: RunEvent[];
  notice: string | null;
  reconnect: () => void;
};

export const runSnapshotQueryKey = (runId: string) => ["runs", runId] as const;
export const runEventPagesQueryKey = (runId: string) => ["runs", runId, "events"] as const;

const MAX_RECONNECT_ERRORS = 3;
const MAX_LIVE_EVENTS = 500;
const RETRY_BUDGET_RESET_MS = 30_000;
function hasLoadedEventPage(value: unknown): boolean {
  return (
    typeof value === "object" &&
    value !== null &&
    "pages" in value &&
    Array.isArray(value.pages) &&
    value.pages.length > 0
  );
}

function parsePayload<T>(event: Event, isPayload: (value: unknown) => value is T): T | null {
  const serialized = (event as MessageEvent<string>).data;
  if (typeof serialized !== "string") return null;
  try {
    const payload: unknown = JSON.parse(serialized);
    return isPayload(payload) ? payload : null;
  } catch {
    return null;
  }
}

export function useRunEventStream(
  runId: string,
  initialWatermark: string,
  enabled: boolean,
): RunEventStream {
  const queryClient = useQueryClient();
  const snapshotKey = useMemo(() => runSnapshotQueryKey(runId), [runId]);
  const eventPagesKey = useMemo(() => runEventPagesQueryKey(runId), [runId]);
  const [connection, setConnection] = useState<StreamConnection>("connecting");
  const [liveEvents, setLiveEvents] = useState<RunEvent[]>([]);
  const [notice, setNotice] = useState<string | null>(null);
  const [resumeAfter, setResumeAfter] = useState(initialWatermark);
  const [sourceGeneration, setSourceGeneration] = useState(0);
  const [paused, setPaused] = useState(false);
  const [initialized, setInitialized] = useState(false);
  const lastSequence = useRef(initialWatermark);
  const syncing = useRef(false);
  const networkRetryAttempt = useRef(0);

  useEffect(() => {
    if (!enabled || initialized) return;
    lastSequence.current = initialWatermark;
    setResumeAfter(initialWatermark);
    setInitialized(true);
  }, [enabled, initialWatermark, initialized]);

  const synchronize = useCallback(async () => {
    if (syncing.current) return;
    syncing.current = true;
    setPaused(true);
    setConnection("syncing");
    setNotice("Checking the current run status and event history before reconnecting.");
    try {
      const outcomes = await Promise.allSettled([
        queryClient.refetchQueries(
          { queryKey: snapshotKey, exact: true },
          { throwOnError: true },
        ),
        queryClient.resetQueries(
          { queryKey: eventPagesKey, exact: true },
          { throwOnError: true },
        ),
      ]);
      const snapshotState = queryClient.getQueryState<RunSnapshot>(snapshotKey);
      const eventsState = queryClient.getQueryState(eventPagesKey);
      const latestSnapshot = queryClient.getQueryData<RunSnapshot>(snapshotKey);
      if (
        outcomes.some((outcome) => outcome.status === "rejected") ||
        snapshotState?.status === "error" ||
        eventsState?.status !== "success" ||
        !hasLoadedEventPage(eventsState.data) ||
        !latestSnapshot
      ) {
        setConnection("disconnected");
        setNotice("Could not refresh the run status and event history. Retry when the service is reachable.");
        return;
      }

      lastSequence.current = latestSnapshot.event_watermark;
      networkRetryAttempt.current = 0;
      setLiveEvents([]);
      setResumeAfter(latestSnapshot.event_watermark);
      setSourceGeneration((generation) => generation + 1);
      setPaused(false);
      setNotice(null);
    } finally {
      syncing.current = false;
    }
  }, [eventPagesKey, queryClient, snapshotKey]);

  const refreshAfterClosedStream = useCallback(async (message: string) => {
    if (syncing.current) return;
    syncing.current = true;
    setPaused(true);
    setConnection("syncing");
    setLiveEvents([]);
    setNotice(message);
    try {
      const outcomes = await Promise.allSettled([
        queryClient.resetQueries(
          { queryKey: snapshotKey, exact: true },
          { throwOnError: true },
        ),
        queryClient.resetQueries(
          { queryKey: eventPagesKey, exact: true },
          { throwOnError: true },
        ),
      ]);
      const snapshotState = queryClient.getQueryState<RunSnapshot>(snapshotKey);
      const eventsState = queryClient.getQueryState(eventPagesKey);
      if (
        outcomes.some((outcome) => outcome.status === "rejected") ||
        snapshotState?.status !== "success" ||
        eventsState?.status !== "success"
      ) {
        setNotice("Current run access could not be confirmed. Review the API response before reconnecting.");
      } else {
        setNotice("The run status and event history were refreshed. Reconnect to resume live updates.");
      }
    } finally {
      syncing.current = false;
    }
  }, [eventPagesKey, queryClient, snapshotKey]);

  useEffect(() => {
    if (!enabled || paused || !initialized) {
      setConnection(paused ? "disconnected" : "syncing");
      return;
    }

    let stopped = false;
    let disposed = false;
    let retryTimer: ReturnType<typeof window.setTimeout> | undefined;
    let openedAt: number | null = null;
    const query = new URLSearchParams({
      after: resumeAfter,
      limit: String(RUN_EVENT_PAGE_SIZE),
    });
    const url = `/api/v1/runs/${encodeURIComponent(runId)}/events?${query.toString()}`;
    let source: EventSource;
    try {
      source = new EventSource(url, { withCredentials: true });
    } catch {
      setPaused(true);
      setConnection("disconnected");
      setNotice("The live event stream could not be opened. Retry when the service is reachable.");
      return;
    }

    const close = () => {
      stopped = true;
      source.close();
    };

    setConnection("connecting");
    setNotice(null);
    source.onopen = () => {
      if (stopped) return;
      openedAt = Date.now();
      setConnection("connected");
      setNotice(null);
    };
    source.addEventListener("run.event", (rawEvent) => {
      if (stopped) return;
      const event = parsePayload(rawEvent, isRunEvent);
      if (!event || event.run_id.toLowerCase() !== runId.toLowerCase()) {
        close();
        setPaused(true);
        setConnection("disconnected");
        setNotice("The event stream returned an unreadable event. Retry to check for newer events.");
        return;
      }

      const order = classifySequence(lastSequence.current, event.sequence);
      if (order === "duplicate") return;
      if (order === "gap") {
        close();
        void synchronize();
        return;
      }

      lastSequence.current = event.sequence;
      setLiveEvents((current) => mergeBySequence(current, [event]).slice(-MAX_LIVE_EVENTS));
      void queryClient.invalidateQueries({ queryKey: snapshotKey, exact: true });
    });
    source.addEventListener("history.reset", (rawEvent) => {
      if (stopped) return;
      const reset = parsePayload(rawEvent, isHistoryReset);
      close();
      if (reset) {
        void synchronize();
      } else {
        setPaused(true);
        setConnection("disconnected");
        setNotice("The event history could not be reconciled. Retry to check for newer events.");
      }
    });
    source.addEventListener("stream.error", (rawEvent) => {
      if (stopped) return;
      const streamError = parsePayload(rawEvent, isEventStreamError);
      close();
      setConnection("disconnected");
      if (streamError?.code === "authentication_required") {
        void refreshAfterClosedStream("Session access changed. Checking access to this run again.");
      } else {
        setPaused(true);
        setNotice(
          streamError
            ? "The service stopped the live event stream. Retry to check for newer events."
            : "The live event stream returned an unreadable error. Retry to check for newer events.",
        );
      }
    });
    source.onerror = () => {
      if (stopped) return;
      if (source.readyState === EventSource.CLOSED) {
        close();
        void refreshAfterClosedStream("The live event stream closed. Checking the current run before stopping.");
        return;
      }
      if (openedAt !== null && Date.now() - openedAt >= RETRY_BUDGET_RESET_MS) {
        networkRetryAttempt.current = 0;
      }
      openedAt = null;
      const attempt = networkRetryAttempt.current + 1;
      if (attempt >= MAX_RECONNECT_ERRORS) {
        close();
        setPaused(true);
        void refreshAfterClosedStream(
          "Live updates paused after three connection attempts. Checking current run access.",
        );
      } else {
        networkRetryAttempt.current = attempt;
        const delay = 1_000 * 2 ** (attempt - 1);
        close();
        setConnection("reconnecting");
        setNotice(`The live connection was interrupted. Retrying in ${delay / 1_000} seconds (connection attempt ${attempt + 1} of ${MAX_RECONNECT_ERRORS}).`);
        retryTimer = window.setTimeout(() => {
          if (disposed) return;
          setResumeAfter(lastSequence.current);
          setSourceGeneration((generation) => generation + 1);
        }, delay);
      }
    };

    return () => {
      disposed = true;
      if (retryTimer !== undefined) window.clearTimeout(retryTimer);
      close();
    };
  }, [queryClient, resumeAfter, runId, snapshotKey, sourceGeneration, synchronize, refreshAfterClosedStream, enabled, paused, initialized]);

  const reconnect = useCallback(() => {
    networkRetryAttempt.current = 0;
    void synchronize();
  }, [synchronize]);
  return { connection, liveEvents, notice, reconnect };
}
