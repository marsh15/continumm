import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams } from "react-router";
import {
  ApiError,
  getRunReview,
  getRunSnapshot,
  submitApprovalDecision,
  type ApprovalDecision,
  type ReviewPhaseEvidence,
} from "../api/client";
import { ErrorDisplay } from "./ErrorDisplay";

const FEEDBACK_LIMIT_BYTES = 16 * 1024;
const terminalStatuses = new Set(["succeeded", "failed", "cancelled", "expired"]);

function formatTimestamp(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.valueOf())
    ? value
    : new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(date);
}

function formatLabel(value: string): string {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function idempotencyKey(): string {
  return typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

type DiffFile = { path: string; text: string };

function splitDiff(text: string, paths: string[]): DiffFile[] {
  const sections = text.split(/(?=^diff --git )/m).filter((section) => section.startsWith("diff --git "));
  return paths.map((path) => {
    const matching = sections.find((section) => {
      const header = section.split("\n", 1)[0];
      const match = /^diff --git a\/(.+) b\/(.+)$/.exec(header);
      return match?.[2] === path || match?.[1] === path;
    });
    return { path, text: matching ?? text };
  });
}

function PhaseEvidence({ title, phase }: { title: string; phase: ReviewPhaseEvidence }) {
  return (
    <section className="review-phase" aria-label={`${title} evidence`}>
      <div className="review-phase-heading">
        <h4>{title}</h4>
        <span className={`evidence-outcome outcome-${phase.outcome.toLowerCase().replaceAll(/[^a-z0-9]+/g, "-")}`}>
          {formatLabel(phase.outcome)}
        </span>
      </div>
      <dl className="review-evidence-facts">
        <div><dt>Test exit code</dt><dd>{phase.test_exit_code ?? "Not recorded"}</dd></div>
        <div><dt>Packages</dt><dd>{phase.package_count ?? "Not recorded"}</dd></div>
        <div><dt>Allocation</dt><dd>{phase.allocation_status ? formatLabel(phase.allocation_status) : "Not recorded"}</dd></div>
        <div><dt>Runtime</dt><dd>{phase.runtime_version ?? "Not recorded"}</dd></div>
        <div><dt>npm</dt><dd>{phase.npm_version ?? "Not recorded"}</dd></div>
        <div><dt>Commands</dt><dd>{phase.commands.length}</dd></div>
        {phase.error_code ? <div><dt>Error code</dt><dd><code>{phase.error_code}</code></dd></div> : null}
      </dl>
      {phase.commands.length > 0 ? (
        <ul className="review-command-list" aria-label={`${title} command results`}>
          {phase.commands.map((command, index) => (
            <li key={`${command.phase}-${index}`}>
              <span>{formatLabel(command.phase)}</span>
              <span>Exit {command.exit_code ?? "unknown"}</span>
              <span>Output {command.stdout_bytes + command.stderr_bytes} bytes</span>
            </li>
          ))}
        </ul>
      ) : <p className="review-muted">No command output was recorded.</p>}
    </section>
  );
}

function LimitMeter({ label, used, limit, unit }: { label: string; used: number; limit: number; unit?: string }) {
  return (
    <div className="review-limit">
      <div className="review-limit-heading">
        <dt>{label}</dt>
        <dd><span className="review-number">{used.toLocaleString()}</span> / {limit.toLocaleString()}{unit ? ` ${unit}` : ""}</dd>
      </div>
      <progress className="review-limit-track" value={Math.min(used, limit)} max={limit} aria-hidden="true" />
    </div>
  );
}

function errorCode(error: unknown): string | undefined {
  return error instanceof ApiError ? error.body.code : undefined;
}

function ReviewForRun({ runId }: { runId: string }) {
  const queryClient = useQueryClient();
  const reviewKey = ["run-review", runId] as const;
  const snapshotKey = ["run-snapshot", runId] as const;
  const reviewQuery = useQuery({
    queryKey: reviewKey,
    queryFn: ({ signal }) => getRunReview(runId, signal),
    retry: false,
    refetchOnMount: "always",
  });
  const snapshotQuery = useQuery({
    queryKey: snapshotKey,
    queryFn: ({ signal }) => getRunSnapshot(runId, signal),
    retry: false,
    refetchOnMount: "always",
  });
  const [selectedPath, setSelectedPath] = useState("");
  const [feedback, setFeedback] = useState("");
  const [now, setNow] = useState(Date.now());
  const [notice, setNotice] = useState("");
  const [showFeedbackError, setShowFeedbackError] = useState(false);
  const [submittedApprovalId, setSubmittedApprovalId] = useState<string | null>(null);
  const feedbackRef = useRef<HTMLTextAreaElement>(null);
  const pendingSubmission = useRef<{
    approvalId: string;
    decision: ApprovalDecision;
    digest: string;
    feedback?: string;
    key: string;
  } | null>(null);

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 30_000);
    return () => window.clearInterval(timer);
  }, []);

  const decision = useMutation({
    mutationFn: (input: NonNullable<typeof pendingSubmission.current>) =>
      submitApprovalDecision(input.approvalId, input.decision, input.digest, input.key, input.feedback),
    onSuccess: async (response) => {
      pendingSubmission.current = null;
      setSubmittedApprovalId(response.approval_id);
      setNotice(`Decision recorded for revision ${response.revision}.`);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: reviewKey }),
        queryClient.invalidateQueries({ queryKey: snapshotKey }),
      ]);
    },
    onError: async (error) => {
      const code = errorCode(error);
      const stale = code === "approval_stale" || code === "approval_expired" || code === "approval_decision_conflict";
      const retryable = !(error instanceof ApiError) || error.body.retryable;
      if (stale) {
        pendingSubmission.current = null;
        setNotice("This review changed while it was open. The latest run and review are being loaded.");
        await Promise.all([
          queryClient.invalidateQueries({ queryKey: reviewKey, refetchType: "all" }),
          queryClient.invalidateQueries({ queryKey: snapshotKey, refetchType: "all" }),
        ]);
      } else if (!retryable) {
        pendingSubmission.current = null;
      }
    },
  });

  const review = reviewQuery.data;
  const paths = review?.diff.changed_paths ?? [];
  const revisionLabel = review?.revisions.used === 0
    ? "Initial candidate"
    : `Revision ${review?.revisions.used} of ${review?.revisions.limit}`;
  const files = useMemo(() => review ? splitDiff(review.diff.text, paths) : [], [paths, review]);
  useEffect(() => {
    if (paths.length > 0 && !paths.includes(selectedPath)) setSelectedPath(paths[0]);
  }, [paths, selectedPath]);

  if (reviewQuery.isPending || snapshotQuery.isPending) {
    return <main id="main-content" tabIndex={-1} className="page-shell review-page"><p className="eyebrow">Candidate review</p><h1>Loading review</h1><p role="status">Loading the current review and run state…</p></main>;
  }
  if (reviewQuery.isError || !review || snapshotQuery.isError || !snapshotQuery.data) {
    return (
      <main id="main-content" tabIndex={-1} className="page-shell review-page">
        <p className="eyebrow">Candidate review</p><h1>Review unavailable</h1>
        {notice ? <p className="review-notice" role="status">{notice}</p> : null}
        <ErrorDisplay error={reviewQuery.error ?? snapshotQuery.error} title="Could not load the current review" />
        {snapshotQuery.data ? <p className="review-current-run" role="status">Latest run state: {formatLabel(snapshotQuery.data.status)} · revision {snapshotQuery.data.revision}</p> : null}
        <button className="text-button" type="button" onClick={() => void Promise.all([reviewQuery.refetch(), snapshotQuery.refetch()])}>Retry loading review</button>
        <Link className="text-link review-back-link" to={`/runs/${encodeURIComponent(runId)}`}>Return to run</Link>
      </main>
    );
  }

  const approvalExpiry = new Date(review.approval.expires_at).getTime();
  const expired = !Number.isFinite(approvalExpiry) || now >= approvalExpiry || review.approval.status === "expired";
  const terminal = terminalStatuses.has(snapshotQuery.data.status);
  const stale = snapshotQuery.data.revision !== review.revision || snapshotQuery.data.status !== "waiting_approval";
  const canDecide = review.decision_eligible && review.approval.status === "pending" && !expired && !terminal && !stale && submittedApprovalId !== review.approval.approval_id;
  const feedbackBytes = new TextEncoder().encode(feedback).byteLength;
  const feedbackInvalid = feedbackBytes > FEEDBACK_LIMIT_BYTES;
  const outputForFile = files.find((file) => file.path === selectedPath)?.text ?? review.diff.text;
  const retryableFailure = decision.isError && pendingSubmission.current !== null;

  function submit(action: ApprovalDecision): void {
    if (!review || !canDecide || decision.isPending) return;
    if (action === "changes" && (feedback.trim().length === 0 || feedbackInvalid)) {
      setShowFeedbackError(true);
      feedbackRef.current?.focus();
      return;
    }
    setShowFeedbackError(false);
    setNotice("");
    const submission = {
      approvalId: review.approval.approval_id,
      decision: action,
      digest: review.manifest_sha256,
      ...(action === "changes" ? { feedback } : {}),
      key: idempotencyKey(),
    };
    pendingSubmission.current = submission;
    decision.mutate(submission);
  }

  function retry(): void {
    if (!pendingSubmission.current || decision.isPending) return;
    decision.mutate(pendingSubmission.current);
  }

  let blockedMessage = "";
  if (expired) blockedMessage = "This approval has expired. Refresh the run to see its latest state.";
  else if (stale) blockedMessage = "This review no longer matches the current run revision. Refreshing the page will load the latest candidate.";
  else if (terminal) blockedMessage = `The run is ${formatLabel(snapshotQuery.data.status).toLowerCase()} and no longer accepts review decisions.`;
  else if (review.approval.status !== "pending") blockedMessage = `This approval is ${formatLabel(review.approval.status).toLowerCase()}.`;
  else if (!review.decision_eligible) blockedMessage = "A decision is not available for the current run state.";

  return (
    <main id="main-content" tabIndex={-1} className="page-shell review-page">
      <div className="review-page-heading">
        <div><p className="eyebrow">Candidate review · {revisionLabel}</p><h1>Review the proposed changes</h1></div>
        <div className="review-heading-links">
          <a href="#decision-heading" className="review-skip-decision">Skip to decision</a>
          <Link to={`/runs/${encodeURIComponent(runId)}`} className="text-link">Run detail</Link>
        </div>
      </div>
      <p className="review-run-line"><span>Run</span><code>{review.run_id}</code><span>Current state</span><strong>{formatLabel(snapshotQuery.data.status)}</strong></p>

      <section className="review-section" aria-labelledby="diff-heading">
        <div className="review-section-heading"><div><p className="eyebrow">Candidate changes</p><h2 id="diff-heading">Files and diff</h2></div><span className="review-count">{paths.length} {paths.length === 1 ? "file" : "files"}</span></div>
        {review.diff.truncated ? <p className="review-notice" role="status">The displayed diff was shortened by the service. The manifest and changed path list remain complete.</p> : null}
        <div className="review-diff-layout">
          <nav className="review-file-nav" aria-label="Changed files">
            {paths.length === 0 ? <p className="review-muted">No changed paths are listed.</p> : paths.map((path) => (
              <button key={path} type="button" className={`review-file-button${selectedPath === path ? " is-selected" : ""}`} aria-pressed={selectedPath === path} onClick={() => setSelectedPath(path)}>
                <span className="review-file-mark" aria-hidden="true">{selectedPath === path ? "•" : ""}</span><code>{path}</code>
              </button>
            ))}
          </nav>
          <div className="review-diff-view" role="region" aria-label={selectedPath ? `Diff for ${selectedPath}` : "Candidate diff"} tabIndex={0}>
            {outputForFile ? <pre><code>{outputForFile}</code></pre> : <p className="review-muted">No diff text is available.</p>}
          </div>
        </div>
      </section>

      <section className="review-section" aria-labelledby="validation-heading">
        <div className="review-section-heading"><div><p className="eyebrow">Validation evidence</p><h2 id="validation-heading">Baseline and candidate</h2></div><span className={`run-status ${review.evidence.classification === "passed" ? "run-status-positive" : review.evidence.classification.startsWith("passed") ? "run-status-waiting" : "run-status-negative"}`}>{formatLabel(review.evidence.classification)}</span></div>
        <div className="review-summary-facts"><div><dt>Regression on original</dt><dd>{formatLabel(review.evidence.regression_on_original)}</dd></div><div><dt>Changed paths</dt><dd>{review.evidence.changed_path_count}</dd></div><div><dt>Evidence version</dt><dd>{review.evidence.evidence_version}</dd></div></div>
        <div className="review-evidence-grid">
          <PhaseEvidence title="Baseline" phase={review.evidence.baseline} />
          <PhaseEvidence title="Candidate" phase={review.evidence.candidate} />
        </div>
        <dl className="review-hashes">
          <div><dt>Candidate archive SHA-256</dt><dd><code>{review.evidence.candidate_archive_sha256 ?? "Not recorded"}</code></dd></div>
          <div><dt>Candidate source tree SHA-256</dt><dd><code>{review.evidence.candidate_source_tree_sha256 ?? "Not recorded"}</code></dd></div>
          <div><dt>Candidate lineage SHA-256</dt><dd><code>{review.evidence.candidate_lineage_sha256 ?? "Not recorded"}</code></dd></div>
          <div><dt>Baseline artifact</dt><dd><code>{review.evidence.baseline_artifact_id ?? "Not recorded"}</code></dd></div>
        </dl>
      </section>

      <section className="review-section" aria-labelledby="manifest-heading">
        <div className="review-section-heading"><div><p className="eyebrow">Immutable publication target</p><h2 id="manifest-heading">Manifest and resource limits</h2></div><span className="review-count">{review.revisions.remaining} revision{review.revisions.remaining === 1 ? "" : "s"} remaining</span></div>
        <div className="review-digest"><dt>Manifest SHA-256</dt><dd><code>{review.manifest_sha256}</code></dd></div>
        <p className="review-expiry">Approval expires <time dateTime={review.approval.expires_at}>{formatTimestamp(review.approval.expires_at)}</time></p>
        <details className="manifest-details" open>
          <summary>Exact manifest JSON</summary>
          <pre><code>{JSON.stringify(review.manifest, null, 2)}</code></pre>
        </details>
        <dl className="review-limits" aria-label="Cumulative run usage and limits">
          <LimitMeter label="Model requests" used={review.limits.requests.used} limit={review.limits.requests.limit} />
          <LimitMeter label="Input tokens" used={review.limits.input_tokens.used} limit={review.limits.input_tokens.limit} />
          <LimitMeter label="Output tokens" used={review.limits.output_tokens.used} limit={review.limits.output_tokens.limit} />
          <LimitMeter label="Sandbox time" used={review.limits.sandbox_seconds.used} limit={review.limits.sandbox_seconds.limit} unit="seconds" />
        </dl>
      </section>

      <section className="review-decision-section" aria-labelledby="decision-heading">
        <div className="review-section-heading"><div><p className="eyebrow">Approval</p><h2 id="decision-heading">Choose what happens next</h2></div><span className={`run-status ${canDecide ? "run-status-waiting" : "run-status-negative"}`}>{canDecide ? "Decision available" : "Decision unavailable"}</span></div>
        {blockedMessage ? <p className="review-blocked" role="status">{blockedMessage}</p> : null}
        {notice ? <p className="review-notice" role="status" aria-live="polite">{notice}</p> : null}
        {decision.isError ? (
          <div className="review-mutation-error" role="alert">
            <ErrorDisplay error={decision.error} title="Decision was not confirmed" />
            {retryableFailure ? <button className="text-button" type="button" onClick={retry} disabled={decision.isPending}>Retry the same decision</button> : null}
            {errorCode(decision.error) === "approval_revisions_exhausted" ? <p>Both revision rounds have been used. This candidate cannot request another revision.</p> : null}
          </div>
        ) : null}
        {review.revisions.remaining === 0 ? <p className="review-muted">The two revision rounds have been used.</p> : null}
        <label className="review-feedback-label" htmlFor="review-feedback">Feedback for the next revision <span>(required when requesting changes; 16 KiB maximum)</span></label>
        <textarea
          id="review-feedback"
          ref={feedbackRef}
          value={feedback}
          onChange={(event) => { setFeedback(event.currentTarget.value); setShowFeedbackError(false); }}
          rows={4}
          maxLength={FEEDBACK_LIMIT_BYTES}
          aria-invalid={showFeedbackError && (feedback.trim().length === 0 || feedbackInvalid)}
          aria-describedby="review-feedback-help review-feedback-count"
          disabled={!canDecide || decision.isPending}
        />
        <div className="review-feedback-meta"><span id="review-feedback-help">Include specific files and behavior to change.</span><span id="review-feedback-count" className={feedbackInvalid ? "review-character-error" : ""}>{feedbackBytes.toLocaleString()} / {FEEDBACK_LIMIT_BYTES.toLocaleString()} bytes</span></div>
        {showFeedbackError ? <p className="field-error" role="alert">{feedbackInvalid ? "Feedback must be no more than 16 KiB." : "Add feedback before requesting changes."}</p> : null}
        <div className="review-actions" aria-label="Review decisions">
          <button className="decision-primary" type="button" onClick={() => submit("approve")} disabled={!canDecide || feedbackInvalid || decision.isPending}>{decision.isPending && pendingSubmission.current?.decision === "approve" ? "Submitting…" : "Approve candidate"}</button>
          <button className="decision-secondary" type="button" onClick={() => submit("changes")} disabled={!canDecide || feedbackInvalid || decision.isPending || review.revisions.remaining === 0}>{decision.isPending && pendingSubmission.current?.decision === "changes" ? "Submitting…" : "Request changes"}</button>
          <button className="decision-danger" type="button" onClick={() => submit("reject")} disabled={!canDecide || feedbackInvalid || decision.isPending}>{decision.isPending && pendingSubmission.current?.decision === "reject" ? "Submitting…" : "Reject candidate"}</button>
        </div>
        <p className="review-idempotency-note">Each decision is bound to manifest <code>{review.manifest_sha256.slice(0, 12)}</code>. A retry reuses the same request key.</p>
      </section>
    </main>
  );
}

export function ReviewPage() {
  const { runId } = useParams();
  if (!runId) return <main id="main-content" tabIndex={-1} className="page-shell review-page"><p className="eyebrow">Candidate review</p><h1>Review unavailable</h1><p>A run ID is required to open this page.</p></main>;
  return <ReviewForRun key={runId} runId={runId} />;
}
