import type { ChangeEvent } from "react";
import type { LisaPolicySuggestion } from "./client";

type LisaPolicySuggestionCardProps = {
  suggestion: LisaPolicySuggestion;
  canReview: boolean;
  editing: boolean;
  editName: string;
  editCriteria: string;
  busy: boolean;
  actionError?: string;
  onApprove: (edited: boolean) => void;
  onDismiss: () => void;
  onEdit: () => void;
  onCancelEdit: () => void;
  onChangeName: (value: string) => void;
  onChangeCriteria: (value: string) => void;
  onOpenActivity: (activityId: string) => void;
};

export function LisaPolicySuggestionCard({
  suggestion,
  canReview,
  editing,
  editName,
  editCriteria,
  busy,
  actionError,
  onApprove,
  onDismiss,
  onEdit,
  onCancelEdit,
  onChangeName,
  onChangeCriteria,
  onOpenActivity,
}: LisaPolicySuggestionCardProps) {
  const citationCount = suggestion.evidence.reduce((count, item) => count + item.evidence.length, 0);
  return <article className="tle-lisa-suggestion-card tle-lisa-recommendation-card">
    <div className={`tle-lisa-suggestion-heading${suggestion.status === "approved" ? " is-approved" : ""}`}>
      <span className="tle-lisa-suggestion-state">{suggestion.status === "approved" ? `Active policy · ${suggestion.policy_id || "approved"}` : suggestion.status === "dismissed" ? "Dismissed recommendation" : "Policy recommendation"}</span>
      <small>{suggestion.signal_count} supporting {suggestion.signal_count === 1 ? "session" : "sessions"}</small>
    </div>
    {editing ? <>
      <label>Policy name<input value={editName} onChange={(event: ChangeEvent<HTMLInputElement>) => onChangeName(event.currentTarget.value)} maxLength={200} /></label>
      <label>Policy criteria<textarea value={editCriteria} onChange={(event: ChangeEvent<HTMLTextAreaElement>) => onChangeCriteria(event.currentTarget.value)} maxLength={20_000} rows={5} /></label>
    </> : <>
      <h4>{suggestion.name}</h4>
      <p className="tle-lisa-suggestion-criteria">{suggestion.criteria}</p>
    </>}
    <p className="tle-lisa-suggestion-scope">{suggestion.scope === "agent" ? `Suggested for ${suggestion.target_agent_name}` : "Suggested for this workspace"}</p>
    {suggestion.overlaps.length > 0 && <section className="tle-lisa-suggestion-overlaps" aria-label="Possible policy overlaps">
      <b>Check existing policies</b>
      {suggestion.overlaps.map((overlap) => <p key={`${overlap.policy_id}-${overlap.relation}`}><code>{overlap.policy_id}</code> · {overlap.relation}: {overlap.rationale}</p>)}
    </section>}
    <details className="tle-lisa-suggestion-evidence">
      <summary>Supporting run evidence · {citationCount} cited {citationCount === 1 ? "span" : "spans"}</summary>
      {suggestion.evidence.map((signal) => <section key={signal.signal_id}>
        <p className="tle-lisa-suggestion-evidence-summary">{signal.summary}</p>
        {signal.evidence.map((evidence) => <article key={`${signal.signal_id}-${evidence.span_id}`}>
          <header><span>{evidence.kind.replaceAll("_", " ")}{evidence.name ? ` · ${evidence.name}` : ""}</span><code>{evidence.span_id}</code></header>
          <pre>{evidence.content || (evidence.payload == null ? evidence.status || "No text captured" : typeof evidence.payload === "string" ? evidence.payload : JSON.stringify(evidence.payload, null, 2))}</pre>
          <small>Session {signal.session_id} · agent {signal.agent_name}</small>
        </article>)}
        <button className="tle-lisa-suggestion-source" type="button" onClick={() => onOpenActivity(signal.activity_id)}>Open evaluation activity</button>
      </section>)}
    </details>
    {actionError && <p className="tle-lisa-error" role="alert">{actionError}</p>}
    {suggestion.status === "pending" && <p className="tle-lisa-recommendation-disclosure">This recommendation stays inactive until an owner approves it.</p>}
    {suggestion.status === "approved" ? <div className="tle-lisa-suggestion-actions"><p className="tle-lisa-recommendation-disclosure">You approved this policy. Lisa will keep checking new runs against it.</p></div> : suggestion.status === "dismissed" ? <p className="tle-lisa-recommendation-disclosure">You dismissed this recommendation. It remains here for reference.</p> : <div className="tle-lisa-suggestion-actions">
      {!canReview && <span className="tle-lisa-suggestion-owner-note">Only Lisa owners can review suggestions.</span>}
      {editing ? <>
        <button type="button" disabled={!canReview || busy || !editName.trim() || !editCriteria.trim()} onClick={() => onApprove(true)}>{busy ? "Approving…" : "Approve edited policy"}</button>
        <button type="button" disabled={busy} onClick={onCancelEdit}>Cancel</button>
      </> : <>
        <button type="button" disabled={!canReview || busy} onClick={() => onApprove(false)}>{busy ? "Approving…" : "Approve policy"}</button>
        <button type="button" disabled={!canReview || busy} onClick={onEdit}>Edit</button>
        <button type="button" disabled={!canReview || busy} onClick={onDismiss}>{busy ? "Saving…" : "Dismiss"}</button>
      </>}
    </div>}
  </article>;
}
