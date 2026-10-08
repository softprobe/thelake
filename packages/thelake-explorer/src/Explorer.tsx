import { useEffect, useMemo, useState } from "react";
import { ExplorerApi, type ExplorerConfig, type Observation, type SessionSummary } from "./client";
import { ChatView } from "./ChatView";
import "./style.css";

export type ThelakeExplorerProps = {
  config: ExplorerConfig;
  className?: string;
  pageSize?: number;
  initialSessionId?: string;
  onSessionOpen?: (sessionId: string) => void;
  onBack?: () => void;
};

function spanDepth(observation: Observation, byId: Map<string, Observation>): number {
  let depth = 0;
  let parent = observation.parent_span_id;
  const seen = new Set([observation.span_id]);
  while (parent && byId.has(parent) && !seen.has(parent) && depth < 12) {
    seen.add(parent);
    parent = byId.get(parent)?.parent_span_id;
    depth += 1;
  }
  return depth;
}

function json(value: unknown): string {
  if (value == null) return "—";
  return typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

export function ThelakeExplorer({
  config,
  className,
  pageSize = 50,
  initialSessionId,
  onSessionOpen,
  onBack,
}: ThelakeExplorerProps) {
  const api = useMemo(() => new ExplorerApi(config), [config]);
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [selected, setSelected] = useState<string | undefined>(initialSessionId);
  const [observations, setObservations] = useState<Observation[]>([]);
  const [selectedSpan, setSelectedSpan] = useState<string>();
  const [cursor, setCursor] = useState<string>();
  const [days, setDays] = useState(7);
  const [query, setQuery] = useState("");
  const [agentName, setAgentName] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [saving, setSaving] = useState(false);
  const [showChat, setShowChat] = useState(!initialSessionId);
  const [error, setError] = useState<string>();

  useEffect(() => setSelected(initialSessionId), [initialSessionId]);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setCursor(undefined);
    setError(undefined);
    api.searchSessions(pageSize, undefined, controller.signal, days, agentName)
      .then((result) => { setSessions(result.items); setCursor(result.nextCursor ?? undefined); })
      .catch((e: unknown) => { if (!controller.signal.aborted) setError(String(e)); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [api, pageSize, days, agentName]);

  useEffect(() => {
    if (!selected) { setObservations([]); setSelectedSpan(undefined); return; }
    const controller = new AbortController();
    setObservations([]);
    setSelectedSpan(undefined);
    setLoadingDetail(true);
    setError(undefined);
    api.getSession(selected, controller.signal).then((session) => {
      const all = session.spans
        .filter((observation) => !observation.session_id || observation.session_id === selected)
        .sort((a, b) => a.start_time.localeCompare(b.start_time));
      setObservations(all);
      setSelectedSpan(all[0]?.span_id);
    }).catch((e: unknown) => { if (!controller.signal.aborted) setError(String(e)); })
      .finally(() => { if (!controller.signal.aborted) setLoadingDetail(false); });
    return () => controller.abort();
  }, [api, selected]);

  const filteredSessions = useMemo(() => {
    const term = query.trim().toLowerCase();
    if (!term) return sessions;
    return sessions.filter((session) => session.session_id.toLowerCase().includes(term)
      || session.models?.some((model) => model.toLowerCase().includes(term))
      || session.user_ids?.some((user) => user.toLowerCase().includes(term)));
  }, [sessions, query]);
  const observationById = useMemo(() => new Map(observations.map((observation) => [observation.span_id, observation])), [observations]);
  const focused = selectedSpan ? observationById.get(selectedSpan) : undefined;
  const traceGroups = useMemo(() => {
    const groups = new Map<string, Observation[]>();
    for (const observation of observations) {
      const group = groups.get(observation.trace_id) ?? [];
      group.push(observation);
      groups.set(observation.trace_id, group);
    }
    return [...groups.entries()];
  }, [observations]);

  async function loadMore() {
    if (!cursor || loadingMore) return;
    setLoadingMore(true);
    try {
      const next = await api.searchSessions(pageSize, cursor, undefined, days, agentName);
      setSessions((prev) => [...prev, ...next.items.filter((item) => !prev.some((row) => row.session_id === item.session_id))]);
      setCursor(next.nextCursor ?? undefined);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoadingMore(false);
    }
  }

  async function saveVerdict(verdict: "correct" | "wrong" | "unsure") {
    if (!selected || !focused) return;
    setSaving(true);
    setError(undefined);
    try {
      const written = await api.createScore({
        name: "human_verdict", data_type: "categorical", string_value: verdict,
        session_id: selected, trace_id: focused.trace_id, span_id: focused.span_id,
      });
      setObservations((prev) => prev.map((item) => item.span_id === focused.span_id
        ? { ...item, scores: [...(item.scores ?? []), written] }
        : item));
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  }

  function goBack() {
    if (onBack) onBack();
    else setSelected(undefined);
  }

  return (
    <main className={`thelake-explorer ${className ?? ""}`}>
      <header>
        <div><strong>theLake</strong><small>Investigate agent behavior</small></div>
        <nav aria-label="Main navigation">
          <button aria-pressed={showChat} onClick={() => setShowChat(true)}>Chat</button>
          <button aria-pressed={!showChat} onClick={() => { setShowChat(false); setSelected(undefined); }}>Sessions</button>
        </nav>
        {selected && !showChat && <button onClick={goBack}>Sessions</button>}
      </header>
      {error && <div role="alert" className="tle-error">{error}</div>}
      {showChat ? <ChatView key={config.chatStorageKey ?? "workspace-unspecified"} api={api} storageKey={config.chatStorageKey} onOpenSession={(sessionId) => { setSelected(sessionId); setShowChat(false); }} /> : <div className="tle-layout">
        <aside aria-label="Sessions">
          <div className="tle-list-tools">
            <h2>Sessions</h2>
            <select aria-label="Session time range" value={days} onChange={(event) => setDays(Number(event.target.value))}>
              <option value={1}>24 hours</option><option value={7}>7 days</option><option value={30}>30 days</option><option value={90}>90 days</option>
            </select>
            <input aria-label="Filter by agent" placeholder="Agent name" value={agentName} onChange={(event) => setAgentName(event.target.value)} />
            <input aria-label="Filter sessions" placeholder="Filter session, model, user" value={query} onChange={(event) => setQuery(event.target.value)} />
          </div>
          {loading ? <p>Loading sessions…</p> : filteredSessions.length === 0 ? <p>No sessions match this range and filter.</p> : filteredSessions.map((session) => (
            <button className={selected === session.session_id ? "tle-selected" : ""} key={session.session_id} onClick={() => { setSelected(session.session_id); onSessionOpen?.(session.session_id); }}>
              <b>{session.session_id}</b>
              <span>{session.span_count} spans · {session.error_count} errors</span>
              <span>{session.models?.join(", ") || "No model recorded"}</span>
              <time>{new Date(session.start_time).toLocaleString()}</time>
            </button>
          ))}
          {cursor && <button disabled={loadingMore} onClick={() => void loadMore()}>{loadingMore ? "Loading…" : "Load more sessions"}</button>}
        </aside>
        <section aria-label="Trace details">
          <h2>{selected ? `Session ${selected}` : "Select a session"}</h2>
          {selected && loadingDetail && <p>Loading traces and spans…</p>}
          {selected && !loadingDetail && observations.length === 0 && !error && <p>No observations in this session.</p>}
          {selected && traceGroups.map(([traceId, spans]) => (
            <div className="tle-trace" key={traceId}>
              <h3>Trace <code>{traceId}</code></h3>
              {spans.map((observation) => (
                <button className={`tle-span ${selectedSpan === observation.span_id ? "tle-span-selected" : ""}`} style={{ marginLeft: `${spanDepth(observation, observationById) * 18}px` }} key={observation.span_id} onClick={() => setSelectedSpan(observation.span_id)}>
                  <b>{observation.name}</b><span>{observation.observation_type}{observation.model_name ? ` · ${observation.model_name}` : ""}</span>
                  <span>{observation.status_code || "OK"} · {observation.total_tokens ?? ((observation.input_tokens ?? 0) + (observation.output_tokens ?? 0))} tokens</span>
                  <time>{observation.start_time}</time>
                </button>
              ))}
            </div>
          ))}
          {focused && <article className="tle-inspector">
            <div className="tle-inspector-title"><div><b>{focused.name}</b><span>{focused.observation_type} · {focused.span_id}</span></div><time>{focused.start_time}</time></div>
            <details open><summary>Input and output</summary><div className="tle-payload"><h4>Input</h4><pre>{json(focused.input)}</pre><h4>Output</h4><pre>{json(focused.output)}</pre></div></details>
            <details><summary>Attributes and events</summary><pre>{json({ attributes: focused.attributes, events: focused.events })}</pre></details>
            <details open><summary>Evaluation results ({focused.scores?.filter((score) => score.source === "evaluator").length ?? 0})</summary>
              {(focused.scores ?? []).filter((score) => score.source === "evaluator").length === 0 ? <p>No evaluator result on this span.</p> : (focused.scores ?? []).filter((score) => score.source === "evaluator").map((score) => <div className={`tle-eval-result tle-eval-${score.string_value ?? "unknown"}`} key={score.score_id}><b>{score.name}: {score.string_value ?? "unknown"}</b><p>{score.comment}</p></div>)}
            </details>
            <details><summary>All scores ({focused.scores?.length ?? 0})</summary><pre>{json(focused.scores ?? [])}</pre></details>
            <div className="tle-score">{(["correct", "wrong", "unsure"] as const).map((verdict) => <button key={verdict} disabled={saving} onClick={() => void saveVerdict(verdict)}>{saving ? "Saving…" : verdict}</button>)}</div>
          </article>}
        </section>
      </div>}
    </main>
  );
}
