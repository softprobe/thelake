import { useEffect, useMemo, useState, type CSSProperties, type MouseEvent } from "react";
import { ExplorerApi, type ExplorerConfig, type Observation, type SessionSummary } from "./client";
import { SoftprobeMark, SoftprobeWordmark } from "./brand/SoftprobeWordmark";
import { buildSpanTree, buildTrajectoryTree, filterSpanTree, flattenSpanTree, type SpanTreeNode } from "./trajectory";
import "./style.css";

export type ThelakeExplorerProps = {
  config: ExplorerConfig;
  className?: string;
  pageSize?: number;
  initialSessionId?: string;
  onSessionOpen?: (sessionId: string) => void;
  onBack?: () => void;
  lisaHref?: string;
};

function json(value: unknown): string {
  if (value == null) return "—";
  return typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

function midTruncate(value: string, head = 12, tail = 8): string {
  return value.length <= head + tail + 1 ? value : `${value.slice(0, head)}…${value.slice(-tail)}`;
}

function formatDuration(start: string, end?: string | null): string {
  if (!end) return "—";
  const ms = Date.parse(end) - Date.parse(start);
  if (!Number.isFinite(ms) || ms < 0) return "—";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(2)}s`;
  return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`;
}

function formatAbsolute(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? value : date.toLocaleString();
}

function formatRelative(value: string): string {
  const seconds = Math.round((Date.now() - Date.parse(value)) / 1000);
  if (!Number.isFinite(seconds)) return "";
  const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  if (Math.abs(seconds) < 60) return rtf.format(-seconds, "second");
  if (Math.abs(seconds) < 3600) return rtf.format(-Math.round(seconds / 60), "minute");
  if (Math.abs(seconds) < 86_400) return rtf.format(-Math.round(seconds / 3600), "hour");
  return rtf.format(-Math.round(seconds / 86_400), "day");
}

export function formatCost(value?: number | null): string {
  return value == null ? "—" : `$${value.toFixed(4)}`;
}

export function formatNumber(value?: number | null): string {
  return value == null ? "—" : value.toLocaleString();
}

function AppNavigation({ lisaHref, workspaceName }: { lisaHref?: string; workspaceName: string }) {
  return (
    <aside className="sidebar" aria-label="Session navigation">
      <div className="brand">
        <SoftprobeWordmark className="brand-logo-full" />
        <SoftprobeMark className="brand-logo-icon" />
      </div>
      <nav className="nav" aria-label="Applications">
        <div className="nav-label">Inspect</div>
        <a className="nav-item active" href={typeof window === "undefined" ? "/explorer/" : window.location.pathname} aria-current="page">
          <span className="nav-icon" aria-hidden="true">
            <svg viewBox="0 0 20 20"><path d="M5 4h10M5 8h10M5 12h10M5 16h10" /><circle cx="2.5" cy="4" r=".45" /><circle cx="2.5" cy="8" r=".45" /><circle cx="2.5" cy="12" r=".45" /><circle cx="2.5" cy="16" r=".45" /></svg>
          </span>
          <span>Sessions</span>
        </a>
      </nav>
      <div className="sidebar-foot">
        <div className="workspace">
          <div className="workspace-avatar" aria-hidden="true">T</div>
          <div className="workspace-name">{workspaceName}</div>
        </div>
      </div>
      {lisaHref ? <a className="sidebar-lisa-link" href={lisaHref} aria-label="Lisa">Open Lisa <span aria-hidden="true">↗</span></a> : null}
    </aside>
  );
}

function SessionTable({
  sessions,
  selected,
  loading,
  onOpen,
}: {
  sessions: SessionSummary[];
  selected?: string;
  loading: boolean;
  onOpen: (sessionId: string, event: MouseEvent<HTMLAnchorElement>) => void;
}) {
  return (
    <div className="table-scroll">
        <table className="session-table" aria-label="Session results table">
          <thead>
            <tr>
              <th>SESSION</th><th>AGENT</th><th>DATE</th><th>ACTIVITY</th><th>FINDING</th><th aria-label="Open session" />
            </tr>
          </thead>
          <tbody>
            {sessions.map((session) => (
              <tr key={session.session_id} data-severity={session.error_count > 0 ? "error" : undefined} className={selected === session.session_id ? "is-selected" : undefined}>
                <td className="session-id-cell">
                  <a href={`?session=${encodeURIComponent(session.session_id)}`} title={session.session_id}
                    onClick={(event) => onOpen(session.session_id, event)}>
                    <span className="session-primary">{session.agent_name ? `${session.agent_name} session` : "Captured session"}</span>
                    <span className="session-secondary">{session.session_id}</span>
                  </a>
                </td>
                <td className="truncate" title={session.agent_name ?? "—"}>{session.agent_name || "—"}</td>
                <td><span className="observed-date">{formatAbsolute(session.start_time)}</span><span className="observed-time">{formatRelative(session.start_time)}</span></td>
                <td><span className="activity-main">{session.span_count.toLocaleString()} nodes</span>{session.error_count > 0 ? <span className="activity-sub">{session.error_count} error{session.error_count === 1 ? "" : "s"}</span> : null}</td>
                <td><span className="finding-tag" title="Finding review is not available for these sessions">—</span></td>
                <td className="row-chevron" aria-hidden="true">›</td>
              </tr>
            ))}
            {sessions.length === 0 ? <tr className="session-table-empty"><td colSpan={6}>{loading ? "Loading sessions…" : "No sessions match this range and filter."}</td></tr> : null}
          </tbody>
        </table>
    </div>
  );
}

export function ThelakeExplorer({
  config,
  className,
  pageSize = 50,
  initialSessionId,
  onSessionOpen,
  onBack,
  lisaHref,
}: ThelakeExplorerProps) {
  const api = useMemo(() => new ExplorerApi(config), [config]);
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [selected, setSelected] = useState<string | undefined>(() => initialSessionId ?? (typeof window === "undefined" ? undefined : new URLSearchParams(window.location.search).get("session") ?? undefined));
  const [observations, setObservations] = useState<Observation[]>([]);
  const [selectedSpan, setSelectedSpan] = useState<string>();
  const [viewMode, setViewMode] = useState<"trajectory" | "trace" | "evidence">("trajectory");
  const [trajectorySearch, setTrajectorySearch] = useState("");
  const [expandedSpans, setExpandedSpans] = useState<Set<string>>();
  const [cursor, setCursor] = useState<string>();
  const [days, setDays] = useState(7);
  const [agentName, setAgentName] = useState("");
  const [filterOpen, setFilterOpen] = useState(false);
  const [loading, setLoading] = useState(true);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [saving, setSaving] = useState(false);
  const [refreshSequence, setRefreshSequence] = useState(0);
  const [error, setError] = useState<string>();

  useEffect(() => {
    setSelected(initialSessionId ?? (typeof window === "undefined" ? undefined : new URLSearchParams(window.location.search).get("session") ?? undefined));
  }, [initialSessionId]);

  useEffect(() => {
    function syncFromLocation() {
      setSelected(new URLSearchParams(window.location.search).get("session") ?? initialSessionId ?? undefined);
    }
    window.addEventListener("popstate", syncFromLocation);
    return () => window.removeEventListener("popstate", syncFromLocation);
  }, [initialSessionId]);

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
  }, [api, pageSize, days, agentName, refreshSequence]);

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

  const filteredSessions = sessions;
  const observationById = useMemo(() => new Map(observations.map((observation) => [observation.span_id, observation])), [observations]);
  const focused = selectedSpan ? observationById.get(selectedSpan) : undefined;
  const spanTree = useMemo(() => {
    if (viewMode === "evidence") return [];
    const tree = viewMode === "trajectory" ? buildTrajectoryTree(observations) : buildSpanTree(observations);
    return filterSpanTree(tree, trajectorySearch);
  }, [viewMode, observations, trajectorySearch]);
  const visibleNodes = useMemo(() => flattenSpanTree(spanTree), [spanTree]);
  const expandableSpanIds = useMemo(() => visibleNodes.filter((node) => node.children.length).map((node) => node.key), [visibleNodes]);
  const effectiveExpandedSpans = expandedSpans ?? new Set(expandableSpanIds);
  const selectedSummary = sessions.find((session) => session.session_id === selected);
  const errorSessions = sessions.filter((session) => session.error_count > 0).length;
  const spanTotal = sessions.reduce((total, session) => total + session.span_count, 0);
  const tokenTotal = sessions.reduce((total, session) => total + (session.total_tokens ?? 0), 0);

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

  function openSession(sessionId: string, event: MouseEvent<HTMLAnchorElement>) {
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    if (!onSessionOpen) {
      const url = new URL(window.location.href);
      url.searchParams.set("session", sessionId);
      window.history.pushState({ sessionId }, "", url);
    }
    setSelected(sessionId);
    onSessionOpen?.(sessionId);
  }

  function goBack() {
    if (onBack) { onBack(); return; }
    const url = new URL(window.location.href);
    url.searchParams.delete("session");
    window.history.replaceState({}, "", url);
    setSelected(undefined);
  }

  function renderSpanNodes(nodes: SpanTreeNode[], depth = 0) {
    return nodes.map((node) => {
      const span = node.span;
      const isOpen = effectiveExpandedSpans.has(node.key);
      const isSelected = node.selectable !== false && selectedSpan === span.span_id;
      const type = span.span_type.toLowerCase();
      const preview = span.output == null ? "" : typeof span.output === "string" ? span.output : json(span.output);
      return (
        <div className="trajectory-node-wrap" key={node.key}>
          <div className={`trajectory-node${isSelected ? " is-selected" : ""}${span.status_code === "ERROR" ? " is-error" : ""}`} style={{ "--depth": depth } as CSSProperties}>
            <button type="button" className="trajectory-node-select" onClick={() => { if (node.selectable !== false) setSelectedSpan(span.span_id); }} aria-current={isSelected ? "true" : undefined} aria-disabled={node.selectable === false || undefined}>
              <span className={`trajectory-node-kind span-type-${type}`} aria-hidden="true" />
              <span className="trajectory-node-copy"><b>{node.label}</b><small>{span.span_type}{span.model_name ? ` · ${span.model_name}` : ""}</small>
                {viewMode === "trajectory" && (node.preview || preview) ? <span className="trajectory-node-preview">{node.preview || preview}</span> : null}
              </span>
              {span.status_code === "ERROR" ? <span className="trajectory-failed">Failed</span> : null}
            </button>
            {node.children.length ? <button type="button" className={`trajectory-expand${isOpen ? " is-open" : ""}`} aria-label={`${isOpen ? "Collapse" : "Expand"} ${node.label}`} onClick={() => setExpandedSpans((current) => {
              const next = new Set(current ?? expandableSpanIds);
              if (next.has(node.key)) next.delete(node.key); else next.add(node.key);
              return next;
            })}>›</button> : null}
          </div>
          {isOpen && node.children.length ? <div className="trajectory-children">{renderSpanNodes(node.children, depth + 1)}</div> : null}
        </div>
      );
    });
  }

  return (
    <div className={`product-shell session-browser${className ? ` ${className}` : ""}`}>
      <AppNavigation lisaHref={lisaHref} workspaceName={config.workspaceName ?? "Workspace"} />
      <div className="product-main">
        <header className="product-topbar">
          <div className="crumb">TheLake / <strong>{selected ? "Session detail" : "Sessions"}</strong></div>
          <div className="top-actions">
            <span className="status-live"><span className="dot success" />{config.workspaceStatus ?? "Connected"}</span>
            {selected && <button type="button" className="btn small ghost" onClick={goBack}>← Sessions</button>}
          </div>
        </header>

        {error && <div role="alert" className="explorer-error">{error}</div>}

        {!selected ? (
          <main className="session-list-page">
            <div className="page-heading">
              <div><h1>Sessions</h1><p>Complete records of agent activity.</p></div>
              <button type="button" className="btn small" onClick={() => setRefreshSequence((value) => value + 1)} disabled={loading} aria-label="Refresh sessions">Refresh</button>
            </div>
            <div className="session-table-wrap">
              <div className="session-table-toolbar">
                <div className="session-filter-summary">
                  <label className="session-range-control"><span className="session-range-label">Range</span><select className="filter-select session-range-select" aria-label="Session time range" value={days} onChange={(event) => setDays(Number(event.target.value))}><option value={1}>24 hours</option><option value={7}>7 days</option><option value={30}>30 days</option><option value={90}>90 days</option></select></label>
                  {agentName ? <button type="button" className="filter-chip" onClick={() => setAgentName("")}><b>Agent:</b> {agentName} <span className="x">×</span></button> : <span className="filter-default">{days} days · All Agents</span>}
                </div>
                <div className="session-toolbar-actions">
                  {agentName ? <button type="button" className="btn ghost small" onClick={() => setAgentName("")}>Clear</button> : null}
                  <div className="filter-wrap">
                    <button type="button" className={`session-filter-trigger${agentName ? " filter-active" : ""}`} aria-expanded={filterOpen} onClick={() => setFilterOpen((open) => !open)}>
                      <svg viewBox="0 0 16 16" aria-hidden="true"><path d="M2 3h12l-4.5 5v4l-3 1V8Z" /></svg><span>Filter</span>{agentName ? <span className="filter-count">1</span> : null}
                    </button>
                    {filterOpen ? <div className="filter-menu open"><div className="filter-section"><div className="filter-menu-title">Agent</div><select className="filter-select" aria-label="Filter Sessions by Agent" value={agentName} onChange={(event) => setAgentName(event.target.value)}><option value="">All Agents</option>{[...new Set(sessions.map((session) => session.agent_name).filter((name): name is string => Boolean(name)))].sort().map((name) => <option key={name} value={name}>{name}</option>)}</select></div><div className="filter-section"><div className="filter-menu-title">Finding</div><span className="finding-unavailable">Finding filters are unavailable for these sessions.</span></div></div> : null}
                  </div>
                </div>
              </div>
              <SessionTable sessions={filteredSessions} selected={selected} loading={loading} onOpen={openSession} />
            </div>
            {cursor && <div className="load-more-row"><button className="btn small ghost" disabled={loadingMore} onClick={() => void loadMore()}>{loadingMore ? "Loading…" : "Load more sessions"}</button></div>}
          </main>
        ) : (
          <main className="session-detail-page">
            <div className="detail-heading">
              <div>
                <button type="button" className="back-link" onClick={goBack}>← All sessions</button>
                <h1 aria-label={`Session ${selected}`} title={selected}>{midTruncate(selected, 30, 16)}</h1>
                <div className="detail-meta"><span>{selectedSummary ? formatAbsolute(selectedSummary.start_time) : "Session"}</span>{selectedSummary && <><span>{selectedSummary.span_count} spans</span><span>{selectedSummary.error_count ? `${selectedSummary.error_count} errors` : "No span errors"}</span></>}</div>
              </div>
              <button type="button" className="btn small ghost copy-session" onClick={() => void navigator.clipboard?.writeText(selected)}>⧉ Copy session ID</button>
            </div>
            <details className="session-technical"><summary>Session details</summary><div className="technical-grid">
              <div><label>Model</label><span>{selectedSummary?.models?.[0] || observations.find((item) => item.model_name)?.model_name || "—"}</span></div>
              <div><label>Duration</label><span>{selectedSummary ? formatDuration(selectedSummary.start_time, selectedSummary.end_time) : "—"}</span></div>
              <div><label>Tokens</label><span>{formatNumber(selectedSummary?.total_tokens)}</span></div>
              <div><label>Source</label><span>TheLake</span></div>
              <div><label>Trace ID</label><span>{observations[0]?.trace_id || "—"}</span></div>
            </div></details>
            {loadingDetail ? <div className="detail-loading"><span className="loading-indicator" /> Loading session details…</div> : observations.length === 0 && !error ? <div className="session-empty-state"><b>No observations in this session</b></div> : (
              <div className="detail-grid">
                <section className="trajectory-panel" aria-label="Session traces and spans">
                  <div className="detail-panel-heading"><span>{viewMode.toUpperCase()}</span><span>{visibleNodes.length} nodes</span></div>
                  <div className="view-mode-switch" role="tablist" aria-label="Session view mode">
                    {(["trajectory", "trace", "evidence"] as const).map((mode) => <button key={mode} type="button" role="tab" aria-selected={viewMode === mode} className={`view-mode-tab${viewMode === mode ? " active" : ""}`} onClick={() => setViewMode(mode)}>{mode[0].toUpperCase() + mode.slice(1)}</button>)}
                  </div>
                  <div className="trajectory-toolbar">
                    <input aria-label="Search spans" placeholder={`Search ${viewMode}`} value={trajectorySearch} onChange={(event) => setTrajectorySearch(event.target.value)} />
                    <button type="button" className="btn small ghost" onClick={() => setExpandedSpans(new Set(expandableSpanIds))}>Expand all</button>
                    <button type="button" className="btn small ghost" onClick={() => setExpandedSpans(new Set())}>Collapse all</button>
                  </div>
                  <div className="trajectory-tree">
                    {viewMode === "evidence" ? <div className="session-empty-state"><b>Finding evidence is unavailable</b><span>TheLake does not yet expose Findings for this session.</span></div> : visibleNodes.length ? renderSpanNodes(spanTree) : <div className="session-empty-state">No spans match this search.</div>}
                  </div>
                </section>
                <section className="inspector-panel" aria-label="Trace details">
                  {focused ? <>
                    <div className="inspector-heading"><div><span className="eyebrow">SPAN DETAILS</span><h2>{focused.name}</h2><p>{focused.span_type}{focused.model_name ? ` · ${focused.model_name}` : ""}</p></div><time>{formatAbsolute(focused.start_time)}</time></div>
                    <div className="inspector-stats"><div><span>DURATION</span><b>{formatDuration(focused.start_time, focused.end_time)}</b></div><div><span>TOKENS</span><b>{formatNumber(focused.total_tokens ?? ((focused.input_tokens ?? 0) + (focused.output_tokens ?? 0)))}</b></div><div><span>COST</span><b>{formatCost(focused.total_cost)}</b></div><div><span>STATUS</span><b className={focused.status_code === "ERROR" ? "text-error" : ""}>{focused.status_code || "OK"}</b></div></div>
                    <details className="inspector-section" open><summary>Input and output</summary><div className="payload-grid"><div><h3>Input</h3><pre>{json(focused.input)}</pre></div><div><h3>Output</h3><pre>{json(focused.output)}</pre></div></div></details>
                    <details className="inspector-section"><summary>Attributes and events</summary><pre>{json({ attributes: focused.attributes, events: focused.events })}</pre></details>
                    <details className="inspector-section" open><summary>Evaluation scores ({focused.scores?.length ?? 0})</summary>{(focused.scores ?? []).length === 0 ? <p className="muted-copy">No evaluation scores recorded on this span.</p> : (focused.scores ?? []).map((score) => <div className="score-entry" key={score.score_id}><div><b>{score.name}</b><span>{score.string_value ?? score.numeric_value ?? String(score.boolean_value ?? "")}</span></div>{score.comment ? <p>{score.comment}</p> : null}</div>)}</details>
                    <div className="human-review"><span>HUMAN REVIEW</span>{(["correct", "wrong", "unsure"] as const).map((verdict) => <button className="btn small ghost" key={verdict} disabled={saving} onClick={() => void saveVerdict(verdict)}>{saving ? "Saving…" : verdict}</button>)}</div>
                  </> : <div className="session-empty-state">Select a span to inspect it.</div>}
                </section>
              </div>
            )}
          </main>
        )}
      </div>
    </div>
  );
}
