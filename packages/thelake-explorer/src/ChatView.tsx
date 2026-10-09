import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { BehaviorEvaluator, ExplorerApi, Observation, ScoreRecord } from "./client";
import "./ChatView.css";

type ChatStage = "criteria" | "agent" | "confirm" | "monitoring";
type ChatMessage = { id: string; role: "assistant" | "user"; text: string };
type EvaluationIssue = {
  scoreId: string;
  sessionId: string;
  traceId: string;
  span: Observation;
  score: ScoreRecord;
};
type ChatThread = {
  id: string;
  title: string;
  stage: ChatStage;
  messages: ChatMessage[];
  criteria?: string;
  agentName?: string;
  evaluatorId?: string;
  consent: boolean;
  results?: EvaluationIssue[];
  refreshError?: string;
};

const STORAGE_KEY = "thelake.web-chats.v1";

function id(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function message(role: ChatMessage["role"], text: string): ChatMessage {
  return { id: id(), role, text };
}

function newThread(): ChatThread {
  return {
    id: id(),
    title: "New conversation",
    stage: "criteria",
    consent: false,
    messages: [
      message("assistant", "Welcome. Tell me a behavior you want to catch, and I’ll turn it into a check that runs on new agent traces."),
    ],
  };
}

function loadThreads(storageKey?: string): ChatThread[] {
  if (!storageKey) return [newThread()];
  if (typeof window === "undefined") return [newThread()];
  try {
    const stored = JSON.parse(window.localStorage.getItem(`${STORAGE_KEY}:${storageKey}`) ?? "[]") as unknown;
    if (Array.isArray(stored) && stored.length > 0 && stored.every(isChatThread)) return stored.map((thread) => ({ ...thread, consent: false }));
  } catch {
    // Start a clean conversation if a browser has an invalid prior draft.
  }
  return [newThread()];
}

function isChatThread(value: unknown): value is ChatThread {
  if (!value || typeof value !== "object") return false;
  const thread = value as Partial<ChatThread>;
  return typeof thread.id === "string"
    && typeof thread.title === "string"
    && ["criteria", "agent", "confirm", "monitoring"].includes(thread.stage ?? "")
    && Array.isArray(thread.messages)
    && thread.messages.every((item) => item && typeof item.id === "string" && (item.role === "assistant" || item.role === "user") && typeof item.text === "string")
    && typeof thread.consent === "boolean";
}

function checkName(criteria: string): string {
  const normalized = criteria.replace(/\s+/g, " ").trim();
  return normalized.length > 200 ? `${normalized.slice(0, 197)}…` : normalized;
}

export function ChatView({ api, storageKey, onOpenSession }: { api: ExplorerApi; storageKey?: string; onOpenSession: (sessionId: string) => void }) {
  const [initialThreads] = useState(() => loadThreads(storageKey));
  const [threads, setThreads] = useState<ChatThread[]>(initialThreads);
  const [activeId, setActiveId] = useState(initialThreads[0]?.id);
  const [draft, setDraft] = useState("");
  const [saving, setSaving] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [pendingActivation, setPendingActivation] = useState<BehaviorEvaluator>();
  const [activationConsent, setActivationConsent] = useState(false);
  const [activationError, setActivationError] = useState<string>();
  const [evaluators, setEvaluators] = useState<BehaviorEvaluator[]>([]);
  const composer = useRef<HTMLTextAreaElement>(null);
  const threadsRef = useRef(threads);
  const refreshingRef = useRef(false);
  threadsRef.current = threads;
  const active = threads.find((thread) => thread.id === activeId) ?? threads[0];

  useEffect(() => {
    if (!storageKey || typeof window === "undefined") return;
    try {
      window.localStorage.setItem(`${STORAGE_KEY}:${storageKey}`, JSON.stringify(threads.slice(-40).map((thread) => ({ ...thread, consent: false }))));
    } catch {
      // Chat history is a convenience; the evaluator and trace remain on theLake.
    }
  }, [storageKey, threads]);

  useEffect(() => {
    api.listEvaluators().then(setEvaluators).catch(() => setEvaluators([]));
  }, [api]);

  const activeEvaluatorName = useMemo(() => active?.criteria ? checkName(active.criteria) : "", [active?.criteria]);

  function updateThread(threadId: string, update: (thread: ChatThread) => ChatThread) {
    setThreads((current) => current.map((thread) => thread.id === threadId ? update(thread) : thread));
  }

  function appendAssistant(threadId: string, text: string) {
    updateThread(threadId, (thread) => ({ ...thread, messages: [...thread.messages, message("assistant", text)] }));
  }

  function startNewChat() {
    const thread = newThread();
    setThreads((current) => [...current, thread]);
    setActiveId(thread.id);
    setDraft("");
  }

  function startCheck() {
    composer.current?.focus();
  }

  function sendMessage() {
    const text = draft.trim();
    if (!text || !active) return;
    setDraft("");
    const threadId = active.id;
    const userMessage = message("user", text);
    if (active.stage === "criteria") {
      updateThread(threadId, (thread) => ({
        ...thread,
        title: text.length > 38 ? `${text.slice(0, 35)}…` : text,
        criteria: text,
        stage: "agent",
        messages: [...thread.messages, userMessage, message("assistant", "Which agent should I watch? Use the exact agent name recorded on its root trace." )],
      }));
      return;
    }
    if (active.stage === "agent") {
      updateThread(threadId, (thread) => ({
        ...thread,
        agentName: text,
        stage: "confirm",
        messages: [...thread.messages, userMessage, message("assistant", "I’ve prepared a behavior check. Review it below, then activate it when you’re ready.")],
      }));
      return;
    }
    updateThread(threadId, (thread) => ({
      ...thread,
      messages: [...thread.messages, userMessage, message("assistant", `I’m checking recent evaluation results for ${thread.agentName}. Open a result’s trace to inspect the evidence.`)],
    }));
    void refreshResults(threadId);
  }

  async function activateCheck() {
    if (!active?.criteria || !active.agentName || !active.consent || saving) return;
    const threadId = active.id;
    const evaluatorId = active.evaluatorId ?? `chat-${id().replace(/[^a-z0-9-]/gi, "").slice(0, 60)}`;
    setSaving(true);
    updateThread(threadId, (thread) => ({ ...thread, evaluatorId, refreshError: undefined }));
    try {
      const evaluator = await api.createEvaluator({
        evaluator_id: evaluatorId,
        version: 1,
        target_agent_name: active.agentName,
        name: activeEvaluatorName,
        criteria: active.criteria,
      });
      const activated = await api.setEvaluatorActive(evaluator.evaluator_id, evaluator.version, true);
      setEvaluators((current) => [activated, ...current.filter((item) => item.evaluator_id !== activated.evaluator_id || item.version !== activated.version)]);
      updateThread(threadId, (thread) => ({
        ...thread,
        stage: "monitoring",
        messages: [...thread.messages, message("assistant", `Behavior check is active for ${thread.agentName}. I’ll bring matching evaluation results into this conversation.`)],
      }));
    } catch (error) {
      appendAssistant(threadId, `I couldn’t activate the check: ${String(error)}`);
    } finally {
      setSaving(false);
    }
  }

  const refreshResults = useCallback(async (threadId = activeId) => {
    const thread = threadsRef.current.find((item) => item.id === threadId);
    if (!thread || thread.stage !== "monitoring" || refreshingRef.current || !thread.agentName || !thread.evaluatorId) return;
    refreshingRef.current = true;
    setRefreshing(true);
    updateThread(thread.id, (current) => ({ ...current, refreshError: undefined }));
    try {
      const { items } = await api.searchSessions(25, undefined, undefined, 7, thread.agentName);
      const details = await Promise.all(items.map(async (summary) => ({ summary, detail: await api.getSession(summary.session_id) })));
      const found = details.flatMap(({ summary, detail }) => detail.spans.flatMap((span) => (span.scores ?? [])
        .filter((score) => score.source === "evaluator" && score.config_id === `evaluator:${thread.evaluatorId}:v1`)
        .map((score) => ({ scoreId: score.score_id, sessionId: summary.session_id, traceId: span.trace_id, span, score }))));
      if (found.length) updateThread(thread.id, (current) => {
        const existing = new Set((current.results ?? []).map((result) => result.scoreId));
        const added = found.filter((result) => !existing.has(result.scoreId));
        return { ...current, results: [...(current.results ?? []), ...added].slice(-20), refreshError: undefined };
      });
    } catch (error) {
      updateThread(thread.id, (current) => ({ ...current, refreshError: String(error) }));
    } finally {
      refreshingRef.current = false;
      setRefreshing(false);
    }
  }, [api, activeId]);

  useEffect(() => {
    if (!active || active.stage !== "monitoring") return;
    const timer = window.setInterval(() => void refreshResults(active.id), 10_000);
    void refreshResults(active.id);
    return () => window.clearInterval(timer);
  }, [active?.id, active?.stage, refreshResults]);

  async function toggleEvaluator(evaluator: BehaviorEvaluator) {
    setActivationError(undefined);
    if (!evaluator.active) {
      setPendingActivation(evaluator);
      setActivationConsent(false);
      return;
    }
    try {
      const updated = await api.setEvaluatorActive(evaluator.evaluator_id, evaluator.version, false);
      setEvaluators((current) => current.map((item) => item.evaluator_id === updated.evaluator_id && item.version === updated.version ? updated : item));
    } catch (error) {
      setActivationError(`Could not pause ${evaluator.name}: ${String(error)}`);
    }
  }

  async function activateSavedEvaluator() {
    if (!pendingActivation || !activationConsent || saving) return;
    setSaving(true);
    setActivationError(undefined);
    try {
      const updated = await api.setEvaluatorActive(pendingActivation.evaluator_id, pendingActivation.version, true);
      setEvaluators((current) => current.map((item) => item.evaluator_id === updated.evaluator_id && item.version === updated.version ? updated : item));
      setPendingActivation(undefined);
      setActivationConsent(false);
    } catch (error) {
      setActivationError(`Could not activate ${pendingActivation.name}: ${String(error)}`);
    } finally {
      setSaving(false);
    }
  }

  return (
    <section className="tle-chat" aria-label="TheLake chat">
      <aside className="tle-chat-sidebar" aria-label="Conversations">
        <div className="tle-chat-sidebar-head"><h2>Conversations</h2><button onClick={startNewChat} aria-label="New conversation">＋</button></div>
        <button className="tle-new-chat" onClick={startNewChat}>＋ New conversation</button>
        <nav aria-label="Chat conversations">
          {[...threads].reverse().map((thread) => (
            <button key={thread.id} className={thread.id === active?.id ? "tle-chat-thread tle-chat-thread-active" : "tle-chat-thread"} onClick={() => { setActiveId(thread.id); setDraft(""); }}>
              <b>{thread.title}</b><span>{thread.stage === "monitoring" ? "Monitoring" : "In progress"}</span>
            </button>
          ))}
        </nav>
        <section className="tle-chat-checks" aria-label="Behavior checks">
          <h3>Behavior checks</h3>
          {evaluators.length === 0 ? <span>No checks yet</span> : evaluators.slice(0, 8).map((evaluator) => (
            <div key={`${evaluator.evaluator_id}:${evaluator.version}`}>
              <span><b>{evaluator.name}</b><small>{evaluator.target_agent_name} · {evaluator.active ? "active" : "paused"}</small></span>
              <button aria-label={`${evaluator.active ? "Pause" : "Activate"} ${evaluator.name}`} onClick={() => void toggleEvaluator(evaluator)}>{evaluator.active ? "Ⅱ" : "▶"}</button>
            </div>
          ))}
        </section>
        <div className="tle-chat-sidebar-note">Checks run on new traces. This chat shows results from the 25 most recent sessions; older scores stay with their traces.</div>
      </aside>
        <section className="tle-chat-main" aria-label="Conversation">
        <header className="tle-chat-heading"><div><h2>{active?.title ?? "Start a conversation"}</h2><span>Behavior check setup</span></div></header>
        <div className="tle-chat-transcript" aria-live="polite">
          {!active && <h2>Start a conversation</h2>}
          {active?.messages.map((item) => (
            <article key={item.id} className={`tle-chat-message tle-chat-${item.role}`}>
              <b>{item.role === "assistant" ? "theLake" : "You"}</b><p>{item.text}</p>
            </article>
          ))}
          {active?.stage === "criteria" && <div className="tle-chat-starters"><button onClick={startCheck}>Create a behavior check</button><span>Example: “Before issuing a refund, check eligibility.”</span></div>}
          {active?.stage === "confirm" && <article className="tle-chat-card" aria-label="Behavior check review">
            <h3>Review behavior check</h3>
            <dl><dt>Agent</dt><dd>{active.agentName}</dd><dt>What to check</dt><dd>{active.criteria}</dd><dt>Check name</dt><dd>{activeEvaluatorName}</dd></dl>
            <div className="tle-chat-consent"><b>Gemini data notice</b><p>Activating sends captured prompts, responses, and tool data for this agent to Gemini. Common credential patterns are redacted; general personal data is not.</p>
              <label><input type="checkbox" checked={active.consent} onChange={(event) => updateThread(active.id, (thread) => ({ ...thread, consent: event.target.checked }))} /> I understand and want to activate this check.</label>
            </div>
            <button className="tle-primary" disabled={!active.consent || saving} onClick={() => void activateCheck()}>{saving ? "Activating…" : "Activate check"}</button>
          </article>}
          {active?.stage === "monitoring" && <div className="tle-chat-monitoring"><span className="tle-status-dot" /> Monitoring new traces for <b>{active.agentName}</b>.
            <button disabled={refreshing} onClick={() => void refreshResults(active.id)}>{refreshing ? "Checking…" : "Refresh recent results"}</button>
          </div>}
          {active?.refreshError && <p className="tle-chat-error" role="alert">Could not check recent traces: {active.refreshError}</p>}
          {active?.results?.map((result) => <article className={`tle-chat-issue tle-result-${result.score.string_value ?? "unknown"}`} aria-label="Evaluation result" key={result.scoreId}>
            <div className="tle-chat-issue-title"><span className={`tle-status-result tle-status-${result.score.string_value ?? "unknown"}`}>{result.score.string_value ?? "unknown"}</span><h3>{result.score.string_value === "fail" ? "Behavior issue found" : "Evaluation result"}</h3></div>
            <p>{result.score.comment}</p>
            <dl><dt>Session</dt><dd>{result.sessionId}</dd><dt>Trace</dt><dd>{result.traceId}</dd><dt>Agent span</dt><dd>{result.span.name}</dd></dl>
            <button onClick={() => onOpenSession(result.sessionId)}>Open trace in Sessions</button>
          </article>)}
          {pendingActivation && <article className="tle-chat-card" aria-label="Reactivate behavior check">
            <h3>Reactivate behavior check</h3>
            <dl><dt>Agent</dt><dd>{pendingActivation.target_agent_name}</dd><dt>What to check</dt><dd>{pendingActivation.criteria}</dd></dl>
            <div className="tle-chat-consent"><b>Gemini data notice</b><p>Activating sends captured prompts, responses, and tool data for this agent to Gemini. Common credential patterns are redacted; general personal data is not.</p>
              <label><input type="checkbox" checked={activationConsent} onChange={(event) => setActivationConsent(event.target.checked)} /> I understand and want to activate this check.</label>
            </div>
            <button className="tle-primary" disabled={!activationConsent || saving} onClick={() => void activateSavedEvaluator()}>{saving ? "Activating…" : "Activate check"}</button>
            <button onClick={() => { setPendingActivation(undefined); setActivationConsent(false); }}>Cancel</button>
          </article>}
          {activationError && <p className="tle-chat-error" role="alert">{activationError}</p>}
        </div>
        <form className="tle-chat-composer" onSubmit={(event) => { event.preventDefault(); sendMessage(); }}>
          <textarea ref={composer} aria-label="Message theLake" placeholder={active?.stage === "agent" ? "Exact agent name from its traces" : "Describe the behavior you want to catch…"} value={draft} onChange={(event) => setDraft(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter" && !event.shiftKey) { event.preventDefault(); sendMessage(); } }} />
          <button className="tle-primary" type="submit" disabled={!draft.trim()}>Send message</button>
        </form>
      </section>
    </section>
  );
}
