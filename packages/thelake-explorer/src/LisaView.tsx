import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AssistantRuntimeProvider,
  ComposerPrimitive,
  MessagePrimitive,
  ThreadPrimitive,
  useExternalStoreRuntime,
  type AppendMessage,
  type EmptyMessagePartProps,
  type TextMessagePartProps,
  type ThreadMessageLike,
  type ToolCallMessagePartProps,
} from "@assistant-ui/react";
import type { BehaviorEvaluator, ExplorerApi, LisaActivity, LisaConversation, LisaMessage } from "./client";
import { LisaMessageContent } from "./LisaMessageContent";
import "./LisaView.css";

type ConversationView = LisaConversation & { activities: LisaActivity[] };

type TimedMessage = {
  order: number;
  timestamp: number;
  sequence?: number;
  message: ThreadMessageLike;
};

function messageTime(value?: string): number {
  if (!value) return Number.POSITIVE_INFINITY;
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) return Number.POSITIVE_INFINITY;

  // Date.parse keeps only milliseconds. Recover the remaining fractional
  // precision so events from older histories can still be ordered reliably.
  const fraction = value.match(/\.(\d+)(?=Z|[+-]\d{2}:?\d{2}$)/)?.[1];
  if (!fraction) return timestamp;
  const parsedMilliseconds = Number((fraction + "000").slice(0, 3));
  return timestamp - parsedMilliseconds + Number(`0.${fraction}`) * 1_000;
}

function parseArguments(value?: string): Record<string, any> {
  if (!value) return {};
  try {
    const parsed: unknown = JSON.parse(value);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, any>;
    }
    return { value: parsed };
  } catch {
    return { value };
  }
}

function redactInternalToolFields(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(redactInternalToolFields);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).filter(([key]) => key !== "failure_key")
        .map(([key, nested]) => [key, redactInternalToolFields(nested)]),
    );
  }
  return value;
}

function displayToolText(value?: string): string {
  if (!value) return "";
  try {
    return JSON.stringify(redactInternalToolFields(JSON.parse(value)), null, 2) ?? value;
  } catch {
    return value;
  }
}

function assistantMessages(messages: LisaMessage[], activities: LisaActivity[], running: boolean): ThreadMessageLike[] {
  const observations = new Map(
    activities
      .filter((activity) => activity.kind === "evidence" && activity.tool_call_id)
      .map((activity) => [activity.tool_call_id!, activity]),
  );
  const timeline: TimedMessage[] = messages.map((message, order) => ({
    order,
    timestamp: messageTime(message.created_at),
    sequence: message.sequence,
    message: {
      id: message.id,
      role: message.role,
      content: [{ type: "text", text: message.text }],
      createdAt: message.created_at ? new Date(message.created_at) : undefined,
    },
  }));

  activities.forEach((activity, index) => {
    if (activity.kind === "tool") {
      const toolCallId = activity.tool_call_id ?? activity.id;
      const observation = observations.get(toolCallId);
      const toolName = activity.tool_name ?? activity.label.replace(/^Lisa used /, "");
      timeline.push({
        order: messages.length + index,
        timestamp: messageTime(activity.created_at),
        sequence: activity.sequence,
        message: {
          id: activity.id,
          role: "assistant",
          content: [{
            type: "tool-call",
            toolCallId,
            toolName,
            args: redactInternalToolFields(parseArguments(activity.arguments)) as Record<string, any>,
            argsText: displayToolText(activity.arguments),
            result: displayToolText(observation?.detail),
          }],
          createdAt: activity.created_at ? new Date(activity.created_at) : undefined,
        },
      });
      return;
    }

    if (activity.kind === "evidence" && activity.tool_call_id && activities.some((item) => item.kind === "tool" && item.tool_call_id === activity.tool_call_id)) return;

    const detail = activity.detail ? `\n\n\`\`\`text\n${activity.detail}\n\`\`\`` : "";
    timeline.push({
      order: messages.length + index,
      timestamp: messageTime(activity.created_at),
      sequence: activity.sequence,
      message: {
        id: activity.id,
        role: "assistant",
        content: [{ type: "text", text: `${activity.label}${detail}` }],
        createdAt: activity.created_at ? new Date(activity.created_at) : undefined,
      },
    });
  });

  if (running) {
    timeline.push({
      order: timeline.length,
      timestamp: Date.now(),
      message: {
        id: "lisa-working",
        role: "assistant",
        content: [],
        status: { type: "running" },
      },
    });
  }

  return timeline
    .sort((left, right) => {
      const byTime = left.timestamp - right.timestamp;
      if (Number.isFinite(byTime) && byTime !== 0) return byTime;
      if (left.timestamp !== right.timestamp) return left.timestamp < right.timestamp ? -1 : 1;
      const leftOrder = left.sequence ?? Number.MAX_SAFE_INTEGER / 2 + left.order;
      const rightOrder = right.sequence ?? Number.MAX_SAFE_INTEGER / 2 + right.order;
      return leftOrder - rightOrder;
    })
    .map(({ message }) => message);
}

function LisaTextPart({ text }: TextMessagePartProps) {
  return <LisaMessageContent content={text} />;
}

function LisaToolCall({ toolName, argsText, args, result, isError }: ToolCallMessagePartProps) {
  return (
    <details className="tle-lisa-tool">
      <summary>Lisa used {toolName}</summary>
      <div className="tle-lisa-tool-content">
        <b>Input</b>
        <pre>{argsText || JSON.stringify(args, null, 2)}</pre>
        {result !== undefined && <><b>{isError ? "Error" : "Result"}</b><pre>{typeof result === "string" ? result : JSON.stringify(result, null, 2)}</pre></>}
      </div>
    </details>
  );
}

function LisaEmptyPart({ status }: EmptyMessagePartProps) {
  return status.type === "running" ? (
    <span className="tle-lisa-working" role="status"><span className="tle-lisa-spinner" /> Lisa is reviewing workspace evidence…</span>
  ) : null;
}

function LisaRuntimeMessage({ role }: { role: string }) {
  const user = role === "user";
  return (
    <MessagePrimitive.Root className={`tle-lisa-message ${user ? "tle-lisa-user" : "tle-lisa-assistant"}`}>
      <b>{user ? "You" : "Lisa"}</b>
      <MessagePrimitive.Content components={{
        Text: LisaTextPart,
        Empty: LisaEmptyPart,
        tools: { Fallback: LisaToolCall },
      }} />
    </MessagePrimitive.Root>
  );
}

function messageText(message: AppendMessage): string {
  return message.content
    .filter((part) => part.type === "text")
    .map((part) => part.text)
    .join("")
    .trim();
}

export function LisaView({ api }: { api: ExplorerApi }) {
  const [conversations, setConversations] = useState<ConversationView[]>([]);
  const [evaluators, setEvaluators] = useState<BehaviorEvaluator[]>([]);
  const [activeId, setActiveId] = useState<string>();
  const [dataConsent, setDataConsent] = useState(false);
  const [loading, setLoading] = useState(true);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string>();
  const loadedConversations = useRef(new Set<string>());
  const active = conversations.find((conversation) => conversation.conversation_id === activeId);
  const runtimeMessages = useMemo(
    () => assistantMessages(active?.messages ?? [], active?.activities ?? [], sending),
    [active?.messages, active?.activities, sending],
  );

  const refresh = useCallback(async () => {
    try {
      setError(undefined);
      const rows = await api.listLisaConversations();
      const checks = await api.listEvaluators();
      setEvaluators(checks);
      setConversations((current) => rows.map((row) => {
        const cached = current.find((item) => item.conversation_id === row.conversation_id);
        return { ...row, messages: cached?.messages ?? row.messages ?? [], activities: cached?.activities ?? row.activities ?? [] };
      }));
      if (rows.length === 0) {
        const created = await api.createLisaConversation();
        loadedConversations.current.add(created.conversation_id);
        setConversations([{ ...created, activities: [] }]);
        setActiveId(created.conversation_id);
      } else {
        setActiveId((current) => rows.some((row) => row.conversation_id === current) ? current : rows[0].conversation_id);
      }
    } catch (cause) {
      setError(`Lisa could not connect: ${String(cause)}`);
    } finally {
      setLoading(false);
    }
  }, [api]);

  useEffect(() => { void refresh(); }, [refresh]);

  useEffect(() => {
    const timer = window.setInterval(() => { void refresh(); }, 10_000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  useEffect(() => {
    if (loading || !activeId || loadedConversations.current.has(activeId)) return;
    loadedConversations.current.add(activeId);
    let cancelled = false;
    void api.getLisaConversation(activeId).then((loaded) => {
      if (cancelled) return;
      setConversations((current) => current.map((item) => item.conversation_id === activeId
        ? { ...item, ...loaded, activities: loaded.activities ?? item.activities }
        : item));
    }).catch((cause) => {
      loadedConversations.current.delete(activeId);
      if (!cancelled) setError(`Could not load this Lisa conversation: ${String(cause)}`);
    });
    return () => { cancelled = true; };
  }, [activeId, api, loading]);

  useEffect(() => {
    if (!activeId || sending || loading) return;
    let cancelled = false;
    const timer = window.setInterval(() => {
      void api.getLisaConversation(activeId).then((loaded) => {
        if (cancelled) return;
        setConversations((current) => current.map((item) => item.conversation_id === activeId
          ? { ...item, ...loaded, activities: loaded.activities ?? item.activities }
          : item));
      }).catch(() => undefined);
    }, 10_000);
    return () => { cancelled = true; window.clearInterval(timer); };
  }, [activeId, api, loading, sending]);

  const onNew = useCallback(async (message: AppendMessage) => {
    const text = messageText(message);
    if (!text || !active || !dataConsent || sending) return;
    const conversationId = active.conversation_id;
    const previousMessages = active.messages;
    const optimistic: LisaMessage = {
      id: `pending-${crypto.randomUUID()}`,
      role: "user",
      text,
      created_at: new Date().toISOString(),
    };
    setConversations((current) => current.map((item) => item.conversation_id === conversationId
      ? { ...item, messages: [...item.messages, optimistic] }
      : item));
    setSending(true);
    setError(undefined);
    try {
      const turn = await api.sendLisaMessage(conversationId, text, dataConsent);
      setConversations((current) => current.map((item) => item.conversation_id === turn.conversation_id
        ? {
          ...item,
          title: item.title === "Lisa conversation" ? text.slice(0, 60) : item.title,
          messages: turn.messages,
          activities: [...item.activities, ...turn.activities],
          updated_at: new Date().toISOString(),
        }
        : item));
    } catch (cause) {
      setConversations((current) => current.map((item) => item.conversation_id === conversationId
        ? { ...item, messages: previousMessages }
        : item));
      setError(`Lisa could not finish that turn: ${String(cause)}`);
    } finally {
      setSending(false);
    }
  }, [active, api, dataConsent, sending]);

  const runtime = useExternalStoreRuntime({
    messages: runtimeMessages,
    convertMessage: (message) => message,
    isRunning: sending,
    onNew,
  });

  async function startNewConversation() {
    setError(undefined);
    try {
      const created = await api.createLisaConversation();
      loadedConversations.current.add(created.conversation_id);
      setConversations((current) => [
        { ...created, activities: [] },
        ...current.filter((item) => item.conversation_id !== created.conversation_id),
      ]);
      setActiveId(created.conversation_id);
    } catch (cause) {
      setError(`Could not start a Lisa conversation: ${String(cause)}`);
    }
  }

  async function setEvaluatorActive(evaluator: BehaviorEvaluator) {
    const enabling = !evaluator.active;
    if (enabling && !window.confirm(
      `Activate “${evaluator.name}” for ${evaluator.target_agent_name}? Lisa will evaluate matching sessions on her schedule. The rubric and bounded trace evidence will be sent to Gemini for judgment.`,
    )) return;
    setError(undefined);
    try {
      await api.setEvaluatorActive(evaluator.evaluator_id, evaluator.version, enabling);
      setEvaluators(await api.listEvaluators());
    } catch (cause) {
      setError(`Could not ${enabling ? "activate" : "pause"} this evaluation: ${String(cause)}`);
    }
  }

  async function deleteConversation() {
    if (!active || !window.confirm("Permanently delete this Lisa conversation and its saved OpenHands state?")) return;
    const deletedId = active.conversation_id;
    setError(undefined);
    try {
      await api.deleteLisaConversation(deletedId);
      loadedConversations.current.delete(deletedId);
      const remaining = conversations.filter((item) => item.conversation_id !== deletedId);
      if (remaining.length > 0) {
        setConversations(remaining);
        setActiveId(remaining[0].conversation_id);
      } else {
        const created = await api.createLisaConversation();
        loadedConversations.current.add(created.conversation_id);
        setConversations([{ ...created, activities: [] }]);
        setActiveId(created.conversation_id);
      }
    } catch (cause) {
      setError(`Could not delete this Lisa conversation: ${String(cause)}`);
    }
  }

  return (
    <section className="tle-lisa" aria-label="Lisa conversation">
      <aside className="tle-lisa-sidebar" aria-label="Lisa conversations">
        <div className="tle-lisa-sidebar-heading"><h2>Lisa</h2><button onClick={() => void startNewConversation()} aria-label="New conversation">＋</button></div>
        <p className="tle-lisa-role">Agent QA engineer</p>
        <button className="tle-lisa-new" onClick={() => void startNewConversation()}>＋ New conversation</button>
        <nav aria-label="Conversations">
          {conversations.map((conversation) => (
            <button key={conversation.conversation_id}
              className={conversation.conversation_id === activeId ? "tle-lisa-thread tle-lisa-thread-active" : "tle-lisa-thread"}
              aria-current={conversation.conversation_id === activeId ? "page" : undefined}
              onClick={() => setActiveId(conversation.conversation_id)}>
              <b>{conversation.title}</b>
              <span>{conversation.event_status ? `Lisa report · ${conversation.event_status}` : conversation.messages.length ? `${conversation.messages.length} messages` : "New conversation"}</span>
            </button>
          ))}
        </nav>
        <p className="tle-lisa-scope">Lisa reviews agent sessions on her schedule, applies active checks, and investigates failures. She stores results in TheLake and can notify Slack when a check has a thread destination.</p>
        <details className="tle-lisa-checks" aria-label="Evaluations">
          <summary>Evaluations ({evaluators.length})</summary>
          {evaluators.length === 0 ? <p>No evaluations yet. Ask Lisa to propose one from an approved policy.</p> : evaluators.map((evaluator) => (
            <article className="tle-lisa-check" key={`${evaluator.evaluator_id}:${evaluator.version}`}>
              <b>{evaluator.name}</b><span>{evaluator.target_agent_name} · {evaluator.active ? "Active" : "Draft"}</span>
              <p>{evaluator.criteria}</p>
              <button onClick={() => void setEvaluatorActive(evaluator)}>{evaluator.active ? "Pause" : "Activate"}</button>
            </article>
          ))}
        </details>
      </aside>

      <section className="tle-lisa-main" aria-label="Chat with Lisa">
        <header className="tle-lisa-header">
          <div><h2>{active?.title ?? "Lisa"}</h2><span>Workspace agent · investigates and drafts checks</span></div>
          <div className="tle-lisa-header-actions">
            <button onClick={() => void refresh()} disabled={loading}>Refresh</button>
            <button onClick={() => void deleteConversation()} disabled={!active} aria-label="Delete Lisa conversation">Delete</button>
          </div>
        </header>
        <AssistantRuntimeProvider runtime={runtime}>
          <ThreadPrimitive.Root className="tle-lisa-thread-root" aria-label="Chat messages">
            <ThreadPrimitive.Viewport className="tle-lisa-transcript" autoScroll role="log" aria-live="polite" aria-relevant="additions text" aria-label="Lisa conversation">
              {active?.event_error && <p className="tle-lisa-error" role="alert">Investigation incomplete: {active.event_error}</p>}
              {loading && <p className="tle-lisa-empty">Connecting to Lisa…</p>}
              {!loading && runtimeMessages.length === 0 && <div className="tle-lisa-empty">
                <h3>Talk with Lisa about your agents</h3>
                <p>Ask her to review recent sessions, explain a behavior issue, or help you understand what the traces show.</p>
              </div>}
              <ThreadPrimitive.Messages>
                {({ message }) => <LisaRuntimeMessage key={message.id} role={message.role} />}
              </ThreadPrimitive.Messages>
            </ThreadPrimitive.Viewport>
            {error && <p className="tle-lisa-error" role="alert">{error}</p>}
            <ComposerPrimitive.Root className="tle-lisa-composer">
              <label className="tle-lisa-consent"><input type="checkbox" checked={dataConsent} onChange={(event) => setDataConsent(event.target.checked)} />
                Send my message and recent workspace telemetry to the configured model provider. Credential patterns are masked; other personal data may remain.
              </label>
              <ComposerPrimitive.Input aria-label="Message Lisa" placeholder="Ask Lisa about agent behavior or a session…" disabled={!active || loading || !dataConsent} />
              <ComposerPrimitive.Send className="tle-lisa-send" disabled={!active || loading || sending || !dataConsent}>{sending ? "Working…" : "Send"}</ComposerPrimitive.Send>
            </ComposerPrimitive.Root>
          </ThreadPrimitive.Root>
        </AssistantRuntimeProvider>
      </section>
    </section>
  );
}
