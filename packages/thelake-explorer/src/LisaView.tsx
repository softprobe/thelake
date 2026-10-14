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
import type { ExplorerApi, LisaActivity, LisaConversation, LisaMessage } from "./client";
import { LisaMessageContent } from "./LisaMessageContent";
import { lisaTimeline, toolActionLabel } from "./LisaTimeline";
import "./LisaView.css";

type ConversationView = LisaConversation & { activities: LisaActivity[] };

function LisaTextPart({ text }: TextMessagePartProps) {
  return <LisaMessageContent content={text} />;
}

function toolResultSummary(result: unknown, isError = false): string {
  if (isError) return "The tool reported an error";
  if (result === undefined || result === null || result === "") return "No result was recorded";
  if (typeof result === "string") {
    try {
      const parsed: unknown = JSON.parse(result);
      if (Array.isArray(parsed)) return `Returned ${parsed.length} items`;
      if (parsed && typeof parsed === "object") {
        const record = parsed as Record<string, unknown>;
        if (typeof record.error === "string") return `Error returned: ${record.error.slice(0, 90)}`;
        const array = Object.entries(record).find(([, value]) => Array.isArray(value));
        if (array) return `${array[0]}: ${(array[1] as unknown[]).length} items`;
      }
    } catch {
      // Plain-text results are shown as a short, literal excerpt below.
    }
    const excerpt = result
      .replace(/^\[Tool '[^']+' executed\.\]\s*/, "")
      .replace(/\s+/g, " ")
      .trim();
    return excerpt.length > 110 ? `${excerpt.slice(0, 107)}…` : excerpt;
  }
  return "Result captured";
}

function LisaToolCall({ toolName, argsText, args, result, isError }: ToolCallMessagePartProps) {
  return (
    <details className="tle-lisa-tool">
      <summary><b>{toolActionLabel(toolName)}</b><span>{toolResultSummary(result, isError)}</span></summary>
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
  const [activeId, setActiveId] = useState<string>();
  const [loading, setLoading] = useState(true);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string>();
  const loadedConversations = useRef(new Set<string>());
  const active = conversations.find((conversation) => conversation.conversation_id === activeId);
  const runtimeMessages = useMemo(
    () => lisaTimeline(active?.messages ?? [], active?.activities ?? [], sending),
    [active?.messages, active?.activities, sending],
  );

  const refresh = useCallback(async () => {
    try {
      setError(undefined);
      const rows = await api.listLisaConversations();
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
    if (!text || !active || sending) return;
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
      const turn = await api.sendLisaMessage(conversationId, text);
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
  }, [active, api, sending]);

  const runtime = useExternalStoreRuntime({
    messages: runtimeMessages,
    convertMessage: (message) => message,
    isRunning: sending,
    onNew,
  });

  return (
    <section className="tle-lisa" aria-label="Lisa">
      <section className="tle-lisa-main" aria-label="Chat with Lisa">
        <header className="tle-lisa-header">
          <div><h2>Lisa</h2><span>Your workspace agent · reviews approved policies and agent behavior</span></div>
          <div className="tle-lisa-header-actions">
            <button onClick={() => void refresh()} disabled={loading}>Refresh</button>
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
              <p className="tle-lisa-disclosure">Messages and recent workspace evidence are sent to your configured model provider. Common credentials are masked; personal data may remain.</p>
              <ComposerPrimitive.Input aria-label="Message Lisa" placeholder="Ask Lisa about agent behavior or a session…" disabled={!active || loading} />
              <ComposerPrimitive.Send className="tle-lisa-send" disabled={!active || loading || sending}>{sending ? "Working…" : "Send"}</ComposerPrimitive.Send>
            </ComposerPrimitive.Root>
          </ThreadPrimitive.Root>
        </AssistantRuntimeProvider>
      </section>
    </section>
  );
}
