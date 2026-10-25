import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
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
  type PartState,
} from "@assistant-ui/react";
import type { ExplorerApi, LisaActivity, LisaActivityDetail, LisaConversation, LisaFeedEntry, LisaMessage, LisaPolicyRecord } from "./client";
import { LisaMessageContent } from "./LisaMessageContent";
import { lisaTimeline, parseLisaToolResult, toolActionLabel } from "./LisaTimeline";
import "./LisaView.css";

type ConversationView = LisaConversation & { activities: LisaActivity[] };
type ActivityPanel = "activities" | "triage" | "findings";
type FeedStatus = { loading: boolean; error?: string };
type LisaPanelTab = "activities" | "rules" | "triage" | "findings" | "identity";

function LisaIcon({ name, size = 18 }: { name: LisaPanelTab | "refresh" | "check" | "clock" | "alert" | "stop" | "close" | "edit" | "dot" | "chat" | "search" | "documents" | "bulb" | "tasks" | "apps" | "analytics" | "theme" | "download" | "menu" | "gift" | "microphone"; size?: number }) {
  const museIcon = name === "activities"
    ? <><path d="M2.5 16.5C3.33 16.5 4 17.17 4 18C4 18.83 3.33 19.5 2.5 19.5C1.67 19.5 1 18.83 1 18C1 17.17 1.67 16.5 2.5 16.5Z"/><path d="M22 17.13C22.48 17.13 22.88 17.52 22.88 18C22.88 18.48 22.48 18.88 22 18.88H8C7.52 18.88 7.13 18.48 7.13 18C7.13 17.52 7.52 17.13 8 17.13H22Z"/><path d="M2.5 10.5C3.33 10.5 4 11.17 4 12C4 12.83 3.33 13.5 2.5 13.5C1.67 13.5 1 12.83 1 12C1 11.17 1.67 10.5 2.5 10.5Z"/><path d="M22 11.13C22.48 11.13 22.88 11.52 22.88 12C22.88 12.48 22.48 12.88 22 12.88H8C7.52 12.88 7.13 12.48 7.13 12C7.13 11.52 7.52 11.13 8 11.13H22Z"/><path d="M2.5 4.5C3.33 4.5 4 5.17 4 6C4 6.83 3.33 7.5 2.5 7.5C1.67 7.5 1 6.83 1 6C1 5.17 1.67 4.5 2.5 4.5Z"/><path d="M22 5.13C22.48 5.13 22.88 5.52 22.88 6C22.88 6.48 22.48 6.88 22 6.88H8C7.52 6.88 7.13 6.48 7.13 6C7.13 5.52 7.52 5.13 8 5.13H22Z"/></>
    : name === "rules"
      ? <g fill="none" stroke="currentColor" strokeWidth="1.55" strokeLinecap="round" strokeLinejoin="round"><path d="M12 2.2 20 5v6.1c0 5.1-3.5 8.4-8 10.7-4.5-2.3-8-5.6-8-10.7V5l8-2.8Z"/><path d="m8.5 11.7 2.2 2.2 4.8-5"/></g>
      : name === "triage"
        ? <><path d="M12 1.13c.73 0 1.43.07 2.12.2.48.1.78.56.69 1.03-.09.47-.55.78-1.03.69A9.2 9.2 0 0 0 12 2.88C6.96 2.88 2.88 6.96 2.88 12s4.08 9.13 9.12 9.13c.61 0 1.21-.06 1.78-.18.48-.09.94.22 1.03.69.09.47-.21.93-.69 1.03-.69.13-1.39.21-2.12.21C5.99 22.88 1.13 18.01 1.13 12S5.99 1.13 12 1.13Z"/><path d="M19.59 17.07c.27-.4.81-.51 1.21-.24.4.27.51.81.24 1.21a11.2 11.2 0 0 1-3 3c-.4.27-.94.16-1.21-.24-.27-.4-.16-.94.24-1.21a9.45 9.45 0 0 0 2.52-2.52Z"/><path d="M21.64 9.19c.47-.09.93.21 1.03.69.13.69.21 1.39.21 2.12s-.08 1.43-.21 2.12c-.1.48-.56.78-1.03.69-.47-.09-.78-.55-.69-1.03.12-.57.18-1.17.18-1.78s-.06-1.21-.18-1.78c-.09-.48.22-.94.69-1.03Z"/><path d="M12 5.13c.48 0 .88.39.88.87v6h-5.9c-.48 0-.87-.39-.87-.87s.39-.87.87-.87h4.13V6c0-.48.4-.87.89-.87Z"/><path d="M16.83 3.2c.27-.4.81-.51 1.21-.24a11.2 11.2 0 0 1 3 3c.27.4.16.94-.24 1.21-.4.27-.94.16-1.21-.24a9.45 9.45 0 0 0-2.52-2.52c-.4-.27-.51-.81-.24-1.21Z"/></>
        : name === "identity"
          ? <><path d="M12 8.13c2.69 0 4.87 2.18 4.87 4.87 0 2.31 1.25 4.44 3.26 5.57l.3.17c.42.23.57.77.33 1.19-.23.42-.77.57-1.19.33l-.29-.16A8.86 8.86 0 0 1 15.12 13c0-1.73-1.39-3.12-3.12-3.12S8.87 11.27 8.87 13v1.44c0 2.69 1.07 5.26 2.97 7.16l.78.78c.34.34.34.9 0 1.24-.34.34-.9.34-1.24 0l-.78-.78a11.88 11.88 0 0 1-3.48-8.4V13c0-2.69 2.19-4.87 4.88-4.87Z"/><path d="M12 4.13c4.58 0 8.35 3.47 8.83 7.92.05.56.12 1.04.27 1.49.11.32.29.61.53.85l.99.99c.34.34.34.9 0 1.24-.34.34-.9.34-1.24 0l-.99-.99a4.6 4.6 0 0 1-.95-1.54c-.21-.64-.29-1.28-.36-1.85-.37-3.58-3.4-6.36-7.08-6.36-3.94 0-7.13 3.18-7.13 7.12 0 2.57.38 5.12 1.12 7.58l.35 1.17c.14.46-.13.95-.59 1.09-.46.14-.95-.13-1.09-.59l-.35-1.17A28.04 28.04 0 0 1 3.12 13c0-4.9 3.98-8.87 8.88-8.87Z"/><path d="M12 12.13c.48 0 .87.39.87.87v1.1c.01 2.51 1.16 4.87 3.14 6.41l1.03.8c.38.3.45.85.15 1.23-.3.38-.85.45-1.23.15l-1.02-.8a10.05 10.05 0 0 1-3.82-7.79V13c0-.48.4-.87.88-.87Z"/><path d="M12 .13c3.86 0 7.56 1.88 9.73 5.13.27.41.16.95-.24 1.22-.41.27-.95.16-1.22-.24C18.44 3.49 15.3 1.88 12 1.88c-3.29 0-6.25 1.53-8.29 4.38-.28.39-.83.48-1.22.2-.39-.28-.48-.83-.2-1.22C4.64 1.96 8.12.13 12 .13Z"/></>
          : null;
  if (museIcon) return <svg width={size} height={size} viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">{museIcon}</svg>;
  const common = { width: size, height: size, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 1.8, strokeLinecap: "round" as const, strokeLinejoin: "round" as const, "aria-hidden": true as const };
  if (name === "activities") return <svg {...common}><path d="M8 6h12M8 12h12M8 18h12"/><path d="M3.5 6h.01M3.5 12h.01M3.5 18h.01"/></svg>;
  if (name === "rules") return <svg {...common}><path d="M12 3 20 6v5c0 5-3.5 8.2-8 10-4.5-1.8-8-5-8-10V6l8-3Z"/><path d="m9 12 2 2 4-4"/></svg>;
  if (name === "triage") return <svg {...common}><path d="M4 5h16M6 5l3 7h6l3-7M12 12v6M8 21h8M5 17h14"/></svg>;
  if (name === "identity") return <svg {...common}><circle cx="12" cy="8" r="3.2"/><path d="M5.5 20c.7-3.1 3.1-5 6.5-5s5.8 1.9 6.5 5"/><path d="M4 4.5a11 11 0 0 1 16 0M3 12a9 9 0 0 0 1.6 5.2"/></svg>;
  if (name === "refresh") return <svg {...common}><path d="M20 7v5h-5M4 17v-5h5"/><path d="M5.5 9a7 7 0 0 1 11.7-2L20 12M4 12l2.8 5a7 7 0 0 0 11.7-2"/></svg>;
  if (name === "check") return <svg {...common}><path d="m5 12 4 4L19 6"/></svg>;
  if (name === "alert") return <svg {...common}><path d="M10.3 4.7 2.9 17.5A1.7 1.7 0 0 0 4.4 20h15.2a1.7 1.7 0 0 0 1.5-2.5L13.7 4.7a2 2 0 0 0-3.4 0Z"/><path d="M12 9v4M12 16.5h.01"/></svg>;
  if (name === "stop") return <svg {...common}><rect x="5" y="5" width="14" height="14" rx="3"/></svg>;
  if (name === "close") return <svg {...common}><path d="m6 6 12 12M18 6 6 18"/></svg>;
  if (name === "edit") return <svg {...common}><path d="m14 5 5 5M4 20l4.2-.8L19 8.4 15.6 5 4.8 15.8 4 20Z"/></svg>;
  if (name === "dot") return <svg width={size} height={size} viewBox="0 0 14 14" fill="currentColor" aria-hidden="true"><circle cx="7" cy="7" r="2" /></svg>;
  if (name === "chat") return <svg {...common}><path d="M20 11.5a7.5 7.5 0 0 1-7.5 7.5H6l-3 2v-5a7.5 7.5 0 1 1 17-4.5Z"/></svg>;
  if (name === "search") return <svg {...common}><circle cx="10.8" cy="10.8" r="6.8"/><path d="m16 16 5 5"/></svg>;
  if (name === "documents") return <svg {...common}><rect x="5" y="4" width="14" height="17" rx="2"/><path d="M9 4V2h10a2 2 0 0 1 2 2v13h-2M8 9h8M8 13h8M8 17h5"/></svg>;
  if (name === "bulb") return <svg {...common}><path d="M9 18h6M10 21h4M8.2 14.7A7 7 0 1 1 16 14.6c-.9.7-1.2 1.4-1.2 2.4h-5.6c0-.9-.3-1.6-1-2.3Z"/></svg>;
  if (name === "tasks") return <svg {...common}><rect x="4" y="3" width="16" height="18" rx="2"/><path d="m8 10 2 2 5-5M8 16h8"/></svg>;
  if (name === "apps") return <svg {...common}><rect x="3" y="3" width="7" height="7" rx="2"/><rect x="14" y="3" width="7" height="7" rx="2"/><rect x="3" y="14" width="7" height="7" rx="2"/><rect x="14" y="14" width="7" height="7" rx="2"/></svg>;
  if (name === "analytics") return <svg {...common}><path d="M4 20V11M10 20V4M16 20v-7M22 20H2"/></svg>;
  if (name === "theme") return <svg {...common}><path d="M20.5 15.4A8.5 8.5 0 0 1 8.6 3.5 8.5 8.5 0 1 0 20.5 15.4Z"/></svg>;
  if (name === "download") return <svg {...common}><path d="M12 3v11m0 0 4-4m-4 4-4-4M4 17v4h16v-4"/></svg>;
  if (name === "menu") return <svg {...common}><path d="M4 7h16M4 12h16M4 17h16"/></svg>;
  if (name === "gift") return <svg {...common}><path d="M3 10h18v11H3zM2 6h20v4H2zM12 6v15M12 6H8.5a2.5 2.5 0 1 1 2.2-3.7L12 6Zm0 0h3.5a2.5 2.5 0 1 0-2.2-3.7L12 6Z"/></svg>;
  if (name === "microphone") return <svg {...common}><rect x="9" y="3" width="6" height="12" rx="3"/><path d="M5.5 11.5a6.5 6.5 0 0 0 13 0M12 18v3m-4 0h8"/></svg>;
  return <svg {...common}><circle cx="12" cy="12" r="8.5"/><path d="M12 7v5l3 2"/></svg>;
}

function LisaTextPart({ text }: TextMessagePartProps) {
  return <LisaMessageContent content={text} />;
}

function LisaToolCall({ toolName, argsText, args, isError, toolCallId }: ToolCallMessagePartProps) {
  return (
    <section className={`tle-lisa-tool${isError ? " tle-lisa-tool--error" : ""}`} aria-label={`${toolActionLabel(toolName)} tool call${isError ? ", failed" : ""}`}>
      <div className="tle-lisa-tool-heading"><b>{toolActionLabel(toolName)}</b><span>{isError ? "Call failed" : "Call"} · {toolCallId}</span></div>
      <div className="tle-lisa-tool-content">
        <b>Input</b>
        <pre>{argsText || JSON.stringify(args, null, 2)}</pre>
      </div>
    </section>
  );
}

function LisaToolResult({ toolCallId, toolName, detail, isError }: NonNullable<ReturnType<typeof parseLisaToolResult>>) {
  return (
    <section className={`tle-lisa-tool${isError ? " tle-lisa-tool--error" : ""}`} aria-label={`${toolActionLabel(toolName)} tool result${isError ? ", error" : ""}`}>
      <div className="tle-lisa-tool-heading"><b>{isError ? "Error" : "Result"} · {toolActionLabel(toolName)}</b><span>{toolCallId}</span></div>
      <div className="tle-lisa-tool-content"><pre>{detail || "No result was recorded"}</pre></div>
    </section>
  );
}

export function LisaToolTrajectory({ count, failed = false, children }: { count: number; failed?: boolean; children: ReactNode }) {
  return (
    <details className={`tle-lisa-trajectory${failed ? " tle-lisa-trajectory--error" : ""}`}>
      <summary>Lisa’s work · {count} {count === 1 ? "step" : "steps"}{failed ? " · includes an error" : ""} <span>View full trajectory</span></summary>
      <div className="tle-lisa-trajectory-content">{children}</div>
    </details>
  );
}

const groupLisaToolCalls = (part: PartState) => {
  if (part.type === "tool-call") {
    return [part.isError ? "group-tool-errors" : "group-tool-trajectory"] as const;
  }
  if (part.type === "text") {
    const result = parseLisaToolResult(part.text);
    if (result) return [result.isError ? "group-tool-errors" : "group-tool-trajectory"] as const;
  }
  return [];
};

function LisaEmptyPart({ status }: EmptyMessagePartProps) {
  return status.type === "running" ? (
    <span className="tle-lisa-working" role="status"><span className="tle-lisa-spinner" /> Lisa is reviewing workspace evidence…</span>
  ) : null;
}

function LisaRuntimeMessage({ role }: { role: string }) {
  const user = role === "user";
  return (
    <MessagePrimitive.Root className={`tle-lisa-message ${user ? "tle-lisa-user" : "tle-lisa-assistant"}`}>
      <MessagePrimitive.GroupedParts groupBy={groupLisaToolCalls}>
        {({ part, children }) => {
          if (part.type === "group-tool-trajectory" || part.type === "group-tool-errors") {
            return <LisaToolTrajectory count={part.indices.length} failed={part.type === "group-tool-errors"}>{children}</LisaToolTrajectory>;
          }
          if (part.type === "tool-call") return <LisaToolCall {...part} />;
          if (part.type === "text") {
            const result = parseLisaToolResult(part.text);
            if (result) return <LisaToolResult {...result} />;
            return <LisaTextPart text={part.text} />;
          }
          if (part.type === "indicator") return <LisaEmptyPart status={{ type: "running" }} />;
          return null;
        }}
      </MessagePrimitive.GroupedParts>
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

function activityTime(value: string): string {
  const date = new Date(value);
  const time = date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  return date.toDateString() === new Date().toDateString()
    ? time
    : `${date.toLocaleDateString([], { month: "short", day: "numeric" })} · ${time}`;
}

function policyFindingSummary(entry: Pick<LisaFeedEntry, "summary" | "subtitle">): string | undefined {
  return [entry.summary, entry.subtitle].find((value) => /^Policy violations? found:\s*.+$/i.test(value || "")) || undefined;
}

export function LisaView({ api, onShowSessions }: { api: ExplorerApi; onShowSessions?: () => void }) {
  const [conversations, setConversations] = useState<ConversationView[]>([]);
  const [activeId, setActiveId] = useState<string>();
  const [loading, setLoading] = useState(true);
  const [apiConnected, setApiConnected] = useState<boolean>();
  const [sending, setSending] = useState(false);
  const [hasDraft, setHasDraft] = useState(false);
  const [error, setError] = useState<string>();
  const [panelOpen, setPanelOpen] = useState(true);
  const [activePanel, setActivePanel] = useState<LisaPanelTab>("activities");
  const [activityFeeds, setActivityFeeds] = useState<Record<ActivityPanel, LisaFeedEntry[]>>({ activities: [], triage: [], findings: [] });
  const [policies, setPolicies] = useState<LisaPolicyRecord[]>([]);
  const [selectedActivity, setSelectedActivity] = useState<LisaActivityDetail>();
  const [selectedActionSeq, setSelectedActionSeq] = useState<number>();
  const [showActionDetail, setShowActionDetail] = useState(false);
  const [feedStatus, setFeedStatus] = useState<Record<ActivityPanel, FeedStatus>>({
    activities: { loading: false }, triage: { loading: false }, findings: { loading: false },
  });
  const [policiesLoading, setPoliciesLoading] = useState(false);
  const [policiesError, setPoliciesError] = useState<string>();
  const [nextPolicyOffset, setNextPolicyOffset] = useState<number | null>(null);
  const loadedConversations = useRef(new Set<string>());
  const feedRequestIds = useRef<Record<ActivityPanel, number>>({ activities: 0, triage: 0, findings: 0 });
  const activityDetailRequestId = useRef(0);
  const activityDialogRef = useRef<HTMLDivElement>(null);
  const previousActivityFocus = useRef<HTMLElement | null>(null);
  const active = conversations.find((conversation) => conversation.conversation_id === activeId);
  const selectedAction = showActionDetail ? selectedActivity?.actions.find((action) => action.seq === selectedActionSeq) : undefined;
  const selectedReferences = selectedAction?.reference_type && selectedAction.reference_id
    ? [{ reference_type: selectedAction.reference_type, reference_id: selectedAction.reference_id }]
    : selectedActivity?.references ?? [];
  const runtimeMessages = useMemo(
    () => lisaTimeline(active?.messages ?? [], active?.activities ?? [], sending),
    [active?.messages, active?.activities, sending],
  );

  const refreshFeed = useCallback(async (panel: ActivityPanel = "activities") => {
    const requestId = ++feedRequestIds.current[panel];
    setFeedStatus((current) => ({ ...current, [panel]: { loading: true, error: undefined } }));
    try {
      const activityTypes = panel === "activities" ? [undefined] : ["policy_evaluation", "session_evaluation"];
      const pages = await Promise.all(activityTypes.map((type) => api.listLisaActivities({
        limit: 100,
        activityType: type,
        query: panel === "findings" ? "Policy violation" : undefined,
      })));
      const allRows = [...new Map(pages.flat().map((row) => [row.id, row])).values()]
        .sort((left, right) => Date.parse(right.finished_at || right.updated_at || right.created_at)
          - Date.parse(left.finished_at || left.updated_at || left.created_at))
        .slice(0, 100);
      const rows = panel === "findings"
        ? allRows.filter((row) => policyFindingSummary(row))
        : allRows;
      if (requestId !== feedRequestIds.current[panel]) return;
      setActivityFeeds((current) => ({ ...current, [panel]: rows }));
    } catch (cause) {
      if (requestId === feedRequestIds.current[panel]) {
        setFeedStatus((current) => ({ ...current, [panel]: { loading: false, error: `Lisa activity feed could not load: ${String(cause)}` } }));
      }
    } finally {
      if (requestId === feedRequestIds.current[panel]) {
        setFeedStatus((current) => ({ ...current, [panel]: { ...current[panel], loading: false } }));
      }
    }
  }, [api]);

  const refreshPolicies = useCallback(async (offset = 0, append = false) => {
    setPoliciesLoading(true);
    setPoliciesError(undefined);
    try {
      const page = await api.listLisaPolicies({ limit: 100, offset });
      setPolicies((current) => append ? [...current, ...page.items] : page.items);
      setNextPolicyOffset(page.next_offset);
    } catch (cause) {
      setPoliciesError(`Lisa rules could not load: ${String(cause)}`);
    } finally {
      setPoliciesLoading(false);
    }
  }, [api]);

  const refreshPanel = useCallback(() => {
    if (activePanel === "activities" || activePanel === "triage" || activePanel === "findings") void refreshFeed(activePanel);
    if (activePanel === "rules") void refreshPolicies();
  }, [activePanel, refreshFeed, refreshPolicies]);

  const selectPanel = useCallback((tab: LisaPanelTab) => {
    if (tab !== activePanel) {
      activityDetailRequestId.current += 1;
      setSelectedActivity(undefined);
    }
    setActivePanel(tab);
    if (tab === "activities" || tab === "triage" || tab === "findings") void refreshFeed(tab);
    if (tab === "rules") void refreshPolicies();
  }, [activePanel, refreshFeed, refreshPolicies]);

  useEffect(() => { void refreshFeed(); }, [refreshFeed]);

  useEffect(() => {
    if (activePanel !== "activities" && activePanel !== "triage" && activePanel !== "findings") return;
    const timer = window.setInterval(() => {
      void refreshFeed(activePanel);
    }, 10_000);
    return () => window.clearInterval(timer);
  }, [activePanel, refreshFeed]);

  const openActivity = useCallback(async (activityId: string) => {
    const requestId = ++activityDetailRequestId.current;
    previousActivityFocus.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    try {
      const detail = await api.getLisaActivity(activityId);
      if (requestId !== activityDetailRequestId.current) return;
      setSelectedActivity(detail);
      setSelectedActionSeq(detail.actions.at(-1)?.seq);
      setShowActionDetail(false);
      const panel: ActivityPanel = activePanel === "triage" || activePanel === "findings" ? activePanel : "activities";
      setFeedStatus((current) => ({ ...current, [panel]: { ...current[panel], error: undefined } }));
    } catch (cause) {
      if (requestId !== activityDetailRequestId.current) return;
      const panel: ActivityPanel = activePanel === "triage" || activePanel === "findings" ? activePanel : "activities";
      setFeedStatus((current) => ({ ...current, [panel]: { ...current[panel], error: `Could not load activity: ${String(cause)}` } }));
    }
  }, [activePanel, api]);

  const closeActivityDetail = useCallback(() => {
    activityDetailRequestId.current += 1;
    setSelectedActivity(undefined);
    setSelectedActionSeq(undefined);
    setShowActionDetail(false);
  }, []);

  const refreshActivityDetail = useCallback(async (activityId: string) => {
    const requestId = activityDetailRequestId.current;
    try {
      const detail = await api.getLisaActivity(activityId);
      if (requestId !== activityDetailRequestId.current) return;
      setSelectedActivity(detail);
      if (!showActionDetail) setSelectedActionSeq(detail.actions.at(-1)?.seq);
    } catch {
      // Keep the last rendered detail visible; the next open retries the request.
    }
  }, [api, showActionDetail]);

  useEffect(() => {
    const activityId = selectedActivity?.activity.id;
    if (!activityId) return;
    const timer = window.setInterval(() => {
      void refreshActivityDetail(activityId);
    }, 10_000);
    return () => window.clearInterval(timer);
  }, [refreshActivityDetail, selectedActivity?.activity.id]);

  useEffect(() => {
    if (!selectedActivity) return;
    const dialog = activityDialogRef.current;
    const focusable = () => Array.from(dialog?.querySelectorAll<HTMLElement>("button:not(:disabled), [href], [tabindex]:not([tabindex='-1'])") ?? []);
    focusable()[0]?.focus();
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        closeActivityDetail();
      } else if (event.key === "Tab") {
        const items = focusable();
        const first = items[0];
        const last = items.at(-1);
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last?.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first?.focus();
        }
      }
    };
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("keydown", onKeyDown);
      previousActivityFocus.current?.focus();
      previousActivityFocus.current = null;
    };
  }, [closeActivityDetail, selectedActivity]);

  const refresh = useCallback(async () => {
    try {
      setError(undefined);
      const rows = await api.listLisaConversations();
      setApiConnected(true);
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
      setApiConnected(false);
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
      if (!cancelled) {
        setApiConnected(false);
        setError(`Could not load this Lisa conversation: ${String(cause)}`);
      }
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
        setApiConnected(true);
      }).catch(() => setApiConnected(false));
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
      setApiConnected(true);
      setConversations((current) => current.map((item) => item.conversation_id === turn.conversation_id
        ? {
          ...item,
          title: item.title === "Lisa conversation" ? text.slice(0, 60) : item.title,
          messages: turn.messages,
          activities: [...item.activities, ...turn.activities],
          updated_at: new Date().toISOString(),
        }
        : item));
      void refreshFeed();
      void refreshFeed("policy_evaluation");
      if (selectedActivity) void refreshActivityDetail(selectedActivity.activity.id);
    } catch (cause) {
      setApiConnected(false);
      setConversations((current) => current.map((item) => item.conversation_id === conversationId
        ? { ...item, messages: previousMessages }
        : item));
      setError(`Lisa could not finish that turn: ${String(cause)}`);
    } finally {
      setSending(false);
    }
  }, [active, api, refreshActivityDetail, refreshFeed, selectedActivity, sending]);

  const runtime = useExternalStoreRuntime({
    messages: runtimeMessages,
    convertMessage: (message) => message,
    isRunning: sending,
    onNew,
  });

  return (
    <section className={`tle-lisa ${panelOpen ? "panel-open" : "panel-closed"}`} aria-label="Lisa">
      <nav className="tle-lisa-rail" aria-label="Muse navigation">
        <button aria-label="Chats" className="active" disabled><LisaIcon name="chat" size={21} /></button>
        <button aria-label="Search" disabled><LisaIcon name="search" size={21} /></button>
        <button aria-label="Documents" disabled><LisaIcon name="documents" size={21} /></button>
        <button aria-label="Skills" disabled><LisaIcon name="bulb" size={21} /></button>
        <button aria-label="Tasks" disabled><LisaIcon name="tasks" size={21} /></button>
        <span />
        <button aria-label="Apps" disabled><LisaIcon name="apps" size={21} /></button>
        {onShowSessions && <button aria-label="Sessions" onClick={onShowSessions}><LisaIcon name="analytics" size={21} /></button>}
        <button aria-label="Theme" disabled><LisaIcon name="theme" size={21} /></button>
        <div className="tle-lisa-rail-footer">
          <button aria-label="Download" disabled><LisaIcon name="download" size={21} /></button>
          <button aria-label="Menu" disabled><LisaIcon name="menu" size={21} /></button>
        </div>
      </nav>
      <section className="tle-lisa-main" aria-label="Chat with Lisa">
        <header className="tle-lisa-header" aria-label="Chat controls">
          <button className="tle-lisa-chats-button" disabled><LisaIcon name="menu" size={16} /> Chats</button>
          <div className="tle-lisa-header-actions">
            {!panelOpen && <button className="tle-lisa-show-panel" aria-label="Show workspace panel" onClick={() => setPanelOpen(true)}><LisaIcon name="identity" size={17} /></button>}
            <button className="tle-lisa-invite-button" aria-label="Invite" disabled><LisaIcon name="gift" size={15} /> Invite</button>
            <button className="tle-lisa-refresh-chat" aria-label="Refresh chat" title="Refresh chat" onClick={() => void refresh()} disabled={loading}><LisaIcon name="refresh" size={16} /></button>
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
              <p className="tle-lisa-disclosure" id="tle-lisa-data-disclosure">Messages and recent workspace evidence are sent to your configured model provider. Common credentials are masked; personal data may remain.</p>
              <div className="tle-lisa-composer-field">
                <button className="tle-lisa-composer-add" aria-label="Add attachment" type="button" disabled><span aria-hidden="true">+</span></button>
                <ComposerPrimitive.Input aria-label="Message Lisa" aria-describedby="tle-lisa-data-disclosure" placeholder="Ask Lisa about agent behavior or a session…" disabled={!active || loading} onChange={(event) => setHasDraft(event.currentTarget.value.trim().length > 0)} />
                <ComposerPrimitive.Send className="tle-lisa-send" aria-label="Send" disabled={!active || loading || sending}>{sending ? "…" : hasDraft ? <svg width="18" height="18" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M5 12h14M13 6l6 6-6 6" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" /></svg> : <LisaIcon name="microphone" size={17} />}</ComposerPrimitive.Send>
              </div>
            </ComposerPrimitive.Root>
          </ThreadPrimitive.Root>
        </AssistantRuntimeProvider>
      </section>
      {panelOpen && <aside className="tle-lisa-panel" aria-label="Lisa workspace panel">
        <button className="tle-lisa-panel-close" aria-label="Close workspace panel" type="button" onClick={() => setPanelOpen(false)}><LisaIcon name="close" size={17} /></button>
        <div className="tle-lisa-panel-top">
          <div className="tle-lisa-panel-profile">
          <div className="tle-lisa-profile-avatar" aria-hidden="true"><span className="tle-lisa-avatar-crop"><img className="tle-lisa-avatar-art" src={new URL("./assets/lisa-avatar.webp", import.meta.url).href} alt="" /></span><i><LisaIcon name="edit" size={11} /></i></div>
            <h2>Lisa</h2>
            <p><span className={`tle-lisa-connected-mark ${loading || apiConnected === undefined ? "connecting" : apiConnected ? "connected" : "unavailable"}`} aria-hidden="true"><svg width="9" height="9" viewBox="0 0 10 10"><path d="M5.8.6 1.25 5.4h3L3.8 9.4l4.95-5.8H5.7Z" fill="currentColor" /></svg></span> {loading || apiConnected === undefined ? "Connecting" : apiConnected ? "Connected" : "Unavailable"}</p>
          </div>
          <div className="tle-lisa-panel-header">
            <div className="tle-lisa-panel-tabs" role="tablist" aria-label="Lisa workspace">
            {(["activities", "rules", "triage", "findings", "identity"] as const).map((tab) => <button
              key={tab}
              id={`tle-lisa-tab-${tab}`}
              role="tab"
              tabIndex={activePanel === tab ? 0 : -1}
              aria-label={tab === "triage" ? "Recent evaluations" : tab === "activities" ? "Activities" : tab[0].toUpperCase() + tab.slice(1)}
              title={tab === "triage" ? "Recent evaluations" : tab[0].toUpperCase() + tab.slice(1)}
              aria-selected={activePanel === tab}
              aria-controls="tle-lisa-panel-content"
              onClick={() => selectPanel(tab)}
              onKeyDown={(event) => {
                const tabs = ["activities", "rules", "triage", "findings", "identity"] as const;
                const currentIndex = tabs.indexOf(tab);
                const nextIndex = event.key === "ArrowRight" ? (currentIndex + 1) % tabs.length
                  : event.key === "ArrowLeft" ? (currentIndex + tabs.length - 1) % tabs.length
                    : event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : -1;
                if (nextIndex >= 0) {
                  event.preventDefault();
                  const nextTab = tabs[nextIndex];
                  document.getElementById(`tle-lisa-tab-${nextTab}`)?.focus();
                  selectPanel(nextTab);
                }
              }}
            >
              <LisaIcon name={tab === "findings" ? "alert" : tab} size={16} />
              <span>{tab === "activities" ? "Activity" : tab === "triage" ? "Evaluations" : tab[0].toUpperCase() + tab.slice(1)}</span>
            </button>)}
            </div>
            {activePanel !== "identity" && <button className="tle-lisa-panel-refresh" aria-label="Refresh panel" title="Refresh" onClick={refreshPanel} disabled={(activePanel === "rules" ? policiesLoading : activePanel === "activities" || activePanel === "triage" || activePanel === "findings" ? feedStatus[activePanel].loading : false)}><LisaIcon name="refresh" size={15} /></button>}
          </div>
        </div>
        <div className="tle-lisa-panel-content" id="tle-lisa-panel-content" role="tabpanel" tabIndex={0} aria-labelledby={`tle-lisa-tab-${activePanel}`}>
          {(activePanel === "activities" || activePanel === "triage" || activePanel === "findings") && <>
            <h3 className="tle-lisa-panel-section-title">{activePanel === "triage" ? "Recent evaluations" : activePanel === "findings" ? "Policy findings" : "Today"}</h3>
            {feedStatus[activePanel].loading && <p className="tle-lisa-panel-message">Loading {activePanel === "triage" ? "evaluations" : activePanel === "findings" ? "findings" : "activities"}…</p>}
            {feedStatus[activePanel].error && <p className="tle-lisa-error" role="alert">{feedStatus[activePanel].error}</p>}
            {!feedStatus[activePanel].loading && !feedStatus[activePanel].error && activityFeeds[activePanel].length === 0 && <p className="tle-lisa-panel-message">{activePanel === "triage" ? "No evaluation activity has been recorded yet." : activePanel === "findings" ? "No policy findings have been recorded." : "No activities yet."}</p>}
            <div className="tle-lisa-activity-list" aria-label={activePanel === "triage" ? "Lisa evaluations" : activePanel === "findings" ? "Lisa policy findings" : "Lisa activities"}>
              {activityFeeds[activePanel].map((item) => {
                const findingSummary = policyFindingSummary(item);
                const finding = activePanel === "triage" || activePanel === "findings"
                  ? findingSummary?.match(/^Policy violations? found:\s*(.+)$/i)
                  : null;
                const resultSummary = findingSummary || item.subtitle || item.summary || "";
                const findingLabel = finding && /^Policy violations/i.test(finding[0])
                  ? "Policy violations found"
                  : "Policy violation found";
                return <button key={item.id} className={`tle-lisa-activity-card ${finding ? "has-finding" : ""}`} onClick={() => void openActivity(item.id)}>
                  <span className={`tle-lisa-activity-icon status-${finding ? "finding" : item.status}`} aria-hidden="true"><LisaIcon name={finding || /error|fail/i.test(item.status) ? "alert" : /stopped/i.test(item.status) ? "stop" : /success|complete|approved/i.test(item.status) ? "check" : "clock"} size={15} /></span>
                  <span className="tle-lisa-activity-card-copy">
                    {finding && <span className="tle-lisa-evaluation-finding">{findingLabel}</span>}
                    <span className="tle-lisa-activity-card-title">{item.title}</span>
                    {resultSummary && <span className={`tle-lisa-activity-subtitle ${finding ? "tle-lisa-evaluation-finding-summary" : ""}`}>{finding ? finding[1] : resultSummary}</span>}
                    {item.next_action && <span className="tle-lisa-activity-next">Next: {item.next_action}</span>}
                    <span className="tle-lisa-activity-meta"><span className={`tle-lisa-activity-status status-${item.status}`}>{item.status_title || item.status}</span><time dateTime={item.updated_at || item.created_at}>{activityTime(item.updated_at || item.created_at)}</time></span>
                  </span>
                </button>;
              })}
            </div>
            {activePanel === "triage" && <p className="tle-lisa-panel-note">Evaluation status shows whether Lisa completed the run. Policy findings are listed separately.</p>}
          </>}
          {activePanel === "rules" && <>
            {policiesLoading && <p className="tle-lisa-panel-message">Loading rules…</p>}
            {policiesError && <p className="tle-lisa-error" role="alert">{policiesError}</p>}
            {!policiesLoading && !policiesError && policies.length === 0 && <p className="tle-lisa-panel-message">No policy rules have been recorded yet.</p>}
            {policies.map((policy) => <article className="tle-lisa-rule-card" key={`${policy.scope}-${policy.id}`}>
              <div><h3>{policy.name}</h3><span className={`tle-lisa-approval ${policy.approved ? "approved" : "pending"}`}>{policy.approved ? "Approved" : "Not approved"}</span></div>
              <p>{policy.criteria}</p>
              <small>{policy.scope === "agent" ? `Agent · ${policy.target_agent_name || "unnamed"}` : "Workspace"} · {policy.id}</small>
            </article>)}
            {nextPolicyOffset !== null && <button className="tle-lisa-load-more" onClick={() => void refreshPolicies(nextPolicyOffset, true)} disabled={policiesLoading}>Load more rules</button>}
            <p className="tle-lisa-panel-note">Create and update policies in Lisa chat. Approve an updated policy with its unchanged readable policy ID.</p>
          </>}
          {activePanel === "identity" && <div className="tle-lisa-identity">
            <div className="tle-lisa-identity-intro"><LisaIcon name="identity" size={18} /><div><h3>Identity & memory</h3><p>How Lisa is configured in this workspace</p></div></div>
            <article className="tle-lisa-identity-file"><header><b>SOUL.md</b><span>Core role and boundaries</span></header><p>Lisa's current identity is defined by her runtime system prompt and packaged skills. The standalone SOUL.md document has not been set up yet.</p></article>
            <article className="tle-lisa-identity-file"><header><b>MEMORY.md</b><span>Conversation context and durable records</span></header><p>Conversation history provides chat context. Policies and activity records are durable workspace records, not free-form memory. A standalone MEMORY.md file has not been set up yet.</p></article>
          </div>}
        </div>
      </aside>}
      {selectedActivity && <div className="tle-lisa-activity-overlay" onMouseDown={(event) => { if (event.target === event.currentTarget) closeActivityDetail(); }}>
        <div className="tle-lisa-activity-dialog" role="dialog" aria-modal="true" aria-labelledby="tle-lisa-activity-dialog-title" ref={activityDialogRef}>
          <header className="tle-lisa-activity-dialog-header">
            <span className={`tle-lisa-dialog-status status-${selectedActivity.activity.status}`}>{selectedActivity.activity.status_title || selectedActivity.activity.status}</span>
            <h2 id="tle-lisa-activity-dialog-title">{selectedActivity.activity.title}</h2>
            <button className="tle-lisa-activity-dialog-close" aria-label="Close activity details" onClick={closeActivityDetail}><LisaIcon name="close" size={18} /></button>
          </header>
          <div className="tle-lisa-activity-dialog-body">
            <nav className="tle-lisa-activity-dialog-timeline" aria-label="Activity timeline">
              <div className="tle-lisa-activity-steps">{selectedActivity.actions.length === 0
                ? <button className="tle-lisa-activity-step selected" aria-current="step" onClick={() => { setSelectedActionSeq(undefined); setShowActionDetail(false); }}><span className="tle-lisa-step-icon"><LisaIcon name="check" size={14} /></span><span>Summary</span></button>
                : selectedActivity.actions.map((action, index) => {
                  const failedAction = /error|fail/i.test(`${action.event_type} ${action.title}`);
                  return <button key={action.seq} className={`tle-lisa-activity-step ${showActionDetail ? action.seq === selectedActionSeq ? "selected" : "" : index === selectedActivity.actions.length - 1 ? "latest" : ""}`} aria-current={showActionDetail && action.seq === selectedActionSeq ? "step" : undefined} onClick={() => { setSelectedActionSeq(action.seq); setShowActionDetail(true); }}>
                    <span className={`tle-lisa-step-icon ${failedAction ? "failed" : index === 0 ? "start" : /success|complete|approved/i.test(selectedActivity.activity.status) ? "completed" : ""}`}><LisaIcon name={failedAction ? "alert" : index === 0 ? "dot" : "check"} size={14} /></span>
                    <span>{action.title}</span>
                  </button>;
                })}</div>
            </nav>
            <article className="tle-lisa-activity-dialog-summary" aria-live="polite">
              <h3>{selectedAction?.title ?? "Summary"}</h3>
              {(selectedAction?.summary || (!selectedAction && selectedActivity.activity.summary))
                ? <p>{selectedAction?.summary || selectedActivity.activity.summary}</p>
                : <p>No additional detail was recorded for this activity.</p>}
              {!selectedAction && selectedActivity.activity.next_action && <p><b>Next:</b> {selectedActivity.activity.next_action}{selectedActivity.activity.next_action_at ? ` · ${new Date(selectedActivity.activity.next_action_at).toLocaleString()}` : ""}</p>}
              {selectedAction && <time dateTime={selectedAction.created_at}>{new Date(selectedAction.created_at).toLocaleString()}</time>}
              {selectedActivity.thread.finish_message && <p>{selectedActivity.thread.finish_message}</p>}
              {selectedReferences.length > 0 && <section className="tle-lisa-activity-references" aria-label="Linked evidence">
                <h4>Linked evidence</h4>
                <ul>{selectedReferences.map((reference) => <li key={`${reference.reference_type}:${reference.reference_id}`}>
                  <span>{reference.reference_type.replaceAll("_", " ")}</span>
                  <code>{reference.reference_id}</code>
                </li>)}</ul>
              </section>}
            </article>
          </div>
        </div>
      </div>}
    </section>
  );
}
