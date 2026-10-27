import { describe, expect, it } from "vitest";
import { lisaTimeline, toolActionLabel } from "./LisaTimeline";
import type { LisaActivity, LisaMessage } from "./client";

describe("Lisa timeline", () => {
  it("shows tool names as plain action labels", () => {
    expect(toolActionLabel("search_sessions")).toBe("Search sessions");
    expect(toolActionLabel("list_approved_policies")).toBe("List approved policies");
  });

  it("uses the agent event sequence to keep messages and tool calls in conversation order", () => {
    const messages: LisaMessage[] = [
      { id: "user-1", role: "user", text: "Review this session", sequence: 1, created_at: "2026-10-13T12:00:00.100Z" },
      { id: "assistant-1", role: "assistant", text: "I found a failure.", sequence: 3, created_at: "2026-10-13T12:00:00.200Z" },
    ];
    const activities: LisaActivity[] = [
      { id: "tool-1", kind: "tool", label: "Lisa used search_sessions", tool_call_id: "call-1", tool_name: "search_sessions", sequence: 2, created_at: "2026-10-13T12:00:00.300Z" },
    ];

    expect(lisaTimeline(messages, activities).map((message) => message.id)).toEqual([
      "user-1",
      "activity-record-tool-1",
      "assistant-1",
    ]);
  });

  it("falls back to timestamps for older messages without event sequence numbers", () => {
    const messages: LisaMessage[] = [
      { id: "later", role: "assistant", text: "Done", created_at: "2026-10-13T12:00:02Z" },
      { id: "earlier", role: "user", text: "Start", created_at: "2026-10-13T12:00:01Z" },
    ];

    expect(lisaTimeline(messages, []).map((message) => message.id)).toEqual([
      "earlier",
      "later",
    ]);
  });

  it("places durable proactive updates in chronological chat order without changing transcript messages", () => {
    const timeline = lisaTimeline([
      { id: "user-1", role: "user", text: "Check recent sessions", sequence: 1, created_at: "2026-10-13T12:00:00Z" },
      { id: "answer-1", role: "assistant", text: "I reviewed the evidence.", sequence: 2, created_at: "2026-10-13T12:00:02Z" },
    ], [], false, [{ update: {
      id: "update-1", source_type: "policy_suggestion", source_id: "suggestion-1",
      created_at: "2026-10-13T12:00:01Z",
    } }]);

    expect(timeline.map((message) => message.id)).toEqual(["user-1", "proactive-update-1", "answer-1"]);
    expect(timeline[1].content[0]).toMatchObject({ type: "text" });
    expect(timeline[0].content).toEqual([{ type: "text", text: "Check recent sessions" }]);
  });

  it("gives mixed unsequenced, sequenced, and optimistic items a deterministic order", () => {
    const messages: LisaMessage[] = [
      { id: "seq-2", role: "assistant", text: "Later event", sequence: 2, created_at: "2026-10-13T12:00:01Z" },
      { id: "pending-user", role: "user", text: "New question", created_at: "2026-10-13T12:00:00Z" },
      { id: "unsequenced", role: "assistant", text: "Old history", created_at: "2026-10-13T12:00:03Z" },
      { id: "seq-1", role: "user", text: "Earlier event", sequence: 1, created_at: "2026-10-13T12:00:04Z" },
    ];

    expect(lisaTimeline(messages, [], true).map((message) => message.id)).toEqual([
      "unsequenced",
      "seq-1",
      "seq-2",
      "pending-user",
      "lisa-working",
    ]);
  });

  it("keeps out-of-order tool results at their recorded event positions", () => {
    const messages: LisaMessage[] = [
      { id: "user", role: "user", text: "Review refunds", sequence: 1 },
      { id: "answer", role: "assistant", text: "I found a violation.", sequence: 6 },
    ];
    const activities: LisaActivity[] = [
      { id: "call-1", kind: "tool", label: "Lisa used search_sessions", tool_call_id: "tool-1", tool_name: "search_sessions", arguments: "{\"agent\":\"refund-agent\"}", sequence: 2 },
      { id: "call-2", kind: "tool", label: "Lisa used search_sessions", tool_call_id: "tool-2", tool_name: "search_sessions", arguments: "{\"agent\":\"travel-agent\"}", sequence: 3 },
      { id: "result-2", kind: "evidence", label: "search_sessions returned evidence", tool_call_id: "tool-2", detail: "{\"sessions\":[{\"id\":\"s2\"},{\"id\":\"s3\"}]}", sequence: 4 },
      { id: "result-1", kind: "evidence", label: "search_sessions returned evidence", tool_call_id: "tool-1", detail: "{\"sessions\":[{\"id\":\"s1\"}]}", sequence: 5 },
    ];

    const timeline = lisaTimeline(messages, activities);
    expect(timeline.map((message) => message.id)).toEqual(["user", "activity-record-call-1", "answer"]);
    const orderedEvents = timeline[1].content.map((part) => part.type === "tool-call"
      ? `call:${part.toolCallId}`
      : part.type === "text" ? part.text : "unexpected");
    expect(orderedEvents[0]).toBe("call:tool-1");
    expect(orderedEvents[1]).toBe("call:tool-2");
    expect(orderedEvents[2]).toContain('"toolCallId":"tool-2"');
    expect(orderedEvents[2]).toContain("s2");
    expect(orderedEvents[3]).toContain('"toolCallId":"tool-1"');
    expect(orderedEvents[3]).toContain("s1");
  });

  it("keeps a tool result after assistant text that arrived before the result", () => {
    const timeline = lisaTimeline([
      { id: "user", role: "user", text: "Search", sequence: 1 },
      { id: "progress", role: "assistant", text: "I am checking the result.", sequence: 4 },
    ], [
      { id: "call", kind: "tool", label: "Lisa used search_sessions", tool_call_id: "tool-1", tool_name: "search_sessions", sequence: 2 },
      { id: "result", kind: "evidence", label: "search_sessions returned evidence", tool_call_id: "tool-1", detail: "found it", sequence: 5 },
    ]);

    expect(timeline.map((message) => message.id)).toEqual([
      "user", "activity-record-call", "progress", "activity-record-result",
    ]);
    expect(timeline[3].content[0].type === "text" && timeline[3].content[0].text).toContain("found it");
  });

  it("keeps tool failure state alongside its paired result", () => {
    const timeline = lisaTimeline([], [
      { id: "call", kind: "tool", label: "Lisa used get_session", tool_call_id: "tool-1", tool_name: "get_session", sequence: 1 },
      { id: "result", kind: "evidence", label: "get_session returned evidence", tool_call_id: "tool-1", detail: "not found", is_error: true, sequence: 2 },
    ]);
    const [call] = timeline[0].content;

    expect(call.type).toBe("tool-call");
    if (call.type === "tool-call") expect(call.isError).toBe(true);
  });

  it("keeps recorded assistant messages as visible narrative while excluding private think actions", () => {
    const messages: LisaMessage[] = [
      { id: "user", role: "user", text: "Review this", sequence: 1 },
      { id: "progress", role: "assistant", text: "I am checking the policy and trace evidence.", sequence: 2 },
      { id: "answer", role: "assistant", text: "I found a violation.", sequence: 5 },
    ];
    const activities: LisaActivity[] = [
      { id: "tool", kind: "tool", label: "Lisa used search_sessions", tool_call_id: "call", tool_name: "search_sessions", sequence: 3 },
    ];

    expect(lisaTimeline(messages, activities).map((message) => message.id)).toEqual([
      "user", "progress", "activity-record-tool", "answer",
    ]);
    expect(lisaTimeline(messages, activities).some((message) => JSON.stringify(message).includes("private think"))).toBe(false);
  });
});
