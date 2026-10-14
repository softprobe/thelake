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

  it("gives mixed legacy, sequenced, and optimistic items a deterministic order", () => {
    const messages: LisaMessage[] = [
      { id: "seq-2", role: "assistant", text: "Later event", sequence: 2, created_at: "2026-10-13T12:00:01Z" },
      { id: "pending-user", role: "user", text: "New question", created_at: "2026-10-13T12:00:00Z" },
      { id: "legacy", role: "assistant", text: "Old history", created_at: "2026-10-13T12:00:03Z" },
      { id: "seq-1", role: "user", text: "Earlier event", sequence: 1, created_at: "2026-10-13T12:00:04Z" },
    ];

    expect(lisaTimeline(messages, [], true).map((message) => message.id)).toEqual([
      "legacy",
      "seq-1",
      "seq-2",
      "pending-user",
      "lisa-working",
    ]);
  });

  it("hides scheduler instructions and labels Lisa's scheduled update", () => {
    const messages: LisaMessage[] = [
      { id: "internal-prompt", role: "user", text: "Internal schedule prompt", hidden: true, turn_source: "scheduled" },
      { id: "scheduled-report", role: "assistant", text: "One session needs review.", turn_source: "scheduled" },
    ];

    const timeline = lisaTimeline(messages, []);
    expect(timeline.map((message) => message.id)).toEqual(["scheduled-report"]);
    expect(timeline[0].content).toEqual([{ type: "text", text: "Scheduled QA update\n\nOne session needs review." }]);
  });

  it("groups tool events into one ordered activity record with each result paired to its call", () => {
    const messages: LisaMessage[] = [
      { id: "user", role: "user", text: "Review refunds", sequence: 1 },
      { id: "answer", role: "assistant", text: "I found a violation.", sequence: 6 },
    ];
    const activities: LisaActivity[] = [
      { id: "call-1", kind: "tool", label: "Lisa used search_sessions", tool_call_id: "tool-1", tool_name: "search_sessions", arguments: "{\"agent\":\"refund-agent\"}", sequence: 2 },
      { id: "result-1", kind: "evidence", label: "search_sessions returned evidence", tool_call_id: "tool-1", detail: "{\"sessions\":[{\"id\":\"s1\"}]}", sequence: 3 },
      { id: "call-2", kind: "tool", label: "Lisa used search_sessions", tool_call_id: "tool-2", tool_name: "search_sessions", arguments: "{\"agent\":\"travel-agent\"}", sequence: 4 },
      { id: "result-2", kind: "evidence", label: "search_sessions returned evidence", tool_call_id: "tool-2", detail: "{\"sessions\":[{\"id\":\"s2\"},{\"id\":\"s3\"}]}", sequence: 5 },
    ];

    const timeline = lisaTimeline(messages, activities);
    expect(timeline.map((message) => message.id)).toEqual(["user", "activity-record-call-1", "answer"]);
    expect(timeline[1].content.every((part) => part.type === "tool-call")).toBe(true);
    const calls = timeline[1].content.filter((part) => part.type === "tool-call");
    expect(calls.map((call) => call.type === "tool-call" ? call.toolName : "")).toEqual([
      "search_sessions",
      "search_sessions",
    ]);
    expect(calls.map((call) => call.type === "tool-call" ? call.result : "")).toEqual([
      "{\n  \"sessions\": [\n    {\n      \"id\": \"s1\"\n    }\n  ]\n}",
      "{\n  \"sessions\": [\n    {\n      \"id\": \"s2\"\n    },\n    {\n      \"id\": \"s3\"\n    }\n  ]\n}",
    ]);
  });
});
