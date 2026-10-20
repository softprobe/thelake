import { describe, expect, it } from "vitest";
import type { Observation } from "./client";
import { buildSpanTree, buildTrajectoryTree, filterSpanTree, flattenSpanTree } from "./trajectory";

const spans: Observation[] = [
  { trace_id: "t1", span_id: "root", name: "agent", span_type: "agent", start_time: "2026-01-01T00:00:00Z" },
  { trace_id: "t1", span_id: "gen", parent_span_id: "root", name: "reasoning", span_type: "generation", start_time: "2026-01-01T00:00:01Z", input: "Find the customer", output: "Found the customer." },
  { trace_id: "t1", span_id: "tool", parent_span_id: "gen", name: "lookup_customer", span_type: "tool", start_time: "2026-01-01T00:00:02Z", events: [{ name: "tool.completed" }] },
  { trace_id: "t1", span_id: "error", name: "failed call", span_type: "tool", start_time: "2026-01-01T00:00:03Z", status_code: "ERROR" },
];

describe("session trajectory", () => {
  it("builds parented span trees and retains chronological roots", () => {
    const tree = buildSpanTree(spans);
    expect(tree.map((node) => node.span.span_id)).toEqual(["root", "error"]);
    expect(flattenSpanTree(tree).map((node) => node.span.span_id)).toEqual(["root", "gen", "tool", "error"]);
  });

  it("keeps matching nodes with their ancestor path for search", () => {
    const filtered = filterSpanTree(buildSpanTree(spans), "lookup_customer");
    expect(flattenSpanTree(filtered).map((node) => node.span.span_id)).toEqual(["root", "gen", "tool"]);
  });

  it("filters unrelated descendants even when an ancestor matches", () => {
    const filtered = filterSpanTree(buildSpanTree(spans), "agent");
    expect(flattenSpanTree(filtered).map((node) => node.span.span_id)).toEqual(["root"]);
  });

  it("projects spans into a user-facing turn and assistant call narrative", () => {
    const narrative = flattenSpanTree(buildTrajectoryTree(spans));
    expect(narrative.map((node) => node.label)).toEqual(["Turn 1", "User", "LLM call", "lookup_customer", "failed call", "Assistant"]);
    expect(narrative[1]?.preview).toBe("Find the customer");
    expect(narrative[2]?.preview).toBe("Found the customer.");
  });

  it("keeps separate generation rounds and folds reasoning acts into Thought", () => {
    const multiple: Observation[] = [
      ...spans.slice(0, 2),
      { trace_id: "t1", span_id: "thought", parent_span_id: "gen", name: "agent.reasoning", span_type: "event", start_time: "2026-01-01T00:00:01.5Z", output: "Check the lookup result." },
      { trace_id: "t1", span_id: "gen-2", parent_span_id: "root", name: "final response", span_type: "generation", start_time: "2026-01-01T00:00:03Z", input: "Find the customer", output: "Here is the answer." },
    ];
    const narrative = flattenSpanTree(buildTrajectoryTree(multiple));
    expect(narrative.filter((node) => node.label === "LLM call")).toHaveLength(2);
    expect(narrative.filter((node) => node.label === "Thought")).toHaveLength(1);
    expect(narrative.map((node) => node.key)).toContain("thought:gen");
  });

  it("uses a real user event before falling back to prompt text", () => {
    const withUser: Observation[] = [
      spans[0]!,
      { trace_id: "t1", span_id: "user", parent_span_id: "root", name: "opencode.message.user", span_type: "event", start_time: "2026-01-01T00:00:00.5Z", output: "Actual user message" },
      spans[1]!,
    ];
    expect(flattenSpanTree(buildTrajectoryTree(withUser))[1]).toMatchObject({ label: "User", preview: "Actual user message" });
  });

  it("projects generation-rooted sessions once and retains prompt and response", () => {
    const generationRoot: Observation = {
      trace_id: "t1", span_id: "generation-root", name: "model call", span_type: "generation",
      start_time: "2026-01-01T00:00:00Z", input: "Rooted user prompt", output: "Rooted assistant response",
    };
    const narrative = flattenSpanTree(buildTrajectoryTree([generationRoot]));
    expect(narrative.map((node) => node.label)).toEqual(["Turn 1", "User", "LLM call", "Assistant"]);
    expect(narrative[1]?.preview).toBe("Rooted user prompt");
    expect(narrative[3]?.preview).toBe("Rooted assistant response");
    expect(narrative.filter((node) => node.key === generationRoot.span_id)).toHaveLength(1);
  });

  it("keeps every user event in chronological narrative order", () => {
    const multipleUsers: Observation[] = [
      { trace_id: "t1", span_id: "agent", name: "agent", span_type: "agent", start_time: "2026-01-01T00:00:00Z" },
      { trace_id: "t1", span_id: "user-1", parent_span_id: "agent", name: "user.message", span_type: "event", start_time: "2026-01-01T00:00:01Z", output: "First request" },
      { trace_id: "t1", span_id: "gen", parent_span_id: "agent", name: "answer", span_type: "generation", start_time: "2026-01-01T00:00:02Z", output: "First answer" },
      { trace_id: "t1", span_id: "user-2", parent_span_id: "agent", name: "human.message", span_type: "event", start_time: "2026-01-01T00:00:03Z", output: "Follow-up" },
    ];
    const narrative = flattenSpanTree(buildTrajectoryTree(multipleUsers));
    expect(narrative.filter((node) => node.label === "User").map((node) => node.preview)).toEqual(["First request", "Follow-up"]);
    expect(narrative.findIndex((node) => node.key === "user-2")).toBeGreaterThan(narrative.findIndex((node) => node.key === "gen"));
  });

  it("nests child-agent turns under their dispatch tool", () => {
    const nested: Observation[] = [
      { trace_id: "t1", span_id: "root", name: "agent", span_type: "agent", start_time: "2026-01-01T00:00:00Z" },
      { trace_id: "t1", span_id: "dispatch", parent_span_id: "root", name: "task", span_type: "tool", start_time: "2026-01-01T00:00:01Z" },
      { trace_id: "t1", span_id: "child-agent", parent_span_id: "dispatch", name: "researcher", span_type: "agent", start_time: "2026-01-01T00:00:02Z" },
      { trace_id: "t1", span_id: "child-gen", parent_span_id: "child-agent", name: "research", span_type: "generation", start_time: "2026-01-01T00:00:03Z", input: "Search" },
    ];
    const tree = buildTrajectoryTree(nested);
    expect(tree[0]?.label).toBe("Turn 1");
    expect(tree[0]?.children[0]?.label).toBe("task");
    expect(tree[0]?.children[0]?.children[0]?.label).toBe("Turn 2");
    expect(flattenSpanTree(tree).map((node) => node.span.span_id)).toContain("child-gen");
  });

  it("extracts the user role from JSON prompt messages and searches projected labels", () => {
    const jsonPrompt = [{ ...spans[1]!, input: JSON.stringify([{ role: "system", content: "Ignore" }, { role: "user", content: "JSON user prompt" }]) }];
    const narrative = buildTrajectoryTree([spans[0]!, ...jsonPrompt]);
    expect(flattenSpanTree(narrative)[1]?.preview).toBe("JSON user prompt");
    expect(flattenSpanTree(filterSpanTree(narrative, "LLM call")).map((node) => node.label)).toContain("LLM call");
  });
});
