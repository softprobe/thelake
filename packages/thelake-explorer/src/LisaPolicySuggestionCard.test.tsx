import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import type { LisaPolicySuggestion } from "./client";
import { LisaPolicySuggestionCard } from "./LisaPolicySuggestionCard";

const suggestion: LisaPolicySuggestion = {
  id: "suggestion-1",
  name: "Confirm before deleting an account",
  criteria: "Ask the user for explicit confirmation before permanently deleting an account.",
  scope: "agent",
  target_agent_name: "account-agent",
  rationale: "The same unsafe behavior appeared in three sessions.",
  signal_count: 3,
  evidence: [{
    signal_id: "signal-1",
    session_id: "session-1",
    activity_id: "activity-1",
    agent_name: "account-agent",
    summary: "Deleted an account without confirmation.",
    span_ids: ["span-1"],
    evidence: [{
      session_id: "session-1",
      activity_id: "activity-1",
      agent_name: "account-agent",
      span_id: "span-1",
      kind: "tool_call",
      name: "delete_account",
      content: "delete_account(account_id=42)",
    }],
  }],
  overlaps: [],
  status: "pending",
  created_at: "2026-10-25T10:00:00Z",
};

describe("LisaPolicySuggestionCard", () => {
  it("shows pending recommendations and keeps policy activation behind owner approval", () => {
    const approve = vi.fn();
    const dismiss = vi.fn();
    const html = renderToStaticMarkup(createElement(LisaPolicySuggestionCard, {
      suggestion,
      canReview: false,
      editing: false,
      editName: suggestion.name,
      editCriteria: suggestion.criteria,
      busy: false,
      onApprove: approve,
      onDismiss: dismiss,
      onEdit: vi.fn(),
      onCancelEdit: vi.fn(),
      onChangeName: vi.fn(),
      onChangeCriteria: vi.fn(),
      onOpenActivity: vi.fn(),
    }));

    expect(html).toContain("Policy recommendation");
    expect(html).toContain("Confirm before deleting an account");
    expect(html).toContain("3 supporting sessions");
    expect(html).toContain("Only Lisa owners can review suggestions.");
    expect(html).not.toContain("Discuss with Lisa");
    expect(html).toContain('disabled=""');
    expect(approve).not.toHaveBeenCalled();
    expect(dismiss).not.toHaveBeenCalled();
  });

  it("keeps an approved recommendation in the chat with its active policy ID", () => {
    const html = renderToStaticMarkup(createElement(LisaPolicySuggestionCard, {
      suggestion: { ...suggestion, status: "approved", policy_id: "P1.1" },
      canReview: true, editing: false, editName: suggestion.name, editCriteria: suggestion.criteria,
      busy: false, onApprove: vi.fn(), onDismiss: vi.fn(), onEdit: vi.fn(), onCancelEdit: vi.fn(),
      onChangeName: vi.fn(), onChangeCriteria: vi.fn(), onOpenActivity: vi.fn(),
    }));
    expect(html).toContain("Active policy · P1.1");
    expect(html).toContain("You approved this policy");
    expect(html).not.toContain("Approve policy");
    expect(html).not.toContain("Discuss with Lisa");
  });
});
