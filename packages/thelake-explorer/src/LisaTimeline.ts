import type { ThreadMessageLike, ToolCallMessagePart } from "@assistant-ui/react";
import type { LisaActivity, LisaMessage } from "./client";

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

export function toolActionLabel(toolName: string): string {
  return toolName.replace(/_/g, " ").replace(/^\w/, (letter) => letter.toUpperCase());
}

function compareTimeline(left: TimedMessage, right: TimedMessage): number {
  const byTime = left.timestamp - right.timestamp;
  if (Number.isFinite(byTime) && byTime !== 0) return byTime;
  if (left.timestamp !== right.timestamp) return left.timestamp < right.timestamp ? -1 : 1;
  return left.order - right.order;
}

function toolCall(item: TimedMessage): ToolCallMessagePart | undefined {
  const content = item.message.content;
  if (!Array.isArray(content)) return undefined;
  const part = content[0];
  return typeof part === "object" && part?.type === "tool-call" ? part : undefined;
}

function groupRecordedActions(items: TimedMessage[]): TimedMessage[] {
  const grouped: TimedMessage[] = [];
  for (let index = 0; index < items.length;) {
    if (!toolCall(items[index])) {
      grouped.push(items[index]);
      index += 1;
      continue;
    }

    const first = items[index];
    const calls: ToolCallMessagePart[] = [];
    while (index < items.length) {
      const part = toolCall(items[index]);
      if (!part) break;
      calls.push(part);
      index += 1;
    }
    // assistant-ui can render several tool parts in one assistant message.
    // Keep each invocation and paired result as its own native tool part;
    // the tool name and result summary already explain what each record is.
    grouped.push({
      ...first,
      message: {
        id: `activity-record-${String(first.message.id)}`,
        role: "assistant",
        content: calls,
      },
    });
  }
  return grouped;
}

export function lisaTimeline(
  messages: LisaMessage[],
  activities: LisaActivity[],
  running = false,
): ThreadMessageLike[] {
  const observations = new Map(
    activities
      .filter((activity) => activity.kind === "evidence" && activity.tool_call_id)
      .map((activity) => [activity.tool_call_id!, activity]),
  );
  const timeline: TimedMessage[] = messages.filter((message) => !message.hidden).map((message, order) => ({
    order,
    timestamp: messageTime(message.created_at),
    sequence: message.sequence,
    message: {
      id: message.id,
      role: message.role,
      content: [{ type: "text", text: message.turn_source === "scheduled" && message.role === "assistant"
        ? `Scheduled QA update\n\n${message.text}`
        : message.text }],
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

  // Persisted sequenced events belong to OpenHands' exact event stream.
  // Legacy events have timestamps only, so place that older history first;
  // optimistic messages have no server sequence and stay after persisted work.
  // This gives mixed histories a total order rather than a non-transitive
  // pairwise comparison between sequence and timestamp.
  const isOptimistic = (item: TimedMessage) => String(item.message.id ?? "").startsWith("pending-");
  const hasLegacy = timeline.some((item) => item.sequence === undefined && !isOptimistic(item));
  timeline.sort((left, right) => {
    if (hasLegacy) {
      const group = (item: TimedMessage) => item.sequence === undefined
        ? (isOptimistic(item) ? 2 : 0)
        : 1;
      const byGroup = group(left) - group(right);
      if (byGroup) return byGroup;
      if (left.sequence !== undefined && right.sequence !== undefined) {
        return left.sequence - right.sequence || left.order - right.order;
      }
      return compareTimeline(left, right);
    }
    if (left.sequence !== undefined && right.sequence !== undefined) {
      return left.sequence - right.sequence || left.order - right.order;
    }
    if (isOptimistic(left) !== isOptimistic(right)) return isOptimistic(left) ? 1 : -1;
    return compareTimeline(left, right);
  });
  const result = groupRecordedActions(timeline).map(({ message }) => message);
  if (running) {
    result.push({
      id: "lisa-working",
      role: "assistant",
      content: [],
      status: { type: "running" },
    });
  }
  return result;
}
