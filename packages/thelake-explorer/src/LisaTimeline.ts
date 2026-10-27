import type { ThreadMessageLike, ToolCallMessagePart } from "@assistant-ui/react";
import type { LisaActivity, LisaMessage, LisaProactiveUpdate, LisaPolicySuggestion, LisaFeedEntry, LisaQaObservation } from "./client";

type TimedMessage = {
  order: number;
  timestamp: number;
  sequence?: number;
  message: ThreadMessageLike;
};

const toolResultPrefix = "\u0000lisa-tool-result:";
const proactiveUpdatePrefix = "\u0000lisa-proactive-update:";

export type LisaTimelineUpdate = {
  update: LisaProactiveUpdate;
  suggestion?: LisaPolicySuggestion;
  activity?: LisaFeedEntry;
  observation?: LisaQaObservation;
};

export function parseLisaProactiveUpdate(value: string): LisaTimelineUpdate | undefined {
  if (!value.startsWith(proactiveUpdatePrefix)) return undefined;
  try {
    const parsed = JSON.parse(value.slice(proactiveUpdatePrefix.length)) as LisaTimelineUpdate;
    return parsed?.update?.id && parsed.update.source_type ? parsed : undefined;
  } catch {
    return undefined;
  }
}

export type LisaToolResultEvent = {
  toolCallId: string;
  toolName: string;
  detail: string;
  isError: boolean;
};

function encodeToolResult(result: LisaToolResultEvent): string {
  return `${toolResultPrefix}${JSON.stringify(result)}`;
}

export function parseLisaToolResult(value: string): LisaToolResultEvent | undefined {
  if (!value.startsWith(toolResultPrefix)) return undefined;
  try {
    const parsed = JSON.parse(value.slice(toolResultPrefix.length)) as Partial<LisaToolResultEvent>;
    if (typeof parsed.toolCallId !== "string" || typeof parsed.toolName !== "string"
      || typeof parsed.detail !== "string" || typeof parsed.isError !== "boolean") return undefined;
    return parsed as LisaToolResultEvent;
  } catch {
    return undefined;
  }
}

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

function isToolTrajectoryPart(item: TimedMessage): boolean {
  if (toolCall(item)) return true;
  const content = item.message.content;
  if (!Array.isArray(content)) return false;
  const part = content[0];
  return typeof part === "object" && part?.type === "text"
    && typeof part.text === "string" && parseLisaToolResult(part.text) !== undefined;
}

function groupRecordedActions(items: TimedMessage[]): TimedMessage[] {
  const grouped: TimedMessage[] = [];
  for (let index = 0; index < items.length;) {
    if (!isToolTrajectoryPart(items[index])) {
      grouped.push(items[index]);
      index += 1;
      continue;
    }

    const first = items[index];
    const content: Array<ToolCallMessagePart | { type: "text"; text: string }> = [];
    while (index < items.length) {
      if (!isToolTrajectoryPart(items[index])) break;
      const part = items[index].message.content;
      if (Array.isArray(part) && part[0]?.type === "tool-call") {
        content.push(part[0]);
      } else if (Array.isArray(part) && part[0]?.type === "text") {
        content.push(part[0]);
      }
      index += 1;
    }
    // Preserve tool call and observation positions inside one disclosure.
    // A result remains its own event so parallel completions stay in order.
    grouped.push({
      ...first,
      message: {
        id: `activity-record-${String(first.message.id)}`,
        role: "assistant",
        content,
      },
    });
  }
  return grouped;
}

export function lisaTimeline(
  messages: LisaMessage[],
  activities: LisaActivity[],
  running = false,
  proactiveUpdates: LisaTimelineUpdate[] = [],
): ThreadMessageLike[] {
  const observations = new Map(
    activities
      .filter((activity) => activity.kind === "evidence" && activity.tool_call_id)
      .map((activity) => [activity.tool_call_id!, activity]),
  );
  const toolCalls = new Map(
    activities
      .filter((activity) => activity.kind === "tool" && activity.tool_call_id)
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
            result: undefined,
            isError: observation?.is_error,
          }],
          createdAt: activity.created_at ? new Date(activity.created_at) : undefined,
        },
      });
      return;
    }

    if (activity.kind === "evidence" && activity.tool_call_id) {
      const call = toolCalls.get(activity.tool_call_id);
      if (call) {
        const toolName = activity.tool_name ?? call.tool_name ?? call.label.replace(/^Lisa used /, "");
        const toolCallId = activity.tool_call_id;
        timeline.push({
          order: messages.length + index,
          timestamp: messageTime(activity.created_at),
          sequence: activity.sequence,
          message: {
            id: activity.id,
            role: "assistant",
            content: [{
              type: "text",
              text: encodeToolResult({
                toolCallId,
                toolName,
                detail: displayToolText(activity.detail),
                isError: Boolean(activity.is_error),
              }),
            }],
            createdAt: activity.created_at ? new Date(activity.created_at) : undefined,
          },
        });
        return;
      }
    }

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

  const proactiveTimeline = proactiveUpdates.map(({ update, ...detail }, index) => ({
      order: messages.length + activities.length + index,
      timestamp: messageTime(update.created_at),
      message: {
        id: `proactive-${update.id}`,
        role: "assistant",
        content: [{ type: "text", text: `${proactiveUpdatePrefix}${JSON.stringify({ update, ...detail })}` }],
        createdAt: update.created_at ? new Date(update.created_at) : undefined,
      },
    }));

  // Persisted sequenced events belong to OpenHands' exact event stream.
  // Unsequenced events have timestamps only, so place that older history first;
  // optimistic messages have no server sequence and stay after persisted work.
  // This gives mixed histories a total order rather than a non-transitive
  // pairwise comparison between sequence and timestamp.
  const isOptimistic = (item: TimedMessage) => String(item.message.id ?? "").startsWith("pending-");
  const hasUnsequenced = timeline.some((item) => item.sequence === undefined && !isOptimistic(item));
  timeline.sort((left, right) => {
    if (hasUnsequenced) {
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
  for (const update of proactiveTimeline.sort(compareTimeline)) {
    const sequencedStart = timeline.findIndex((item) => item.sequence !== undefined);
    const start = sequencedStart < 0 ? 0 : sequencedStart;
    const pendingIndex = timeline.findIndex(isOptimistic);
    const end = pendingIndex < 0 ? timeline.length : pendingIndex;
    let insertAt = end;
    for (let index = start; index < end; index += 1) {
      if (timeline[index].timestamp > update.timestamp) {
        insertAt = index;
        break;
      }
    }
    timeline.splice(insertAt, 0, update);
  }
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
