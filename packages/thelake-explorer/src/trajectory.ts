import type { Observation } from "./client";

export type SpanTreeNode = {
  key: string;
  span: Observation;
  label: string;
  preview?: string;
  selectable?: boolean;
  children: SpanTreeNode[];
};

function chronological(a: SpanTreeNode, b: SpanTreeNode): number {
  return a.span.start_time.localeCompare(b.span.start_time) || a.span.span_id.localeCompare(b.span.span_id);
}

export function buildSpanTree(observations: Observation[]): SpanTreeNode[] {
  const nodes = new Map(observations.map((span) => [span.span_id, { key: span.span_id, span, label: span.name, children: [] as SpanTreeNode[] }]));
  const roots: SpanTreeNode[] = [];
  for (const node of nodes.values()) {
    const parent = node.span.parent_span_id ? nodes.get(node.span.parent_span_id) : undefined;
    if (parent && parent.span.trace_id === node.span.trace_id && parent !== node) parent.children.push(node);
    else roots.push(node);
  }
  for (const node of nodes.values()) node.children.sort(chronological);
  return roots.sort(chronological);
}

function matches(node: SpanTreeNode, term: string): boolean {
  if (!term) return true;
  const span = node.span;
  const searchable = [
    node.label,
    node.preview,
    span.name,
    span.span_type,
    span.model_name,
    span.input,
    span.output,
    span.attributes,
    span.events,
    span.scores,
  ];
  return searchable.some((value) => JSON.stringify(value ?? "").toLowerCase().includes(term));
}

function filterNode(node: SpanTreeNode, term: string): SpanTreeNode | undefined {
  const children = node.children.map((child) => filterNode(child, term)).filter((child): child is SpanTreeNode => Boolean(child));
  return matches(node, term) || children.length ? { ...node, children } : undefined;
}

export function filterSpanTree(tree: SpanTreeNode[], query: string): SpanTreeNode[] {
  const term = query.trim().toLowerCase();
  return tree.map((node) => filterNode(node, term)).filter((node): node is SpanTreeNode => Boolean(node));
}

export function flattenSpanTree(tree: SpanTreeNode[]): SpanTreeNode[] {
  const result: SpanTreeNode[] = [];
  const visit = (nodes: SpanTreeNode[]) => {
    for (const node of nodes) {
      result.push(node);
      visit(node.children);
    }
  };
  visit(tree);
  return result;
}

function promptText(span: Observation): string {
  const userMessage = (value: unknown): string => {
    let parsed = value;
    if (typeof parsed === "string") {
      const raw = parsed;
      try { parsed = JSON.parse(raw); } catch { return raw.trim(); }
    }
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed) && "messages" in parsed) {
      parsed = (parsed as { messages: unknown }).messages;
    }
    if (Array.isArray(parsed)) {
      const messages = parsed as Array<{ role?: unknown; content?: unknown }>;
      const users = messages.filter((message) => String(message.role ?? "").toLowerCase() === "user");
      const selected = users.length ? users : messages;
      return selected.map((message) => textPreview(message.content ?? message) ?? "").filter(Boolean).join("\n");
    }
    return textPreview(parsed) ?? "";
  };
  const fromInput = userMessage(span.input ?? span.attributes?.["sp.input"]);
  if (fromInput) return fromInput;
  for (const event of span.events ?? []) {
    if (event.name !== "gen_ai.content.prompt") continue;
    const message = userMessage(event.attributes?.content ?? event.attributes?.messages);
    if (message) return message;
  }
  return "";
}

function textPreview(value: unknown): string | undefined {
  if (value == null) return undefined;
  const text = typeof value === "string" ? value : JSON.stringify(value);
  return text.replace(/\s+/g, " ").trim().slice(0, 180) || undefined;
}

function trajectoryLabel(span: Observation, isRoot: boolean, turnIndex: number): string {
  if (isRoot && span.span_type.toLowerCase() === "agent") return `Turn ${turnIndex}`;
  const type = span.span_type.toLowerCase();
  if (type === "generation") return "LLM call";
  if (type === "event" && /(^|[.\-_])(user|human|customer)([.\-_]|$)/i.test(span.name)) return "User";
  return span.name;
}

/** Product-style narrative: expose user prompts and LLM calls as turn steps. */
export function buildTrajectoryTree(observations: Observation[]): SpanTreeNode[] {
  const byId = new Map(observations.map((span) => [span.span_id, span]));
  const agents = observations.filter((span) => span.span_type.toLowerCase() === "agent")
    .sort(chronologicalObservation);
  const agentAncestor = (span: Observation): Observation | undefined => {
    const seen = new Set([span.span_id]);
    let parent = span.parent_span_id ? byId.get(span.parent_span_id) : undefined;
    while (parent && !seen.has(parent.span_id)) {
      if (parent.span_type.toLowerCase() === "agent") return parent;
      seen.add(parent.span_id);
      parent = parent.parent_span_id ? byId.get(parent.parent_span_id) : undefined;
    }
    return undefined;
  };
  const nestedDispatch = new Map<string, Observation>();
  const topAgents: Observation[] = [];
  for (const agent of agents) {
    const dispatch = agent.parent_span_id ? byId.get(agent.parent_span_id) : undefined;
    if (dispatch && dispatch.span_type.toLowerCase() !== "agent" && agentAncestor(agent)) nestedDispatch.set(dispatch.span_id, agent);
    else topAgents.push(agent);
  }
  const roots = topAgents.length ? topAgents : agents.slice(0, 1);
  const rootForSpan = (span: Observation) => agentAncestor(span) ?? roots[0];
  const owned = new Map(roots.map((root) => [root.span_id, observations.filter((span) =>
    span.span_id === root.span_id || (span.span_type.toLowerCase() !== "agent" && rootForSpan(span)?.span_id === root.span_id),
  )]));
  const allNested = new Map(nestedDispatch);

  const makeNode = (span: Observation, key: string, label: string, preview?: string, children: SpanTreeNode[] = [], selectable = true): SpanTreeNode => ({
    key, span, label, preview: textPreview(preview), selectable, children,
  });
  const buildTurn = (root: Observation, turnNumber: number, seen = new Set<string>(), turnSpans?: Observation[]): SpanTreeNode => {
    if (seen.has(root.span_id)) return makeNode(root, `turn:${root.span_id}`, `Turn ${turnNumber}`);
    const nextSeen = new Set(seen).add(root.span_id);
    const spans = turnSpans ?? (roots.includes(root) ? owned.get(root.span_id) ?? [root] : observations.filter((span) =>
      span.span_id === root.span_id || (span.span_type.toLowerCase() !== "agent" && agentAncestor(span)?.span_id === root.span_id),
    ));
    const generations = spans.filter((span) => span.span_type.toLowerCase() === "generation").sort(chronologicalObservation);
    const userEvents = spans.filter((span) => span.span_type.toLowerCase() === "event" && /(^|[.\-_])(user|human|customer)([.\-_]|$)/i.test(span.name)).sort(chronologicalObservation);
    const children: SpanTreeNode[] = [];
    if (userEvents.length) {
      for (const user of userEvents) {
      children.push(makeNode(user, user.span_id, "User", outputText(user) || inputText(user)));
      }
    } else if (generations[0]) {
      const first = generations[0]!;
      const prompt = promptText(first);
      if (prompt) children.push(makeNode(first, `prompt:${first.span_id}`, "User", prompt, [], false));
    }

    const assigned = new Map(generations.map((generation) => [generation.span_id, [] as Observation[]]));
    const generationOwner = (span: Observation): Observation | undefined => {
      const seenParents = new Set([span.span_id]);
      let parent = span.parent_span_id ? byId.get(span.parent_span_id) : undefined;
      while (parent && !seenParents.has(parent.span_id)) {
        if (parent.span_type.toLowerCase() === "generation" && assigned.has(parent.span_id)) return parent;
        seenParents.add(parent.span_id);
        parent = parent.parent_span_id ? byId.get(parent.parent_span_id) : undefined;
      }
      return generations.reduce<Observation | undefined>((best, current) => {
        if (!best) return current;
        return Math.abs(Date.parse(current.start_time) - Date.parse(span.start_time)) < Math.abs(Date.parse(best.start_time) - Date.parse(span.start_time)) ? current : best;
      }, undefined);
    };
    const assignedIds = new Set<string>(userEvents.map((span) => span.span_id));
    for (const span of spans) {
      if (["generation", "agent"].includes(span.span_type.toLowerCase()) || assignedIds.has(span.span_id)) continue;
      const generation = generationOwner(span);
      if (generation && assigned.has(generation.span_id)) {
        assigned.get(generation.span_id)!.push(span);
        assignedIds.add(span.span_id);
      }
    }
    for (let index = 0; index < generations.length; index++) {
      const generation = generations[index]!;
      const acts = (assigned.get(generation.span_id) ?? []).sort(chronologicalObservation);
      const reasoning = acts.filter((span) => /reasoning|thought/i.test(`${span.name} ${span.span_type}`));
      const roundChildren: SpanTreeNode[] = [];
      if (reasoning.length) {
        const text = reasoning.map(outputText).filter(Boolean).join("\n\n");
        roundChildren.push(makeNode(reasoning[0]!, `thought:${generation.span_id}`, "Thought", text || reasoning.map((span) => span.name).join(", ")));
      }
      for (const act of acts.filter((span) => !reasoning.includes(span))) {
        const type = act.span_type.toLowerCase();
        const label = type === "tool" || type === "retriever" || type === "guardrail"
          ? String(act.attributes?.["gen_ai.tool.name"] ?? act.attributes?.["sp.tool.name"] ?? act.name)
          : act.name;
        const nestedAgent = allNested.get(act.span_id);
        const nested = nestedAgent ? [buildTurn(nestedAgent, turnNumber + 1, nextSeen)] : [];
        roundChildren.push(makeNode(act, act.span_id, label, outputText(act), nested));
      }
      const response = outputText(generation);
      if (response) roundChildren.push(makeNode(generation, `assistant:${generation.span_id}`, "Assistant", response, [], false));
      const roundPreview = response || roundChildren[0]?.preview || (generation.model_name ? `LLM · ${generation.model_name}` : undefined);
      children.push(makeNode(generation, generation.span_id, "LLM call", roundPreview, roundChildren));
    }
    for (const span of spans.filter((item) => !["agent", "generation"].includes(item.span_type.toLowerCase()) && !assignedIds.has(item.span_id)).sort(chronologicalObservation)) {
      const nestedAgent = allNested.get(span.span_id);
      children.push(makeNode(span, span.span_id, span.name, outputText(span) || inputText(span), nestedAgent ? [buildTurn(nestedAgent, turnNumber + 1, nextSeen)] : []));
    }
    children.sort(chronological);
    return makeNode(root, `turn:${root.span_id}`, `Turn ${turnNumber}`, children.at(-1)?.preview, children);
  };

  if (roots.length) return roots.map((root, index) => buildTurn(root, index + 1));
  if (!observations.length) return [];
  const root = [...observations].sort(chronologicalObservation)[0]!;
  return [buildTurn(root, 1, new Set(), observations)];
}

function chronologicalObservation(a: Observation, b: Observation): number {
  return a.start_time.localeCompare(b.start_time) || a.span_id.localeCompare(b.span_id);
}

function outputText(span: Observation): string {
  return textPreview(span.output ?? span.attributes?.["sp.output"]) ?? "";
}

function inputText(span: Observation): string {
  return textPreview(span.input ?? span.attributes?.["sp.input"]) ?? "";
}
