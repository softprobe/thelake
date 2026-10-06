export type AuthProvider = {
  headers?: () => HeadersInit | Promise<HeadersInit>;
};

export type ExplorerConfig = {
  /** API root, for example `/v1` or `/api/thelake/v1`. */
  apiBasePath: string;
  auth?: AuthProvider;
  fetch?: typeof fetch;
};

export type SessionSummary = {
  session_id: string; start_time: string; end_time?: string | null;
  trace_count: number; observation_count: number; error_count: number;
  total_tokens?: number | null; total_cost?: number | null;
  user_ids?: string[]; models?: string[];
};

export type TraceSummary = {
  trace_id: string; name?: string | null; start_time: string; end_time?: string | null;
  observation_count: number; error_count?: number; total_tokens?: number | null;
  total_cost?: number | null;
};

export type SessionDetail = { session_id: string; traces: TraceSummary[]; next_cursor?: string | null };
export type Observation = {
  trace_id: string; span_id: string; parent_span_id?: string | null; session_id?: string | null;
  name: string; observation_type: string; start_time: string; end_time?: string | null;
  status_code?: string | null; model_name?: string | null; input_tokens?: number | null;
  output_tokens?: number | null; total_tokens?: number | null; total_cost?: number | null;
  input?: unknown; output?: unknown; attributes?: Record<string, unknown>;
  events?: Array<{ name: string; timestamp?: string; attributes?: Record<string, unknown> }>;
  scores?: Array<{ score_id: string; name: string; data_type: string; numeric_value?: number | null; string_value?: string | null; boolean_value?: boolean | null; comment?: string | null }>;
};
export type ScoreRecord = {
  score_id: string; name: string; data_type: string; numeric_value?: number | null;
  string_value?: string | null; boolean_value?: boolean | null; comment?: string | null;
};

export class ExplorerApi {
  constructor(readonly config: ExplorerConfig) {}

  private async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const base = this.config.apiBasePath.replace(/\/$/, "");
    const authHeaders = await this.config.auth?.headers?.();
    const response = await (this.config.fetch ?? fetch)(`${base}${path}`, {
      ...init,
      headers: { Accept: "application/json", ...authHeaders, ...init.headers },
    });
    if (!response.ok) throw new Error(`${init.method ?? "GET"} ${base}${path}: HTTP ${response.status}`);
    return response.json() as Promise<T>;
  }

  async searchSessions(pageSize = 50, cursor?: string, signal?: AbortSignal, days = 7): Promise<{ items: SessionSummary[]; nextCursor?: string | null }> {
    const now = new Date();
    const from = new Date(now.getTime() - days * 86400_000).toISOString();
    const result = await this.request<{ items: SessionSummary[]; next_cursor?: string | null }>(
      "/llm/sessions/search",
      { method: "POST", signal, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ from, to: now.toISOString(), order_by: "start_time", order: "desc", limit: pageSize, cursor, roots_only: true }) },
    );
    return { items: result.items ?? [], nextCursor: result.next_cursor };
  }

  async getSession(sessionId: string, signal?: AbortSignal): Promise<SessionDetail> {
    const path = `/llm/sessions/${encodeURIComponent(sessionId)}`;
    const query = new URLSearchParams({ limit: "200" });
    const first = await this.request<SessionDetail>(`${path}?${query}`, { signal });
    const traces = [...first.traces];
    let cursor = first.next_cursor ?? null;
    let page = 1;
    while (cursor && page < 25) {
      query.set("cursor", cursor);
      const next = await this.request<SessionDetail>(`${path}?${query}`, { signal });
      traces.push(...next.traces);
      cursor = next.next_cursor ?? null;
      page += 1;
    }
    return { ...first, traces, next_cursor: cursor };
  }

  async getTrace(traceId: string, signal?: AbortSignal, sessionId?: string): Promise<{ observations: Observation[]; next_cursor?: string | null }> {
    const path = `/llm/traces/${encodeURIComponent(traceId)}`;
    const query = new URLSearchParams({ limit: "200" });
    if (sessionId) query.set("session_id", sessionId);
    const first = await this.request<{ observations: Observation[]; next_cursor?: string | null }>(`${path}?${query}`, { signal });
    const observations = [...first.observations];
    let cursor = first.next_cursor ?? null;
    let page = 1;
    while (cursor && page < 25) {
      query.set("cursor", cursor);
      const next = await this.request<{ observations: Observation[]; next_cursor?: string | null }>(`${path}?${query}`, { signal });
      observations.push(...next.observations);
      cursor = next.next_cursor ?? null;
      page += 1;
    }
    return { ...first, observations, next_cursor: cursor };
  }

  createScore(input: { name: string; data_type: "categorical"; string_value: string; session_id?: string; trace_id?: string; span_id?: string; comment?: string }): Promise<ScoreRecord> {
    return this.request("/llm/scores", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ score_id: crypto.randomUUID(), timestamp: new Date().toISOString(), source: "annotation", ...input }) });
  }
}
