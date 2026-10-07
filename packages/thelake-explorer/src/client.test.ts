import { describe, expect, it, vi } from "vitest";
import { ExplorerApi } from "./client";

describe("ExplorerApi", () => {
  it("uses the configured API prefix and auth provider headers", async () => {
    const fetcher = vi.fn(async () => new Response(JSON.stringify({ items: [] }), { status: 200 }));
    const api = new ExplorerApi({ apiBasePath: "/api/thelake/v1/", auth: { headers: () => ({ Authorization: "Bearer test" }) }, fetch: fetcher as typeof fetch });
    await api.searchSessions();
    expect(fetcher.mock.calls[0][0]).toBe("/api/thelake/v1/llm/sessions/search");
    expect((fetcher.mock.calls[0][1]?.headers as Record<string, string>).Authorization).toBe("Bearer test");
  });

  it("sends the service cursor on the next session page", async () => {
    const fetcher = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body));
      expect(body.cursor).toBe("server-cursor");
      return new Response(JSON.stringify({ items: [] }), { status: 200 });
    });
    await new ExplorerApi({ apiBasePath: "/v1", fetch: fetcher as typeof fetch }).searchSessions(20, "server-cursor");
  });

  it("reads holistic session spans and follows trace observation cursors", async () => {
    const fetcher = vi.fn(async (input: RequestInfo | URL) => {
      const url = new URL(String(input), "http://local");
      if (url.pathname.endsWith("/sessions/s1")) {
        return new Response(JSON.stringify({ session_id: "s1", from: "2026-01-01T00:00:00Z", to: "2026-01-02T00:00:00Z", trace_count: 1, span_count: 2, spans: [{ span_id: "span-1" }, { span_id: "span-2" }] }), { status: 200 });
      }
      return new Response(JSON.stringify(url.searchParams.has("cursor")
        ? { observations: [{ span_id: "span-2" }], next_cursor: null }
        : { observations: [{ span_id: "span-1" }], next_cursor: "observation-page-2" }), { status: 200 });
    });
    const api = new ExplorerApi({ apiBasePath: "/v1", fetch: fetcher as typeof fetch });
    const session = await api.getSession("s1");
    const trace = await api.getTrace("t1");
    expect(session.spans.map((item) => item.span_id)).toEqual(["span-1", "span-2"]);
    expect(trace.observations.map((item) => item.span_id)).toEqual(["span-1", "span-2"]);
    expect(fetcher).toHaveBeenCalledTimes(3);
  });

  it("writes verdict scores through the configured API and auth provider", async () => {
    const fetcher = vi.fn(async () => new Response("{}", { status: 200 }));
    const api = new ExplorerApi({ apiBasePath: "/api/thelake/v1", auth: { headers: () => ({ "X-Softprobe-Assertion": "signed" }) }, fetch: fetcher as typeof fetch });
    await api.createScore({ name: "human_verdict", data_type: "categorical", string_value: "correct", session_id: "s1" });
    expect(fetcher.mock.calls[0][0]).toBe("/api/thelake/v1/llm/scores");
    const body = JSON.parse(String(fetcher.mock.calls[0][1]?.body));
    expect(body).toMatchObject({ name: "human_verdict", string_value: "correct", session_id: "s1", source: "annotation" });
  });
});
