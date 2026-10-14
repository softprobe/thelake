import { describe, expect, it, vi } from "vitest";
import { ExplorerApi } from "./client";

describe("ExplorerApi", () => {
  it("deletes a Lisa conversation through the authenticated API", async () => {
    const fetcher = vi.fn(async () => new Response(JSON.stringify({ deleted: true }), { status: 200 }));
    const api = new ExplorerApi({ apiBasePath: "/v1", lisaApiBasePath: "http://127.0.0.1:18082/v1", auth: { headers: () => ({ Authorization: "Bearer workspace-token" }) }, fetch: fetcher as typeof fetch });
    await api.deleteLisaConversation("lisa-1");
    expect(fetcher.mock.calls[0][0]).toBe("http://127.0.0.1:18082/v1/conversations/lisa-1");
    expect(fetcher.mock.calls[0][1]?.method).toBe("DELETE");
  });

  it("uses the authenticated Lisa conversation API", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const fetcher = async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ url: String(input), init });
      const data = calls.length === 1
        ? { conversation_id: "lisa-1", title: "New conversation", messages: [] }
        : { conversation_id: "lisa-1", messages: [{ id: "m2", role: "assistant", text: "I found an error session." }], activities: [{ id: "a1", kind: "tool", label: "Reviewed 3 recent sessions" }] };
      return new Response(JSON.stringify(data), { status: 200, headers: { "Content-Type": "application/json" } });
    };
    const api = new ExplorerApi({ apiBasePath: "/v1", lisaApiBasePath: "http://lisa.local/v1", auth: { headers: () => ({ "X-Softprobe-Assertion": "signed" }) }, fetch: fetcher as typeof fetch });

    const conversation = await api.createLisaConversation();
    const reply = await api.sendLisaMessage(conversation.conversation_id, "Investigate recent errors");

    expect(calls[0].url).toBe("http://lisa.local/v1/conversations");
    expect(calls[0].init?.method).toBe("POST");
    expect(calls[0].init?.headers).toMatchObject({ "X-Softprobe-Assertion": "signed" });
    expect(calls[1].url).toBe("http://lisa.local/v1/conversations/lisa-1/messages");
    expect(JSON.parse(String(calls[1].init?.body))).toEqual({ text: "Investigate recent errors", consent: true });
    expect(reply.messages[0]?.text).toBe("I found an error session.");
    expect(reply.activities[0]?.label).toContain("sessions");
  });

  it("uses the configured API prefix and auth provider headers", async () => {
    const fetcher = vi.fn(async () => new Response(JSON.stringify({ items: [] }), { status: 200 }));
    const api = new ExplorerApi({ apiBasePath: "/api/thelake/v1/", auth: { headers: () => ({ Authorization: "Bearer test" }) }, fetch: fetcher as typeof fetch });
    await api.searchSessions();
    expect(fetcher.mock.calls[0][0]).toBe("/api/thelake/v1/sessions/search");
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
    expect(fetcher.mock.calls[0][0]).toBe("/api/thelake/v1/scores");
    const body = JSON.parse(String(fetcher.mock.calls[0][1]?.body));
    expect(body).toMatchObject({ name: "human_verdict", string_value: "correct", session_id: "s1", source: "annotation" });
  });

});
