import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { LisaApp } from "./LisaApp";

describe("LisaApp", () => {
  it("serves Lisa's workspace chat independently from Explorer", () => {
    const html = renderToStaticMarkup(createElement(LisaApp, {
      config: { apiBasePath: "/v1", lisaApiBasePath: "http://localhost:18083/v1" },
    }));

    expect(html).toContain('aria-label="Lisa conversation"');
    expect(html).toContain('aria-label="Message Lisa"');
    expect(html).toContain('aria-label="Lisa workspace panel"');
    expect(html).toContain("Activities");
    expect(html).toContain("Rules");
    expect(html).toContain("Triage / judges");
    expect(html).toContain("Identity");
    expect(html.match(/role="tab"/g)).toHaveLength(4);
  });
});
