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
    expect(html).toContain("Activity");
    expect(html).toContain("Rules");
    expect(html).toContain("Evaluations");
    expect(html).toContain("Findings");
    expect(html).toContain("Identity");
    expect(html.match(/role="tab"/g)).toHaveLength(5);
    expect(html).not.toContain('aria-label="Sessions"');
  });

  it("shows the shared sessions navigation when Explorer is configured", () => {
    const html = renderToStaticMarkup(createElement(LisaApp, {
      config: { apiBasePath: "/v1" },
      explorerUrl: "https://lake.example/explorer/",
    }));

    expect(html).toContain('aria-label="Sessions"');
  });
});
