import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { ThelakeExplorer } from "./Explorer";

describe("ThelakeExplorer", () => {
  it("keeps Lisa navigation configurable for standalone hosts", () => {
    const html = renderToStaticMarkup(createElement(ThelakeExplorer, {
      config: { apiBasePath: "/v1" },
    }));

    expect(html).toContain("theLake");
    expect(html).not.toContain('href="/lisa/"');
    expect(html).toContain("Sessions");
    expect(html).toContain('aria-label="Sessions"');
    expect(html).toContain('aria-label="Trace details"');
    expect(html).not.toContain('aria-label="Lisa conversation"');
    expect(html).not.toContain('aria-label="Lisa workspace panel"');

    const standalone = renderToStaticMarkup(createElement(ThelakeExplorer, {
      config: { apiBasePath: "/v1" },
      lisaHref: "/lisa/",
    }));
    expect(standalone).toContain('href="/lisa/"');
  });
});
