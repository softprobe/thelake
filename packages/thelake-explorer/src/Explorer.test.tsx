import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { ThelakeExplorer } from "./Explorer";

describe("ThelakeExplorer", () => {
  it("opens into the first-party chat and keeps trace exploration available", () => {
    const html = renderToStaticMarkup(createElement(ThelakeExplorer, {
      config: { apiBasePath: "/v1" },
    }));

    expect(html).toContain("theLake");
    expect(html).toContain('aria-label="Lisa conversation"');
    expect(html).toContain("Connecting to Lisa");
    expect(html).toContain('aria-label="Message Lisa"');
    expect(html).toContain("Lisa reviews agent sessions on her schedule");
    expect(html).toContain("Online checks");
    expect(html).toContain("Workspace agent · investigates and drafts checks");
    expect(html).not.toContain("Review behavior check");
    expect(html).toContain("Sessions");
  });
});
