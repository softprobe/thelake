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
    expect(html).toContain('aria-label="TheLake chat"');
    expect(html).toContain("Welcome. Tell me a behavior you want to catch");
    expect(html).toContain("Sessions");
  });
});
