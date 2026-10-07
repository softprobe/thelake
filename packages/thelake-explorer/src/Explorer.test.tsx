import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { ThelakeExplorer } from "./Explorer";

describe("ThelakeExplorer", () => {
  it("renders the session and trace navigation shell", () => {
    const html = renderToStaticMarkup(createElement(ThelakeExplorer, {
      config: { apiBasePath: "/v1" },
    }));

    expect(html).toContain("thelake Explorer");
    expect(html).toContain('aria-label="Sessions"');
    expect(html).toContain('aria-label="Trace details"');
    expect(html).toContain("Select a session");
  });
});
