import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { formatCost, formatNumber, ThelakeExplorer } from "./Explorer";

describe("ThelakeExplorer", () => {
  it("uses the session browsing shell and full-width results table", () => {
    const html = renderToStaticMarkup(createElement(ThelakeExplorer, {
      config: { apiBasePath: "/v1" },
    }));

    expect(html).toContain('class="product-shell session-browser"');
    expect(html).toContain('aria-label="Session navigation"');
    expect(html).toContain('aria-label="Session results table"');
    expect(html).toContain("SESSION");
    expect(html).toContain("AGENT");
  });

  it("keeps Lisa navigation configurable for standalone hosts", () => {
    const html = renderToStaticMarkup(createElement(ThelakeExplorer, {
      config: { apiBasePath: "/v1" },
    }));

    expect(html).toContain("Workspace");
    expect(html).not.toContain('href="/lisa/"');
    expect(html).toContain("Sessions");
    expect(html).toContain('aria-label="Session navigation"');
    expect(html).toContain('aria-label="Session results table"');
    expect(html).not.toContain('aria-label="Lisa conversation"');
    expect(html).not.toContain('aria-label="Lisa workspace panel"');

    const standalone = renderToStaticMarkup(createElement(ThelakeExplorer, {
      config: { apiBasePath: "/v1" },
      lisaHref: "/lisa/",
    }));
    expect(standalone).toContain('href="/lisa/"');
  });

  it("uses workspace identity and connection labels supplied by the host", () => {
    const html = renderToStaticMarkup(createElement(ThelakeExplorer, {
      config: { apiBasePath: "/v1", workspaceName: "QA workspace", workspaceStatus: "Offline" },
    }));
    expect(html).toContain("QA workspace");
    expect(html).toContain("Offline");
    expect(html).not.toContain("Local workspace");
  });

  it("renders valid zero telemetry as zero and reserves a dash for missing values", () => {
    expect(formatNumber(0)).toBe("0");
    expect(formatNumber(null)).toBe("—");
    expect(formatCost(0)).toBe("$0.0000");
    expect(formatCost(undefined)).toBe("—");
  });
});
