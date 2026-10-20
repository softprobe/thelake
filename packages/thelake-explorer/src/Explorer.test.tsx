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
    expect(html).not.toContain("Send my message and recent workspace telemetry");
    expect(html).not.toContain('type="checkbox"');
    expect(html).toContain("Messages and recent workspace evidence are sent to your configured model provider");
    expect(html).toContain('aria-label="Chat with Lisa"');
    expect(html).toContain("Activities");
    expect(html).toContain("Rules");
    expect(html).toContain("Triage / judges");
    expect(html).toContain("Identity");
    expect(html).toContain('aria-label="Lisa workspace panel"');
    expect(html.match(/role="tab"/g)).toHaveLength(4);
    expect(html).not.toContain("New conversation");
    expect(html).not.toContain("Delete Lisa conversation");
    expect(html).not.toContain("Activate");
    expect(html).not.toContain("Evaluations (");
    expect(html).toContain('aria-label="Muse navigation"');
    expect(html).toContain('aria-label="Chat controls"');
    expect(html).toContain('aria-label="Invite"');
    expect(html).toContain("Connecting");
    expect(html).not.toContain("Review behavior check");
    expect(html).toContain("Sessions");
  });
});
