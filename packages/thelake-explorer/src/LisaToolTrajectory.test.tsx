import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { LisaToolTrajectory } from "./LisaView";

describe("Lisa tool trajectory disclosure", () => {
  it("starts collapsed and announces the number of steps", () => {
    const html = renderToStaticMarkup(createElement(LisaToolTrajectory, {
      count: 3,
      children: createElement("p", null, "Tool details"),
    }));

    expect(html).toContain("<details");
    expect(html).not.toContain("<details open");
    expect(html).toContain("Lisa’s work · 3 steps");
    expect(html).toContain("Tool details");
  });

  it("keeps failed trajectory state visible while collapsed", () => {
    const html = renderToStaticMarkup(createElement(LisaToolTrajectory, {
      count: 1,
      failed: true,
      children: createElement("p", null, "Tool details"),
    }));

    expect(html).toContain("includes an error");
    expect(html).toContain("tle-lisa-trajectory--error");
    expect(html).not.toContain("<details open");
  });
});
