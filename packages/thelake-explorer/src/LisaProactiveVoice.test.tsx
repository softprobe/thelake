import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { LisaProactiveVoice } from "./LisaProactiveVoice";

describe("Lisa's proactive voice", () => {
  it("renders model-authored messages without adding or rewriting copy", () => {
    const messages = [
      "I noticed a concerning action in this run. The agent proceeded without checking with the user.",
      "I saved the run so we can review it together.",
    ];
    const html = renderToStaticMarkup(createElement(LisaProactiveVoice, { messages }));

    expect(html).toContain("I noticed a concerning action in this run. The agent proceeded without checking with the user.");
    expect(html).toContain("I saved the run so we can review it together.");
    expect(html).not.toContain("I noticed something in this run that looks wrong");
    expect(html).not.toContain("Run observation");
  });
});
