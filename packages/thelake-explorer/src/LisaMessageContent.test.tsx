import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { LisaMessageContent } from "./LisaMessageContent";

describe("LisaMessageContent", () => {
  it("renders markdown emphasis, lists, and code blocks as safe readable content", () => {
    const html = renderToStaticMarkup(createElement(LisaMessageContent, {
      content: "**Finding**\n\n- inspect `refund_ticket`\n\n```ts\ncheckEligibility()\n```",
    }));

    expect(html).toContain("<strong>Finding</strong>");
    expect(html).toContain("<li>inspect <code>refund_ticket</code></li>");
    expect(html).toContain("<pre><code class=\"language-ts\">checkEligibility()");
  });
});
