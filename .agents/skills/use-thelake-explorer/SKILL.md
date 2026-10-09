---
name: use-thelake-explorer
description: Use or develop Explorer for sessions, traces, scores, and its scripted behavior-check setup flow; the composer is not a conversational AI.
---

# Use TheLake Explorer

Read `docs/how-to/explorer.md`; for a first-time product walkthrough, follow
`docs/quickstart.md`. Explorer is served by TheLake at `/explorer/` and can
also be embedded as a React package.

## Behavior-check wizard

The UI labels this area **Chat**, but `ChatView` is a scripted wizard and
results inbox, not a conversational AI agent. Assistant prompts are hardcoded;
the composer does not call an LLM or interpret free-form follow-ups. It stores
criteria and agent name in browser state, creates/activates an evaluator via
TheLake's API, and polls for that evaluator's scores every 10 seconds. Gemini
is used by the separate online evaluation runner to judge matching traces,
not to power this UI.

## User workflows

- **Sessions:** select a time range, open a session, inspect its traces/spans,
  attributes, events, and scores, then follow evidence links.
- **Human review:** record `correct`, `wrong`, or `unsure` on the focused span.
- **Behavior checks:** enter a behavior in the wizard, specify the exact agent
  name from its root trace, review the drafted criteria and data notice, then
  activate only when the online evaluation runner is configured and approved.
- **Debug a missing session:** verify time range and workspace first; session
  summaries can lag trace commit briefly. Compare the authenticated workspace
  with the one receiving telemetry.

Chat history is held in browser memory by default; embedded hosts can enable
workspace-scoped browser persistence. Evaluators and scores are stored by
TheLake. Activating a check sends captured conversation/tool evidence to the
configured evaluation runner/provider; review the data handling notice and
trace payload sensitivity before activation.

## Local development

Use `run-thelake-local` for the stack and `run-thelake-tests` for checks. The
Explorer guide covers embedded assets, Vite SPA development, anonymous local
mode, auth headers, and Playwright browser tests. Do not treat anonymous mode
as an authentication pattern for a network-exposed deployment.
