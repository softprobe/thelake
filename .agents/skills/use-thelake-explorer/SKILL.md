---
name: use-thelake-explorer
description: Use or develop TheLake's session and trace browser; Lisa is a separate chat app.
---

# Use TheLake Explorer

Read `docs/how-to/explorer.md`; for local setup, follow
`docs/how-to/local-development.md`. Explorer is served by TheLake at
`/explorer/` and can also be embedded as a React package. Lisa is a separate
app at `/lisa/`; use its chat for policy-management requests, not Explorer's
session browser.

## User workflows

- **Sessions:** use the full-width session list to filter by time range and
  agent. Open a session to inspect its span navigation,
  trajectory, input/output, attributes, events, and scores. Session URLs preserve
  selection across reloads and browser back/forward navigation.
- **Human review:** record `correct`, `wrong`, or `unsure` on the focused span.
- **Debug a missing session:** verify time range and workspace first; session
  summaries can lag trace commit briefly. Compare the authenticated workspace
  with the one receiving telemetry.

The session browser follows the `sp-llm/apps/explorer` session table and
two-pane detail hierarchy, adapted to TheLake's `/v1` API. Its left pane shows
the span tree and its right pane shows the selected span. The React package
scopes styles to its root and takes workspace identity from host configuration.
The source Finding column stays unavailable until TheLake supports findings.
Lisa's `/lisa/` route is a separate app; use Lisa's chat for policy management
and behavior-check setup.

## Local development

Use `run-thelake-local` for the stack and `run-thelake-tests` for checks. The
Explorer guide covers embedded assets, Vite SPA development, anonymous local
mode, auth headers, and Playwright browser tests. Do not treat anonymous mode
as an authentication pattern for a network-exposed deployment.
