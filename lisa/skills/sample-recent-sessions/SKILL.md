---
name: sample-recent-sessions
description: Sample complete recent TheLake sessions and queue selected traces for Lisa evaluation.
---

# Sample recent sessions

Use this skill only when the `thelake` tool namespace is available. The
integration is optional; standalone Lisa does not query TheLake.

1. Call `sample_recent_sessions` once with its defaults unless the owner asks
   for a different lookback or sample cap.
2. The tool chooses a bounded, stratified sample, retrieves complete evidence,
   and submits it through Lisa's normal session-trace intake. The evaluation
   worker creates the usual evaluation activity for each newly queued session.
3. Report only the returned counts. Do not read or quote raw session payloads,
   create a second evaluation activity, or call `evaluate_session` for evidence
   just submitted by this tool.

The tool may report eligible sessions that were not queued because they were
already stored, still active, incomplete, or failed retrieval. Do not claim
that this sample represents all sessions in TheLake or that evaluation has
finished. A queued receipt means Lisa accepted evidence; the activity feed
shows the eventual policy result. Already-stored selections are not replaced by
other sessions during that sampling window, so repeated wakeups do not drain the
entire lookback range.

For an hourly scheduled wakeup, follow this procedure without creating an
activity or inspecting individual session content. If the tool is unavailable,
report that sampling is not configured and do not attempt another access path.
