# Lisa activity-management benchmark dataset

`cases.jsonl` is the labeled task set for the activity agent benchmark.
`manifests/benchmark.json` pins its SHA-256 digest and case count. Change the
manifest deliberately whenever cases change. Each case is an ordered,
multi-turn task with chat provenance, owner identity, tool-action expectations,
reply checks, ordered tool-use and exact call-count labels, and expected state
labels. Owner chat turns require each newly linked user and assistant message on every resolved activity and check that runtime events link only to their resolved task. The request-resolution cases cover brief questions, greetings, independent asks, continuations, switches, and closed work.

The curated release includes ten held-out activity-tracking cases
across distinct user requests. Each uses the shared scorer to check feed shape,
ordered detail actions, and evidence linkage. It also covers activity creation,
lookup and resumption, duplicate prevention, ambiguity,
evidence-backed completion, owner boundaries, one-turn questions, explicit
greeting skips, separate activities for independent requests, continuation after
closure, error outcomes, policy creation and lookup, exact-ID approval, in-place
update, confirmed deletion, and
session lookup failure before the no-approved-policies shortcut. Evaluation
lookup errors are recorded and close the resolved activity, so Lisa reports the
error without a second lookup or completion attempt. Wrong policy IDs are
verified against the policy catalog before Lisa records the failed approval
attempt. Three cases check whether Lisa can
retrieve and report saved policy-triage decisions and judge verdicts without
rerunning an evaluation. Synthetic history is seeded in each disposable Postgres
case database through Lisa's repositories; judge-run completion uses a
parameterized benchmark SQL fixture that acknowledges the seeded score before
the evaluation activity is marked successful. A later chat lookup is a new
request activity, separate from that completed evaluation. State labels can inspect activity
timeline text. Held-out database cases check that the policy catalog query uses
supported columns and reports the absence of archive state, and that both
activity event views accept their reference columns; all turns are also checked
for unexpected MCP tool errors. Development
and benchmark splits are disjoint by case. Use development cases for tuning and
reserve benchmark cases for final measurement. The dataset is author-curated; it
is not human-adjudicated and does not estimate production prevalence. The
benchmark split is small, so report raw case and check counts with all rates.

Every case currently starts with a user turn. Slack cases simulate conversation
context only. This dataset does not measure pushed-session evaluation or Slack
message delivery. Those behaviors need a runner that invokes the real services
and checks isolated telemetry and delivery receipts before they can be claimed
as benchmark coverage.

Load and validate the frozen release with
`datasets.activity_dataset.load_activity_dataset()` (offline harness under
`lisa/benchmark/`, not `lisa.evaluation`). Run it through Lisa's real agent
runtime using
[`docs/how-to/lisa-activity-evaluation.md`](../../../docs/how-to/lisa-activity-evaluation.md).
Expected labels are used only by the benchmark scorer, never included in Lisa's
conversation or MCP context.
