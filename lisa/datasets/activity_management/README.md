# Lisa activity-management benchmark dataset

`cases.jsonl` is the labeled task set for the activity agent benchmark.
`manifests/benchmark.json` pins its SHA-256 digest and case count. Change the
manifest deliberately whenever cases change. Each case is an ordered,
multi-turn task with chat provenance, owner identity, tool-action expectations,
reply checks, ordered tool-use and exact call-count labels, and expected state
labels.

The 16-case curated release covers activity creation, lookup and resumption,
duplicate prevention, ambiguity, evidence-backed completion, owner boundaries,
policy proposal/approval/rejection, exact confirmation boundaries, and
follow-up proposal/approval/deduplication. State labels can inspect activity
timeline text and follow-up count, event trigger, and prompt fields. Development
and test splits are disjoint by case. Use development cases for tuning and
reserve test cases for final measurement. The dataset is author-curated; it is
not human-adjudicated and does not estimate production prevalence. The test
split is a small benchmark, so report raw case and check counts with all rates.

Every case currently starts with a user turn. Slack cases simulate conversation
context only. This dataset does not measure scheduled QA, autonomous discovery,
follow-up worker execution, or Slack message delivery. Those behaviors need a
runner that invokes the real scheduled services and checks isolated telemetry
and delivery receipts before they can be claimed as benchmark coverage.

Load and validate the frozen release with
`lisa.evaluation.activity_dataset.load_activity_dataset()`. Run it through
Lisa's real agent runtime using
[`docs/how-to/lisa-activity-evaluation.md`](../../../docs/how-to/lisa-activity-evaluation.md).
Expected labels are used only by the benchmark scorer, never included in Lisa's
conversation or MCP context.
