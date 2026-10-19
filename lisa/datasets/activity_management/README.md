# Lisa activity-management benchmark dataset

`cases.jsonl` is the labeled task set for the activity agent benchmark.
`manifests/benchmark.json` pins its SHA-256 digest and case count. Change the
manifest deliberately whenever cases change. Each case is an ordered,
multi-turn task with chat provenance, owner identity, tool-action expectations,
reply checks, and expected state labels.

The initial curated release covers activity creation, lookup and resumption,
progress updates, owner boundaries, policy proposal/approval, and follow-up
proposal/approval. Development and test splits are disjoint by case. The dataset
is author-curated; it is not human-adjudicated and does not estimate production
prevalence. The test split is a small initial benchmark, so report raw case and
check counts with all rates.

Load and validate the frozen release with
`lisa.evaluation.activity_dataset.load_activity_dataset()`. Run it through
Lisa's real agent runtime using
[`docs/how-to/lisa-activity-evaluation.md`](../../../docs/how-to/lisa-activity-evaluation.md).
Expected labels are used only by the benchmark scorer, never included in Lisa's
conversation or MCP context.
