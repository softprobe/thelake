---
name: run-lisa-activity-evaluation
description: Run Lisa's labeled activity-management agent benchmark through the real OpenHands and MCP runtime.
---

# Run Lisa activity agent benchmark

Use this workflow for dataset-based agent quality measurement. Do not report
pytest counts or the separate model-backed integration scenarios as benchmark
quality. Read [`docs/how-to/lisa-activity-evaluation.md`](../../../docs/how-to/lisa-activity-evaluation.md)
and [`lisa/datasets/activity_management/README.md`](../../../lisa/datasets/activity_management/README.md)
before a live run.

## Run

Live provider calls require an explicit user request because they may incur
charges. Check that model credentials are already available in the process
environment; never print or copy their values. Run the frozen benchmark split by
default:

```sh
uv run --project lisa python lisa/scripts/evaluate_activity_dataset.py \
  --live --split benchmark
```

Use `--split development` for prompt iteration. Use `--split all` only for a
complete report after development is finished. Do not tune against the
benchmark split and then present it as an independent result.

The runner validates the manifest digest, starts a run-owned Postgres service
through `scripts/test_stack.py`, and provisions an isolated Lisa database and
conversation directory for every case. It drives Lisa's OpenHands conversation
backend and registered MCP tools with the case's main-chat or Slack-context
turns, then scores actual tool events (including required order and exact call
counts), activity timeline text, follow-up fields, database state, and labeled
reply checks.
The runner removes its transient Compose resources and stores the report,
redacted stack logs, and local model recordings beneath
`target/test-artifacts/lisa-activity-benchmark/<run-id>/`.

Report the dataset and case digests, split, model/runtime configuration, case
support, per-category pass counts, state/tool/communication scores, and failures.
Inspect per-case evidence and do not rely on aggregate rates from the small
curated release. Slack-context cases do not verify Slack delivery. The dataset
is author-curated, not human-adjudicated or production-representative.
