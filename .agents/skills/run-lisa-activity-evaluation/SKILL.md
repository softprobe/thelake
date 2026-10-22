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

Use `--case-prefix activity_tracking_` to run the ten held-out activity
tracking cases against the pinned full manifest. This focused run still records
the release digest while selecting only the matching benchmark cases.

Use `--split development` for prompt iteration. Use `--split all` only for a
complete report after development is finished. Do not tune against the
benchmark split and then present it as an independent result.

The live runner requires a direct Gemini model and Google or Gemini key, and
rejects a custom base URL. It validates the manifest digest, then uses
[`lisa/conftest.py`](../../../lisa/conftest.py) to start a run-owned Postgres
service and provision an isolated Lisa database plus conversation directory for
every case. It drives Lisa's OpenHands conversation backend and registered MCP
tools with the case's main-chat or Slack-context turns, then scores actual tool
events (including required order and exact call counts), activity timeline text,
database state, and labeled reply checks. Teardown also goes
through `conftest`; reports, redacted stack logs, and local model recordings stay
under `target/test-artifacts/lisa-activity-benchmark/<run-id>/`.

Report the dataset and case digests, split, model/runtime configuration, case
support, per-category pass counts, state/tool/communication scores, and failures.
Inspect per-case evidence and do not rely on aggregate rates from the small
curated release. Tool checks fail on unexpected MCP errors; expected error
paths must be labeled on the turn. The policy-catalog case checks the columns
against Lisa's actual schema and catches unsupported fields such as
`archived_at`; an activity-view case checks that both event views accept their
`reference_type` and `reference_id` fields. Slack-context cases do not verify
Slack delivery. The dataset is author-curated, not human-adjudicated or
production-representative.
