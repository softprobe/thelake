# Lisa activity agent benchmark

The activity agent benchmark is the labeled dataset in
[`lisa/datasets/activity_management`](../../lisa/datasets/activity_management/README.md).
Each case defines a sequence of main-chat and Slack-context user turns, tool
actions that must or must not occur, labeled final activity and policy
state, and reply checks. The manifest pins the dataset bytes and the runner
reports results by development/benchmark split and behavior category.

The activity benchmark also checks that each feed row has a renderable ID,
kind, title, status, timestamps, and details object. For tracked turns it reads
activity detail and checks ordered actions, current-turn message links, and
activity-scoped runtime-event links. Live activity runs require the direct
Gemini model path and record the provider in the report.

Run the benchmark explicitly because every turn invokes Lisa's configured model
and may incur provider charges:

```sh
uv run --project lisa python lisa/scripts/evaluate_activity_dataset.py \
  --live --split benchmark
```

For a focused live run against a labeled subset while retaining the pinned
release manifest, add `--case-prefix activity_tracking_`.

Provide `GOOGLE_API_KEY` or `GEMINI_API_KEY` in the process environment. The
live activity runner uses a direct Gemini model path and rejects custom base
URLs. It uses the actual OpenHands
conversation backend and Lisa MCP tools. Disposable Postgres and per-case Lisa
databases come only from [`lisa/conftest.py`](../../lisa/conftest.py)
(`start_lisa_postgres` / `create_lisa_test_database` / `stop_lisa_postgres`).
Conversation/model recordings stay in the run-owned artifact directory; the
transient service is removed at exit. Reports and captured logs are kept under
`target/test-artifacts/lisa-activity-benchmark/<run-id>/`.

The runner deterministically scores database state and required/forbidden tool
actions, rejects unexpected tool errors, and supports explicitly labeled
expected failures. Its policy-catalog case verifies Lisa uses the actual
`lisa.policies` columns and does not invent `archived_at`. State checks include
activity timeline evidence.
Turns labeled as substantive also verify that the
selected activity links all newly recorded chat messages and runtime events.
Tool checks can require ordered tool subsequences
and exact per-turn call counts. Its separate communication rubric checks labeled
phrases that must or must not appear in a reply; it is lexical, not a semantic
quality judgment. These authored labels are a curated behavioral benchmark,
not a human-adjudicated production sample or a measure of production
prevalence. Use development cases for iteration and reserve the benchmark split
for final measurement. Inspect per-case evidence and raw support counts; do not rely
on one aggregate score. Slack cases provide channel and thread context to
Lisa's real runtime but do not exercise pushed-session evaluation, Slack
event delivery, or message posting.

The activity dataset also includes three historical-evaluation lookup cases.
The runner seeds their synthetic triage and judge records into each disposable
Postgres database through Lisa's repositories and a parameterized benchmark SQL
fixture for completed judge rows, then scores Lisa's actual MCP
queries and replies. The cases verify historical lookup; they do not call the
judge or triage provider while seeding.

## Integration tests

`lisa/tests/test_activity_management_integration.py` contains three opt-in
model-backed integration scenarios. They are software integration tests, not
the benchmark and not a dataset-based quality estimate. They use transient
Postgres and temporary conversation storage. To run them explicitly:

```sh
LISA_RUN_ACTIVITY_MANAGEMENT_INTEGRATION=1 \
LISA_OWNER_USER_IDS=activity-eval-owner \
uv run --project lisa pytest -q lisa/tests/test_activity_management_integration.py
```

The test process does not load `.env` automatically. The benchmark and these
integration tests are separate from Lisa's policy triage benchmark in
[`policy-triage-evaluation.md`](policy-triage-evaluation.md).
