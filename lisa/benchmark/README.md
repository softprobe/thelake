# Lisa performance benchmarks

Live agent evaluation — **not** unit tests.

## Layout (Java-style)

| Path | Role |
|------|------|
| `lisa/src/lisa/` | Production code (main) |
| `lisa/tests/` | All software tests (UT/IT) |
| `lisa/datasets/` | Labeled resources **and** their loaders |
| `lisa/benchmark/` | Live runners only (env lifecycle via `lisa/conftest.py`) |

## Where to run

thelake repository root (directory that contains `lisa/` and `examples/`):

```sh
test -d lisa && test -f lisa/pyproject.toml && pwd
```

## Software tests (not the benchmark)

```sh
uv run --project lisa pytest -q lisa/tests
```

## Credentials (live runs)

Need `GOOGLE_API_KEY` or `GEMINI_API_KEY`, or `LISA_BASE_URL` + `LISA_API_KEY`.
Check without printing secrets:

```sh
python -c 'import os; print(bool(os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY") or (os.getenv("LISA_BASE_URL") and os.getenv("LISA_API_KEY"))))'
```

If keys are only in the repo-root `.env`, pass `--env-file .env` to `uv run`.
`--live` is required for paid runs.

Splits: `development` (iterate), `benchmark` (final score). Never name a
held-out split `test`.

---

## Activity agent benchmark

```sh
uv run --project lisa --env-file .env \
  python lisa/scripts/evaluate_activity_dataset.py \
  --live --split benchmark
```

- Resources + loader: `lisa/datasets/activity_management/`, `datasets.activity_dataset`
- Report: `target/test-artifacts/lisa-activity-benchmark/<run-id>/report.json`
- Details: [`docs/how-to/lisa-activity-evaluation.md`](../../docs/how-to/lisa-activity-evaluation.md)
- SQL-specific column restrictions use `tool_query_arguments_must_not_include`,
  so activity updates can accurately explain why a field is unavailable without
  being miscounted as a database column probe.
- A turn may list `unchanged_activities` to snapshot matched activity fields and
  timeline events before the turn and assert they remain identical afterward.

## Conversation context benchmark

```sh
uv run --project lisa --env-file .env \
  python lisa/scripts/evaluate_context_dataset.py --live --split benchmark
```

- Resources + loader: `lisa/datasets/conversation_context/`,
  `datasets.activity_dataset`
- Report: `target/test-artifacts/lisa-context-benchmark/<run-id>/report.json`
- Covers interleaved tasks in the single main chat, Slack thread isolation,
  real checkpoint queue generation and adoption, intact transcript projection,
  and namespace reload after checkpoint adoption.
- The checkpoint assertion checks that generated summary text is absent from
  visible chat messages, and requires the `activities` namespace to be loaded
  before an activity tool on an adopted turn.
- At capture time, the runner checks that each saved checkpoint prefix exactly
  matches the input view and ends at one of OpenHands' valid manipulation
  boundaries, including tool-call, result, batch, and tool-loop constraints.
- PGMQ keeps at most one pending checkpoint job per conversation; later
  snapshots coalesce until that job is processed.
- This runner sets a small context budget in its own process to make background
  checkpointing observable with short cases. It runs the actual checkpoint
  worker and configured Gemini model; it does not change deployment settings.

## Per-activity memory benchmark

```sh
uv run --project lisa --env-file .env \
  python lisa/scripts/evaluate_activity_memory_dataset.py --live --split development

uv run --project lisa --env-file .env \
  python lisa/scripts/evaluate_activity_memory_dataset.py --live --split benchmark
```

- Dataset, source mapping, licenses, and limitations:
  [`datasets/activity_memory/README.md`](../datasets/activity_memory/README.md)
- Report: `target/test-artifacts/lisa-activity-memory-benchmark/<run-id>/report.json`
- The 70 text-only Lisa cases adapt ten LongMemEval and LongMemEval-V2 memory
  abilities, with three cases per ability in each split and five Lisa-specific
  adversarial cases per split. They are not official upstream benchmark scores.
- Each case interleaves task activities, forces global compaction before task
  follow-up, and checks task resolution before activity-memory loading. Cases
  cover ambiguity, corrected facts, completed-parent/child continuation,
  pending snapshot fallback, and cross-activity isolation, including facts
  placed in another activity's resolver summary and title. Domain-order checks
  ignore OpenHands schema setup calls (`discover_tool_namespaces`,
  `load_tool_namespace`, and `invoke_skill`) because those do not perform
  activity work; actual tool errors remain separately scored.
- The MCP server rejects task-specific policy, session, database, checklist,
  evaluation, and activity write tools until the current activity memory is
  loaded. When a turn resolves multiple activities, Lisa must record durable
  facts separately in each activity's timeline or progress before moving on.
- The live MCP path returns a bounded activity-thread projection (12 recent
  actions, 1,200 characters per summary) while the persisted and user-visible
  timeline remains complete; regression coverage verifies the serialized
  response remains below the 32 KiB payload cap.
- Reports separate answer success, activity selection, evidence-reference
  integrity, abstention, verbatim repeat fidelity, tool-sequence adherence, and
  tool-error-free rate. Reference integrity confirms citations resolve to
  captured events; it does not prove semantic support for each summary claim or
  final answer. Fact-anchor rubrics allow equivalent wording. Exact quote
  fidelity is reported separately; it gates answer success only when the user
  explicitly asks for verbatim or exact wording. A request to repeat a result
  briefly is scored by its labeled facts, while quote fidelity remains a
  diagnostic. Cases that request one of several mutually exclusive valid
  explanations or actions use `reply_must_match_exactly_one_of`: exactly one
  complete option must match, subject to its labeled contradiction anchors.
  Final-answer leak checks use per-turn exclusions for facts
  recorded only in another activity; abstention checks accept clear missing-
  record statements, insufficient-information statements, and requests for
  clarification, but fail if the reply includes a labeled forbidden fact.
  `memory_evaluation.heldout_acceptance.correct_cases` counts cases whose answer
  rubric, activity-memory selection, evidence references, isolation, and
  applicable state/context checks all pass. Recoverable tool-call errors and
  deviations from the prescribed lookup sequence are reported separately and
  remain visible in strict per-case results; they do not erase an otherwise
  correct, properly scoped answer. The held-out product gate requires at least
  32/35 core-correct cases, valid evidence references for every memory result,
  zero detected cross-task leaks, and zero writes to the wrong activity.
- Evidence scoring reads the original OpenHands `load_activity_memory`
  observation by event ID instead of the 4,000-character conversation activity
  projection, which may cut valid JSON. The scorer accepts at most 32 KiB per
  observation; larger results fail evidence integrity instead of being treated
  as complete. The event file read is capped at 208 KiB, including worst-case
  JSON escaping and event-envelope allowance, and oversized files are rejected
  before parsing.
- The runner applies a process-local context limit to force the real
  OpenHands checkpoint worker. All benchmark databases and workspaces are
  transient and run-owned; the limit does not change deployment settings.

---

## Policy production-path benchmark

### 1. Start disposable dogfood (leave running)

```sh
RUN_ID=20261019-a1
THELAKE_DOGFOOD_PROJECT_NAME=lisa-policy-eval-$RUN_ID \
THELAKE_DOGFOOD_ENV_FILE=warehouse/dogfood/lisa-policy-eval-$RUN_ID.env \
THELAKE_DOGFOOD_DATA_PATH=./warehouse/dogfood/lisa-policy-eval-$RUN_ID \
THELAKE_DEFAULT_WORKSPACE_ID=$(uuidgen | tr '[:upper:]' '[:lower:]') \
LISA_LLM_TRACE_ENABLED=1 \
bash examples/lisa-dogfood/start.sh
```

### 2. Evaluate (second shell)

```sh
RUN_ID=20261019-a1
LISA_LLM_TRACE_ENABLED=1 uv run --project lisa --env-file .env \
  python lisa/scripts/evaluate_policy_dataset.py --live \
  --dogfood-env-file warehouse/dogfood/lisa-policy-eval-$RUN_ID.env \
  --manifest lisa/datasets/policy_evaluation/manifests/evaluation.json \
  --output /tmp/lisa-policy-eval-$RUN_ID/report.json
```

- Resources + loader: `lisa/datasets/policy_evaluation/`, `datasets.policy_dataset`
- Details: [`docs/how-to/policy-triage-evaluation.md`](../../docs/how-to/policy-triage-evaluation.md)
