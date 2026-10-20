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
