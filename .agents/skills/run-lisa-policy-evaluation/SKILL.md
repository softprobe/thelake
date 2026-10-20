---
name: run-lisa-policy-evaluation
description: Evaluate Lisa's policy triage and judgments against labeled trajectories through Lisa's production services and approved-policy database.
---

# Run Lisa Policy Evaluation

Use this skill for labeled dataset-based quality runs. Use
`run-lisa-evaluation-dogfood` for the scheduled agent-to-issue workflow.
Use [Lisa evaluation terms](../../../docs/architecture/lisa-terms.md) for the
distinction between a session ID, its derived trajectory, and a policy ID.

## Required production path

- Use the dataset's trajectory and policy records to prepare the isolated
  evaluation workspace and sessions. Use annotations only as scoring truth.
  Never pass a manifest's annotated policy subset directly to the triager and
  call that a Lisa production evaluation.
- Run each trajectory through Lisa's production `EvaluationService` (or the
  production MCP/API entry point that calls it). It must retrieve approved
  candidate rules through `PolicyService` backed by Lisa's PostgreSQL policy
  repository, then invoke the production triager and judge.
- Load the dataset's complete policy catalog into the disposable evaluation
  database with the benchmark-only `lisa/benchmark/policy_seed.py` helper,
  marking the test target agent's rows approved. This helper exists only to
  load a complete labeled-evaluation catalog; live policy changes still use
  Lisa's owner-gated `write_policy`, `approve_policy`, and `delete_policy`
  tools. Deletion requires a standalone `Delete policy: <policy_id>` command
  and is refused when saved evaluation runs refer to the policy. Keep policy
  state in `lisa.policies` and use `PolicyService` when evaluating sessions.
  The evaluation run must own its transient database and workspace and clean
  them up automatically. Never reuse or modify a developer's running instance
  or pre-existing workspace data.
- Store each dataset trajectory as a test session in the configured local
  TheLake instance and have `EvaluationService` fetch it through the normal
  `SessionService`/TheLake path. Do not inject a prebuilt trajectory into
  `EvaluationService` in a live quality run.
- Record the approved-candidate count and policy IDs read from PostgreSQL for
  each evaluated session. Confirm that the Decisions request has one question
  per candidate and that the failed sample is among the candidates. The
  candidate set must come from the DB, not from annotation references.
- Use adjudicated annotations only for accuracy. Compare decisions to sparse
  labels only where that exact `(session_id, policy_id)` pair is adjudicated. Unlabeled
  candidates are workload observations, never negatives. Report per-policy
  support, candidate-set size, recall, false escalation counts on labeled
  negatives, and judge outcomes separately.

If no disposable Lisa PostgreSQL database and local TheLake session workspace are
available, stop the live quality run and report the missing prerequisite. Do not
substitute a direct triager/judge call on manifest `policy_ids`; that bypasses
Lisa's approved-catalog and session path and is not a quality measurement.

## Run procedure

1. Read `docs/how-to/policy-triage-evaluation.md`, the dataset README, and the
   current release notes. Use the skill to create a reproducible 10–20 pair
   sample if requested. Keep the canonical data immutable.
2. Start an isolated local dogfood stack using a new Compose project, data
   directory and workspace. Pass `LISA_LLM_TRACE_ENABLED=1` so the real provider
   request/response records are retained. Never reuse a non-disposable policy
   database or write fixture policies into a user's workspace.
3. Load the full release catalog using the benchmark-only policy seed helper
   into the run-owned PostgreSQL database and unique agent scope. Verify by
   reading it back through `PolicyService.approved_rules(target_agent_name)`;
   count, names, and readable policy IDs must match the full catalog, not the
   sample's annotated policy subset.
4. Ingest selected dataset trajectories into local TheLake as sessions,
   preserving event and tool-call semantics and the exact dataset `session_id`.
   Before catalog writes or ingestion, confirm each complete fixture session ID
   is absent from the disposable workspace; stop if any already exists. Capture
   the trace ID, then verify Lisa's `SessionService` reconstructs a complete,
   ordered trajectory.
5. Call Lisa's existing production `EvaluationService.evaluate_session` for
   each session. Its policy lookup must read the approved PostgreSQL catalog.
   Preserve request recordings and responses for the production
   `policy_triage` and `judge` calls when trace recording is enabled. Do not
   automatically retry provider failures or alter labels.
6. Join production `policy_decisions` and final verdicts back to annotations by
   exact `(session_id, policy_id)`. Score only adjudicated pairs; report
   missing/unlabeled pairs separately.
7. Save the report and temporary inputs outside the repository. Record dataset
   digest, sample seed, workspace ID (redacted or locally scoped), DB-read
   candidate counts, session/trace IDs, models, and report path. Never expose
   secrets or full sensitive traces.

Run the production evaluator from the repository root against a fresh local
dogfood Compose project, data directory, and workspace so the policy database
and session workspace are disposable. Choose a fresh run suffix and UUID for
every run. Write the report outside the repository:

```bash
THELAKE_DOGFOOD_PROJECT_NAME=lisa-policy-eval-<run-id> \
THELAKE_DOGFOOD_ENV_FILE=warehouse/dogfood/lisa-policy-eval-<run-id>.env \
THELAKE_DOGFOOD_DATA_PATH=./warehouse/dogfood/lisa-policy-eval-<run-id> \
THELAKE_DEFAULT_WORKSPACE_ID=<fresh-uuid> \
LISA_LLM_TRACE_ENABLED=1 LISA_QA_CRON="0 0 * * *" \
bash examples/lisa-dogfood/start.sh
```

Leave that process running while evaluating. In another shell:

```bash
LISA_LLM_TRACE_ENABLED=1 uv run --project lisa --env-file .env \
  python lisa/scripts/evaluate_policy_dataset.py --live \
  --dogfood-env-file warehouse/dogfood/lisa-policy-eval-<run-id>.env \
  --manifest <sample-manifest> --output /tmp/lisa-policy-eval-<run-id>/report.json
```

The runner reads the generated dogfood connection metadata without printing
credentials and puts Lisa's request recordings in a separate directory under
`/tmp`. The dogfood script stores run-specific Compose, config, and endpoint
metadata under `warehouse/dogfood`; it refuses to reuse existing run files.
Preserve and restore any pre-existing `.env` changes after the run. Env bind and
teardown go only through [`lisa/conftest.py`](../../../lisa/conftest.py)
(`configure_lisa_dogfood` / `dispose_lisa_dogfood`): the evaluator removes
this run's Compose project, volumes, and DuckLake data directory after it writes
the report or exits with an error. It retains the report when produced and the
LLM request recordings. Do not start evaluation against a developer's existing
dogfood.

Live provider calls may incur charges. Run them only when explicitly requested.

## Interpret results

- Triage recall counts `suspected_violation` and `insufficient_evidence` as
  escalations on adjudicated violation pairs.
- End-to-end recall counts final `fail` verdicts across all eligible
  adjudicated violations, including policies Lisa did not escalate.
- `not_applicable` is negative for that exact policy-trajectory pair;
  `compliant` means the policy applied and behavior complied;
  `insufficient_evidence` is reported separately from binary accuracy.
- Always report raw support counts. The curated release is AI-adjudicated,
  has few violations, and is not a human-reviewed or production-prevalence
  benchmark. A small run is diagnostic, not a precise production estimate.
