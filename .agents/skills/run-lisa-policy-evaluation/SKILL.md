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
- Load the dataset's complete policy catalog into a disposable evaluation
  Lisa workspace through Lisa's policy-management service/API, with approved
  status for the test target agent. Do not seed the policy table with SQL or
  pass `PolicyRule` instances from the dataset directly to evaluation. The
  evaluation run must own its transient database and workspace and clean them
  up automatically. Never reuse or modify a developer's running instance or
  pre-existing workspace data.
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
available, stop the live quality run and report the missing prerequisite. A
manifest-driven call to `lisa/scripts/evaluate_policies.py` may be used for
prompt calibration or fixture validation, but label it as a harness-only
diagnostic; it does not measure production policy selection.

## Run procedure

1. Read `docs/how-to/policy-triage-evaluation.md`, the dataset README, and the
   current release notes. Use the skill to create a reproducible 10–20 pair
   sample if requested. Keep the canonical data immutable.
2. Start an isolated local quickstart stack using a new Compose project, data
   directory and workspace. Pass `LISA_LLM_TRACE_ENABLED=1` so the real provider
   request/response records are retained. Never reuse a non-disposable policy
   database or write fixture policies into a user's workspace.
3. Load the full release policy catalog using Lisa's `PolicyService.update_policy`
   in a unique agent scope. This preserves approval checks and writes through
   Lisa's PostgreSQL repository. Verify by reading it back through
  `PolicyService.approved_rules(target_agent_name)`; count, names, and policy IDs
  must match the full catalog, not the sample's annotated policy subset.
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

Run the production evaluator from the repository root after the isolated stack
is ready. Choose a fresh run suffix and UUID for every run. Start quickstart
with a new Compose project, data directory, and workspace so the policy DB and
workspace are disposable. Write the report outside the repository:

```bash
THELAKE_QUICKSTART_PROJECT_NAME=lisa-policy-eval-<run-id> \
THELAKE_QUICKSTART_ENV_FILE=warehouse/quickstart/lisa-policy-eval-<run-id>.env \
THELAKE_QUICKSTART_DATA_PATH=./warehouse/quickstart/lisa-policy-eval-<run-id> \
THELAKE_DEFAULT_WORKSPACE_ID=<fresh-uuid> \
LISA_LLM_TRACE_ENABLED=1 LISA_QA_CRON="0 0 * * *" \
bash examples/quickstart/start.sh
```

Leave that process running while evaluating. In another shell:

```bash
LISA_LLM_TRACE_ENABLED=1 uv run --project lisa --env-file .env \
  python lisa/scripts/evaluate_policy_dataset.py --live \
  --quickstart-env-file warehouse/quickstart/lisa-policy-eval-<run-id>.env \
  --manifest <sample-manifest> --output /tmp/lisa-policy-eval-<run-id>/report.json
```

The runner reads the generated quickstart connection metadata without printing
credentials and puts Lisa's request recordings in a separate directory under
`/tmp`. The quickstart script stores run-specific Compose, config, and endpoint
metadata under `warehouse/quickstart`; it refuses to reuse existing run files.
Preserve and restore any pre-existing `.env` changes after the run. The
production evaluator automatically removes this
run's Compose project, its volumes, and its DuckLake data directory after it
writes the report or exits with an error. It retains the report when produced
and the LLM request recordings. Do not start evaluation against a developer's
existing quickstart. The separate `lisa/scripts/evaluate_policies.py`
accepts pair manifests directly and remains a harness-only prompt diagnostic;
never report its metrics as production policy-selection quality.

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
