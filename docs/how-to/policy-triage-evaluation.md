# Evaluate policy violations

Use the shared [Lisa evaluation terms](../architecture/lisa-terms.md) for
session, trajectory, policy, and evaluation-pair identity.

Evaluation data lives in [`lisa/datasets/policy_evaluation`](../../lisa/datasets/policy_evaluation/README.md).
There is one schema and loader: trajectories, exact policies, sparse pair
annotations, and a manifest pinning file hashes, splits and policy
catalogs. Synthetic, STATE-Bench and itinerary examples use that same schema.

The official release contains 48 trajectories, 18 policies, and 202 curated
adjudicated pairs: 17 violations, 18 compliant, 143 not applicable, and 24
insufficient evidence. The annotations file contains one current record per labeled pair. No absent annotation is treated
as a negative. See the dataset [release review](../../lisa/datasets/policy_evaluation/synthesize/release.md)
for annotation provenance and limitations.

## Offline validation

```sh
lisa/.venv/bin/python -m pytest -q \
  lisa/tests/test_evaluation_dataset.py \
  lisa/tests/test_policy_evaluation_metrics.py \
  lisa/tests/test_evaluation_prompt.py
```

Checks cover record integrity, sparse annotations, input isolation, production
metric arithmetic and provider wiring. Mocked results do not establish model
quality.

## Production-path evaluation

Production quality runs must call Lisa's `EvaluationService` with the full
approved catalog loaded into Lisa's PostgreSQL policy database. The disposable
benchmark harness seeds that isolated catalog directly; live policy changes
still go through Lisa's owner-gated `write_policy` and `approve_policy` tools.
A manifest's sparse pair list is scoring truth, never the candidate-policy
source. The runner below uses a unique evaluation agent scope,
reads approved candidates back from PostgreSQL, persists dataset trajectories
in Lisa's local evidence directory, and calls the production
`EvaluationService.evaluate_session` through its normal session and policy
services:

```bash
LISA_LLM_TRACE_ENABLED=1 uv run --project lisa --env-file .env \
  python lisa/scripts/evaluate_policy_dataset.py --live \
  --dogfood-env-file warehouse/dogfood/lisa-policy-eval-<run-id>.env \
  --manifest /tmp/policy-sample/manifests/sample.json \
  --output /tmp/policy-sample/production-report.json
```

Start the runner against a fresh local dogfood Compose project and workspace;
do not use a user's existing Lisa database. The runner also checks that each
complete dataset `session_id` is absent before seeding policies or persisting;
it stops if a session already exists, so repeated runs cannot append spans to
old evidence. The runner uses the complete catalog
from `evaluation.json` by default, even if the sample manifest references only
one labeled policy per trajectory. It verifies that Lisa reads the full catalog
from PostgreSQL and that the reconstructed sessions remain complete before
making provider calls. `LISA_LLM_TRACE_ENABLED=1` records the actual Decisions
and Gemini request/response artifacts under the isolated Lisa data directory.
Keep them and the report private. Provider failures abort; they do not become
healthy negatives.

When the CLI exits, it removes only the run-owned Compose project, its volumes,
and its local session evidence directory, even when evaluation fails. The report remains
when produced, and `--data-dir` LLM recordings remain on disk. The runner refuses
cleanup metadata that does not identify a `lisa-policy-eval-*` project and
matching data directory.

Accuracy joins Lisa's decisions to adjudicated labels for those exact
`(session_id, policy_id)` pairs. Unlabeled candidate decisions contribute to candidate
coverage and workload only; they are never scored as negatives. Capture-gated
trajectories are reported separately.

The current runtime stops before model calls for incomplete captures or uncertain
ordering. The runner mirrors that gate and records gated pairs separately. Of
the AI-adjudicated synthetic subset, 31 complete cases / 186 pairs are eligible
for semantic evaluation; four incomplete cases / 24 pairs exercise the gate.
Two eligible pairs lack external approval state despite complete local capture;
the policies explicitly permit approval granted before the conversation.

## Metrics

- Violation recall: adjudicated violations escalated / all eligible adjudicated
  violations. Uncertain triage counts as escalation.
- Triage precision and accuracy: escalation versus adjudicated violation on
  resolved pairs. Compliant and not-applicable labels are negatives; report the
  confusion counts alongside rates.
- Uncertain escalation recall: separately measures routing of adjudicated
  insufficient-evidence pairs, which are excluded from binary accuracy.
- End-to-end violation recall: confirmed failures / all eligible adjudicated
  violations, including those lost at triage. A judge-stage abstention is a miss
  on a known violation.
- End-to-end detection precision/accuracy: failure alerts against resolved
  adjudicated pairs. They measure detection, not proof that every negative is
  compliant.
- Workload: escalation rate and judge count across the whole eligible presented
  catalog, including unreviewed pairs. These are call-volume measures, not costs.

Zero denominators return null. Accuracy can be dominated by unrelated negatives;
always inspect raw support, missed violations and annotation coverage. Sparse
annotation accuracy describes the adjudicated subset, not the full catalog or
production prevalence.

## Production-quality evaluation

Build independently human-adjudicated challenge cases and a separate sanitized
production sample drawn independently of selector outcomes, including negatives.
Use two reviewers and adjudicate disagreements. Label exact policy IDs,
evidence, violated clauses, exceptions and missing facts; never use model
agreement as ground truth.

Keep source/template ancestry together. Explicitly distinguish unseen
trajectories with known policies from unseen policy families. Current manifest
splits promise only trajectory separation, not unseen policies. Include
omissions, boundaries, exceptions, misleading assertions, failures, indirect
obligations, overlapping rules, long traces and multiple violations.

Evaluate realistic catalogs of 10, 100 and 300 distinct policies. Fully annotate
bounded subsets for reliable precision/recall; large sparsely annotated catalogs
can measure workload and recall on known positives, but cannot establish
whole-catalog accuracy. Record prompt/model versions, latency, actual provider
cost and confidence intervals clustered by trajectory ancestry. Compare variants
on the same frozen manifest. Independently audit production triage negatives.
These scale studies and production audits are not automated by the seed dataset.
