# Evaluate policy violations

Evaluation data lives in [`lisa/datasets/policy_evaluation`](../../lisa/datasets/policy_evaluation/README.md).
There is one schema and loader: trajectories, exact policies, sparse pair
annotations, and a manifest pinning revisions, file hashes, splits and policy
catalogs. Synthetic, STATE-Bench and itinerary examples use that same schema.

The release contains 48 trajectories, 17 policies, 223 annotations and 313
presented pairs. Only the 210 AI-adjudicated synthetic annotations enter accuracy
metrics. Thirteen proposed author annotations remain available for explicit
calibration checks, and 90 unreviewed pairs remain unlabeled. No absent annotation
is treated as a negative. Read the [synthetic example review](policy-triage-dataset-review.md)
for review findings and remaining quality limits.

## Offline validation

```sh
lisa/.venv/bin/python -m pytest -q \
  lisa/tests/test_evaluation_dataset.py \
  lisa/tests/test_evaluation_benchmark.py \
  lisa/tests/test_evaluation_prompt.py
```

Checks cover record integrity, sparse annotations, input isolation, metric
arithmetic and provider wiring. Mocked results do not establish model quality.

## Live evaluation

With credentials provisioned, explicitly opt into paid inference:

```sh
lisa/.venv/bin/python lisa/scripts/evaluate_policies.py \
  --live --mode pipeline --split development \
  --output /tmp/policy-evaluation.json
```

`--mode triage` uses Decisions only. `--mode judge` evaluates the presented
catalog directly with Gemini. `--mode pipeline` runs Decisions then Gemini on
suspected violations and uncertain evidence. Select another frozen catalog with
`--manifest`; omit `--split` to run all manifest batches.

Reports retain every presented pair, exact manifest digest, available annotation
revision/status, actual decisions and judge responses. Proposed, disputed and
unreviewed pairs contribute to workload, but not accuracy. Provider execution
failures abort rather than becoming healthy negatives. Reports stay outside the
dataset; keep reports private when evaluating private inputs.

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

Zero denominators return null. Judge-only mode does not report triage metrics.
Accuracy can be dominated by unrelated negatives; always inspect raw support,
missed violations and annotation coverage. Sparse annotation accuracy describes
the adjudicated subset, not the full catalog or production prevalence.

## Production-quality evaluation

Build independently human-adjudicated challenge cases and a separate sanitized
production sample drawn independently of selector outcomes, including negatives.
Use two reviewers and adjudicate disagreements. Label exact policy revisions,
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
