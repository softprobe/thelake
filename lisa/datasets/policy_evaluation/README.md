# Policy evaluation dataset

Use the shared [Lisa evaluation terms](../../../docs/architecture/lisa-terms.md).
Dataset records use the same runtime identifiers: trajectories are keyed by
TheLake `session_id`, policies by Lisa `policy_id`, and annotations by
`(session_id, policy_id)`. A trajectory is the ordered evidence Lisa derives
from its session; it has no separate ID.

One schema covers synthetic, STATE-Bench and itinerary examples. Source is
provenance, not a different format. API contract snapshots remain under
`tests/fixtures`; they are not labeled evaluation data.

| File | Record |
|---|---|
| `trajectories.jsonl` | One captured session ID, descriptive name, lineage, provenance and trajectory events. |
| `policies.jsonl` | One policy with its document section `policy_id`, lineage and provenance. |
| `annotations.jsonl` | One sparse session-policy judgment, rationale, evidence and review history. |
| `manifests/evaluation.json` | Exact file digests, session splits and candidate policy IDs per session. |

IDs are readable strings: for example `S017` and `P1.2`. A policy ID follows
its policy document section. Changing dataset contents means updating the file
and its manifest SHA-256 digest deliberately. Annotation identity is the pair
it labels.

## Annotations

Absence of an annotation means **unreviewed**, never a negative. A catalog may
contain many policies while only some session-policy pairs are labeled. Do not
manufacture labels for the rest. Candidate policy lists in a manifest are
separate from its sparse annotations.

Outcomes describe what the supplied evidence supports:

- `violation`: at least one established breach.
- `compliant`: relevant requirements were triggered and shown satisfied.
- `not_applicable`: evidence supports absent scope or trigger.
- `insufficient_evidence`: applicability or satisfaction cannot be resolved.

For multiple occurrences, an established violation wins. Otherwise unresolved
relevant occurrences yield insufficient evidence, satisfied relevant
occurrences yield compliance, and established absence of relevant occurrences
yields not applicable. Incomplete capture does not prove that an unseen trigger
was absent.

Review status is separate: `proposed`, `disputed`, or `adjudicated`. Preserve
reviewer judgments in `reviews`, including reviewer kind (`ai`, `human`,
`author`) and scope. The selected annotation's outcome and rationale record the
resolution; conflicting reviews may remain in history. Adjudicated status
requires review provenance and never implies human review. Annotation metadata
is never supplied as model input.

The official release contains 202 curated pairs from `golden_pairs.jsonl`,
across 48 sessions and 18 policies. Every selected annotation is
AI-adjudicated: 17 violations, 18 compliant, 143 not applicable, and 24
insufficient evidence. Each pair received two blind Gemini judgments and a
separate blind Codex review of its full inputs, followed by explicit
adjudication. This is AI-reviewed gold, not human-reviewed ground truth. See
the [release review](synthesize/release.md) for provenance and limitations.

The selection is curated, not sampled from production prevalence. Aggregate
accuracy and precision describe this selected set. Report violation recall and
uncertainty handling separately; approval examples currently lack authoritative
approval records and do not establish positive violation/compliance coverage.

## Splits and limits

The manifest owns split membership. Related sessions and counterfactual
variants share a lineage and must stay in one split. Current splits evaluate
held-out sessions; policy catalogs overlap between development and benchmark.
They are **not** an unseen-policy benchmark or a sealed holdout.

Capture completeness belongs to the trajectory. Evidence sufficiency belongs
to the pair annotation. Runtime abstention belongs to the prediction. Reports
keep these distinct and exclude capture-gated rows from model-quality metrics.
Production-path evaluations load the full policy catalog through Lisa's
`PolicyService` and its PostgreSQL repository. Dataset annotations provide
scoring truth only.

Load with `datasets.policy_dataset.load_dataset()` (offline harness under
`lisa/benchmark/`, not `lisa.evaluation`). Run and interpret results using
[the evaluation guide](../../../docs/how-to/policy-triage-evaluation.md).
No model predictions are stored in these dataset files. Generate candidate
annotations with the [synthesis tool](synthesize/README.md).
