# Policy evaluation dataset

One schema for synthetic, STATE-Bench and itinerary examples. Source is
provenance, not a different format. API contract snapshots remain under
`tests/fixtures`; they are not labeled evaluation data.

| File | Record |
|---|---|
| `trajectories.jsonl` | One captured trajectory, descriptive name, lineage and provenance. |
| `policies.jsonl` | One exact policy, lineage and provenance. |
| `annotations.jsonl` | One versioned pair judgment, rationale, evidence and review history. |
| `manifests/evaluation.json` | Exact file digests, trajectory splits, ordered policy catalogs and selected annotation revisions. |

Each record has an opaque `id` and integer `revision`; references are `id@revision`.
A change to an input or annotation requires a new revision. Never silently
transfer an annotation to revised inputs. The manifest pins exact file bytes
with SHA-256; update its digests deliberately when publishing a changed release.
The runner records the manifest digest in every report. The loader rejects
unknown references, duplicate revisions/pairs, invalid evidence, changed bytes
and trajectory ancestry crossing splits.

## Annotations

Absence of an annotation means **unreviewed**, never a negative. A 10-policy
catalog may contain only one annotated pair. Do not manufacture labels for the
other nine. A manifest can select one annotation revision per presented pair.

Outcomes describe what the supplied evidence supports:

- `violation`: at least one established breach.
- `compliant`: relevant requirements were triggered and shown satisfied.
- `not_applicable`: evidence supports absent scope or trigger.
- `insufficient_evidence`: applicability or satisfaction cannot be resolved.

For multiple occurrences, an established violation wins. Otherwise unresolved
relevant occurrences yield insufficient evidence, satisfied relevant occurrences
yield compliance, and established absence of relevant occurrences yields not
applicable. Incomplete capture does not prove that an unseen trigger was absent.

Review status is separate: `proposed`, `disputed`, or `adjudicated`. Preserve
reviewer judgments in `reviews`, including reviewer kind (`ai`, `human`, `author`)
and scope. The selected annotation's outcome and rationale record the final
resolution; conflicting reviews may remain in history. Adjudicated status
requires review provenance and never implies human review. Annotation metadata
is never supplied as model input.

The official release contains exactly the 202 curated pairs from
`golden_pairs.jsonl`, across 48 trajectories and 18 policies. Every selected
annotation is AI-adjudicated: 17 violations, 18 compliant, 143 not applicable,
and 24 insufficient evidence. The shared annotation store contains 425 records,
including 223 unchanged earlier revisions; the manifest selects only the 202
current judgments. All sources use the same loader and runtime request
construction. Default evaluation loads this official manifest.

Each pair received two blind Gemini judgments and a separate blind Codex review
of its full inputs, followed by explicit adjudication. This is AI-reviewed gold,
not human-reviewed ground truth. See the [release review](synthesize/release.md)
for generation configuration, disagreements, evidence decisions and limitations.
Explicit scope exclusions and approval exemptions are not applicable. A caller's
approval flag alone does not establish external approval or its absence.

The selection is curated, not sampled from production prevalence. Aggregate
accuracy and precision describe this selected set. Report violation recall and
uncertainty handling separately; approval examples currently lack authoritative
approval records and do not establish positive violation/compliance coverage.

## Splits and limits

The manifest owns split membership. Related trajectories and counterfactual
variants share a lineage and must stay in one split. Current splits evaluate
held-out trajectories; policy catalogs overlap between development and test.
They are **not** an unseen-policy benchmark or a sealed holdout. A future
unseen-policy release must also separate policy ancestry and catalogs.

Capture completeness belongs to the trajectory. Evidence sufficiency belongs
to the pair annotation. Runtime abstention belongs to the prediction. Reports
keep these distinct and exclude capture-gated rows from model-quality metrics.
All presented policies remain in run output, including unreviewed pairs, so
workload and annotation coverage are observable.

Load with `lisa.evaluation.dataset.load_dataset()`. Run and interpret results
using [the evaluation guide](../../../docs/how-to/policy-triage-evaluation.md).
No model predictions are stored in these dataset files.

Generate candidate annotations with the [synthesis tool](synthesize/README.md).
It consumes explicit rows from `golden_pairs.jsonl` without cross-product
expansion; generated candidates never publish themselves. The file is a curated
selection, not a replacement for reviewed pair annotations. Its optional
`domain_applicable` field is selection metadata and never a model input or a
compliance label.
