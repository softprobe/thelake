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

The current release contains 48 trajectories, 17 policies, 223 annotations and
313 presented pairs. The 210 synthetic annotations are AI-adjudicated. The ten
STATE-Bench and three itinerary annotations remain proposed author judgments.
There are 90 unreviewed pairs in the STATE-Bench catalogs. All sources use the
same loader and runtime request construction.

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
Its checked-in verification selection is limited to three synthetic policies
and three trajectories; generated candidates never publish themselves.
