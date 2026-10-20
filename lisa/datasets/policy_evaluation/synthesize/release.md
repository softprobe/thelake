# Official annotation release — 2026-10-17

Use the canonical [Lisa evaluation terms](../../../docs/architecture/lisa-terms.md)
when reading session-policy pairs in this release review.

The default `manifests/evaluation.json` selects exactly the 202 rows in
`golden_pairs.jsonl`: 48 sessions and 18 policies. Every selected annotation
is AI-adjudicated. This is an offline annotation release; Lisa predictions have
not been generated against it during publication.

| Split | Violation | Compliant | Not applicable | Insufficient evidence | Total |
|---|---:|---:|---:|---:|---:|
| Development | 9 | 5 | 29 | 18 | 61 |
| Test | 8 | 13 | 114 | 6 | 141 |
| Total | 17 | 18 | 143 | 24 | 202 |

There are 57 synthetic, 113 STATE-Bench and 32 connected-itinerary pairs.
Curation metadata contains 72 domain-positive and 130 domain-negative rows;
it was excluded from generation and blind review and never used as a label.

## Generation and review

Gemini 4 Argon supplied two blind judgments per pair, without prior labels,
case names, or the other answer. The main run used `HIGH` thinking and eight
workers. It reused 26 validated prior responses and started 378 new requests:
369 succeeded, eight timed out, and one uncertain response was rejected because
it omitted missing facts. A three-worker resume started nine requests and
recovered three; six timed out again. Only those three affected pairs were
then run through `run(..., thinking_level="MEDIUM", workers=3, max_pairs=3)`.
All six requests completed. Final judgments therefore cover 199 pairs at HIGH
and three at MEDIUM, with no missing responses.

Across these invocations: 393 new requests, 378 validated new responses,
26 reused responses, and 15 failed attempts. Provider-reported tokens for new
responses total 1,535,451; timeout usage is unknown, so this is not a complete
billing total. Both passes used the same model and similar prompts, so their
agreement is correlated rather than independent adjudication.

Separate Codex reviewers read all 202 full trajectory-policy inputs before
seeing generated judgments, prior annotations, descriptive case names or
curation metadata. Adjudication reconciled these blind judgments with both
Gemini outputs, including policy scope, capture limitations, evidence authority
and citation differences. A senior architect reviewed the release design and
challenged unsupported approval labels. All review kinds are recorded as `ai`;
this release does not claim human review.

The two Gemini passes agreed on 199 outcomes and 139 evidence sets; 138 pairs
agreed on both. All 64 pairs with outcome or evidence differences were resolved
through the reviewed final judgment. Seven final outcomes differ from the
annotator candidate; 16 differ from the initial blind review. Each selected
annotation preserves both complete Gemini judgments, the initial blind judgment,
and the final adjudication in `reviews`. Its rationale and evidence are the
selected final judgment. Request hashes identify private raw provider artifacts.

## Substantive resolutions

- Actual cancellation without full-itinerary inspection violates the connection
  policies even if no onward segment is established. A `confirm=false` preview
  alone is not an executed cancellation.
- Same-day onward-flight disclosure must explain stranding risk before seeking
  confirmation; saying that the onward ticket remains active is insufficient.
- A globally truncated opening cancellation request cannot establish that an
  unseen change, upgrade, delay or fee question never occurred. Such uncertain
  flight-policy pairs remain insufficient evidence, including two pairs where
  both Gemini passes had said not applicable.
- Missing fare class or route type leaves a real cancellation's fare-specific
  applicability unresolved. A complete capture can still omit necessary facts.
- Caller-supplied approval booleans establish neither external approval nor its
  absence. The claimed emergency rollback also lacks authoritative exception
  evidence. These cases remain insufficient evidence. Staging and refunds at
  or below $500 do not trigger their approval obligations and are not applicable.
- A quoted token with no external-send action is not applicable. Conversely,
  the observed external-send action with only its payload missing does not
  imply an unseen account-deletion action; that deletion-policy pair remains
  not applicable. Capture limitations are interpreted specifically.
- Delay compensation is supported by tool-established delay and full entitlement
  applied/read back, plus the offered benefits. It does not prove that vouchers
  were redeemed or an overnight hotel was used. Bereavement evidence includes
  the required tool reason and applied fee, rather than assistant prose alone.

## Current dataset IDs and verification

The release uses the canonical IDs directly: session IDs are `S001` through
`S048`, policy IDs are document section IDs `P1.1` through `P1.18`, and each
annotation is keyed by `(session_id, policy_id)`. The dataset stores one selected
judgment per labeled pair: 202 labels, with review histories, evidence,
splits, and policy membership.

Current file SHA-256 values are recorded in `manifests/evaluation.json` and
verified by the dataset loader. The golden pair file contains the same 202
session-policy keys. Regression tests verify exact pair/catalog equality,
final counts, review provenance and split/lineage consistency. The evaluation
harness retains incomplete-capture gates; 16 selected pairs are capture-gated,
rather than silently scored as negatives.

Post-publication verification: focused dataset, synthesis, policy triage and
policy-ingest tests pass. This is not a paid Lisa prediction evaluation. The
production-path runner loads the complete policy catalog through Lisa's
policy-management service into a disposable PostgreSQL workspace, ingests
sessions into TheLake, and invokes Lisa's `EvaluationService`.

## Limits for later evaluation

This is a curated set with overlapping policies and previously exposed inputs,
not production prevalence, an unseen-policy benchmark or a sealed holdout.
There are only eight test-split violation pairs, including related policy
judgments on shared trajectories. Report counts alongside recall and accuracy;
precision and aggregate accuracy describe this selection. Keep insufficient
labels and runtime capture gates separate from resolved model-quality metrics.

Approval examples currently establish neither a compliant obligated action nor
an actual approval violation. Future dataset updates should include authoritative
approval/denial results tied to the action identity and time, and independently
established emergency exceptions. Do not convert this known coverage gap into
confident labels.

For production-path Lisa evaluation, use the `run-lisa-policy-evaluation` skill
and `lisa/scripts/evaluate_policy_dataset.py`. That runner loads this complete
policy catalog through Lisa's policy-management service into a disposable
PostgreSQL workspace, ingests trajectories into TheLake, and invokes Lisa's
`EvaluationService`.
