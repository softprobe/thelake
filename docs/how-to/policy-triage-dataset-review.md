# Synthetic policy triage dataset review

Reviewed on 2026-10-17. Scope: the synthetic-source records in `lisa/datasets/policy_evaluation`,
including literal policy semantics, every pair label, evidence references,
provider-visible inputs, capture metadata, splits and benchmark accounting.

## Review method and conclusion

Two separate AI subagents reviewed the original 25 cases / 150 pairs. One
performed a full evidence and format audit; the second recorded its own verdicts
before comparing stored labels. The second review initially saw outcome-bearing
identifiers, so it cannot honestly be called strictly blind. It subsequently
removed those identifiers from its view. No external model API calls or human
adjudication were used.

The reviewers found real defects. After repair and ten additional policy
contrasts, both reviewed the changes. Final status: **all 35 cases / 210 pair
labels approved for AI-reviewed synthetic smoke calibration**, with no remaining
blocking label disagreement. This does not establish production recall or a
human-adjudicated golden benchmark.

## Findings and resolutions

| Finding | Resolution |
|---|---|
| Raw event IDs disclosed `violation`, `compliance` and `missing-evidence` to Decisions. | Replaced all trace/span/call IDs with opaque identifiers; descriptive case names stay outside provider input. Added leakage regression. |
| Privacy missing capture was encoded as literal tool arguments `payload_capture=unavailable`. | Arguments are now null, capture is incomplete, and a formal limitation records missing arguments. No invented marker is interpreted as evidence. |
| Consent, disclosure and ordering cases asserted capture loss while claiming complete capture. | Marked incomplete, added specific limitations, removed invented context messages, and set tool capture false where tool evidence is missing. |
| Approval labels depended on conflicting interpretations of “obtain approval.” | Policies now explicitly require prior approval and allow grants in the external system before the conversation. Unknown lookup responses support uncertainty; they do not establish approval or its absence. Both reviewers agreed after clarification. |
| Empty timestamps and mismatched call/result span shapes reduced telemetry fidelity. | Added ordered ISO timestamps and matched tool call/result IDs and span IDs. These remain synthetic snippets, not full telemetry replays. |
| Repeated artificial greetings made unrelated examples conspicuous. | Replaced with distinct ordinary requests. |
| Important policy contrasts were missing. | Added approved and below-threshold refunds, nonzero disclosure, zero-fee omission, benign export, wrong-order and late reservations, staging and approved production changes, and explicit refusal before deletion. |
| Direct model calls on incomplete captures differed from production's pre-triage gate. | Benchmark now stops before provider calls, records gated rows separately and excludes them from semantic model metrics. |

## Final label accounting

| Subset | Cases | Pairs | Fail | Pass | Uncertain | Out of scope |
|---|---:|---:|---:|---:|---:|---:|
| Complete captured evidence | 31 | 186 | 12 | 12 | 2 | 160 |
| Incomplete capture gate | 4 | 24 | 0 | 0 | 4 | 20 |
| Total | 35 | 210 | 12 | 12 | 6 | 180 |

For capture-incomplete cases, out-of-scope annotations describe the governed
activities shown and the specifically stated missing observations. They do not
assert absence of all possible violations from arbitrary uncaptured history.
Production and the benchmark preserve unresolved status for every policy in
those cases.

The complete-capture uncertain cases are `refund-missing-evidence` and
`exception-missing-evidence`. Both show an external approval lookup returning
unknown before execution. Neither infers external approval history from silence.

The combined refund/deletion example intentionally reuses development evidence;
it verifies multiple findings, not an additional independent violation mechanism.

## Remaining limits

- Only six policy families and twelve positive pairs; several variants are
  correlated. Pair count overstates independent evidence.
- Short snippets, some approval booleans, and no full production capture replay.
  Tool arguments in these fixtures are stipulated authoritative environment
  facts; they do not test forged approval claims.
- Family-disjoint splits still share evidence templates. They do not establish
  generalization to unseen structures or provide a sealed holdout.
- Negative pairs dominate. Aggregate accuracy must accompany recall, false-alert
  counts, per-family results and uncertainty accounting.
- No long conversations, overlapping policy catalogs, adversarial instructions,
  or realistic 100–300-policy workloads in this fixture.

The [evaluation design](policy-triage-evaluation.md) describes the independent
human review, production sampling and scale experiments still required.
