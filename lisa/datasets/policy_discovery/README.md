# Policy discovery evaluation cases

`cases.jsonl` checks whether the replaceable policy-gap provider detects clear
safeguards supported by spans without flagging a correctly confirmed
destructive action or a read-only task. `consolidation_cases.jsonl` checks
whether synthesis suppresses exact and semantic duplicates, rejects proposals
that conflict with an approved rule, respects previous owner approvals and
dismissals, reports compatible overlaps, and consolidates recurring signals
into one proposal. A reusable proposal requires three distinct error samples
from three sessions; a clear error in one sampled run is handled as an immediate
QA observation outside this consolidation benchmark. Cases with only one or two
samples test that the recurrence threshold prevents a recommendation. Eligible
positive cases also check policy scope, target agent, required behavior concepts
in the criteria, and coverage of the supporting signals. Scored behavior checks
verify confirmation before destructive actions, fee disclosure and customer
acceptance before commitment, and cost comparison before booking changes;
unsafe inversions do not pass those checks.
Expected outcomes are scorer-only and are never sent to Gemini.

The consolidation set also contains four cases adapted from Microsoft
[STATE-Bench](https://github.com/microsoft/STATE-Bench): two policy-gap examples
that each have only two samples and therefore do not qualify for a proposal,
one correctly handled control, and one existing-policy duplicate control.
STATE-Bench describes its trajectories as synthetically generated;
these are realistic simulated cases, not production customer traces. The
dataset keeps short, paraphrased evidence summaries and links to the exact
source files rather than copying full conversations. `message_indices_zero_based`
points into the file's single `conversation` array, using zero-based message
positions. Live reports carry those source references beside each result.

These datasets remain small diagnostic contract cases, not a representative
estimate of production quality. Add adjudicated variants with different
wording, scopes, agent types, and policy relationships before using scores as
a release-quality threshold.

Run the real provider path with an isolated output file:

```sh
uv run --project lisa --env-file .env python lisa/scripts/evaluate_policy_discovery_dataset.py \
  --live --output /tmp/lisa-policy-discovery-report.json
```

The runner reports per-case detection and cited-span validity, then scores
proposal counts and expected overlap relations for each consolidation case.
It reports accuracy by check category, duplicate proposals, suggestions, and
recorded token usage. The current dataset makes four gap-review calls and at
most seven consolidation calls; cases below the recurrence threshold skip the
synthesis call. It does not mutate Lisa's policy catalog. Owner
approval, edits, and dismissal authorization are covered by transient
Postgres/API integration tests; the consolidation dataset separately tests
whether prior owner outcomes affect new proposals.
