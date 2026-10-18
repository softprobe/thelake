# Synthesize pair annotations

`annotate.py` creates candidate annotations for explicitly listed trajectory–policy
pairs. It uses Gemini 4 Argon through Google REST with Application Default
Credentials (ADC). The runtime's API-key client and SDK dependencies are unchanged.

The default input is `../golden_pairs.jsonl`: 202 unique pairs referencing 48
trajectories and 18 policies. Only listed pairs are generated; repeated policies
or trajectories do not expand into a Cartesian product. The complete file is
validated before any limit or provider initialization. Invalid references,
duplicate pairs and malformed metadata are rejected.

Each JSONL row contains `trajectory_ref`, `policy_ref`, and optional boolean
`domain_applicable` curation metadata. The current file includes 72 true and 130
false rows. Both groups are retained; this field is never supplied to the model
or interpreted as a compliance outcome. The filename does not make the pairs
reviewed gold. A domain-relevant policy may still have an absent specific trigger.

The default pair budget remains nine. Running the full 202-row input without an
explicit limit or budget increase fails; the tool never silently truncates or
expands the selection. `--limit N` explicitly chooses a prefix in file order.

## Run

Use securely provisioned ADC and an authorized Google Cloud project. This tool
never logs in or changes credentials. Project resolution is explicit `--project`,
then `GOOGLE_CLOUD_PROJECT`, then ADC's project; location defaults to `global`.
The default model is `gemini-4-argon` with `HIGH` thinking.

From the repository root, after installing Lisa's dependencies:

```sh
uv run --project lisa --with 'google-auth[requests]>=2,<3' python \
  lisa/datasets/policy_evaluation/synthesize/annotate.py \
  --limit 9 --live --output /tmp/lisa-annotation-verification
```

The example above selects exactly nine curated pairs, with at most eighteen new
calls. To process the entire file in a separately authorized live run, omit
`--limit` and explicitly set `--max-pairs 202`. `--pairs` can select another
JSONL file with the same row schema. No separate policy-list/trajectory-list
selection format is supported.

Google's [Argon REST documentation](https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/guides/gemini-4-argon)
describes the `v1beta1` endpoint and supported thinking levels. Direct REST avoids
upgrading the application's Gen AI SDK just for offline annotation.

The output directory must be private (0700) and outside both the default and
active canonical dataset directories. Output files are written atomically with
private permissions. Use one process per output directory. `--workers` bounds
parallel requests (default three, maximum eight). Requests have bounded connect
and read timeouts; application-level retries are disabled. An explicit rerun
can reuse completed calls and retry failed calls, and may incur additional cost.

## What the model sees

Each call receives exactly one full policy and one full trajectory, including
capture completeness, limitations, ordering metadata, and opaque evidence IDs.
There is no event clipping, runtime capture gate, existing annotation, case
name, lineage, first-pass answer, or cross-pair answer in the prompt.

Two calls derive judgments without seeing each other's answers. Both use the
same model and nearly the same prompt; these are correlated blind samples, not
independent adjudication. Agreement never promotes a result to gold.

Agreement requires the same outcome **and exact equality of cited evidence
sets**, ignoring citation order and duplicates. Either mismatch marks the
candidate disputed. This deliberately conservative rule can flag two valid
alternative citations; it asks for review, not an automatic label correction.
Two empty evidence sets can match on a negative outcome, so evidence agreement
alone is not proof of grounding. Missing-fact descriptions and rationales are
preserved for semantic review rather than compared as raw strings.

An established breach wins even when other evidence is missing. Otherwise the
model distinguishes satisfied relevant requirements, established absent scope,
and unresolved evidence. It must identify missing facts for insufficient
evidence and cite actual spans for violations/compliance. Every evidence ID is
validated; malformed output, refusal, incomplete generation and unknown IDs are
errors, never negative annotations. Responses marked `MAX_TOKENS`, `SAFETY`, or
`CONTINUATION` are rejected rather than accepted as complete labels.

## Artifacts and review

- `calls/*.json`: successful raw provider responses, exact requests, model
  version, token usage, elapsed time and parsed judgments for each role.
- `calls/*.error.json`: failed attempt diagnostics and any returned raw response.
- `annotations.jsonl`: canonical-schema candidates. Matching outcome and evidence
  yields `proposed`; any mismatch yields `disputed`. The annotator still supplies
  candidate fields; disputed status does not resolve the competing judgments.
- `comparisons.jsonl`: side-by-side full judgments for every pair: outcomes,
  rationales, original evidence lists, missing facts, separate outcome/evidence
  agreement flags, explicit dispute reasons, and each call's request hash/model
  version. `annotation_ref` links the comparison to the candidate. Reviewers do
  not need to search raw call files to understand a dispute.
- `run.json`: selected input revisions, source manifest digest, model/prompt
  configuration, exact selected rows and source-file SHA-256/total count/limit,
  attempted/completed/cached/failed counts, separate outcome and
  evidence agreement counts, and outcome counts.

`attempted_calls` counts new requests started, including failed ones.
`completed_calls` counts new responses that passed validation and were saved.
`cached_calls` counts validated reuse; `failed_calls` includes failed new tasks
and invalid cached responses, so it can be nonzero with no new attempts.
`new_response_tokens` counts reported usage from new responses, including
malformed judgments. `cached_response_tokens` is historical usage from validated
cache hits, not new spending. Missing usage or a transport failure increments
`new_calls_without_usage`; `token_usage_complete` becomes false when any new or
cached call lacks valid usage. Even complete reported tokens are not a billing
estimate, and counters describe this invocation, not lifetime run spending.

Resume with the same command and output directory. Model, thinking level,
prompt/schema, selected pairs, pair-source file and manifest changes require a new directory. Each
call additionally hashes its exact request, role, input revisions and model;
that per-call hash is authoritative for generation parameters and input bytes.
Cached raw responses are revalidated before reuse. Errors are never reused as
successful calls. A failed run does not export candidates, and failed resume
removes both previous candidate and comparison exports to avoid stale success artifacts.

This command never edits canonical inputs, annotations, or manifests. Review
candidates explicitly before publishing new annotation revisions. Keep generated
artifacts outside the public repository. Pair coverage is not violation coverage:
the curated file still includes domain-negative controls. Inspect difficult relevant
cases and false alarms separately rather than reporting aggregate accuracy alone.

## Earlier small-selection verification

A nine-pair synthetic smoke sample was verified on 2026-10-17 using ADC and actual `gemini-4-argon` responses. Both
samples matched outcomes on all nine pairs; comparison with existing synthetic
annotations, performed only after generation, also matched all nine. Outcomes were
one violation, one compliant pair, one insufficient-evidence pair and six
not-applicable pairs. These are smoke results, not a production accuracy estimate.

The first run completed seventeen responses and had one read timeout; it exported
no candidates. Resume made one new request and reused seventeen responses. A
further resume with a client that forbids network calls reused all eighteen
responses and made zero calls. There were nineteen provider attempts across the
initial run and retry. These initial results used outcome-only agreement.

Reprocessing those eighteen cached responses with the evidence rule made zero
provider calls: six pairs matched outcomes and evidence, and three became
disputed solely because their evidence sets differed. All three evidence
mismatches were not-applicable pairs where one sample cited off-topic events
and the other cited none. This does not establish three wrong labels; the
side-by-side export makes those differences reviewable. Historical cached usage
was 35,088 tokens; new response usage during reprocessing was zero. Canonical
data remained unchanged. No STATE-Bench pairs were annotated.

## Curated-selection verification

Offline tests resolve all 202 rows against the canonical inputs, verify the
72/130 curation metadata counts, and exercise nonrectangular selections without
expansion. Invalid tail rows are rejected even with `--limit 1`; applicability
metadata stays outside requests. No live synthesis of this curated file was
performed as part of the selection change.

## Live check on curated pairs

The first nine curated pairs were subsequently run through the real CLI and
reviewed individually against their complete evidence. All 18 calls succeeded;
nine outcomes matched the pre-generation direct inspection. One evidence-only
dispute was reviewed and resolved without changing the raw output. See the
[per-pair verification report](verification.md) for findings, scope and input
hashes. The remaining 193 curated pairs were not covered by that run.
