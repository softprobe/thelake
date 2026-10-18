# Synthesize pair annotations

`annotate.py` creates candidate annotations for an explicitly selected Cartesian
product of trajectory revisions and policy revisions. It uses Gemini 4 Argon
through Google REST with Application Default Credentials (ADC). The runtime's
API-key client and SDK dependencies are unchanged.

The checked-in `selection.json` selects exactly three synthetic policies and
three synthetic trajectories: **nine pairs and eighteen blind-sample calls**.
It includes an unapproved refund, a consented deletion, and a wire disclosure
with explicitly incomplete capture. There are no STATE-Bench trajectories in
this selection. The default pair limit is nine; the script never silently
expands to the manifest's catalogs or the whole dataset.

## Run

Use securely provisioned ADC and an authorized Google Cloud project. This tool
never logs in or changes credentials. Project resolution is explicit `--project`,
then `GOOGLE_CLOUD_PROJECT`, then ADC's project; location defaults to `global`.
The default model is `gemini-4-argon` with `HIGH` thinking.

From the repository root, after installing Lisa's dependencies:

```sh
uv run --project lisa --with 'google-auth[requests]>=2,<3' python \
  lisa/datasets/policy_evaluation/synthesize/annotate.py \
  --live --output /tmp/lisa-annotation-verification
```

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
  configuration, attempted/completed/cached/failed counts, separate outcome and
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
prompt/schema, selection and manifest changes require a new directory. Each
call additionally hashes its exact request, role, input revisions and model;
that per-call hash is authoritative for generation parameters and input bytes.
Cached raw responses are revalidated before reuse. Errors are never reused as
successful calls. A failed run does not export candidates, and failed resume
removes both previous candidate and comparison exports to avoid stale success artifacts.

This command never edits canonical inputs, annotations, or manifests. Review
candidates explicitly before publishing new annotation revisions. Keep generated
artifacts outside the public repository. Pair coverage is not violation coverage:
a dense cross product can be mostly easy negatives.

## Small-selection verification

Verified on 2026-10-17 using ADC and actual `gemini-4-argon` responses. Both
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
