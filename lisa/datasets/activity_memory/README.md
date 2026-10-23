# Lisa activity-memory benchmark

This is a 70-case, text-only benchmark for retrieving the right task context in
Lisa's single chat. It exercises per-activity memory after explicit activity
resolution and after forced global conversation compaction. It is a Lisa
adaptation and does not report an official LongMemEval or LongMemEval-V2 score.

## Splits and coverage

There are 35 development cases and 35 held-out benchmark cases. Each split has
three adapted cases for each of ten source abilities, plus five Lisa-specific
adversarial cases. Every ported case records its source benchmark, revision,
source question ID, source category, and adaptation note in `cases.jsonl`.
Each ordinary case interleaves two activities and then asks task-specific
follow-ups after the runner forces global context compaction. Adversarial cases
cover ambiguous task references, corrected facts, archived-parent/child
continuation, a deliberately pending activity snapshot, and cross-activity
fact separation. An ambiguous reference is tracked as a separate clarification
activity: Lisa loads only that activity's memory and asks which candidate the
user means, without reading either candidate thread or loading either
candidate's memory. The case rejects candidate-specific facts in the
clarification memory. The cross-activity adversary asks Lisa to copy another task's
facts into a resolver summary and an activity title. The expected resolver
summary stays task-scoped; storage tests separately inject contaminated
resolver metadata and verify that thread and memory projections suppress it.

The source-to-Lisa ability mapping is:

| Lisa ability | Source categories | Stable selection rule |
|---|---|---|
| Information extraction | LongMemEval `single-session-user` and `single-session-assistant` | Select from the union, ordered by SHA-256 of `question_id` |
| Cross-session reasoning | LongMemEval `multi-session` | Select by SHA-256 of `question_id` |
| Temporal reasoning | LongMemEval `temporal-reasoning` | Select by SHA-256 of `question_id` |
| Knowledge updates | LongMemEval `knowledge-update` | Select by SHA-256 of `question_id` |
| Abstention | LongMemEval IDs ending in `_abs` | Select by SHA-256 of `question_id` |
| Static state recall | LongMemEval-V2 `static-environment` | Require membership in the pinned small-haystack index and no image field; select by SHA-256 of `id` |
| Dynamic state tracking | LongMemEval-V2 `dynamic-environment` | Require membership in the pinned small-haystack index and no image field; select by SHA-256 of `id` |
| Workflow knowledge | LongMemEval-V2 `procedure` | Require membership in the pinned small-haystack index and no image field; select by SHA-256 of `id` |
| Environment gotchas | LongMemEval-V2 `errors-gotchas` | Require membership in the pinned small-haystack index; only text is adapted and no screenshot asset or image content is included |
| Premise awareness | LongMemEval-V2 categories ending in `-abs` | Require membership in the pinned small-haystack index and no image field; select by SHA-256 of `id` |

For each source ability, the first six eligible source IDs in ascending
SHA-256 order are selected. The first three are development cases and the next
three are held out. Each adapted case uses its pinned upstream question and
answer as a task-specific follow-up and evidence record, then places that record
in a Lisa-authored synthetic activity history. This preserves source-derived
memory questions and answer targets while making the task identity, history,
interleaving, explicit activity selection, and compaction behavior Lisa-specific.
The suite does not copy upstream conversations, trajectories, or raw records.
Each turn's expected reply must match the fact requested in its prompt.
`expects_abstention` is reserved for a detail absent from that activity; a
source-derived question with a recorded answer must check that answer even when
the upstream item belongs to a premise-awareness category. Dataset integrity
tests enforce these pairings for the adapted negative-premise cases.
Answer checks use short, independent fact anchors rather than requiring one
sentence's exact punctuation or formatting. This lets an equivalent answer
pass while still checking each material fact. The ambiguous-task adversary
requires a separate clarification activity and a memory load for that activity;
its memory title must not identify or reveal either candidate's task memory.
For source questions that explicitly request one explanation or one action,
`reply_must_match_exactly_one_of` lists mutually exclusive valid answer
options. Exactly one option must match all its `must_include` phrases, and no
`must_not_include` contradiction anchor from any option may appear. This also
rejects answers that list multiple alternatives or pair one valid option with
a contradictory action when the source question asks for one.
Turns that explicitly ask Lisa for verbatim or exact wording require the full
recorded answer text. A request to repeat a result briefly is scored against
its fact anchors; any attached exact-quote comparison remains a diagnostic and
does not fail answer success over punctuation or paraphrase. The scorer ignores
Markdown presentation and whitespace when it checks exact wording.
The V2 `errors-gotchas` questions may identify screenshots in their upstream
metadata, but only their text questions and answers are adapted; the suite
contains no screenshot assets or image content. `source_checksums` also pins the
source trajectory file used by the upstream release; the benchmark cases
themselves contain no trajectory dump.

## Pinned inputs

The manifest at `manifests/benchmark.json` pins the exact input commits and
SHA-256 checksums used for source selection:

- LongMemEval repository commit `9e0b455f4ef0e2ab8f2e582289761153549043fc`;
  cleaned Hugging Face data commit `98d7416c24c778c2fee6e6f3006e7a073259d48f`.
- LongMemEval-V2 repository commit `2cc8c540bdb87fe6761629b585e727e1c4704520`;
  Hugging Face data commit `f152293e235517d504809563c833d7190b8c713b`.

The checksums in the manifest cover the LongMemEval oracle input, LongMemEval-V2
question input, V2 small-haystack index, and V2 trajectory text source. The
`cases_sha256` covers the Lisa JSONL resource. Any case change requires updating
that digest and reviewing source IDs, split assignments, and this selection
description together.

## Attribution and licenses

- LongMemEval: Di Wu, Hongwei Wang, Wenhao Yu, Yuwei Zhang, Kai-Wei Chang, and
  Dong Yu, “LongMemEval: Benchmarking Chat Assistants on Long-Term Interactive
  Memory.” The repository and cleaned dataset are published under the MIT
  License. See the [official repository](https://github.com/xiaowu0162/LongMemEval)
  and [cleaned dataset card](https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned).
- LongMemEval-V2: authors and source details are listed in the
  [official repository](https://github.com/xiaowu0162/LongMemEval-V2) and
  [dataset card](https://huggingface.co/datasets/xiaowu0162/longmemeval-v2).
  The pinned dataset revision declares Apache-2.0.

This repository contains Lisa-authored activity scenarios with adapted
source-derived question and answer text rather than upstream data files. The
upstream IDs and revisions are retained for reproducibility and attribution.

## Run and score

Run the development split while iterating and the held-out split for acceptance:

```sh
uv run --project lisa --env-file .env \
  python lisa/scripts/evaluate_activity_memory_dataset.py --live --split development

uv run --project lisa --env-file .env \
  python lisa/scripts/evaluate_activity_memory_dataset.py --live --split benchmark
```

Each case uses an isolated transient Postgres database and workspace through
`scripts/test_stack.py` lifecycle helpers. The runner sets a process-local small
context window to trigger the real OpenHands conversation-checkpoint path; it
does not modify Lisa deployment settings. Reports are written under
`target/test-artifacts/lisa-activity-memory-benchmark/`.

Reports separate answer success, activity selection, evidence-reference
integrity, and abstention. Reference integrity confirms that citations resolve
to events from the selected activity; this suite does not establish semantic
support for every summary claim or final answer. The held-out gate requires at
least 32 of 35 cases to pass, valid memory evidence references in every memory
load, no detected cross-activity leaks, and no writes routed to the wrong
activity. It is a local product gate, not an upstream benchmark score.
