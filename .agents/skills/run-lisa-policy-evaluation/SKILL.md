---
name: run-lisa-policy-evaluation
description: Run Lisa's labeled trajectory-policy evaluation on a balanced, reproducible sample or a frozen manifest, then interpret triage and end-to-end results.
---

# Run Lisa Policy Evaluation

Use this skill for dataset-based Lisa policy quality runs. Use `run-lisa-evaluation-dogfood` for the autonomous scheduled agent-to-issue path.

## Sample and run

- Start with the frozen manifest at `lisa/datasets/policy_evaluation/manifests/evaluation.json` and inspect `docs/how-to/policy-triage-evaluation.md` plus the current dataset release notes. Treat the dataset README and release notes as authoritative when older guide counts differ.
- For a 10–20 pair sample, use `scripts/create_sample_manifest.py`. Its default is 16 pairs from the `test` split, balanced across `violation`, `compliant`, `not_applicable`, and `insufficient_evidence`, with source and trajectory diversity where available. It selects adjudicated pairs only, retains exact policy/annotation references, and writes a temporary self-contained manifest without editing canonical dataset files. Review its printed sample before the live run.
- Run the recommended two-stage pipeline with `lisa/scripts/evaluate_policies.py --live --mode pipeline --manifest <sample-manifest> --output <report.json>`. The run sends trajectory/policy inputs to OpenAI Decisions and selected inputs to Gemini; provider calls may incur charges. Run only when the user has explicitly requested a live evaluation.
- Keep sample manifest, sample summary, and report outside the repository. Never put credentials or raw provider secrets in commands, files, or output. Provider keys must be available to the process; use `uv run --env-file <known dotenv file>` only when needed and never print dotenv contents.
- Preserve the first provider or validation failure. Do not relabel data to fit model output or retry failed calls automatically. Keep the manifest digest, sample seed/count, split, model configuration and report path with the results.

## Interpret results

- `violation_recall` measures triage escalation of adjudicated violation pairs. Triage `insufficient_evidence` is an escalation.
- `end_to_end_violation_recall` measures final `fail` verdicts across all eligible adjudicated violation pairs, including violations missed by triage.
- Review `triage_confusion`, `end_to_end_detection_confusion`, `end_to_end_precision`, `judge_accuracy_on_escalated_resolved_pairs`, `uncertain_escalation_recall`, `workload`, and `capture_gate` alongside recall.
- `not_applicable` is a negative label for that exact policy/trajectory pair. `compliant` is a different negative: the policy applied and the behavior satisfied it. `insufficient_evidence` is reported separately from binary accuracy. Missing annotations remain unreviewed.
- Always report raw support counts. The curated release is AI-adjudicated, has few violation examples, overlaps policy families across splits, and is not a human-reviewed or production-prevalence benchmark. A small run is diagnostic; do not present its rates as a precise production estimate.
