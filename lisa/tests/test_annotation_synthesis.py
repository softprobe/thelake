"""Annotation synthesis uses full evidence, two blind passes and private run artifacts."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from lisa.evaluation.dataset import Annotation, load_dataset

DIRECTORY = Path(__file__).resolve().parents[1] / "datasets/policy_evaluation/synthesize"
spec = importlib.util.spec_from_file_location("annotation_synthesis", DIRECTORY / "annotate.py")
synthesis = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = synthesis
spec.loader.exec_module(synthesis)


def selection():
    # Explicit nine-pair smoke sample; production only consumes listed pairs.
    trajectories = ["S001", "S006", "S011"]
    policies = ["P1.1", "P1.2", "P1.3"]
    return [
        synthesis.SelectedPair(session_id=t, policy_id=p)
        for t in trajectories
        for p in policies
    ]


def write_pairs(path, selected):
    path.write_text("".join(row.model_dump_json(exclude_none=True) + "\n" for row in selected))
    return path


class FakeClient:
    def __init__(self, disagree=False):
        self.requests = []
        self.disagree = disagree

    def generate(self, request):
        self.requests.append(request)
        inputs = json.loads(request["contents"][0]["parts"][0]["text"])
        span = inputs["trajectory"]["events"][0]["span_id"]
        outcome = "not_applicable"
        if (
            self.disagree
            and "independent review" in request["systemInstruction"]["parts"][0]["text"]
        ):
            outcome = "violation"
        return {
            "candidates": [
                {
                    "finishReason": "STOP",
                    "content": {
                        "parts": [
                            {
                                "text": json.dumps(
                                    {
                                        "outcome": outcome,
                                        "rationale": (
                                            "The policy trigger is absent from this evidence."
                                        ),
                                        "evidence": [span] if outcome == "violation" else [],
                                        "missing_facts": [],
                                    }
                                )
                            }
                        ]
                    },
                }
            ],
            "modelVersion": "test-model",
            "usageMetadata": {"totalTokenCount": 10},
        }


def test_selection_is_exactly_three_by_three():
    data = load_dataset()
    chosen = selection()
    pairs = synthesis.select_pairs(data, chosen)
    assert len(pairs) == 9
    assert len({t.session_id for t, _ in pairs}) == 3
    assert len({p.policy_id for _, p in pairs}) == 3
    assert all(t.provenance["source"] == "synthetic" for t, _ in pairs)


def test_selection_rejects_duplicate_pairs():
    selected = selection()
    with pytest.raises(ValueError, match="Duplicate"):
        synthesis.select_pairs(load_dataset(), [*selected, selected[0]], max_pairs=10)


def test_prompts_hide_labels_and_keep_full_incomplete_evidence():
    data = load_dataset()
    for trajectory, policy in synthesis.select_pairs(data, selection()):
        request = synthesis.build_request(trajectory, policy, "annotator", "HIGH")
        payload = json.loads(request["contents"][0]["parts"][0]["text"])
        assert set(payload) == {"policy", "trajectory"}
        assert payload["policy"] == policy.policy.model_dump(mode="json")
        assert payload["trajectory"] == trajectory.trajectory.model_dump(
            mode="json", exclude_unset=True
        )
        assert trajectory.name not in json.dumps(request)
        if not trajectory.trajectory.complete:
            assert payload["trajectory"]["limitations"]


def test_nine_pairs_get_two_independent_passes_and_resume(tmp_path):
    data = load_dataset()
    client = FakeClient()
    report = synthesis.run(data, selection(), tmp_path, client, workers=2)
    assert len(client.requests) == 18
    assert report["pair_count"] == 9
    assert report["attempted_calls"] == 18
    assert report["agreement_count"] == 9
    rows = [
        Annotation.model_validate_json(line)
        for line in (tmp_path / "annotations.jsonl").read_text().splitlines()
    ]
    assert len(rows) == 9
    assert all(row.status == "proposed" and len(row.reviews) == 2 for row in rows)
    assert all(row.reviews[0].kind == "ai" for row in rows)
    assert all(
        "outcome" not in request["contents"][0]["parts"][0]["text"] for request in client.requests
    )
    report = synthesis.run(data, selection(), tmp_path, client, workers=2)
    assert len(client.requests) == 18
    assert report["attempted_calls"] == 0
    assert report["cached_calls"] == 18
    assert report["completed_calls"] == 0
    assert report["new_response_tokens"] == 0
    assert report["cached_response_tokens"] == 180


def test_disagreement_is_not_adjudicated(tmp_path):
    report = synthesis.run(load_dataset(), selection(), tmp_path, FakeClient(disagree=True))
    assert report["disagreement_count"] == 9
    rows = [json.loads(line) for line in (tmp_path / "annotations.jsonl").read_text().splitlines()]
    assert all(row["status"] == "disputed" for row in rows)


@pytest.mark.parametrize("finish_reason", ["MAX_TOKENS", "SAFETY", "CONTINUATION"])
def test_noncomplete_responses_are_rejected(finish_reason):
    response = FakeClient().generate(
        {
            "contents": [
                {"parts": [{"text": json.dumps({"trajectory": {"events": [{"span_id": "span"}]}})}]}
            ],
            "systemInstruction": {"parts": [{"text": ""}]},
        }
    )
    response["candidates"][0]["finishReason"] = finish_reason
    with pytest.raises(ValueError, match="finish"):
        synthesis.parse_response(response, {"span"})


def test_unknown_evidence_and_unsupported_labels_are_rejected():
    for result in [
        {
            "outcome": "violation",
            "rationale": "Breach",
            "evidence": ["invented"],
            "missing_facts": [],
        },
        {"outcome": "compliant", "rationale": "Fine", "evidence": [], "missing_facts": []},
        {
            "outcome": "insufficient_evidence",
            "rationale": "Unknown",
            "evidence": [],
            "missing_facts": [],
        },
        {"outcome": "pass", "rationale": "Fine", "evidence": ["span"], "missing_facts": []},
    ]:
        response = {
            "candidates": [
                {"finishReason": "STOP", "content": {"parts": [{"text": json.dumps(result)}]}}
            ]
        }
        with pytest.raises(ValueError):
            synthesis.parse_response(response, {"span"})


def test_failed_response_is_recorded_and_never_exported_as_negative(tmp_path):
    class BadClient:
        def generate(self, request):
            return {"candidates": []}

    with pytest.raises(RuntimeError, match="failed"):
        synthesis.run(load_dataset(), selection(), tmp_path, BadClient())
    assert not (tmp_path / "annotations.jsonl").exists()
    assert not (tmp_path / "comparisons.jsonl").exists()
    report = json.loads((tmp_path / "run.json").read_text())
    assert report["failed_calls"] > 0


def test_long_event_is_not_clipped():
    trajectory, policy = synthesis.select_pairs(load_dataset(), selection())[0]
    trajectory = trajectory.model_copy(deep=True)
    trajectory.trajectory.events[0].content = "x" * 5000 + "decisive suffix"
    request = synthesis.build_request(trajectory, policy, "annotator", "HIGH")
    assert "x" * 5000 + "decisive suffix" in request["contents"][0]["parts"][0]["text"]


def test_failed_resume_removes_stale_successful_export(tmp_path):
    data = load_dataset()
    client = FakeClient()
    synthesis.run(data, selection(), tmp_path, client)
    path = next((tmp_path / "calls").glob("*.json"))
    cached = json.loads(path.read_text())
    cached["response"]["candidates"][0]["finishReason"] = "MAX_TOKENS"
    path.write_text(json.dumps(cached))
    with pytest.raises(RuntimeError, match="failed"):
        synthesis.run(data, selection(), tmp_path, client)
    assert len(client.requests) == 18
    assert not (tmp_path / "annotations.jsonl").exists()
    assert not (tmp_path / "comparisons.jsonl").exists()


def test_custom_dataset_cannot_be_output_directory(tmp_path):
    with pytest.raises(ValueError, match="canonical dataset"):
        synthesis.run(load_dataset(), selection(), tmp_path, FakeClient(), dataset_root=tmp_path)


def test_resume_rejects_different_request_configuration(tmp_path):
    client = FakeClient()
    synthesis.run(load_dataset(), selection(), tmp_path, client)
    with pytest.raises(ValueError, match="different run configuration"):
        synthesis.run(load_dataset(), selection(), tmp_path, client, thinking_level="LOW")
    assert len(client.requests) == 18


def test_unknown_selection_fails_before_provider_calls(tmp_path):
    chosen = [selection()[0].model_copy(update={"policy_id": "P1.999"})]
    client = FakeClient()
    with pytest.raises(ValueError, match="unknown policy"):
        synthesis.run(load_dataset(), chosen, tmp_path, client)
    assert client.requests == []


def test_cli_requires_explicit_live_authorization_before_client(tmp_path, monkeypatch):
    pair_file = write_pairs(tmp_path / "pairs.jsonl", selection())
    monkeypatch.setattr(
        sys, "argv", ["annotate.py", "--pairs", str(pair_file), "--output", str(tmp_path)]
    )
    monkeypatch.setattr(
        synthesis, "ADCClient", lambda *_: pytest.fail("No provider initialization")
    )
    with pytest.raises(SystemExit) as error:
        synthesis.main()
    assert error.value.code == 2


def test_cli_enforces_pair_limit_before_client(tmp_path, monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "annotate.py",
            "--pairs",
            str(write_pairs(tmp_path / "pairs.jsonl", selection())),
            "--output",
            str(tmp_path),
            "--live",
            "--max-pairs",
            "8",
        ],
    )
    monkeypatch.setattr(
        synthesis, "ADCClient", lambda *_: pytest.fail("No provider initialization")
    )
    with pytest.raises(ValueError, match="pair limit"):
        synthesis.main()


def test_cli_wires_the_nine_pair_run_and_closes_client(tmp_path, monkeypatch, capsys):
    client = FakeClient()
    closed = []
    client.close = lambda: closed.append(True)
    monkeypatch.setattr(synthesis, "ADCClient", lambda *_: client)
    pair_file = write_pairs(tmp_path / "pairs.jsonl", selection())
    monkeypatch.setattr(
        sys, "argv", ["annotate.py", "--pairs", str(pair_file), "--live", "--output", str(tmp_path)]
    )
    synthesis.main()
    assert len(client.requests) == 18
    assert closed == [True]
    assert json.loads((tmp_path / "run.json").read_text())["pair_count"] == 9
    assert '"maximum_new_calls": 18' in capsys.readouterr().out


def test_matching_label_with_different_evidence_requires_review(tmp_path):
    class DifferentEvidenceClient(FakeClient):
        def generate(self, request):
            response = super().generate(request)
            inputs = json.loads(request["contents"][0]["parts"][0]["text"])
            reviewer = "independent review" in request["systemInstruction"]["parts"][0]["text"]
            response["candidates"][0]["content"]["parts"][0]["text"] = json.dumps(
                {
                    "outcome": "violation",
                    "rationale": "Reviewer rationale" if reviewer else "Annotator rationale",
                    "evidence": [inputs["trajectory"]["events"][-1 if reviewer else 0]["span_id"]],
                    "missing_facts": ["Context still needs review"] if reviewer else [],
                }
            )
            return response

    chosen = selection()
    chosen = [chosen[4]]
    report = synthesis.run(load_dataset(), chosen, tmp_path, DifferentEvidenceClient())
    assert report["agreement_count"] == 0
    assert report["outcome_agreement_count"] == 1
    assert report["evidence_agreement_count"] == 0
    row = json.loads((tmp_path / "annotations.jsonl").read_text())
    assert row["status"] == "disputed"
    comparison = json.loads((tmp_path / "comparisons.jsonl").read_text())
    assert (comparison["session_id"], comparison["policy_id"]) == (row["session_id"], row["policy_id"])
    assert comparison["dispute_reasons"] == ["evidence_mismatch"]
    assert comparison["judgments"]["annotator"]["rationale"] == "Annotator rationale"
    assert comparison["judgments"]["reviewer"]["rationale"] == "Reviewer rationale"
    assert comparison["judgments"]["reviewer"]["missing_facts"] == ["Context still needs review"]
    assert (
        comparison["judgments"]["annotator"]["evidence"]
        != comparison["judgments"]["reviewer"]["evidence"]
    )


def test_evidence_agreement_ignores_order_and_duplicate_citations():
    first = synthesis.Judgment(
        outcome="violation", rationale="One breach", evidence=["a", "b", "a"], missing_facts=[]
    )
    second = synthesis.Judgment(
        outcome="violation", rationale="Same breach", evidence=["b", "a"], missing_facts=[]
    )
    assert synthesis.compare_judgments(first, second)["dispute_reasons"] == []


def test_same_evidence_does_not_override_outcome_disagreement():
    first = synthesis.Judgment(
        outcome="violation", rationale="Breach", evidence=["a"], missing_facts=[]
    )
    second = synthesis.Judgment(
        outcome="compliant", rationale="Satisfied", evidence=["a"], missing_facts=[]
    )
    assert synthesis.compare_judgments(first, second)["dispute_reasons"] == ["outcome_mismatch"]


def test_invalid_generated_answer_still_records_new_response_tokens(tmp_path):
    class InvalidClient:
        def generate(self, request):
            return {"candidates": [], "usageMetadata": {"totalTokenCount": 7}}

    with pytest.raises(RuntimeError, match="failed"):
        synthesis.run(load_dataset(), selection(), tmp_path, InvalidClient())
    report = json.loads((tmp_path / "run.json").read_text())
    assert report["attempted_calls"] == 18
    assert report["completed_calls"] == 0
    assert report["new_response_tokens"] == 126
    assert report["token_usage_complete"] is True


def test_transport_failure_reports_unknown_usage(tmp_path):
    class TimeoutClient:
        def generate(self, request):
            raise TimeoutError("No response")

    with pytest.raises(RuntimeError, match="failed"):
        synthesis.run(load_dataset(), selection(), tmp_path, TimeoutClient())
    report = json.loads((tmp_path / "run.json").read_text())
    assert report["attempted_calls"] == 18
    assert report["completed_calls"] == 0
    assert report["new_response_tokens"] == 0
    assert report["token_usage_complete"] is False
    assert report["new_calls_without_usage"] == 18


@pytest.mark.parametrize("usage", [None, {}, {"totalTokenCount": True}, {"totalTokenCount": -1}])
def test_missing_or_invalid_usage_does_not_discard_valid_judgments(tmp_path, usage):
    class MissingUsageClient(FakeClient):
        def generate(self, request):
            response = super().generate(request)
            response["usageMetadata"] = usage
            return response

    report = synthesis.run(load_dataset(), selection(), tmp_path, MissingUsageClient())
    assert report["completed_calls"] == 18
    assert report["new_calls_without_usage"] == 18
    assert report["token_usage_complete"] is False
    assert report["new_response_tokens"] == 0


def test_curated_pairs_are_not_expanded(tmp_path):
    selected = [selection()[0], selection()[4]]
    report = synthesis.run(load_dataset(), selected, tmp_path, FakeClient())
    assert report["pair_count"] == 2
    assert report["attempted_calls"] == 4
    rows = [json.loads(line) for line in (tmp_path / "annotations.jsonl").read_text().splitlines()]
    assert [(r["session_id"], r["policy_id"]) for r in rows] == [
        (r.session_id, r.policy_id) for r in selected
    ]


def test_load_curated_file_preserves_rows_and_metadata():
    data = load_dataset()
    path = DIRECTORY.parent / "golden_pairs.jsonl"
    selected, file_digest = synthesis.load_pairs(path, data)
    assert len(selected) == 202
    assert sum(p.domain_applicable is True for p in selected) == 72
    assert sum(p.domain_applicable is False for p in selected) == 130
    assert len(file_digest) == 64
    assert len(synthesis.select_pairs(data, selected, max_pairs=202)) == 202
    with pytest.raises(ValueError, match="pair limit"):
        synthesis.select_pairs(data, selected)


@pytest.mark.parametrize(
    "row",
    [
        {"session_id": "unknown", "policy_id": "P1.999"},
        {
            "session_id": "S001",
            "policy_id": "P1.1",
            "domain_applicable": "false",
        },
    ],
)
def test_invalid_tail_is_rejected_before_cli_limit(tmp_path, monkeypatch, row):
    path = write_pairs(tmp_path / "pairs.jsonl", selection()[:1])
    with path.open("a") as file:
        file.write(json.dumps(row) + "\n")
    monkeypatch.setattr(
        sys,
        "argv",
        ["annotate.py", "--pairs", str(path), "--limit", "1", "--live", "--output", str(tmp_path)],
    )
    monkeypatch.setattr(
        synthesis, "ADCClient", lambda *_: pytest.fail("No provider initialization")
    )
    with pytest.raises(ValueError):
        synthesis.main()


def test_domain_curation_metadata_never_becomes_model_input(tmp_path):
    selected = [selection()[0].model_copy(update={"domain_applicable": False})]
    client = FakeClient()
    synthesis.run(load_dataset(), selected, tmp_path, client)
    assert all("domain_applicable" not in json.dumps(request) for request in client.requests)
    record = json.loads((tmp_path / "run.json").read_text())
    assert record["pairs"][0]["domain_applicable"] is False


def test_cli_explicit_limit_records_full_pair_source(tmp_path, monkeypatch, capsys):
    client = FakeClient()
    client.close = lambda: None
    monkeypatch.setattr(synthesis, "ADCClient", lambda *_: client)
    monkeypatch.setattr(
        sys, "argv", ["annotate.py", "--live", "--limit", "2", "--output", str(tmp_path)]
    )
    synthesis.main()
    report = json.loads((tmp_path / "run.json").read_text())
    assert len(client.requests) == 4
    assert len(report["pairs"]) == 2
    assert report["pair_source"]["total_pairs"] == 202
    assert report["pair_source"]["limit"] == 2
    assert len(report["pair_source"]["digest"]) == 64
    assert '"available_pairs": 202' in capsys.readouterr().out
