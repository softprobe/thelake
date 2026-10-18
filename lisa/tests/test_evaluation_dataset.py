"""One dataset contract for all sources; labels never inferred from missing pairs."""

import hashlib
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path

import pytest

from lisa.evaluation.dataset import load_dataset

ROOT = Path(__file__).resolve().parents[1] / "datasets/policy_evaluation"
SAMPLE_SPEC = importlib.util.spec_from_file_location(
    "lisa_policy_evaluation_sample",
    Path(__file__).resolve().parents[2]
    / ".agents/skills/run-lisa-policy-evaluation/scripts/create_sample_manifest.py",
)
SAMPLE = importlib.util.module_from_spec(SAMPLE_SPEC)
sys.modules[SAMPLE_SPEC.name] = SAMPLE
SAMPLE_SPEC.loader.exec_module(SAMPLE)


def write_dataset(tmp_path):
    trajectory = {
        "trace_id": "0123456789abcdef0123456789abcdef",
        "events": [{"span_id": "abcdef0123456789", "sequence": 0, "timestamp": "", "kind": "user_message", "content": "Hello"}],
        "complete": True,
        "tool_events_captured": True,
        "tool_order_certain": True,
        "message_order_certain": True,
        "limitations": [],
    }
    records = {
        "trajectories": [{"session_id": "S001", "name": "example", "lineage": "example", "provenance": {"source": "synthetic"}, "trajectory": trajectory}],
        "policies": [{"policy_id": "P1.1", "lineage": "policy", "provenance": {"source": "synthetic"}, "policy": {"name": "Rule", "scope": "workspace", "approved": True, "criteria": "Do not disclose a secret."}}],
        "annotations": [{"session_id": "S001", "policy_id": "P1.1", "outcome": "not_applicable", "rationale": "No governed activity.", "evidence": [], "status": "proposed", "reviews": []}],
    }
    manifest = {"name": "test", "files": {}, "batches": [{"session_id": "S001", "policy_ids": ["P1.1"], "split": "development"}]}
    for kind, rows in records.items():
        path = tmp_path / f"{kind}.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        manifest["files"][kind] = hashlib.sha256(path.read_bytes()).hexdigest()
    (tmp_path / "manifests").mkdir()
    path = tmp_path / "manifests/test.json"
    path.write_text(json.dumps(manifest))
    return path


def mutate(path, kind, change):
    root = path.parent.parent
    file = root / f"{kind}.jsonl"
    rows = [json.loads(line) for line in file.read_text().splitlines()]
    change(rows)
    file.write_text("".join(json.dumps(row) + "\n" for row in rows))
    manifest = json.loads(path.read_text())
    manifest["files"][kind] = hashlib.sha256(file.read_bytes()).hexdigest()
    path.write_text(json.dumps(manifest))


def test_release_has_all_sources_without_inventing_pairs():
    data = load_dataset(ROOT / "manifests/evaluation.json")
    assert len(data.trajectories) == 48
    assert len(data.annotations) == 202
    assert len(data.pairs()) == 202
    assert len(data.pairs(adjudicated_only=True)) == 202
    assert {t.provenance["source"] for t in data.trajectories.values()} == {"synthetic", "state_bench", "connected_itinerary"}
    assert sum(len(batch.policy_ids) for batch in data.manifest.batches) == 202


def test_pair_builds_only_runtime_inputs(tmp_path):
    data = load_dataset(write_dataset(tmp_path))
    pair = data.pairs()[0]
    request = pair.request()
    assert list(data.trajectories) == ["S001"]
    assert list(data.policies) == ["P1.1"]
    assert list(data.annotations) == [("S001", "P1.1")]
    assert request["run_id"] == "S001:P1.1"
    assert request["policy"]["name"] == "Rule"
    assert request["trajectory"]["events"][0]["content"] == "Hello"
    assert "outcome" not in json.dumps(request)
    assert pair.annotation.outcome == "not_applicable"
    assert data.pairs(adjudicated_only=True) == []


def test_rejects_duplicate_annotation_for_session_policy_pair(tmp_path):
    path = write_dataset(tmp_path)
    mutate(path, "annotations", lambda rows: rows.append(dict(rows[0])))
    with pytest.raises(ValueError, match="Duplicate annotation for pair"):
        load_dataset(path)


def test_missing_annotation_remains_unlabeled(tmp_path):
    path = write_dataset(tmp_path)
    mutate(path, "annotations", lambda rows: rows.clear())
    data = load_dataset(path)
    assert data.pairs() == []
    assert len(data.manifest.batches[0].policy_ids) == 1


def test_rejects_changed_release_content(tmp_path):
    path = write_dataset(tmp_path)
    (tmp_path / "policies.jsonl").write_text("{}\n")
    with pytest.raises(ValueError, match="digest"):
        load_dataset(path)


@pytest.mark.parametrize("kind,change", [
    ("policies", lambda rows: rows.append(rows[0])),
    ("annotations", lambda rows: rows[0].update(policy_id="P1.9")),
    ("annotations", lambda rows: rows[0].update(evidence=["missing-span"])),
    ("annotations", lambda rows: rows[0].update(outcome="fail")),
    ("annotations", lambda rows: rows[0].update(status="adjudicated")),
])
def test_rejects_invalid_dataset_records(tmp_path, kind, change):
    path = write_dataset(tmp_path)
    mutate(path, kind, change)
    with pytest.raises(ValueError):
        load_dataset(path)


def test_rejects_lineage_crossing_splits(tmp_path):
    path = write_dataset(tmp_path)
    manifest = json.loads(path.read_text())
    manifest["batches"].append({**manifest["batches"][0], "split": "test"})
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="lineage"):
        load_dataset(path)


@pytest.mark.parametrize("change", [
    lambda m: m["batches"][0]["policy_ids"].append("P1.1"),
    lambda m: m["batches"][0]["policy_ids"].append("P1.9"),
    lambda m: m["batches"][0].update(session_id="missing"),
])
def test_rejects_invalid_manifest_membership(tmp_path, change):
    path = write_dataset(tmp_path)
    manifest = json.loads(path.read_text())
    change(manifest)
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        load_dataset(path)


def test_annotation_cannot_move_outside_manifest_batch(tmp_path):
    path = write_dataset(tmp_path)
    mutate(path, "annotations", lambda rows: rows[0].update(session_id="S002"))
    with pytest.raises(ValueError):
        load_dataset(path)


def test_requires_named_input_provenance(tmp_path):
    path = write_dataset(tmp_path)
    mutate(path, "policies", lambda rows: rows[0].update(provenance={}))
    with pytest.raises(ValueError, match="source"):
        load_dataset(path)


def test_all_source_inputs_use_opaque_ids_and_hide_annotations():
    from lisa.evaluation.prompt import format_trajectory_for_llm

    data = load_dataset()
    for pair in data.pairs():
        trajectory = pair.request()["trajectory"]
        identifiers = [trajectory["trace_id"]]
        for event in trajectory["events"]:
            identifiers.append(event["span_id"])
            if event.get("call_id"):
                identifiers.append(event["call_id"])
        assert all(len(value) in {16, 32} and all(c in "0123456789abcdef" for c in value) for value in identifiers)
        rendered = format_trajectory_for_llm(trajectory["events"], short_ids=False).text
        assert pair.trajectory.name not in rendered
        assert set(pair.request()) == {"run_id", "policy", "trajectory"}


def test_itinerary_controls_preserve_warning_before_confirmation_and_action():
    data = load_dataset()
    pairs = [
        pair
        for pair in data.pairs()
        if pair.trajectory.provenance["source"] == "connected_itinerary"
        and pair.policy.policy.name == "Connected itinerary disclosure"
    ]
    assert {pair.annotation.outcome for pair in pairs} == {
        "compliant",
        "violation",
        "insufficient_evidence",
    }
    pair = next(pair for pair in pairs if pair.annotation.outcome == "compliant")
    events = pair.trajectory.trajectory.events
    warning = next(event.sequence for event in events if event.content and "strand" in event.content)
    confirmation = next(event.sequence for event in events if event.content == "Yes, cancel it.")
    cancellation = next(
        event.sequence
        for event in events
        if event.kind == "tool_call" and event.name == "cancel_flight"
    )
    assert warning < confirmation < cancellation
    incomplete = next(
        pair for pair in pairs if pair.annotation.outcome == "insufficient_evidence"
    )
    assert not incomplete.trajectory.trajectory.complete


def test_official_golden_release_exactly_covers_curated_pairs():
    data = load_dataset(ROOT / "manifests/evaluation.json")
    selected = [json.loads(line) for line in (ROOT / "golden_pairs.jsonl").read_text().splitlines()]
    expected = {(row["session_id"], row["policy_id"]) for row in selected}
    actual = {(pair.trajectory.session_id, pair.policy.policy_id) for pair in data.pairs()}
    catalog = {(batch.session_id, policy_id) for batch in data.manifest.batches for policy_id in batch.policy_ids}
    assert len(expected) == len(selected) == 202
    assert actual == catalog == expected
    assert len(data.pairs(adjudicated_only=True)) == 202
    assert Counter(pair.annotation.outcome for pair in data.pairs()) == {"violation": 17, "compliant": 18, "not_applicable": 143, "insufficient_evidence": 24}
    assert all(any(review.kind == "ai" and "adjudicat" in review.scope.lower() for review in pair.annotation.reviews) for pair in data.pairs())


def test_annotation_identity_is_the_session_policy_pair():
    data = load_dataset(ROOT / "manifests/evaluation.json")
    assert set(data.annotations) == {(p.trajectory.session_id, p.policy.policy_id) for p in data.pairs()}
    assert all(pair.annotation.reviews[-1].outcome == pair.annotation.outcome for pair in data.pairs())


def test_sample_manifest_preserves_exact_session_policy_pairs(tmp_path):
    data = load_dataset()
    selected = SAMPLE.choose_pairs(data, count=10, split="test", seed=17)
    path = SAMPLE.write_manifest(
        data, selected, output_root=tmp_path / "sample", split="test", seed=17
    )
    sample = load_dataset(path)
    pairs = sample.pairs(adjudicated_only=True)

    assert len(pairs) == 10
    assert Counter(pair.annotation.outcome for pair in pairs) == Counter(
        pair.annotation.outcome for pair in selected
    )
    assert {
        (pair.trajectory.session_id, pair.policy.policy_id) for pair in pairs
    } == {(pair.trajectory.session_id, pair.policy.policy_id) for pair in selected}
