"""One dataset contract for all sources; labels never inferred from missing pairs."""

import hashlib
import json
from pathlib import Path

import pytest

from lisa.evaluation.dataset import load_dataset

ROOT = Path(__file__).resolve().parents[1] / "datasets/policy_evaluation"


def write_dataset(tmp_path):
    trajectory = {
        "trace_id": "0123456789abcdef",
        "events": [
            {
                "span_id": "abcdef0123456789",
                "sequence": 0,
                "timestamp": "",
                "kind": "user_message",
                "content": "Hello",
            }
        ],
        "complete": True,
        "tool_events_captured": True,
        "tool_order_certain": True,
        "message_order_certain": True,
        "limitations": [],
    }
    records = {
        "trajectories": [
            {
                "id": "t1",
                "revision": 1,
                "name": "example",
                "lineage": "example",
                "provenance": {"source": "synthetic"},
                "trajectory": trajectory,
            }
        ],
        "policies": [
            {
                "id": "p1",
                "revision": 1,
                "lineage": "policy",
                "provenance": {"source": "synthetic"},
                "policy": {
                    "name": "Rule",
                    "scope": "workspace",
                    "approved": True,
                    "criteria": "Do not disclose a secret.",
                },
            }
        ],
        "annotations": [
            {
                "id": "a1",
                "revision": 1,
                "trajectory_ref": "t1@1",
                "policy_ref": "p1@1",
                "outcome": "not_applicable",
                "rationale": "No governed activity.",
                "evidence": [],
                "status": "proposed",
                "reviews": [],
            }
        ],
    }
    manifest = {
        "name": "test",
        "files": {},
        "batches": [
            {
                "trajectory_ref": "t1@1",
                "policy_refs": ["p1@1"],
                "annotation_refs": ["a1@1"],
                "split": "development",
            }
        ],
    }
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
    assert len(data.annotations) == 223
    assert len(data.pairs()) == 223
    assert len(data.pairs(adjudicated_only=True)) == 210
    assert {t.provenance["source"] for t in data.trajectories.values()} == {
        "synthetic",
        "state_bench",
        "connected_itinerary",
    }
    assert sum(len(batch.policy_refs) for batch in data.manifest.batches) == 313


def test_pair_builds_only_runtime_inputs(tmp_path):
    data = load_dataset(write_dataset(tmp_path))
    pair = data.pairs()[0]
    request = pair.request()
    assert request["policy"]["name"] == "Rule"
    assert request["trajectory"]["events"][0]["content"] == "Hello"
    assert "outcome" not in json.dumps(request)
    assert pair.annotation.outcome == "not_applicable"
    assert data.pairs(adjudicated_only=True) == []


def test_missing_annotation_remains_unlabeled(tmp_path):
    path = write_dataset(tmp_path)
    manifest = json.loads(path.read_text())
    manifest["batches"][0]["annotation_refs"] = []
    path.write_text(json.dumps(manifest))
    data = load_dataset(path)
    assert data.pairs() == []
    assert len(data.manifest.batches[0].policy_refs) == 1


def test_rejects_changed_release_content(tmp_path):
    path = write_dataset(tmp_path)
    (tmp_path / "policies.jsonl").write_text("{}\n")
    with pytest.raises(ValueError, match="digest"):
        load_dataset(path)


@pytest.mark.parametrize(
    "kind,change",
    [
        ("policies", lambda rows: rows.append(rows[0])),
        ("annotations", lambda rows: rows[0].update(policy_ref="unknown@1")),
        ("annotations", lambda rows: rows[0].update(evidence=["missing-span"])),
        ("annotations", lambda rows: rows[0].update(outcome="fail")),
        ("annotations", lambda rows: rows[0].update(status="adjudicated")),
    ],
)
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


@pytest.mark.parametrize(
    "change",
    [
        lambda m: m["batches"][0]["policy_refs"].append("p1@1"),
        lambda m: m["batches"][0]["policy_refs"].append("p1@2"),
        lambda m: m["batches"][0]["annotation_refs"].append("a1@1"),
        lambda m: m["batches"][0]["annotation_refs"].append("missing@1"),
        lambda m: m["batches"][0].update(trajectory_ref="missing@1"),
    ],
)
def test_rejects_invalid_manifest_membership(tmp_path, change):
    path = write_dataset(tmp_path)
    manifest = json.loads(path.read_text())
    change(manifest)
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        load_dataset(path)


def test_annotation_cannot_move_to_another_trajectory(tmp_path):
    path = write_dataset(tmp_path)
    mutate(path, "trajectories", lambda rows: rows.append({**rows[0], "id": "t2"}))
    mutate(path, "annotations", lambda rows: rows[0].update(trajectory_ref="t2@1"))
    with pytest.raises(ValueError, match="outside batch"):
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
        request = pair.request()
        trajectory = request["trajectory"]
        identifiers = [trajectory["trace_id"]]
        for event in trajectory["events"]:
            identifiers.append(event["span_id"])
            if event.get("call_id"):
                identifiers.append(event["call_id"])
        assert all(
            len(value) in {16, 32} and all(c in "0123456789abcdef" for c in value)
            for value in identifiers
        )
        rendered = format_trajectory_for_llm(trajectory["events"], short_ids=False).text
        assert pair.trajectory.name not in rendered
        assert pair.annotation.id not in rendered
        assert "reviews" not in request


def test_itinerary_controls_preserve_warning_before_confirmation_and_action():
    data = load_dataset()
    pairs = [p for p in data.pairs() if p.trajectory.provenance["source"] == "connected_itinerary"]
    assert {p.annotation.outcome for p in pairs} == {
        "compliant",
        "violation",
        "insufficient_evidence",
    }
    pair = next(p for p in pairs if p.annotation.outcome == "compliant")
    events = pair.trajectory.trajectory.events
    warning = next(e.sequence for e in events if e.content and "strand" in e.content)
    confirmation = next(e.sequence for e in events if e.content == "Yes, cancel it.")
    cancellation = next(
        e.sequence for e in events if e.kind == "tool_call" and e.name == "cancel_flight"
    )
    assert warning < confirmation < cancellation
    incomplete = next(p for p in pairs if p.annotation.outcome == "insufficient_evidence")
    assert not incomplete.trajectory.trajectory.complete
