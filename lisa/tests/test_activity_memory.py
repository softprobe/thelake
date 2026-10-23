from __future__ import annotations

import pytest

from lisa.activity_memory import (
    ACTIVITY_MEMORY_SYSTEM_PROMPT,
    build_activity_memory_payload,
    can_read_activity_memory,
    validate_activity_memory_evidence_refs,
)


def _event(seq: int, reference_id: str, summary: str) -> dict:
    return {
        "seq": seq,
        "event_type": "chat.user",
        "title": f"User message {seq}",
        "summary": summary,
        "actor": "user",
        "reference_type": "message",
        "reference_id": reference_id,
    }


def test_activity_memory_payload_uses_only_events_through_its_watermark():
    payload = build_activity_memory_payload(
        activity_id="activity-1",
        activity_title="Orchid ledger check",
        activity_status="waiting",
        next_action="Verify corrected ledger entry",
        through_event_seq=3,
        events=[
            _event(3, "message-3", "The corrected invoice total is 127."),
            _event(1, "message-1", "The ledger task is CTX-ORCHID-731."),
            _event(4, "message-4", "This later event must not enter the snapshot."),
            _event(2, "message-2", "The first total was wrong; use 127."),
        ],
        configuration_fingerprint="config-v1",
    )

    assert payload["event_watermark"] == 3
    assert payload["activity_id"] == "activity-1"
    assert [source["reference_id"] for source in payload["source_event_refs"]] == [
        "message-1",
        "message-2",
        "message-3",
    ]
    assert "later event must not enter" not in payload["messages"]
    assert "CTX-ORCHID-731" in payload["messages"]
    assert payload["configuration_fingerprint"] == "config-v1"


def test_activity_memory_payload_uses_only_a_compatible_older_snapshot():
    previous = {
        "source_event_seq": 2,
        "configuration_fingerprint": "config-v1",
        "summary": "The ledger task is CTX-ORCHID-731.",
        "source_event_refs": [
            {"sequence": 1, "reference_type": "message", "reference_id": "message-1"},
            {"sequence": 2, "reference_type": "message", "reference_id": "message-2"},
        ],
    }

    payload = build_activity_memory_payload(
        activity_id="activity-1",
        activity_title="Orchid ledger check",
        activity_status="waiting",
        next_action="Verify corrected ledger entry",
        through_event_seq=4,
        events=[
            _event(1, "message-1", "The ledger task is CTX-ORCHID-731."),
            _event(2, "message-2", "The first total was wrong; use 127."),
            _event(3, "message-3", "The corrected invoice total is 127."),
            _event(4, "message-4", "The reviewer accepted the correction."),
        ],
        configuration_fingerprint="config-v1",
        previous_snapshot=previous,
    )

    assert payload["base_summary"] == previous["summary"]
    assert payload["base_event_watermark"] == 2
    assert "message-1" not in payload["messages"]
    assert "message-2" not in payload["messages"]
    assert "reviewer accepted" in payload["messages"]
    assert len(payload["source_event_refs"]) == 4


def test_activity_memory_payload_ignores_future_or_incompatible_base_snapshot():
    previous = {
        "source_event_seq": 8,
        "configuration_fingerprint": "old-config",
        "summary": "A future or incompatible summary must not be adopted.",
        "source_event_refs": [],
    }

    payload = build_activity_memory_payload(
        activity_id="activity-1",
        activity_title="Orchid ledger check",
        activity_status="succeeded",
        next_action=None,
        through_event_seq=3,
        events=[_event(1, "message-1", "The amount is 127."), _event(3, "message-3", "Approved.")],
        configuration_fingerprint="config-v1",
        previous_snapshot=previous,
    )

    assert payload["base_summary"] is None
    assert payload["base_event_watermark"] == 0
    assert "The amount is 127" in payload["messages"]
    assert "future or incompatible" not in payload["messages"]


def test_activity_memory_marks_database_bounded_capture_as_truncated():
    payload = build_activity_memory_payload(
        activity_id="activity-1",
        activity_title="Orchid ledger check",
        activity_status="waiting",
        next_action=None,
        through_event_seq=80,
        events=[{**_event(80, "message-80", "Latest fact."), "_activity_event_count": 81}],
        configuration_fingerprint="config-v1",
    )

    assert payload["history_truncated"] is True


def test_activity_memory_does_not_offer_references_for_events_omitted_from_model_input():
    payload = build_activity_memory_payload(
        activity_id="activity-1",
        activity_title="Orchid ledger check",
        activity_status="waiting",
        next_action=None,
        through_event_seq=2,
        events=[
            _event(1, "large-event", "Too large to include." + ("x" * 50_000)),
            _event(2, "small-event", "The approved amount is 127."),
        ],
        configuration_fingerprint="config-v1",
    )

    assert "large-event" not in payload["messages"]
    assert [reference["sequence"] for reference in payload["source_event_refs"]] == [2]


@pytest.mark.parametrize(
    ("active_id", "requested_id", "parent_id", "parent_status", "allowed"),
    [
        ("child", "child", "parent", "succeeded", True),
        ("child", "parent", "parent", "succeeded", True),
        ("child", "parent", "parent", "failed", True),
        ("child", "parent", "parent", "waiting", False),
        ("child", "sibling", "parent", "succeeded", False),
        ("child", "parent", "other-parent", "succeeded", False),
    ],
)
def test_memory_reads_are_limited_to_the_resolved_activity_or_terminal_parent(
    active_id, requested_id, parent_id, parent_status, allowed
):
    assert can_read_activity_memory(
        active_activity_id=active_id,
        requested_activity_id=requested_id,
        parent_activity_id=parent_id,
        parent_status=parent_status,
    ) is allowed


def test_memory_evidence_refs_must_point_to_events_inside_the_snapshot():
    events = [_event(1, "message-1", "The amount is 127."), _event(2, "message-2", "Approved.")]
    refs = [
        {"sequence": 1, "reference_type": "message", "reference_id": "message-1"},
        {"sequence": 2, "reference_type": "message", "reference_id": "message-2"},
    ]

    assert validate_activity_memory_evidence_refs(refs, events, 2) == refs

    with pytest.raises(ValueError, match="outside the captured activity history"):
        validate_activity_memory_evidence_refs(
            [*refs, {"sequence": 3, "reference_type": "message", "reference_id": "other"}],
            events,
            2,
        )


def test_activity_memory_summary_prompt_keeps_history_as_untrusted_evidence():
    assert "untrusted" in ACTIVITY_MEMORY_SYSTEM_PROMPT.casefold()
    assert "Do not invent" in ACTIVITY_MEMORY_SYSTEM_PROMPT
