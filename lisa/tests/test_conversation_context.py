from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from lisa.conversation_context import (
    CHECKPOINT_SYSTEM_PROMPT,
    CheckpointSummaryEvent,
    ConversationCheckpointCondenser,
    configured_context_window,
    context_configuration_fingerprint,
    event_prefix_fingerprint,
    prepare_checkpoint_request,
)
from lisa.repositories.conversation_context import ConversationContextRepository
from openhands.sdk.context.view import View
from openhands.sdk.event.llm_convertible.action import ActionEvent
from openhands.sdk.event.llm_convertible.message import MessageEvent
from openhands.sdk.event.llm_convertible.observation import ObservationEvent
from openhands.sdk.llm import Message, TextContent
from openhands.sdk.llm.message import MessageToolCall, ThinkingBlock
from openhands.sdk.mcp.definition import MCPToolObservation


def test_checkpoint_summary_prompt_preserves_facts_without_future_directives():
    prompt = " ".join(CHECKPOINT_SYSTEM_PROMPT.split()).lower()
    assert "do not phrase the summary as instructions to future lisa" in prompt
    assert "record prior instructions as historical facts" in prompt


def message(event_id: str, role: str, text: str) -> MessageEvent:
    return MessageEvent(
        id=event_id,
        source="user" if role == "user" else "agent",
        llm_message=Message(role=role, content=[TextContent(text=text)]),
    )


def test_explicit_context_window_caps_model_window_for_controlled_compaction(monkeypatch):
    class Model:
        effective_max_input_tokens = 1_000_000

    monkeypatch.setenv("LISA_CONTEXT_WINDOW_TOKENS", "12000")

    assert configured_context_window(Model()) == 12_000


def artifact(events: list[MessageEvent], prefix_count: int, config: str) -> dict:
    prefix = events[:prefix_count]
    return {
        "summary": "The user asked Lisa to inspect a policy and retain the pending update.",
        "prefix_event_ids": [event.id for event in prefix],
        "prefix_fingerprint": event_prefix_fingerprint(prefix),
        "config_fingerprint": config,
        "captured_at": datetime.now(UTC).isoformat(),
    }


def test_checkpoint_summary_is_hidden_from_transcript_and_precedes_intact_tail(tmp_path: Path):
    events = [
        message("u1", "user", "Create a policy."),
        message("a1", "assistant", "I drafted policy P1.1."),
        message("u2", "user", "Leave it pending."),
        message("a2", "assistant", "It remains pending approval."),
    ]
    view = View(events=events)
    repository = ConversationContextRepository(tmp_path)
    config = context_configuration_fingerprint("gemini/gemini-3.8-flash", 20_000)
    repository.publish_checkpoint("checkpoint-1", artifact(events, 2, config))
    original_ids = [event.id for event in view.events]

    condensed = ConversationCheckpointCondenser(
        directory=str(tmp_path),
        model_name="gemini/gemini-3.8-flash",
        retained_tail_tokens=20_000,
    ).condense(view)

    assert isinstance(condensed, View)
    assert isinstance(condensed.events[0], CheckpointSummaryEvent)
    assert condensed.events[0].to_llm_message().role == "user"
    assert "untrusted reference summary" in condensed.events[0].summary
    assert [event.id for event in condensed.events[1:]] == ["u2", "a2"]
    assert [event.id for event in view.events] == original_ids


def test_checkpoint_is_rejected_after_prefix_rewrite_or_model_change(tmp_path: Path):
    old_events = [message("u1", "user", "Original request."), message("a1", "assistant", "Done.")]
    repository = ConversationContextRepository(tmp_path)
    config = context_configuration_fingerprint("model-a", 20_000)
    repository.publish_checkpoint("checkpoint-1", artifact(old_events, 1, config))

    rewritten = View(events=[message("u1", "user", "Edited request."), old_events[1]])
    changed_model = ConversationCheckpointCondenser(
        directory=str(tmp_path), model_name="model-b", retained_tail_tokens=20_000
    ).condense(View(events=old_events))
    changed_history = ConversationCheckpointCondenser(
        directory=str(tmp_path), model_name="model-a", retained_tail_tokens=20_000
    ).condense(rewritten)

    assert changed_model.events == old_events
    assert changed_history.events == rewritten.events


def test_checkpoint_fingerprint_tracks_prompt_provider_and_model_window():
    baseline = context_configuration_fingerprint(
        "model-a",
        20_000,
        system_prompt="Lisa prompt v1",
        runtime_configuration={"provider": "gemini", "context_window": 128_000},
    )

    assert baseline != context_configuration_fingerprint(
        "model-a",
        20_000,
        system_prompt="Lisa prompt v2",
        runtime_configuration={"provider": "gemini", "context_window": 128_000},
    )
    assert baseline != context_configuration_fingerprint(
        "model-a",
        20_000,
        system_prompt="Lisa prompt v1",
        runtime_configuration={"provider": "openai", "context_window": 128_000},
    )


def test_checkpoint_publication_is_idempotent_and_requires_matching_fingerprint(tmp_path: Path):
    events = [message("u1", "user", "Persist this work.")]
    config = context_configuration_fingerprint("model-a", 20_000)
    checkpoint = artifact(events, 1, config)
    repository = ConversationContextRepository(tmp_path)

    first = repository.publish_checkpoint("checkpoint-1", checkpoint)
    second = repository.publish_checkpoint("checkpoint-1", {**checkpoint, "summary": "replacement"})

    assert first == second
    assert second["summary"] == checkpoint["summary"]
    assert repository.usable_checkpoint(["u1"], config, event_prefix_fingerprint(events)) == first
    assert repository.usable_checkpoint(["rewritten"], config, "different") is None


def test_checkpoint_redelivery_repairs_pointer_after_publish_crash(tmp_path: Path):
    events = [message("u1", "user", "Persist this work.")]
    config = context_configuration_fingerprint("model-a", 20_000)
    checkpoint = artifact(events, 1, config)
    repository = ConversationContextRepository(tmp_path)
    request_id = "c" * 64
    artifact_path = tmp_path / "context" / "checkpoints" / f"{request_id}.json"
    repository._write_json(artifact_path, {**checkpoint, "request_id": request_id})

    repository.publish_checkpoint(request_id, checkpoint)

    assert repository.current_checkpoint(config)["request_id"] == request_id


def test_checkpoint_input_is_published_once_for_an_immutable_request(tmp_path: Path):
    repository = ConversationContextRepository(tmp_path)
    request_id = "a" * 64

    repository.write_checkpoint_input(request_id, {"messages": [{"content": "first"}]})
    repository.write_checkpoint_input(request_id, {"messages": [{"content": "second"}]})

    assert repository.read_checkpoint_input(request_id) == {"messages": [{"content": "first"}]}


def test_checkpoint_adoption_resets_loaded_namespaces(tmp_path: Path):
    events = [
        message("u1", "user", "Earlier request."),
        message("a1", "assistant", "Completed earlier work."),
        message("u2", "user", "Continue with the next step."),
    ]
    repository = ConversationContextRepository(tmp_path)
    config = context_configuration_fingerprint("model-a", 20_000)
    repository.publish_checkpoint("checkpoint-1", artifact(events, 2, config))
    repository.set_loaded_namespaces({"activities", "policies"})

    condensed = ConversationCheckpointCondenser(
        directory=str(tmp_path), model_name="model-a", retained_tail_tokens=20_000
    ).condense(View(events=events))

    assert isinstance(condensed.events[0], CheckpointSummaryEvent)
    assert repository.loaded_namespaces() == set()


def test_pending_turn_resets_tool_namespaces_before_mcp_server_starts(tmp_path: Path):
    events = [
        message("u1", "user", "Earlier request."),
        message("a1", "assistant", "Completed earlier work."),
        message("u2", "user", "Continue with the next step."),
    ]
    repository = ConversationContextRepository(tmp_path)
    config = context_configuration_fingerprint("model-a", 20_000)
    repository.publish_checkpoint("checkpoint-1", artifact(events, 2, config))
    repository.set_loaded_namespaces({"activities", "policies"})
    condenser = ConversationCheckpointCondenser(
        directory=str(tmp_path), model_name="model-a", retained_tail_tokens=20_000
    )

    assert condenser.prepare_for_turn(events)
    assert repository.loaded_namespaces() == set()


def test_checkpoint_tail_uses_safe_tool_exchange_boundary():
    from lisa.conversation_context import _tail_start

    call = MessageToolCall(id="call-1", name="lookup", arguments="{}", origin="completion")
    action = ActionEvent(
        id="action-1",
        thought=[],
        tool_name="lookup",
        tool_call_id="call-1",
        tool_call=call,
        llm_response_id="response-1",
    )
    observation = ObservationEvent(
        id="observation-1",
        tool_name="lookup",
        tool_call_id="call-1",
        action_id="action-1",
        observation=MCPToolObservation(
            tool_name="lookup", content=[TextContent(text="tool result")]
        ),
    )
    events = [
        message("u1", "user", "Old context."),
        action,
        observation,
        message("u2", "user", "Current request."),
    ]

    class CountingModel:
        def get_token_count(self, messages):
            return len(messages)

    view = View(events=events)
    assert view.manipulation_indices == {0, 1, 3, 4}
    start = _tail_start(events, view, CountingModel(), tail_tokens=2)

    assert start == 3
    assert start in view.manipulation_indices
    assert [event.id for event in events[start:]] == ["u2"]


def test_checkpoint_tail_keeps_parallel_tool_batch_and_results_together():
    from lisa.conversation_context import _tail_start

    first_call = MessageToolCall(id="call-1", name="lookup", arguments="{}", origin="completion")
    second_call = MessageToolCall(id="call-2", name="forecast", arguments="{}", origin="completion")
    first_action = ActionEvent(
        id="action-1",
        thought=[],
        tool_name="lookup",
        tool_call_id="call-1",
        tool_call=first_call,
        llm_response_id="response-1",
    )
    second_action = ActionEvent(
        id="action-2",
        thought=[],
        tool_name="forecast",
        tool_call_id="call-2",
        tool_call=second_call,
        llm_response_id="response-1",
    )
    first_observation = ObservationEvent(
        id="observation-1",
        tool_name="lookup",
        tool_call_id="call-1",
        action_id="action-1",
        observation=MCPToolObservation(tool_name="lookup", content=[TextContent(text="found")]),
    )
    second_observation = ObservationEvent(
        id="observation-2",
        tool_name="forecast",
        tool_call_id="call-2",
        action_id="action-2",
        observation=MCPToolObservation(tool_name="forecast", content=[TextContent(text="sunny")]),
    )
    events = [
        message("u1", "user", "Earlier context."),
        first_action,
        second_action,
        first_observation,
        second_observation,
        message("u2", "user", "Current request."),
    ]

    class CountingModel:
        def get_token_count(self, messages):
            return len(messages)

    view = View(events=events)
    assert view.manipulation_indices == {0, 1, 5, 6}
    start = _tail_start(events, view, CountingModel(), tail_tokens=1)

    assert start == 5
    assert start in view.manipulation_indices
    assert [event.id for event in events[:start]] == [
        "u1",
        "action-1",
        "action-2",
        "observation-1",
        "observation-2",
    ]
    assert [event.id for event in events[start:]] == ["u2"]


def test_checkpoint_tail_keeps_thought_enabled_tool_loop_together():
    from lisa.conversation_context import _tail_start

    first_call = MessageToolCall(id="call-1", name="lookup", arguments="{}", origin="completion")
    second_call = MessageToolCall(id="call-2", name="forecast", arguments="{}", origin="completion")
    first_action = ActionEvent(
        id="action-1",
        thought=[],
        thinking_blocks=[ThinkingBlock(thinking="Check the current state first.")],
        tool_name="lookup",
        tool_call_id="call-1",
        tool_call=first_call,
        llm_response_id="response-1",
    )
    second_action = ActionEvent(
        id="action-2",
        thought=[],
        tool_name="forecast",
        tool_call_id="call-2",
        tool_call=second_call,
        llm_response_id="response-2",
    )
    first_observation = ObservationEvent(
        id="observation-1",
        tool_name="lookup",
        tool_call_id="call-1",
        action_id="action-1",
        observation=MCPToolObservation(tool_name="lookup", content=[TextContent(text="found")]),
    )
    second_observation = ObservationEvent(
        id="observation-2",
        tool_name="forecast",
        tool_call_id="call-2",
        action_id="action-2",
        observation=MCPToolObservation(tool_name="forecast", content=[TextContent(text="sunny")]),
    )
    events = [
        message("u1", "user", "Earlier context."),
        first_action,
        first_observation,
        second_action,
        second_observation,
        message("u2", "user", "Current request."),
    ]

    class CountingModel:
        def get_token_count(self, messages):
            return len(messages)

    view = View(events=events)
    assert view.manipulation_indices == {0, 1, 5, 6}
    start = _tail_start(events, view, CountingModel(), tail_tokens=1)

    assert start == 5
    assert start in view.manipulation_indices
    assert [event.id for event in events[:start]] == [
        "u1",
        "action-1",
        "observation-1",
        "action-2",
        "observation-2",
    ]
    assert [event.id for event in events[start:]] == ["u2"]


def test_checkpoint_request_triggers_at_configured_threshold_and_saves_snapshot(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setenv("LISA_CONTEXT_CHECKPOINT_TRIGGER_RATIO", "0.5")
    monkeypatch.setenv("LISA_CONTEXT_RETAINED_TAIL_TOKENS", "1")
    events = [
        message(f"e{index}", "user" if index % 2 == 0 else "assistant", f"Request {index}.")
        for index in range(4)
    ]

    class CountingModel:
        model = "model-a"
        effective_max_input_tokens = 8

        def get_token_count(self, messages):
            return len(messages)

    repository = ConversationContextRepository(tmp_path)
    request = prepare_checkpoint_request(View(events=events), CountingModel(), repository)

    assert request is not None
    request_id, payload = request
    assert payload["prefix_event_ids"] == ["e0", "e1", "e2"]
    assert payload["captured_at"]
    assert repository.read_checkpoint_input(request_id) == payload


def test_checkpoint_request_waits_below_configured_threshold(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("LISA_CONTEXT_CHECKPOINT_TRIGGER_RATIO", "0.5")
    monkeypatch.setenv("LISA_CONTEXT_RETAINED_TAIL_TOKENS", "1")

    class CountingModel:
        model = "model-a"
        effective_max_input_tokens = 8

        def get_token_count(self, messages):
            return len(messages)

    request = prepare_checkpoint_request(
        View(events=[message("e0", "user", "Only a short conversation.")]),
        CountingModel(),
        ConversationContextRepository(tmp_path),
    )

    assert request is None


def test_checkpoint_trigger_reserves_room_for_system_prompt_and_tools(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("LISA_CONTEXT_CHECKPOINT_TRIGGER_RATIO", "0.5")
    monkeypatch.setenv("LISA_CONTEXT_RETAINED_TAIL_TOKENS", "1")

    class CountingModel:
        model = "model-a"
        effective_max_input_tokens = 100

        def get_token_count(self, messages):
            return len(messages)

    events = [
        message(f"e{index}", "user" if index % 2 == 0 else "assistant", "Work")
        for index in range(45)
    ]

    request = prepare_checkpoint_request(
        View(events=events), CountingModel(), ConversationContextRepository(tmp_path)
    )

    assert request is not None


def test_emergency_checkpoint_summarizes_before_context_limit(tmp_path: Path):
    events = [
        message(f"e{index}", "user" if index % 2 == 0 else "assistant", f"Request {index}.")
        for index in range(10)
    ]

    class CountingModel:
        model = "model-a"
        effective_max_input_tokens = 10

        def get_token_count(self, messages):
            return len(messages)

        def completion(self, _messages, *, max_tokens):
            assert max_tokens == 4096
            return SimpleNamespace(
                message=SimpleNamespace(content=[TextContent(text="Keep the open request.")])
            )

    repository = ConversationContextRepository(tmp_path)
    repository.set_loaded_namespaces({"activities"})
    condensed = ConversationCheckpointCondenser(
        directory=str(tmp_path), model_name="model-a", retained_tail_tokens=1
    ).condense(View(events=events), agent_llm=CountingModel())

    assert isinstance(condensed.events[0], CheckpointSummaryEvent)
    assert len(condensed.events) == 2
    assert repository.loaded_namespaces() == set()
    current = repository.current_checkpoint(context_configuration_fingerprint("model-a", 1))
    assert current["summary"] == "Keep the open request."
    assert list((tmp_path / "context" / "checkpoint-inputs").glob("*.json")) == []


def test_todo_write_replaces_snapshot_and_removes_finished_items(tmp_path: Path):
    repository = ConversationContextRepository(tmp_path)
    repository.write_todo(
        [
            {"id": "t1", "text": "Find the current policy", "status": "open"},
            {"id": "t2", "text": "Draft an update", "status": "completed"},
            {"id": "t3", "text": "Ask owner", "status": "cancelled"},
        ]
    )

    assert repository.read_todo() == [
        {"id": "t1", "text": "Find the current policy", "status": "open"}
    ]
    repository.write_todo([])
    assert repository.read_todo() == []


def test_loaded_tool_namespaces_are_persistent_and_can_be_reset(tmp_path: Path):
    repository = ConversationContextRepository(tmp_path)

    repository.set_loaded_namespaces({"policies", "activities"})

    assert repository.loaded_namespaces() == {"activities", "policies"}
    repository.clear_loaded_namespaces()
    assert repository.loaded_namespaces() == set()
