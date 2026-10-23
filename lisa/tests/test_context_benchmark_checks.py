from benchmark.activity_runner import (
    _activity_namespace_reloaded_before_use,
    _checkpoint_cut_is_safe,
    _checkpoint_namespaces_cleared,
    _slack_threads_have_distinct_conversations,
    _transcript_excludes_checkpoint_summaries,
)
from openhands.sdk.context.view import View
from openhands.sdk.event.llm_convertible.action import ActionEvent
from openhands.sdk.event.llm_convertible.message import MessageEvent
from openhands.sdk.event.llm_convertible.observation import ObservationEvent
from openhands.sdk.llm import Message, TextContent
from openhands.sdk.llm.message import MessageToolCall
from openhands.sdk.mcp.definition import MCPToolObservation


def test_checkpoint_cut_check_requires_complete_event_prefix_and_safe_boundary():
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
        observation=MCPToolObservation(tool_name="lookup", content=[TextContent(text="found")]),
    )
    current = MessageEvent(
        id="u2",
        source="user",
        llm_message=Message(role="user", content=[TextContent(text="Continue.")]),
    )
    view = View(events=[action, observation, current])

    assert _checkpoint_cut_is_safe(view, ["action-1", "observation-1"])
    assert not _checkpoint_cut_is_safe(view, ["action-1"])
    assert not _checkpoint_cut_is_safe(view, ["rewritten", "observation-1"])


def test_transcript_check_rejects_checkpoint_summary_leaking_into_visible_messages():
    messages = [
        {"role": "user", "text": "Continue the Orchid task."},
        {"role": "assistant", "text": "Checkpoint: The Orchid task is pending."},
    ]

    assert not _transcript_excludes_checkpoint_summaries(messages, ["The Orchid task is pending."])


def test_namespace_check_requires_activities_to_load_before_activity_tool():
    turn = {
        "tool_calls": [
            {"name": "load_tool_namespace", "arguments": {"namespace": "activities"}},
            {"name": "find_activity", "arguments": {"query": "Orchid"}},
        ]
    }

    assert _activity_namespace_reloaded_before_use(turn)
    assert _activity_namespace_reloaded_before_use(
        {
            "tool_calls": [
                {
                    "name": "load_tool_namespace",
                    "arguments": '{"namespace":"activities"}',
                },
                {"name": "find_activity", "arguments": '{"query":"Orchid"}'},
            ]
        }
    )
    assert not _activity_namespace_reloaded_before_use(
        {
            "tool_calls": [
                {"name": "load_tool_namespace", "arguments": {"namespace": "policies"}},
                {"name": "find_activity", "arguments": {"query": "Orchid"}},
            ]
        }
    )


def test_checkpoint_adoption_check_requires_namespace_reset():
    assert _checkpoint_namespaces_cleared([{"loaded_namespaces": []}])
    assert not _checkpoint_namespaces_cleared([{"loaded_namespaces": ["activities"]}])


def test_slack_threads_have_distinct_conversation_ids():
    assert _slack_threads_have_distinct_conversations(
        {"slack:C1:T1": "conversation-1", "slack:C1:T2": "conversation-2"}
    )
    assert not _slack_threads_have_distinct_conversations(
        {"slack:C1:T1": "conversation-1", "slack:C1:T2": "conversation-1"}
    )
    assert not _activity_namespace_reloaded_before_use(
        {
            "tool_calls": [
                {"name": "find_activity", "arguments": {"query": "Orchid"}},
                {"name": "load_tool_namespace", "arguments": {"namespace": "activities"}},
            ]
        }
    )
