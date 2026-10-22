---
name: lisa-activities
description: Create, update, query, and inspect Lisa activity feed entries and monitor threads through Lisa's MCP tools.
---

# Lisa activities

Use Lisa activity tools to create and inspect user-visible work records.

## Lifecycle

- For each distinct request in owner web or Slack chat, use
  `resolve_chat_activity` once per intent before task-specific work. Continue
  only a clearly matching open activity after reading its thread; create a new
  one for new work, task switches, uncertain continuation, or closed work.
  Continuations retain the same activity across web and Slack chats. If a
  request names an open policy or work item found by `find_activity`, resolve
  with `disposition="continue"` and that activity ID, even when the request
  asks for the next action such as approval.
  Independent requests in one message need separate request keys. Include
  brief factual requests; use `skip_chat_activity_for_turn` only for greetings
  and filler. The runtime links chat messages and scopes tool events for you.
- Never resolve the same request twice in one turn. Keep the exact activity ID
  returned by the first resolver call and keep its request key stable. If a
  later tool fails or returns an unexpected activity ID, inspect and record the
  failure on the first resolved activity instead of creating a replacement.
- Use the repository's `create_activity` operation for non-chat system work
  such as evaluations and workers. It is not an exposed chat MCP tool.
- Use `update_activity` to revise its presentation metadata and optionally
  append an event to the visible timeline. Supply event type and event title
  together.
- Do not represent a lifecycle outcome by changing card text. Worker lifecycle
  operations own queued, running, waiting, and terminal status transitions.
- Use `set_activity_section` to attach or update the thread's section metadata.
- Use `link_activity_reference` to associate an agent, message, runtime-work
  item, or carrier message with the thread.

## Query

- Use `list_recent_activities` for the recent feed. Its limit is 1–100 and it
  supports `is_goal` and `activity_type` filters.
- Use `get_activity_thread` for one activity's thread, ordered actions,
  sections, and linked references.
- Use `lisa_db` for read-only analysis of `readable_activity_feed`,
  `readable_activity_monitor_threads`, `readable_activity_monitor_thread_actions`,
  `readable_activity_monitor_links`, `readable_activities`, and the evaluation
  views. Name columns explicitly.

Lisa's generic `activities` and `activity_events` are canonical. Feed and
monitor tables are projections; Lisa's evaluation identity and lease lifecycle
remain authoritative.
