---
name: lisa-activities
description: Create, update, query, and inspect Lisa activity feed entries and monitor threads through Lisa's MCP tools.
---

# Lisa activities

Use Lisa activity tools to create and inspect user-visible work records.

## Lifecycle

- Use `create_activity` for a new Lisa action. Supply a meaningful title,
  activity type, source, and optional subtitle, task label, goal flag, agent ID,
  or structured details.
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
