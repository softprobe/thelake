"""Tool-facing description of Lisa's read-only operational database."""

from lisa.repositories.lisa_database import MAX_ROWS, POLICY_COLUMNS

LISA_DATABASE_SCHEMA = f"""Use lisa_db for explicit read-only database diagnostics
or to retrieve Lisa's recorded evaluation history. For activity work, use Lisa's activity tools;
for policy work, use read_policy, list_policies, write_policy, approve_policy,
and delete_policy.
Do not query lisa_db to find, create, update, or inspect activities. For a
historical triage lookup by policy name, join lisa.policies on policy_id and
filter by its name.

When explicit database diagnostics are requested, these approved relations and
columns are available:

- lisa.readable_activities: id, kind, parent_id, status, title, summary, source,
  created_at, started_at, updated_at, finished_at
- lisa.readable_evaluations: activity_id, policy_id, target_agent_name, session_id,
  policy_name, evaluation_id, verdict, score_id, started_at, finished_at. These
  rows are Lisa's actual judge runs and link to an activity by activity_id.
- lisa.readable_evaluation_triage: session_id, policy_id, decision, created_at.
  Join to lisa.policies when the user specifies a policy name.
- lisa.readable_activity_events: activity_id, seq, event_type, title, summary,
  actor, reference_type, reference_id, created_at
- lisa.readable_activity_feed: activity_id, activity_type, is_goal, message_id,
  subtitle, status_title, task_label, agent_id, details, kind, status, title,
  summary, source, created_at, started_at, updated_at, finished_at, next_action,
  next_action_at, origin_conversation_id, origin_slack_channel_id,
  origin_slack_thread_ts, slack_channel_id, slack_thread_ts
- lisa.readable_activity_monitor_threads: activity_id, icon, details, section_id,
  section_title, section_status, section_order
- lisa.readable_activity_monitor_thread_actions: activity_id, seq, event_type,
  title, summary, actor, reference_type, reference_id, status, created_at
- lisa.readable_activity_monitor_links: activity_id, reference_type,
  reference_id. Types are agent, message, runtime_event, runtime_work, and
  carrier_message.
- lisa.readable_activity_context: id, activity_id, seq, kind, source_ref,
  content_hash, created_at
- lisa.policies: {", ".join(POLICY_COLUMNS)}. Policy deletion removes the row;
  there is no archived_at column.

Lisa's activity tables store activity history and outcomes, not workspace
session telemetry. Keep diagnostic queries bounded and name columns explicitly.
Queries return at most {MAX_ROWS} rows and include a ``truncated`` flag; report
when a result is truncated instead of saying it contains every row.
Use Lisa's ``search_sessions`` and ``get_session`` tools when the user asks for
telemetry evidence. Policy
changes still go through owner-confirmed tools; the SQL role is read-only. Other
Lisa base tables and hidden columns are not exposed.
"""
