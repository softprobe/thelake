"""Tool-facing description of Lisa's read-only operational database."""

LISA_DATABASE_SCHEMA = """Lisa's operational database is read-only through lisa_db. Query the
approved lifecycle views and the Lisa-owned policies table listed below:

- lisa.readable_activities: id, kind, parent_id, status, title, summary, source,
  created_at, started_at, updated_at, finished_at
- lisa.readable_evaluations: activity_id, policy_id, target_agent_name, session_id,
  policy_name, evaluation_id, verdict, score_id, started_at, finished_at. These
  rows are Lisa's actual judge runs and link to an activity by activity_id.
- lisa.readable_activity_events: activity_id, seq, event_type, title, summary,
  actor, created_at
- lisa.readable_activity_context: id, activity_id, seq, kind, source_ref,
  content_hash, created_at
- lisa.readable_approvals: id, activity_id, scope, status, request_summary,
  requested_at, expires_at, resolved_at, resolved_by, decision_ref
- lisa.policies: id (text), scope, target_agent_name, name, criteria, approved,
  created_at, updated_at, archived_at

Use these views to find activities and inspect their status, event timeline,
context, and any evaluation runs linked to the activity. Activity events record
Lisa's general actions; the evaluation views contain judge runs, while the
triage view contains Lisa's durable session-policy routing cache. Keep queries
bounded and name columns explicitly. Use TheLake session tools for telemetry evidence. Lisa's database
stores Lisa activity history and outcomes, not the telemetry warehouse. Use lisa.policies
to inspect policy IDs, approval, criteria, and scope. Policy changes still go
through owner-confirmed tools; the SQL role is read-only. Other Lisa base tables
and hidden columns are not exposed.
"""
