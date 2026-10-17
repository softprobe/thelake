"""Tool-facing description of Lisa's read-only operational database."""

LISA_DATABASE_SCHEMA = """Lisa's operational database is read-only through lisa_db. Query the
approved lifecycle views and the Lisa-owned policies table listed below:

- lisa.readable_work_items: id, kind, parent_id, status, title, summary, source,
  created_at, started_at, updated_at, finished_at
- lisa.readable_evaluations: work_item_id, policy_id, target_agent_name, session_id,
  policy_name, verdict, score_id, started_at, finished_at
- lisa.readable_work_events: work_item_id, seq, event_type, title, summary,
  actor, created_at
- lisa.readable_work_context: id, work_item_id, seq, kind, source_ref,
  content_hash, created_at
- lisa.readable_approvals: id, work_item_id, scope, status, request_summary,
  requested_at, expires_at, resolved_at, resolved_by, decision_ref
- lisa.policies: id (text), scope, target_agent_name, name, criteria, approved,
  created_at, updated_at, archived_at

Use these views to find runs, inspect their current status and event timeline,
and review outcomes or pending approvals. Keep queries bounded and name columns
explicitly. Use TheLake session tools for telemetry evidence. Lisa's database
stores work tracking and outcomes, not the telemetry warehouse. Use lisa.policies
to inspect policy IDs, approval, criteria, and scope. Policy changes still go
through owner-confirmed tools; the SQL role is read-only. Other Lisa base tables
and hidden columns are not exposed.
"""
