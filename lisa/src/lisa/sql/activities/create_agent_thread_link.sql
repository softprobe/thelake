INSERT INTO lisa.activity_monitor_agent_threads(agent_id, activity_id)
SELECT %(agent_id)s::text, %(activity_id)s::uuid WHERE %(agent_id)s::text IS NOT NULL
ON CONFLICT DO NOTHING;
