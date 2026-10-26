INSERT INTO lisa.policy_discovery_signals
    (id, session_id, activity_id, agent_name, summary, span_ids, evidence)
VALUES (%(id)s, %(session_id)s, %(activity_id)s, %(agent_name)s, %(summary)s,
        %(span_ids)s::jsonb, %(evidence)s::jsonb)
ON CONFLICT (session_id) DO NOTHING
RETURNING id;
