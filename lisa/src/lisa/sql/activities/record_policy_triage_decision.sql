INSERT INTO lisa.evaluation_triage(session_id, policy_id, decision)
VALUES (%(session_id)s, %(policy_id)s, %(decision)s)
ON CONFLICT (session_id, policy_id) DO NOTHING;
