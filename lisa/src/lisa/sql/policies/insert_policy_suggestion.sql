INSERT INTO lisa.policy_suggestions
    (id, candidate_hash, name, criteria, scope, target_agent_name, rationale,
     signal_ids, evidence, overlap_details)
VALUES (%(id)s, %(candidate_hash)s, %(name)s, %(criteria)s, %(scope)s,
        %(target_agent_name)s, %(rationale)s, %(signal_ids)s::jsonb,
        %(evidence)s::jsonb, %(overlap_details)s::jsonb)
ON CONFLICT (candidate_hash) DO NOTHING
RETURNING id;
