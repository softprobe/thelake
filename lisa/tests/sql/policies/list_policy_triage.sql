SELECT session_id
FROM lisa.evaluation_triage
WHERE policy_id = %(policy_id)s;
