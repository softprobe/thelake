UPDATE lisa.policy_discovery_samples
SET gap_reviewed = true
WHERE session_id = %(session_id)s;
