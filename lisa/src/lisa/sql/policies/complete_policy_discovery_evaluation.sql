UPDATE lisa.policy_discovery_samples
SET evaluation_complete = true, gap_reviewed = %(gap_reviewed)s
WHERE session_id = %(session_id)s;
