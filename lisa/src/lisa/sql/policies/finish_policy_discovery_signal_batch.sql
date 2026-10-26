UPDATE lisa.policy_discovery_signals
SET state = 'reviewed'
WHERE state = 'pending' AND id = ANY(%(signal_ids)s::uuid[]);
