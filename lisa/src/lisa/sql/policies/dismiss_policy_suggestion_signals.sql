UPDATE lisa.policy_discovery_signals
SET state = 'dismissed'
WHERE id = ANY(%(signal_ids)s::uuid[]);
