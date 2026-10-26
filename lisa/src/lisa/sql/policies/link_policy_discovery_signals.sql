UPDATE lisa.policy_discovery_signals
SET state = 'synthesized', suggestion_id = %(suggestion_id)s
WHERE id = ANY(%(signal_ids)s::uuid[]);
