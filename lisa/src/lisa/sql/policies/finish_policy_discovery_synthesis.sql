UPDATE lisa.policy_discovery_schedule
SET last_synthesis_at = now(), lease_until = NULL
WHERE singleton AND lease_until IS NOT NULL;
