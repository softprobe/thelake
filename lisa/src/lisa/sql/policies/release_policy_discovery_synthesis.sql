UPDATE lisa.policy_discovery_schedule
SET lease_until = NULL
WHERE singleton;
