UPDATE lisa.policy_discovery_schedule
SET lease_until = now() + interval '10 minutes'
WHERE singleton
  AND (lease_until IS NULL OR lease_until < now())
  AND (last_synthesis_at IS NULL OR last_synthesis_at < date_trunc('day', now()))
RETURNING singleton;
