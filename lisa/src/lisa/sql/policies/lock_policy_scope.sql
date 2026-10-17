SELECT pg_advisory_xact_lock(hashtextextended(%(lock_key)s, 0));
