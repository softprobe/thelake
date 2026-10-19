-- case: reject_update
UPDATE lisa.activities SET status = 'succeeded';

-- case: reject_delete
DELETE FROM lisa.activities;

-- case: reject_catalog
SELECT * FROM pg_catalog.pg_tables;

-- case: reject_sleep
SELECT pg_sleep(5) FROM lisa.readable_activities;

-- case: reject_sequence
SELECT nextval('sequence') FROM lisa.readable_activities;

-- case: reject_cte
WITH items AS (SELECT id FROM lisa.readable_activities) SELECT * FROM items;

-- case: reject_column
SELECT secret FROM lisa.readable_activities;

-- case: reject_multiple
SELECT id FROM lisa.readable_activities; SELECT id FROM lisa.readable_activities;

-- case: reject_qualified_function
SELECT other.lower(title) FROM lisa.readable_activities;

-- case: reject_quoted_qualified_function
SELECT other."lower"(title) FROM lisa.readable_activities;

-- case: reject_fully_quoted_qualified_function
SELECT "other"."lower"(title) FROM lisa.readable_activities;

-- case: allowed_bounded
SELECT id, title, status FROM lisa.readable_activities
WHERE status = 'running' ORDER BY created_at DESC LIMIT 1000;

-- case: allowed_large_limit
SELECT id FROM lisa.readable_activities LIMIT 10000;

-- case: reject_context_wildcard
SELECT * FROM lisa.readable_activity_context;

-- case: reject_qualified_context_wildcard
SELECT c.* FROM lisa.readable_activity_context AS c;

-- case: readable_activity
SELECT activities.status, evaluations.verdict FROM lisa.readable_activities AS activities
JOIN lisa.readable_evaluations AS evaluations
  ON activities.id = evaluations.activity_id
WHERE activities.id = 'ACTIVITY_ID';

-- case: reject_base_table
SELECT * FROM lisa.activities;
