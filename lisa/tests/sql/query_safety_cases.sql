-- case: reject_update
UPDATE lisa.work_items SET status = 'succeeded';

-- case: reject_delete
DELETE FROM lisa.work_items;

-- case: reject_catalog
SELECT * FROM pg_catalog.pg_tables;

-- case: reject_sleep
SELECT pg_sleep(5) FROM lisa.readable_work_items;

-- case: reject_sequence
SELECT nextval('sequence') FROM lisa.readable_work_items;

-- case: reject_cte
WITH items AS (SELECT id FROM lisa.readable_work_items) SELECT * FROM items;

-- case: reject_column
SELECT secret FROM lisa.readable_work_items;

-- case: reject_multiple
SELECT id FROM lisa.readable_work_items; SELECT id FROM lisa.readable_work_items;

-- case: reject_qualified_function
SELECT other.lower(title) FROM lisa.readable_work_items;

-- case: reject_quoted_qualified_function
SELECT other."lower"(title) FROM lisa.readable_work_items;

-- case: reject_fully_quoted_qualified_function
SELECT "other"."lower"(title) FROM lisa.readable_work_items;

-- case: allowed_bounded
SELECT id, title, status FROM lisa.readable_work_items
WHERE status = 'running' ORDER BY created_at DESC LIMIT 1000;

-- case: allowed_large_limit
SELECT id FROM lisa.readable_work_items LIMIT 10000;

-- case: reject_context_wildcard
SELECT * FROM lisa.readable_work_context;

-- case: reject_qualified_context_wildcard
SELECT c.* FROM lisa.readable_work_context AS c;

-- case: readable_work_item
SELECT status, verdict FROM lisa.readable_work_items
JOIN lisa.readable_evaluations ON id = work_item_id
WHERE id = 'WORK_ITEM_ID';

-- case: reject_base_table
SELECT * FROM lisa.work_items;
