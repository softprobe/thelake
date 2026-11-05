SELECT count(*)::bigint
FROM pg_prepared_statements
WHERE position($1 in statement) > 0
