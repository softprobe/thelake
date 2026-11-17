SELECT DISTINCT strftime(timestamp, '%Y-%m-%d') AS day
FROM {{src_table}}
WHERE timestamp IS NOT NULL
ORDER BY day
