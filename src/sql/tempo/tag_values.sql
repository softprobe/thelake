SELECT DISTINCT CAST(({{tag_expr}}) AS VARCHAR) AS value
FROM traces
WHERE {{timestamp_filter}}
  AND ({{tag_expr}}) IS NOT NULL
ORDER BY value ASC
LIMIT {{limit}}
