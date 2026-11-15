SELECT DISTINCT
    events,
    CAST(links AS JSON) AS links
FROM traces
WHERE {{timestamp_filter}}
  AND (
    (events IS NOT NULL AND events NOT IN ('', '[]', 'null'))
    OR (links IS NOT NULL AND cardinality(links) > 0)
  )
LIMIT {{limit}}
