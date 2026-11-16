SELECT DISTINCT
    CAST(events AS JSON) AS events,
    CAST(links AS JSON) AS links
FROM traces
WHERE {{timestamp_filter}}
  AND (
    (events IS NOT NULL AND json_array_length(CAST(events AS JSON)) > 0)
    OR (links IS NOT NULL AND json_array_length(CAST(links AS JSON)) > 0)
  )
LIMIT {{limit}}
