SELECT
  CAST(epoch_ns(timestamp) AS BIGINT) AS timestamp_ns,
  body,
  CAST(attributes AS JSON) AS attributes,
  CAST(resource_attributes AS JSON) AS resource_attributes,
  {{promoted}}
FROM logs
WHERE 1=1{{timestamp_filter}}
ORDER BY timestamp ASC
LIMIT {{limit}}
