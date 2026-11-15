WITH span_attr_keys AS (
    SELECT
        UNNEST(map_keys(attributes)) AS key,
        (service_name IS NULL OR attributes['service_name'] IS DISTINCT FROM service_name) AS keep_service_name
    FROM traces
    WHERE {{timestamp_filter}}
),
resource_attr_keys AS (
    SELECT UNNEST(map_keys(resource_attributes)) AS key
    FROM traces
    WHERE {{timestamp_filter}}
),
service_name_key AS (
    SELECT 'service.name' AS key
    FROM traces
    WHERE {{timestamp_filter}} AND (service_name IS NOT NULL OR app_id IS NOT NULL)
    LIMIT 1
)
SELECT DISTINCT key FROM (
    SELECT key
    FROM span_attr_keys
    WHERE key != 'service_name' OR keep_service_name
    UNION ALL
    SELECT key FROM resource_attr_keys
    UNION ALL
    SELECT key FROM service_name_key
) AS tag_keys
WHERE key IS NOT NULL AND key != ''
ORDER BY key ASC
LIMIT {{limit}}
