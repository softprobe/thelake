SELECT DISTINCT key FROM (
    SELECT key
    FROM (
        SELECT UNNEST(map_keys(attributes)) AS key, attributes, service_name
        FROM traces
        WHERE {{timestamp_filter}}
    ) AS attr_keys
    WHERE key != 'service_name'
       OR service_name IS NULL
       OR attributes['service_name'] IS DISTINCT FROM service_name
    UNION ALL
    SELECT UNNEST(map_keys(resource_attributes)) AS key
    FROM traces
    WHERE {{timestamp_filter}}
    UNION ALL
    SELECT 'service.name' AS key
    FROM traces
    WHERE {{timestamp_filter}} AND (service_name IS NOT NULL OR app_id IS NOT NULL)
) AS tag_keys
WHERE key IS NOT NULL AND key != ''
ORDER BY key ASC
LIMIT {{limit}}
