WITH qualified_traces AS (
    SELECT trace_id
    FROM traces
    WHERE {{trace_id_prefix}}{{timestamp_filter}}{{default_observation_filter}}
    GROUP BY trace_id
    HAVING {{duration_predicate}}
    ORDER BY MIN(CAST(epoch_ns(timestamp) AS BIGINT)) DESC, trace_id DESC
    LIMIT {{trace_limit}}
)
SELECT
    trace_id,
    span_id,
    parent_span_id,
    message_type,
    span_kind,
    app_id,
    agent_id,
    agent_name,
    session_id,
    CAST(epoch_ns(timestamp) AS BIGINT) AS start_time_unix_nano,
    CAST(epoch_ns(end_timestamp) AS BIGINT) AS end_time_unix_nano,
    CASE
        WHEN attributes IS NULL THEN NULL
        WHEN TRY_CAST(attributes AS MAP(VARCHAR, VARCHAR)) IS NOT NULL
            THEN CAST(MAP_FROM_ENTRIES(list_filter(map_entries(TRY_CAST(attributes AS MAP(VARCHAR, VARCHAR))), e -> length(e.value) <= 4096)) AS JSON)
        ELSE CAST(attributes AS JSON)
    END AS attributes,
    CASE
        WHEN resource_attributes IS NULL THEN NULL
        WHEN TRY_CAST(resource_attributes AS MAP(VARCHAR, VARCHAR)) IS NOT NULL
            THEN CAST(MAP_FROM_ENTRIES(list_filter(map_entries(TRY_CAST(resource_attributes AS MAP(VARCHAR, VARCHAR))), e -> length(e.value) <= 4096)) AS JSON)
        ELSE CAST(resource_attributes AS JSON)
    END AS resource_attributes,
    status_code,
    status_message,
    observation_type,
    model_name,
    model_provider,
    user_id,
    session_attr_id,
    service_name
FROM traces
WHERE {{trace_id_prefix}}{{timestamp_filter}}{{default_observation_filter}}
  AND trace_id IN (SELECT trace_id FROM qualified_traces)
ORDER BY trace_id ASC, start_time_unix_nano ASC, span_id ASC
LIMIT {{cap}}
