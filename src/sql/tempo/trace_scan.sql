WITH filter_base AS (
    SELECT
        trace_id,
        CAST(epoch_ns(timestamp) AS BIGINT) AS start_time_unix_nano,
        CAST(epoch_ns(end_timestamp) AS BIGINT) AS end_time_unix_nano{{filter_extra_columns}}
    FROM traces
    WHERE {{trace_id_prefix}}{{timestamp_filter}}{{default_observation_filter}}
),
matching_traces AS (
    SELECT DISTINCT trace_id
    FROM filter_base
    WHERE {{row_predicate}}
),
qualified_traces AS (
    SELECT trace_id
    FROM traces
    WHERE {{trace_id_prefix}}{{timestamp_filter}}{{default_observation_filter}}
      AND trace_id IN (SELECT trace_id FROM matching_traces)
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
    CAST(attributes AS JSON) AS attributes,
    CAST(resource_attributes AS JSON) AS resource_attributes,
    CAST(instrumentation_scope AS JSON) AS instrumentation_scope,
    CAST(links AS JSON) AS links,
    status_code,
    status_message,
    events,
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
