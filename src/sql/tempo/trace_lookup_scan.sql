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
ORDER BY trace_id ASC, start_time_unix_nano ASC, span_id ASC
LIMIT {{cap}}
