SELECT {{projection}}
FROM traces
WHERE {{identity_prefix}}{{timestamp_filter}}{{cursor_suffix}}
ORDER BY timestamp DESC, span_id DESC
LIMIT {{fetch}}
