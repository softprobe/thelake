COALESCE(arg_max(session_id, timestamp) FILTER (WHERE session_id <> ''), '')
