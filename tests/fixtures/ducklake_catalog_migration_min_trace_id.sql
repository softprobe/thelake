SELECT MIN(trace_id) AS m FROM traces WHERE timestamp >= '{{START}}' AND timestamp <= '{{END}}'
