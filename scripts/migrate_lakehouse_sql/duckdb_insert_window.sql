INSERT INTO {{dst_db}}.{{table}} ({{insert_cols}})
SELECT {{select_exprs}}
FROM {{src_db}}.{{table}}
WHERE timestamp >= '{{start_ts}}'::TIMESTAMPTZ::TIMESTAMP_NS
  AND timestamp < '{{end_ts}}'::TIMESTAMPTZ::TIMESTAMP_NS;
