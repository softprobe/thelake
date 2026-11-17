INSERT INTO {{dst_table}} ({{insert_cols}})
SELECT {{select_exprs}}
FROM {{src_table}} s
WHERE s.timestamp >= '{{day_start}}'::TIMESTAMPTZ::TIMESTAMP_NS
  AND s.timestamp < ('{{day_start}}'::TIMESTAMPTZ + INTERVAL '1 day')::TIMESTAMP_NS{{dedup_clause}};
