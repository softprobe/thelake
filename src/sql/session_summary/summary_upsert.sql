INSERT INTO {{schema_quoted}}.session_summary ({{insert_columns}})
SELECT {{select_columns}}
FROM UNNEST({{array_placeholders}}) AS input ({{input_columns}})
ON CONFLICT ({{conflict_keys}}) DO UPDATE SET
{{set_list}}
