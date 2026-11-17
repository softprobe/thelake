INSERT INTO {{dst_schema}}.{{table}} ({{insert_list}})
SELECT {{select_list}}
FROM {{src_schema}}.{{table}}
ON CONFLICT DO NOTHING;
