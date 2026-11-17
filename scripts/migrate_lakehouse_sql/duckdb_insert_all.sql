INSERT INTO {{dst_db}}.{{table}} ({{insert_cols}})
SELECT {{select_exprs}}
FROM {{src_db}}.{{table}};
