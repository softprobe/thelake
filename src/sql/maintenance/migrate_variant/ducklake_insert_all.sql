INSERT INTO {{dst_table}} ({{insert_cols}})
SELECT {{select_exprs}}
FROM {{src_table}};
