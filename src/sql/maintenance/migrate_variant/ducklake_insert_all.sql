INSERT INTO dst_lake.{{table}} ({{insert_cols}})
SELECT {{select_exprs}}
FROM src_lake.{{table}};
