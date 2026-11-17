CREATE TABLE {{dst_schema}}.{{table}} (LIKE {{src_schema}}.{{table}} INCLUDING ALL);
INSERT INTO {{dst_schema}}.{{table}} SELECT * FROM {{src_schema}}.{{table}};
