INSERT INTO bench_map SELECT id, ts, attributes, events, sp_user_id FROM read_parquet('{{PARQUET}}');
