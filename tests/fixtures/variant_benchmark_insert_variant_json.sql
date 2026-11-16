INSERT INTO bench_variant SELECT id, ts, CAST(attributes_json AS JSON)::VARIANT, CAST(events_json AS JSON)::VARIANT, sp_user_id FROM read_parquet('{{PARQUET}}');
