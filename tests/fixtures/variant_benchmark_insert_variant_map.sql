INSERT INTO bench_variant SELECT id, ts, CAST(CAST(attributes AS JSON) AS VARIANT), CAST(events AS JSON)::VARIANT, sp_user_id FROM read_parquet('{{PARQUET}}');
