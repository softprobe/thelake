use arrow::array::{ArrayRef, Int64Builder, MapBuilder, StringBuilder, TimestampNanosecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use softprobe_runtime::ingest::IngestEngine;
use softprobe_runtime::storage::ducklake::migrate_ducklake_catalog_from_config;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

use crate::util::config::file_backed_test_config;

const SETUP_SQL: &str = include_str!("../fixtures/variant_benchmark_setup.sql");
const EMPTY_ARRAY_SQL: &str = include_str!("../fixtures/variant_benchmark_empty_array.sql");
const ROUNDTRIP_ROW_SQL: &str = include_str!("../fixtures/variant_benchmark_roundtrip_row.sql");
const FLUSH_SQL: &str = include_str!("../fixtures/variant_benchmark_flush.sql");
const INSERT_MAP_SQL: &str = include_str!("../fixtures/variant_benchmark_insert_map.sql");
const INSERT_VARIANT_JSON_SQL: &str =
    include_str!("../fixtures/variant_benchmark_insert_variant_json.sql");
const INSERT_VARIANT_MAP_SQL: &str =
    include_str!("../fixtures/variant_benchmark_insert_variant_map.sql");
const FILTER_ATTR_SQL: &str = include_str!("../fixtures/variant_benchmark_filter_attr.sql");
const FILTER_PROMOTED_SQL: &str = include_str!("../fixtures/variant_benchmark_filter_promoted.sql");
const PROJECT_JSON_SQL: &str = include_str!("../fixtures/variant_benchmark_project_json.sql");
const CLEAR_SQL: &str = include_str!("../fixtures/variant_benchmark_clear.sql");

fn write_map_parquet(path: &std::path::Path, rows: usize, id_offset: i64) {
    let key_field = Arc::new(Field::new("keys", DataType::Utf8, false));
    let val_field = Arc::new(Field::new("values", DataType::Utf8, true));
    let map_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![key_field.clone(), val_field.clone()].into()),
            false,
        )),
        false,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("attributes", map_type, true),
        Field::new("events", DataType::Utf8, true),
        Field::new("sp_user_id", DataType::Utf8, true),
    ]));

    let mut id_b = Int64Builder::with_capacity(rows);
    let mut ts_b = TimestampNanosecondBuilder::with_capacity(rows);
    let mut map_b = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    let mut evt_b = StringBuilder::with_capacity(rows, rows * 64);
    let mut usr_b = StringBuilder::with_capacity(rows, rows * 16);

    let base_ns: i64 = 1_767_225_600_000_000_000;
    for i in 0..rows {
        let row_id = id_offset + i as i64;
        let user = format!("user-{}", i % 100);
        id_b.append_value(row_id);
        ts_b.append_value(base_ns + (i as i64) * 1_000_000);
        map_b.keys().append_value("sp.user.id");
        map_b.values().append_value(&user);
        map_b.keys().append_value("gen_ai.request.model");
        map_b.values().append_value("gpt-4o");
        map_b.keys().append_value("gen_ai.usage.input_tokens");
        map_b.values().append_value(format!("{}", 100 + (i % 50)));
        map_b.keys().append_value("http.method");
        map_b.values().append_value("POST");
        map_b.append(true).expect("map append");
        if i % 3 == 0 {
            evt_b.append_value("[]");
        } else {
            evt_b.append_value(r#"[{"name":"llm.token","ts":123}]"#);
        }
        usr_b.append_value(&user);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_b.finish()) as ArrayRef,
            Arc::new(ts_b.finish()) as ArrayRef,
            Arc::new(map_b.finish()) as ArrayRef,
            Arc::new(evt_b.finish()) as ArrayRef,
            Arc::new(usr_b.finish()) as ArrayRef,
        ],
    )
    .expect("map batch");

    let file = File::create(path).expect("create map parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close parquet");
}

fn write_json_parquet(path: &std::path::Path, rows: usize, id_offset: i64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("attributes_json", DataType::Utf8, true),
        Field::new("events_json", DataType::Utf8, true),
        Field::new("sp_user_id", DataType::Utf8, true),
    ]));

    let mut id_b = Int64Builder::with_capacity(rows);
    let mut ts_b = TimestampNanosecondBuilder::with_capacity(rows);
    let mut attr_b = StringBuilder::with_capacity(rows, rows * 128);
    let mut evt_b = StringBuilder::with_capacity(rows, rows * 64);
    let mut usr_b = StringBuilder::with_capacity(rows, rows * 16);

    let base_ns: i64 = 1_767_225_600_000_000_000;
    for i in 0..rows {
        let row_id = id_offset + i as i64;
        let user = format!("user-{}", i % 100);
        id_b.append_value(row_id);
        ts_b.append_value(base_ns + (i as i64) * 1_000_000);
        attr_b.append_value(format!(
            r#"{{"sp.user.id":"{user}","gen_ai.request.model":"gpt-4o","gen_ai.usage.input_tokens":{},"http.method":"POST"}}"#,
            100 + (i % 50)
        ));
        if i % 3 == 0 {
            evt_b.append_value("[]");
        } else {
            evt_b.append_value(r#"[{"name":"llm.token","ts":123}]"#);
        }
        usr_b.append_value(&user);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_b.finish()) as ArrayRef,
            Arc::new(ts_b.finish()) as ArrayRef,
            Arc::new(attr_b.finish()) as ArrayRef,
            Arc::new(evt_b.finish()) as ArrayRef,
            Arc::new(usr_b.finish()) as ArrayRef,
        ],
    )
    .expect("json batch");

    let file = File::create(path).expect("create json parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close parquet");
}

fn median_and_range_ms(samples: &[f64]) -> (f64, f64, f64) {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = sorted[sorted.len() / 2];
    (mid, sorted[0], sorted[sorted.len() - 1])
}

#[tokio::test]
async fn variant_empty_array_and_postgres_inlining_stability() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.data_inlining_row_limit = Some(500);

    // Bootstrap catalog via IngestEngine, then migrate to 1.1.
    let _pipeline = IngestEngine::bound_default(&config)
        .await
        .expect("pipeline");
    let conn = migrate_ducklake_catalog_from_config(&config.ducklake);

    conn.execute_batch(SETUP_SQL).expect("setup tables");
    conn.execute_batch(EMPTY_ARRAY_SQL)
        .expect("insert VARIANT rows with empty arrays");

    // Verify Postgres inlined-tail reads before flushing to Parquet.
    for (id, expected_attr_sub, expected_evt) in
        [(1, "empty_arr", "[]"), (2, "[]", "evt"), (3, "{}", "[]")]
    {
        let sql = ROUNDTRIP_ROW_SQL.replace("{{ID}}", &id.to_string());
        let (attrs_json, events_json): (String, String) = conn
            .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
            .expect("read inlined VARIANT row");
        assert!(
            attrs_json.contains(expected_attr_sub),
            "inlined attrs for id={id}: {attrs_json}"
        );
        assert!(
            events_json.contains(expected_evt),
            "inlined events for id={id}: {events_json}"
        );
    }

    // Flush inlined rows to Parquet and verify flushed reads still succeed.
    let flush_sql = FLUSH_SQL
        .replace("{{ALIAS}}", &config.ducklake.catalog_alias)
        .replace("{{TABLE}}", "bench_variant");
    conn.execute_batch(&flush_sql)
        .expect("flush VARIANT rows to Parquet");

    for (id, expected_attr_sub, expected_evt) in
        [(1, "empty_arr", "[]"), (2, "[]", "evt"), (3, "{}", "[]")]
    {
        let sql = ROUNDTRIP_ROW_SQL.replace("{{ID}}", &id.to_string());
        let (attrs_json, events_json): (String, String) = conn
            .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
            .expect("read flushed VARIANT row");
        assert!(
            attrs_json.contains(expected_attr_sub),
            "flushed attrs for id={id}: {attrs_json}"
        );
        assert!(
            events_json.contains(expected_evt),
            "flushed events for id={id}: {events_json}"
        );
    }
}

#[tokio::test]
#[ignore = "explicitly run in --release to measure Phase 2 VARIANT benchmark"]
async fn variant_vs_map_postgres_benchmark() {
    let temp = TempDir::new().expect("tempdir");
    let mut config = file_backed_test_config(&temp);
    config.ducklake.data_inlining_row_limit = Some(500);

    let _pipeline = IngestEngine::bound_default(&config)
        .await
        .expect("pipeline");
    let conn = migrate_ducklake_catalog_from_config(&config.ducklake);
    conn.execute_batch(SETUP_SQL).expect("setup tables");

    let map_500 = temp.path().join("map_500.parquet");
    let json_500 = temp.path().join("json_500.parquet");
    write_map_parquet(&map_500, 450, 1);
    write_json_parquet(&json_500, 450, 1);

    // Alternating A/B write benchmark for 450 inlined rows (<= 500 limit).
    let mut map_write_ms = Vec::new();
    let mut var_json_write_ms = Vec::new();
    let mut var_map_write_ms = Vec::new();

    for iter in 0..5 {
        conn.execute_batch(&CLEAR_SQL.replace("{{TABLE}}", "bench_map"))
            .expect("clear map");
        conn.execute_batch(&CLEAR_SQL.replace("{{TABLE}}", "bench_variant"))
            .expect("clear variant");

        let t0 = Instant::now();
        conn.execute_batch(&INSERT_MAP_SQL.replace("{{PARQUET}}", &map_500.to_string_lossy()))
            .expect("insert map");
        map_write_ms.push(t0.elapsed().as_secs_f64() * 1000.0);

        let t1 = Instant::now();
        conn.execute_batch(
            &INSERT_VARIANT_JSON_SQL.replace("{{PARQUET}}", &json_500.to_string_lossy()),
        )
        .expect("insert variant from json");
        var_json_write_ms.push(t1.elapsed().as_secs_f64() * 1000.0);

        conn.execute_batch(&CLEAR_SQL.replace("{{TABLE}}", "bench_variant"))
            .expect("clear variant");
        let t2 = Instant::now();
        conn.execute_batch(
            &INSERT_VARIANT_MAP_SQL.replace("{{PARQUET}}", &map_500.to_string_lossy()),
        )
        .expect("insert variant from map");
        var_map_write_ms.push(t2.elapsed().as_secs_f64() * 1000.0);

        if iter == 4 {
            // Keep final 450 rows in both tables for inlined-tail read benchmark.
        }
    }

    let start = "2025-12-31T00:00:00Z";
    let end = "2026-01-02T00:00:00Z";

    let mut inlined_map_filter_ms = Vec::new();
    let mut inlined_var_filter_ms = Vec::new();
    let mut inlined_promoted_filter_ms = Vec::new();
    let mut inlined_map_proj_ms = Vec::new();
    let mut inlined_var_proj_ms = Vec::new();

    let sql_map_f = FILTER_ATTR_SQL
        .replace("{{TABLE}}", "bench_map")
        .replace("{{START}}", start)
        .replace("{{END}}", end);
    let sql_var_f = FILTER_ATTR_SQL
        .replace("{{TABLE}}", "bench_variant")
        .replace("{{START}}", start)
        .replace("{{END}}", end);
    let sql_prom_f = FILTER_PROMOTED_SQL
        .replace("{{TABLE}}", "bench_map")
        .replace("{{START}}", start)
        .replace("{{END}}", end);
    let sql_map_p = PROJECT_JSON_SQL
        .replace("{{TABLE}}", "bench_map")
        .replace("{{START}}", start)
        .replace("{{END}}", end);
    let sql_var_p = PROJECT_JSON_SQL
        .replace("{{TABLE}}", "bench_variant")
        .replace("{{START}}", start)
        .replace("{{END}}", end);

    for _ in 0..7 {
        let t0 = Instant::now();
        let c0: i64 = conn.query_row(&sql_map_f, [], |r| r.get(0)).expect("map f");
        inlined_map_filter_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(c0, 5);

        let t1 = Instant::now();
        let c1: i64 = conn.query_row(&sql_var_f, [], |r| r.get(0)).expect("var f");
        inlined_var_filter_ms.push(t1.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(c1, 5);

        let t2 = Instant::now();
        let c2: i64 = conn
            .query_row(&sql_prom_f, [], |r| r.get(0))
            .expect("prom f");
        inlined_promoted_filter_ms.push(t2.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(c2, 5);

        let t3 = Instant::now();
        let mut s3 = conn.prepare(&sql_map_p).expect("prep map p");
        let cnt3 = s3.query_map([], |r| r.get::<_, i64>(0)).expect("q").count();
        inlined_map_proj_ms.push(t3.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(cnt3, 100);

        let t4 = Instant::now();
        let mut s4 = conn.prepare(&sql_var_p).expect("prep var p");
        let cnt4 = s4.query_map([], |r| r.get::<_, i64>(0)).expect("q").count();
        inlined_var_proj_ms.push(t4.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(cnt4, 100);
    }

    // Flush and benchmark 4,500 Parquet rows.
    let map_4500 = temp.path().join("map_4500.parquet");
    let json_4500 = temp.path().join("json_4500.parquet");
    write_map_parquet(&map_4500, 4500, 1000);
    write_json_parquet(&json_4500, 4500, 1000);
    conn.execute_batch(&INSERT_MAP_SQL.replace("{{PARQUET}}", &map_4500.to_string_lossy()))
        .expect("insert 4500 map");
    conn.execute_batch(
        &INSERT_VARIANT_JSON_SQL.replace("{{PARQUET}}", &json_4500.to_string_lossy()),
    )
    .expect("insert 4500 variant");

    for tbl in ["bench_map", "bench_variant"] {
        conn.execute_batch(
            &FLUSH_SQL
                .replace("{{ALIAS}}", &config.ducklake.catalog_alias)
                .replace("{{TABLE}}", tbl),
        )
        .expect("flush");
    }

    let mut parquet_map_filter_ms = Vec::new();
    let mut parquet_var_filter_ms = Vec::new();
    let mut parquet_promoted_filter_ms = Vec::new();

    for _ in 0..7 {
        let t0 = Instant::now();
        let c0: i64 = conn
            .query_row(&sql_map_f, [], |r| r.get(0))
            .expect("pq map");
        parquet_map_filter_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(c0, 50);

        let t1 = Instant::now();
        let c1: i64 = conn
            .query_row(&sql_var_f, [], |r| r.get(0))
            .expect("pq var");
        parquet_var_filter_ms.push(t1.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(c1, 50);

        let t2 = Instant::now();
        let c2: i64 = conn
            .query_row(&sql_prom_f, [], |r| r.get(0))
            .expect("pq prom");
        parquet_promoted_filter_ms.push(t2.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(c2, 50);
    }

    println!("=== VARIANT vs MAP Benchmark (Postgres + MinIO DuckLake 1.1) ===");
    for (label, vals) in [
        ("write_450_map", &map_write_ms),
        ("write_450_variant_from_json", &var_json_write_ms),
        ("write_450_variant_from_map", &var_map_write_ms),
        ("inlined_450_filter_map", &inlined_map_filter_ms),
        ("inlined_450_filter_variant", &inlined_var_filter_ms),
        ("inlined_450_filter_promoted", &inlined_promoted_filter_ms),
        ("inlined_450_project_json_map", &inlined_map_proj_ms),
        ("inlined_450_project_json_variant", &inlined_var_proj_ms),
        ("parquet_4950_filter_map", &parquet_map_filter_ms),
        ("parquet_4950_filter_variant", &parquet_var_filter_ms),
        ("parquet_4950_filter_promoted", &parquet_promoted_filter_ms),
    ] {
        let (med, min, max) = median_and_range_ms(vals);
        println!(
            "{label}: median={med:.2}ms range=[{min:.2}, {max:.2}]ms n={}",
            vals.len()
        );
    }
}
