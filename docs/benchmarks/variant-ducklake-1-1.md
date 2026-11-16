# DuckLake 1.1 VARIANT Benchmark on PostgreSQL Catalog

Benchmark report for `softprobe/dev#194` (Phase 2 of `softprobe/dev#189`), evaluating `VARIANT` for attribute maps and `traces.events` on DuckDB 2.0.0 and DuckLake 1.1 with PostgreSQL `BYTEA` inlining (`data_inlining_row_limit = 500`) and MinIO Parquet storage.

## Pre-registered Decision Rule

- **GO** for Phase 3 (`softprobe/dev#195`) if:
  1. Empty-array `[]` and nested JSON/integer/boolean/string `VARIANT` roundtrips pass with 0 errors on both PostgreSQL inlined rows (`<= 500`) and flushed Parquet files, and
  2. Inlined-tail read latency and write latency are within `2.0x` of `MAP(VARCHAR, VARCHAR)`.
- **NO-GO** if empty-array or PostgreSQL `BYTEA` inlining fails, or if latency regresses beyond `2.0x`.

## Environment and Harness

- Build: `cargo test --release --features integration-e2e --test tests integration::variant_benchmark`
- Host: Linux x86_64, 4 CPU cores (`nproc = 4`), load average `3.09` at start
- Catalog: PostgreSQL (`ducklake-postgres`, `DATA_INLINING_ROW_LIMIT 500`, migrated to DuckLake `1.1`)
- Object store: local MinIO (`s3://warehouse/...`)
- Harness: `tests/integration/variant_benchmark.rs` (`n = 5` writes, `n = 7` reads, alternating A/B per iteration)

## 1. Empty-Array and Inlining Stability

`variant_empty_array_and_postgres_inlining_stability` passed with 0 errors:

- `events = '[]'::JSON::VARIANT` and `attributes = '[]'::JSON::VARIANT`
- Nested `{"sp.user.id":"user-1","nested":{"empty_arr":[],"count":42,"ok":true}}`
- Empty object `{}`

All rows roundtripped cleanly through `CAST(col AS JSON)` both while inlined in PostgreSQL `BYTEA` (`ducklake_inlined_data_*`) and after `CALL ducklake_flush_inlined_data(...)` to MinIO Parquet.

## 2. Performance Results (`--release`)

| Benchmark | Median | Range | Runs (`n`) | vs MAP Baseline |
|---|---:|---:|---:|---:|
| `write_450_map` (Arrow `Map` -> `MAP`) | `22.12 ms` | `17.63–25.46 ms` | 5 | `1.00x` |
| `write_450_variant_from_map` (Arrow `Map` -> `JSON::VARIANT`) | `35.97 ms` | `32.26–46.54 ms` | 5 | `1.63x` |
| `write_450_variant_from_json` (Arrow `Utf8` JSON -> `JSON::VARIANT`) | `42.55 ms` | `37.84–53.28 ms` | 5 | `1.92x` |
| `inlined_450_filter_map` (`CAST(attributes['sp.user.id'] AS VARCHAR)`) | `18.64 ms` | `17.57–30.25 ms` | 7 | `1.00x` |
| `inlined_450_filter_variant` (`CAST(attributes['sp.user.id'] AS VARCHAR)`) | `16.13 ms` | `15.69–29.03 ms` | 7 | `0.87x` (faster) |
| `inlined_450_filter_promoted` (`sp_user_id = 'user-42'`) | `13.92 ms` | `13.44–17.81 ms` | 7 | `0.75x` |
| `inlined_450_project_json_map` (`CAST(attributes AS JSON)`) | `19.71 ms` | `18.24–24.05 ms` | 7 | `1.00x` |
| `inlined_450_project_json_variant` (`CAST(attributes AS JSON)`) | `17.97 ms` | `17.56–19.04 ms` | 7 | `0.91x` (faster) |
| `parquet_4950_filter_map` (MinIO Parquet) | `17.95 ms` | `15.79–31.38 ms` | 7 | `1.00x` |
| `parquet_4950_filter_variant` (MinIO Parquet) | `25.07 ms` | `22.27–38.72 ms` | 7 | `1.40x` |
| `parquet_4950_filter_promoted` (MinIO Parquet) | `15.63 ms` | `14.28–19.51 ms` | 7 | `0.87x` |

### Limiters

- **Inlined-tail reads (`13.92–19.71 ms`):** Bound by PostgreSQL catalog roundtrip + `ducklake_inlined_data_*` table scan; `VARIANT` `BYTEA` extraction is faster than `MAP(VARCHAR, VARCHAR)` for both predicate filtering (`-13%`) and JSON projection (`-9%`).
- **Writes (`35.97–42.55 ms` vs `22.12 ms`):** Bound by single-core JSON serialization/parsing and `VARIANT` binary encoding before PostgreSQL `COPY`. Both staging paths remain under `2.0x`.
- **Flushed Parquet scans (`25.07 ms` vs `17.95 ms`):** Bound by unpromoted `VARIANT` decoding on 4,950 rows (`+7.12 ms`); promoted typed columns (`15.63 ms`) remain the fast path for hot filter keys via `src/promotion.rs`.

## Decision

**GO** for Phase 3 (`softprobe/dev#195`). Both stability and performance meet the exit criteria, and migrating to `VARIANT` removes `sp.json:` prefix encoding, `rehydrate_map_json_values`, and `ATTRIBUTE_MAP_INT64_KEYS` / `ATTRIBUTE_MAP_FLOAT64_KEYS` type-coercion workarounds.
