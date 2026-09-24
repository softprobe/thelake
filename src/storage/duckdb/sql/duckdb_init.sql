-- Single DuckDB connection init script (query / writer / maintenance / tests).
--
-- Rendered by `crate::storage::duckdb::init` — do not INSTALL/LOAD/SET these inline
-- in Rust. Placeholders:
--   threads / memory_limit (mustache-style double-brace vars)
--   enable_query_tuning section (optional query-path SETs)
--   cache_directory section (optional cache_httpfs; also substitutes the path var)

INSTALL httpfs;
LOAD httpfs;
INSTALL ducklake;
LOAD ducklake;
INSTALL postgres;
LOAD postgres;

-- DuckLake extension conflict-retry defaults (official concurrent-write mechanism).
SET ducklake_max_retry_count = 10;
SET ducklake_retry_backoff = 1.5;
SET ducklake_retry_wait_ms = 100;

-- Resource caps (also applied at Connection::open when possible).
SET threads = {{threads}};
SET memory_limit = '{{memory_limit}}';

-- Read-after-write visibility: never serve a guessed/stale DuckLake snapshot.
SET unsafe_enable_version_guessing = false;

{{#enable_query_tuning}}
-- Query-path caches (required when the query section is enabled).
SET enable_object_cache = true;
SET enable_external_file_cache = true;
SET enable_http_metadata_cache = true;
SET parquet_metadata_cache = true;
SET experimental_metadata_reuse = true;
{{/enable_query_tuning}}

{{#cache_directory}}
-- On-disk HTTP/S3 cache (community extension). Directory is templated.
INSTALL cache_httpfs FROM community;
LOAD cache_httpfs;
SET cache_httpfs_cache_block_size = 8388608;
SET cache_httpfs_disk_cache_reader_enable_memory_cache = 0;
SET cache_httpfs_type = 'on_disk';
SET cache_httpfs_min_disk_bytes_for_cache = 10737418240;
SET cache_httpfs_evict_policy = 'lru_sp';
SET cache_httpfs_enable_glob_cache = true;
SET cache_httpfs_cache_directory = '{{cache_directory}}';
{{/cache_directory}}
