# Configuration reference

Canonical example: [`config.yaml`](../../config.yaml).
Validation and defaults: `src/config.rs` (`deny_unknown_fields` — unknown or
legacy keys are rejected).

Object-store credentials come from the environment, never from YAML.
`ducklake.metadata_path` is a PostgreSQL connection string and may include a
catalog password.

## Precedence

1. Supported environment overrides (below)
2. `CONFIG_FILE` (default `config.yaml`)
3. Built-in defaults when a section or the file is omitted

## Top-level sections

| Section | Role | Default highlights |
|---------|------|--------------------|
| `server` | HTTP bind port / body size / worker hint | `port: 8090`, `max_body_size: 104857600` |
| `object_store` | Non-secret S3/GCS settings | `region`; optional `endpoint` (MinIO/R2) |
| `query` | DuckDB query pool | `max_connections: 10`, `sql_gate: false` |
| `maintenance` | DuckLake merge / expire / clean | `enabled: true`, `interval_seconds: 60` |
| `async_jobs` | Cross-replica leases | `lease_ttl_seconds: 120`, `heartbeat_seconds: 30` |
| `ingest` | Soft coalesce | `flush_interval_seconds: 0` (flush-through) |
| `session_summary` | Dirty reduce / rebuild | always on for Postgres catalog |
| `self_monitoring` | Process OTLP metrics export | `enabled: false` |
| `ducklake` | Catalog + data path (**required**) | Postgres `metadata_path`, `data_path`, … |

### `ducklake` (required)

| Key | Meaning |
|-----|---------|
| `metadata_path` | PostgreSQL connection string (DuckLake catalog; always Postgres) |
| `data_path` | Local, `s3://`, or `gs://` warehouse path |
| `catalog_alias` | ATTACH alias (example: `softprobe`) |
| `metadata_schema` | Catalog schema name |
| `extension_path` | DuckLake extension file |
| `data_inlining_row_limit` | Default `500`; `0` only for Parquet-per-batch fixtures |
| `writer_pool_size` | Default `4`, clamped to `1..=16` |

### `query`

| Key | Default | Meaning |
|-----|---------|---------|
| `max_connections` | `10` | Query-worker DuckDB pool size |
| `cache_dir` | (optional) | Local cache directory for httpfs |
| `sql_gate` | `false` | When true, query workers run an EXPLAIN gate on fact scans |

### `ingest`

| Key | Default | Meaning |
|-----|---------|---------|
| `flush_interval_seconds` | `0` | `0` = drain each batch immediately; `>0` = soft coalesce window |
| `buffer_size_mb` | `256` | Soft in-memory coalesce budget (MiB; absolute ceiling 256) |
| `write_timeout_seconds` | `60` | Per-write wall clock; `0` disables (clamped to 3600) |

Soft coalesce acknowledges OTLP before DuckLake commit. Unflushed rows can be
lost on crash or post-ack write failure. See [architecture overview](../architecture/overview.md).

### `maintenance`

| Key | Default | Meaning |
|-----|---------|---------|
| `enabled` | `true` | Run physical-scope maintenance |
| `interval_seconds` | `60` | Wake interval per physical scope |
| `metadata_enabled` | `true` | Snapshot expire + scheduled file cleanup |
| `reader_safety_grace_seconds` | `300` | Protect readers of recently expired snapshots |

### `async_jobs` / `session_summary`

See [async jobs](../how-to/async-jobs.md) and
[session summaries](../how-to/session-summaries.md). Heartbeat must be strictly
less than lease TTL.

### `self_monitoring`

When `enabled: true`, process Meter instruments export via the standard OTLP
metrics exporter (`OTEL_EXPORTER_OTLP_*`). Metrics are **not** written to
customer DuckLake.

## Object-store credentials (environment)

| Paths | Variables |
|-------|-----------|
| `s3://` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN` |
| `gs://` | `GCS_HMAC_ACCESS_KEY_ID` / `GCS_HMAC_SECRET` (or `GCP_HMAC_*`) |

## Direct environment overrides

| Variable | Effect |
|----------|--------|
| `CONFIG_FILE` | Path to YAML |
| `PORT` | Overrides `server.port` |
| `S3_REGION` | Overrides `object_store.region` |
| `SOFTPROBE_MAX_HTTP_BODY_BYTES` | Overrides `server.max_body_size` |

## Deployment / runtime environment

| Variable | Effect |
|----------|--------|
| `SOFTPROBE_LISTEN_ADDR` | Full listen address (preferred over `server.host`, which is not used for bind) |
| `OTEL_GRPC_PORT` | OTLP/gRPC traces listen port (default `4317`) |
| `SOFTPROBE_GRPC_DISABLE` | `1` disables gRPC listener |
| `SOFTPROBE_AUTH_URL` | External assertion auth base URL |
| `SOFTPROBE_ADMIN_API_KEY` | Bearer for `POST /v1/workspaces` |
| `SOFTPROBE_LOCAL_ANONYMOUS` | Local single-workspace mode (see root README) |
| `THELAKE_DEFAULT_WORKSPACE_ID` | Workspace UUID used with local anonymous mode |
