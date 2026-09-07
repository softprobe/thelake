# Design: self-monitoring ops lake (Design 2)

## Catalog model
Same Postgres/SQLite catalog DSN as customers. Ops isolation = reserved `metadata_schema` + dedicated `data_path` (e.g. `s3://warehouse/_thelake_ops/`). `build_tenant_storage` already overrides only schema+path.

## Engine bind
Constant `OPS_TENANT_ID = "thelake-ops"`. `RuntimeEngineManager::build_engine` reserved branch builds scope from `self_monitoring.ops_*` config (no customer registry resolve required for bind; startup may still ensure registry row for maintenance). Compat/Prom use auth tenant id → `engine_for`.

## Write path
SDK metric snapshot → converter → `DuckLakeWriter::write_metric_batches`. No forked schema writer.

## SelfHeal
`DuckDBCore.counts_toward_liveness = false` for ops engines. `rebuild_worker_state` skips `SELF_HEAL` fetch_add when false.

## Startup
Best-effort bootstrap after listen bind (spawn); never abort process; never postpone customer HTTP.

## Auth map shape
Env `THELAKE_AUTH_STUB_KEY_TENANTS` = comma-separated `apiKey:tenantId` pairs (default `local-tenant-key:softprobe-local`). Production Softprobe auth must resolve keys to distinct tenants the same way when ops Grafana is enabled.
