# Softprobe Runtime Goals

**Status:** Current
**Last updated:** 2026-09-21

## Product goal

Preserve production AI traces and application recordings as durable,
customer-controlled evidence that can be reused across investigation,
evaluation, regression, governance, and continuous-improvement workflows.
Keep the original business context directly queryable with SQL so engineers
and AI agents can build those workflows on open data rather than short-lived
operational telemetry.

## Current technical goals

1. **Complete capture**
   - Accept OTLP **traces and logs** (product signals).
   - Preserve HTTP bodies and business attributes needed for investigation.
   - Avoid sampling in this storage service.

2. **Simple durable storage**
   - Use one DuckLake write and query path.
   - Use PostgreSQL metadata for production and SQLite metadata for local
     development.
   - Keep non-inlined data in Parquet under a configurable local or
     object-store data path.

3. **Tenant isolation**
   - Bind tenant identity before ingest, query, session, or promotion work.
   - Give each provisioned tenant a DuckLake metadata schema and data path.

4. **SQL accessibility**
   - Query with DuckDB through the attached DuckLake catalog.
   - Support telemetry APIs for common evidence searches.
   - Provide tenant-scoped connection material for local DuckDB clients.

5. **Operational simplicity**
   - Let the OpenTelemetry collector batch upstream.
   - Commit each request directly to DuckLake.
   - Rely on DuckLake for conflict retries, snapshots, data inlining, and file
     management.
   - Run DuckLake-native compaction and retention maintenance.

6. **Schema evolution without parallel storage paths**
   - Keep canonical trace and log schemas in one shared module.
   - Add tenant-scoped nullable columns through promotion manifests
     (`POST /v1/promotions/apply`).
   - Keep `sp.*` as an explicit instrumentation convention; promote only the
     fields a tenant declares.

7. **Query-only observability compatibility**
   - Keep OTLP as the canonical write path for traces and logs.
   - Expose Loki- and Tempo-compatible **query** APIs so existing Grafana
     datasources can read lake evidence without a second write pipeline. See
     [compat/matrix.md](compat/matrix.md).
   - Product metrics / Prometheus / PromQL are **out of scope** (removed).

8. **Process self-monitoring (operators)**
   - When `self_monitoring.enabled` is true, thelake records process Meter
     instruments and exports them via the standard OTLP metrics exporter
     (`OTEL_EXPORTER_OTLP_*` / `OTEL_EXPORTER_OTLP_METRICS_*`).
   - Process metrics are **not** written into customer or ops DuckLake.

## Non-goals

- Customer OTLP metrics ingest, `metric_*` product tables as a live path,
  Prometheus HTTP API, or PromQL.
- Reintroducing Apache Iceberg or a second durable table format.
- Maintaining a staged Parquet tier or application WAL. Optional soft coalesce
  (`ingest.flush_interval_seconds` > 0; default 0 = flush-through) is allowed;
  it is not a durable buffer.
- Hiding failed commits behind an application retry/fallback path.
- Accepting arbitrary tenant identifiers in already tenant-bound operational
  APIs.

## References

- [Current architecture](design.md)
- [Current architecture decisions](decision_log.md)
- [Instrumentation guide](instrumentation_guide.md)
- [Schema promotion](promotion.md)
- [Ad hoc DuckDB/DuckLake queries](adhoc-duckdb-ducklake.md)
- [Compatibility matrix (Loki/Tempo)](compat/matrix.md)
- [Legacy documentation](legacy/README.md)
