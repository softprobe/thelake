# Softprobe Runtime Goals

**Status:** Current

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
   - Use the PostgreSQL DuckLake catalog in every runtime environment.
   - Keep non-inlined data in Parquet under a configurable local or
     object-store data path.

3. **Workspace isolation**
   - Bind `workspace_id` before ingest, query, session, or promotion work.
   - Give each provisioned workspace a DuckLake metadata schema and data path
     (or a shared physical scope with row-level `workspace_id`).

4. **SQL accessibility**
   - Query with DuckDB through the attached DuckLake catalog.
   - Support telemetry APIs for common evidence searches.
   - Provide workspace-scoped connection material for local DuckDB clients
     (isolated mode).

5. **Operational simplicity**
   - Let the OpenTelemetry collector batch upstream.
   - Default flush-through: commit each OTLP request to DuckLake before ack
     (`ingest.flush_interval_seconds: 0`; soft coalesce is optional).
   - Rely on DuckLake for conflict retries, snapshots, data inlining, and file
     management.
   - Run DuckLake-native compaction and retention maintenance.

6. **Schema evolution without parallel storage paths**
   - Keep canonical trace and log schemas in one shared module.
   - Add workspace-scoped nullable columns through promotion manifests
     (`POST /v1/promotions/apply`).
   - Keep `sp.*` as an explicit instrumentation convention; promote only the
     fields a workspace declares.

7. **Query-only observability compatibility**
   - Keep OTLP as the canonical write path for traces and logs.
   - Expose Loki- and Tempo-compatible **query** APIs so existing Grafana
     datasources can read lake evidence without a second write pipeline. See
     [compat/matrix.md](../compat/matrix.md).
   - Product signals are traces and logs. Process self-monitoring metrics are
     exported through OTLP and are not stored as customer telemetry.

8. **Process self-monitoring (operators)**
   - When `self_monitoring.enabled` is true, thelake records process Meter
     instruments and exports them via the standard OTLP metrics exporter
     (`OTEL_EXPORTER_OTLP_*` / `OTEL_EXPORTER_OTLP_METRICS_*`).
   - Process metrics are **not** written into customer or ops DuckLake.

## Non-goals

- Customer OTLP metrics ingest, `metric_*` product tables as a live path,
  Prometheus HTTP API, or PromQL.
- A second durable table format.
- Maintaining a staged Parquet tier or application WAL. Optional soft coalesce
  (`ingest.flush_interval_seconds` > 0; default 0 = flush-through) is allowed;
  it is not a durable buffer.
- Hiding failed commits behind an application retry/fallback path.
- Accepting arbitrary workspace identifiers in already workspace-bound
  operational APIs.

## References

- [Current architecture](overview.md)
- [Workspace identity](workspace-identity.md)
- [Instrumentation guide](../how-to/instrumentation.md)
- [Schema promotion](../how-to/promotion.md)
- [Ad hoc DuckDB/DuckLake queries](../how-to/adhoc-duckdb.md)
- [Compatibility matrix (Loki/Tempo)](../compat/matrix.md)
- [Performance documentation](../perf/README.md)
