# Tasks: remove-metrics-signal

## 1. OpenSpec
- [x] 1.1 Scaffold proposal/design/tasks/spec deltas
- [x] 1.2 Validate with `openspec validate remove-metrics-signal --strict`

## 2. Shared helpers
- [x] 2.1 Extract `sanitize_label_name` for Loki before deleting Prom projection

## 3. Delete product metrics
- [x] 3.1 Delete ingest, models, layout write, sql/prom, Prom/PromQL backends, metrics compaction, OTLP metrics protos
- [x] 3.2 Strip shared glue (`TelemetryTable`, `TableFamily`, routes, `add_metrics`, writer/query branches)
- [x] 3.3 Drop `promql-parser`; keep OTel SDK metrics features for process instruments

## 4. Retarget self-monitoring
- [x] 4.1 Wire OTLP metrics exporter; delete DuckLake push/convert/ops bootstrap
- [x] 4.2 Remove ops Prom dashboard and ops-only config knobs

## 5. Tests / CI / Makefile
- [x] 5.1 Delete metrics/Prom integration and compat tests
- [x] 5.2 Trim Makefile Prom targets and compatibility.yml Prometheus jobs
- [x] 5.3 Edit manifests, fixtures, one_clock scripts; add thin OTLP bootstrap check

## 6. Docs
- [x] 6.1 Delete metrics/Prom primary docs; rewrite goals/design/README/OpenAPI; decision_log

## 7. Verify
- [x] 7.1 `make test` green (lib + integration); grep audit clean of product metrics routes/modules; Loki/Tempo gates run separately
