## 1. Docs / OpenSpec
- [x] OpenSpec proposal/design/tasks/spec
- [x] Rewrite `docs/variant_shredding.md` as temporary MAP rollback
- [x] ADR in `docs/decision_log.md`; promotion prefer-promoted rule; positioning/perf/design-42 updates

## 2. Prefer-promoted helpers
- [x] Shared `prefer_attr_*` helpers + unit tests

## 3. Promotion manifests
- [x] Traces/logs hot-key YAMLs; extend metrics hot labels as needed
- [x] Apply hooks in grafana-up / bench
- [x] Apply → ingest coverage tests

## 4. Compilers
- [x] LLM, telemetry, Loki, Tempo prefer promoted; Prom hardened
- [x] SQL-shape tests fail if bag path leads when promotion active

## 5. MAP write path
- [x] Arrow `string_map` staging; drop `::JSON::VARIANT` bridge
- [x] `metric_series.labels` MAP; type gates accept MAP / reject VARIANT
- [x] Rewrite variant_shredding + compaction fixtures

## 6. CPU bench
- [x] Full-OTLP overlay + `make bench-demo-cpu-full`
- [x] Mean Softprobe process CPU &lt; 0.85 under Grafana 10s; artifacts under `docs/perf/results/`
  (gate profile: maintenance/self-mon off, 5% traces, coalesce flush, PromQL range cache TTL under coalesce)

## 7. Verify
- [x] `make test` green
- [x] CPU bench evidence (honest profile + wall-clock in artifacts; `20260911T035422Z-demo-cpu-full.*`, mean=0.3711 &lt; 0.85)
- [x] Build-system + senior-architect review (blocking findings cleared; remaining nits non-blocking)
