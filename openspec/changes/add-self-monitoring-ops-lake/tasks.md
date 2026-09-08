## 1. Config + constants
- [x] Add `SelfMonitoringConfig` (`enabled`, `export_interval_seconds`, `ops_metadata_schema`, `ops_data_path`)
- [x] Add `OPS_TENANT_ID` constant

## 2. Ops engine + SelfHeal
- [x] Reserved branch in `build_engine` / `engine_for`
- [x] `counts_toward_liveness` on DuckDB query core; ops = false
- [x] Reject reserved id on `POST /v1/tenants` (all paths)
- [x] Best-effort bootstrap (non-blocking listen)

## 3. OTel export
- [x] Meter API instruments + periodic export → `write_metric_batches`
- [x] Export-drop counter + `/health` `exportDrops` field

## 4. Auth + Grafana
- [x] Auth stub key→tenant map
- [x] Ops Prom datasource + dashboard (subset PromQL)

## 5. Verify
- [x] Unit tests: config, convert, reserved id, health exportDrops
- [x] `cargo test --lib` green (491)
- [ ] Workspace `make build && make e2e` / compose PromQL smoke (gate before merge)
