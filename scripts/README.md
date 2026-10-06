# Scripts

`make` is the public command interface. Scripts implement Make targets or
provide explicit operator utilities.

## Make helpers

| Area | Scripts | Make targets |
|---|---|---|
| Build | `download-ducklake-extension.sh`, `assert-duckdb-version.sh` | `build-release`, `test`, `test-e2e`, `test-perf` |
| Test orchestration | `run-e2e-matrix.sh`, `run-isolated-cargo-tests.sh` | `test-e2e`, `test-perf` |
| Compatibility | `compat/*.sh`, `compat/run-with-timeout` | `test-compat`, `test-grafana-static`, `test-grafana-system`, `test-loki-diff`, `test-tempo-diff` |
| Grafana | `grafana-system-smoke.sh`, `grafana-manual-up.sh`, `grafana-manual-down.sh`, `test-grafana-browser.sh` | `test-grafana-system`, `grafana-up`, `grafana-down`, `test-grafana-browser` |
| Performance | `bench-demo-cpu-full.sh`, `bench-llm-seeded.sh`, `perf/*.py` | `bench-demo-cpu-full`, `bench-llm-seeded`, `test-perf-helpers` |
| Local operations | `stress-test.sh`, `seed-lake-from-parquet.sh`, `interactive_query*.sh`, `duckdb_ducklake_*`, `demo_session_queries.sh`, `drop_all_tables.sh`, `generate_telemetry.py`, `telemetrygen_hosted.sh` | `stress`, `seed-lake`, `duckdb-shell*`, `demo-session`, `drop-tables`, `generate-telemetry`, `telemetrygen` |

Shared shell helpers live under `lib/`. Compatibility validation and image
pinning helpers live under `compat/`; performance utilities live under
`perf/`.

## Operator utilities

These scripts are run directly and do not have Make targets:

- `check_session_details.py` — inspect session details from a configured
  DuckLake.
- `copy_traces_workspace_uuid.py` — copy traces into a workspace-scoped lake;
  see its usage and environment-variable documentation in the script header.
- `reingest_traces_otlp.py` — replay stored traces through the OTLP ingest API;
  see its usage and environment-variable documentation in the script header.

```bash
python3 scripts/copy_traces_workspace_uuid.py --help
python3 scripts/reingest_traces_otlp.py --help
```

## Common targets

```text
Build:     build | build-release | package | publish
Checks:    test | test-e2e | test-perf | test-compat | ci | release
Local:     setup | teardown | doctor | duckdb-shell | duckdb-shell-prod
Ops:       stress BACKEND=local|r2|gcs | seed-lake | demo-session
Grafana:   grafana-up | grafana-down | test-grafana-system | test-grafana-browser
```

Cargo registry and build caches live under `~/.cache/thelake` by default and
can be moved with `THELAKE_CACHE_ROOT`.
