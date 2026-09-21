# Softprobe Runtime — performance docs

| Doc / area | Purpose |
|------------|---------|
| [`results/`](results/) | Captured demo CPU / ingest wall-clock artifacts |
| `make test-perf` | Manual / release performance suites (latency, concurrency, stability) |
| `make bench-demo-cpu-full` | Full OTEL demo + Grafana refresh CPU gate |

Product metrics / Prometheus query benchmarks have been removed along with the
Prometheus product surface. Prefer Loki/Tempo and evidence-SQL workloads for
compatibility performance work.

## Captured results

JSON/Markdown under [`results/`](results/) named
`<stamp>-<label>.{json,md}`. See [`results/README.md`](results/README.md).
