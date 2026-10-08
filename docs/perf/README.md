# Softprobe Runtime — performance docs

| Doc / area | Purpose |
|------------|---------|
| `make test-perf` | Manual / release performance suites (`PERF_SUITE=all|latency|concurrency`) |
| `make bench-demo-cpu-full` | Full OTEL demo + Grafana refresh CPU gate |

Compatibility performance work uses Loki and Tempo query workloads together
with the evidence SQL paths.

## Benchmark ownership

The full-system benchmark writes JSON and Markdown artifacts to `target/perf/`.
Set `BENCH_RESULTS_DIR` to choose another output directory. The repository
keeps benchmark definitions and commands; generated run output stays outside
the documentation tree.
