# Compatibility matrix

OpenTelemetry is the canonical write path for traces and logs. Loki and Tempo
provide tenant-scoped, query-only APIs over the same DuckLake data. Grafana
uses native Loki and Tempo data sources.

| Product | Supported surface | Contract |
|---|---|---|
| Loki | Query, range query, labels, label values, series, index stats, live tail | [Loki API subset](loki.md) |
| Tempo | Trace lookup, search, tags, tag values | [Tempo API subset](tempo.md) |
| Grafana | Loki and Tempo data sources, Explore, dashboards | [Grafana integration](../../tests/compat/grafana/README.md) |

Requests use authenticated workspace context. A tenant or workspace identifier
in a query parameter cannot select another workspace. See
[authentication and isolation](auth.md).

Supported attribute conversion is defined in
[projection policies](projections.md). Route and query limits are documented
with each protocol contract.

## Validation

- `make test-compat` — protocol conformance corpus.
- `make test-grafana-static` — configuration and compose contracts.
- `make test-loki-diff` / `make test-tempo-diff` — comparison against pinned
  upstream services.
- `make test-grafana-system` — Grafana system workflow.

Pinned images and fixture attribution are listed in
[reference provenance](references-provenance.md).
