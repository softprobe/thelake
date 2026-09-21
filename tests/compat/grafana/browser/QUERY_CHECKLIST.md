# Query Features & Aggregations Verification Checklist

See canonical reference at: `docs/compat/query_features_checklist.md`

This checklist covers Loki LogQL and Tempo protocol checks exercised by the
automated browser test suite in this directory
(`tests/compat/grafana/browser/`). Customer metrics / Prometheus / PromQL are
out of scope.

### Test Suites in this Directory:
- `query_features.ts`: Loki LogQL inventory matching the checklist.
- `e2e_ingestion.spec.ts`: Softprobe readiness plus live Loki/Tempo smoke.
- `e2e_grafana_dashboards.spec.ts`: Loki / Tempo / cross-signal smoke dashboards.
- `e2e_grafana_explore.spec.ts`: Grafana Explore UI for Loki queries.
- `e2e_all_query_features.spec.ts`: Loki LogQL via Grafana proxy + Tempo protocol endpoints.
