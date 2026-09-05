# Query Features & Aggregations Verification Checklist

See canonical reference at: `docs/compat/query_features_checklist.md`

This checklist specifies all query features, functions, and aggregations tested and verified by the automated browser test suite in this directory (`tests/compat/grafana/browser/`).

### Test Suites in this Directory:
- `query_features.ts`: Definitive executable inventory of queries matching every checklist item.
- `e2e_ingestion.spec.ts`: Ingestion validation (OTel Demo stream freshness, rate continuity).
- `e2e_grafana_dashboards.spec.ts`: Real Grafana settings & browser automation verifying all 20+ provisioned dashboards render without errors.
- `e2e_grafana_explore.spec.ts`: Browser automation executing queries across all functions and aggregations in Grafana Explore UI (`/explore`).
- `e2e_all_query_features.spec.ts`: End-to-end verification of all functions, aggregations, operators, and metadata APIs via Grafana proxy and query engine.
