# Design: temporary MAP bags + prefer-promoted SQL

## Bag type

Restore `MAP(VARCHAR, VARCHAR)` for hot attribute bags. Proven Postgres-inline
path (scores metadata), matches pre-#11 physical type, keeps OTel string-map
model and existing `CAST(col['k'] AS VARCHAR)` / `CAST(col AS JSON)` query forms.

Out of scope (already MAP): `scores.metadata`, nested `traces.events[].attributes`.

## Inlining

Catalog-global `DATA_INLINING_ROW_LIMIT` stays **0**:

- AC-F7 requires zero inlined bytes for `metric_samples` / hist / postings so
  TWCS merge sees Parquet.
- Limit is not per-table; re-enabling 10_000 would inline skinny metrics too.

MAP *could* inline; we choose not to until per-table inlining or #42 lands.
Primary win for this change is removing VARIANT cast/shredding CPU.

## Prefer-promoted rule

Shared helpers emit `COALESCE(promoted_col, CAST(bag['key'] AS VARCHAR), …)`
with promoted first when a matching active promotion exists. Bag-only when no
promotion. Compilers: LLM query, telemetry, Prom labels, Loki, Tempo.

## Promotion shipping

Versioned YAMLs under `docs/promotion/`; tenant `POST /v1/promotions/apply` only.
Demo/bench scripts apply all hot manifests at bring-up (same pattern as
`metrics-prom-hot-labels.yaml`).

## Full-OTLP CPU gate

Profile sends traces+logs+metrics to Softprobe at demo load-generator rates.
Grafana keeps `refresh=10s` during measure. Softprobe pinned to one core.
Host samples `/proc/<pid>/stat`; mean CPU ratio must be &lt; 0.85 (durable
headroom under one-core pin). External
sampling (not self-mon export) so the gate does not compete for the core.

## Migration

Fail-fast if hot columns are still VARIANT. Operator rebuilds catalog/data path
(same class of break as MAP→VARIANT). No auto-drop.
