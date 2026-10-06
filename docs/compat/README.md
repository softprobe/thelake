# Loki and Tempo compatibility

OpenTelemetry is the write path for traces and logs. Loki and Tempo expose
tenant-scoped, query-only APIs over the same DuckLake data. Grafana is the
supported dashboard client for these APIs.

- [Compatibility matrix](matrix.md) — supported routes, parameters, and limits.
- [Loki API](loki.md)
- [Tempo API](tempo.md)
- [Authentication and isolation](auth.md)
- [Attribute projections](projections.md)
- [Queryability guarantees](queryability.md)
- [Conformance corpus](conformance.md)
- [Reference images and fixture provenance](references-provenance.md)
- [Capability manifest](capability.v0.yaml)
- [Image pins](references.v0.yaml)
- [Query feature checklist](query_features_checklist.md)

Run local checks with `make test-compat` and `make test-grafana-static`.
Service-backed checks are available through `make test-loki-diff`,
`make test-tempo-diff`, and `make test-grafana-system`.
