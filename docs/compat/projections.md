# OTel attribute projection policies

**Status:** Phase 0 contract  
**Last updated:** 2026-09-21

Projection is implemented in shared `compat::projection` code, not in HTTP
handlers. Handlers call typed backends that already apply these policies.

## Shared rules

1. **Resource vs datapoint/span attributes:** datapoint/span attributes win on
   key collision when projecting to a flat label/tag map.
2. **Non-string OTel values:** scalars use stable string forms (`true`/`false`,
   decimal integers/floats). Arrays and kvlists are **preserved** in storage as
   nested JSON inside attribute bags. In the in-memory attribute map they are
   tagged with the `sp.json:` prefix so plain OTLP StringValues that look like
   JSON are not rehydrated. Bytes are stored as standard base64. Protocol
   label/tag projection uses the stored string/JSON text form (no silent drop).
3. **Empty keys:** dropped. Nested empty/unencodeable AnyValue children become
   JSON `null` (not omitted from arrays/objects).
4. **Cardinality:** projected label/tag sets are subject to
   `limits.max_labels_per_series` (see capability manifest). Excess keys are
   dropped in lexicographic key order after reserved keys are kept; adapters
   must not silently invent values.
5. **Promoted columns:** tenant promotion adds SQL columns; projection still
   reads canonical attribute maps unless a phase explicitly maps a promoted
   column into a protocol label.

## Loki labels and structured metadata

| Source | Projection |
|--------|------------|
| Low-cardinality resource attrs (`service.name`, `deployment.environment`, …) | Stream labels (allowlist in capability / projection code) |
| Remaining attributes | Structured metadata (not stream labels) |
| Log body | Log line |

High-cardinality keys must not become stream labels. Default allowlist is
conservative; tenants may promote columns for SQL without expanding Loki
stream cardinality.

## Tempo tags

| Source | Projection |
|--------|------------|
| Span and resource attributes | Search tags |
| Intrinsic fields | `traceID`, `spanID`, `name`, `status`, duration derived from timestamps |

Span **links** and **instrumentation scope** name/version are stored and
returned on trace lookup; arbitrary instrumentation-scope fields remain
explicit TraceQL unsupported features.

## Explicit non-goals

- Caller-supplied tenant ids as labels
- Trusting Grafana datasource UIDs as tenancy
- Expanding every OTel attribute into protocol labels without sanitization
  or cardinality caps
- Prometheus label projection (product Prometheus removed)
