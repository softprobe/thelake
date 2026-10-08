# Softprobe SDK contracts

Language-neutral schemas and fixtures shared by `@softprobe/tracing` and
`softprobe`. Narrative docs: [`docs/sdk/`](../docs/sdk/README.md).

- `schemas/` — JSON Schema for observation types, attributes, content events,
  score requests, and normalized spans
- `fixtures/` — canonical catalogs and expected nested span/score payloads

Validate with:

```bash
make contracts-test
# or: python3 scripts/validate_contracts.py
```
