# Compat fixtures

Signal-neutral OTel fixtures and protocol contract goldens shared across
Loki, Tempo, and Grafana tests.

| File | Purpose |
|------|---------|
| `loki_error_unsupported.json` | Loki stub error envelope |
| `tempo_error_unsupported.json` | Tempo stub error envelope |
| `*_success_minimal.json` | Target success shapes |
| `auth_missing_bearer.json` | Missing Authorization → 401 (status-only) |
| `auth_forbidden.json` | Rejected API key → 403 (status-only) |
| `auth_scope_mismatch.json` | Scope header mismatch → 403 + protocol body |
| `auth_status_only.md` | Auth middleware body contract notes |

Prefer builders in `tests/util/otlp.rs` and helpers under `tests/compat/support/`.

Canonical docs: `docs/compat/matrix.md`.
