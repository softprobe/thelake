---
name: run-thelake-tests
description: Select and run TheLake's local, browser, online, integration, compatibility, and release test gates.
---

# Run TheLake tests

Choose tests from the change surface; read the Makefile target before running
unfamiliar or externally connected suites. Preserve the first failure and
report commands and environmental limits accurately.

Any automated test that writes state must run against transient, run-owned
databases and workspaces and clean them up automatically, including after
failure. Never point tests at a developer's running service or pre-existing
data. Docker-backed local test environments must use the shared lifecycle
owner, `scripts/test_stack.py`; Compose files declare topology but test fixtures
and harnesses must not start or remove containers directly. Reports and logs
stay under `target/test-artifacts/`, while run-owned service resources and
scratch data are removed. See
[Tests and evaluations](../../../docs/how-to/test-and-eval.md).

## Common gates

- `make doctor`: check the local toolchain and build prerequisites.
- `make check-fmt` and `make lint`: formatting and static checks, including
  SQL guardrails.
- `make test`: Rust library/integration/compatibility tests plus Explorer npm
  tests; it downloads/builds required artifacts.
- `make test-e2e`: provisions its own transient Postgres and MinIO stack,
  initializes the test bucket, runs the shared E2E matrix for DuckLake workspace
  scopes, then removes its stack while retaining test artifacts.
- `make ci`: repository pre-merge gate; see its Makefile recipe for the exact
  current composition.

## Focused checks

- Explorer browser path: `make test-explorer-ui`.
- Live Decisions triage, Gemini evaluator, and browser path:
  `make test-explorer-online-e2e`.
  This uses paid provider calls; run only when explicitly requested and when
  credentials are securely provisioned.
- Explorer component tests: `npm --prefix packages/thelake-explorer test`.
- Slack alert end-to-end verification: use `verify-lisa-slack-alert`; it sends
  an external Slack message and requires explicit authorization.
- Compatibility and performance suites are resource-intensive and may start
  reference services; inspect `make test-compat` / `make test-perf` before use.

For behavior-evaluation dogfood, use `run-lisa-evaluation-dogfood`, which
separates fixture calibration, automatic online evaluation, and visible issue
verification. Never substitute a direct evaluator API call for a test of the
automatic trace-triggered path.
