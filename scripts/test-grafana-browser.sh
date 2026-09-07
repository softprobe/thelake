#!/usr/bin/env bash
# End-to-end browser automation + Grafana + OTel demo ingestion test suite.
# Usage (repo root): ./scripts/test-grafana-browser.sh [playwright args]
# Make: make test-grafana-browser

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BROWSER_DIR="$ROOT/tests/compat/grafana/browser"
CONFIG_FILE="$BROWSER_DIR/playwright.config.ts"
SOFTPROBE_URL="${SOFTPROBE_URL:-http://127.0.0.1:8090}"
GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3000}"

echo "================================================================"
echo " TheLake E2E Browser Automation + Grafana + OTel Ingestion Suite"
echo "================================================================"

# Check node & npx
if ! command -v node >/dev/null 2>&1; then
  echo "ERROR: node is required for browser automation tests." >&2
  exit 1
fi

# Deterministically install declared Playwright dependencies
if [[ ! -d "$BROWSER_DIR/node_modules/@playwright/test" ]]; then
  echo "==> Installing browser automation dependencies in $BROWSER_DIR..."
  if [[ -f "$BROWSER_DIR/package-lock.json" ]]; then
    npm --prefix "$BROWSER_DIR" ci --no-audit
  else
    npm --prefix "$BROWSER_DIR" install --no-audit
  fi
fi

# Ensure Playwright Chromium browser binary is installed
if ! (cd "$BROWSER_DIR" && npx playwright --version >/dev/null 2>&1); then
  echo "ERROR: playwright executable not found after install." >&2
  exit 1
fi
(cd "$BROWSER_DIR" && npx playwright install chromium)

# Force the manual stack to run the current binary from reviewed source
export GRAFANA_REUSE_STACK="${GRAFANA_REUSE_STACK:-0}"
# TWCS open-day waves under demo load starve fresh k6_http_reqs (I-02/I-03).
export THELAKE_MAINTENANCE_ENABLED="${THELAKE_MAINTENANCE_ENABLED:-false}"
echo "==> Ensuring Grafana stack + live OTel ingestion are ready (GRAFANA_REUSE_STACK=$GRAFANA_REUSE_STACK THELAKE_MAINTENANCE_ENABLED=$THELAKE_MAINTENANCE_ENABLED)..."
./scripts/grafana-manual-up.sh

echo "==> Verifying Softprobe and Grafana connectivity..."
curl -sf "$SOFTPROBE_URL/ready" >/dev/null || { echo "ERROR: Softprobe not ready at $SOFTPROBE_URL" >&2; exit 1; }
curl -sf -u admin:admin "$GRAFANA_URL/api/health" >/dev/null || { echo "ERROR: Grafana not healthy at $GRAFANA_URL" >&2; exit 1; }

echo "==> Running Playwright E2E test suite..."
export SOFTPROBE_URL
export GRAFANA_URL

cd "$BROWSER_DIR"
npx playwright test --config "$CONFIG_FILE" "$@"

echo "================================================================"
echo " All E2E browser automation & query feature tests PASSED!"
echo " Checklist: docs/compat/query_features_checklist.md"
echo "================================================================"
