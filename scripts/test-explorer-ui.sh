#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BROWSER_DIR="$ROOT/tests/compat/grafana/browser"
EXPLORER_DIR="$ROOT/packages/thelake-explorer"

command -v node >/dev/null 2>&1 || { echo "ERROR: node is required for Explorer UI E2E." >&2; exit 1; }

if [[ ! -x "$BROWSER_DIR/node_modules/.bin/playwright" ]]; then
  npm --prefix "$BROWSER_DIR" ci --no-audit
fi
if [[ ! -x "$EXPLORER_DIR/node_modules/.bin/vite" ]]; then
  npm --prefix "$EXPLORER_DIR" ci --no-audit
fi

if [[ "$(uname -s)" == "Linux" && "${CI:-}" == "true" ]]; then
  (cd "$BROWSER_DIR" && npx playwright install --with-deps chromium)
else
  (cd "$BROWSER_DIR" && npx playwright install chromium)
fi

cd "$BROWSER_DIR"
npx playwright test --config playwright.explorer.config.ts "$@"
