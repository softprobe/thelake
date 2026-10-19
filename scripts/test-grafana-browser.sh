#!/usr/bin/env bash
# Run Grafana browser tests in a disposable, run-owned Compose stack.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BROWSER_DIR="$ROOT/tests/compat/grafana/browser"
CONFIG_FILE="$BROWSER_DIR/playwright.config.ts"
command -v node >/dev/null 2>&1 || { echo "ERROR: node is required for browser tests." >&2; exit 1; }
[[ -n "${GRAFANA_COMPOSE_IMAGE:-}" ]] || { echo "ERROR: GRAFANA_COMPOSE_IMAGE must be supplied from the pinned reference manifest." >&2; exit 1; }

if [[ ! -d "$BROWSER_DIR/node_modules/@playwright/test" ]]; then
  npm --prefix "$BROWSER_DIR" ci --no-audit
fi
(cd "$BROWSER_DIR" && npx playwright install chromium)

ARTIFACT_DIR="$ROOT/target/test-artifacts/grafana-browser/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$ARTIFACT_DIR"
export GRAFANA_SEED_ARTIFACT_DIR="$ARTIFACT_DIR"
# Let Docker choose free host ports. The tests talk to Grafana through the
# mapped port returned by the shared test-stack lifecycle owner.
export GRAFANA_AUTH_MOCK_PORT=0 GRAFANA_POSTGRES_PORT=0 GRAFANA_MINIO_PORT=0
export GRAFANA_SOFTPROBE_HTTP_PORT=0 GRAFANA_SOFTPROBE_GRPC_PORT=0 GRAFANA_PORT=0
export GRAFANA_PLAYWRIGHT_REPORT_DIR="$ARTIFACT_DIR/playwright-report"
export GRAFANA_PLAYWRIGHT_OUTPUT_DIR="$ARTIFACT_DIR/test-results"

stack_json=""
stack_state=""
cleanup() {
  local status=$?
  if [[ -n "$stack_state" ]]; then
    python3 "$ROOT/scripts/test_stack.py" stop --state-file "$stack_state" || status=$?
  fi
  echo "Grafana browser artifacts retained at $ARTIFACT_DIR" >&2
  exit "$status"
}
trap cleanup EXIT

stack_json="$(python3 scripts/test_stack.py start-compose \
  --suite grafana-browser \
  --artifact-dir "$ARTIFACT_DIR" \
  --compose-file tests/compat/grafana/docker-compose.ci.yml \
  --service grafana \
  --published-port grafana:3000 \
  --published-port softprobe:8090 \
  --wait)"
stack_state="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["state_file"])' "$stack_json")"
grafana_port="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["ports"]["grafana:3000"][0].rsplit(":",1)[1])' "$stack_json")"
softprobe_port="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["ports"]["softprobe:8090"][0].rsplit(":",1)[1])' "$stack_json")"
seed_window="$(python3 - "$ARTIFACT_DIR/seed-receipt.json" <<'PY'
import json, sys
receipt = json.load(open(sys.argv[1], encoding="utf-8"))
if receipt.get("status") != "pass":
    raise SystemExit("Grafana seed did not produce a passing receipt")
print(receipt["window_start_s"] * 1_000_000_000, receipt["window_end_s"] * 1_000_000_000)
PY
)"
read -r GRAFANA_SEED_START_NS GRAFANA_SEED_END_NS <<<"$seed_window"
export GRAFANA_SEED_START_NS GRAFANA_SEED_END_NS
export GRAFANA_URL="http://127.0.0.1:$grafana_port"
export SOFTPROBE_URL="http://127.0.0.1:$softprobe_port"
export SOFTPROBE_API_KEY=grafana-test-tenant-a
export SOFTPROBE_TENANT_ID=cccccccc-cccc-cccc-cccc-cccccccccccc
curl -sf -u admin:admin "$GRAFANA_URL/api/health" >/dev/null
curl -sf -H "Authorization: Bearer $SOFTPROBE_API_KEY" -H "X-Scope-OrgID: $SOFTPROBE_TENANT_ID" "$SOFTPROBE_URL/ready" >/dev/null

(cd "$BROWSER_DIR" && npx playwright test --config "$CONFIG_FILE" "$@")
