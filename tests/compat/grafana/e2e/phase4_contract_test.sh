#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
HARNESS="$ROOT_DIR/scripts/grafana-system-smoke.sh"
PROVISIONING="$ROOT_DIR/tests/compat/grafana/provisioning/datasources"
DASHBOARDS="$ROOT_DIR/tests/compat/grafana/dashboards"

# Keep this contract tied to the checked-in provisioning layout.  A previous
# version looked one directory too high and therefore passed without checking
# the Loki and Tempo datasource contracts at all.
[[ -d "$PROVISIONING" ]] || {
  echo "Grafana datasource provisioning directory is missing: $PROVISIONING" >&2
  exit 1
}
for datasource in prometheus loki tempo; do
  [[ -f "$PROVISIONING/$datasource.yaml" ]] || {
    echo "missing Grafana datasource provisioning: $PROVISIONING/$datasource.yaml" >&2
    exit 1
  }
done
if grep -Eq 'provisioning/(prometheus|loki|tempo)\.yaml' "$ROOT_DIR/tests/compat/grafana/e2e"/*.sh; then
  echo 'Grafana contract tests use the stale datasource provisioning path' >&2
  exit 1
fi

for symbol in \
  validate_grafana_reference_pin \
  validate_dashboard_round_trip \
  check_dashboard_variables \
  check_dashboard_panels \
  validate_explore_response \
  validate_cross_signal_links \
  check_cross_signal \
  check_errors \
  redact; do
  grep -Eq "^${symbol}[[:space:]]*\(\)" "$HARNESS" || {
    echo "Grafana harness is missing required assertion: $symbol" >&2
    exit 1
  }
done

for case_id in G1 G2 G3 G7 G8; do
  grep -Fq "record_case $case_id" "$HARNESS" || {
    echo "Grafana harness does not record $case_id" >&2
    exit 1
  }
done
grep -Fq 'run_signal_case G4 prometheus' "$HARNESS"
grep -Fq 'run_signal_case G5 loki' "$HARNESS"
grep -Fq 'run_signal_case G6 tempo' "$HARNESS"

grep -Fq 'GRAFANA_CHECK_DASHBOARD_QUERIES=1' "$HARNESS"
grep -Fq 'GRAFANA_REFERENCE_DIGEST' "$HARNESS"
grep -Fq 'GRAFANA_REFERENCE_MANIFEST' "$HARNESS"
grep -Fq 'docker image inspect' "$HARNESS"
grep -Fq 'sha256:' "$HARNESS"
grep -Fq 'validate_repeat_response' "$HARNESS"
grep -Fq 'validate_explore_response' "$HARNESS"
grep -Fq 'validate_signal_response' "$HARNESS"
grep -Fq 'validate_error_response' "$HARNESS"
grep -Fq 'credential_probe' "$HARNESS"
grep -Fq 'missing_credentials' "$HARNESS"
grep -Fq 'invalid_credentials' "$HARNESS"
grep -Fq 'mismatched_tenant' "$HARNESS"
grep -Fq '__missing__' "$HARNESS"
grep -Fq 'Authorization' "$HARNESS"
grep -Fq 'write_case_bundle' "$HARNESS"
grep -Fq '| redact' "$HARNESS"
grep -Fq 'X-Scope-OrgID' "$HARNESS"
grep -Fq 'SOFTPROBE_TENANT_A_API_KEY' "$HARNESS"
grep -Fq 'SOFTPROBE_TENANT_B_API_KEY' "$HARNESS"
DATASOURCES="$PROVISIONING"
grep -Fq 'X-Scope-OrgID' "$DATASOURCES/prometheus.yaml"
grep -Fq 'X-Scope-OrgID' "$DATASOURCES/loki.yaml"
grep -Fq 'X-Scope-OrgID' "$DATASOURCES/tempo.yaml"
grep -Fq 'tracesToLogsV2' "$DATASOURCES/tempo.yaml"
grep -Fq 'derivedFields' "$DATASOURCES/loki.yaml"

python3 - "$DASHBOARDS" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
dashboards = {}
for path in sorted(root.glob("*.json")):
    document = json.loads(path.read_text())
    dashboard = document.get("dashboard", document)
    uid = dashboard.get("uid")
    if not uid:
        raise SystemExit(f"dashboard has no stable uid: {path}")
    if uid in dashboards:
        raise SystemExit(f"duplicate dashboard uid {uid}")
    dashboards[uid] = dashboard

required = {
    "softprobe-prom-smoke",
    "softprobe-loki-smoke",
    "softprobe-tempo-smoke",
    "softprobe-cross-signal",
}
missing = required - dashboards.keys()
if missing:
    raise SystemExit(f"missing representative dashboards: {sorted(missing)}")

cross = dashboards["softprobe-cross-signal"]
variables = {item.get("name") for item in cross.get("templating", {}).get("list", [])}
if not {"job", "service"} <= variables:
    raise SystemExit(f"cross-signal dashboard variables are incomplete: {variables}")

text = json.dumps(cross, sort_keys=True)
for uid in ("softprobe-prom-a", "softprobe-loki-a", "softprobe-tempo-a"):
    if uid not in text:
        raise SystemExit(f"cross-signal dashboard is missing datasource {uid}")

def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)

for uid in required:
    panels = [item for item in walk(dashboards[uid]) if "targets" in item and item.get("type")]
    if not panels:
        raise SystemExit(f"dashboard {uid} has no query panels")
PY

echo 'Grafana Phase 4 static contract: PASS'
