#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
HARNESS="$ROOT_DIR/scripts/grafana-system-smoke.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/grafana-tempo-tenant-contract.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

tempo_response="$TMP_DIR/tempo.json"
cat >"$tempo_response" <<'JSON'
{
  "traceID": "qqqqqqqqqqqqqqqqqqqqqg==",
  "batches": [{
    "resource": {"attributes": [{"key": "tenant.marker", "value": {"stringValue": "grafana-phase4-tenant-a"}}]},
    "scopeSpans": [{
      "scope": {"name": "grafana-seeder"},
      "spans": [{
        "traceId": "qqqqqqqqqqqqqqqqqqqqqg==",
        "spanId": "AAAAAAAAAAE=",
        "parentSpanId": "AAAAAAAAAAA=",
        "startTimeUnixNano": "1700000010000000000",
        "endTimeUnixNano": "1700000011000000000",
        "status": {"code": "STATUS_CODE_OK"},
        "events": [{"name": "checkout.started", "timeUnixNano": "1700000010500000000"}],
        "links": [{"traceId": "qqqqqqqqqqqqqqqqqqqqqg==", "spanId": "AAAAAAAAAAI="}]
      }]
    }]
  }]
}
JSON

if ! bash -c 'source "$1"; validate_tempo_trace_response "$2" aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa grafana-phase4-tenant-a grafana-phase4-tenant-b' _ "$HARNESS" "$tempo_response"; then
  echo 'Tempo validator did not normalize the raw search trace ID to canonical wire Base64' >&2
  exit 1
fi

prometheus_response="$TMP_DIR/prometheus.json"
printf '%s\n' '{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"checkout"},"value":[1700000030,"1"]}]}}' >"$prometheus_response"
if bash -c 'source "$1"; validate_signal_response "$2" prometheus grafana-phase4-tenant-a grafana-phase4-tenant-b softprobe-prom-a' _ "$HARNESS" "$prometheus_response"; then
  echo 'Prometheus validator accepted an ambiguous tenant response' >&2
  exit 1
fi

explore_response="$TMP_DIR/explore.json"
printf '%s\n' '{"results":{"A":{"refId":"A","frames":[{"schema":{"name":"data","fields":[{"name":"value"}]},"data":{"values":[[1]]}}]}}}' >"$explore_response"
if bash -c 'source "$1"; validate_explore_response "$2" grafana-phase4-tenant-a grafana-phase4-tenant-b softprobe-prom-a prometheus' _ "$HARNESS" "$explore_response"; then
  echo 'Explore validator accepted an ambiguous tenant response' >&2
  exit 1
fi

mock_tempo="$TMP_DIR/mock-tempo.json"
bash -c 'source "$1"; mock_signal_response tempo softprobe-tempo-a grafana-phase4-tenant-a' _ "$HARNESS" >"$mock_tempo"
python3 - "$mock_tempo" <<'PY'
import json
import pathlib
import sys

trace_id = json.loads(pathlib.Path(sys.argv[1]).read_text())["traces"][0]["traceID"]
if trace_id != "qqqqqqqqqqqqqqqqqqqqqg==":
    raise SystemExit(f"mock Tempo search used a noncanonical trace ID: {trace_id!r}")
PY

echo 'Grafana Tempo/tenant-positive contract: PASS'
