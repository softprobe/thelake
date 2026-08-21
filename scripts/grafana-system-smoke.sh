#!/usr/bin/env bash
set -euo pipefail

GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3000}"
GRAFANA_ADMIN_USER="${GRAFANA_ADMIN_USER:-admin}"
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-admin}"
ARTIFACT_DIR="${ARTIFACT_DIR:-target/compat/grafana}"
MOCK_FIXTURE_DIR="${MOCK_FIXTURE_DIR:-tests/compat/fixtures}"
GRAFANA_DASHBOARD_DIR="${GRAFANA_DASHBOARD_DIR:-tests/compat/grafana/dashboards}"
MOCK_MODE="${MOCK:-0}"
if [[ -n "${CI:-}" ]]; then
  # The CI harness contract always exercises dashboard variables and panel
  # data frames.  Do not allow a caller-provided environment value to weaken
  # the Phase 4 acceptance gate.
  GRAFANA_CHECK_DASHBOARD_QUERIES=1
elif [[ -z "${GRAFANA_CHECK_DASHBOARD_QUERIES+x}" ]]; then
  GRAFANA_CHECK_DASHBOARD_QUERIES=0
fi
GRAFANA_REFERENCE_IMAGE="${GRAFANA_REFERENCE_IMAGE:-grafana/grafana:11.2.0}"
GRAFANA_REFERENCE_DIGEST="${GRAFANA_REFERENCE_DIGEST:-}"
GRAFANA_REFERENCE_MANIFEST="${GRAFANA_REFERENCE_MANIFEST:-docs/compat/references.v0.yaml}"

DATASOURCE_UIDS=(
  softprobe-prom softprobe-prom-a softprobe-prom-b
  softprobe-loki-a softprobe-loki-b
  softprobe-tempo-a softprobe-tempo-b
)
DASHBOARD_UIDS=()
if [[ -d "$GRAFANA_DASHBOARD_DIR" ]]; then
  while IFS= read -r dashboard_uid; do
    [[ -n "$dashboard_uid" ]] && DASHBOARD_UIDS+=("$dashboard_uid")
  done < <(python3 - "$GRAFANA_DASHBOARD_DIR" <<'PY'
import json
import pathlib
import sys

directory = pathlib.Path(sys.argv[1])
for path in sorted(directory.glob("*.json")):
    document = json.loads(path.read_text())
    dashboard = document.get("dashboard", document)
    uid = dashboard.get("uid")
    if not uid:
        raise SystemExit(f"dashboard fixture has no uid: {path}")
    print(uid)
PY
  )
fi

TENANT_A_ID="${GRAFANA_TEST_TENANT_A_ID:-grafana-phase4-tenant-a}"
TENANT_B_ID="${GRAFANA_TEST_TENANT_B_ID:-grafana-phase4-tenant-b}"
SEED_SOFTPROBE_URL="${GRAFANA_SEED_SOFTPROBE_URL:-http://127.0.0.1:${GRAFANA_SOFTPROBE_HTTP_PORT:-18090}}"
GRAFANA_AUTH_MOCK_URL="${GRAFANA_AUTH_MOCK_URL:-http://127.0.0.1:${GRAFANA_AUTH_MOCK_PORT:-18080}}"
CROSS_START_NS="1700000000000000000"
CROSS_END_NS="1700000060000000000"
TRACE_START_S="1700000000"
TRACE_END_S="1700000060"
CHECKS=()
SKIPPED=0
VARIABLE_BUNDLE_ARGS=()

mkdir -p "$ARTIFACT_DIR" "$ARTIFACT_DIR/.work"

validate_grafana_reference_pin() {
  [[ -f "$GRAFANA_REFERENCE_MANIFEST" ]] || {
    echo "Grafana reference manifest not found: $GRAFANA_REFERENCE_MANIFEST" >&2
    return 1
  }
  local manifest_image manifest_digest
  IFS=$'\t' read -r manifest_image manifest_digest < <(python3 - "$GRAFANA_REFERENCE_MANIFEST" <<'PY'
import pathlib
import re
import sys

text = pathlib.Path(sys.argv[1]).read_text()
match = re.search(
    r"(?ms)^\s+grafana:\s*\n\s+image:\s*([^\s#]+)\s*\n\s+tag:\s*[\"']?([^\s\"']+).*?\n\s+digest:\s*[\"']?([^\s\"']+)",
    text,
)
if not match:
    raise SystemExit("Grafana reference is missing image/tag/digest in the compatibility manifest")
print(f"{match.group(1)}:{match.group(2)}\t{match.group(3)}")
PY
  ) || return 1
  [[ "$GRAFANA_REFERENCE_IMAGE" == "$manifest_image" ]] || {
    echo "Grafana reference image drift: expected $manifest_image, got $GRAFANA_REFERENCE_IMAGE" >&2
    return 1
  }
  [[ "$manifest_digest" =~ ^sha256:[0-9a-fA-F]{64}$ ]] || {
    echo "Grafana reference manifest must contain an immutable sha256 digest" >&2
    return 1
  }
  if [[ -n "$GRAFANA_REFERENCE_DIGEST" && "$GRAFANA_REFERENCE_DIGEST" != "$manifest_digest" ]]; then
    echo "Grafana reference digest drift: expected $manifest_digest, got $GRAFANA_REFERENCE_DIGEST" >&2
    return 1
  fi
  GRAFANA_REFERENCE_DIGEST="$manifest_digest"
  # Mock mode validates the manifest/tag/digest contract but has no local
  # image to inspect.
  if [[ "$MOCK_MODE" == "1" ]]; then
    return 0
  fi
  command -v docker >/dev/null 2>&1 || {
    echo "docker is required to validate the Grafana image digest" >&2
    return 1
  }
  docker image inspect --format '{{join .RepoDigests "\\n"}}' "$GRAFANA_REFERENCE_IMAGE" \
    | grep -Fq "@${GRAFANA_REFERENCE_DIGEST}" || {
      echo "Grafana image does not resolve to $GRAFANA_REFERENCE_DIGEST" >&2
      return 1
    }
}

redact() {
  local value="${1:-}"
  if (( $# == 0 )); then
    value="$(cat)"
  fi
  REDACTION_INPUT="$value" python3 - <<'PY'
import json
import os
import re

value = os.environ.get("REDACTION_INPUT", "")
credential_name = re.compile(
    r"(?:password|passwd|secret|token|api[_-]?key|access[_-]?key|"
    r"authorization|proxyauthorization|cookie|credential|bearer|"
    r"httpheadervalue|headervalue|tenantkey|clientkey)",
    re.IGNORECASE,
)
query_credential = re.compile(
    r"(?i)([?&][^=&#\s\"']*(?:password|passwd|secret|token|api[_-]?key|"
    r"access[_-]?token|authorization|credential|bearer)[^=&#\s\"']*=)"
    r"([^&#\s\"']+)"
)
text_credential = re.compile(
    r"(?i)(\b(?:authorization|proxy-authorization|api[_-]?key|token|"
    r"secret|password|passwd|credential|bearer)\b\s*[:=]\s*)"
    r"([\"']?)([^\"'\s,;\&#]+)"
)
bearer = re.compile(r"(?i)\bBearer\s+[^\s\"'`,;]+")

values = []
for name, candidate in os.environ.items():
    if name == "GRAFANA_ADMIN_USER" or credential_name.search(name):
        if candidate and candidate not in values:
            values.append(candidate)
values.sort(key=len, reverse=True)

def redact_text(text):
    for secret in values:
        text = text.replace(secret, "[REDACTED]")
    text = query_credential.sub(r"\1[REDACTED]", text)
    text = text_credential.sub(r"\1\2[REDACTED]", text)
    return bearer.sub("Bearer [REDACTED]", text)

def is_credential_key(key):
    return bool(credential_name.search(re.sub(r"[^A-Za-z0-9]", "", key)))

def scrub(item):
    if isinstance(item, dict):
        return {
            key: "[REDACTED]" if is_credential_key(key) else scrub(val)
            for key, val in item.items()
        }
    if isinstance(item, list):
        return [scrub(val) for val in item]
    if isinstance(item, str):
        return redact_text(item)
    return item

try:
    print(json.dumps(scrub(json.loads(value)), indent=2, sort_keys=True))
except json.JSONDecodeError:
    print(redact_text(value), end="")
PY
}

write_outcome() {
  local outcome="$1" reason="${2:-}" payload
  payload="$(python3 - "$outcome" "$reason" "$MOCK_MODE" <<'PY'
import json
import sys
outcome, reason, mock = sys.argv[1:]
result = {"outcome": outcome}
if reason:
    result["reason"] = reason
if mock == "1":
    result["validation_only"] = True
print(json.dumps(result, indent=2, sort_keys=True))
PY
)"
  redact "$payload" > "$ARTIFACT_DIR/outcome.json"
}

write_summary() {
  local outcome="$1" reason="${2:-}" payload
  payload="$(python3 - "$outcome" "$reason" "$MOCK_MODE" "${CHECKS[@]-}" <<'PY'
import json
import sys
outcome, reason, mock, *checks = sys.argv[1:]
result = {"outcome": outcome, "checks": checks}
if reason:
    result["reason"] = reason
if mock == "1":
    result["validation_only"] = True
print(json.dumps(result, indent=2, sort_keys=True))
PY
)"
  redact "$payload" > "$ARTIFACT_DIR/summary.json"
}

normalize_tempo_trace_id() {
  python3 - "$1" <<'PY'
import base64
import re
import sys

value = sys.argv[1]
if re.fullmatch(r"[0-9a-fA-F]{32}", value):
    print(base64.b64encode(bytes.fromhex(value)).decode())
    raise SystemExit(0)
try:
    decoded = base64.b64decode(value, validate=True)
except Exception as exc:
    raise SystemExit(f"invalid Tempo trace ID: {value!r}") from exc
if len(decoded) != 16 or base64.b64encode(decoded).decode() != value:
    raise SystemExit(f"Tempo trace ID is not canonical padded Base64: {value!r}")
print(value)
PY
}

extract_tempo_search_id() {
  python3 - "$1" <<'PY'
import base64
import json
import pathlib
import re
import sys

obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
traces = obj.get("traces")
if not isinstance(traces, list):
    raise SystemExit("Tempo search response has no traces list")
for item in traces:
    if not isinstance(item, dict):
        continue
    value = item.get("traceID", item.get("traceId"))
    if re.fullmatch(r"[0-9a-fA-F]{32}", value or ""):
        print(value)
        raise SystemExit(0)
    try:
        decoded = base64.b64decode(value or "", validate=True)
    except Exception:
        continue
    if len(decoded) == 16 and base64.b64encode(decoded).decode() == value:
        print(value)
        raise SystemExit(0)
raise SystemExit("Tempo search returned no exact trace ID")
PY
}

finish_skip() {
  local reason="$1"
  write_outcome "environment_skip" "$reason"
  write_summary "environment_skip" "$reason"
  if [[ "$MOCK_MODE" == "1" ]]; then
    exit 0
  fi
  exit 1
}

finish_failure() {
  local reason="$1"
  write_outcome "failure" "$reason"
  write_summary "failure" "$reason"
  exit 1
}

record_case() {
  local case_id="$1" outcome="$2" reason="${3:-}"
  CHECKS+=("${case_id}:${outcome}")
  local payload
  payload="$(python3 - "$case_id" "$outcome" "$reason" "$MOCK_MODE" <<'PY'
import json
import sys
case_id, outcome, reason, mock = sys.argv[1:]
result = {"case": case_id, "outcome": outcome}
if reason:
    result["reason"] = reason
if mock == "1":
    result["validation_only"] = True
print(json.dumps(result, indent=2, sort_keys=True))
PY
)"
  redact "$payload" > "$ARTIFACT_DIR/${case_id}.outcome.json"
}

write_case_bundle() {
  local case_id="$1"; shift
  python3 - "$case_id" "$MOCK_MODE" "$@" <<'PY' | redact > "$ARTIFACT_DIR/${case_id}.raw.json"
import json
import pathlib
import sys

case_id, mock, *items = sys.argv[1:]
responses = {}
for item in items:
    label, path = item.split("=", 1)
    try:
        responses[label] = json.loads(pathlib.Path(path).read_text())
    except json.JSONDecodeError:
        responses[label] = {"raw": pathlib.Path(path).read_text()}
result = {"case": case_id, "validation_only": mock == "1", "responses": responses}
print(json.dumps(result, indent=2, sort_keys=True))
PY
  python3 - "$ARTIFACT_DIR/${case_id}.raw.json" <<'PY' | redact > "$ARTIFACT_DIR/${case_id}.normalized.json"
import json
import pathlib
import sys
source = pathlib.Path(sys.argv[1])
obj = json.loads(source.read_text())
obj["normalization"] = "sorted-json-response-envelope"
print(json.dumps(obj, indent=2, sort_keys=True))
PY
}

mock_signal_response() {
  local signal="$1" uid="$2" tenant="$3"
  python3 - "$MOCK_FIXTURE_DIR" "$signal" "$uid" "$tenant" <<'PY'
import json
import pathlib
import sys

fixture_dir, signal, uid, tenant = sys.argv[1:]
fixture_name = {
    "prometheus": "prometheus_success_minimal.json",
    "loki": "loki_success_minimal.json",
    "tempo": "tempo_success_minimal.json",
}[signal]
try:
    obj = json.loads((pathlib.Path(fixture_dir) / fixture_name).read_text())
except (FileNotFoundError, json.JSONDecodeError) as exc:
    print(json.dumps({"error": f"mock fixture unavailable: {exc}"}))
    raise SystemExit(0)
if signal == "prometheus":
    obj["data"]["result"].append({
        "metric": {"tenant": tenant, "datasource_uid": uid},
        "value": [1700000000, "1"],
    })
elif signal == "loki":
    obj["data"]["result"].append({
        "stream": {"tenant": tenant, "datasource_uid": uid},
        "values": [["1700000000000000000", json.dumps({
            "tenant": tenant,
            "source_uid": uid,
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        })]],
    })
else:
    obj["traces"] = [{"traceID": "qqqqqqqqqqqqqqqqqqqqqg==", "rootServiceName": tenant, "datasource_uid": uid}]
print(json.dumps(obj))
PY
}

mock_datasource() {
  local uid="$1"
  python3 - "$uid" "$TENANT_A_ID" "$TENANT_B_ID" <<'PY'
import json
import sys
uid, tenant_a, tenant_b = sys.argv[1:]
is_tenant = uid.endswith("-a") or uid.endswith("-b")
tenant = tenant_a if uid.endswith("-a") else tenant_b
signal = "prometheus" if "prom" in uid else "loki" if "loki" in uid else "tempo"
headers = {"httpHeaderName1": "Authorization"}
secure = {"httpHeaderValue1": True}
if is_tenant:
    headers["httpHeaderName2"] = "X-Scope-OrgID"
    secure["httpHeaderValue2"] = True
data = {
    "uid": uid,
    "name": uid,
    "type": signal,
    "url": "http://mock.invalid",
    "jsonData": headers,
    "secureJsonFields": secure,
}
if is_tenant:
    data["tenant_validation_marker"] = tenant
if uid == "softprobe-loki-a":
    data["jsonData"]["derivedFields"] = [{"datasourceUid": "softprobe-tempo-a"}]
elif uid == "softprobe-loki-b":
    data["jsonData"]["derivedFields"] = [{"datasourceUid": "softprobe-tempo-b"}]
elif uid == "softprobe-tempo-a":
    data["jsonData"]["tracesToLogsV2"] = {"datasourceUid": "softprobe-loki-a"}
elif uid == "softprobe-tempo-b":
    data["jsonData"]["tracesToLogsV2"] = {"datasourceUid": "softprobe-loki-b"}
print(json.dumps(data))
PY
}

mock_dashboard() {
  local uid="$1"
  python3 - "$GRAFANA_DASHBOARD_DIR" "$uid" <<'PY'
import json
import pathlib
import sys
dashboard_dir, uid = sys.argv[1:]
path = pathlib.Path(dashboard_dir) / f"{uid}.json"
dashboard = json.loads(path.read_text())
print(json.dumps({
    "meta": {"isFolder": False, "folderTitle": "Softprobe", "folderUid": "softprobe-folder"},
    "dashboard": dashboard,
}))
PY
}

mock_tempo_trace_response() {
  local trace_id="$1" tenant="$2"
  python3 - "$trace_id" "$tenant" <<'PY'
import base64
import json
import re
import sys
trace_id, tenant = sys.argv[1:]
if re.fullmatch(r"[0-9a-fA-F]{32}", trace_id):
    trace_bytes = bytes.fromhex(trace_id)
    trace_wire_id = base64.b64encode(trace_bytes).decode("ascii")
else:
    trace_bytes = base64.b64decode(trace_id, validate=True)
    trace_wire_id = base64.b64encode(trace_bytes).decode("ascii")
    if len(trace_bytes) != 16 or trace_wire_id != trace_id:
        raise SystemExit("mock Tempo trace lookup requires canonical padded Base64")
span_wire_id = base64.b64encode(bytes.fromhex("01" * 8)).decode("ascii")
parent_wire_id = base64.b64encode(bytes.fromhex("00" * 8)).decode("ascii")
link_wire_id = base64.b64encode(bytes.fromhex("02" * 8)).decode("ascii")
print(json.dumps({
    "traceID": trace_wire_id,
    "batches": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": tenant}}]},
        "scopeSpans": [{
            "scope": {"name": "grafana-seeder", "version": "1.0.0"},
            "spans": [{
                "traceId": trace_wire_id,
                "spanId": span_wire_id,
                "parentSpanId": parent_wire_id,
                "name": "checkout",
                "startTimeUnixNano": "1700000010000000000",
                "endTimeUnixNano": "1700000011000000000",
                "status": {"code": "STATUS_CODE_OK"},
                "events": [{"name": "checkout.started", "timeUnixNano": "1700000010500000000"}],
                "links": [{"traceId": trace_wire_id, "spanId": link_wire_id}]
            }]
        }],
    }, {
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": tenant}}]},
        "scopeSpans": [{
            "scope": {"name": "grafana-secondary", "version": "1.0.0"},
            "spans": [{
                "traceId": trace_wire_id,
                "spanId": base64.b64encode(bytes.fromhex("03" * 8)).decode("ascii"),
                "name": "checkout.child",
                "startTimeUnixNano": "1700000011000000000",
                "endTimeUnixNano": "1700000012000000000",
                "status": {"code": "STATUS_CODE_UNSET"}
            }]
        }]
    }],
}))
PY
}

mock_response() {
  local endpoint="$1"
  case "$endpoint" in
    /api/health)
      printf '%s\n' '{"database":"ok","version":"mock","commit":"mock"}'
      ;;
    /api/datasources)
      printf '%s\n' '[{"uid":"softprobe-prom"},{"uid":"softprobe-prom-a"},{"uid":"softprobe-prom-b"},{"uid":"softprobe-loki-a"},{"uid":"softprobe-loki-b"},{"uid":"softprobe-tempo-a"},{"uid":"softprobe-tempo-b"}]'
      ;;
    /api/datasources/uid/*/health)
      printf '%s\n' '{"status":"OK","message":"mock datasource health"}'
      ;;
    /api/datasources/uid/*)
      mock_datasource "${endpoint##*/}"
      ;;
    "/api/search?type=dash-db")
      python3 - "$GRAFANA_DASHBOARD_DIR" <<'PY'
import json
import pathlib
import sys

items = []
for path in sorted(pathlib.Path(sys.argv[1]).glob("*.json")):
    dashboard = json.loads(path.read_text())
    items.append({"uid": dashboard["uid"], "title": dashboard.get("title", dashboard["uid"])})
print(json.dumps(items))
PY
      ;;
    /api/dashboards/uid/*)
      mock_dashboard "${endpoint##*/}"
      ;;
    /api/datasources/proxy/uid/softprobe-tempo-a/api/traces/*)
      local trace_endpoint="${endpoint##*/}"
      mock_tempo_trace_response "${trace_endpoint%%\?*}" "$TENANT_A_ID"
      ;;
    /api/datasources/proxy/uid/softprobe-tempo-b/api/traces/*)
      local trace_endpoint="${endpoint##*/}"
      mock_tempo_trace_response "${trace_endpoint%%\?*}" "$TENANT_B_ID"
      ;;
    /api/datasources/proxy/uid/softprobe-prom-a/api/v1/query*) mock_signal_response prometheus softprobe-prom-a "$TENANT_A_ID" ;;
    /api/datasources/proxy/uid/softprobe-prom-b/api/v1/query*) mock_signal_response prometheus softprobe-prom-b "$TENANT_B_ID" ;;
    /api/datasources/proxy/uid/softprobe-loki-a/loki/api/v1/query*) mock_signal_response loki softprobe-loki-a "$TENANT_A_ID" ;;
    /api/datasources/proxy/uid/softprobe-loki-b/loki/api/v1/query*) mock_signal_response loki softprobe-loki-b "$TENANT_B_ID" ;;
    /api/datasources/proxy/uid/softprobe-tempo-a/api/search*) mock_signal_response tempo softprobe-tempo-a "$TENANT_A_ID" ;;
    /api/datasources/proxy/uid/softprobe-tempo-b/api/search*) mock_signal_response tempo softprobe-tempo-b "$TENANT_B_ID" ;;
    *) return 1 ;;
  esac
}

mock_response_post() {
  local endpoint="$1" payload="$2" credential="${3:-}" scope="${4:-}"
  if [[ "$endpoint" != /api/ds/query ]]; then
    return 1
  fi
  python3 - "$MOCK_FIXTURE_DIR" "$payload" "$TENANT_A_ID" "$TENANT_B_ID" "$credential" "$scope" <<'PY'
import json
import pathlib
import sys

fixture_dir, payload, tenant_a, tenant_b, credential, scope = sys.argv[1:]
request = json.loads(payload)
query = request.get("queries", [{}])[0]
uid = query.get("datasource", {}).get("uid", "")
tenant = tenant_a if uid.endswith("-a") else tenant_b
signal = "prometheus" if "prom" in uid else "loki" if "loki" in uid else "tempo"
text = json.dumps(query).lower()
if "credential_probe" in text:
    if credential == "__missing__":
        message = "missing credentials"
    elif credential == "invalid-credential":
        message = "invalid credentials"
    elif scope and scope != tenant:
        message = "mismatched tenant X-Scope-OrgID"
    else:
        message = "credential probe unexpectedly succeeded"
    print(json.dumps({"results": {"A": {"error": message, "errorSource": "auth"}}}))
    raise SystemExit
if uid.endswith("-invalid"):
    print(json.dumps({"results": {"A": {"error": "datasource authentication failed", "errorSource": "downstream"}}}))
    raise SystemExit
if "malformed_frame_probe" in text:
    print(json.dumps({"results": {"A": {"refId": "A", "frames": [{"schema": {}}]}}}))
    raise SystemExit
if "empty_result_probe" in text:
    print(json.dumps({"results": {"A": {"refId": "A", "frames": [{"schema": {"fields": [{"name": "value"}]}, "data": {"values": [[]]}}]}}}))
    raise SystemExit
if "unsupported" in text:
    names = {"prometheus": "prometheus_error_unsupported.json", "loki": "loki_error_unsupported.json", "tempo": "tempo_error_unsupported.json"}
    try:
        fixture = json.loads((pathlib.Path(fixture_dir) / names[signal]).read_text())
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(json.dumps({"results": {"A": {"error": f"mock fixture unavailable: {exc}"}}}))
        raise SystemExit(0)
    message = fixture.get("error") or fixture.get("message") or "unsupported_feature"
    print(json.dumps({"results": {"A": {"error": message, "errorSource": "downstream"}}}))
    raise SystemExit

if "label_values" in text:
    values = ["load-generator"] if "k6_http_reqs" in text else ["checkout"]
    frame = {"schema": {"name": tenant, "fields": [{"name": "value"}]}, "data": {"values": [values]}}
elif signal == "prometheus":
    frame = {"schema": {"name": tenant, "fields": [{"name": "value"}]}, "data": {"values": [[1]]}}
elif signal == "loki":
    frame = {"schema": {"name": tenant, "fields": [{"name": "line"}]}, "data": {"values": [[json.dumps({"tenant": tenant})]]}}
else:
    frame = {"schema": {"name": tenant, "fields": [{"name": "traceID"}]}, "data": {"values": [["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]]}}
print(json.dumps({"results": {"A": {"refId": "A", "frames": [frame]}}}))
PY
}

# 0 = successful 2xx response, 1 = HTTP/API failure, 2 = unreachable service.
api_request() {
  local method="$1" endpoint="$2" payload="${3:-}" artifact="$4" status curl_status
  local response_tmp stderr_tmp
  response_tmp="$(mktemp "${TMPDIR:-/tmp}/grafana-response.XXXXXX")"
  stderr_tmp="$(mktemp "${TMPDIR:-/tmp}/grafana-curl-stderr.XXXXXX")"
  : > "$artifact"
  : > "$artifact.status"
  if [[ "$MOCK_MODE" == "1" ]]; then
    if [[ "$method" == POST ]]; then
      if ! mock_response_post "$endpoint" "$payload" "${5:-}" "${6:-}" | redact > "$artifact"; then
        redact "mock response unavailable for $endpoint" > "$artifact"
        rm -f "$response_tmp" "$stderr_tmp"
        return 1
      fi
    else
      if ! mock_response "$endpoint" | redact > "$artifact"; then
        redact "mock response unavailable for $endpoint" > "$artifact"
        rm -f "$response_tmp" "$stderr_tmp"
        return 1
      fi
    fi
    printf '200\n' > "$artifact.status"
    rm -f "$response_tmp" "$stderr_tmp"
    return 0
  fi
  local credential="${5:-}" scope="${6:-}" auth_args=()
  local scope_args=()
  if [[ -n "$scope" ]]; then
    scope_args=(--header "X-Scope-OrgID: $scope")
  fi
  if [[ -z "$credential" ]]; then
    auth_args=(--user "$GRAFANA_ADMIN_USER:$GRAFANA_ADMIN_PASSWORD")
  elif [[ "$credential" != "__missing__" ]]; then
    auth_args=(--header "Authorization: Bearer $credential")
  fi
  if [[ "$method" == POST ]]; then
    if status="$(curl --silent --show-error --location \
        --connect-timeout "${CURL_CONNECT_TIMEOUT:-3}" \
        --max-time "${CURL_MAX_TIME:-15}" \
        "${auth_args[@]}" "${scope_args[@]}" \
        --header 'Accept: application/json' --header 'Content-Type: application/json' \
        --data "$payload" --output "$response_tmp" --write-out '%{http_code}' \
        "$GRAFANA_URL$endpoint" 2> "$stderr_tmp")"; then
      curl_status=0
    else
      curl_status=$?
    fi
  else
    if status="$(curl --silent --show-error --location \
        --connect-timeout "${CURL_CONNECT_TIMEOUT:-3}" \
        --max-time "${CURL_MAX_TIME:-15}" \
        "${auth_args[@]}" "${scope_args[@]}" \
        --header 'Accept: application/json' \
        --output "$response_tmp" --write-out '%{http_code}' \
        "$GRAFANA_URL$endpoint" 2> "$stderr_tmp")"; then
      curl_status=0
    else
      curl_status=$?
    fi
  fi
  redact "$(<"$response_tmp")" > "$artifact"
  if [[ -s "$stderr_tmp" ]]; then
    redact "$(<"$stderr_tmp")" > "$artifact.stderr"
  else
    rm -f "$artifact.stderr"
  fi
  rm -f "$response_tmp" "$stderr_tmp"
  if (( curl_status != 0 )); then
      printf '000\n' > "$artifact.status"
      return 2
  fi
  printf '%s\n' "$status" > "$artifact.status"
  [[ "$status" == 2* ]] || return 1
}

api_get() { api_request GET "$1" "" "$2"; }
api_post() { api_request POST "$1" "$2" "$3"; }
api_post_credentials() { api_request POST "$1" "$2" "$3" "$4" "$5"; }

http_get_artifact() {
  local url="$1" artifact="$2" status curl_status
  local response_tmp stderr_tmp
  response_tmp="$(mktemp "${TMPDIR:-/tmp}/grafana-composition-response.XXXXXX")"
  stderr_tmp="$(mktemp "${TMPDIR:-/tmp}/grafana-composition-stderr.XXXXXX")"
  if status="$(curl --silent --show-error --location \
      --connect-timeout "${CURL_CONNECT_TIMEOUT:-3}" \
      --max-time "${CURL_MAX_TIME:-15}" \
      --output "$response_tmp" --write-out '%{http_code}' \
      "$url" 2> "$stderr_tmp")"; then
    curl_status=0
  else
    curl_status=$?
  fi
  redact "$(<"$response_tmp")" > "$artifact"
  if [[ -s "$stderr_tmp" ]]; then
    redact "$(<"$stderr_tmp")" > "$artifact.stderr"
  else
    rm -f "$artifact.stderr"
  fi
  rm -f "$response_tmp" "$stderr_tmp"
  if (( curl_status != 0 )); then
    printf '000\n' > "$artifact.status"
    return 2
  fi
  printf '%s\n' "$status" > "$artifact.status"
  [[ "$status" == 2* ]]
}

check_composition_readiness() {
  local runtime="$ARTIFACT_DIR/.work/composition-softprobe-ready.json"
  local auth="$ARTIFACT_DIR/.work/composition-auth-mock-health.json"
  http_get_artifact "$SEED_SOFTPROBE_URL/ready" "$runtime" || return 1
  http_get_artifact "$GRAFANA_AUTH_MOCK_URL/__admin/health" "$auth" || return 1
  validate_json "$runtime" || return 1
  validate_json "$auth" || return 1
}

run_deterministic_seed() {
  if [[ "${GRAFANA_SEED_IN_COMPOSE:-0}" == "1" ]]; then
    local receipt="$ARTIFACT_DIR/seed-receipt.json"
    [[ -s "$receipt" ]] || {
      echo "compose Grafana seed did not write $receipt" >&2
      return 1
    }
    python3 - "$receipt" "$TENANT_A_ID" "$TENANT_B_ID" <<'PY'
import json
import pathlib
import sys

receipt = json.loads(pathlib.Path(sys.argv[1]).read_text())
if receipt.get("status") != "pass":
    raise SystemExit(f"compose seed status is not pass: {receipt.get('status')!r}")
tenants = {item.get("tenant_id"): item for item in receipt.get("tenants", [])}
for tenant_id in sys.argv[2:]:
    item = tenants.get(tenant_id)
    if not item or not all(item.get(key) is True for key in (
        "scope_provisioned", "metrics_sent", "logs_sent", "traces_sent",
        "metrics_queryable", "logs_queryable", "traces_queryable",
    )):
        raise SystemExit(f"compose seed is not queryable for tenant {tenant_id}")
PY
    return 0
  fi
  local seed_bin="${GRAFANA_SEED_BIN:-${CARGO_TARGET_DIR:-target}/debug/grafana_seed_otlp}"
  if [[ ! -x "$seed_bin" ]]; then
    DUCKDB_DOWNLOAD_LIB=1 cargo build --quiet --bin grafana_seed_otlp || return 1
  fi
  seed_bin="${GRAFANA_SEED_BIN:-${CARGO_TARGET_DIR:-target}/debug/grafana_seed_otlp}"
  GRAFANA_SEED_SOFTPROBE_URL="$SEED_SOFTPROBE_URL" \
    GRAFANA_SEED_RECEIPT="$ARTIFACT_DIR/seed-receipt.json" \
    SOFTPROBE_TENANT_A_ID="$TENANT_A_ID" \
    SOFTPROBE_TENANT_B_ID="$TENANT_B_ID" \
    SOFTPROBE_ADMIN_API_KEY="${SOFTPROBE_ADMIN_API_KEY:-grafana-phase4-admin}" \
    SOFTPROBE_TENANT_A_API_KEY="${GRAFANA_TEST_TENANT_A_API_KEY:-grafana-phase4-tenant-a}" \
    SOFTPROBE_TENANT_B_API_KEY="${GRAFANA_TEST_TENANT_B_API_KEY:-grafana-phase4-tenant-b}" \
    "$seed_bin"
}

write_request_artifact() {
  local path="$1" method="$2" endpoint="$3" payload="${4:-}"
  python3 - "$method" "$endpoint" "$payload" <<'PY' | redact > "$path"
import json
import sys
method, endpoint, payload = sys.argv[1:]
request = {"method": method, "endpoint": endpoint}
if payload:
    try:
        request["payload"] = json.loads(payload)
    except json.JSONDecodeError:
        request["payload"] = payload
print(json.dumps(request, indent=2, sort_keys=True))
PY
}

validate_json() {
  python3 - "$1" <<'PY'
import json
import pathlib
import sys
json.loads(pathlib.Path(sys.argv[1]).read_text())
PY
}

validate_uid_list() {
  python3 - "$1" "${@:2}" <<'PY'
import json
import pathlib
import sys
items = json.loads(pathlib.Path(sys.argv[1]).read_text())
expected = set(sys.argv[2:])
actual = {item.get("uid") for item in items if isinstance(item, dict)}
missing = sorted(expected - actual)
if missing:
    raise SystemExit("missing UIDs: " + ", ".join(missing))
PY
}

validate_health() {
  python3 - "$1" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
if obj.get("database") != "ok":
    raise SystemExit("Grafana database is not healthy")
PY
}

validate_datasource_config() {
  python3 - "$1" "$2" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
uid = sys.argv[2]
if obj.get("uid") != uid:
    raise SystemExit(f"datasource UID mismatch: expected {uid}")
json_data = obj.get("jsonData", {})
secure = obj.get("secureJsonFields", {})
if json_data.get("httpHeaderName1") != "Authorization" or not secure.get("httpHeaderValue1"):
    raise SystemExit(f"{uid} is missing configured Authorization credentials")
if uid.endswith(("-a", "-b")):
    if json_data.get("httpHeaderName2") != "X-Scope-OrgID" or not secure.get("httpHeaderValue2"):
        raise SystemExit(f"{uid} is missing configured X-Scope-OrgID credentials")
PY
}

validate_native_health() {
  python3 - "$1" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
status = str(obj.get("status", "")).lower()
if status not in {"ok", "success", "healthy"}:
    raise SystemExit(f"native datasource health is not healthy: {obj}")
PY
}

validate_dashboard_refs() {
  python3 - "$1" "$2" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text()).get("dashboard", {})
uid = sys.argv[2]
if obj.get("uid") != uid:
    raise SystemExit(f"dashboard UID mismatch: expected {uid}")
allowed = {"softprobe-prom", "softprobe-prom-a", "softprobe-prom-b", "softprobe-loki-a", "softprobe-loki-b", "softprobe-tempo-a", "softprobe-tempo-b"}
refs = []
def visit(panel):
    source = panel.get("datasource")
    if isinstance(source, dict) and source.get("uid"):
        refs.append(source["uid"])
    elif isinstance(source, str) and source:
        refs.append(source)
    for target in panel.get("targets", []):
        source = target.get("datasource")
        if isinstance(source, dict) and source.get("uid"):
            refs.append(source["uid"])
        elif isinstance(source, str) and source:
            refs.append(source)
    for child in panel.get("panels", []):
        visit(child)
for panel in obj.get("panels", []):
    visit(panel)
if not refs:
    raise SystemExit(f"{uid} has no panel datasource references")
unknown = sorted(set(refs) - allowed)
if unknown:
    raise SystemExit(f"{uid} has unknown datasource refs: {', '.join(unknown)}")
PY
}

validate_dashboard_round_trip() {
  python3 - "$1" "$2" "$GRAFANA_DASHBOARD_DIR" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
uid = sys.argv[2]
fixture = json.loads((pathlib.Path(sys.argv[3]) / f"{uid}.json").read_text())
dashboard = obj.get("dashboard", {})
meta = obj.get("meta", {})
if dashboard.get("uid") != uid:
    raise SystemExit(f"dashboard round-trip UID mismatch: expected {uid}")
for key in ("uid", "version", "refresh", "time", "templating"):
    if dashboard.get(key) != fixture.get(key):
        raise SystemExit(f"{uid} dashboard {key} differs from checked-in fixture")
if not isinstance(dashboard.get("version"), int) or dashboard["version"] < 1:
    raise SystemExit(f"{uid} dashboard version is missing or invalid")
if meta.get("folderTitle") != "Softprobe" or not meta.get("folderUid"):
    raise SystemExit(f"{uid} dashboard is not provisioned in the Softprobe folder")
variables = dashboard.get("templating", {}).get("list")
if not isinstance(variables, list):
    raise SystemExit(f"{uid} dashboard templating list is malformed")
for variable in variables:
    if not variable.get("name") or variable.get("type") not in {"query", "custom", "constant", "interval"}:
        raise SystemExit(f"{uid} contains an invalid dashboard variable")
    if variable.get("type") == "query" and not variable.get("query"):
        raise SystemExit(f"{uid} query variable has no query")
PY
}

validate_panel_response() {
  python3 - "$1" "$2" "$3" <<'PY'
import json
import pathlib
import sys
path, signal, uid = sys.argv[1:]
obj = json.loads(pathlib.Path(path).read_text())
result = obj.get("results", {}).get("A", {})
if result.get("error"):
    raise SystemExit(f"{uid} {signal} panel returned an error: {result['error']}")
frames = result.get("frames") or result.get("data")
if not frames:
    raise SystemExit(f"{uid} {signal} panel returned no data frames")
def has_values(value):
    if isinstance(value, dict):
        values = value.get("values")
        if isinstance(values, list) and any(item not in ([], None, "") for item in values):
            return True
        return any(has_values(item) for item in value.values())
    if isinstance(value, list):
        return any(has_values(item) for item in value)
    return False
if not has_values(frames):
    raise SystemExit(f"{uid} {signal} panel returned empty data")
PY
}

validate_tempo_trace_response() {
  python3 - "$1" "$2" "$3" "$4" <<'PY'
import json
import pathlib
import re
import sys
import base64
path, trace_id, expected, other = sys.argv[1:]
obj = json.loads(pathlib.Path(path).read_text())
text = json.dumps(obj, sort_keys=True)
if not re.fullmatch(r"[0-9a-fA-F]{32}", trace_id):
    raise SystemExit("Tempo lookup trace ID must be exactly 32 hexadecimal characters")
requested_trace = bytes.fromhex(trace_id)
if expected and expected not in text:
    raise SystemExit(f"Tempo trace response is missing tenant marker {expected}")
if other and other in text:
    raise SystemExit("Tempo trace response contains the other tenant marker")
groups = obj.get("batches") or obj.get("resourceSpans")
if not isinstance(groups, list) or not groups:
    raise SystemExit("Tempo trace response contains no spans")

def valid_id(value, byte_length):
    if not isinstance(value, str) or not value or re.fullmatch(r"[0-9a-fA-F]+", value):
        return None
    try:
        decoded = base64.b64decode(value, validate=True)
    except Exception:
        return None
    if len(decoded) != byte_length or base64.b64encode(decoded).decode("ascii") != value:
        return None
    return decoded

def timestamp(value):
    return isinstance(value, (int, str)) and bool(re.fullmatch(r"[0-9]+", str(value)))

matched = 0
for group in groups:
    resource = group.get("resource")
    attrs = resource.get("attributes") if isinstance(resource, dict) else None
    if not isinstance(attrs, list) or expected not in json.dumps(attrs, sort_keys=True):
        raise SystemExit("Tempo trace response is missing the expected resource tenant attribute")
    scopes = group.get("scopeSpans") or group.get("scope_spans")
    if not isinstance(scopes, list) or not scopes:
        raise SystemExit("Tempo trace response is missing ScopeSpans groups")
    for scope_group in scopes:
        scope = scope_group.get("scope")
        if not isinstance(scope, dict) or not scope.get("name"):
            raise SystemExit("Tempo trace response is missing instrumentation scope")
        spans = scope_group.get("spans")
        if not isinstance(spans, list) or not spans:
            raise SystemExit("Tempo trace response is missing spans in a ScopeSpans group")
        for span in spans:
            span_trace_id = span.get("traceId", span.get("traceID"))
            decoded_trace_id = valid_id(span_trace_id, 16)
            if decoded_trace_id is None:
                raise SystemExit("Tempo trace response has a malformed/noncanonical trace ID")
            if decoded_trace_id != requested_trace:
                continue
            matched += 1
            span_id = span.get("spanId", span.get("spanID"))
            if valid_id(span_id, 8) is None:
                raise SystemExit("Tempo trace response has a malformed span ID")
            parent = span.get("parentSpanId", span.get("parentSpanID"))
            if parent is not None and parent != "" and valid_id(parent, 8) is None:
                raise SystemExit("Tempo trace response has a malformed parent span ID")
            start = span.get("startTimeUnixNano")
            end = span.get("endTimeUnixNano")
            if not timestamp(start) or not timestamp(end) or int(end) <= int(start):
                raise SystemExit("Tempo trace response has invalid nanosecond timing")
            status = span.get("status")
            if status is not None:
                if not isinstance(status, dict) or status.get("code") not in {"STATUS_CODE_UNSET", "STATUS_CODE_OK", "STATUS_CODE_ERROR"}:
                    raise SystemExit("Tempo trace response has an invalid status enum")
            events = span.get("events")
            if events is not None:
                if not isinstance(events, list):
                    raise SystemExit("Tempo trace response events are not a list")
                for event in events:
                    if not isinstance(event, dict) or not event.get("name") or not timestamp(event.get("timeUnixNano")):
                        raise SystemExit("Tempo trace response has an invalid event")
            links = span.get("links")
            if links is not None:
                if not isinstance(links, list):
                    raise SystemExit("Tempo trace response links are not a list")
                for link in links:
                    if not isinstance(link, dict) or valid_id(link.get("traceId", link.get("traceID")), 16) is None or valid_id(link.get("spanId", link.get("spanID")), 8) is None:
                        raise SystemExit("Tempo trace response has an invalid link")
if matched == 0:
    raise SystemExit("Tempo trace response contains no span for the requested trace")
PY
}

validate_cross_signal_links() {
  python3 - "$1" "$2" "$3" "$4" "$5" <<'PY'
import json
import pathlib
import sys
cross, loki_a, loki_b, tempo_a, tempo_b = map(pathlib.Path, sys.argv[1:])
cross_obj = json.loads(cross.read_text()).get("dashboard", {})
refs = set()
for panel in cross_obj.get("panels", []):
    for item in [panel.get("datasource"), *[target.get("datasource") for target in panel.get("targets", [])]]:
        if isinstance(item, dict) and item.get("uid"):
            refs.add(item["uid"])
if not {"softprobe-prom-a", "softprobe-loki-a", "softprobe-tempo-a"}.issubset(refs):
    raise SystemExit("cross-signal dashboard is missing a Prometheus/Loki/Tempo panel reference")
for path, expected in ((loki_a, "softprobe-tempo-a"), (loki_b, "softprobe-tempo-b")):
    fields = json.loads(path.read_text()).get("jsonData", {}).get("derivedFields", [])
    if not any(field.get("datasourceUid") == expected for field in fields):
        raise SystemExit(f"{path.name} is missing a trace-to-Tempo derived field for {expected}")
for path, expected in ((tempo_a, "softprobe-loki-a"), (tempo_b, "softprobe-loki-b")):
    target = json.loads(path.read_text()).get("jsonData", {}).get("tracesToLogsV2", {}).get("datasourceUid")
    if target != expected:
        raise SystemExit(f"{path.name} trace-to-logs target is {target!r}, expected {expected!r}")
PY
}

resolve_cross_signal_links() {
  local tenant uid other trace_id log_trace_id loki_artifact trace_artifact status loki_endpoint trace_endpoint
  for tenant in a b; do
    if [[ "$tenant" == "a" ]]; then
      uid="softprobe-tempo-a"; other="$TENANT_B_ID"
    else
      uid="softprobe-tempo-b"; other="$TENANT_A_ID"
    fi
    trace_id="$(python3 - "$ARTIFACT_DIR/.work/G6-${tenant}-direct.json" <<'PY'
import json
import pathlib
import re
import sys
text = pathlib.Path(sys.argv[1]).read_text()
match = re.search(r"[0-9a-fA-F]{32}", json.dumps(json.loads(text), sort_keys=True))
if not match:
    raise SystemExit("cross-signal trace-to-log source has no trace ID")
print(match.group(0))
PY
    )"
    loki_artifact="$ARTIFACT_DIR/.work/G7-${tenant}-trace-to-log.json"
    loki_endpoint="/api/datasources/proxy/uid/softprobe-loki-${tenant}/loki/api/v1/query_range?query=%7Bservice_name%3D%22checkout%22%7D%20%7C%3D%20%22error%22&start=$CROSS_START_NS&end=$CROSS_END_NS&limit=10"
    [[ "$loki_endpoint" == *"start=$CROSS_START_NS"* && "$loki_endpoint" == *"end=$CROSS_END_NS"* ]] || return 1
    write_request_artifact "$ARTIFACT_DIR/.work/G7-${tenant}-trace-to-log.request.json" GET "$loki_endpoint"
    if api_get "$loki_endpoint" "$loki_artifact"; then
      :
    else
      status=$?
      return "$status"
    fi
    validate_json "$loki_artifact" || return 1
    validate_signal_response "$loki_artifact" loki "$([[ "$tenant" == "a" ]] && printf '%s' "$TENANT_A_ID" || printf '%s' "$TENANT_B_ID")" "$other" "softprobe-loki-${tenant}" || return 1

    log_trace_id="$(python3 - "$loki_artifact" <<'PY'
import json
import pathlib
import re
import sys
text = pathlib.Path(sys.argv[1]).read_text()
obj = json.loads(text)
match = re.search(r"[0-9a-fA-F]{32}", json.dumps(obj, sort_keys=True))
if not match:
    raise SystemExit("cross-signal log-to-trace source has no trace ID")
print(match.group(0))
PY
    )"
    trace_artifact="$ARTIFACT_DIR/.work/G7-${tenant}-log-to-trace.json"
    trace_endpoint="/api/datasources/proxy/uid/$uid/api/traces/$log_trace_id?start=$TRACE_START_S&end=$TRACE_END_S"
    [[ "$trace_endpoint" == *"/$log_trace_id?"* && "$trace_endpoint" == *"start=$TRACE_START_S"* && "$trace_endpoint" == *"end=$TRACE_END_S"* ]] || return 1
    write_request_artifact "$ARTIFACT_DIR/.work/G7-${tenant}-log-to-trace.request.json" GET "$trace_endpoint"
    if api_get "$trace_endpoint" "$trace_artifact"; then
      :
    else
      status=$?
      return "$status"
    fi
    validate_json "$trace_artifact" || return 1
    validate_tempo_trace_response "$trace_artifact" "$log_trace_id" "$([[ "$tenant" == "a" ]] && printf '%s' "$TENANT_A_ID" || printf '%s' "$TENANT_B_ID")" "$other" || return 1
    [[ "$trace_id" == "$log_trace_id" ]] || return 1
  done
}

validate_signal_response() {
  python3 - "$1" "$2" "$3" "$4" "$5" "$MOCK_MODE" <<'PY'
import json
import pathlib
import sys
path, signal, expected, other, uid, mock = sys.argv[1:]
obj = json.loads(pathlib.Path(path).read_text())
text = json.dumps(obj, sort_keys=True)
if other and other in text:
    raise SystemExit(f"{uid} response contains the other tenant marker")
if mock == "1" and expected not in text:
    raise SystemExit(f"mock {uid} response is missing its tenant marker")
if signal == "prometheus":
    if obj.get("status") != "success" or obj.get("data", {}).get("resultType") not in {"vector", "matrix", "scalar", "string"}:
        raise SystemExit(f"{uid} is not a Prometheus success response")
    if not obj.get("data", {}).get("result"):
        raise SystemExit(f"{uid} Prometheus response has empty result")
elif signal == "loki":
    if obj.get("status") != "success" or obj.get("data", {}).get("resultType") != "streams":
        raise SystemExit(f"{uid} is not a Loki streams response")
    if not obj.get("data", {}).get("result"):
        raise SystemExit(f"{uid} Loki response has empty result")
else:
    traces = obj.get("traces", obj.get("batches"))
    if not isinstance(traces, list) or not traces:
        raise SystemExit(f"{uid} Tempo search response is empty or malformed")
PY
}

validate_explore_response() {
  python3 - "$1" "$2" "$3" "$4" "$MOCK_MODE" <<'PY'
import json
import pathlib
import sys
path, expected, other, uid, mock = sys.argv[1:]
obj = json.loads(pathlib.Path(path).read_text())
text = json.dumps(obj, sort_keys=True)
if other and other in text:
    raise SystemExit(f"Explore response for {uid} contains the other tenant marker")
if mock == "1" and expected not in text:
    raise SystemExit(f"mock Explore response for {uid} is missing its tenant marker")
result = obj.get("results", {}).get("A", {})
if not isinstance(obj.get("results"), dict) or result.get("error"):
    raise SystemExit(f"Explore query for {uid} returned an error")
if not result.get("frames") and not result.get("data"):
    raise SystemExit(f"Explore query for {uid} returned no frames/data")
def has_values(value):
    if isinstance(value, dict):
        values = value.get("values")
        if isinstance(values, list) and any(item not in ([], None, "") for item in values):
            return True
        return any(has_values(item) for item in value.values())
    if isinstance(value, list):
        return any(has_values(item) for item in value)
    return False
if not has_values(result.get("frames") or result.get("data")):
    raise SystemExit(f"Explore query for {uid} returned empty data")
PY
}

validate_error_response() {
  python3 - "$1" "${2:-}" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text())
expected = sys.argv[2].lower()
def has_error(item):
    if isinstance(item, dict):
        return any(key in item for key in ("error", "errorType", "message", "softprobe_code")) or any(has_error(v) for v in item.values())
    if isinstance(item, list):
        return any(has_error(v) for v in item)
    return False
if not has_error(obj):
    raise SystemExit("expected an explicit unsupported/error response")
if expected and expected not in json.dumps(obj, sort_keys=True).lower():
    raise SystemExit(f"error response does not identify {expected}")
PY
}

query_payload() {
  local signal="$1" uid="$2" query="$3"
  python3 - "$signal" "$uid" "$query" <<'PY'
import json
import sys
signal, uid, query = sys.argv[1:]
item = {"refId": "A", "datasource": {"type": signal, "uid": uid}, "intervalMs": 15000, "maxDataPoints": 1000}
if signal == "prometheus":
    item["expr"] = query
elif signal == "loki":
    item.update({"expr": query, "queryType": "range"})
else:
    item.update({"query": query, "queryType": "traceql"})
print(json.dumps({"from": "1700000000000", "to": "1700000060000", "queries": [item]}))
PY
}

validate_repeat_response() {
  python3 - "$1" "$2" <<'PY'
import json
import pathlib
import sys
first, second = (json.loads(pathlib.Path(path).read_text()) for path in sys.argv[1:])
if first != second:
    raise SystemExit("repeated Grafana query returned different data")
PY
}

panel_payload() {
  python3 - "$1" <<'PY'
import json
import sys
target = json.loads(sys.argv[1])
source = target.get("datasource", {})
signal = source.get("type", "") if isinstance(source, dict) else ""
if signal == "loki":
    target.setdefault("queryType", "range")
elif signal == "tempo":
    target.setdefault("queryType", "traceql")
print(json.dumps({"from": "now-15m", "to": "now", "queries": [target]}))
PY
}

variable_payload() {
  python3 - "$1" <<'PY'
import json
import sys
variable = json.loads(sys.argv[1])
item = {
    "refId": "A",
    "datasource": variable["datasource"],
    "intervalMs": 15000,
    "maxDataPoints": 1000,
    "expr": variable["query"],
    "queryType": "variable",
}
print(json.dumps({"from": "now-15m", "to": "now", "queries": [item]}))
PY
}

validate_variable_response() {
  python3 - "$1" "$2" "$3" <<'PY'
import json
import pathlib
import sys
path, name, current = sys.argv[1:]
obj = json.loads(pathlib.Path(path).read_text())
result = obj.get("results", {}).get("A", {})
if result.get("error"):
    raise SystemExit(f"dashboard variable {name} returned an error: {result['error']}")
frames = result.get("frames") or result.get("data")
if not frames:
    raise SystemExit(f"dashboard variable {name} returned no options")
if current and current not in json.dumps(frames, sort_keys=True):
    raise SystemExit(f"dashboard variable {name} did not preserve selected value")
PY
}

check_dashboard_variables() {
  local detail="$1" uid="$2" variable name current payload artifact status
  VARIABLE_BUNDLE_ARGS=()
  while IFS= read -r variable; do
    [[ -n "$variable" ]] || continue
    name="$(python3 - "$variable" <<'PY'
import json, sys
print(json.loads(sys.argv[1])["name"])
PY
)"
    current="$(python3 - "$variable" <<'PY'
import json, sys
value = json.loads(sys.argv[1]).get("current", {})
print(value.get("value", "") if isinstance(value, dict) else "")
PY
)"
    payload="$(variable_payload "$variable")"
    artifact="$ARTIFACT_DIR/.work/G3-${uid}-variable-${name}.json"
    if api_post /api/ds/query "$payload" "$artifact"; then
      :
    else
      status=$?
      return "$status"
    fi
    validate_json "$artifact" || return 1
    validate_variable_response "$artifact" "$name" "$current" || return 1
    VARIABLE_BUNDLE_ARGS+=("variable_${name}=$artifact")
  done < <(python3 - "$detail" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text()).get("dashboard", {})
for variable in obj.get("templating", {}).get("list", []):
    if variable.get("type") == "query":
        print(json.dumps(variable, sort_keys=True))
PY
)
}

check_dashboard_panels() {
  local detail="$1" uid="$2" panel_id panel_type target payload artifact status signal target_uid
  while IFS=$'\t' read -r panel_id panel_type target; do
    [[ -n "$target" ]] || continue
    signal="$(python3 - "$target" <<'PY'
import json, sys
print(json.loads(sys.argv[1]).get("datasource", {}).get("type", "unknown"))
PY
)"
    target_uid="$(python3 - "$target" <<'PY'
import json, sys
print(json.loads(sys.argv[1]).get("datasource", {}).get("uid", "unknown"))
PY
)"
    payload="$(panel_payload "$target")"
    artifact="$ARTIFACT_DIR/.work/G3-${uid}-panel-${panel_id}.json"
    if api_post /api/ds/query "$payload" "$artifact"; then
      :
    else
      status=$?
      return "$status"
    fi
    validate_json "$artifact" || return 1
    validate_panel_response "$artifact" "$signal" "$target_uid" || return 1
    VARIABLE_BUNDLE_ARGS+=("panel_${panel_id}=$artifact")
  done < <(python3 - "$detail" <<'PY'
import json
import pathlib
import sys
obj = json.loads(pathlib.Path(sys.argv[1]).read_text()).get("dashboard", {})
def emit(panel):
    for target in panel.get("targets", []):
        print("\t".join((str(panel.get("id", "unknown")), panel.get("type", "unknown"), json.dumps(target, sort_keys=True))))
    for child in panel.get("panels", []):
        emit(child)
for panel in obj.get("panels", []):
    emit(panel)
PY
)
}

check_health() {
  local artifact="$ARTIFACT_DIR/.work/G1-health.json" last_status=2 attempt
  for attempt in $(seq 1 "${HEALTH_RETRIES:-30}"); do
    if api_get /api/health "$artifact"; then
      validate_json "$artifact" && validate_health "$artifact" || return 1
      write_case_bundle G1 "health=$artifact"
      record_case G1 pass
      return 0
    else
      last_status=$?
    fi
    [[ "$MOCK_MODE" == "1" ]] || sleep "${HEALTH_RETRY_DELAY:-1}"
  done
  return "$last_status"
}

check_datasources() {
  local list="$ARTIFACT_DIR/.work/G2-datasources.json" status uid
  if ! api_get /api/datasources "$list"; then
    return $?
  fi
  validate_uid_list "$list" "${DATASOURCE_UIDS[@]}" || return 1
  local bundle_args=("list=$list")
  for uid in "${DATASOURCE_UIDS[@]}"; do
    local detail="$ARTIFACT_DIR/.work/G2-${uid}.json"
    if ! api_get "/api/datasources/uid/$uid" "$detail"; then
      status=$?
      return "$status"
    fi
    validate_json "$detail" || return 1
    validate_datasource_config "$detail" "$uid" || return 1
    bundle_args+=("$uid=$detail")
    local health="$ARTIFACT_DIR/.work/G2-${uid}-health.json"
    if api_get "/api/datasources/uid/$uid/health" "$health"; then
      :
    else
      status=$?
      return "$status"
    fi
    validate_json "$health" || return 1
    validate_native_health "$health" || return 1
    bundle_args+=("${uid}_health=$health")
  done
  write_case_bundle G2 "${bundle_args[@]}"
  record_case G2 pass
}

check_dashboards() {
  local list="$ARTIFACT_DIR/.work/G3-dashboards.json" status uid
  if ! api_get "/api/search?type=dash-db" "$list"; then
    return $?
  fi
  validate_uid_list "$list" "${DASHBOARD_UIDS[@]}" || return 1
  local bundle_args=("list=$list")
  for uid in "${DASHBOARD_UIDS[@]}"; do
    local detail="$ARTIFACT_DIR/.work/G3-${uid}.json"
    if ! api_get "/api/dashboards/uid/$uid" "$detail"; then
      status=$?
      return "$status"
    fi
    validate_json "$detail" || return 1
    validate_dashboard_round_trip "$detail" "$uid" || return 1
    validate_dashboard_refs "$detail" "$uid" || return 1
    if [[ "$GRAFANA_CHECK_DASHBOARD_QUERIES" == "1" ]]; then
      check_dashboard_variables "$detail" "$uid" || return $?
      check_dashboard_panels "$detail" "$uid" || return $?
    fi
    bundle_args+=("$uid=$detail")
    if ((${#VARIABLE_BUNDLE_ARGS[@]})); then
      bundle_args+=("${VARIABLE_BUNDLE_ARGS[@]}")
    fi
  done
  write_case_bundle G3 "${bundle_args[@]}"
  record_case G3 pass
}

run_signal_case() {
  local case_id="$1" signal="$2" query="$3" status tenant uid other expected direct explore payload endpoint
  local bundle_args=()
  for tenant in a b; do
    if [[ "$tenant" == a ]]; then
      uid="softprobe-$signal-a"; expected="$TENANT_A_ID"; other="$TENANT_B_ID"
    else
      uid="softprobe-$signal-b"; expected="$TENANT_B_ID"; other="$TENANT_A_ID"
    fi
    direct="$ARTIFACT_DIR/.work/${case_id}-${tenant}-direct.json"
    case "$signal" in
      prometheus) endpoint="/api/datasources/proxy/uid/softprobe-prom-${tenant}/api/v1/query?query=grafana_phase4_requests_total&time=1700000030" ;;
      loki) endpoint="/api/datasources/proxy/uid/$uid/loki/api/v1/query_range?query=%7Bservice_name%3D%22checkout%22%7D%20%7C%3D%20%22error%22&start=$CROSS_START_NS&end=$CROSS_END_NS&limit=10&direction=forward" ;;
      tempo) endpoint="/api/datasources/proxy/uid/$uid/api/search?limit=20&start=$TRACE_START_S&end=$TRACE_END_S" ;;
      *) return 1 ;;
    esac
    write_request_artifact "$ARTIFACT_DIR/.work/${case_id}-${tenant}-direct.request.json" GET "$endpoint"
    if api_get "$endpoint" "$direct"; then
      :
    else
      status=$?
      if (( status == 2 )); then
        write_case_bundle "$case_id" "${tenant}_direct=$direct"
        record_case "$case_id" environment_skip "${signal} datasource service unavailable"
        SKIPPED=1
        [[ "$MOCK_MODE" == "1" ]] && return 0
        return 2
      fi
      return "$status"
    fi
    validate_json "$direct" || return 1
    validate_signal_response "$direct" "$signal" "$expected" "$other" "$uid" || return 1
    bundle_args+=("${tenant}_direct=$direct" "${tenant}_direct_request=$ARTIFACT_DIR/.work/${case_id}-${tenant}-direct.request.json")
    if [[ "$signal" == "tempo" ]]; then
      local trace_id trace_artifact
      trace_id="$(python3 - "$direct" <<'PY'
import json
import pathlib
import re
import sys
text = pathlib.Path(sys.argv[1]).read_text()
obj = json.loads(text)
match = re.search(r"[0-9a-fA-F]{32}", json.dumps(obj, sort_keys=True))
if not match:
    raise SystemExit("Tempo search returned no trace ID")
print(match.group(0))
PY
)"
      trace_artifact="$ARTIFACT_DIR/.work/${case_id}-${tenant}-trace.json"
      local trace_endpoint="/api/datasources/proxy/uid/$uid/api/traces/$trace_id?start=$TRACE_START_S&end=$TRACE_END_S"
      write_request_artifact "$ARTIFACT_DIR/.work/${case_id}-${tenant}-trace.request.json" GET "$trace_endpoint"
      if api_get "$trace_endpoint" "$trace_artifact"; then
        :
      else
        status=$?
        return "$status"
      fi
      validate_json "$trace_artifact" || return 1
      validate_tempo_trace_response "$trace_artifact" "$trace_id" "$expected" "$other" || return 1
      bundle_args+=("${tenant}_trace=$trace_artifact" "${tenant}_trace_request=$ARTIFACT_DIR/.work/${case_id}-${tenant}-trace.request.json")
    fi

    payload="$(query_payload "$signal" "$uid" "$query")"
    explore="$ARTIFACT_DIR/.work/${case_id}-${tenant}-explore.json"
    write_request_artifact "$ARTIFACT_DIR/.work/${case_id}-${tenant}-explore.request.json" POST /api/ds/query "$payload"
    if api_post /api/ds/query "$payload" "$explore"; then
      :
    else
      status=$?
      if (( status == 2 )); then
        write_case_bundle "$case_id" "${bundle_args[@]}" "${tenant}_explore=$explore"
        record_case "$case_id" environment_skip "Grafana /api/ds/query unavailable"
        SKIPPED=1
        [[ "$MOCK_MODE" == "1" ]] && return 0
        return 2
      fi
      return "$status"
    fi
    validate_json "$explore" || return 1
    validate_explore_response "$explore" "$expected" "$other" "$uid" || return 1
    local repeat="$ARTIFACT_DIR/.work/${case_id}-${tenant}-repeat.json"
    if api_post /api/ds/query "$payload" "$repeat"; then
      :
    else
      status=$?
      return "$status"
    fi
    validate_json "$repeat" || return 1
    validate_explore_response "$repeat" "$expected" "$other" "$uid" || return 1
    validate_repeat_response "$explore" "$repeat" || return 1
    bundle_args+=("${tenant}_explore=$explore" "${tenant}_repeat=$repeat" "${tenant}_explore_request=$ARTIFACT_DIR/.work/${case_id}-${tenant}-explore.request.json")
  done
  write_case_bundle "$case_id" "${bundle_args[@]}"
  record_case "$case_id" pass
}

check_cross_signal() {
  local cross="$ARTIFACT_DIR/.work/G7-cross.json" loki_a="$ARTIFACT_DIR/.work/G7-loki-a.json" loki_b="$ARTIFACT_DIR/.work/G7-loki-b.json" tempo_a="$ARTIFACT_DIR/.work/G7-tempo-a.json" tempo_b="$ARTIFACT_DIR/.work/G7-tempo-b.json"
  local files=("/api/dashboards/uid/softprobe-cross-signal=$cross" "/api/datasources/uid/softprobe-loki-a=$loki_a" "/api/datasources/uid/softprobe-loki-b=$loki_b" "/api/datasources/uid/softprobe-tempo-a=$tempo_a" "/api/datasources/uid/softprobe-tempo-b=$tempo_b") item endpoint path status
  for item in "${files[@]}"; do
    endpoint="${item%%=*}"; path="${item#*=}"
    if api_get "$endpoint" "$path"; then
      :
    else
      status=$?
      return "$status"
    fi
  done
  validate_cross_signal_links "$cross" "$loki_a" "$loki_b" "$tempo_a" "$tempo_b" || return 1
  resolve_cross_signal_links || return $?
  write_case_bundle G7 "cross=$cross" "loki_a=$loki_a" "loki_b=$loki_b" "tempo_a=$tempo_a" "tempo_b=$tempo_b" \
    "a_trace_to_log_request=$ARTIFACT_DIR/.work/G7-a-trace-to-log.request.json" \
    "a_log_to_trace_request=$ARTIFACT_DIR/.work/G7-a-log-to-trace.request.json" \
    "b_trace_to_log_request=$ARTIFACT_DIR/.work/G7-b-trace-to-log.request.json" \
    "b_log_to_trace_request=$ARTIFACT_DIR/.work/G7-b-log-to-trace.request.json" \
    "a_trace_to_log=$ARTIFACT_DIR/.work/G7-a-trace-to-log.json" \
    "a_log_to_trace=$ARTIFACT_DIR/.work/G7-a-log-to-trace.json" \
    "b_trace_to_log=$ARTIFACT_DIR/.work/G7-b-trace-to-log.json" \
    "b_log_to_trace=$ARTIFACT_DIR/.work/G7-b-log-to-trace.json"
  record_case G7 pass
}

check_panel_rejection() {
  local signal="$1" tenant="$2" probe="$3" uid="softprobe-$signal-$tenant"
  local payload artifact request_artifact status
  payload="$(query_payload "$signal" "$uid" "${probe}_probe")"
  artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.json"
  request_artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.request.json"
  write_request_artifact "$request_artifact" POST /api/ds/query "$payload"
  if api_post /api/ds/query "$payload" "$artifact"; then
    :
  else
    status=$?
    return "$status"
  fi
  validate_json "$artifact" || return 1
  if validate_panel_response "$artifact" "$signal" "$uid" >/dev/null 2>&1; then
    return 1
  fi
}

validate_credential_rejection() {
  local artifact="$1" expected_error="$2" code
  code="$(tr -d '[:space:]' < "$artifact.status")"
  if [[ "$code" == 2* ]]; then
    validate_error_response "$artifact" "$expected_error" || return 1
  elif [[ "$code" != 401 && "$code" != 403 ]]; then
    echo "credential probe returned an unexpected HTTP status: $code" >&2
    return 1
  fi
  validate_json "$artifact" 2>/dev/null || {
    [[ "$code" == 401 || "$code" == 403 ]] || return 1
  }
}

check_errors() {
  local case_id=G8 signal tenant uid other payload artifact request_artifact status
  local credential scope expected_scope valid_credential expected_error probe
  local bundle_args=()
  for signal in prometheus loki tempo; do
    for tenant in a b; do
      if [[ "$tenant" == a ]]; then uid="softprobe-$signal-a"; other="$TENANT_B_ID"; else uid="softprobe-$signal-b"; other="$TENANT_A_ID"; fi
      case "$signal" in
        prometheus) payload="$(query_payload "$signal" "$uid" "unsupported_feature_probe()")" ;;
        loki) payload="$(query_payload "$signal" "$uid" '{service_name="checkout"} | unsupported_feature_probe')" ;;
        tempo) payload="$(query_payload "$signal" "$uid" '{ .unsupported_feature_probe }')" ;;
      esac
      artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}.json"
      if api_post /api/ds/query "$payload" "$artifact"; then
        :
      else
        status=$?
        if (( status == 2 )); then
          write_case_bundle G8 "${bundle_args[@]}" 2>/dev/null || true
          record_case G8 environment_skip "Grafana error/query API unavailable"
          SKIPPED=1
          [[ "$MOCK_MODE" == "1" ]] && return 0
          return 2
        fi
      fi
      validate_json "$artifact" || return 1
      validate_error_response "$artifact" unsupported_feature || return 1
      if grep -Fq "$other" "$artifact"; then
        return 1
      fi
      bundle_args+=("${signal}_${tenant}=$artifact")
    done
  done
  for signal in prometheus loki tempo; do
    uid="softprobe-$signal-invalid"
    payload="$(query_payload "$signal" "$uid" "invalid_datasource_auth_probe")"
    artifact="$ARTIFACT_DIR/.work/G8-${signal}-invalid-datasource.json"
    write_request_artifact "$ARTIFACT_DIR/.work/G8-${signal}-invalid-datasource.request.json" POST /api/ds/query "$payload"
    if api_post /api/ds/query "$payload" "$artifact"; then
      :
    else
      status=$?
      if (( status == 2 )); then
        return 2
      fi
    fi
    validate_json "$artifact" || return 1
    validate_error_response "$artifact" datasource || return 1
    bundle_args+=("${signal}_invalid=$artifact" "${signal}_invalid_request=$ARTIFACT_DIR/.work/G8-${signal}-invalid-datasource.request.json")
  done
  for signal in prometheus loki tempo; do
    for tenant in a b; do
      if [[ "$tenant" == a ]]; then
        uid="softprobe-$signal-a"
        other="$TENANT_B_ID"
        expected_scope="$TENANT_A_ID"
        valid_credential="${GRAFANA_TEST_TENANT_A_API_KEY:-grafana-phase4-tenant-a}"
      else
        uid="softprobe-$signal-b"
        other="$TENANT_A_ID"
        expected_scope="$TENANT_B_ID"
        valid_credential="${GRAFANA_TEST_TENANT_B_API_KEY:-grafana-phase4-tenant-b}"
      fi
      for probe in missing_credentials invalid_credentials mismatched_tenant; do
        case "$probe" in
          missing_credentials) credential="__missing__"; scope="$expected_scope"; expected_error=missing ;;
          invalid_credentials) credential="invalid-credential"; scope="$expected_scope"; expected_error=invalid ;;
          mismatched_tenant)
            credential="$valid_credential"
            scope="$([[ "$tenant" == a ]] && printf '%s' "$TENANT_B_ID" || printf '%s' "$TENANT_A_ID")"
            expected_error=mismatched
            ;;
        esac
        payload="$(query_payload "$signal" "$uid" "${probe}_credential_probe")"
        artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.json"
        request_artifact="$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.request.json"
        write_request_artifact "$request_artifact" POST /api/ds/query "$payload"
        if api_post_credentials /api/ds/query "$payload" "$artifact" "$credential" "$scope"; then
          :
        else
          status=$?
          if (( status == 2 )); then
            return 2
          fi
        fi
        validate_credential_rejection "$artifact" "$expected_error" || return 1
        if grep -Fq "$other" "$artifact"; then
          echo "credential rejection leaked the other tenant marker" >&2
          return 1
        fi
        bundle_args+=(
          "${signal}_${tenant}_${probe}=$artifact"
          "${signal}_${tenant}_${probe}_request=$request_artifact"
        )
      done
    done
  done
  for signal in prometheus loki tempo; do
    for tenant in a b; do
      for probe in malformed_frame empty_result; do
        if check_panel_rejection "$signal" "$tenant" "$probe"; then
          :
        else
          status=$?
          if (( status == 2 )); then
            return 2
          fi
          return "$status"
        fi
        bundle_args+=(
          "${signal}_${tenant}_${probe}=$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.json"
          "${signal}_${tenant}_${probe}_request=$ARTIFACT_DIR/.work/G8-${signal}-${tenant}-${probe}.request.json"
        )
      done
    done
  done
  write_case_bundle G8 "${bundle_args[@]}"
  record_case G8 pass
}

main() {
  local status
  if ! validate_grafana_reference_pin; then
    finish_failure "Grafana reference image/digest validation failed"
  fi
  if [[ "$MOCK_MODE" != "1" ]]; then
    if ! check_composition_readiness; then
      finish_failure "Self-contained Grafana composition is not ready"
    fi
    if ! run_deterministic_seed; then
      finish_failure "Deterministic OTLP seed failed or detected tenant leakage"
    fi
  fi
  if check_health; then :; else
    status=$?
    (( status == 2 )) && finish_skip "Grafana health endpoint unavailable"
    finish_failure "Grafana health API failure"
  fi
  if check_datasources; then :; else
    status=$?
    (( status == 2 )) && finish_skip "Grafana datasource API unavailable"
    finish_failure "Grafana datasource API failure or credential/header assertion"
  fi
  if check_dashboards; then :; else
    status=$?
    (( status == 2 )) && finish_skip "Grafana dashboard API unavailable"
    finish_failure "Grafana dashboard API failure or panel datasource assertion"
  fi
  if run_signal_case G4 prometheus grafana_phase4_requests_total; then :; else
    status=$?
    (( status == 2 )) && finish_skip "Grafana Prometheus datasource unavailable"
    finish_failure "Grafana Prometheus query or tenant-isolation assertion"
  fi
  if run_signal_case G5 loki '{service_name="checkout"} |= "error"'; then :; else
    status=$?
    (( status == 2 )) && finish_skip "Grafana Loki datasource unavailable"
    finish_failure "Grafana Loki query or tenant-isolation assertion"
  fi
  if run_signal_case G6 tempo '{}'; then :; else
    status=$?
    (( status == 2 )) && finish_skip "Grafana Tempo datasource unavailable"
    finish_failure "Grafana Tempo query or tenant-isolation assertion"
  fi
  if check_cross_signal; then :; else
    status=$?
    (( status == 2 )) && finish_skip "Grafana cross-signal datasource unavailable"
    finish_failure "Grafana cross-signal link assertion"
  fi
  if check_errors; then :; else
    status=$?
    (( status == 2 )) && finish_skip "Grafana error/query API unavailable"
    finish_failure "Grafana explicit error assertion"
  fi
  if (( SKIPPED )); then
    write_outcome environment_skip "One or more Grafana cases were unavailable"
    write_summary environment_skip "One or more Grafana cases were unavailable"
  else
    write_outcome pass
    write_summary pass
  fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
