#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
MANIFEST=$(printenv MANIFEST 2>/dev/null || printf '%s' 'tests/compat/manifests/cases.v0.yaml')
OUT=$(printenv OUT 2>/dev/null || printf '%s' 'target/compat/conformance')
RUN_ID=$(printenv RUN_ID 2>/dev/null || date -u +%Y%m%dT%H%M%SZ)
PROTOCOL_FILTER=
CASE_FILTER=
MOCK=false
DRIFT=false
DRIFT_CANDIDATE_IMAGE=${DRIFT_CANDIDATE_IMAGE:-}
DRIFT_CANDIDATE_VERSION=${DRIFT_CANDIDATE_VERSION:-}
DRIFT_BASELINE_IMAGE=${DRIFT_BASELINE_IMAGE:-}
DRIFT_BASELINE_VERSION=${DRIFT_BASELINE_VERSION:-}

usage() {
	cat <<'EOF'
Usage: scripts/compat/conformance.sh [--mock|--drift] [--protocol PROTOCOL] [--case CASE_ID] [--out DIRECTORY]
       [--candidate-image IMAGE] [--candidate-version VERSION]
       [--baseline-image IMAGE] [--baseline-version VERSION]
EOF
}

while [ "$#" -gt 0 ]; do
	case "$1" in
		--mock) MOCK=true; shift ;;
		--drift) DRIFT=true; shift ;;
		--protocol)
			[ "$#" -ge 2 ] || { echo "ERROR: --protocol requires a value" >&2; exit 2; }
			PROTOCOL_FILTER=$2; shift 2 ;;
		--protocol=*) PROTOCOL_FILTER=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--case)
			[ "$#" -ge 2 ] || { echo "ERROR: --case requires a value" >&2; exit 2; }
			CASE_FILTER=$2; shift 2 ;;
		--case=*) CASE_FILTER=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--out)
			[ "$#" -ge 2 ] || { echo "ERROR: --out requires a directory" >&2; exit 2; }
			OUT=$2; shift 2 ;;
		--out=*) OUT=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--candidate-image)
			[ "$#" -ge 2 ] || { echo "ERROR: --candidate-image requires a value" >&2; exit 2; }
			DRIFT_CANDIDATE_IMAGE=$2; shift 2 ;;
		--candidate-image=*) DRIFT_CANDIDATE_IMAGE=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--candidate-version)
			[ "$#" -ge 2 ] || { echo "ERROR: --candidate-version requires a value" >&2; exit 2; }
			DRIFT_CANDIDATE_VERSION=$2; shift 2 ;;
		--candidate-version=*) DRIFT_CANDIDATE_VERSION=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--baseline-image)
			[ "$#" -ge 2 ] || { echo "ERROR: --baseline-image requires a value" >&2; exit 2; }
			DRIFT_BASELINE_IMAGE=$2; shift 2 ;;
		--baseline-image=*) DRIFT_BASELINE_IMAGE=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--baseline-version)
			[ "$#" -ge 2 ] || { echo "ERROR: --baseline-version requires a value" >&2; exit 2; }
			DRIFT_BASELINE_VERSION=$2; shift 2 ;;
		--baseline-version=*) DRIFT_BASELINE_VERSION=$(printf '%s' "$1" | cut -d= -f2-); shift ;;
		--help|-h) usage; exit 0 ;;
		*) echo "ERROR: unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
done

if [ "$MOCK" = true ] && [ "$DRIFT" = true ]; then
	echo "ERROR: --mock and --drift are mutually exclusive" >&2
	exit 2
fi
if [ "$DRIFT" = true ]; then
	for drift_value in DRIFT_CANDIDATE_IMAGE DRIFT_CANDIDATE_VERSION DRIFT_BASELINE_IMAGE DRIFT_BASELINE_VERSION; do
		[ -n "${!drift_value}" ] || { echo "ERROR: $drift_value is required in drift mode" >&2; exit 2; }
	done
	case "$DRIFT_CANDIDATE_IMAGE" in
		*latest|*@*|*:latest) echo "ERROR: candidate image must be a mutable-tag-free repository name" >&2; exit 2 ;;
	esac
	case "$DRIFT_CANDIDATE_VERSION" in
		""|latest|*latest*) echo "ERROR: candidate version must be immutable and non-latest" >&2; exit 2 ;;
	esac
	[ "$DRIFT_CANDIDATE_IMAGE" != "$DRIFT_BASELINE_IMAGE" ] || [ "$DRIFT_CANDIDATE_VERSION" != "$DRIFT_BASELINE_VERSION" ] || {
		echo "ERROR: candidate reference must differ from the baseline reference" >&2
		exit 2
	}
	export DRIFT_CANDIDATE_IMAGE DRIFT_CANDIDATE_VERSION DRIFT_BASELINE_IMAGE DRIFT_BASELINE_VERSION
fi

resolve_path() {
	case "$1" in
		/*) printf '%s\n' "$1" ;;
		*) printf '%s/%s\n' "$ROOT_DIR" "$1" ;;
	esac
}

MANIFEST_PATH=$(resolve_path "$MANIFEST")
OUT_PATH=$(resolve_path "$OUT")
TMP_BASE=$(printenv TMPDIR 2>/dev/null || printf '%s' /tmp)
TMP_DIR=$(mktemp -d "$TMP_BASE/compat-conformance.XXXXXX")
trap 'rm -rf "$TMP_DIR"' EXIT

[ -f "$MANIFEST_PATH" ] || { echo "ERROR: manifest does not exist: $MANIFEST_PATH" >&2; exit 2; }

if ! ruby -ryaml -rjson - "$MANIFEST_PATH" "$PROTOCOL_FILTER" "$CASE_FILTER" "$ROOT_DIR" >"$TMP_DIR/selected.json" <<'RUBY'
manifest_path, protocol_filter, case_filter, repo_root = ARGV
begin
  document = YAML.safe_load(File.read(manifest_path), permitted_classes: [], permitted_symbols: [], aliases: false)
rescue StandardError => e
  warn "ERROR: invalid YAML manifest: #{e.message}"
  exit 2
end

errors = []
errors << "manifest root must be a mapping" unless document.is_a?(Hash)
cases = document.is_a?(Hash) ? document["cases"] : nil
errors << "manifest cases must be a non-empty sequence" unless cases.is_a?(Array) && !cases.empty?
errors << "manifest version must be compat.v0" unless document.is_a?(Hash) && document["version"] == "compat.v0"
required = %w[id protocol endpoint request fixture capability expected normalization reference evidence]
allowed_protocols = %w[prometheus loki tempo]
allowed_methods = %w[GET POST]
allowed_capability_statuses = %w[phase_1 supported supported_subset ignored unsupported_feature]
capability_ids = Array(cases).each_with_object([]) do |entry, ids|
  capability_id = entry.is_a?(Hash) && entry.dig("capability", "id")
  ids << capability_id if capability_id
end
normalization_policies = {
  "prometheus" => "src/compat/prometheus/diff_normalize.rs::normalize_prom_response",
  "loki" => "tests/compat/support/loki.rs::normalize_loki_response",
  "tempo" => "tests/compat/support/tempo.rs::normalize_tempo_response"
}
seen = {}
validated = []

safe_repo_relative_path = lambda do |path|
  path.is_a?(String) && !path.empty? &&
    !path.start_with?("/") && !path.match?(/\A[A-Za-z]:[\\\/]/) &&
    !path.include?("\\") && !path.include?("\0") &&
    path.split("/").none? { |part| part.empty? || part == "." || part == ".." }
end

existing_repo_file = lambda do |path|
  full_path = File.expand_path(path, repo_root)
  inside_repo = full_path == repo_root || full_path.start_with?(repo_root + File::SEPARATOR)
  inside_repo && File.file?(full_path)
end

validate_unsupported_allowlist = lambda do |value, prefix|
  next if value.nil?
  unless value.is_a?(Array) || value.is_a?(Hash)
    errors << "#{prefix} unsupported-feature allowlist must be a sequence or mapping"
    next
  end
  entries = value.is_a?(Hash) ? value.to_a : value.map { |item| [nil, item] }
  entries.each do |capability_key, item|
    capability_id = capability_key
    features = item
    if item.is_a?(Hash)
      capability_id ||= item["capability"] || item["capability_id"] || item["id"]
      features = item["features"] || item["feature"] || item["name"] || item["path"]
    end
    unless capability_id.is_a?(String) && capability_ids.include?(capability_id)
      errors << "#{prefix} unsupported-feature entry references unknown capability: #{capability_id.inspect}"
    end
    unless (features.is_a?(String) && !features.empty?) ||
           (features.is_a?(Array) && !features.empty? && features.all? { |feature| feature.is_a?(String) && !feature.empty? })
      errors << "#{prefix} unsupported-feature entry must contain a non-empty feature or features list"
    end
  end
end

validate_unsupported_allowlist.call(document["unsupported_features"], "manifest") if document.is_a?(Hash)
validate_unsupported_allowlist.call(document["unsupported_feature_allowlist"], "manifest") if document.is_a?(Hash)

Array(cases).each_with_index do |entry, index|
  prefix = "case #{index + 1}"
  unless entry.is_a?(Hash)
    errors << "#{prefix} must be a mapping"
    next
  end
  missing = required.reject { |field| entry.key?(field) }
  errors << "#{prefix} missing required fields: #{missing.join(', ')}" unless missing.empty?
  id = entry["id"]
  safe_id = id.is_a?(String) && id.match?(/\A[A-Za-z0-9][A-Za-z0-9_.-]*\z/) && id != "." && id != ".."
  errors << "#{prefix} id must be a unique safe path component" unless safe_id
  if id.is_a?(String) && seen.key?(id)
    errors << "duplicate case id: #{id}"
  elsif safe_id
    seen[id] = true
  end
  protocol = entry["protocol"]
  errors << "#{prefix} protocol must be one of #{allowed_protocols.join(', ')}" unless allowed_protocols.include?(protocol)
  endpoint = entry["endpoint"]
  unless endpoint.is_a?(Hash) && allowed_methods.include?(endpoint["method"]) &&
         endpoint["path"].is_a?(String) && endpoint["path"].start_with?("/")
    errors << "#{prefix} endpoint must contain a supported method (GET or POST) and absolute path"
  end
  request = entry["request"]
  if !request.is_a?(Hash) || !request["params"].is_a?(Hash)
    errors << "#{prefix} request must contain a params mapping"
  elsif request["params"].any? { |key, value| !key.is_a?(String) || !value.is_a?(String) }
    errors << "#{prefix} request.params keys and values must be strings"
  end
  fixture = entry["fixture"]
  if !fixture.is_a?(Hash) || !fixture["id"].is_a?(String) || fixture["id"].empty? ||
     !safe_repo_relative_path.call(fixture["path"])
    errors << "#{prefix} fixture.path must be a safe repository-relative path"
  elsif !existing_repo_file.call(fixture["path"])
    errors << "#{prefix} fixture.path does not exist in the repository: #{fixture["path"]}"
  end
  capability = entry["capability"]
  unless capability.is_a?(Hash) && capability["id"].is_a?(String) && !capability["id"].empty? &&
         allowed_capability_statuses.include?(capability["status"])
    errors << "#{prefix} capability.status must be one of: #{allowed_capability_statuses.join(', ')}"
  end
  expected = entry["expected"]
  unless expected.is_a?(Hash) && expected["status"].is_a?(Integer) && expected["status"].between?(100, 599) &&
         expected["envelope"].is_a?(String) && expected["envelope"].match?(/\A[A-Za-z0-9_.-]+\z/)
    errors << "#{prefix} expected must contain an HTTP status from 100..599 and a safe envelope"
  end
  normalization = entry["normalization"]
  unless normalization.is_a?(Hash) && normalization_policies[protocol] == normalization["policy"]
    errors << "#{prefix} normalization.policy is unsupported for #{protocol}"
  end
  reference = entry["reference"]
  unless reference.is_a?(Hash) && reference["service"].is_a?(String) && !reference["service"].empty? &&
         reference["version"].is_a?(String) && !reference["version"].empty?
    errors << "#{prefix} reference must contain service and version"
  end
  evidence = entry["evidence"]
  errors << "#{prefix} evidence must contain boolean retain" unless evidence.is_a?(Hash) && [true, false].include?(evidence["retain"])
  if evidence.is_a?(Hash) && evidence.key?("path")
    evidence_path = evidence["path"]
    if !safe_repo_relative_path.call(evidence_path)
      errors << "#{prefix} evidence.path must be a safe repository-relative path"
    elsif !existing_repo_file.call(evidence_path)
      errors << "#{prefix} evidence.path does not exist in the repository: #{evidence_path}"
    end
  end
  validate_unsupported_allowlist.call(entry["unsupported_features"], prefix)
  validate_unsupported_allowlist.call(entry["unsupported_feature_allowlist"], prefix)
  validate_unsupported_allowlist.call(capability["unsupported_features"], "#{prefix} capability") if capability.is_a?(Hash)
  validated << entry if missing.empty? && safe_id && allowed_protocols.include?(protocol)
end

unless errors.empty?
  errors.each { |error| warn "ERROR: #{error}" }
  exit 2
end

selected = validated.select do |entry|
  (protocol_filter.empty? || entry["protocol"] == protocol_filter) &&
    (case_filter.empty? || entry["id"] == case_filter)
end
if selected.empty?
  filters = []
  filters << "protocol=#{protocol_filter}" unless protocol_filter.empty?
  filters << "case=#{case_filter}" unless case_filter.empty?
  warn "ERROR: no cases selected#{filters.empty? ? '' : " (#{filters.join(', ')})"}"
  exit 2
end
STDOUT.write(JSON.generate("version" => document["version"], "cases" => selected))
RUBY
then
	exit 2
fi

write_json() {
	local destination=$1
	local json=$2
	printf '%s' "$json" | ruby -rjson -e '
destination = ARGV.fetch(0)
object = JSON.parse(STDIN.read)
def redact_text(value)
  value
    .gsub(/(?i)\bBearer\s+[^\s"'"'"']+/) { "Bearer [REDACTED]" }
    .gsub(/(?i)(["'"'"']?\b(?:access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret)\b["'"'"']?\s*[:=]\s*)(["'"'"']?)([^"'"'"'",\s}\]]+)\2/) { "#{$1}#{$2}[REDACTED]#{$2}" }
end
def redact(value, key = nil)
  if key && key.to_s.match?(/(?:token|password|passwd|secret)/i)
    "[REDACTED]"
  elsif value.is_a?(Hash)
    value.each_with_object({}) { |(k, v), result| result[k] = redact(v, k) }
  elsif value.is_a?(Array)
    value.map { |v| redact(v) }
  elsif value.is_a?(String)
    redact_text(value)
  else
    value
  end
end
File.open(destination, "w") { |file| file.write(JSON.pretty_generate(redact(object)) + "\n") }
' "$destination"
}

json_value() {
	local expression=$1
	local json=$2
	ruby -rjson -e '
  object = JSON.parse(ARGV.fetch(0))
  value = eval(ARGV.fetch(1))
  STDOUT.write(JSON.generate(value))
' "$json" "$expression"
}

canonical_json() {
	local json=$1
	ruby -rjson -e '
  def canonical(value)
    case value
    when Hash
      value.keys.map(&:to_s).sort.each_with_object({}) { |key, result| result[key] = canonical(value[key]) }
    when Array
      value.map { |item| canonical(item) }
    else
      value
    end
  end
  STDOUT.write(JSON.generate(canonical(JSON.parse(ARGV.fetch(0)))))
' "$json"
}

sha256_text() {
	local value=$1
	ruby -rdigest -e 'STDOUT.write(Digest::SHA256.hexdigest(ARGV.fetch(0)))' "$value"
}

mock_payload() {
	ruby -rjson -e '
  entry = JSON.parse(ARGV.fetch(0))
  STDOUT.write(JSON.generate(
    "case_id" => entry["id"],
    "protocol" => entry["protocol"],
    "endpoint" => entry["endpoint"],
    "request" => entry["request"],
    "expected" => entry["expected"],
    "fixture" => entry["fixture"]["id"],
    "response" => {
      "status" => entry["expected"]["status"],
      "envelope" => entry["expected"]["envelope"],
      "deterministic" => true
    }
  ))
' "$1"
}

runner_for_protocol() {
	case "$1" in
		prometheus) printf '%s\n' 'make test-prom-compat' ;;
		loki) printf '%s\n' 'make test-loki-diff' ;;
		tempo) printf '%s\n' 'make test-tempo-diff' ;;
		*) return 1 ;;
	esac
}

classify_failure() {
	local output=$1
	if printf '%s' "$output" | ruby -e 'exit(ARGF.read.match?(/environment[_ ]skip|\bskip(?:ped)?\b|not run/i) ? 0 : 1)'; then
		printf '%s\n' environment_skip
	elif printf '%s' "$output" | ruby -e 'exit(ARGF.read.match?(/docker|image|unavailable|not found|no such file|cannot connect|connection refused|daemon|timed? ?out|timeout/i) ? 0 : 1)'; then
		printf '%s\n' infrastructure_failure
	else
		printf '%s\n' product_regression
	fi
}

monotonic_seconds() {
	ruby -e 'printf "%.6f\n", Process.clock_gettime(Process::CLOCK_MONOTONIC)'
}

reference_image_for_protocol() {
	case "$1" in
		prometheus) printf '%s\n' prom/prometheus ;;
		loki) printf '%s\n' grafana/loki ;;
		tempo) printf '%s\n' grafana/tempo ;;
		*) return 1 ;;
	esac
}

redact_file() {
	local source=$1
	local destination=$2
	ruby - "$source" "$destination" <<'RUBY'
def redact_text(value)
  value
    .gsub(/(?i)\bBearer\s+[^\s"']+/) { "Bearer [REDACTED]" }
    .gsub(/(?i)(["']?\b(?:access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret)\b["']?\s*[:=]\s*)(["']?)([^"'\s},\]]+)\2/) { "#{$1}#{$2}[REDACTED]#{$2}" }
end
File.open(ARGV.fetch(1), "w") { |file| file.write(redact_text(File.read(ARGV.fetch(0)))) }
RUBY
}

copy_redacted_artifact() {
	local source=$1
	local destination=$2
	mkdir -p "$(dirname "$destination")"
	if [ "${source##*.}" = json ]; then
		write_json "$destination" "$(cat "$source")"
	else
		redact_file "$source" "$destination"
	fi
}

copy_first_artifact() {
	local destination=$1
	shift
	local source
	for source in "$@"; do
		if [ -f "$source" ]; then
			copy_redacted_artifact "$source" "$destination"
			return 0
		fi
	done
	return 1
}

write_case_provenance() {
	local case_dir=$1
	local case_json=$2
	local protocol=$3
	local suite_artifact_source=$4
	local evidence_source=$5
	local provenance_json

	provenance_json=$(ruby -rjson -rdigest - "$case_json" "$case_dir/request.raw.json" "$RUN_ID" "$protocol" \
		"$suite_artifact_source" "$evidence_source" <<'RUBY'
case_document = JSON.parse(File.read(ARGV.fetch(0)))
request_document = JSON.parse(File.read(ARGV.fetch(1)))
run_id, protocol, suite_artifact_source, evidence_source = ARGV.drop(2)

def canonical(value)
  case value
  when Hash
    value.keys.map(&:to_s).sort.each_with_object({}) { |key, result| result[key] = canonical(value[key]) }
  when Array
    value.map { |item| canonical(item) }
  else
    value
  end
end

canonical_request = canonical(request_document)
canonical_request_json = JSON.generate(canonical_request)
request_fingerprint = Digest::SHA256.hexdigest(canonical_request_json)
endpoint = case_document.fetch("endpoint")
fixture = case_document.fetch("fixture")
capability = case_document.fetch("capability")
expected = case_document.fetch("expected")
normalization = case_document.fetch("normalization")
reference = case_document.fetch("reference")

STDOUT.write(JSON.generate(
  "case_id" => case_document.fetch("id"),
  "protocol" => protocol,
  "endpoint" => { "method" => endpoint.fetch("method"), "path" => endpoint.fetch("path") },
  "canonical_request_json" => canonical_request,
  "canonical_request_json_text" => canonical_request_json,
  "request_fingerprint" => request_fingerprint,
  "request_fingerprint_algorithm" => "SHA-256",
  "request_sha256" => request_fingerprint,
  "fixture" => { "id" => fixture.fetch("id"), "path" => fixture.fetch("path") },
  "fixture_id" => fixture.fetch("id"),
  "fixture_path" => fixture.fetch("path"),
  "capability" => { "id" => capability.fetch("id"), "status" => capability.fetch("status") },
  "capability_id" => capability.fetch("id"),
  "capability_status" => capability.fetch("status"),
  "expected" => { "status" => expected.fetch("status"), "envelope" => expected.fetch("envelope") },
  "expected_status" => expected.fetch("status"),
  "expected_envelope" => expected.fetch("envelope"),
  "normalization" => { "policy" => normalization.fetch("policy") },
  "normalization_policy" => normalization.fetch("policy"),
  "reference" => { "service" => reference.fetch("service"), "version" => reference.fetch("version") },
  "reference_service" => reference.fetch("service"),
  "reference_version" => reference.fetch("version"),
  "run_id" => run_id,
  "suite_artifact_source" => suite_artifact_source,
  "case_artifact_source" => (evidence_source.empty? ? nil : evidence_source),
  "evidence" => {
    "provenance_path" => "case_provenance.json",
    "raw" => ["softprobe.raw.json", "reference.raw.json"],
    "normalized" => ["softprobe.normalized.json", "reference.normalized.json"]
  }
))
RUBY
	)
	if [ "${COMPAT_DRIFT_MODE:-}" = drift ]; then
		provenance_json=$(ruby -rjson -e 'value = JSON.parse(STDIN.read); value["release_evidence"] = false; puts JSON.generate(value)' <<<"$provenance_json")
	fi
	write_json "$case_dir/case_provenance.json" "$provenance_json"
}

validate_case_provenance() {
	local case_dir=$1
	if [ ! -f "$case_dir/case_provenance.json" ]; then
		printf '%s\n' 'infrastructure_failure|missing_provenance'
		return 0
	fi
	ruby -rjson -rdigest - "$case_dir" <<'RUBY'
case_dir = ARGV.fetch(0)
begin
  provenance = JSON.parse(File.read(File.join(case_dir, "case_provenance.json")))
  case_document = JSON.parse(File.read(File.join(case_dir, "case.json")))
  request_document = JSON.parse(File.read(File.join(case_dir, "request.raw.json")))
rescue Errno::ENOENT
  puts "infrastructure_failure|missing_provenance_input"
  exit
rescue JSON::ParserError, StandardError => e
  puts "product_regression|invalid_provenance:#{e.class}"
  exit
end

def canonical(value)
  case value
  when Hash
    value.keys.map(&:to_s).sort.each_with_object({}) { |key, result| result[key] = canonical(value[key]) }
  when Array
    value.map { |item| canonical(item) }
  else
    value
  end
end

canonical_request = canonical(request_document)
canonical_request_json = JSON.generate(canonical_request)
expected_fingerprint = Digest::SHA256.hexdigest(canonical_request_json)
errors = []
errors << "case_id" unless provenance["case_id"] == case_document["id"]
errors << "protocol" unless provenance["protocol"] == case_document["protocol"]
errors << "canonical_request_json" unless provenance["canonical_request_json"] == canonical_request
errors << "canonical_request_json_text" unless provenance["canonical_request_json_text"] == canonical_request_json
errors << "request_fingerprint" unless provenance["request_fingerprint"] == expected_fingerprint
errors << "request_sha256" unless provenance["request_sha256"] == expected_fingerprint
errors << "request_fingerprint_algorithm" unless provenance["request_fingerprint_algorithm"] == "SHA-256"
errors << "evidence_link" unless provenance.dig("evidence", "provenance_path") == "case_provenance.json"
%w[softprobe.raw.json reference.raw.json softprobe.normalized.json reference.normalized.json].each do |name|
  errors << "evidence:#{name}" unless File.file?(File.join(case_dir, name))
end
if errors.empty?
  puts "pass|provenance_valid"
else
  puts "product_regression|mismatched_provenance:#{errors.join(',')}"
end
RUBY
}

validate_execution_receipt() {
	local receipt_path=$1
	local selected_cases_path=$2
	local protocol=$3
	local expected_run_id=$4
	local expected_runner_status=$5
	if [ ! -f "$receipt_path" ]; then
		printf '%s\n' 'infrastructure_failure|missing_execution_receipt'
		return 0
	fi
	ruby -rjson -rdigest - "$receipt_path" "$selected_cases_path" "$protocol" "$expected_run_id" "$expected_runner_status" <<'RUBY'
receipt_path, selected_path, protocol, expected_run_id, expected_runner_status = ARGV

begin
  receipt = JSON.parse(File.read(receipt_path))
  selected_document = JSON.parse(File.read(selected_path))
rescue Errno::ENOENT
  puts "infrastructure_failure|missing_execution_receipt_input"
  exit
rescue JSON::ParserError, StandardError => e
  puts "product_regression|invalid_execution_receipt:#{e.class}"
  exit
end

def canonical(value)
  case value
  when Hash
    value.keys.map(&:to_s).sort.each_with_object({}) { |key, result| result[key] = canonical(value[key]) }
  when Array
    value.map { |item| canonical(item) }
  else
    value
  end
end

def mismatch(errors, name, actual, expected)
  errors << name unless actual == expected
end

selected_cases = selected_document.fetch("cases")
expected_case_ids = selected_cases.map { |entry| entry.fetch("id") }
expected_fixture_ids = selected_cases.map { |entry| entry.fetch("fixture").fetch("id") }
errors = []
mismatch(errors, "run_id", receipt["run_id"], expected_run_id)
mismatch(errors, "protocol", receipt["protocol"], protocol)
mismatch(errors, "status", receipt["status"], expected_runner_status)
mismatch(errors, "selected_case_ids", receipt["selected_case_ids"], expected_case_ids)
mismatch(errors, "selected_fixture_ids", receipt["selected_fixture_ids"], expected_fixture_ids)

executed_case_ids = receipt["executed_case_ids"]
executed_fixture_ids = receipt["executed_fixture_ids"]
unless executed_case_ids.is_a?(Array) && executed_fixture_ids.is_a?(Array)
  errors << "executed_ids"
  executed_case_ids = []
  executed_fixture_ids = []
end
if executed_case_ids.uniq != executed_case_ids || executed_fixture_ids.uniq != executed_fixture_ids
  errors << "duplicate_executed_ids"
end
unless executed_case_ids.all? { |case_id| expected_case_ids.include?(case_id) }
  errors << "executed_case_ids"
end
expected_executed_fixture_ids = executed_case_ids.map do |case_id|
  index = expected_case_ids.index(case_id)
  index.nil? ? nil : expected_fixture_ids[index]
end
mismatch(errors, "executed_fixture_ids", executed_fixture_ids, expected_executed_fixture_ids)
if expected_runner_status == "pass" && executed_case_ids != expected_case_ids
  errors << "incomplete_execution"
end

receipt_cases = receipt["cases"]
unless receipt_cases.is_a?(Array)
  errors << "cases"
  receipt_cases = []
end
if receipt_cases.length != expected_case_ids.length
  errors << "case_count"
end

selected_cases.each_with_index do |entry, index|
  record = receipt_cases[index]
  unless record.is_a?(Hash)
    errors << "case_#{index}_record"
    next
  end
  case_id = entry.fetch("id")
  fixture_id = entry.fetch("fixture").fetch("id")
  endpoint = entry.fetch("endpoint")
  params = entry.fetch("request").fetch("params")
  expected_request = canonical(
    "method" => endpoint.fetch("method"),
    "path" => endpoint.fetch("path"),
    "params" => params
  )
  expected_text = JSON.generate(expected_request)
  expected_fingerprint = Digest::SHA256.hexdigest(expected_text)
  mismatch(errors, "case_#{index}_case_id", record["case_id"], case_id)
  mismatch(errors, "case_#{index}_fixture_id", record["fixture_id"], fixture_id)
  mismatch(errors, "case_#{index}_canonical_request", record["canonical_request"], expected_request)
  mismatch(errors, "case_#{index}_canonical_text", record["canonical_text"], expected_text)
  mismatch(errors, "case_#{index}_canonical_request_json", record["canonical_request_json"], expected_text)
  mismatch(errors, "case_#{index}_fingerprint", record["fingerprint"], expected_fingerprint)
  mismatch(errors, "case_#{index}_request_fingerprint", record["request_fingerprint"], expected_fingerprint)
  mismatch(errors, "case_#{index}_nested_fingerprint", record.dig("fingerprints", "canonical_request"), expected_fingerprint)
  mismatch(errors, "case_#{index}_fingerprint_algorithm", record["fingerprint_algorithm"], "SHA-256")
end

if errors.empty?
  puts "pass|execution_receipt_valid"
else
  puts "product_regression|mismatched_execution_receipt:#{errors.join(',')}"
end
RUBY
}

self_check_provenance() {
	local self_dir="$TMP_DIR/provenance-self-check"
	local case_json='{"id":"provenance-self-check","protocol":"prometheus"}'
	local request_json='{"endpoint":{"method":"GET","path":"/api/v1/query"},"request":{"params":{"query":"up"}}}'
	local canonical_request
	local request_fingerprint
	local provenance_json
	local validation
	mkdir -p "$self_dir"
	write_json "$self_dir/case.json" "$case_json"
	write_json "$self_dir/request.raw.json" "$request_json"
	: >"$self_dir/softprobe.raw.json"
	: >"$self_dir/reference.raw.json"
	: >"$self_dir/softprobe.normalized.json"
	: >"$self_dir/reference.normalized.json"
	canonical_request=$(canonical_json "$request_json")
	request_fingerprint=$(sha256_text "$canonical_request")
	provenance_json=$(ruby -rjson -e '
  request = JSON.parse(ARGV.fetch(0))
  canonical = JSON.generate(request.keys.sort.each_with_object({}) { |key, result| result[key] = request[key] })
  fingerprint = ARGV.fetch(1)
  puts JSON.generate(
    "case_id" => "provenance-self-check", "protocol" => "prometheus",
    "canonical_request_json" => request, "canonical_request_json_text" => canonical,
    "request_fingerprint" => fingerprint, "request_fingerprint_algorithm" => "SHA-256",
    "request_sha256" => fingerprint,
    "evidence" => { "provenance_path" => "case_provenance.json" }
  )
' "$canonical_request" "$request_fingerprint")
	write_json "$self_dir/case_provenance.json" "$provenance_json"
	validation=$(validate_case_provenance "$self_dir")
	[ "$validation" = 'pass|provenance_valid' ] || { echo "self-check failed: valid provenance ($validation)" >&2; return 1; }
	validation=$(validate_case_provenance "$TMP_DIR/provenance-self-check-missing")
	[ "$validation" = 'infrastructure_failure|missing_provenance' ] || { echo "self-check failed: missing provenance ($validation)" >&2; return 1; }
	ruby -rjson -e '
  path = ARGV.fetch(0)
  document = JSON.parse(File.read(path))
  document["request_fingerprint"] = "0" * 64
  File.write(path, JSON.generate(document))
' "$self_dir/case_provenance.json"
	validation=$(validate_case_provenance "$self_dir")
	[ "$validation" = 'product_regression|mismatched_provenance:request_fingerprint' ] || { echo "self-check failed: mismatched provenance ($validation)" >&2; return 1; }
	echo 'provenance self-check: PASS (valid, missing, mismatched)'
}

self_check_receipt() {
	local self_dir="$TMP_DIR/receipt-self-check"
	local receipt_path="$self_dir/execution-receipt.json"
	local validation
	mkdir -p "$self_dir"
	write_json "$self_dir/selected.json" '{"version":"compat.v0","cases":[{"id":"receipt-case-a","endpoint":{"method":"GET","path":"/api/v1/query"},"request":{"params":{"query":"up"}},"fixture":{"id":"fixture-a"}},{"id":"receipt-case-b","endpoint":{"method":"GET","path":"/api/v1/labels"},"request":{"params":{}},"fixture":{"id":"fixture-b"}}]}'
	ruby -rjson -rdigest - "$self_dir/selected.json" "$receipt_path" <<'RUBY'
selected_path, receipt_path = ARGV
selected = JSON.parse(File.read(selected_path))
canonical = lambda do |value|
  case value
  when Hash
    value.keys.map(&:to_s).sort.each_with_object({}) { |key, result| result[key] = canonical.call(value[key]) }
  when Array
    value.map { |item| canonical.call(item) }
  else
    value
  end
end
records = selected.fetch("cases").map do |entry|
  endpoint = entry.fetch("endpoint")
  request = canonical.call("method" => endpoint.fetch("method"), "path" => endpoint.fetch("path"), "params" => entry.fetch("request").fetch("params"))
  text = JSON.generate(request)
  fingerprint = Digest::SHA256.hexdigest(text)
  {
    "case_id" => entry.fetch("id"),
    "source_id" => entry.fetch("id"),
    "fixture_id" => entry.fetch("fixture").fetch("id"),
    "canonical_request" => request,
    "canonical_text" => text,
    "canonical_request_json" => text,
    "fingerprint" => fingerprint,
    "request_fingerprint" => fingerprint,
    "fingerprints" => { "canonical_request" => fingerprint },
    "fingerprint_algorithm" => "SHA-256"
  }
end
File.write(receipt_path, JSON.generate(
  "run_id" => "receipt-self-check-run",
  "protocol" => "prometheus",
  "selected_case_ids" => selected.fetch("cases").map { |entry| entry.fetch("id") },
  "executed_case_ids" => selected.fetch("cases").map { |entry| entry.fetch("id") },
  "selected_fixture_ids" => selected.fetch("cases").map { |entry| entry.fetch("fixture").fetch("id") },
  "executed_fixture_ids" => selected.fetch("cases").map { |entry| entry.fetch("fixture").fetch("id") },
  "status" => "pass",
  "outcome" => "pass",
  "cases" => records
))
RUBY
	validation=$(validate_execution_receipt "$receipt_path" "$self_dir/selected.json" prometheus receipt-self-check-run pass)
	[ "$validation" = 'pass|execution_receipt_valid' ] || { echo "self-check failed: valid receipt ($validation)" >&2; return 1; }
	validation=$(validate_execution_receipt "$self_dir/missing.json" "$self_dir/selected.json" prometheus receipt-self-check-run pass)
	[ "$validation" = 'infrastructure_failure|missing_execution_receipt' ] || { echo "self-check failed: missing receipt ($validation)" >&2; return 1; }
	ruby -rjson -e '
  path = ARGV.fetch(0)
  document = JSON.parse(File.read(path))
  document["cases"][0]["fingerprint"] = "0" * 64
  File.write(path, JSON.generate(document))
' "$receipt_path"
	validation=$(validate_execution_receipt "$receipt_path" "$self_dir/selected.json" prometheus receipt-self-check-run pass)
	case "$validation" in
		product_regression\|mismatched_execution_receipt:*fingerprint*) ;;
		*) echo "self-check failed: mismatched receipt ($validation)" >&2; return 1 ;;
	esac
	echo 'execution receipt self-check: PASS (valid, missing, mismatched)'
}

case_artifact_dir() {
	local protocol=$1
	local case_id=$2
	local candidate
	for candidate in \
		"$RUN_ARTIFACT_DIR/$protocol/$case_id" \
		"$RUN_ARTIFACT_DIR/$case_id"; do
		if [ -d "$candidate" ]; then
			printf '%s\n' "$candidate"
			return 0
		fi
	done
	if [ "${RUN_PROTOCOL_CASE_COUNT:-0}" -eq 1 ] && [ -d "$RUN_ARTIFACT_DIR" ]; then
		printf '%s\n' "$RUN_ARTIFACT_DIR"
		return 0
	fi
	return 1
}

suite_artifact_dir() {
	local protocol=$1
	local candidate
	for candidate in \
		"$RUN_ARTIFACT_DIR/$protocol" \
		"$RUN_ARTIFACT_DIR/suite" \
		"$RUN_ARTIFACT_DIR"; do
		if [ -d "$candidate" ]; then
			printf '%s\n' "$candidate"
			return 0
		fi
	done
	return 1
}

extract_wrapper_side() {
	local source=$1
	local destination=$2
	shift 2
	local value
	value=$(ruby -rjson -e '
  object = JSON.parse(File.read(ARGV.fetch(0)))
  sides = ARGV.drop(1)
  value = sides.lazy.map { |side| object[side] }.find { |candidate| !candidate.nil? }
  exit 1 if value.nil?
  STDOUT.write(JSON.generate(value))
' "$source" "$@" 2>/dev/null) || return 1
	write_json "$destination" "$value"
}

ingest_from_source() {
	local source_dir=$1
	local case_dir=$2
	local copied=0

	if copy_first_artifact "$case_dir/softprobe.raw.json" \
		"$source_dir/softprobe.raw.json" "$source_dir/lake.raw.json" \
		"$source_dir/raw.softprobe.json" "$source_dir/raw.lake.json"; then
		copied=$((copied + 1))
	fi
	if copy_first_artifact "$case_dir/reference.raw.json" \
		"$source_dir/reference.raw.json" "$source_dir/oracle.raw.json" \
		"$source_dir/raw.reference.json" "$source_dir/raw.oracle.json"; then
		copied=$((copied + 1))
	fi
	if copy_first_artifact "$case_dir/softprobe.normalized.json" \
		"$source_dir/softprobe.normalized.json" "$source_dir/lake.normalized.json" \
		"$source_dir/normalized.softprobe.json" "$source_dir/normalized.lake.json"; then
		copied=$((copied + 1))
	fi
	if copy_first_artifact "$case_dir/reference.normalized.json" \
		"$source_dir/reference.normalized.json" "$source_dir/oracle.normalized.json" \
		"$source_dir/normalized.reference.json" "$source_dir/normalized.oracle.json"; then
		copied=$((copied + 1))
	fi
	# The shared lifecycle helper also emits raw/normalized wrappers. Split only
	# actual members; never invent a missing reference or Softprobe side.
	if [ ! -f "$case_dir/softprobe.raw.json" ] &&
		extract_wrapper_side "$source_dir/raw.json" "$case_dir/softprobe.raw.json" softprobe lake; then
		copied=$((copied + 1))
	fi
	if [ ! -f "$case_dir/reference.raw.json" ] &&
		extract_wrapper_side "$source_dir/raw.json" "$case_dir/reference.raw.json" reference oracle; then
		copied=$((copied + 1))
	fi
	if [ ! -f "$case_dir/softprobe.normalized.json" ] &&
		extract_wrapper_side "$source_dir/normalized.json" "$case_dir/softprobe.normalized.json" softprobe lake; then
		copied=$((copied + 1))
	fi
	if [ ! -f "$case_dir/reference.normalized.json" ] &&
		extract_wrapper_side "$source_dir/normalized.json" "$case_dir/reference.normalized.json" reference oracle; then
		copied=$((copied + 1))
	fi

	ARTIFACT_COUNT=$copied
	[ "$copied" -eq 4 ]
}

ingest_case_artifacts() {
	local protocol=$1
	local case_id=$2
	local case_dir=$3
	local source_dir
	ARTIFACT_COUNT=0
	EVIDENCE_SCOPE=none
	EVIDENCE_SOURCE=

	source_dir=$(case_artifact_dir "$protocol" "$case_id" 2>/dev/null || true)
	if [ -n "$source_dir" ] && ingest_from_source "$source_dir" "$case_dir"; then
		EVIDENCE_SCOPE=case
		EVIDENCE_SOURCE=$source_dir
		return 0
	fi

	return 1
}

write_normalized_diff() {
	local case_dir=$1
	local case_json=$2
	local diff_json
diff_json=$(ruby -rjson - "$case_dir/softprobe.normalized.json" "$case_dir/reference.normalized.json" "$case_json" <<'RUBY'
softprobe = JSON.parse(File.read(ARGV.fetch(0)))
reference = JSON.parse(File.read(ARGV.fetch(1)))
manifest_case = JSON.parse(File.read(ARGV.fetch(2)))
allowlist = []
%w[unsupported_features unsupported_feature_allowlist].each do |key|
  value = manifest_case[key]
  allowlist.concat(value.is_a?(Array) ? value : [])
end
allowlist.concat(Array(manifest_case.dig("capability", "unsupported_features")))
allowlist = allowlist.each_with_object([]) do |item, paths|
  path = if item.is_a?(Hash)
    item["path"] || item["diff_path"]
  elsif item.is_a?(String)
    item
  end
  paths << path if path
end

def path_for_key(path, key)
  key.match?(/\A[A-Za-z_][A-Za-z0-9_]*\z/) ? "#{path}.#{key}" : "#{path}[#{key.inspect}]"
end

def approved_path?(path, allowlist)
  allowlist.any? do |entry|
    entry == "*" || entry == path ||
      (entry.end_with?(".*") && path.start_with?(entry[0...-1])) ||
      path.start_with?(entry + ".") || path.start_with?(entry + "[")
  end
end

def collect_differences(left, right, path, allowlist, differences)
  if left.is_a?(Hash) && right.is_a?(Hash)
    (left.keys | right.keys).map(&:to_s).sort.each do |key|
      collect_differences(left[key], right[key], path_for_key(path, key), allowlist, differences)
    end
  elsif left.is_a?(Array) && right.is_a?(Array)
    [left.length, right.length].max.times do |index|
      collect_differences(left[index], right[index], "#{path}[#{index}]", allowlist, differences)
    end
  elsif left != right
    differences << {
      "path" => path,
      "softprobe" => left,
      "reference" => right,
      "approved" => approved_path?(path, allowlist)
    }
  end
end

differences = []
collect_differences(softprobe, reference, "$", allowlist, differences)
unapproved = differences.reject { |difference| difference["approved"] }
puts JSON.generate(
  "equal" => unapproved.empty?,
  "differences" => differences,
  "approved_differences" => differences.count { |difference| difference["approved"] },
  "unapproved_differences" => unapproved.length,
  "release_evidence" => (ENV["COMPAT_DRIFT_MODE"] == "drift" ? false : true)
)
RUBY
	)
	equal=$(printf '%s' "$diff_json" | ruby -rjson -e 'puts JSON.parse(STDIN.read).fetch("equal")')
	write_json "$case_dir/diff.json" "$diff_json"
	[ "$equal" = true ]
}

copy_artifact_tree() {
	local source_dir=$1
	local destination_dir=$2
	local source relative
	[ -d "$source_dir" ] || return 0
	while IFS= read -r -d '' source; do
		relative=${source#"$source_dir"/}
		copy_redacted_artifact "$source" "$destination_dir/$relative"
	done < <(find "$source_dir" -type f -print0)
}

run_protocol_target() {
	local protocol=$1
	local artifact_dir=$2
	local selected_cases=$3
	local command=$4
	local -a protocol_env
	case "$protocol" in
		prometheus)
			protocol_env=(
				"PROMETHEUS_DIFF_ARTIFACT_DIR=$artifact_dir"
				"PROMETHEUS_RAW_ARTIFACT=$artifact_dir/raw.json"
				"PROMETHEUS_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
				"PROMETHEUS_DIFF_RAW_ARTIFACT=$artifact_dir/raw.json"
				"PROMETHEUS_DIFF_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
			)
			if [ "$DRIFT" = true ]; then
				protocol_env+=("PROMETHEUS_REFERENCE_IMAGE=$DRIFT_CANDIDATE_IMAGE:$DRIFT_CANDIDATE_VERSION")
			fi
			;;
		loki)
			protocol_env=(
				"LOKI_DIFF_ARTIFACT_DIR=$artifact_dir"
				"LOKI_RAW_ARTIFACT=$artifact_dir/raw.json"
				"LOKI_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
				"LOKI_DIFF_RAW_ARTIFACT=$artifact_dir/raw.json"
				"LOKI_DIFF_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
			)
			;;
			tempo)
			protocol_env=(
				"TEMPO_DIFF_ARTIFACT_DIR=$artifact_dir"
				"TEMPO_RAW_ARTIFACT=$artifact_dir/raw.json"
				"TEMPO_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
				"TEMPO_DIFF_RAW_ARTIFACT=$artifact_dir/raw.json"
				"TEMPO_DIFF_NORMALIZED_ARTIFACT=$artifact_dir/normalized.json"
			)
			;;
		*) return 2 ;;
	esac
	(
		cd "$ROOT_DIR"
		env "${protocol_env[@]}" \
			"SOFTPROBE_COMPAT_ARTIFACT_DIR=$artifact_dir" \
			COMPAT_CASE_ID=__suite__ \
			"COMPAT_PROTOCOL=$protocol" \
			"COMPAT_CASE_JSON=$selected_cases" \
			"COMPAT_CASE_IDS=$(ruby -rjson -e 'JSON.parse(File.read(ARGV.fetch(0))).fetch(\"cases\").map { |entry| entry.fetch(\"id\") }.join(\",\")' \"$selected_cases\")" \
			"COMPAT_CONFORMANCE_OUT=$artifact_dir" \
			"COMPAT_RUN_ID=$RUN_ID" \
			COMPAT_RUN_SCOPE=suite \
			bash -c "$command"
	)
}

self_check_drift() {
	local self_out="$OUT_PATH/drift-self-check"
	export COMPAT_DRIFT_MODE=drift
	rm -rf "$self_out"
	mkdir -p "$self_out"

	while IFS='|' read -r protocol baseline candidate; do
		local case_dir="$self_out/$protocol/drift-self-check"
		mkdir -p "$case_dir"
		write_json "$case_dir/case.json" "$(printf '{\"id\":\"drift-self-check\",\"protocol\":\"%s\",\"unsupported_features\":[],\"release_evidence\":false}' "$protocol")"
		write_json "$case_dir/request.raw.json" '{"request":{"params":{"query":"self-check"}}}'
		write_json "$case_dir/request.normalized.json" '{"request":{"params":{"query":"self-check"}}}'
		write_json "$case_dir/softprobe.raw.json" "$(printf '{\"reference\":\"%s\",\"value\":\"baseline\"}' "$baseline")"
		write_json "$case_dir/reference.raw.json" "$(printf '{\"reference\":\"%s\",\"value\":\"candidate\"}' "$candidate")"
		write_json "$case_dir/softprobe.normalized.json" '{"semantic_value":"baseline"}'
		write_json "$case_dir/reference.normalized.json" '{"semantic_value":"candidate"}'
		if write_normalized_diff "$case_dir" "$case_dir/case.json"; then
			echo "drift self-check unexpectedly reported equal semantics for $protocol" >&2
			return 1
		fi
		write_json "$case_dir/case_provenance.json" "$(printf '{\"mode\":\"drift\",\"protocol\":\"%s\",\"baseline\":\"%s\",\"candidate\":\"%s\",\"release_evidence\":false}' "$protocol" "$baseline" "$candidate")"
		if [ "$protocol" = prometheus ]; then
			write_json "$case_dir/outcome.json" "$(printf '{\"mode\":\"drift\",\"status\":\"drift\",\"classification\":\"drift\",\"review_status\":\"needs_review\",\"reference_image\":\"%s\",\"baseline\":{\"image\":\"%s\"},\"candidate\":{\"image\":\"%s\"},\"release_evidence\":false}' "$candidate" "$baseline" "$candidate")"
		else
			write_json "$case_dir/outcome.json" "$(printf '{\"mode\":\"drift\",\"status\":\"drift\",\"classification\":\"drift\",\"review_status\":\"needs_review\",\"baseline\":{\"image\":\"%s\"},\"candidate\":{\"image\":\"%s\"},\"release_evidence\":false}' "$baseline" "$candidate")"
		fi
	done <<'EOF'
prometheus|prom/prometheus:v2.54.1|prom/prometheus:v2.55.0
loki|grafana/loki:3.1.1|grafana/loki:3.2.0
tempo|grafana/tempo:2.6.1|grafana/tempo:2.7.0
EOF

	ruby -rjson - "$self_out" <<'RUBY'
root = ARGV.fetch(0)
%w[prometheus loki tempo].each do |protocol|
  path = File.join(root, protocol, "drift-self-check", "outcome.json")
  outcome = JSON.parse(File.read(path))
  abort "invalid drift classification for #{protocol}" unless outcome["classification"] == "drift" && outcome["review_status"] == "needs_review" && outcome["release_evidence"] == false
end
prometheus_root = File.join(root, "prometheus", "drift-self-check")
prometheus_outcome = JSON.parse(File.read(File.join(prometheus_root, "outcome.json")))
abort "missing Prometheus candidate metadata" unless prometheus_outcome["reference_image"] == "prom/prometheus:v2.55.0"
abort "Prometheus drift self-check emitted configuration failure placeholder" if File.exist?(File.join(prometheus_root, "configuration_failure.json"))
report = {
  "mode" => "drift",
  "release_evidence" => false,
  "classification" => "drift",
  "review_status" => "needs_review",
  "protocols" => %w[prometheus loki tempo]
}
File.write(File.join(root, "report.json"), JSON.pretty_generate(report) + "\n")
RUBY
	echo "drift self-check: PASS (Prometheus, Loki, Tempo)"
	echo "drift self-check artifacts: $self_out"
}

if [ "${COMPAT_CONFORMANCE_SELF_CHECK:-}" = provenance ]; then
	self_check_provenance
	exit 0
fi
if [ "${COMPAT_CONFORMANCE_SELF_CHECK:-}" = receipt ]; then
	self_check_receipt
	exit 0
fi
if [ "${COMPAT_CONFORMANCE_SELF_CHECK:-}" = drift ]; then
	self_check_drift
	exit 0
fi

mkdir -p "$OUT_PATH"
: >"$OUT_PATH/report.jsonl"
mode=$([ "$MOCK" = true ] && printf mock || { [ "$DRIFT" = true ] && printf drift || printf real; })
export COMPAT_DRIFT_MODE="$mode"
mkdir -p "$TMP_DIR/durations"
if [ "$MOCK" = true ]; then
	cat >"$OUT_PATH/NOTICE.txt" <<'EOF'
This artifact was produced by the explicit mock/fast harness selector.
Mock comparisons are manifest-shaped checks, not service-backed compatibility evidence.
EOF
fi

case_count=0
pass_count=0
product_regressions=0
drift_cases=0
infrastructure_failures=0
environment_skips=0
{
	echo "# Compatibility conformance"
	echo
	echo "- Run ID: $RUN_ID"
	echo "- Mode: $mode"
	if [ "$DRIFT" = true ]; then
		echo "- Candidate: $DRIFT_CANDIDATE_IMAGE:$DRIFT_CANDIDATE_VERSION"
		echo "- Baseline: $DRIFT_BASELINE_IMAGE:$DRIFT_BASELINE_VERSION"
		echo "- Classification: semantic differences are drift; review_status=needs_review"
	fi
	echo "- Manifest: $MANIFEST"
	echo
	echo "| Case | Fixture | Protocol | Status |"
	echo "| --- | --- | --- | --- |"
} >"$OUT_PATH/summary.md"

if [ "$MOCK" != true ]; then
	while IFS= read -r protocol; do
		[ -n "$protocol" ] || continue

		RUN_ARTIFACT_DIR="$OUT_PATH/.differential/$protocol/$RUN_ID"
		suite_dir="$OUT_PATH/suite/$protocol"
		mkdir -p "$RUN_ARTIFACT_DIR" "$suite_dir/artifacts"
		runner_command=$(printenv COMPAT_RUNNER_CMD 2>/dev/null || true)
		if [ -z "$runner_command" ]; then
			runner_command=$(runner_for_protocol "$protocol" || true)
		fi
		if [ "$DRIFT" = true ] && [ "$protocol" != prometheus ]; then
			runner_command="unsupported: $protocol candidate reference override is not supported by the repository runner; reference is compile-time manifest data"
		fi
		printf '%s\n' "$runner_command" >"$suite_dir/command.txt"
		protocol_cases="$TMP_DIR/$protocol.selected.json"
		ruby -rjson -e '
  document = JSON.parse(File.read(ARGV.fetch(0)))
  protocol = ARGV.fetch(1)
  selected = document.fetch("cases").select { |entry| entry.fetch("protocol") == protocol }
  STDOUT.write(JSON.generate("version" => document.fetch("version"), "cases" => selected))
' "$TMP_DIR/selected.json" "$protocol" >"$protocol_cases"
		stdout_file="$TMP_DIR/$protocol.stdout"
		stderr_file="$TMP_DIR/$protocol.stderr"
		runner_exit_code=127
		runner_started=$(monotonic_seconds)
		if [ "$DRIFT" = true ] && [ "$protocol" != prometheus ]; then
			: >"$stdout_file"
			printf '%s\n' "$runner_command" >"$stderr_file"
			runner_exit_code=125
			protocol_status=infrastructure_failure
			write_json "$suite_dir/configuration_failure.json" "$(printf '{\"classification\":\"infrastructure_failure\",\"reason\":\"candidate_reference_override_unsupported\",\"protocol\":\"%s\",\"candidate_image\":\"%s\",\"candidate_version\":\"%s\",\"release_evidence\":false}' "$protocol" "$DRIFT_CANDIDATE_IMAGE" "$DRIFT_CANDIDATE_VERSION")"
		elif [ -n "$runner_command" ]; then
			set +e
			run_protocol_target "$protocol" "$RUN_ARTIFACT_DIR" "$protocol_cases" "$runner_command" \
				>"$stdout_file" 2>"$stderr_file"
			runner_exit_code=$?
			set -e
		else
			printf '%s\n' "no differential target configured for protocol $protocol" >"$stderr_file"
			: >"$stdout_file"
		fi
		runner_finished=$(monotonic_seconds)
		runner_duration=$(ruby -e 'printf "%.6f\n", ARGV[1].to_f - ARGV[0].to_f' "$runner_started" "$runner_finished")
		printf '%s\n' "$runner_duration" >"$TMP_DIR/durations/$protocol"
		if [ "$runner_exit_code" -eq 0 ]; then
			protocol_status=pass
		else
			protocol_output=$(cat "$stdout_file" "$stderr_file")
			if [ "$DRIFT" = true ]; then
				protocol_status=infrastructure_failure
			else
				protocol_status=$(classify_failure "$protocol_output")
				if [ "$runner_exit_code" -eq 127 ] && [ -z "$runner_command" ]; then
					protocol_status=infrastructure_failure
				fi
			fi
		fi
		receipt_validation_status=not_applicable
		receipt_validation_reason=drift_mode
		if [ "$DRIFT" != true ]; then
			expected_receipt_status=$([ "$runner_exit_code" -eq 0 ] && printf '%s' pass || printf '%s' failure)
			receipt_validation=$(validate_execution_receipt \
				"$RUN_ARTIFACT_DIR/execution-receipt.json" "$protocol_cases" "$protocol" "$RUN_ID" "$expected_receipt_status")
			receipt_validation_status=${receipt_validation%%|*}
			receipt_validation_reason=${receipt_validation#*|}
			if [ "$receipt_validation_status" != pass ] && [ "$protocol_status" = pass ]; then
				protocol_status=$receipt_validation_status
			fi
		fi
		redact_file "$stdout_file" "$suite_dir/runner.stdout"
		redact_file "$stderr_file" "$suite_dir/runner.stderr"
		candidate_reference_image=
		if [ "$DRIFT" = true ] && [ "$protocol" = prometheus ]; then
			candidate_reference_image="$DRIFT_CANDIDATE_IMAGE:$DRIFT_CANDIDATE_VERSION"
		fi
		suite_outcome=$(ruby -rjson -e '
  puts JSON.generate(
    "run_id" => ARGV[0], "protocol" => ARGV[1], "scope" => "suite",
    "status" => ARGV[2], "runner_command" => ARGV[3],
    "runner_exit_code" => Integer(ARGV[4]),
    "artifact_dir" => ARGV[5],
    "selected_cases" => JSON.parse(ARGV[6]).fetch("cases").map { |entry| entry.fetch("id") },
    "selected_case_records" => JSON.parse(ARGV[6]).fetch("cases").map { |entry| {
      "case_id" => entry.fetch("id"), "fixture_id" => entry.fetch("fixture").fetch("id")
    } },
	    "mode" => ARGV[7],
	    "classification" => (ARGV[2] == "drift" ? "drift" : ARGV[2]),
	    "review_status" => (ARGV[2] == "drift" ? "needs_review" : "not_required"),
	    "release_evidence" => (ARGV[7] == "drift" ? false : true),
	    "reference_image" => (ARGV[8].empty? ? nil : ARGV[8]),
	    "execution_receipt_path" => (ARGV[9] == "not_applicable" ? nil : "execution-receipt.json"),
	    "execution_receipt_status" => ARGV[9],
	    "execution_receipt_reason" => ARGV[10]
	  )
' "$RUN_ID" "$protocol" "$protocol_status" "$runner_command" "$runner_exit_code" \
			"$RUN_ARTIFACT_DIR" "$(cat "$protocol_cases")" "$mode" "$candidate_reference_image" "$receipt_validation_status" "$receipt_validation_reason")
		write_json "$suite_dir/outcome.json" "$suite_outcome"
		if [ -n "$(find "$RUN_ARTIFACT_DIR" -type f -print -quit 2>/dev/null)" ]; then
			copy_artifact_tree "$RUN_ARTIFACT_DIR" "$suite_dir/artifacts"
		fi
done < <(ruby -rjson -e 'JSON.parse(File.read(ARGV.fetch(0))).fetch("cases").map { |entry| entry.fetch("protocol") }.uniq.each { |protocol| puts protocol }' "$TMP_DIR/selected.json")
fi

while IFS= read -r case_id; do
	[ -n "$case_id" ] || continue
	case_json=$(ruby -rjson -e '
  document = JSON.parse(File.read(ARGV.fetch(0)))
  entry = document.fetch("cases").find { |item| item["id"] == ARGV.fetch(1) }
  abort "case not found" unless entry
  STDOUT.write(JSON.generate(entry))
' "$TMP_DIR/selected.json" "$case_id")
	protocol=$(json_value 'object.fetch("protocol")' "$case_json")
	protocol=$(printf '%s' "$protocol" | tr -d '"')
	fixture_id=$(json_value 'object.fetch("fixture").fetch("id")' "$case_json")
	fixture_id=$(printf '%s' "$fixture_id" | tr -d '"')
	reference_service=$(json_value 'object.fetch("reference").fetch("service")' "$case_json")
	reference_service=$(printf '%s' "$reference_service" | tr -d '"')
	reference_version=$(json_value 'object.fetch("reference").fetch("version")' "$case_json")
	reference_version=$(printf '%s' "$reference_version" | tr -d '"')
	reference_image=$(reference_image_for_protocol "$protocol")
	if [ "$DRIFT" = true ]; then
		reference_image="$DRIFT_CANDIDATE_IMAGE"
		reference_version="$DRIFT_CANDIDATE_VERSION"
		case_json=$(ruby -rjson -e 'value = JSON.parse(STDIN.read); value["release_evidence"] = false; puts JSON.generate(value)' <<<"$case_json")
	fi
	case_dir="$OUT_PATH/$case_id"
	mkdir -p "$case_dir"
	write_json "$case_dir/case.json" "$case_json"
	request_json=$(json_value '{ "endpoint" => object.fetch("endpoint"), "request" => object.fetch("request") }' "$case_json")
	write_json "$case_dir/request.raw.json" "$request_json"
	write_json "$case_dir/request.normalized.json" "$request_json"

	status=pass
	runner_exit_code=0
	runner_duration=0.000000
	runner_command=
	reason=mock_deterministic_payload
	artifact_count=0
	evidence_scope=none
	evidence_source=
	receipt_validation_status=not_applicable
	receipt_validation_reason=mock_mode
	if [ "$MOCK" = true ]; then
		payload=$(mock_payload "$case_json")
		write_json "$case_dir/softprobe.raw.json" "$payload"
		write_json "$case_dir/softprobe.normalized.json" "$payload"
		write_json "$case_dir/reference.raw.json" "$payload"
		write_json "$case_dir/reference.normalized.json" "$payload"
		diff_json='{"equal":true,"differences":[]}'
		write_json "$case_dir/diff.json" "$diff_json"
	else
		RUN_ARTIFACT_DIR="$OUT_PATH/.differential/$protocol/$RUN_ID"
		RUN_PROTOCOL_CASE_COUNT=$(ruby -rjson -e '
  document = JSON.parse(File.read(ARGV.fetch(0)))
  puts document.fetch("cases").count { |entry| entry.fetch("protocol") == ARGV.fetch(1) }
' "$TMP_DIR/selected.json" "$protocol")
		protocol_status=$(cat "$OUT_PATH/suite/$protocol/outcome.json" | ruby -rjson -e 'puts JSON.parse(STDIN.read).fetch("status")')
		runner_command=$(cat "$OUT_PATH/suite/$protocol/command.txt")
		runner_exit_code=$(cat "$OUT_PATH/suite/$protocol/outcome.json" | ruby -rjson -e 'puts JSON.parse(STDIN.read).fetch("runner_exit_code")')
			runner_duration=$(cat "$TMP_DIR/durations/$protocol" 2>/dev/null || printf '%s' 0.000000)
			status=$protocol_status
			reason="suite_$protocol_status"
			receipt_validation_status=$(cat "$OUT_PATH/suite/$protocol/outcome.json" | ruby -rjson -e 'puts JSON.parse(STDIN.read).fetch("execution_receipt_status")')
			receipt_validation_reason=$(cat "$OUT_PATH/suite/$protocol/outcome.json" | ruby -rjson -e 'puts JSON.parse(STDIN.read).fetch("execution_receipt_reason")')
		if ingest_case_artifacts "$protocol" "$case_id" "$case_dir"; then
			artifact_count=$ARTIFACT_COUNT
			evidence_scope=$EVIDENCE_SCOPE
			evidence_source=$EVIDENCE_SOURCE
		else
			artifact_count=$ARTIFACT_COUNT
			if [ "$status" = pass ]; then
				status=infrastructure_failure
				reason=missing_required_evidence
			fi
		fi
		if [ "$status" = pass ] || { [ "$DRIFT" = true ] && [ "$status" = drift ]; }; then
			if ! write_normalized_diff "$case_dir" "$case_dir/case.json"; then
				if [ "$DRIFT" = true ]; then
					status=drift
					reason=semantic_candidate_difference
				else
					status=product_regression
					reason=normalized_mismatch
				fi
			fi
		fi
		copy_redacted_artifact "$OUT_PATH/suite/$protocol/outcome.json" "$case_dir/suite.evidence.json"
	fi

	suite_artifact_source="$OUT_PATH/suite/$protocol"
	[ "$MOCK" = true ] && suite_artifact_source=mock
	write_case_provenance "$case_dir" "$case_dir/case.json" "$protocol" "$suite_artifact_source" "$evidence_source"
	provenance_validation=$(validate_case_provenance "$case_dir")
	provenance_status=${provenance_validation%%|*}
	provenance_reason=${provenance_validation#*|}
	if [ "$MOCK" != true ] && [ "$provenance_status" != pass ] && { [ "$status" = pass ] || [ "$DRIFT" = true ] && [ "$status" = drift ]; }; then
		if [ "$DRIFT" = true ]; then
			status=infrastructure_failure
			reason=provenance_failure
		else
			status=$provenance_status
			reason=$provenance_reason
		fi
	fi
	classification=$status
	review_status=not_required
	if [ "$DRIFT" = true ] && [ "$status" = drift ]; then
		classification=drift
		review_status=needs_review
	fi

	outcome_json=$(ruby -rjson -e '
  STDOUT.write(JSON.generate("run_id" => ARGV[0], "case_id" => ARGV[1], "protocol" => ARGV[2],
                              "mode" => ARGV[3], "status" => ARGV[4], "runner_command" => ARGV[5],
                              "runner_exit_code" => Integer(ARGV[6]), "reason" => ARGV[7],
                              "artifact_count" => Integer(ARGV[8]),
                              "evidence_scope" => ARGV[9], "evidence_source" => ARGV[10],
                              "provenance_path" => "case_provenance.json",
	                              "provenance_status" => ARGV[16], "provenance_reason" => ARGV[17],
	                              "classification" => ARGV[18], "review_status" => ARGV[19],
	                              "execution_receipt_status" => ARGV[20], "execution_receipt_reason" => ARGV[21],
	                              "release_evidence" => (ARGV[3] == "drift" ? false : true),
                              "fixture_id" => ARGV[11], "reference" => {
                                "service" => ARGV[12], "version" => ARGV[13], "image" => ARGV[14]
                              }, "runner_duration_seconds" => Float(ARGV[15])))
' "$RUN_ID" "$case_id" "$protocol" "$mode" "$status" "$runner_command" "$runner_exit_code" "$reason" "$artifact_count" "$evidence_scope" "$evidence_source" "$fixture_id" "$reference_service" "$reference_version" "$reference_image" "$runner_duration" "$provenance_status" "$provenance_reason" "$classification" "$review_status" "$receipt_validation_status" "$receipt_validation_reason")
	write_json "$case_dir/outcome.json" "$outcome_json"
	report_line=$(ruby -rjson -e '
  puts JSON.generate("run_id" => ARGV[0], "case_id" => ARGV[1], "protocol" => ARGV[2],
                     "fixture_id" => ARGV[3], "status" => ARGV[4], "mode" => ARGV[5],
                     "reference_version" => ARGV[6], "outcome" => ARGV[7],
                     "classification" => ARGV[8], "review_status" => ARGV[9],
                     "release_evidence" => (ARGV[5] == "drift" ? false : true),
                     "provenance_path" => "case_provenance.json")
' "$RUN_ID" "$case_id" "$protocol" "$fixture_id" "$status" "$mode" "$reference_version" "$reason" "$classification" "$review_status")
	printf '%s\n' "$report_line" >>"$OUT_PATH/report.jsonl"
	printf '| %s | %s | %s | %s |\n' "$case_id" "$fixture_id" "$protocol" "$status" >>"$OUT_PATH/summary.md"
	case_count=$((case_count + 1))
	case "$status" in
		pass) pass_count=$((pass_count + 1)) ;;
		drift) drift_cases=$((drift_cases + 1)) ;;
		product_regression) product_regressions=$((product_regressions + 1)) ;;
		infrastructure_failure) infrastructure_failures=$((infrastructure_failures + 1)) ;;
		environment_skip) environment_skips=$((environment_skips + 1)) ;;
	esac
done < <(ruby -rjson -e 'JSON.parse(File.read(ARGV.fetch(0))).fetch("cases").each { |entry| puts entry.fetch("id") }' "$TMP_DIR/selected.json")

write_json "$OUT_PATH/versions.json" "$(ruby -rjson - "$TMP_DIR/selected.json" "$TMP_DIR/durations" "$OUT_PATH/suite" "$RUN_ID" "$MANIFEST_PATH" "$mode" <<'RUBY'
selected_path, duration_dir, suite_dir, run_id, manifest_path, mode = ARGV
document = JSON.parse(File.read(selected_path))
cases = document.fetch("cases")
image_names = {
  "prometheus" => "prom/prometheus",
  "loki" => "grafana/loki",
  "tempo" => "grafana/tempo"
}
protocols = cases.map { |entry| entry.fetch("protocol") }.uniq
runner_records = protocols.to_h do |protocol|
  command_path = File.join(suite_dir, protocol, "command.txt")
  duration_path = File.join(duration_dir, protocol)
  command = File.file?(command_path) ? File.read(command_path).strip : nil
  duration = File.file?(duration_path) ? File.read(duration_path).to_f : 0.0
  [protocol, { "command" => command, "duration_seconds" => duration }]
end
  reference_records = cases.map do |entry|
  reference = entry.fetch("reference")
  protocol = entry.fetch("protocol")
  image = image_names.fetch(protocol)
  record = {
    "case_id" => entry.fetch("id"),
    "fixture_id" => entry.fetch("fixture").fetch("id"),
    "protocol" => protocol,
    "service" => reference.fetch("service"),
    "version" => reference.fetch("version"),
    "image" => image,
    "image_tag" => "#{image}:#{reference.fetch("version")}" 
  }
  if mode == "drift"
    record["baseline"] = { "image" => ENV.fetch("DRIFT_BASELINE_IMAGE"), "version" => ENV.fetch("DRIFT_BASELINE_VERSION") }
    record["candidate"] = { "image" => ENV.fetch("DRIFT_CANDIDATE_IMAGE"), "version" => ENV.fetch("DRIFT_CANDIDATE_VERSION") }
  end
  record
end
puts JSON.generate(
  "run_id" => run_id,
  "manifest" => manifest_path,
  "mode" => mode,
  "ruby" => RUBY_DESCRIPTION,
  "selected_protocols" => protocols,
  "selected_case_ids" => cases.map { |entry| entry.fetch("id") },
  "selected_fixture_ids" => cases.map { |entry| entry.fetch("fixture").fetch("id") },
  "references" => reference_records,
  "runners" => runner_records,
  "release_evidence" => (mode != "drift"),
  "candidate" => (mode == "drift" ? { "image" => ENV.fetch("DRIFT_CANDIDATE_IMAGE"), "version" => ENV.fetch("DRIFT_CANDIDATE_VERSION") } : nil),
  "baseline" => (mode == "drift" ? { "image" => ENV.fetch("DRIFT_BASELINE_IMAGE"), "version" => ENV.fetch("DRIFT_BASELINE_VERSION") } : nil)
)
RUBY
)"

{
	echo
	echo "Cases: $case_count"
	echo "Pass: $pass_count"
	echo "Drift cases: $drift_cases"
	echo "Infrastructure failures: $infrastructure_failures"
	echo "Product regressions: $product_regressions"
	echo "Environment skips: $environment_skips"
} >>"$OUT_PATH/summary.md"

if [ "$product_regressions" -gt 0 ] || [ "$infrastructure_failures" -gt 0 ] || [ "$environment_skips" -gt 0 ]; then
	exit 1
fi
exit 0
