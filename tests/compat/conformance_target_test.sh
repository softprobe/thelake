#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)

real_plan=$(make --no-print-directory -n test-compat 2>&1)
grep -Fq 'scripts/compat/conformance.sh' <<<"$real_plan"
grep -Fq 'real) ;;' <<<"$real_plan"
grep -Fq 'mock) args+=(--mock)' <<<"$real_plan"

# The scheduled drift report must derive every release-evidence input before
# evaluating the gate.
ruby - "$ROOT_DIR/.github/workflows/compatibility.yml" <<'RUBY'
workflow = File.read(ARGV.fetch(0))
drift_report = workflow[/^  drift-report:.*?(?=^  [A-Za-z0-9_-]+:|\z)/m]
abort "missing drift-report workflow job" unless drift_report
release_index = drift_report.index("release_evidence =")
abort "missing drift release-evidence calculation" unless release_index
abort "drift report must remain non-release evidence" unless drift_report.include?('"release_evidence": False')
report_index = drift_report.index("report = {")
abort "drift report must calculate release evidence before serializing the report" unless report_index && release_index < report_index
RUBY

MANIFEST="$ROOT_DIR/tests/compat/manifests/cases.v0.yaml"
grep -Fq 'runner_case_id:' "$MANIFEST"
ruby -ryaml - "$MANIFEST" <<'RUBY'
document = YAML.load_file(ARGV.fetch(0))
release_cases = document.fetch("cases").select do |entry|
  %w[loki tempo].include?(entry.fetch("protocol")) && entry.fetch("evidence").fetch("retain")
end
release_cases.each do |entry|
  runner_case_id = entry["runner_case_id"]
  case_id = entry.fetch("id")
  # Conformance exclusions (with a recorded reason and release_evidence=false)
  # are allowed to skip differential execution; anything else must map 1:1 to
  # a protocol-runner case.
  excluded = entry["conformance_exclusion"].is_a?(Hash) && entry["conformance_exclusion"]["release_evidence"] == false
  abort "release-selected case lacks runner_case_id: #{case_id}" unless excluded || (runner_case_id.is_a?(String) && !runner_case_id.empty?)
end
executable = release_cases.select { |entry| entry["runner_case_id"].is_a?(String) && !entry["runner_case_id"].empty? }
runner_pairs = executable.map { |entry| [entry.fetch("id"), entry["runner_case_id"]] }
abort "release-selected runner_case_id mapping is not one-to-one" unless runner_pairs.map(&:last).uniq.length == runner_pairs.length
abort "unexpected prometheus release cases remain" unless executable.none? { |entry| entry.fetch("protocol") == "prometheus" }
RUBY
if grep -Fq "make test-prom-compat" "$ROOT_DIR/scripts/compat/conformance.sh"; then
	echo "conformance must not use the broad Prometheus compatibility suite" >&2
	exit 1
fi
if grep -Eq 'prometheus' "$ROOT_DIR/scripts/compat/conformance.sh"; then
	echo "conformance must not retain prometheus protocol support" >&2
	exit 1
fi
grep -Fq 'COMPAT_CASE_IDS=' "$ROOT_DIR/scripts/compat/conformance.sh"
if grep -Fq 'COMPAT_CASE_ID=__suite__' "$ROOT_DIR/scripts/compat/conformance.sh"; then
	echo "conformance must not send the suite sentinel to protocol runners" >&2
	exit 1
fi

static_plan=$(make --no-print-directory -n test-grafana-static 2>&1)
grep -Fq 'GRAFANA_COMPOSE_IMAGE=' <<<"$static_plan"
# Avoid `make -n test-grafana-system`: its recipe contains $(MAKE) which GNU Make
# still executes under -n (check-grafana-reference-pin → docker pull).
grep -Fq 'GRAFANA_COMPOSE_IMAGE=' "$ROOT_DIR/Makefile"
grep -Eq '^test-grafana-system:' "$ROOT_DIR/Makefile"

# otel_collector is a manual Grafana demo dependency, not a conformance
# oracle or CI pull.  Keep it out of the immutable reference-service gate
# until the manual demo is promoted into a reproducible CI lane.
if grep -R -n -E 'otel/opentelemetry-collector|otel_collector' \
	"$ROOT_DIR/.github/workflows" "$ROOT_DIR/Makefile" \
	"$ROOT_DIR/tests/compat/grafana/docker-compose.ci.yml" >/dev/null 2>&1; then
	echo "otel_collector unexpectedly entered the supported CI/reference pull set" >&2
	exit 1
fi

pin_output=$(make --no-print-directory check-compat-reference-pins)
grep -Fq 'loki:' <<<"$pin_output"
grep -Fq 'tempo:' <<<"$pin_output"
grep -Fq 'grafana:' <<<"$pin_output"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/compat-target-test.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

protocol_label() {
	case "$1" in
		loki) printf '%s\n' Loki ;;
		tempo) printf '%s\n' Tempo ;;
		grafana) printf '%s\n' Grafana ;;
		*) printf '%s\n' "$1" ;;
	esac
}

# Exercise Loki mock selection so discovery cases cannot be silently dropped.
loki_selection_dir="$tmp_dir/loki-selection"
scripts/compat/conformance.sh --mock --protocol loki --out "$loki_selection_dir" >/dev/null
ruby -rjson - "$loki_selection_dir" <<'RUBY'
root = ARGV.fetch(0)
report = File.readlines(File.join(root, "report.jsonl"), chomp: true).map { |line| JSON.parse(line) }
abort "expected Loki mock report rows" if report.empty?
abort "Loki mock cases were not reported as pass" unless report.all? { |entry| entry["status"] == "pass" && entry["runner_case_id"].is_a?(String) }
RUBY

first_loki_case=$(ruby -ryaml -e '
document = YAML.load_file(ARGV.fetch(0))
puts document.fetch("cases").find { |entry| entry.fetch("protocol") == "loki" }.fetch("id")
' "$MANIFEST")
case_dir="$tmp_dir/loki-case-$first_loki_case"
scripts/compat/conformance.sh --mock --case "$first_loki_case" --out "$case_dir" >/dev/null
test "$(ruby -rjson -e 'puts File.readlines(File.join(ARGV.fetch(0), "report.jsonl"), chomp: true).reject(&:empty?).length' "$case_dir")" -eq 1

# Keep the exclusion path fail-closed: a temporary manifest that drops a
# runner mapping without declaring an explicit non-release exclusion must be
# rejected rather than silently treated as executable or reference-only.
cp "$MANIFEST" "$tmp_dir/missing-runner.yaml"
ruby -ryaml - "$tmp_dir/missing-runner.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("cases").first.delete("runner_case_id")
File.write(path, YAML.dump(document))
RUBY
set +e
missing_runner_output=$(MANIFEST="$tmp_dir/missing-runner.yaml" scripts/compat/conformance.sh --mock --protocol loki --out "$tmp_dir/missing-runner" 2>&1)
missing_runner_status=$?
set -e
test "$missing_runner_status" -eq 2
grep -Fq 'missing runner_case_id requires an explicit non-release conformance_exclusion reason' <<<"$missing_runner_output"

# An unknown capability must fail manifest validation before any allowlisted
# difference can be approved.
cp "$MANIFEST" "$tmp_dir/unknown-allowlist.yaml"
ruby -ryaml - "$tmp_dir/unknown-allowlist.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("cases").first["unsupported_features"] = {
  "capability.not-in-canonical-manifest" => ["$.response.data"]
}
File.write(path, YAML.dump(document))
RUBY
set +e
unknown_allowlist_output=$(MANIFEST="$tmp_dir/unknown-allowlist.yaml" scripts/compat/conformance.sh --mock --case "$first_loki_case" --out "$tmp_dir/unknown-allowlist" 2>&1)
unknown_allowlist_status=$?
set -e
test "$unknown_allowlist_status" -eq 2
grep -Fq 'unsupported-feature entry references unknown capability' <<<"$unknown_allowlist_output"

# An unknown feature name must fail against the canonical capability registry.
cp "$MANIFEST" "$tmp_dir/unknown-feature-allowlist.yaml"
ruby -ryaml - "$tmp_dir/unknown-feature-allowlist.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("cases").first["unsupported_features"] = {
  "capability" => "loki.query",
  "feature" => "feature.not-in-canonical-capability-manifest",
  "path" => "$.response.data"
}
File.write(path, YAML.dump(document))
RUBY
set +e
unknown_feature_output=$(MANIFEST="$tmp_dir/unknown-feature-allowlist.yaml" scripts/compat/conformance.sh --mock --case "$first_loki_case" --out "$tmp_dir/unknown-feature-allowlist" 2>&1)
unknown_feature_status=$?
set -e
test "$unknown_feature_status" -eq 2
grep -Fq 'unsupported-feature "feature.not-in-canonical-capability-manifest" is not declared for capability loki.query' <<<"$unknown_feature_output"

cp docs/compat/references.v0.yaml "$tmp_dir/references.yaml"
ruby -ryaml - "$tmp_dir/references.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("references").fetch("loki")["tag"] = "3.9.0"
File.write(path, YAML.dump(document))
RUBY
set +e
drift_output=$(COMPAT_REFERENCE_MANIFEST="$tmp_dir/references.yaml" make --no-print-directory check-compat-reference-pins 2>&1)
drift_status=$?
set -e
test "$drift_status" -ne 0
grep -Fq 'Loki reference drift' <<<"$drift_output"

cp docs/compat/references.v0.yaml "$tmp_dir/empty-tag.yaml"
ruby -ryaml - "$tmp_dir/empty-tag.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("references").fetch("loki")["tag"] = ""
File.write(path, YAML.dump(document))
RUBY
set +e
empty_tag_output=$(COMPAT_REFERENCE_MANIFEST="$tmp_dir/empty-tag.yaml" COMPAT_REFERENCE_CANONICAL_MANIFEST="$tmp_dir/empty-tag.yaml" COMPAT_REFERENCE_ALLOW_MANIFEST_OVERRIDE=1 make --no-print-directory check-compat-reference-pins 2>&1)
empty_tag_status=$?
set -e
test "$empty_tag_status" -ne 0
grep -Eq 'loki reference requires a non-empty image and tag|reference is missing a valid immutable sha256 digest' <<<"$empty_tag_output"

for protocol in loki tempo grafana; do
	cp docs/compat/references.v0.yaml "$tmp_dir/$protocol-drift.yaml"
	ruby -ryaml - "$tmp_dir/$protocol-drift.yaml" "$protocol" <<'RUBY'
path, protocol = ARGV
document = YAML.load_file(path)
document.fetch("references").fetch(protocol)["tag"] = "drift-version"
File.write(path, YAML.dump(document))
RUBY
	set +e
	metadata_drift_output=$(COMPAT_REFERENCE_MANIFEST="$tmp_dir/$protocol-drift.yaml" make --no-print-directory check-compat-reference-pins 2>&1)
	metadata_drift_status=$?
	set -e
	test "$metadata_drift_status" -ne 0
	grep -Fq "$(protocol_label "$protocol") reference drift from canonical manifest" <<<"$metadata_drift_output"
done

cp "$MANIFEST" "$tmp_dir/duplicate-runner.yaml"
ruby -ryaml - "$tmp_dir/duplicate-runner.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
loki = document.fetch("cases").select { |entry| entry.fetch("protocol") == "loki" }
abort "need at least two Loki cases" unless loki.length >= 2
loki.fetch(1)["runner_case_id"] = loki.fetch(0).fetch("runner_case_id")
File.write(path, YAML.dump(document))
RUBY
set +e
duplicate_output=$(MANIFEST="$tmp_dir/duplicate-runner.yaml" COMPAT_REFERENCE_MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml" scripts/compat/conformance.sh --mock --protocol loki --out "$tmp_dir/duplicate-runner" 2>&1)
duplicate_status=$?
set -e
test "$duplicate_status" -ne 0
grep -Fq 'duplicate runner_case_id' <<<"$duplicate_output"

set +e
sentinel_output=$(scripts/compat/conformance.sh --mock --case __suite__ --out "$tmp_dir/sentinel" 2>&1)
sentinel_status=$?
set -e
test "$sentinel_status" -ne 0
grep -Fq 'suite sentinel' <<<"$sentinel_output"

for protocol in loki tempo; do
	cp tests/compat/manifests/cases.v0.yaml "$tmp_dir/$protocol-case.yaml"
	ruby -ryaml - "$tmp_dir/$protocol-case.yaml" "$protocol" <<'RUBY'
path, protocol = ARGV
document = YAML.load_file(path)
document.fetch("cases").find { |entry| entry.fetch("protocol") == protocol }.fetch("reference")["version"] = "drift-version"
File.write(path, YAML.dump(document))
RUBY
	set +e
	case_output=$(MANIFEST="$tmp_dir/$protocol-case.yaml" COMPAT_REFERENCE_MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml" scripts/compat/conformance.sh --mock --protocol "$protocol" --out "$tmp_dir/$protocol-mismatched-case" 2>&1)
	case_status=$?
	set -e
	test "$case_status" -ne 0
	grep -Fq 'reference version drift' <<<"$case_output"
done

cp tests/compat/manifests/cases.v0.yaml "$tmp_dir/metadata-drift.yaml"
ruby -ryaml - "$tmp_dir/metadata-drift.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("metadata").fetch("reference_pins").fetch("protocols").fetch("loki")["tag"] = "drift-version"
File.write(path, YAML.dump(document))
RUBY
set +e
metadata_case_output=$(MANIFEST="$tmp_dir/metadata-drift.yaml" COMPAT_REFERENCE_MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml" scripts/compat/conformance.sh --mock --protocol loki --out "$tmp_dir/metadata-mismatch" 2>&1)
metadata_case_status=$?
set -e
test "$metadata_case_status" -ne 0
grep -Fq 'metadata.reference_pins.protocols.loki.tag drift' <<<"$metadata_case_output"

cp tests/compat/manifests/cases.v0.yaml "$tmp_dir/metadata-cases.yaml"
ruby -ryaml - "$tmp_dir/metadata-cases.yaml" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("metadata").fetch("reference_pins").fetch("protocols").fetch("loki")["digest"] = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
File.write(path, YAML.dump(document))
RUBY
set +e
metadata_output=$(MANIFEST="$tmp_dir/metadata-cases.yaml" COMPAT_REFERENCE_MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml" COMPAT_CONFORMANCE_MODE=mock scripts/compat/conformance.sh --mock --protocol loki --out "$tmp_dir/mismatched-metadata" 2>&1)
metadata_status=$?
set -e
test "$metadata_status" -ne 0
grep -Fq 'metadata.reference_pins.protocols.loki.digest drift' <<<"$metadata_output"

COMPAT_CONFORMANCE_MODE=mock \
COMPAT_CONFORMANCE_OUT="$tmp_dir/mock" \
	make --no-print-directory test-compat >/dev/null

test "$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).fetch("mode")' "$tmp_dir/mock/versions.json")" = mock
grep -Fq 'not service-backed compatibility evidence' "$tmp_dir/mock/NOTICE.txt"
test -s "$tmp_dir/mock/artifact-index.json"
test -s "$tmp_dir/mock/execution-receipt.json"
test -s "$tmp_dir/mock/outcome.json"
ruby -rjson - "$tmp_dir/mock/execution-receipt.json" <<'RUBY'
receipt = JSON.parse(File.read(ARGV.fetch(0)))
selected = receipt.fetch("selected_case_ids")
runner = receipt.fetch("selected_runner_case_ids")
records = receipt.fetch("cases")
excluded_ids = %w[
  tempo-search-span-selector
  tempo-search-tags
  tempo-tag-values-peer-service
]
abort "selection receipt lost runner_case_id mapping" unless runner.zip(selected).all? { |id, case_id|
  if excluded_ids.include?(case_id)
    id.nil?
  else
    id.is_a?(String) && !id.empty?
  end
}
abort "selection receipt case/runner mapping length mismatch" unless selected.length == runner.length
records.each_with_index do |record, index|
  abort "selection receipt lost manifest case_id" unless record.fetch("case_id") == selected.fetch(index)
  abort "selection receipt lost runner_case_id" unless record.fetch("runner_case_id") == runner.fetch(index)
end
RUBY
scripts/compat/validate-artifacts.sh --root "$tmp_dir/mock"

set +e
invalid_output=$(COMPAT_CONFORMANCE_MODE=invalid make --no-print-directory test-compat 2>&1)
invalid_status=$?
set -e
test "$invalid_status" -eq 2
grep -Fq 'COMPAT_CONFORMANCE_MODE must be real or mock' <<<"$invalid_output"

echo "compatibility target regression: PASS"
