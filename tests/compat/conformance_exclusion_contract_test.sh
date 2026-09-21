#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
MANIFEST="$ROOT_DIR/tests/compat/manifests/cases.v0.yaml"
REFERENCE_MANIFEST="$ROOT_DIR/docs/compat/references.v0.yaml"
CAPABILITY_MANIFEST="$ROOT_DIR/docs/compat/capability.v0.yaml"
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/compat-exclusion-contract.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

null_runner_manifest="$tmp_dir/null-runner.yaml"
cp "$MANIFEST" "$null_runner_manifest"
ruby -ryaml - "$null_runner_manifest" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
loki = document.fetch("cases").select { |entry| entry.fetch("protocol") == "loki" }
abort "expected at least one Loki case" unless loki.length >= 1
loki.fetch(0)["runner_case_id"] = nil
loki.fetch(0)["conformance_exclusion"] = {
  "reason" => "runner does not expose this reference-only case",
  "release_evidence" => false
}
File.write(path, YAML.dump(document))
RUBY

mock_output="$tmp_dir/mock"
excluded_case_id=$(ruby -ryaml -e '
document = YAML.load_file(ARGV.fetch(0))
loki = document.fetch("cases").select { |entry| entry.fetch("protocol") == "loki" }
puts loki.fetch(0).fetch("id")
' "$null_runner_manifest")
MANIFEST="$null_runner_manifest" \
COMPAT_REFERENCE_MANIFEST="$REFERENCE_MANIFEST" \
CAPABILITY_MANIFEST="$CAPABILITY_MANIFEST" \
  "$ROOT_DIR/scripts/compat/conformance.sh" --mock --protocol loki --out "$mock_output" >/dev/null

ruby -rjson - "$mock_output" "$excluded_case_id" <<'RUBY'
root = ARGV.fetch(0)
excluded_case_id = ARGV.fetch(1)
report = File.readlines(File.join(root, "report.jsonl"), chomp: true).reject(&:empty?).map { |line| JSON.parse(line) }
abort "expected Loki mock report rows" if report.empty?
excluded = report.find { |entry| entry["case_id"] == excluded_case_id }
abort "excluded case missing from report" unless excluded
abort "excluded case did not remain skipped" unless excluded["status"] == "skipped" && excluded["outcome"] == "conformance_exclusion"
abort "excluded case was treated as release evidence" unless excluded["release_evidence"] == false
abort "excluded case lost its nullable runner metadata" unless JSON.parse(File.read(File.join(root, excluded_case_id, "case.json")))["runner_case_id"].nil?
executable = report.reject { |entry| entry["case_id"] == excluded_case_id }
abort "executable mock cases were not reported as pass" unless executable.all? { |entry| entry["status"] == "pass" && entry["runner_case_id"].is_a?(String) }
RUBY

unknown_capability_manifest="$tmp_dir/unknown-capability.yaml"
cp "$MANIFEST" "$unknown_capability_manifest"
ruby -ryaml - "$unknown_capability_manifest" <<'RUBY'
path = ARGV.fetch(0)
document = YAML.load_file(path)
document.fetch("cases").first["unsupported_features"] = {
  "capability.not-in-canonical-manifest" => ["$.response.data"]
}
File.write(path, YAML.dump(document))
RUBY

first_case_id=$(ruby -ryaml -e 'puts YAML.load_file(ARGV.fetch(0)).fetch("cases").fetch(0).fetch("id")' "$unknown_capability_manifest")
set +e
unknown_output=$(MANIFEST="$unknown_capability_manifest" \
  COMPAT_REFERENCE_MANIFEST="$REFERENCE_MANIFEST" \
  CAPABILITY_MANIFEST="$CAPABILITY_MANIFEST" \
  "$ROOT_DIR/scripts/compat/conformance.sh" --mock --case "$first_case_id" --out "$tmp_dir/unknown-capability" 2>&1)
unknown_status=$?
set -e
test "$unknown_status" -eq 2
grep -Fq 'unsupported-feature entry references unknown capability' <<<"$unknown_output"

echo "conformance exclusion contract: PASS"
