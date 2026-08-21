#!/usr/bin/env bash

set -euo pipefail

manifest=${COMPAT_REFERENCE_MANIFEST:-docs/compat/references.v0.yaml}
test -f "$manifest" || { echo "missing compatibility reference manifest: $manifest" >&2; exit 1; }

ruby -ryaml - "$manifest" <<'RUBY'
manifest = YAML.load_file(ARGV.fetch(0))
manifest.fetch("references").each do |name, reference|
  prefix = name.upcase
  image = reference.fetch("image")
  tag = reference.fetch("tag")
  digest = reference["digest"]
  immutable = digest && !digest.empty? ? "#{image}@#{digest}" : "#{image}:#{tag}"
  puts "BASELINE_#{prefix}_IMAGE=#{immutable}"
  puts "BASELINE_#{prefix}_VERSION=#{tag}"
  puts "BASELINE_#{prefix}_REFERENCE=#{immutable}"
end
RUBY
