#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lock_file="${DUCKLAKE_EXTENSION_LOCK_FILE:-$repo_root/scripts/ducklake-extension.lock}"
target_dir="${DUCKLAKE_EXTENSION_TARGET_DIR:-$repo_root/target/ducklake-extension}"
extension="$target_dir/ducklake.duckdb_extension"

[[ -f "$lock_file" ]] || { echo "DuckLake extension lock is missing: $lock_file" >&2; exit 1; }

repository=""
release=""
duckdb_version=""
platform_sha=""
extension_sha=""
platform="${DUCKLAKE_EXTENSION_PLATFORM:-}"
if [[ -z "$platform" ]]; then
  case "$(uname -s):$(uname -m)" in
    Linux:x86_64) platform=linux_amd64 ;;
    Linux:aarch64|Linux:arm64) platform=linux_arm64 ;;
    Darwin:arm64) platform=osx_arm64 ;;
    Darwin:x86_64) platform=osx_amd64 ;;
    *) echo "Unsupported DuckLake extension platform: $(uname -s)/$(uname -m)" >&2; exit 1 ;;
  esac
fi

while read -r key value hash extra; do
  [[ -z "${key:-}" || "$key" == \#* ]] && continue
  [[ -z "${extra:-}" ]] || { echo "Invalid DuckLake extension lock row: $key" >&2; exit 1; }
  case "$key" in
    repository) repository="$value" ;;
    release) release="$value" ;;
    duckdb_version) duckdb_version="$value" ;;
    "$platform") platform_sha="$value"; extension_sha="$hash" ;;
    *) ;;
  esac
done < "$lock_file"

[[ -n "$repository" && -n "$release" && -n "$duckdb_version" ]] || {
  echo "Incomplete DuckLake extension lock: $lock_file" >&2
  exit 1
}
[[ "$platform_sha" =~ ^[[:xdigit:]]{64}$ && "$extension_sha" =~ ^[[:xdigit:]]{64}$ ]] || {
  echo "No pinned DuckLake extension for $platform in $lock_file" >&2
  exit 1
}

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

if [[ -f "$extension" ]] && [[ "$(sha256 "$extension")" == "$extension_sha" ]]; then
  echo "Using pinned DuckLake $duckdb_version extension for $platform"
  exit 0
fi

command -v curl >/dev/null 2>&1 || {
  echo "curl is required to download the pinned DuckLake build $release" >&2
  exit 1
}
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
archive="ducklake-v${duckdb_version}-${platform}.tar.gz"
curl --fail --location --retry 3 \
  --output "$tmp/$archive" \
  "https://github.com/$repository/releases/download/$release/$archive"
[[ -s "$tmp/$archive" ]] || { echo "DuckLake release is missing $archive" >&2; exit 1; }
actual_archive_sha="$(sha256 "$tmp/$archive")"
[[ "$actual_archive_sha" == "$platform_sha" ]] || {
  echo "DuckLake archive checksum mismatch for $platform" >&2
  exit 1
}

tar -xOzf "$tmp/$archive" "$platform/ducklake.duckdb_extension" > "$tmp/ducklake.duckdb_extension"
actual_extension_sha="$(sha256 "$tmp/ducklake.duckdb_extension")"
[[ "$actual_extension_sha" == "$extension_sha" ]] || {
  echo "DuckLake extension checksum mismatch for $platform" >&2
  exit 1
}
mkdir -p "$target_dir"
install -m 0644 "$tmp/ducklake.duckdb_extension" "$extension.tmp"
mv -f "$extension.tmp" "$extension"
echo "Downloaded pinned DuckLake $duckdb_version extension for $platform"
