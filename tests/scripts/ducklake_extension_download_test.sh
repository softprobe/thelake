#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
downloader="$repo_root/scripts/download-ducklake-extension.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

[[ -x "$downloader" ]] || fail "extension downloader is missing or not executable"
[[ ! -e "$repo_root/scripts/build-ducklake-extension.sh" ]] || fail "source build script remains"
[[ ! -e "$repo_root/scripts/patches/ducklake-newer-than-v1.5.5.patch" ]] || fail "source patch remains"
[[ -f "$repo_root/scripts/ducklake-extension.lock" ]] || fail "pinned public release lock is missing"
awk '
  $1 == "repository" && $2 == "softprobe/ducklake" { repository = 1 }
  $1 == "release" && $2 ~ /^duckdb-v1[.]5[.]6-[[:xdigit:]]+$/ { release = 1 }
  $1 == "duckdb_version" && $2 == "1.5.6" { version = 1 }
  ($1 == "linux_amd64" || $1 == "osx_arm64") && length($2) == 64 && $2 !~ /[^[:xdigit:]]/ && length($3) == 64 && $3 !~ /[^[:xdigit:]]/ { platforms++ }
  END { exit !(repository && release && version && platforms == 2) }
' "$repo_root/scripts/ducklake-extension.lock" || fail "release lock does not pin both tested platforms and digests"

awk '
  /^  manifest-conformance:/ { in_job = 1; next }
  in_job && /^  [[:alnum:]_-]+:/ { exit }
  in_job && /- name: Download pinned DuckLake extension/ { found = 1 }
  END { exit !found }
' "$repo_root/.github/workflows/compatibility.yml" || fail "manifest conformance job does not download the pinned DuckLake extension"

mkdir -p "$tmp/assets/linux_amd64" "$tmp/bin" "$tmp/target"
printf 'pinned extension bytes\n' > "$tmp/assets/linux_amd64/ducklake.duckdb_extension"
printf 'duckdb_version=1.5.6\nducklake_source=test\nplatform=linux_amd64\n' > "$tmp/assets/linux_amd64/BUILD.txt"
tar -czf "$tmp/ducklake-v1.5.6-linux_amd64.tar.gz" -C "$tmp/assets" linux_amd64
printf 'pinned macOS extension bytes\n' > "$tmp/assets/osx_arm64.duckdb_extension"
mkdir "$tmp/assets/osx_arm64"
mv "$tmp/assets/osx_arm64.duckdb_extension" "$tmp/assets/osx_arm64/ducklake.duckdb_extension"
printf 'duckdb_version=1.5.6\nducklake_source=test\nplatform=osx_arm64\n' > "$tmp/assets/osx_arm64/BUILD.txt"
tar -czf "$tmp/ducklake-v1.5.6-osx_arm64.tar.gz" -C "$tmp/assets" osx_arm64
linux_archive_sha="$(shasum -a 256 "$tmp/ducklake-v1.5.6-linux_amd64.tar.gz" | awk '{print $1}')"
linux_extension_sha="$(shasum -a 256 "$tmp/assets/linux_amd64/ducklake.duckdb_extension" | awk '{print $1}')"
mac_archive_sha="$(shasum -a 256 "$tmp/ducklake-v1.5.6-osx_arm64.tar.gz" | awk '{print $1}')"
mac_extension_sha="$(shasum -a 256 "$tmp/assets/osx_arm64/ducklake.duckdb_extension" | awk '{print $1}')"
cat > "$tmp/lock" <<EOF
repository softprobe/ducklake
release duckdb-v1.5.6-test
duckdb_version 1.5.6
linux_amd64 $linux_archive_sha $linux_extension_sha
osx_arm64 $mac_archive_sha $mac_extension_sha
EOF
cat > "$tmp/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == --fail && "$2" == --location ]] || exit 30
while (($#)); do
  if [[ "$1" == --output ]]; then shift; out="$1"; fi
  url="$1"
  shift || true
done
case "$url" in
  https://github.com/softprobe/ducklake/releases/download/duckdb-v1.5.6-test/ducklake-v1.5.6-linux_amd64.tar.gz) cp "$FAKE_GH_ASSET_LINUX" "$out" ;;
  https://github.com/softprobe/ducklake/releases/download/duckdb-v1.5.6-test/ducklake-v1.5.6-osx_arm64.tar.gz) cp "$FAKE_GH_ASSET_MAC" "$out" ;;
  *) exit 31 ;;
esac
EOF
chmod +x "$tmp/bin/curl"

PATH="$tmp/bin:$PATH" \
FAKE_GH_ASSET_LINUX="$tmp/ducklake-v1.5.6-linux_amd64.tar.gz" \
FAKE_GH_ASSET_MAC="$tmp/ducklake-v1.5.6-osx_arm64.tar.gz" \
DUCKLAKE_EXTENSION_LOCK_FILE="$tmp/lock" \
DUCKLAKE_EXTENSION_PLATFORM=linux_amd64 \
DUCKLAKE_EXTENSION_TARGET_DIR="$tmp/target" \
  "$downloader"
cmp "$tmp/assets/linux_amd64/ducklake.duckdb_extension" "$tmp/target/ducklake.duckdb_extension"

PATH="$tmp/bin:$PATH" \
FAKE_GH_ASSET_LINUX="$tmp/ducklake-v1.5.6-linux_amd64.tar.gz" \
FAKE_GH_ASSET_MAC="$tmp/ducklake-v1.5.6-osx_arm64.tar.gz" \
DUCKLAKE_EXTENSION_LOCK_FILE="$tmp/lock" \
DUCKLAKE_EXTENSION_PLATFORM=osx_arm64 \
DUCKLAKE_EXTENSION_TARGET_DIR="$tmp/macos-target" \
  "$downloader"
cmp "$tmp/assets/osx_arm64/ducklake.duckdb_extension" "$tmp/macos-target/ducklake.duckdb_extension"

if PATH="$tmp/bin:$PATH" \
   FAKE_GH_ASSET_LINUX="$tmp/ducklake-v1.5.6-linux_amd64.tar.gz" \
   FAKE_GH_ASSET_MAC="$tmp/ducklake-v1.5.6-osx_arm64.tar.gz" \
   DUCKLAKE_EXTENSION_LOCK_FILE="$tmp/lock" \
   DUCKLAKE_EXTENSION_PLATFORM=windows_amd64 \
   DUCKLAKE_EXTENSION_TARGET_DIR="$tmp/unsupported" \
   "$downloader" >/dev/null 2>&1; then
  fail "unsupported platform was accepted"
fi

printf 'tampered archive\n' > "$tmp/ducklake-v1.5.6-linux_amd64.tar.gz"
if PATH="$tmp/bin:$PATH" \
   FAKE_GH_ASSET_LINUX="$tmp/ducklake-v1.5.6-linux_amd64.tar.gz" \
   FAKE_GH_ASSET_MAC="$tmp/ducklake-v1.5.6-osx_arm64.tar.gz" \
   DUCKLAKE_EXTENSION_LOCK_FILE="$tmp/lock" \
   DUCKLAKE_EXTENSION_PLATFORM=linux_amd64 \
   DUCKLAKE_EXTENSION_TARGET_DIR="$tmp/tampered" \
   "$downloader" >/dev/null 2>&1; then
  fail "archive with a mismatched SHA-256 was accepted"
fi

echo "DuckLake extension download contract passed"
