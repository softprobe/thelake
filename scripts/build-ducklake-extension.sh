#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="$repo_root/target/ducklake-extension-src"
build_dir="$repo_root/target/ducklake-extension"
ducklake_base="ac7595b0a1305bea3d4cfaca763b0ce964c763a2"
ducklake_feature="4ab9b12411c91827d383a7b3b67e9c942b084b47"

if [[ ! -d "$source_dir/.git" ]]; then
  git clone https://github.com/duckdb/ducklake.git "$source_dir"
fi

git -C "$source_dir" fetch origin "$ducklake_base"
if [[ "$(git -C "$source_dir" rev-parse HEAD)" != "$ducklake_base" ]]; then
  git -C "$source_dir" checkout --detach "$ducklake_base"
  git -C "$source_dir" reset --hard "$ducklake_base"
fi
git -C "$source_dir" submodule update --init --recursive
if ! git -C "$source_dir" apply --reverse --check "$repo_root/scripts/patches/ducklake-newer-than-v1.5.5.patch"; then
  git -C "$source_dir" apply --check "$repo_root/scripts/patches/ducklake-newer-than-v1.5.5.patch"
  git -C "$source_dir" apply "$repo_root/scripts/patches/ducklake-newer-than-v1.5.5.patch"
fi

if [[ "$(cat "$source_dir/.github/duckdb-version")" != "v1.5.5" ]]; then
  echo "DuckLake source no longer pins DuckDB v1.5.5" >&2
  exit 1
fi

extension="$source_dir/build/release/extension/ducklake/ducklake.duckdb_extension"
if [[ ! -f "$extension" ]]; then
  make -C "$source_dir/duckdb" setup-vcpkg
  VCPKG_TOOLCHAIN_PATH="$source_dir/duckdb/vcpkg/scripts/buildsystems/vcpkg.cmake" \
    GEN=ninja make -C "$source_dir" release
fi
"$source_dir/build/release/test/unittest" --test-dir "$source_dir" \
  "test/sql/compaction/merge_adjacent_newer_than.test"
test -f "$extension"
mkdir -p "$build_dir"
cp -f "$extension" "$build_dir/ducklake.duckdb_extension"

"$source_dir/build/release/duckdb" -unsigned -c \
  "LOAD '$build_dir/ducklake.duckdb_extension'; SELECT count(*) > 0 AS newer_than_supported FROM duckdb_functions() WHERE function_name = 'ducklake_merge_adjacent_files' AND list_contains(parameters, 'newer_than');" \
  | rg -q 'true'

echo "Built DuckLake with newer_than from $ducklake_base + $ducklake_feature"
