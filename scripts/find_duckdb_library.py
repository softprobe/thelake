#!/usr/bin/env python3
"""Find the DuckDB library matching Cargo.lock for the current build host."""

from __future__ import annotations

import argparse
import platform
import re
import tomllib
from pathlib import Path


def engine_version(crate_version: str) -> str:
    match = re.fullmatch(r"1\.([0-9])([0-9]{2})([0-9]{2})\.[0-9]+", crate_version)
    if match is None:
        raise ValueError(f"Cannot decode DuckDB crate version {crate_version!r}")
    major, minor, patch = match.groups()
    return f"{major}.{int(minor)}.{int(patch)}"


def host_triple() -> str:
    system = platform.system()
    machine = platform.machine().lower()
    if system == "Darwin":
        arch = "aarch64" if machine in {"arm64", "aarch64"} else "x86_64"
        return f"{arch}-apple-darwin"
    if system == "Linux":
        arch = "aarch64" if machine in {"arm64", "aarch64"} else "x86_64"
        return f"{arch}-unknown-linux-gnu"
    raise ValueError(f"Unsupported build host {system}/{machine}")


def find_library(lockfile: Path, library_root: Path, triple: str | None = None) -> Path:
    packages = tomllib.loads(lockfile.read_text(encoding="utf-8"))["package"]
    crate_version = next(
        package["version"] for package in packages if package["name"] == "duckdb"
    )
    engine = engine_version(crate_version)
    host = triple or host_triple()
    directory = library_root / host / engine
    candidates = [directory / "libduckdb.so", directory / "libduckdb.dylib"]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f"No DuckDB {engine} library for {host} under {library_root}; build with DUCKDB_DOWNLOAD_LIB=1"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lockfile", type=Path)
    parser.add_argument("library_root", type=Path)
    args = parser.parse_args()
    print(find_library(args.lockfile, args.library_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
