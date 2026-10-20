#!/usr/bin/env python3
"""Initialize ignored local settings for the single interactive dev stack."""

from __future__ import annotations

import argparse
import secrets
from pathlib import Path


WORKSPACE_ID = "550e8400-e29b-41d4-a716-446655440000"
DEFAULTS = {
    "THELAKE_DEFAULT_WORKSPACE_ID": WORKSPACE_ID,
    "THELAKE_DEV_PORT": "8090",
    "THELAKE_DEV_POSTGRES_PORT": "55432",
    "THELAKE_LISA_PUBLIC_PORT": "18083",
    "LISA_LOCAL_ANONYMOUS": "1",
    "LISA_QA_ENABLED": "0",
    "LISA_ACTIVITY_FOLLOWUPS_ENABLED": "1",
    "LISA_OWNER_USER_IDS": "local",
    "LISA_QA_CRON": "0 * * * *",
    "SOFTPROBE_LOCAL_ANONYMOUS": "1",
    "SOFTPROBE_GRPC_DISABLE": "1",
}
GENERATED = {
    "LISA_THELAKE_TOKEN": lambda: secrets.token_urlsafe(32),
    "LISA_DATABASE_WRITER_PASSWORD": lambda: secrets.token_urlsafe(32),
    "LISA_DATABASE_READER_PASSWORD": lambda: secrets.token_urlsafe(32),
}


def _read(path: Path) -> tuple[list[str], dict[str, str]]:
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()
    else:
        example = path.with_name(".env.example")
        lines = example.read_text(encoding="utf-8").splitlines() if example.exists() else []
    values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip("\"'")
    return lines, values


def initialize(path: Path) -> None:
    """Fill missing local-only values while preserving configured credentials."""
    lines, values = _read(path)
    updates: dict[str, str] = {}
    derived_defaults = {
        "LISA_PUBLIC_URL": (
            f"http://127.0.0.1:{values.get('THELAKE_LISA_PUBLIC_PORT', DEFAULTS['THELAKE_LISA_PUBLIC_PORT'])}/v1"
        ),
        "LISA_CORS_ORIGINS": (
            "http://127.0.0.1:{port},http://localhost:{port}".format(
                port=values.get("THELAKE_DEV_PORT", DEFAULTS["THELAKE_DEV_PORT"])
            )
        ),
    }
    for key, value in {**DEFAULTS, **derived_defaults}.items():
        if not values.get(key):
            updates[key] = value
    for key, factory in GENERATED.items():
        if not values.get(key):
            updates[key] = factory()

    written: set[str] = set()
    output: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in updates:
                output.append(f"{key}={updates[key]}")
                written.add(key)
                continue
        output.append(line)
    output.extend(f"{key}={value}" for key, value in updates.items() if key not in written)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(output).rstrip() + "\n", encoding="utf-8")
    path.chmod(0o600)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("init", choices=("init",))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--require-model", action="store_true")
    args = parser.parse_args()
    initialize(args.env_file)
    _, values = _read(args.env_file)
    if args.require_model and not (
        values.get("GOOGLE_API_KEY")
        or values.get("GEMINI_API_KEY")
        or (values.get("LISA_BASE_URL") and values.get("LISA_API_KEY"))
    ):
        parser.error(
            f"Set GOOGLE_API_KEY or GEMINI_API_KEY in {args.env_file} before starting Lisa."
        )
    print(f"Initialized local development settings in {args.env_file}; existing values were preserved.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
