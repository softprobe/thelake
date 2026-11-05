#!/usr/bin/env python3
"""Mint an HS256 Softprobe assertion JWT. Prints the token on stdout."""

import argparse
import base64
import hashlib
import hmac
import json
import time


def b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--secret", required=True)
    parser.add_argument("--ttl-seconds", type=int, default=300)
    args = parser.parse_args()
    header = b64(b'{"alg":"HS256","typ":"JWT"}')
    now = int(time.time())
    payload = {
        "iss": "softprobe-edge",
        "aud": "sp-backend",
        "sub": "verify-thelake",
        "workspace_id": args.workspace_id,
        "roles": ["member"],
        "exp": now + args.ttl_seconds,
    }
    payload_b64 = b64(json.dumps(payload, separators=(",", ":")).encode())
    signing = f"{header}.{payload_b64}"
    sig = hmac.new(args.secret.encode(), signing.encode(), hashlib.sha256).digest()
    print(f"{signing}.{b64(sig)}")


if __name__ == "__main__":
    main()
