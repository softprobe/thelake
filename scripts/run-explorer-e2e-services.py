#!/usr/bin/env python3
"""Start TheLake and Lisa on atomically assigned test ports."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path


def _listener() -> socket.socket:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    return listener


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port-file", type=Path, required=True)
    parser.add_argument("--thelake-log", type=Path, required=True)
    parser.add_argument("--lisa-log", type=Path, required=True)
    parser.add_argument("--thelake-bin", type=Path, required=True)
    parser.add_argument("--config-file", type=Path, required=True)
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--lisa-postgres-url")
    parser.add_argument("--lisa-data-dir", type=Path, default=Path(""))
    args = parser.parse_args()

    lake_socket = _listener()
    lake_port = lake_socket.getsockname()[1]
    lisa_socket = _listener()
    lisa_port = lisa_socket.getsockname()[1]
    args.port_file.write_text(
        json.dumps({"thelake": lake_port, "lisa": lisa_port}) + "\n",
        encoding="utf-8",
    )

    lake_env = os.environ.copy()
    lake_env.update(
        {
            "CONFIG_FILE": str(args.config_file),
            "SOFTPROBE_LOCAL_ANONYMOUS": "1",
            "THELAKE_DEFAULT_WORKSPACE_ID": args.workspace_id,
            "SOFTPROBE_LISTEN_ADDR": f"127.0.0.1:{lake_port}",
            "SOFTPROBE_LISTEN_FD": str(lake_socket.fileno()),
            "SOFTPROBE_GRPC_DISABLE": "1",
        }
    )
    lake_log = args.thelake_log.open("w", encoding="utf-8")
    processes: list[subprocess.Popen] = []
    log_handles = [lake_log]
    try:
        lake = subprocess.Popen(
            [str(args.thelake_bin)],
            env=lake_env,
            pass_fds=(lake_socket.fileno(),),
            stdout=lake_log,
            stderr=subprocess.STDOUT,
        )
        processes.append(lake)
        if not args.lisa_postgres_url:
            raise RuntimeError("Explorer E2E requires its disposable Lisa PostgreSQL URL")
        lisa_root = Path(__file__).resolve().parents[1] / "lisa"
        sys.path.insert(0, str(lisa_root))
        from conftest import create_lisa_test_database

        database = create_lisa_test_database(args.lisa_postgres_url, args.workspace_id)
        lisa_env = os.environ.copy()
        lisa_env.update(
            {
                "LISA_WORKSPACE_ID": args.workspace_id,
                "LISA_DATA_DIR": str(args.lisa_data_dir),
                # This database and owner identity belong to this isolated browser run.
                "LISA_DATABASE_URL": database.writer_url,
                "LISA_DATABASE_READ_URL": database.reader_url,
                "LISA_SERVICE_TOKEN": secrets.token_urlsafe(32),
                "LISA_EXPLORER_URL": f"http://127.0.0.1:{lake_port}/explorer/",
                "LISA_LOCAL_ANONYMOUS": "1",
                "LISA_LOCAL_ACTOR_ID": "e2e-owner",
                "LISA_OWNER_USER_IDS": "e2e-owner",
                "LISA_SLACK_BOT_TOKEN": "",
                "LISA_SLACK_CHANNEL_ID": "",
            }
        )
        lisa_log = args.lisa_log.open("w", encoding="utf-8")
        log_handles.append(lisa_log)
        lisa = subprocess.Popen(
            [
                "uv",
                "run",
                "--project",
                str(Path(__file__).resolve().parents[1] / "lisa"),
                "--extra",
                "openhands",
                "uvicorn",
                "lisa.app:app",
                "--host",
                "127.0.0.1",
                "--fd",
                str(lisa_socket.fileno()),
            ],
            env=lisa_env,
            pass_fds=(lisa_socket.fileno(),),
            stdout=lisa_log,
            stderr=subprocess.STDOUT,
        )
        processes.append(lisa)

        lake_socket.close()
        lisa_socket.close()

        stopping = False

        def stop(_signum, _frame):
            nonlocal stopping
            stopping = True
            for process in processes:
                if process.poll() is None:
                    process.terminate()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        while not stopping:
            for process in processes:
                code = process.poll()
                if code is not None:
                    stop(signal.SIGTERM, None)
                    return code or 1
            time.sleep(0.2)
        for process in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        return 0
    finally:
        lake_socket.close()
        lisa_socket.close()
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for handle in log_handles:
            handle.close()


if __name__ == "__main__":
    sys.exit(main())
