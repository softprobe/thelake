"""Remove this Compose project's local Lisa state before a fresh dev-up."""

from __future__ import annotations

import argparse
import shlex
import subprocess
from collections.abc import Callable, Sequence


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
LISA_VOLUME_LABELS = ("lisa-postgres-data", "lisa-data")
COMPOSE_PROJECT_LABEL = "com.docker.compose.project"
COMPOSE_VOLUME_LABEL = "com.docker.compose.volume"


def purge_lisa_dev_volumes(
    compose_command: str,
    *,
    runner: CommandRunner | None = None,
) -> None:
    """Remove only Lisa's named volumes for the active Compose project."""
    run = runner or subprocess.run
    compose = shlex.split(compose_command)
    if not compose:
        raise RuntimeError("Compose command cannot be empty")

    run([*compose, "--profile", "lisa", "up", "--detach", "lisa-postgres"], check=True)
    project = run(
        [
            "docker",
            "inspect",
            "--format",
            '{{ index .Config.Labels "com.docker.compose.project" }}',
            "lisa-postgres",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not project or project == "<no value>":
        raise RuntimeError("Could not determine Lisa's Compose project")

    run(
        [
            *compose,
            "--profile",
            "lisa",
            "rm",
            "--stop",
            "--force",
            "lisa",
            "lisa-provision",
            "lisa-postgres",
        ],
        check=True,
    )

    for logical_volume in LISA_VOLUME_LABELS:
        names = run(
            [
                "docker",
                "volume",
                "ls",
                "--filter",
                f"label={COMPOSE_PROJECT_LABEL}={project}",
                "--filter",
                f"label={COMPOSE_VOLUME_LABEL}={logical_volume}",
                "--format",
                "{{.Name}}",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        volumes = [name.strip() for name in names if name.strip()]
        if logical_volume == "lisa-postgres-data" and not volumes:
            raise RuntimeError(f"Lisa Postgres volume was not found for Compose project {project}")
        for volume in volumes:
            run(["docker", "volume", "rm", volume], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compose", required=True, help="Compose command configured by Make")
    args = parser.parse_args()
    purge_lisa_dev_volumes(args.compose)


if __name__ == "__main__":
    main()
