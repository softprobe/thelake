from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.dev_reprovision_lisa import purge_lisa_dev_volumes


class LisaDevReprovisionTest(unittest.TestCase):
    def test_removes_only_lisa_volumes_from_the_active_compose_project(self):
        runner = Mock(
            side_effect=[
                subprocess.CompletedProcess([], 0),
                subprocess.CompletedProcess([], 0, stdout="thelake-dev\n"),
                subprocess.CompletedProcess([], 0),
                subprocess.CompletedProcess(
                    [], 0, stdout="thelake-dev_lisa-postgres-data\n"
                ),
                subprocess.CompletedProcess([], 0),
                subprocess.CompletedProcess([], 0, stdout="thelake-dev_lisa-data\n"),
                subprocess.CompletedProcess([], 0),
            ]
        )

        purge_lisa_dev_volumes("docker compose", runner=runner)

        commands = [call.args[0] for call in runner.call_args_list]
        self.assertEqual(
            commands,
            [
                ["docker", "compose", "--profile", "lisa", "up", "--detach", "lisa-postgres"],
                [
                    "docker",
                    "inspect",
                    "--format",
                    '{{ index .Config.Labels "com.docker.compose.project" }}',
                    "lisa-postgres",
                ],
                [
                    "docker",
                    "compose",
                    "--profile",
                    "lisa",
                    "rm",
                    "--stop",
                    "--force",
                    "lisa",
                    "lisa-provision",
                    "lisa-postgres",
                ],
                [
                    "docker",
                    "volume",
                    "ls",
                    "--filter",
                    "label=com.docker.compose.project=thelake-dev",
                    "--filter",
                    "label=com.docker.compose.volume=lisa-postgres-data",
                    "--format",
                    "{{.Name}}",
                ],
                ["docker", "volume", "rm", "thelake-dev_lisa-postgres-data"],
                [
                    "docker",
                    "volume",
                    "ls",
                    "--filter",
                    "label=com.docker.compose.project=thelake-dev",
                    "--filter",
                    "label=com.docker.compose.volume=lisa-data",
                    "--format",
                    "{{.Name}}",
                ],
                ["docker", "volume", "rm", "thelake-dev_lisa-data"],
            ],
        )
        self.assertTrue(all(call.kwargs.get("check") for call in runner.call_args_list))

    def test_requires_the_project_database_volume_before_removing_any_volume(self):
        runner = Mock(
            side_effect=[
                subprocess.CompletedProcess([], 0),
                subprocess.CompletedProcess([], 0, stdout="thelake-dev\n"),
                subprocess.CompletedProcess([], 0),
                subprocess.CompletedProcess([], 0, stdout=""),
            ]
        )

        with self.assertRaisesRegex(RuntimeError, "Postgres volume was not found"):
            purge_lisa_dev_volumes("docker compose", runner=runner)

        self.assertFalse(
            any(call.args[0][:3] == ["docker", "volume", "rm"] for call in runner.call_args_list)
        )


if __name__ == "__main__":
    unittest.main()
