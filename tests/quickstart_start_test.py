import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]


class QuickstartStartTest(unittest.TestCase):
    def test_printed_agent_command_loads_the_generated_runner_token(self):
        with tempfile.TemporaryDirectory(prefix="thelake-quickstart-test-") as temp:
            root = Path(temp)
            quickstart = root / "examples" / "quickstart"
            quickstart.mkdir(parents=True)
            shutil.copy(REPO / "examples/quickstart/start.sh", quickstart / "start.sh")
            shutil.copy(REPO / "examples/quickstart/config.yaml", quickstart / "config.yaml")

            bin_dir = root / "bin"
            bin_dir.mkdir()
            docker_log = root / "docker.log"
            (bin_dir / "docker").write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = info ]; then exit 0; fi\n"
                "if [ \"$1\" = compose ]; then\n"
                "  shift\n"
                "  if [ \"$1\" = --env-file ]; then\n"
                "    [ -s \"$2\" ] || exit 2\n"
                "  fi\n"
                "fi\n"
                "printf '%s\\n' \"$*\" >> \"$QUICKSTART_DOCKER_LOG\"\n",
                encoding="utf-8",
            )
            (bin_dir / "curl").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            (bin_dir / "make").write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = ducklake-extension ]; then "
                "mkdir -p target/ducklake-extension; touch target/ducklake-extension/ducklake.duckdb_extension; fi\n"
                "exit 0\n",
                encoding="utf-8",
            )
            for command in bin_dir.iterdir():
                command.chmod(0o755)

            env = os.environ.copy()
            env.update(
                {
                    "PATH": f"{bin_dir}:{env['PATH']}",
                    "QUICKSTART_DOCKER_LOG": str(docker_log),
                    "GOOGLE_API_KEY": "test-key",
                    "THELAKE_QUICKSTART_DB_PORT": "55430",
                    "THELAKE_QUICKSTART_RUNNER_PORT": "18080",
                    "THELAKE_QUICKSTART_PORT": "8091",
                }
            )
            env.pop("GEMINI_API_KEY", None)
            env.pop("THELAKE_EVALUATION_RUNNER_TOKEN", None)

            result = subprocess.run(
                ["bash", str(quickstart / "start.sh")],
                cwd=root,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )

            env_file = root / "warehouse/quickstart/compose.env"
            self.assertTrue(env_file.is_file())
            self.assertRegex(
                env_file.read_text(encoding="utf-8"),
                r"^THELAKE_EVALUATION_RUNNER_TOKEN=[A-Za-z0-9_-]+\n$",
            )
            self.assertEqual(env_file.stat().st_mode & 0o777, 0o600)
            self.assertIn("--env-file warehouse/quickstart/compose.env", result.stdout)
            agent_command = next(
                line.strip() for line in result.stdout.splitlines()
                if line.strip().startswith("docker compose --env-file")
            )
            subprocess.run(
                ["bash", "-c", agent_command],
                cwd=root,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn(
                "--env-file warehouse/quickstart/compose.env --project-name thelake-quickstart",
                docker_log.read_text(encoding="utf-8"),
            )


if __name__ == "__main__":
    unittest.main()
