import os
import shutil
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]


class DogfoodStartTest(unittest.TestCase):
    def test_start_rejects_a_port_selected_for_two_services(self):
        with tempfile.TemporaryDirectory(prefix="thelake-dogfood-port-test-") as temp:
            root = Path(temp)
            dogfood = root / "examples" / "lisa-dogfood"
            dogfood.mkdir(parents=True)
            shutil.copy(REPO / "examples/lisa-dogfood/start.sh", dogfood / "start.sh")
            shutil.copy(REPO / "examples/lisa-dogfood/config.yaml", dogfood / "config.yaml")
            (root / ".env").write_text("GEMINI_API_KEY=local-test-gemini\n", encoding="utf-8")

            bin_dir = root / "bin"
            bin_dir.mkdir()
            docker_log = root / "docker.log"
            (bin_dir / "docker").write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = info ]; then exit 0; fi\n"
                "printf '%s\\n' \"$*\" >> \"$DOGFOOD_DOCKER_LOG\"\n",
                encoding="utf-8",
            )
            (bin_dir / "curl").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            (bin_dir / "make").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            for command in bin_dir.iterdir():
                command.chmod(0o755)

            allocated_ports = set()

            def free_port():
                while True:
                    with socket.socket() as sock:
                        sock.bind(("127.0.0.1", 0))
                        port = sock.getsockname()[1]
                    if port not in allocated_ports:
                        allocated_ports.add(port)
                        return port

            shared_port = free_port()
            lake_port = free_port()

            env = os.environ.copy()
            env.update(
                {
                    "PATH": f"{bin_dir}:{env['PATH']}",
                    "DOGFOOD_DOCKER_LOG": str(docker_log),
                    "THELAKE_DOGFOOD_DB_PORT": str(shared_port),
                    "THELAKE_DOGFOOD_LISA_PORT": str(shared_port),
                    "THELAKE_DOGFOOD_PORT": str(lake_port),
                    "THELAKE_DOGFOOD_PROJECT_NAME": "lisa-policy-eval-port-test",
                    "THELAKE_DOGFOOD_ENV_FILE": "warehouse/dogfood/lisa-policy-eval-port-test.env",
                    "THELAKE_DOGFOOD_DATA_PATH": "./warehouse/dogfood/lisa-policy-eval-port-test",
                }
            )
            env.pop("GOOGLE_API_KEY", None)
            env.pop("GEMINI_API_KEY", None)

            result = subprocess.run(
                ["bash", str(dogfood / "start.sh")],
                cwd=root,
                env=env,
                input="",
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0, msg=f"stdout={result.stdout}\nstderr={result.stderr}")
            self.assertIn("already assigned", result.stderr)

            env["THELAKE_DOGFOOD_DB_PORT"] = "0"
            env["THELAKE_DOGFOOD_LISA_PORT"] = str(free_port())
            result = subprocess.run(
                ["bash", str(dogfood / "start.sh")],
                cwd=root,
                env=env,
                input="",
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0, msg=f"stdout={result.stdout}\nstderr={result.stderr}")
            self.assertIn("between 1 and 65535", result.stderr)
            self.assertFalse(docker_log.exists())

            dogfood_data = root / "warehouse" / "dogfood"
            dogfood_data.mkdir(parents=True)
            metadata_target = root / "outside-eval-env"
            env_file = dogfood_data / "lisa-policy-eval-symlink.env"
            env_file.symlink_to(metadata_target)
            env.update(
                {
                    "THELAKE_DOGFOOD_DB_PORT": str(free_port()),
                    "THELAKE_DOGFOOD_LISA_PORT": str(free_port()),
                    "THELAKE_DOGFOOD_PORT": str(free_port()),
                    "THELAKE_DOGFOOD_PROJECT_NAME": "lisa-policy-eval-symlink",
                    "THELAKE_DOGFOOD_ENV_FILE": "warehouse/dogfood/lisa-policy-eval-symlink.env",
                    "THELAKE_DOGFOOD_DATA_PATH": "./warehouse/dogfood/lisa-policy-eval-symlink",
                }
            )
            result = subprocess.run(
                ["bash", str(dogfood / "start.sh")],
                cwd=root,
                env=env,
                input="",
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0, msg=f"stdout={result.stdout}\nstderr={result.stderr}")
            self.assertIn("Refusing to reuse existing Lisa policy evaluation data or metadata", result.stderr)
            self.assertTrue(env_file.is_symlink())
            self.assertFalse(metadata_target.exists())

    def test_start_requires_a_run_owned_qa_project_and_creates_its_stack(self):
        with tempfile.TemporaryDirectory(prefix="thelake-dogfood-test-") as temp:
            root = Path(temp)
            dogfood = root / "examples" / "lisa-dogfood"
            dogfood.mkdir(parents=True)
            shutil.copy(REPO / "examples/lisa-dogfood/start.sh", dogfood / "start.sh")
            shutil.copy(REPO / "examples/lisa-dogfood/config.yaml", dogfood / "config.yaml")
            (root / ".env").write_text("GEMINI_API_KEY=local-test-gemini\n", encoding="utf-8")

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
                "printf '%s\\n' \"$*\" >> \"$DOGFOOD_DOCKER_LOG\"\n",
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
            (bin_dir / "uv").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            for command in bin_dir.iterdir():
                command.chmod(0o755)

            env = os.environ.copy()
            allocated_ports = set()

            def free_port():
                while True:
                    with socket.socket() as sock:
                        sock.bind(("127.0.0.1", 0))
                        port = sock.getsockname()[1]
                    if port not in allocated_ports:
                        allocated_ports.add(port)
                        return port

            db_port = free_port()
            lisa_port = free_port()
            lake_port = free_port()
            env.update(
                {
                    "PATH": f"{bin_dir}:{env['PATH']}",
                    "DOGFOOD_DOCKER_LOG": str(docker_log),
                    "THELAKE_DOGFOOD_DB_PORT": str(db_port),
                    "THELAKE_DOGFOOD_LISA_PORT": str(lisa_port),
                    "THELAKE_DOGFOOD_PORT": str(lake_port),
                    "THELAKE_DOGFOOD_PROJECT_NAME": "lisa-policy-eval-guided",
                    "THELAKE_DOGFOOD_ENV_FILE": "warehouse/dogfood/lisa-policy-eval-guided.env",
                    "THELAKE_DOGFOOD_DATA_PATH": "./warehouse/dogfood/lisa-policy-eval-guided",
                }
            )
            env.pop("GOOGLE_API_KEY", None)
            env.pop("GEMINI_API_KEY", None)

            result = subprocess.run(
                ["bash", str(dogfood / "start.sh")],
                cwd=root,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )

            env_file = root / "warehouse/dogfood/lisa-policy-eval-guided.env"
            self.assertTrue(env_file.is_file())
            generated_env = env_file.read_text(encoding="utf-8")
            self.assertRegex(generated_env, r"(?m)^LISA_THELAKE_TOKEN=[A-Za-z0-9_-]+$")
            self.assertIn(f"THELAKE_DOGFOOD_DB_PORT={db_port}", generated_env)
            self.assertIn(f"THELAKE_DOGFOOD_LISA_PORT={lisa_port}", generated_env)
            self.assertIn(f"THELAKE_DOGFOOD_PORT={lake_port}", generated_env)
            self.assertEqual(env_file.stat().st_mode & 0o777, 0o600)
            self.assertEqual((root / ".env").stat().st_mode & 0o777, 0o600)
            self.assertIn("GEMINI_API_KEY=local-test-gemini", (root / ".env").read_text())
            generated_config = (root / "warehouse/dogfood/lisa-policy-eval-guided.yaml").read_text()
            self.assertIn(f'metadata_path: "host=127.0.0.1 port={db_port} dbname=ducklake user=ducklake password=ducklake"', generated_config)
            self.assertIn('data_path: "./warehouse/dogfood/lisa-policy-eval-guided"', generated_config)
            self.assertNotIn("local-test-gemini", result.stdout)
            self.assertIn("Starting disposable Lisa QA stack", result.stdout)
            self.assertNotIn("Leave this terminal running", result.stdout)
            self.assertFalse((root / "warehouse/dogfood/api_url").exists())

            (root / ".env").write_text("GEMINI_API_KEY=\n", encoding="utf-8")
            env.update(
                {
                    "THELAKE_DOGFOOD_PROJECT_NAME": "lisa-policy-eval-prompted",
                    "THELAKE_DOGFOOD_ENV_FILE": "warehouse/dogfood/lisa-policy-eval-prompted.env",
                    "THELAKE_DOGFOOD_DATA_PATH": "./warehouse/dogfood/lisa-policy-eval-prompted",
                }
            )
            prompted = subprocess.run(
                ["bash", str(dogfood / "start.sh")],
                cwd=root,
                env=env,
                input="prompted-test-gemini\n",
                check=True,
                capture_output=True,
                text=True,
            )
            saved_env = (root / ".env").read_text(encoding="utf-8")
            self.assertIn("GEMINI_API_KEY=prompted-test-gemini", saved_env)
            self.assertEqual((root / ".env").stat().st_mode & 0o777, 0o600)
            self.assertNotIn("prompted-test-gemini", prompted.stdout)


if __name__ == "__main__":
    unittest.main()
