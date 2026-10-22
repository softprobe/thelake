from pathlib import Path
import re
import unittest

import yaml


REPO = Path(__file__).resolve().parents[1]


class DogfoodStartTest(unittest.TestCase):
    def test_dogfood_runs_standalone_lisa_with_private_trace_intake(self):
        compose = yaml.safe_load((REPO / "examples/lisa-dogfood/compose.yaml").read_text())
        services = compose["services"]
        assert set(services) == {"postgres", "lisa", "refund-agent"}
        lisa = services["lisa"]
        assert lisa["build"]["dockerfile"] == "lisa/Dockerfile"
        assert "LISA_DATABASE_URL" in lisa["environment"]
        assert "LISA_SERVICE_TOKEN" in lisa["environment"]
        assert "LISA_AGENT_WORKSPACE_ROOT" in lisa["environment"]
        assert "LISA_THELAKE_API_URL" not in lisa["environment"]
        assert "LISA_QA_CRON" not in lisa["environment"]

        start = (REPO / "examples/lisa-dogfood/start.sh").read_text()
        assert start.index('"${compose[@]}" up --build --detach postgres') < start.index("provision_lisa.py")
        assert start.index("provision_lisa.py") < start.index('"${compose[@]}" up --build --detach lisa')
        assert "session-traces" in start
        assert "Standalone Lisa dogfood is ready" in start
        assert "Dogfood is disposable QA, not a development environment" in start

    def test_dogfood_stores_only_run_owned_credentials_in_mode_600_env(self):
        start = (REPO / "examples/lisa-dogfood/start.sh").read_text()
        assert 'umask 077' in start
        assert 'chmod 600 "$THELAKE_DOGFOOD_ENV_FILE"' in start
        assert re.search(r"^LISA_SERVICE_TOKEN=\$LISA_SERVICE_TOKEN$", start, re.MULTILINE)
        assert "LISA_THELAKE_TOKEN" not in start
        assert "LISA_QA_CRON" not in start

    def test_lisa_python_image_owns_and_serves_its_frontend_bundle(self):
        dockerfile = (REPO / "lisa/Dockerfile").read_text()
        frontend_config = (REPO / "packages/thelake-explorer/vite.lisa.config.ts").read_text()
        e2e_services = (REPO / "scripts/run-explorer-e2e-services.py").read_text()
        e2e_script = (REPO / "scripts/test-explorer-ui.sh").read_text()

        assert "npm run build:lisa-frontend" in dockerfile
        assert "COPY --from=lisa-frontend /ui/lisa-dist ./static" in dockerfile
        assert 'new URL("./lisa-dist/", import.meta.url)' in frontend_config
        assert "LISA_FRONTEND_OUT_DIR" in frontend_config
        assert "lisa-embedded" not in e2e_services
        assert "LISA_STATIC_DIR" in e2e_script
        assert "build:lisa-frontend" in e2e_script
        assert "lisa-embedded" not in e2e_script


if __name__ == "__main__":
    unittest.main()
