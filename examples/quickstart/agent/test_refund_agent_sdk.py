import ast
import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, Mock, patch
import unittest


AGENT_DIR = Path(__file__).parent
AGENT_SOURCE = (AGENT_DIR / "refund_agent.py").read_text(encoding="utf-8")
AGENT_TREE = ast.parse(AGENT_SOURCE)


class RefundAgentSdkContractTests(unittest.TestCase):
    def test_agent_uses_softprobe_sdk_without_handwritten_otlp_transport(self):
        imported_modules = {
            alias.name
            for node in ast.walk(AGENT_TREE)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported_modules.update(
            node.module
            for node in ast.walk(AGENT_TREE)
            if isinstance(node, ast.ImportFrom) and node.module
        )

        self.assertTrue(any(name == "softprobe" or name.startswith("softprobe.") for name in imported_modules))
        self.assertIn("softprobe.openai", imported_modules)
        self.assertNotIn("google.genai", imported_modules)
        self.assertNotIn("urllib.request", imported_modules)
        self.assertNotIn("/v1/traces", AGENT_SOURCE)
        self.assertNotIn("resourceSpans", AGENT_SOURCE)

    def test_agent_image_installs_sdk_and_openai_compatibility_extra(self):
        dockerfile = (AGENT_DIR / "Dockerfile").read_text(encoding="utf-8")

        self.assertIn("softprobe[openai]", dockerfile)
        self.assertNotIn("google-genai", dockerfile)

    def test_sdk_lifecycle_and_observation_types_are_explicit(self):
        calls = {
            node.func.attr
            for node in ast.walk(AGENT_TREE)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        calls.update(
            node.func.id
            for node in ast.walk(AGENT_TREE)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        )

        self.assertTrue({"observation", "start_agent"} & calls)
        self.assertIn("observe_openai", calls)
        self.assertIn("create_gemini_openai_client", calls)
        self.assertNotIn("generation", calls)
        self.assertIn("start_tool", calls)
        self.assertIn("force_flush", calls)
        self.assertIn("shutdown", calls)

    def test_tool_execution_uses_auto_generation_as_parent(self):
        self.assertIn('"parent_span_id": client.last_generation_span_id', AGENT_SOURCE)
        self.assertIn('"trace_id": client.last_generation_trace_id', AGENT_SOURCE)
        self.assertIn('"gen_ai.tool.result"', AGENT_SOURCE)
        self.assertIn("session_id=session_id", AGENT_SOURCE)

    def test_agent_uses_one_model_call_and_returns_a_local_confirmation(self):
        fake_softprobe = ModuleType("softprobe")
        fake_softprobe.SoftprobeClient = object
        fake_openai = ModuleType("softprobe.openai")
        fake_openai.create_gemini_openai_client = lambda **kwargs: None
        fake_openai.observe_openai = lambda client, **kwargs: client
        fake_softprobe.openai = fake_openai

        spec = importlib.util.spec_from_file_location(
            "quickstart_refund_agent_under_test", AGENT_DIR / "refund_agent.py"
        )
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"softprobe": fake_softprobe, "softprobe.openai": fake_openai}):
            spec.loader.exec_module(module)

        function_call = SimpleNamespace(
            id="call-1",
            function=SimpleNamespace(
                name="issue_refund",
                arguments='{"ticket_id":"DEMO-42"}',
            ),
        )
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(tool_calls=[function_call]))]
        )
        completions = SimpleNamespace(create=Mock(return_value=response))
        client = SimpleNamespace(
            chat=SimpleNamespace(completions=completions),
            last_generation_trace_id="trace-1",
            last_generation_span_id="generation-1",
        )
        telemetry = MagicMock()
        agent = Mock()
        telemetry.observation.return_value.__enter__.return_value = agent
        telemetry.start_tool.return_value = Mock()
        telemetry.force_flush.return_value = True

        with (
            patch.object(module, "SoftprobeClient", return_value=telemetry),
            patch.object(module, "create_gemini_openai_client", return_value=client),
            patch.object(module, "observe_openai", return_value=client),
        ):
            module.run_agent("quickstart-refund-agent", "http://localhost:8090", "gemini-test")

        completions.create.assert_called_once()
        agent.update.assert_called_once_with(
            output={"content": "Your refund for DEMO-42 has been issued."}
        )
        telemetry.start_tool.assert_called_once()
        telemetry.force_flush.assert_called_once()
        telemetry.shutdown.assert_called_once()

    def test_agent_has_no_post_tool_model_request(self):
        requests = sorted(
            (
                node
                for node in ast.walk(AGENT_TREE)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "create"
            ),
            key=lambda node: node.lineno,
        )
        self.assertEqual(len(requests), 1)
        self.assertIn("agent.update(output=", AGENT_SOURCE)


if __name__ == "__main__":
    unittest.main()
