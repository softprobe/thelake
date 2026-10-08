import ast
from pathlib import Path
import unittest


AGENT_DIR = Path(__file__).parent
AGENT_SOURCE = (AGENT_DIR / "refund_agent.py").read_text(encoding="utf-8")
AGENT_TREE = ast.parse(AGENT_SOURCE)


class RefundAgentSdkContractTests(unittest.TestCase):
    @staticmethod
    def _generation_call(name: str) -> ast.Call:
        for node in ast.walk(AGENT_TREE):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "generation":
                continue
            if any(
                keyword.arg == "name"
                and isinstance(keyword.value, ast.Constant)
                and keyword.value.value == name
                for keyword in node.keywords
            ):
                return node
        raise AssertionError(f"generation {name!r} was not found")

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
        self.assertIn("google.genai", imported_modules)
        self.assertNotIn("urllib.request", imported_modules)
        self.assertNotIn("/v1/traces", AGENT_SOURCE)
        self.assertNotIn("resourceSpans", AGENT_SOURCE)

    def test_agent_image_installs_sdk_and_native_provider_client(self):
        dockerfile = (AGENT_DIR / "Dockerfile").read_text(encoding="utf-8")

        self.assertIn("softprobe", dockerfile)
        self.assertIn("google-genai", dockerfile)

    def test_sdk_lifecycle_and_observation_types_are_explicit(self):
        calls = {
            node.func.attr
            for node in ast.walk(AGENT_TREE)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }

        self.assertTrue({"observation", "start_agent"} & calls)
        self.assertIn("generation", calls)
        self.assertIn("start_tool", calls)
        self.assertIn("force_flush", calls)
        self.assertIn("shutdown", calls)

    def test_tool_call_is_not_emitted_as_assistant_completion(self):
        first_generation = self._generation_call("gemini.generate_content")
        self.assertFalse(
            {keyword.arg for keyword in first_generation.keywords}
            & {"output", "completion_event"}
        )

    def test_final_generation_does_not_replay_evaluator_evidence_input(self):
        first_generation = self._generation_call("gemini.generate_content")
        final_generation = self._generation_call("gemini.final_response")
        first_prompt = next(
            (keyword for keyword in first_generation.keywords if keyword.arg == "prompt_event"),
            None,
        )
        final_prompt = next(
            (keyword for keyword in final_generation.keywords if keyword.arg == "prompt_event"),
            None,
        )
        final_input = next(
            (keyword for keyword in final_generation.keywords if keyword.arg == "input"),
            None,
        )

        self.assertIsNotNone(first_prompt)
        self.assertIsNone(final_prompt)
        self.assertIsNone(final_input)
        self.assertIn('"gen_ai.tool.result"', AGENT_SOURCE)


if __name__ == "__main__":
    unittest.main()
