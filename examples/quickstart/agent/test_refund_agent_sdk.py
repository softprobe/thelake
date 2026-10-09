import ast
from pathlib import Path
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

    def test_final_request_does_not_replay_user_turn(self):
        self.assertNotIn("messages.append", AGENT_SOURCE)
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
        self.assertEqual(len(requests), 2)
        final_messages = next(keyword.value for keyword in requests[1].keywords if keyword.arg == "messages")
        self.assertIsInstance(final_messages, ast.List)
        roles = [
            next(
                value.value
                for key, value in zip(item.keys, item.values, strict=True)
                if isinstance(key, ast.Constant) and key.value == "role"
            )
            for item in final_messages.elts
            if isinstance(item, ast.Dict)
        ]
        self.assertEqual(roles, ["system"])
        self.assertIn("final.choices[0].message.content", ast.unparse(AGENT_TREE))


if __name__ == "__main__":
    unittest.main()
