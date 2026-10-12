import importlib.util
import platform
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
EVALUATOR_SPEC = importlib.util.spec_from_file_location(
    "quickstart_evaluator", ROOT / "examples/quickstart/evaluator.py"
)
evaluator = importlib.util.module_from_spec(EVALUATOR_SPEC)
EVALUATOR_SPEC.loader.exec_module(evaluator)
E2E_SPEC = importlib.util.spec_from_file_location(
    "quickstart_online_eval", ROOT / "tests/e2e/quickstart_online_eval.py"
)
e2e = importlib.util.module_from_spec(E2E_SPEC)
E2E_SPEC.loader.exec_module(e2e)


class QuickstartDemoTest(unittest.TestCase):
    @patch.object(e2e.time, "sleep")
    @patch.object(e2e, "request_json")
    def test_wait_for_evaluation_retries_until_session_summary_is_ready(self, request, sleep):
        request.side_effect = [
            e2e.TheLakeApiError(404, '{"error":"not found"}'),
            {
                "scores": [
                    {
                        "name": evaluator.EVALUATOR_NAME,
                        "source": "evaluator",
                        "string_value": "fail",
                    }
                ]
            },
        ]

        result = e2e.wait_for_evaluation("http://127.0.0.1:8090", "session-123", evaluator.EVALUATOR_NAME)

        self.assertEqual(result["string_value"], "fail")
        self.assertEqual(request.call_count, 2)
        sleep.assert_called_once_with(3)

    def test_active_check_lookup_requires_the_sample_agent_and_name(self):
        with patch.object(evaluator, "request_json", return_value=[
            {"name": "Wrong agent", "target_agent_name": "other", "active": True},
            {"name": "Quickstart rule", "target_agent_name": "quickstart-refund-agent", "active": False},
            {"name": "Quickstart rule", "target_agent_name": "quickstart-refund-agent", "active": True},
        ]):
            result = evaluator.find_active_evaluator("http://lake", "Quickstart rule")

        self.assertTrue(result["active"])
        self.assertEqual(result["target_agent_name"], "quickstart-refund-agent")

    def test_result_lookup_returns_only_the_sample_evaluator_verdict(self):
        session = {
            "scores": [
                {"name": "Other check", "source": "evaluator", "string_value": "pass"},
                {
                    "name": "Refund eligibility before refund",
                    "source": "evaluator",
                    "string_value": "fail",
                    "comment": "The agent issued the refund without checking eligibility.",
                },
            ]
        }

        result = e2e.refund_evaluation_result(session, "Refund eligibility before refund")

        self.assertEqual(result["string_value"], "fail")
        self.assertIn("without checking eligibility", result["comment"])

    @patch.object(e2e.subprocess, "run")
    def test_agent_run_uses_the_local_compose_environment_and_returns_session(self, run):
        run.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="agent=quickstart-refund-agent session_id=demo-123 tool=issue_refund\n", stderr=""
        )

        session_id = e2e.run_sample_agent("http://127.0.0.1:8090")

        self.assertEqual(session_id, "demo-123")
        command = run.call_args.args[0]
        self.assertIn("--env-file", command)
        self.assertIn("warehouse/quickstart/compose.env", command)
        self.assertIn("quickstart-refund-agent", command)
        self.assertIn(
            "http://127.0.0.1:8090" if platform.system() == "Linux" else "http://host.docker.internal:8090",
            command,
        )

    @patch.object(e2e, "lisa_request")
    def test_demo_authors_and_activates_the_check_through_lisa_chat(self, lisa_request):
        lisa_request.side_effect = [
            {"conversation_id": "conversation-1"},
            {
                "activities": [
                    {"label": "Lisa used create_evaluator_draft"},
                    {"label": "Lisa used activate_evaluator"},
                ]
            },
        ]

        conversation_id, _turn = e2e.ask_lisa_to_create_check("http://lisa/v1", "Quickstart rule")

        self.assertEqual(conversation_id, "conversation-1")
        self.assertEqual(lisa_request.call_args_list[0].args, ("http://lisa/v1", None, "/v1/conversations", {}))
        message_request = lisa_request.call_args_list[1]
        self.assertIn("Create and activate a behavior check", message_request.args[3]["text"])
        self.assertTrue(message_request.args[3]["consent"])


if __name__ == "__main__":
    unittest.main()
