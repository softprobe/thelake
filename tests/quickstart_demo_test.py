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
    def test_online_eval_uses_the_readable_quickstart_policy_name(self):
        self.assertEqual(e2e.EVALUATOR_NAME, evaluator.EVALUATOR_NAME)

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
    def test_demo_saves_policy_only_after_exact_owner_confirmation(self, lisa_request):
        import json
        lisa_request.side_effect = [
            {"conversation_id": "conversation-1"}, {"activities": []},
            {"activities": [{"tool_name": "update_policy", "arguments": json.dumps({
                "scope": "agent", "agent_name": "quickstart-refund-agent",
                "content": "## Quickstart rule\napproved: yes\n\nCheck eligibility.",
            })}]},
        ]
        conversation_id, _turn = e2e.ask_lisa_to_approve_policy("http://lisa/v1", "Quickstart rule")
        self.assertEqual(conversation_id, "conversation-1")
        proposal_prompt = lisa_request.call_args_list[1].args[3]["text"]
        self.assertIn("Do not save it yet", proposal_prompt)
        self.assertIn("scope=agent", proposal_prompt)
        self.assertIn("agent_name=quickstart-refund-agent", proposal_prompt)
        self.assertEqual(lisa_request.call_args_list[2].args[3]["text"], "Confirm policy rule: Quickstart rule")
        self.assertTrue(lisa_request.call_args_list[2].args[3]["consent"])


if __name__ == "__main__":
    unittest.main()
