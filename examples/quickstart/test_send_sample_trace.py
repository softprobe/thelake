import unittest

from send_sample_trace import build_sample_trace, validate_local_api_url


class SampleTraceTests(unittest.TestCase):
    def test_api_url_must_point_to_loopback(self):
        self.assertEqual(validate_local_api_url("http://localhost:8090/"), "http://localhost:8090")
        self.assertEqual(validate_local_api_url("http://[::1]:8090"), "http://[::1]:8090")
        with self.assertRaisesRegex(ValueError, "loopback"):
            validate_local_api_url("https://example.com")

    def test_sample_trace_has_agent_identity_conversation_and_refund_action(self):
        trace_id = "a" * 32
        body, session_id = build_sample_trace(trace_id=trace_id, now_ns=1_800_000_000_000_000_000)

        self.assertEqual(len(body["resourceSpans"]), 1)
        spans = body["resourceSpans"][0]["scopeSpans"][0]["spans"]
        self.assertEqual({span["traceId"] for span in spans}, {trace_id})
        root = next(span for span in spans if not span.get("parentSpanId"))
        root_attributes = {item["key"]: item["value"]["stringValue"] for item in root["attributes"]}
        self.assertEqual(root_attributes["sp.agent.name"], "quickstart-refund-agent")
        self.assertEqual(root_attributes["sp.session.id"], session_id)

        event_names = {event["name"] for span in spans for event in span.get("events", [])}
        self.assertIn("gen_ai.content.prompt", event_names)
        self.assertIn("gen_ai.content.completion", event_names)

        tool = next(span for span in spans if span.get("name") == "issue_refund")
        tool_attributes = {item["key"]: item["value"]["stringValue"] for item in tool["attributes"]}
        self.assertEqual(tool_attributes["gen_ai.tool.name"], "issue_refund")
        observed_tool_names = [
            item["value"]["stringValue"]
            for span in spans
            for item in span["attributes"]
            if item["key"] == "gen_ai.tool.name"
        ]
        self.assertEqual(observed_tool_names, ["issue_refund"])


if __name__ == "__main__":
    unittest.main()
