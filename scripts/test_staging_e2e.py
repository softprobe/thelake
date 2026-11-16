import sys
import unittest
from http.client import IncompleteRead
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

sys.path.insert(0, str(Path(__file__).resolve().parent))
import staging_e2e


class StagingE2ETest(unittest.TestCase):
    def test_payload_carries_the_trace_and_session_identity(self):
        payload = staging_e2e.trace_payload(
            "a" * 32,
            "b" * 16,
            1_800_000_000_000_000_000,
        )

        span = payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        self.assertEqual(span["traceId"], "a" * 32)
        self.assertEqual(span["spanId"], "b" * 16)
        self.assertEqual(span["startTimeUnixNano"], "1800000000000000000")

    def test_readback_requires_the_expected_trace_and_span_session(self):
        readback = {
            "trace": {"trace_id": "a" * 32, "session_id": "ses-smoke"},
            "spans": [
                {
                    "trace_id": "a" * 32,
                    "span_id": "b" * 16,
                    "session_id": "ses-smoke",
                }
            ],
        }

        self.assertTrue(
            staging_e2e.readback_matches(readback, "a" * 32, "b" * 16, "ses-smoke")
        )
        self.assertFalse(
            staging_e2e.readback_matches(readback, "a" * 32, "b" * 16, "ses-other")
        )
        self.assertFalse(
            staging_e2e.readback_matches(readback, "c" * 32, "b" * 16, "ses-smoke")
        )

    def test_trace_readback_retries_transient_http_and_network_errors(self):
        for error, expected_status in (
            (HTTPError("https://staging/trace", 502, "Bad Gateway", {}, None), 502),
            (URLError("connection reset"), None),
        ):
            with self.subTest(error=error), patch.object(
                staging_e2e, "urlopen", side_effect=error
            ):
                self.assertEqual(
                    staging_e2e.request_json("https://staging/trace", "GET"),
                    (expected_status, None),
                )

    def test_trace_readback_keeps_client_errors_fatal(self):
        error = HTTPError("https://staging/trace", 401, "Unauthorized", {}, None)
        with patch.object(staging_e2e, "urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "HTTP 401"):
                staging_e2e.request_json("https://staging/trace", "GET")

    def test_trace_readback_retries_truncated_and_invalid_json_responses(self):
        class Response:
            status = 200

            def __init__(self, body=None, error=None):
                self.body = body
                self.error = error

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                return False

            def read(self):
                if self.error:
                    raise self.error
                return self.body

        for response in (
            Response(error=IncompleteRead(b'{"trace"', 20)),
            Response(body=b"not json"),
        ):
            with self.subTest(response=response), patch.object(
                staging_e2e, "urlopen", return_value=response
            ):
                self.assertEqual(
                    staging_e2e.request_json("https://staging/trace", "GET"),
                    (None, None),
                )


if __name__ == "__main__":
    unittest.main()
