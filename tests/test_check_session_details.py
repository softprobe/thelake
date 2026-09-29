import importlib.util
import pathlib
import sys
import unittest


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "check_session_details.py"
SPEC = importlib.util.spec_from_file_location("check_session_details", SCRIPT)
CHECKER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CHECKER
assert SPEC.loader is not None
SPEC.loader.exec_module(CHECKER)


class ValidateDetailPayloadTests(unittest.TestCase):
    def detail(self, *, span_count=1, events=None):
        return {
            "session_id": "session-1",
            "span_count": span_count,
            "spans": [{"events": [] if events is None else events}],
            "scores": [],
        }

    def test_accepts_complete_detail(self):
        self.assertIsNone(
            CHECKER.validate_detail_payload(
                "session-1", self.detail(events=[{"name": "event"}]), True
            )
        )

    def test_rejects_span_count_mismatch(self):
        failure = CHECKER.validate_detail_payload("session-1", self.detail(span_count=2))
        self.assertIn("span_count mismatch", failure.status)

    def test_storage_path_probe_requires_an_event(self):
        failure = CHECKER.validate_detail_payload(
            "session-1", self.detail(), require_event=True
        )
        self.assertEqual(failure.status, "storage-path probe returned no events")

    def test_requires_events_array_on_every_span(self):
        detail = self.detail()
        detail["spans"][0].pop("events")
        failure = CHECKER.validate_detail_payload("session-1", detail)
        self.assertEqual(failure.status, "span 0 has no events array")

    def test_accepts_complete_recording_payload(self):
        recording = {
            "session_id": "session-1",
            "truncated": False,
            "batches": [{"events": [{"eventIndex": 0}]}],
            "events": [{"eventIndex": 0}],
        }
        self.assertIsNone(CHECKER.validate_recording_payload("session-1", recording))

    def test_rejects_truncated_recording_payload(self):
        recording = {
            "session_id": "session-1",
            "truncated": True,
            "batches": [{"events": [{"eventIndex": 0}]}],
            "events": [{"eventIndex": 0}],
        }
        failure = CHECKER.validate_recording_payload("session-1", recording)
        self.assertEqual(failure.status, "recording response is truncated")


if __name__ == "__main__":
    unittest.main()
