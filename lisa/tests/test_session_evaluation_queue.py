from __future__ import annotations

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from lisa.repositories.session_evaluation_queue import SessionEvaluationQueue
from lisa.services.session_evaluations import MAX_DELIVERIES, SessionEvaluationWorker


def test_session_evaluation_queue_persists_and_deletes_messages(lisa_test_db):
    queue = SessionEvaluationQueue(lisa_test_db.writer_url)

    queue.send("session-1", "checkout-agent", "a" * 64)
    assert queue.send("session-1", "checkout-agent", "a" * 64) is False
    with pytest.raises(ValueError, match="different evidence"):
        queue.send("session-1", "checkout-agent", "b" * 64)
    message = queue.read_one()

    assert message is not None
    assert message["message"]["session_id"] == "session-1"
    assert message["message"]["agent_name"] == "checkout-agent"
    assert message["message"]["payload_hash"] == "a" * 64
    assert message["read_ct"] == 1
    queue.delete(message["msg_id"])
    assert queue.read_one() is None


def test_exhausted_missing_session_is_recorded_and_archived(tmp_path):
    class Queue:
        def __init__(self):
            self.archived = []

        def archive(self, message_id):
            self.archived.append(message_id)

    class ActivityStore:
        def __init__(self):
            self.created = []
            self.events = []
            self.completed = []

        def get_activity_thread(self, _activity_id):
            return None

        def create_activity(self, **activity):
            self.created.append(activity)

        def record_activity_event(self, activity_id, *args, **kwargs):
            self.events.append((activity_id, args, kwargs))

        def fail_exhausted_session_evaluation(self, activity_id, summary):
            self.completed.append((activity_id, summary, "failed"))
            return True

    queue = Queue()
    activities = ActivityStore()
    services = SimpleNamespace(
        context=SimpleNamespace(workspace_id=lambda: "workspace"),
        activity_store=activities,
        evaluations=SimpleNamespace(),
    )
    worker = SessionEvaluationWorker(queue, SimpleNamespace(load=lambda _id: (_ for _ in ()).throw(FileNotFoundError())), services)

    asyncio.run(worker._process({
        "msg_id": 7,
        "read_ct": MAX_DELIVERIES,
        "message": {"session_id": "missing-session", "agent_name": "agent"},
    }))

    assert activities.created[0]["source"] == "session_push"
    assert activities.completed[0][2] == "failed"
    assert queue.archived == [7]


def test_exhausted_provider_failure_terminalizes_linked_evaluation_in_postgres(
    lisa_test_db,
):
    from lisa.repositories.activities import PostgresActivityStore

    workspace_id = "11111111-1111-4111-8111-111111111111"
    activity_store = PostgresActivityStore(lisa_test_db.writer_url, workspace_id)
    queue = SessionEvaluationQueue(lisa_test_db.writer_url)
    session_id = f"session-{uuid4()}"
    evaluation_ids = []
    activity_ids = []

    class Evaluations:
        def evaluate_session(self, _agent_name, _session_id, **kwargs):
            activity_ids.append(kwargs["activity_id"])
            run = activity_store.create_evaluation(
                title="Evaluate policy",
                source="session_push",
                target_agent_name="checkout-agent",
                session_id=session_id,
                policy_name="Refund eligibility",
                policy_id="P1.2",
                policy_hash="a" * 64,
                judge_config={},
                request_key=str(uuid4()),
                activity_id=kwargs["activity_id"],
            )
            evaluation_ids.append(run.id)
            raise RuntimeError("provider unavailable")

    class Traces:
        def load(self, loaded_session_id):
            assert loaded_session_id == session_id
            return {
                "agent_names": ["checkout-agent"],
                "session_id": session_id,
                "payload_hash": "b" * 64,
            }

    queue.send(session_id, "checkout-agent", "b" * 64)
    message = queue.read_one()
    assert message is not None
    archived = []
    archive = queue.archive

    def archive_and_record(message_id):
        archived.append(message_id)
        archive(message_id)

    queue.archive = archive_and_record

    worker = SessionEvaluationWorker(
        queue,
        Traces(),
        SimpleNamespace(
            context=SimpleNamespace(workspace_id=lambda: workspace_id),
            activity_store=activity_store,
            evaluations=Evaluations(),
        ),
    )

    asyncio.run(worker._process({**message, "read_ct": MAX_DELIVERIES}))

    activity_id = activity_ids[0]
    thread = activity_store.get_activity_thread(activity_id)
    assert thread["activity"]["status"] == "error"
    run = activity_store.get_evaluation(evaluation_ids[0])
    assert run.status == "failed"
    assert activity_store.list_events(activity_id)[-1].event_type == "failed"
    assert archived == [message["msg_id"]]


def test_transient_evaluation_failure_is_retried_before_activity_is_terminal():
    class Queue:
        def __init__(self):
            self.deleted = []
            self.archived = []

        def delete(self, message_id):
            self.deleted.append(message_id)

        def archive(self, message_id):
            self.archived.append(message_id)

    class ActivityStore:
        def __init__(self):
            self.status = None

        def get_activity_thread(self, _activity_id):
            return {"activity": {"status": self.status}} if self.status else None

        def create_activity(self, **_activity):
            self.status = "running"

        def complete_agent_activity(self, _activity_id, _summary, *, status):
            self.status = status

    class Evaluations:
        def __init__(self, activities):
            self.activities = activities
            self.attempts = 0

        def evaluate_session(self, _agent_name, _session_id, **kwargs):
            assert kwargs["defer_activity_failure"] is True
            self.attempts += 1
            if self.attempts == 1:
                return {"status": "evaluation_error"}
            self.activities.status = "succeeded"
            return {"status": "pass"}

    class Traces:
        def load(self, session_id):
            return {
                "agent_names": ["checkout-agent"],
                "session_id": session_id,
                "payload_hash": "a" * 64,
            }

        def list_manifests(self):
            raise AssertionError("Worker should read only the session's manifest")

    activities = ActivityStore()
    evaluations = Evaluations(activities)
    queue = Queue()
    worker = SessionEvaluationWorker(
        queue,
        Traces(),
        SimpleNamespace(
            context=SimpleNamespace(workspace_id=lambda: "workspace"),
            activity_store=activities,
            evaluations=evaluations,
        ),
    )
    message = {
        "msg_id": 9,
        "read_ct": 1,
        "message": {
            "session_id": "session-1",
            "agent_name": "checkout-agent",
            "payload_hash": "a" * 64,
        },
    }

    asyncio.run(worker._process(message))
    assert activities.status == "running"
    assert queue.deleted == []

    message["read_ct"] = 2
    asyncio.run(worker._process(message))
    assert evaluations.attempts == 2
    assert activities.status == "succeeded"
    assert queue.deleted == [9]


def test_provider_exception_is_retried_before_activity_is_terminal():
    class Queue:
        def __init__(self):
            self.deleted = []
            self.archived = []

        def delete(self, message_id):
            self.deleted.append(message_id)

        def archive(self, message_id):
            self.archived.append(message_id)

    class ActivityStore:
        status = None

        def get_activity_thread(self, _activity_id):
            return {"activity": {"status": self.status}} if self.status else None

        def create_activity(self, **_activity):
            self.status = "running"

        def complete_agent_activity(self, _activity_id, _summary, *, status):
            self.status = status

    class Evaluations:
        def __init__(self, activities):
            self.activities = activities
            self.attempts = 0

        def evaluate_session(self, _agent_name, _session_id, **kwargs):
            assert kwargs["defer_activity_failure"] is True
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("temporary provider failure")
            self.activities.status = "succeeded"
            return {"status": "pass"}

    class Traces:
        def load(self, session_id):
            return {
                "agent_names": ["checkout-agent"],
                "session_id": session_id,
                "payload_hash": "a" * 64,
            }

        def list_manifests(self):
            raise AssertionError("Worker should read only the session's manifest")

    activities = ActivityStore()
    evaluations = Evaluations(activities)
    queue = Queue()
    worker = SessionEvaluationWorker(
        queue,
        Traces(),
        SimpleNamespace(
            context=SimpleNamespace(workspace_id=lambda: "workspace"),
            activity_store=activities,
            evaluations=evaluations,
        ),
    )
    message = {
        "msg_id": 10,
        "read_ct": 1,
        "message": {
            "session_id": "session-1",
            "agent_name": "checkout-agent",
            "payload_hash": "a" * 64,
        },
    }

    asyncio.run(worker._process(message))
    assert activities.status == "running"
    assert queue.deleted == []

    message["read_ct"] = 2
    asyncio.run(worker._process(message))
    assert evaluations.attempts == 2
    assert activities.status == "succeeded"
    assert queue.deleted == [10]
