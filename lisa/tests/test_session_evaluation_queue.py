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

        def record_session_evaluation_attempt_failure(self, activity_id, **failure):
            self.events.append((activity_id, failure))

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


def test_retryable_session_failure_is_visible_and_idempotent(lisa_test_db):
    from lisa.repositories.activities import PostgresActivityStore

    store = PostgresActivityStore(
        lisa_test_db.writer_url,
        "11111111-1111-4111-8111-111111111111",
    )
    activity = store.create_activity(
        title="Evaluate session for checkout-agent",
        source="session_push",
        kind="session_evaluation",
        activity_id=str(uuid4()),
    )
    failure = {
        "attempt": 2,
        "max_attempts": MAX_DELIVERIES,
        "error": "Judge timed out; token=private-value" + "x" * 5000,
        "delivery_key": "delivery-2",
    }

    store.record_session_evaluation_attempt_failure(activity.id, **failure)
    store.record_session_evaluation_attempt_failure(activity.id, **failure)

    feed_entry = next(
        item for item in store.list_activity_feed(limit=100) if item["id"] == activity.id
    )
    detail = store.get_activity_thread(activity.id)
    failures = [
        action for action in detail["actions"]
        if action["event_type"] == "session_evaluation_attempt_failed"
    ]
    assert feed_entry["status"] == "pending"
    assert "Judge timed out" in feed_entry["summary"]
    assert "private-value" not in feed_entry["summary"]
    assert len(feed_entry["summary"]) < 1400
    assert len(failures) == 1
    assert "Evaluation attempt 2 of 8 failed" in failures[0]["summary"]
    assert "will retry" in failures[0]["summary"]
    assert failures[0]["reference_id"] is None


def test_retryable_evaluation_event_is_enriched_without_duplicate_and_live_lease_is_preserved(
    lisa_test_db,
):
    from lisa.repositories.activities import PostgresActivityStore

    store = PostgresActivityStore(
        lisa_test_db.writer_url,
        "11111111-1111-4111-8111-111111111111",
    )
    activity = store.create_activity(
        title="Evaluate a provider retry",
        source="session_push",
        kind="session_evaluation",
        activity_id=str(uuid4()),
    )
    claim = store.claim_activity(activity.id, "lisa-evaluation", lease_seconds=60)
    assert claim is not None
    store.release_activity(
        activity.id,
        actor=claim.owner,
        fencing_token=claim.fencing_token,
        reason="Judge call interrupted",
        failure_reference_id="delivery-1:policy-P1.2",
    )

    store.record_session_evaluation_attempt_failure(
        activity.id,
        attempt=1,
        max_attempts=MAX_DELIVERIES,
        error="Provider returned a timeout",
        delivery_key="delivery-1",
    )
    detail = store.get_activity_thread(activity.id)
    assert len([
        action for action in detail["actions"]
        if action["event_type"] == "session_evaluation_attempt_failed"
    ]) == 1
    assert not any(action["event_type"] == "retryable_error" for action in detail["actions"])
    assert "Judge call interrupted" in detail["actions"][-1]["summary"]
    assert "Provider returned a timeout" in detail["actions"][-1]["summary"]
    assert detail["activity"]["status"] == "pending"

    live_activity = store.create_activity(
        title="Evaluate while a lease is active",
        source="session_push",
        kind="session_evaluation",
        activity_id=str(uuid4()),
    )
    live_claim = store.claim_activity(live_activity.id, "lisa-evaluation", lease_seconds=60)
    assert live_claim is not None
    store.record_session_evaluation_attempt_failure(
        live_activity.id,
        attempt=1,
        max_attempts=MAX_DELIVERIES,
        error="Timestamp construction failed",
        delivery_key="delivery-1",
    )
    live_detail = store.get_activity_thread(live_activity.id)
    assert live_detail["activity"]["status"] == "pending"
    assert not store.fail_exhausted_session_evaluation(
        live_activity.id, "A live evaluator must retain ownership."
    )
    assert not store.finish_activity_without_claim(
        live_activity.id, "An active lease must remain owned by the evaluator.", status="failed"
    )


def test_retryable_evaluation_error_is_redacted_before_persistence_and_marker_is_not_trusted(
    lisa_test_db,
):
    from lisa.repositories.activities import PostgresActivityStore

    store = PostgresActivityStore(
        lisa_test_db.writer_url,
        "11111111-1111-4111-8111-111111111111",
    )
    activity = store.create_activity(
        title="Evaluate a provider error",
        source="session_push",
        kind="session_evaluation",
        activity_id=str(uuid4()),
    )
    claim = store.claim_activity(activity.id, "lisa-evaluation", lease_seconds=60)
    assert claim is not None
    store.release_activity(
        activity.id,
        actor=claim.owner,
        fencing_token=claim.fencing_token,
        reason="Evaluation attempt 1 of 8 failed: token=provider-secret",
        failure_reference_id="delivery-1:policy-P1.2",
    )

    event_before_worker_record = store.list_events(activity.id)[-1]
    assert event_before_worker_record.summary == "Evaluation attempt 1 of 8 failed: token=[REDACTED]"
    assert "provider-secret" not in event_before_worker_record.summary
    second_claim = store.claim_activity(activity.id, "lisa-evaluation", lease_seconds=60)
    assert second_claim is not None
    store.release_activity(
        activity.id,
        actor=second_claim.owner,
        fencing_token=second_claim.fencing_token,
        reason="Policy P1.3 failed: token=second-provider-secret",
        failure_reference_id="delivery-1:policy-P1.3",
    )

    store.record_session_evaluation_attempt_failure(
        activity.id,
        attempt=1,
        max_attempts=MAX_DELIVERIES,
        error=(
            "Judge response failed: token=second-secret; "
            "Policy P1.3 failed: token=third-secret"
        ),
        delivery_key="delivery-1",
    )
    store.record_session_evaluation_attempt_failure(
        activity.id,
        attempt=1,
        max_attempts=MAX_DELIVERIES,
        error="Duplicate recorder delivery",
        delivery_key="delivery-1",
    )

    thread = store.get_activity_thread(activity.id)
    failures = [
        action for action in thread["actions"]
        if action["event_type"] == "session_evaluation_attempt_failed"
    ]
    other_retry_events = [
        action for action in thread["actions"]
        if action["event_type"] == "retryable_error"
    ]
    assert len(failures) == 1
    assert "Judge response failed: token=[REDACTED]" in failures[0]["summary"]
    assert "Policy P1.3 failed: token=[REDACTED]" in failures[0]["summary"]
    assert "provider-secret" not in failures[0]["summary"]
    assert "second-provider-secret" not in failures[0]["summary"]
    assert "second-secret" not in failures[0]["summary"]
    assert "third-secret" not in failures[0]["summary"]
    assert len(other_retry_events) == 1
    assert "second-provider-secret" not in other_retry_events[0]["summary"]
    assert thread["activity"]["summary"].startswith(
        "Evaluation attempt 1 of 8 failed: Judge response failed: token=[REDACTED]"
    )
    assert "Duplicate recorder delivery" not in thread["activity"]["summary"]
    assert "second-secret" not in thread["activity"]["summary"]


def test_successful_retry_replaces_the_pending_failure_summary(lisa_test_db):
    from lisa.repositories.activities import PostgresActivityStore

    store = PostgresActivityStore(
        lisa_test_db.writer_url,
        "11111111-1111-4111-8111-111111111111",
    )
    activity = store.create_activity(
        title="Evaluate a recovered session",
        source="session_push",
        kind="session_evaluation",
        activity_id=str(uuid4()),
    )
    store.record_session_evaluation_attempt_failure(
        activity.id,
        attempt=1,
        max_attempts=MAX_DELIVERIES,
        error="Temporary timeout",
        delivery_key="delivery-1",
    )
    assert store.finish_activity_when_complete(activity.id)

    assert store.clear_session_evaluation_retry_summary(activity.id)
    thread = store.get_activity_thread(activity.id)
    assert thread["activity"]["status"] == "success"
    assert thread["activity"]["summary"] == "Evaluation completed after a retry."


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
            raise RuntimeError("provider unavailable token=private-value")

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
    assert "provider unavailable" in thread["activity"]["summary"]
    assert "private-value" not in thread["activity"]["summary"]
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
            self.failures = []

        def get_activity_thread(self, _activity_id):
            return {"activity": {"status": self.status}} if self.status else None

        def create_activity(self, **_activity):
            self.status = "running"

        def complete_agent_activity(self, _activity_id, _summary, *, status):
            self.status = status

        def record_session_evaluation_attempt_failure(self, activity_id, **failure):
            self.failures.append((activity_id, failure))

        def clear_session_evaluation_retry_summary(self, _activity_id):
            return False

    class Evaluations:
        def __init__(self, activities):
            self.activities = activities
            self.attempts = 0

        def evaluate_session(self, _agent_name, _session_id, **kwargs):
            assert kwargs["defer_activity_failure"] is True
            assert kwargs["failure_delivery_key"] in {"delivery-1", "delivery-2"}
            self.attempts += 1
            if self.attempts == 1:
                return {
                    "status": "evaluation_error",
                    "evaluations": [{"status": "error", "rationale": "Judge timed out"}],
                }
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
    assert len(activities.failures) == 1
    assert activities.failures[0][1] == {
        "attempt": 1,
        "max_attempts": MAX_DELIVERIES,
        "error": "Judge timed out",
        "delivery_key": "delivery-1",
    }

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

        def __init__(self):
            self.failures = []

        def get_activity_thread(self, _activity_id):
            return {"activity": {"status": self.status}} if self.status else None

        def create_activity(self, **_activity):
            self.status = "running"

        def complete_agent_activity(self, _activity_id, _summary, *, status):
            self.status = status

        def record_session_evaluation_attempt_failure(self, activity_id, **failure):
            self.failures.append((activity_id, failure))

        def clear_session_evaluation_retry_summary(self, _activity_id):
            return False

    class Evaluations:
        def __init__(self, activities):
            self.activities = activities
            self.attempts = 0

        def evaluate_session(self, _agent_name, _session_id, **kwargs):
            assert kwargs["defer_activity_failure"] is True
            assert kwargs["failure_delivery_key"] in {"delivery-1", "delivery-2"}
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
    assert len(activities.failures) == 1
    assert activities.failures[0][1] == {
        "attempt": 1,
        "max_attempts": MAX_DELIVERIES,
        "error": "temporary provider failure",
        "delivery_key": "delivery-1",
    }

    message["read_ct"] = 2
    asyncio.run(worker._process(message))
    assert evaluations.attempts == 2
    assert activities.status == "succeeded"
    assert queue.deleted == [10]


def test_failure_record_write_error_keeps_message_queued_and_redacts_operator_log(caplog):
    class Queue:
        def __init__(self):
            self.deleted = []
            self.archived = []

        def delete(self, message_id):
            self.deleted.append(message_id)

        def archive(self, message_id):
            self.archived.append(message_id)

    class ActivityStore:
        def get_activity_thread(self, _activity_id):
            return None

        def create_activity(self, **_activity):
            return None

        def record_session_evaluation_attempt_failure(self, _activity_id, **_failure):
            raise RuntimeError("database unavailable token=database-secret")

    class Traces:
        def load(self, session_id):
            return {
                "agent_names": ["checkout-agent"],
                "session_id": session_id,
                "payload_hash": "a" * 64,
            }

    class Evaluations:
        def evaluate_session(self, *_args, **_kwargs):
            raise RuntimeError("provider unavailable token=provider-secret")

    queue = Queue()
    worker = SessionEvaluationWorker(
        queue,
        Traces(),
        SimpleNamespace(
            context=SimpleNamespace(workspace_id=lambda: "workspace"),
            activity_store=ActivityStore(),
            evaluations=Evaluations(),
        ),
    )
    message = {
        "msg_id": 11,
        "read_ct": 1,
        "message": {
            "session_id": "session-1",
            "agent_name": "checkout-agent",
            "payload_hash": "a" * 64,
        },
    }

    asyncio.run(worker._process(message))

    assert queue.deleted == []
    assert queue.archived == []
    assert "database-secret" not in caplog.text
    assert "provider-secret" not in caplog.text
    assert "token=[REDACTED]" in caplog.text


def test_sampled_push_enables_optional_gap_signal_without_changing_evaluation_result():
    class Queue:
        def __init__(self):
            self.deleted = []

        def delete(self, message_id):
            self.deleted.append(message_id)

    class ActivityStore:
        def get_activity_thread(self, _activity_id):
            return None

        def create_activity(self, **_activity):
            return None

        def clear_session_evaluation_retry_summary(self, _activity_id):
            pass

    class Traces:
        def load(self, session_id):
            return {"agent_names": ["agent"], "payload_hash": "hash"}

    class Evaluations:
        def evaluate_session(self, *_args, **kwargs):
            assert kwargs["policy_discovery_enabled"] is True
            return {"status": "completed", "evaluations": [{"status": "pass"}]}

    class Repository:
        def __init__(self):
            self.completed = []

        def complete_sample_evaluation(self, session_id, *, gap_reviewed):
            self.completed.append((session_id, gap_reviewed))

    class Discovery:
        def __init__(self):
            self.repository = Repository()

        def reserve_sample(self, **_kwargs):
            return True

        def record_judge_signal(self, **_kwargs):
            return False

    queue, discovery = Queue(), Discovery()
    worker = SessionEvaluationWorker(
        queue,
        Traces(),
        SimpleNamespace(
            context=SimpleNamespace(workspace_id=lambda: "workspace"),
            activity_store=ActivityStore(), evaluations=Evaluations(),
        ),
        policy_discovery=discovery,
    )
    asyncio.run(worker._process({
        "msg_id": 11, "read_ct": 1,
        "message": {"session_id": "session-1", "agent_name": "agent", "payload_hash": "hash"},
    }))
    assert queue.deleted == [11]
    assert discovery.repository.completed == [("session-1", False)]
