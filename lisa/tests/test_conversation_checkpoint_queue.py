from __future__ import annotations

from lisa.context import WorkspaceContext
from lisa.repositories.conversation_checkpoint_queue import ConversationCheckpointQueue
from lisa.repositories.conversation_context import ConversationContextRepository
from lisa.repositories.conversations import FileConversationRepository
from lisa.services import conversation_checkpoints
from lisa.services.conversation_checkpoints import ConversationCheckpointWorker

from lisa import conversation_context

WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"
CONVERSATION_ID = "22222222-2222-4222-8222-222222222222"


def test_checkpoint_queue_carries_only_stable_conversation_and_request_keys(lisa_test_db):
    queue = ConversationCheckpointQueue(lisa_test_db.writer_url)

    queue.send("conversation-1", "request-1")
    message = queue.read_one()

    assert message is not None
    assert message["message"] == {
        "conversation_id": "conversation-1",
        "request_id": "request-1",
    }
    queue.delete(message["msg_id"])
    assert queue.read_one() is None


def test_checkpoint_queue_coalesces_pending_jobs_for_one_conversation(lisa_test_db):
    queue = ConversationCheckpointQueue(lisa_test_db.writer_url)

    assert queue.send("conversation-1", "request-1") == (True, None)
    assert queue.send("conversation-1", "request-2") == (False, "request-1")
    assert queue.send("conversation-2", "request-3") == (True, None)

    first = queue.read_one()
    second = queue.read_one()
    assert first is not None
    assert second is not None
    assert {first["message"]["conversation_id"], second["message"]["conversation_id"]} == {
        "conversation-1",
        "conversation-2",
    }
    queue.delete(first["msg_id"])
    queue.delete(second["msg_id"])
    assert queue.read_one() is None


def test_checkpoint_worker_publishes_before_ack_and_reuses_published_result(tmp_path, monkeypatch):
    conversations = FileConversationRepository(tmp_path)
    directory = conversations.conversation_directory(WORKSPACE_ID, CONVERSATION_ID)
    repository = ConversationContextRepository(directory)
    request_id = "a" * 64
    checkpoint_input = {
        "captured_at": "2026-10-22T00:00:00+00:00",
        "prefix_event_ids": ["event-1"],
        "prefix_fingerprint": "prefix-hash",
        "config_fingerprint": "a" * 64,
        "messages": [{"role": "user", "content": "Review policy P1.1."}],
    }
    repository.write_checkpoint_input(request_id, checkpoint_input)

    class Queue:
        deleted = 0
        archived = 0

        def delete(self, _message_id):
            assert repository.read_checkpoint(request_id) is not None
            assert not (directory / "context" / "checkpoint-inputs" / f"{request_id}.json").exists()
            self.deleted += 1

        def archive(self, _message_id):
            self.archived += 1

    queue = Queue()
    calls = []
    monkeypatch.setattr(
        conversation_context,
        "generate_checkpoint_summary",
        lambda payload, llm: calls.append((payload, llm)) or "Keep P1.1 pending.",
    )
    worker = ConversationCheckpointWorker(
        queue,
        conversations,
        lambda: "configured-model",
        WorkspaceContext(workspace=WORKSPACE_ID, data_dir=tmp_path),
    )
    message = {
        "msg_id": 7,
        "read_ct": 1,
        "message": {"conversation_id": CONVERSATION_ID, "request_id": request_id},
    }

    worker._process(message)
    worker._process(message)

    assert len(calls) == 1
    assert calls[0][0] == checkpoint_input
    assert queue.deleted == 2
    assert queue.archived == 0
    assert repository.current_checkpoint("a" * 64)["summary"] == "Keep P1.1 pending."


def test_checkpoint_worker_archives_after_retry_limit(tmp_path, monkeypatch):
    conversations = FileConversationRepository(tmp_path)
    directory = conversations.conversation_directory(WORKSPACE_ID, CONVERSATION_ID)
    repository = ConversationContextRepository(directory)
    request_id = "b" * 64
    repository.write_checkpoint_input(
        request_id,
        {
            "captured_at": "2026-10-22T00:00:00+00:00",
            "prefix_event_ids": ["event-1"],
            "prefix_fingerprint": "prefix-hash",
            "config_fingerprint": "b" * 64,
            "messages": [{"role": "user", "content": "Review policy."}],
        },
    )

    class Queue:
        deleted = 0
        archived = 0

        def delete(self, _message_id):
            self.deleted += 1

        def archive(self, _message_id):
            self.archived += 1

    queue = Queue()
    monkeypatch.setattr(
        conversation_context,
        "generate_checkpoint_summary",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("model unavailable")),
    )
    worker = ConversationCheckpointWorker(
        queue,
        conversations,
        lambda: "configured-model",
        WorkspaceContext(workspace=WORKSPACE_ID, data_dir=tmp_path),
    )

    worker._process(
        {
            "msg_id": 8,
            "read_ct": conversation_checkpoints.MAX_DELIVERIES,
            "message": {"conversation_id": CONVERSATION_ID, "request_id": request_id},
        }
    )

    assert queue.deleted == 0
    assert queue.archived == 1
    assert repository.read_checkpoint(request_id) is None
    assert not (directory / "context" / "checkpoint-inputs" / f"{request_id}.json").exists()


def test_checkpoint_worker_keeps_input_for_intermediate_retry(tmp_path, monkeypatch):
    conversations = FileConversationRepository(tmp_path)
    directory = conversations.conversation_directory(WORKSPACE_ID, CONVERSATION_ID)
    repository = ConversationContextRepository(directory)
    request_id = "c" * 64
    checkpoint_input = {
        "captured_at": "2026-10-22T00:00:00+00:00",
        "prefix_event_ids": ["event-1"],
        "prefix_fingerprint": "prefix-hash",
        "config_fingerprint": request_id,
        "messages": [{"role": "user", "content": "Review policy."}],
    }
    repository.write_checkpoint_input(request_id, checkpoint_input)

    class Queue:
        deleted = 0
        archived = 0

        def delete(self, _message_id):
            self.deleted += 1

        def archive(self, _message_id):
            self.archived += 1

    queue = Queue()
    calls = []

    def summarize(payload, _llm):
        calls.append(payload)
        if len(calls) == 1:
            raise RuntimeError("temporary model failure")
        return "Keep the policy review pending."

    monkeypatch.setattr(conversation_context, "generate_checkpoint_summary", summarize)
    worker = ConversationCheckpointWorker(
        queue,
        conversations,
        lambda: "configured-model",
        WorkspaceContext(workspace=WORKSPACE_ID, data_dir=tmp_path),
    )
    message = {
        "msg_id": 9,
        "read_ct": 1,
        "message": {"conversation_id": CONVERSATION_ID, "request_id": request_id},
    }

    worker._process(message)

    assert repository.read_checkpoint_input(request_id) == checkpoint_input
    assert queue.deleted == 0
    assert queue.archived == 0

    worker._process({**message, "read_ct": 2})

    assert calls == [checkpoint_input, checkpoint_input]
    assert repository.read_checkpoint(request_id)["summary"] == ("Keep the policy review pending.")
    assert not (directory / "context" / "checkpoint-inputs" / f"{request_id}.json").exists()
    assert queue.deleted == 1
    assert queue.archived == 0


def test_checkpoint_worker_discards_job_for_deleted_conversation(tmp_path, monkeypatch):
    conversations = FileConversationRepository(tmp_path)
    directory = conversations.conversation_directory(WORKSPACE_ID, CONVERSATION_ID)
    repository = ConversationContextRepository(directory)
    request_id = "d" * 64
    directory.mkdir(parents=True)
    repository.write_checkpoint_input(
        request_id,
        {
            "captured_at": "2026-10-22T00:00:00+00:00",
            "prefix_event_ids": ["event-1"],
            "prefix_fingerprint": "prefix-hash",
            "config_fingerprint": "d" * 64,
            "messages": [{"role": "user", "content": "Review policy."}],
        },
    )
    import shutil

    shutil.rmtree(directory)

    class Queue:
        deleted = 0
        archived = 0

        def delete(self, _message_id):
            self.deleted += 1

        def archive(self, _message_id):
            self.archived += 1

    queue = Queue()
    generated = []
    monkeypatch.setattr(
        conversation_context,
        "generate_checkpoint_summary",
        lambda *_args: generated.append(True) or "Must not be generated.",
    )
    worker = ConversationCheckpointWorker(
        queue,
        conversations,
        lambda: "configured-model",
        WorkspaceContext(workspace=WORKSPACE_ID, data_dir=tmp_path),
    )

    worker._process(
        {
            "msg_id": 9,
            "read_ct": 1,
            "message": {"conversation_id": CONVERSATION_ID, "request_id": request_id},
        }
    )

    assert generated == []
    assert queue.deleted == 1
    assert queue.archived == 0
    assert not directory.exists()
