import multiprocessing
import shutil
from pathlib import Path

from lisa.repositories.conversations import FileConversationRepository
from lisa.services.conversations import ConversationService


def _acquire_process_lock(data_dir, directory, entered, release):
    repository = FileConversationRepository(Path(data_dir))
    with repository.process_lock(Path(directory)):
        entered.set()
        release.wait(timeout=5)


def test_instances_for_same_data_directory_share_conversation_locks(tmp_path: Path):
    first = FileConversationRepository(tmp_path)
    second = FileConversationRepository(tmp_path)
    directory = first.conversation_directory(
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
    )

    assert first.lock_for(directory) is second.lock_for(directory)


def test_same_conversation_id_is_scoped_to_its_workspace(tmp_path: Path):
    repository = FileConversationRepository(tmp_path)
    conversations = ConversationService(repository)
    first_workspace = "11111111-1111-4111-8111-111111111111"
    second_workspace = "33333333-3333-4333-8333-333333333333"
    conversation_id = "22222222-2222-4222-8222-222222222222"
    first = conversations.create_conversation(first_workspace, conversation_id)
    conversations.create_conversation(second_workspace, conversation_id)

    first_directory = repository.conversation_directory(first_workspace, conversation_id)
    first_metadata = repository.load_metadata(first_directory, first_workspace)
    first_metadata["messages"] = [{"id": "m1", "role": "user", "text": "workspace A"}]
    repository.save_metadata(first_directory, first_metadata)

    assert first["workspace_id_hash"] != conversations.get_conversation(
        second_workspace, conversation_id
    )["workspace_id_hash"]
    assert conversations.get_conversation(first_workspace, conversation_id)["messages"][0][
        "text"
    ] == "workspace A"
    assert conversations.get_conversation(second_workspace, conversation_id)["messages"] == []


def test_process_lock_serializes_independent_processes(tmp_path: Path):
    repository = FileConversationRepository(tmp_path)
    directory = repository.conversation_directory(
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
    )
    context = multiprocessing.get_context("spawn")
    first_entered = context.Event()
    release_first = context.Event()
    second_entered = context.Event()
    release_second = context.Event()
    first = context.Process(
        target=_acquire_process_lock,
        args=(str(tmp_path), str(directory), first_entered, release_first),
    )
    second = context.Process(
        target=_acquire_process_lock,
        args=(str(tmp_path), str(directory), second_entered, release_second),
    )

    first.start()
    assert first_entered.wait(timeout=5)
    second.start()
    assert not second_entered.wait(timeout=0.25)
    release_first.set()
    assert second_entered.wait(timeout=5)
    release_second.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert first.exitcode == 0
    assert second.exitcode == 0


def test_lock_stays_stable_when_conversation_directory_is_deleted(tmp_path: Path):
    repository = FileConversationRepository(tmp_path)
    directory = repository.conversation_directory(
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
    )
    directory.mkdir(parents=True)
    context = multiprocessing.get_context("spawn")
    first_entered = context.Event()
    release_first = context.Event()
    second_entered = context.Event()
    release_second = context.Event()
    first = context.Process(
        target=_acquire_process_lock,
        args=(str(tmp_path), str(directory), first_entered, release_first),
    )
    second = context.Process(
        target=_acquire_process_lock,
        args=(str(tmp_path), str(directory), second_entered, release_second),
    )

    first.start()
    assert first_entered.wait(timeout=5)
    shutil.rmtree(directory)
    second.start()
    assert not second_entered.wait(timeout=0.25)
    release_first.set()
    assert second_entered.wait(timeout=5)
    release_second.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert first.exitcode == 0
    assert second.exitcode == 0
    assert not directory.exists()
