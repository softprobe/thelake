from pathlib import Path

from lisa.repositories.conversations import FileConversationRepository


def test_instances_for_same_data_directory_share_conversation_locks(tmp_path: Path):
    first = FileConversationRepository(tmp_path)
    second = FileConversationRepository(tmp_path)
    directory = first.conversation_directory(
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
    )

    assert first.lock_for(directory) is second.lock_for(directory)
